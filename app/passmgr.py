"""passmgr.py — 免 SSH 修改节点账号密码（v2.5.0 F6）。

三类密码，三条通道（混在一起谈才会觉得绕）
------------------------------------------
  ① 数据库账号密码（root / ha_monitor / replication / app）
     → **服务端直连主库改** + binlog 复制到从库。不需要 SSH，也不需要 agent。
  ② 节点文件凭据（keepalived 的 ha_check.conf、compose 的 MYSQL_ROOT_PASSWORD 等）
     → 数据库复制管不到，**必须靠 agent** 的 update_files + restart_service。
  ③ agent token → agent 的 set_credential。

两条铁律（都是真实事故换来的）
------------------------------
  1. **单一写入口**：绝不在只读副本上执行 CREATE/ALTER USER —— 会与复制线程争用
     mysql.* 元数据锁，导致复制死锁、Seconds_Behind 飙升。只在主库改，靠复制下发。
  2. **不同步 ha_check.conf 会导致 VIP 乱漂**：keepalived 拿旧密码做健康检查会全部失败，
     VIP 在节点间乱跳甚至双主，比改密失败危险得多。所以文件同步失败 → 整体回滚。

顺序为什么是 app → repl → ha_monitor → root
------------------------------------------
root 是管理器自愈的**回退凭据**，先改 root 的话中途失败就再也连不上节点了。
"""
import logging
import time

from .db import query, execute
from .state import get_current_master
from .clusters import get_cluster_creds, update_cluster
from .ha_client import connect
from . import crypto as _crypto

log = logging.getLogger(__name__)

# 可单独勾选修改的账号
ACCT_APP = "app"
ACCT_REPL = "replication"
ACCT_HA = "ha_monitor"
ACCT_ROOT = "root"

# 改密顺序（root 最后）
ORDER = (ACCT_APP, ACCT_REPL, ACCT_HA, ACCT_ROOT)

# 等待复制把新密码下发到从库的上限（秒）
REPL_WAIT_SEC = 30


def _master(cluster_id):
    h, p = get_current_master(cluster_id)
    return h, int(p or 0)


def _node_root(host, port):
    rows = query("SELECT root_user, root_pass FROM nodes WHERE ip=%s AND port=%s",
                 (host, int(port)))
    r = rows[0] if rows else {}
    return (r.get("root_user") or "root"), (r.get("root_pass") or "")


def _conn(host, port, user, pwd, timeout=5):
    return connect(host, int(port), user, pwd, timeout=timeout)


def _root_conn(cluster_id):
    mh, mp = _master(cluster_id)
    ru, rp = _node_root(mh, mp)
    c = _conn(mh, mp, ru, rp)
    if not c:
        raise RuntimeError("无法用 root 连接主库 %s:%s（改密必须在主库执行）" % (mh, mp))
    return (mh, mp), c


def _flag_on(v) -> bool:
    """把 @@read_only 类变量统一解析为 bool（MariaDB 返回 'ON'/'OFF'，MySQL 返回 1/0）。"""
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(int(v))
    return str(v).strip().upper() in ("1", "ON", "TRUE")


def _is_replica(conn):
    """自检：本节点是不是只读副本（是则拒绝写账号操作）。"""
    try:
        cur = conn.cursor()
        # MariaDB 的 @@read_only 返回 'OFF'/'ON' 字符串（MySQL 为 0/1 整型）：
        # int('ON') 抛 ValueError，会被本函数外层 except 吞掉并返回 False
        # （"不是副本"），导致**只读副本被误判为可写**，账号写操作（ALTER USER）
        # 直接打到副本上。故按两种形态统一解析。
        cur.execute("SELECT @@read_only")
        r = cur.fetchone()
        ro = _flag_on(r[0] if r else None)
        try:
            cur.execute("SELECT @@super_read_only")
            r2 = cur.fetchone()
            sro = _flag_on(r2[0] if r2 else None)
        except Exception:
            sro = False
        return bool(ro or sro)
    except Exception:
        return False


def _alter_user(conn, user, host, new_pass):
    cur = conn.cursor()
    cur.execute("ALTER USER '%s'@'%s' IDENTIFIED BY '%s'"
                % (user.replace("'", ""), host.replace("'", ""),
                   str(new_pass).replace("'", "").replace("\\", "\\\\")))


def _root_hosts(conn):
    cur = conn.cursor()
    cur.execute("SELECT host FROM mysql.user WHERE user='root'")
    return [r[0] for r in (cur.fetchall() or [])]


def change(cluster_id, changes, actor="", sync_node_files=True):
    """修改集群账号密码。

    changes: {"ha_monitor": "新密码", "replication": ..., "app": ..., "root": ...}
    只传要改的账号（UI 勾选哪些就改哪些）。

    返回 {ok, changed:[], steps:[], error}
    任一环节失败 → **回滚主库旧密码**并恢复 DB 记录，绝不留下“半套新密码”。
    """
    changes = {k: v for k, v in (changes or {}).items() if v}
    if not changes:
        return {"ok": False, "error": "没有给出任何新密码"}
    bad = [k for k in changes if k not in ORDER]
    if bad:
        return {"ok": False, "error": "不支持的账号：%s（仅 %s）"
                % ("、".join(bad), "、".join(ORDER))}

    steps = []
    try:
        (mh, mp), conn = _root_conn(cluster_id)
    except Exception as e:
        return {"ok": False, "error": str(e), "steps": steps}

    # ---- 闸门：绝不 在只读副本上改账号（会与复制线程争用 mysql.* 元数据锁）----
    if _is_replica(conn):
        try:
            conn.close()
        except Exception:
            pass
        return {"ok": False,
                "error": "当前主库 %s:%s 处于只读状态，拒绝改密（应连真正的主库）" % (mh, mp),
                "steps": steps}

    creds = get_cluster_creds(cluster_id) or {}
    old = {
        ACCT_HA: creds.get("ha_pass") or "",
        ACCT_REPL: creds.get("repl_pass") or "",
        ACCT_APP: creds.get("app_pass") or "",
        ACCT_ROOT: _node_root(mh, mp)[1] or "",
    }
    # 账号 → 数据库里真实用户名
    name_of = {
        ACCT_HA: creds.get("ha_user") or "ha_monitor",
        ACCT_REPL: creds.get("repl_user") or "replication",
        ACCT_APP: creds.get("app_user") or "app",
        ACCT_ROOT: "root",
    }

    try:
        # ---- ① 主库改账号（按 app → repl → ha_monitor → root 顺序）----
        for acct in ORDER:
            if acct not in changes:
                continue
            user = name_of[acct]
            if acct == ACCT_ROOT:
                hosts = _root_hosts(conn)
                if not hosts:
                    raise RuntimeError("主库上查不到 root 账号，无法改 root 密码")
                for h in hosts:
                    _alter_user(conn, "root", h, changes[acct])
            else:
                # 业务账号可能授权给多个 host（网段/172.28.%/127.0.0.1），逐个改
                cur = conn.cursor()
                cur.execute("SELECT host FROM mysql.user WHERE user=%s", (user,))
                hosts = [r[0] for r in (cur.fetchall() or [])]
                if not hosts:
                    raise RuntimeError("主库上不存在账号 %s" % user)
                for h in hosts:
                    _alter_user(conn, user, h, changes[acct])
            steps.append("主库已修改 %s" % acct)

        # ---- ② 更新管理端记录（加密落库）----
        _db_update(cluster_id, mh, mp, changes, name_of)
        steps.append("管理端凭证记录已更新")

        # ---- ③ 等复制把新密码下发到从库 ----
        ok_wait, wait_msg = _wait_replica_applied(cluster_id, mh, mp, changes)
        steps.append(wait_msg)
        if not ok_wait:
            raise RuntimeError(wait_msg)

        # ---- ④ 同步节点侧凭据文件（ha_check.conf 等）+ 重启 keepalived ----
        if sync_node_files:
            fs = _sync_node_files(cluster_id)
            steps.append(fs)

        return {"ok": True, "changed": sorted(changes.keys()), "steps": steps}
    except Exception as e:
        log.exception("改密失败，回滚")
        steps.append("失败：%s" % e)
        try:
            _rollback(conn, name_of, old, changes)
            _db_update(cluster_id, mh, mp, {k: old.get(k, "") for k in changes}, name_of)
            steps.append("已回滚主库旧密码与凭证记录")
        except Exception as re:
            steps.append("回滚失败（需人工介入）：%s" % re)
        return {"ok": False, "error": str(e), "steps": steps}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _db_update(cluster_id, mh, mp, changes, name_of):
    fields = {}
    if ACCT_HA in changes:
        fields["ha_pass"] = changes[ACCT_HA]
    if ACCT_REPL in changes:
        fields["repl_pass"] = changes[ACCT_REPL]
    if ACCT_APP in changes:
        fields["app_pass"] = changes[ACCT_APP]
    if fields:
        update_cluster(cluster_id, **fields)
    if ACCT_ROOT in changes:
        execute("UPDATE nodes SET root_pass=%s WHERE ip=%s AND port=%s",
                (_crypto.encrypt(changes[ACCT_ROOT]), mh, int(mp)))


def _rollback(conn, name_of, old, changes):
    for acct in ORDER:
        if acct not in changes or not old.get(acct):
            continue
        user = name_of[acct]
        if acct == ACCT_ROOT:
            for h in _root_hosts(conn):
                _alter_user(conn, "root", h, old[acct])
        else:
            cur = conn.cursor()
            cur.execute("SELECT host FROM mysql.user WHERE user=%s", (user,))
            for (h,) in (cur.fetchall() or []):
                _alter_user(conn, user, h, old[acct])


def _wait_replica_applied(cluster_id, mh, mp, changes):
    """等从库用新密码能连上（说明复制已把账号变更下发）。"""
    from . import syncgate
    deadline = time.time() + REPL_WAIT_SEC
    rows = query("SELECT host, port FROM hosts WHERE cluster_id=%s", (cluster_id,))
    reps = [(r["host"], int(r["port"])) for r in (rows or [])
            if not (r["host"] == mh and int(r["port"]) == mp)]
    if not reps:
        return True, "无从库，无需等待复制下发"
    pending = list(reps)
    while time.time() < deadline:
        still = []
        for h, p in pending:
            c = None
            try:
                c = _conn(h, p, "ha_monitor",
                          changes.get(ACCT_HA) or (get_cluster_creds(cluster_id) or {}).get("ha_pass", ""),
                          timeout=3)
                if not c:
                    still.append((h, p))
            except Exception:
                still.append((h, p))
            finally:
                try:
                    if c:
                        c.close()
                except Exception:
                    pass
        pending = still
        if not pending:
            return True, "复制已把新密码下发到全部从库"
        time.sleep(2)
    return False, "等待复制下发超时：%s 仍未生效（可能复制中断）" % \
                  "、".join("%s:%s" % x for x in pending)


def _regen_node(ip):
    """按节点已保存的参数重新生成单元（与「重新生成」按钮同一条路径）。

    必须沿用已存的 node_type / mysql_version，否则会按默认 docker 重建，
    把裸机 MariaDB 节点错误生成成「容器 + keepalived」合一部署。
    """
    from .node_gen import generate_node
    row = query(
        "SELECT n.ip, n.port, n.root_user, n.root_pass, n.cluster_id, h.initial_role, "
        "n.node_type, n.mysql_version "
        "FROM nodes n LEFT JOIN hosts h ON h.host=n.ip AND h.cluster_id=n.cluster_id "
        "WHERE n.ip=%s", (ip,))
    if not row:
        raise RuntimeError("未找到节点的生成记录")
    n = row[0]
    generate_node(n["ip"], n["port"], n["root_user"], n["root_pass"],
                  n.get("initial_role") or "slave", int(n["cluster_id"] or 0),
                  node_type=n.get("node_type") or "mariadb-docker",
                  mysql_version=n.get("mysql_version"))


def _sync_node_files(cluster_id):
    """重新生成节点单元（拿到含新密码的 ha_check.conf），并下发 agent 更新 + 重启 keepalived。

    为什么要重启 keepalived：它用 ha_check.conf 里的密码做健康检查，
    不重启就还在用旧密码 → 健康检查全失败 → VIP 乱漂/双主。
    """
    from . import node_gen, agent_hub
    import base64
    rows = query("SELECT ip, port FROM nodes WHERE cluster_id=%s", (cluster_id,))
    if not rows:
        return "无节点需要同步"
    done, fail = 0, []
    for r in rows:
        ip = r["ip"]
        try:
            _regen_node(ip)
        except Exception as e:
            fail.append("%s 重新生成失败：%s" % (ip, e))
            continue
        try:
            import os
            from .config import NODES_DIR
            p = os.path.join(NODES_DIR, ip, "keepalived", "ha_check.conf")
            if not os.path.isfile(p):
                fail.append("%s 缺少 ha_check.conf" % ip)
                continue
            with open(p, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            agent_hub.enqueue(ip, "update_files",
                              {"files": {"/etc/keepalived/ha_check.conf": b64},
                               "restart": ["keepalived"]},
                              actor="passmgr", ttl_sec=1800,
                              dedupe_key="ha_check_%s" % ip)
            done += 1
        except Exception as e:
            fail.append("%s 下发失败：%s" % (ip, e))
    msg = "已向 %d 个节点下发凭据文件更新（含 keepalived 重启）" % done
    if fail:
        msg += "；失败：%s" % "；".join(fail)
    return msg
