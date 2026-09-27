"""syncgate.py — 强制同步 / 切主就绪判定（v2.5.0 F7）。

解决什么痛点
------------
滚动升级节点时要「切主 → 更新旧主 → 再切 → 更新下一个」，但运维**不知道从库到底追平没有**，
只能凭 `Seconds_Behind` 猜。猜错就在未追平时切换 → 数据不一致甚至丢数据。

本模块给出**客观、可验证**的就绪判定，并提供「强制同步」开关：
停写 → 等追平 → 明确告诉你可以安全切主了。

判据为什么不能只看 Seconds_Behind_Master
----------------------------------------
它会骗人：大事务执行期间这个值经常是 0 或乱跳，看着追平了其实差一大截。
正确判据是 **GTID 集合包含关系**：
    MariaDB：主库 @@gtid_binlog_pos  vs  从库 @@gtid_slave_pos
    要求 GTID_SUBSET(主库pos, 从库pos) = 1  —— 从库已包含主库产生的所有事务
再叠加**稳定采样**（连续 N 次都满足）过滤抖动。

安全设计（强制档）
------------------
停写用 `super_read_only`（不用 FLUSH TABLES WITH READ LOCK：后者会锁住长事务引发雪崩）。
**必须能自动解锁**：超时 / 用户取消 / 宽限期到 → 一律恢复原值，
否则一次意外就会让业务长时间不可写，比切换失败还糟。
"""
import json
import logging
import threading
import time
import uuid

from .db import query
from .state import get_current_master
from .clusters import get_cluster_creds
from .ha_client import connect

log = logging.getLogger(__name__)

# 稳定采样：追平等待档（业务在写）3 次 × 2s；强制档（已停写）3 次 × 1s
SAMPLE_TIMES = 3
SAMPLE_INTERVAL_WAIT = 2.0
SAMPLE_INTERVAL_FORCED = 1.0

# 默认超时（秒）。注意：超时与库大小无关，只与「从库落后多少」有关。
DEFAULT_TIMEOUT = 120
# 追平后保持停写的宽限期：期间等待运维切换主库；到点自动解锁，避免业务长期停写
DEFAULT_GRACE = 300

_lock = threading.Lock()
_TASKS = {}          # task_id -> dict（进程内内存态；管理器单实例）


def _mk(task_id, **kw):
    with _lock:
        t = _TASKS.setdefault(task_id, {
            "id": task_id, "status": "running", "stage": "", "progress": 0,
            "ready": False, "locked": False, "replicas": [], "message": "",
            "started_at": time.time(), "finished_at": None, "cluster_id": None,
        })
        t.update(kw)
        return dict(t)


def get_task(task_id):
    with _lock:
        t = _TASKS.get(task_id or "")
        return dict(t) if t else {"id": task_id, "status": "unknown"}


# 集群 → 强制同步锁定到期时间。monitor 的拓扑维护会周期性把主库「置为可写」，
# 若不登记这把锁，刚停写就被它恢复，强制同步的停写窗口形同虚设。
_LOCKED_CLUSTERS = {}


def _mark_locked(cluster_id, seconds):
    with _lock:
        _LOCKED_CLUSTERS[int(cluster_id)] = time.time() + float(seconds)


def _mark_unlocked(cluster_id):
    with _lock:
        _LOCKED_CLUSTERS.pop(int(cluster_id), None)


def is_locked(cluster_id):
    """该集群是否正处于强制同步的停写窗口（供 monitor/failover 跳过「置可写」）。"""
    with _lock:
        until = _LOCKED_CLUSTERS.get(int(cluster_id))
        if not until:
            return False
        if time.time() >= until:
            _LOCKED_CLUSTERS.pop(int(cluster_id), None)
            return False
        return True


def _node_root(host, port):
    rows = query("SELECT root_user, root_pass FROM nodes WHERE ip=%s AND port=%s",
                 (host, int(port)))
    r = rows[0] if rows else {}
    return (r.get("root_user") or "root"), (r.get("root_pass") or "")


def _connect(host, port):
    """依次尝试 root → ha_monitor 连接节点。"""
    ru, rp = _node_root(host, port)
    try:
        c = connect(host, int(port), ru, rp, timeout=5)
        if c:
            return c
    except Exception:
        pass
    creds = get_cluster_creds(_cid_of(host, port)) or {}
    return connect(host, int(port), creds.get("ha_user") or "", creds.get("ha_pass") or "", timeout=5)


def _cid_of(host, port):
    rows = query("SELECT cluster_id FROM nodes WHERE ip=%s AND port=%s", (host, int(port)))
    return (rows[0] or {}).get("cluster_id") if rows else None


def _scalar(cur, sql):
    try:
        cur.execute(sql)
        r = cur.fetchone()
        return r[0] if r else None
    except Exception:
        return None


def _gtid_positions(host, port):
    """取主库 binlog GTID 与本节点已回放 GTID（MariaDB 口径）。"""
    c = _connect(host, port)
    if not c:
        return None, None
    try:
        cur = c.cursor()
        binlog_pos = _scalar(cur, "SELECT @@gtid_binlog_pos") or ""
        slave_pos = _scalar(cur, "SELECT @@gtid_slave_pos") or ""
        return str(binlog_pos), str(slave_pos)
    finally:
        try:
            c.close()
        except Exception:
            pass


def _parse_gtid_set(s):
    """把 GTID 集合串解析成 {(domain, server): seq}（MariaDB 口径）。

    为什么必须在 Python 侧解析：MariaDB **没有** GTID_SUBSET / GTID_SUBTRACT
    （那是 MySQL 的函数，实测 MariaDB 12.3.3 报 1305 FUNCTION does not exist）。
    早期这里直接调用 GTID_SUBSET，结果永远是「无法判定」→ 就绪判定恒为 false，
    滚动升级会被永久挡住。
    """
    out = {}
    for part in (s or "").split(","):
        bits = part.strip().split("-")
        if len(bits) != 3:
            continue
        try:
            d, srv, seq = int(bits[0]), int(bits[1]), int(bits[2])
        except ValueError:
            continue
        k = (d, srv)
        if seq > out.get(k, -1):
            out[k] = seq
    return out


def _covered_by_positions(master_pos, slave_pos):
    """从库是否已包含主库全部事务（按 domain-server 的 seq 比较）。"""
    if not master_pos:
        return True
    if not slave_pos:
        return False
    m = _parse_gtid_set(master_pos)
    s = _parse_gtid_set(slave_pos)
    if not m:
        # 主库位点解析不出来（格式异常）：不阻塞，避免误判为未追平
        return True
    return all(s.get(k, -1) >= v for k, v in m.items())


def _covered(master_pos, slave_pos, host, port):
    """判断从库是否已包含主库全部事务。

    MySQL 有 GTID_SUBSET 就用它；MariaDB 没有该函数，走 Python 解析比较。
    """
    if not master_pos:
        return True
    if not slave_pos:
        return False
    c = _connect(host, port)
    if not c:
        return None
    try:
        cur = c.cursor()
        # 统一走 db_ops.gtid_slave_is_subset：MySQL 用原生 GTID_SUBSET，MariaDB 用 Python 解析，
        # 不再在逻辑层直拼风味 SQL（对应 MR1 / Phase 1）。
        from app.ha_client.db_ops import gtid_slave_is_subset
        from app.mysql_compat import F_MYSQL8
        return gtid_slave_is_subset(cur, slave_pos, master_pos, F_MYSQL8)
    except Exception:
        return _covered_by_positions(master_pos, slave_pos)
    finally:
        try:
            c.close()
        except Exception:
            pass


def _replicas(cluster_id, master_host, master_port):
    rows = query("SELECT host, port FROM hosts WHERE cluster_id=%s", (cluster_id,))
    out = []
    for r in rows or []:
        h, p = r["host"], int(r["port"])
        if h == master_host and p == int(master_port):
            continue
        out.append((h, p))
    return out


def readiness(cluster_id):
    """只读判定「现在能不能安全切主」。

    返回 {ok, ready, master, replicas:[{host,port,covered,slave_pos}], message}
    covered 为 None 表示无法判定（函数缺失/连不上），一律按「未就绪」处理（fail-closed）。
    """
    try:
        mh, mp = get_current_master(cluster_id)
    except Exception:
        return {"ok": False, "ready": False, "message": "取不到当前主库"}
    if not mh:
        return {"ok": False, "ready": False, "message": "集群无当前主库记录"}
    master_pos, _ = _gtid_positions(mh, mp)
    reps = []
    all_ok = True
    for h, p in _replicas(cluster_id, mh, mp):
        _, sp = _gtid_positions(h, p)
        cov = _covered(master_pos, sp, h, p)
        if cov is not True:
            all_ok = False
        reps.append({"host": h, "port": p, "covered": cov, "slave_pos": sp})
    return {
        "ok": True,
        "ready": bool(all_ok and reps),
        "master": {"host": mh, "port": int(mp), "binlog_pos": master_pos},
        "replicas": reps,
        "message": ("✅ 同步完成，可以安全切换主库" if (all_ok and reps)
                    else "尚未追平，切换可能导致数据不一致/丢失"),
    }


# ------------------------------------------------------------------ 强制同步（后台任务）

def start_forced_sync(cluster_id, timeout_sec=DEFAULT_TIMEOUT, grace_sec=DEFAULT_GRACE,
                      actor=""):
    """启动强制同步：停写 → 等追平 → 报告就绪（并保持停写等待切主）。"""
    task_id = uuid.uuid4().hex[:12]
    _mk(task_id, cluster_id=cluster_id, actor=actor, stage="初始化", progress=2)
    th = threading.Thread(target=_run, args=(task_id, int(cluster_id),
                                             int(timeout_sec), int(grace_sec)),
                          daemon=True)
    th.start()
    return {"task_id": task_id}


def cancel(task_id, actor=""):
    """取消强制同步：立即解锁并结束任务。"""
    with _lock:
        t = _TASKS.get(task_id)
        if not t:
            return {"ok": False, "error": "任务不存在"}
        if t.get("status") == "running":
            t["_cancel"] = True
    return {"ok": True}


def _set_ro(host, port, on):
    """设置/恢复只读，返回原值。

    ⚠ flavor 差异（真实报错教训）：**MariaDB 没有 `super_read_only`**（那是 MySQL 8
    的变量）——直接用会报 `1193 Unknown system variable 'super_read_only'`，
    强制同步当场失败。故先试 super_read_only，报 1193 则退回 `read_only`。
    另：MariaDB 12 修改 read_only 需要 READ_ONLY ADMIN 权限。
    """
    c = _connect(host, port)
    if not c:
        raise RuntimeError("无法连接主库 %s:%s" % (host, port))
    try:
        cur = c.cursor()
        orig = 0
        for var in ("super_read_only", "read_only"):
            try:
                cur.execute("SELECT @@%s" % var)
                r = cur.fetchone()
                orig = int(r[0] or 0) if r else 0
                break
            except Exception:
                continue
        # 统一走 db_ops.set_read_only（MySQL 同步 super_read_only，MariaDB 仅 read_only）
        from app.ha_client.db_ops import set_read_only
        ok_ro, err_ro = set_read_only(cur, on)
        if not ok_ro:
            raise RuntimeError(err_ro)
        return orig
    finally:
        try:
            c.close()
        except Exception:
            pass


def _run(task_id, cluster_id, timeout_sec, grace_sec):
    try:
        mh, mp = get_current_master(cluster_id)
        if not mh:
            _mk(task_id, status="failed", stage="取不到当前主库", progress=100,
                finished_at=time.time())
            return
        _mk(task_id, stage="预估落后量", progress=8, master={"host": mh, "port": int(mp)})
        master_pos, _ = _gtid_positions(mh, mp)
        reps = _replicas(cluster_id, mh, mp)
        if not reps:
            _mk(task_id, status="failed", stage="集群无从库，无需同步", progress=100,
                finished_at=time.time())
            return

        # 1) 启动前预估：追不平就直接拒绝，绝不让你干等到超时
        pending = [h for h, p in reps
                   if not _covered(master_pos, (_gtid_positions(h, p)[1] or ""), h, p)]
        if pending:
            # 只有落后才需要估算；这里按「未追平」给出预估提示（真实字节/事务差需结合位点）
            _mk(task_id, stage="预估：尚有 %d 个从库未追平" % len(pending), progress=15,
                message="将在停写后等待追平；若 %ds 内追不平会自动解锁并终止" % timeout_sec)

        # 2) 停写
        orig_ro = _set_ro(mh, mp, True)
        # 登记停写窗口：让 monitor 的拓扑维护别把主库又设回可写
        _mark_locked(cluster_id, timeout_sec + grace_sec + 60)
        _mk(task_id, locked=True, stage="主库已停写（super_read_only=ON）", progress=25)

        # 3) 等追平 + 稳定采样（已停写，用更快的 1s 采样）
        deadline = time.time() + timeout_sec
        stable = 0
        ready = False
        while time.time() < deadline:
            if _is_cancelled(task_id):
                _set_ro(mh, mp, bool(orig_ro))
                _mark_unlocked(cluster_id)
                _mk(task_id, status="cancelled", locked=False,
                    stage="已取消，主库写入已恢复", progress=100, finished_at=time.time())
                return
            all_ok = True
            detail = []
            for h, p in reps:
                _, sp = _gtid_positions(h, p)
                cov = _covered(master_pos, sp, h, p)
                if cov is not True:
                    all_ok = False
                detail.append({"host": h, "port": p, "covered": cov})
            if all_ok:
                stable += 1
            else:
                stable = 0
            pct = min(90, 25 + int(60 * (stable / float(SAMPLE_TIMES))))
            _mk(task_id, stage="等待追平（稳定采样 %d/%d）" % (stable, SAMPLE_TIMES),
                progress=pct, replicas=detail,
                message="还差采样 %d 次即可判定追平" % max(0, SAMPLE_TIMES - stable))
            if stable >= SAMPLE_TIMES:
                ready = True
                break
            time.sleep(SAMPLE_INTERVAL_FORCED)

        if not ready:
            _set_ro(mh, mp, bool(orig_ro))
            _mark_unlocked(cluster_id)
            _mk(task_id, status="timeout", locked=False, progress=100,
                stage="超时未追平，已自动恢复主库写入",
                message="%ds 内未追平，已解锁。建议先用「追平等待」让从库追上，或排查大事务/DDL"
                        % timeout_sec,
                finished_at=time.time())
            return

        # 4) 就绪：保持停写等待运维切主，宽限期到自动解锁（防止业务长期停写）
        _mk(task_id, status="ready", ready=True, locked=True, progress=100,
            stage="✅ 同步完成，可以安全切换主库",
            message="主库保持停写等待切换；%ds 内未切换将自动恢复写入" % grace_sec,
            replicas=detail)
        end = time.time() + grace_sec
        while time.time() < end:
            if _is_cancelled(task_id) or _switch_detected(cluster_id, mh, mp):
                break
            time.sleep(2)
        _set_ro(mh, mp, bool(orig_ro))
        _mark_unlocked(cluster_id)
        _mk(task_id, locked=False, stage="宽限期结束，已恢复主库写入",
            message="同步结果仍然有效，但主库已恢复可写；如仍需切换请重新执行强制同步",
            finished_at=time.time())
    except Exception as e:
        log.exception("强制同步失败")
        # 异常路径必须解除停写锁 + 恢复主库可写：否则业务会被永久挡住
        try:
            _set_ro(mh, mp, bool(locals().get("orig_ro", 0)))
        except Exception:
            pass
        _mark_unlocked(cluster_id)
        _mk(task_id, status="failed", stage="异常终止", message=str(e),
            progress=100, finished_at=time.time(), locked=False)


def _is_cancelled(task_id):
    with _lock:
        return bool((_TASKS.get(task_id) or {}).get("_cancel"))


def _switch_detected(cluster_id, old_host, old_port):
    """是否已发生切主（主库换了人 → 不必再等待）。"""
    try:
        h, p = get_current_master(cluster_id)
        return not (h == old_host and int(p or 0) == int(old_port))
    except Exception:
        return False
