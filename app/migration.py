"""migration.py — 既有 MariaDB 集群 → Custos 托管集群 数据迁移向导后端。

⚠ 本模块**专注 MariaDB**：所有复制 / GTID / 客户端语法均使用 MariaDB 形态，
   不混入 MySQL 的 START SLAVE / RESET SLAVE / MASTER_AUTO_POSITION /
   CHANGE REPLICATION SOURCE TO / xtrabackup 等语法：
     · 复制命令：START REPLICA / STOP REPLICA / RESET REPLICA ALL / SHOW REPLICA STATUS
     · 复制建立：CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos   （MariaDB GTID 语义）
     · GTID 体系：gtid_domain_id / gtid_current_pos / gtid_slave_pos / @@gtid_strict_mode
     · 客户端：mariadb-dump / mariadb（10.5+），降级 mysqldump / mysql
     · 物理备份：mariabackup（MariaDB 官方物理备份工具）

关键时序（已在代码层确认，见 monitor.py / failover.py）：
   Custos 的 ensure_topology 每周期都会对「当前主」执行 promote_to_master
   （STOP REPLICA + RESET REPLICA ALL + read_only=0），且维护模式(v2.3.3)只抑制
   「自动切换」与「状态告警」，**不抑制 ensure_topology**。
   ⇒ 迁移期间目标节点【绝不注册进 Custos 受管集群】；只有停桥(STOP REPLICA)之后，
     cutover 才把目标节点登记进 Custos（create_cluster + 插入 hosts/nodes），
     随后 monitor 接管并提升新主。这样桥接通道不会被提前切断。
"""
import json
import os
import time
import shutil
import subprocess
import threading
import urllib.request
import urllib.error
import pymysql
from datetime import datetime, timedelta

from .db import query, execute, get_setting
from . import crypto
from .config import get_mgmt_host, NODE_AGENT_PORT, NODE_AGENT_TOKEN, NODE_AGENT_TIMEOUT
from .ha_client import connect, ensure_ha_creds
from .clusters import create_cluster, get_cluster_creds
from . import node_gen
from .config import (get_mgmt_host, get_manager_public_ip,
                     get_manager_docker_subnet_wildcard)
from .mysql_compat import (detect_flavour, change_replication_sql,
                           start_replication_sql, F_MYSQL8)

# ---- 库范围常量 ----
# MariaDB 系统库（永远不迁移、桥接也须忽略）
SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys", "ndbinfo"}
# Custos 自有库（不迁移、桥接忽略）
CUSTOS_DBS = {"ha_local"}
IGNORE_DBS = SYSTEM_DBS | CUSTOS_DBS

# 数据量阈值（GB）：小于→逻辑通道A；大于等于→物理通道B（用户可覆盖）
SIZE_THRESHOLD_GB = 500

# 向导步骤（顺序即步进条）
STEPS = ["seed", "bridge", "verify", "cutover", "cleanup"]
STEP_LABELS = {
    "seed": "数据种子",
    "bridge": "建立桥接",
    "verify": "一致性校验",
    "cutover": "停桥切换",
    "cleanup": "收尾工作",
}

_lock = threading.Lock()


# ---------------------------------------------------------------------------
# 配置加密 / 任务持久化
# ---------------------------------------------------------------------------
def _enc(obj):
    return crypto.encrypt(json.dumps(obj, ensure_ascii=False))


def _dec(blob):
    if not blob:
        return {}
    try:
        return json.loads(crypto.decrypt(blob))
    except Exception:
        try:
            return json.loads(blob)
        except Exception:
            return {}


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_task(task):
    cfg = _enc(task.get("config", {}))
    steps = json.dumps(task.get("steps", {}), ensure_ascii=False)
    if task.get("id"):
        execute(
            "UPDATE migration_tasks SET channel=%s,status=%s,config=%s,steps=%s,log=%s,updated_at=NOW() "
            "WHERE id=%s",
            (task.get("channel"), task["status"], cfg, steps, task.get("log", ""), task["id"]),
        )
    else:
        new_id = execute(
            "INSERT INTO migration_tasks (name, channel, status, config, steps, log, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,'',NOW(),NOW())",
            (task["name"], task.get("channel"), task["status"], cfg, steps),
        )
        task["id"] = new_id


def get_task(tid):
    rows = query("SELECT * FROM migration_tasks WHERE id=%s", (tid,))
    if not rows:
        return None
    t = rows[0]
    return {
        "id": t["id"],
        "name": t["name"],
        "channel": t.get("channel") or "logical",
        "status": t.get("status") or "draft",
        "progress": int(t.get("progress") or 0),
        "stage": t.get("stage") or "",
        "log": t.get("log") or "",
        "config": _dec(t.get("config")),
        "steps": json.loads(t.get("steps") or "{}"),
        "created_at": t.get("created_at") or "",
        "updated_at": t.get("updated_at") or "",
    }


def list_tasks():
    rows = query(
        "SELECT id, name, channel, status, progress, stage, created_at, updated_at "
        "FROM migration_tasks ORDER BY id DESC"
    )
    return rows


def delete_task(tid):
    """删除迁移任务。

    允许删除「执行中(running)」的任务记录：删除仅移除管理记录，不会停止后台
    正在进行的迁移步骤（如数据导出/恢复、GTID 桥接）。后台线程跑完后会在
    finally 中自行清理 _RUNNING，并发门禁随之自动解除，不会造成永久死锁。
    前端在删除执行中任务时会做二次确认并提示该风险。
    """
    task = get_task(tid)
    if not task:
        return {"ok": False, "error": "任务不存在"}
    execute("DELETE FROM migration_tasks WHERE id=%s", (tid,))
    return {"ok": True}


def cleanup_stale_running_tasks():
    """清理「卡死/陈旧」的执行中任务：状态为 running 但已无后台线程在跑
    （典型场景：管理器重启后，原本在跑的任务线程丢失、状态却残留在 running）。

    真正在跑的任务（tid in _RUNNING）不会被清理，避免误杀在途迁移。
    """
    try:
        if _RUNNING:
            busy = tuple(sorted(_RUNNING))
            rows = query("SELECT id FROM migration_tasks WHERE status='running' AND id NOT IN %s", (busy,))
        else:
            rows = query("SELECT id FROM migration_tasks WHERE status='running'")
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    ids = [int(r["id"]) for r in rows]
    if not ids:
        return {"ok": True, "deleted": 0, "ids": []}
    execute("DELETE FROM migration_tasks WHERE id IN %s", (tuple(ids),))
    return {"ok": True, "deleted": len(ids), "ids": ids}


# ---------------------------------------------------------------------------
# 源端探测（MariaDB 语法）
# ---------------------------------------------------------------------------
def probe_source(cfg):
    """连源 MariaDB，探测版本 / GTID / 数据量 / 用户库。"""
    conn = connect(cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"], timeout=8)
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
        # ⚠ 变量名必须是 MariaDB 写法 log_slave_updates。
        #   实测 MariaDB 12.3.3：SELECT @@log_replica_updates → ERROR 1193 Unknown system variable。
        #   （MySQL 8 才改成 source/replica 命名；MariaDB 12.3.3 不接受该别名。）
        #   逐项探测，任一变量缺失都不能拖垮整个探测。
        def _var(sql):
            try:
                cur.execute(sql)
                row = cur.fetchone()
                return row[0] if row else None
            except Exception:
                return None

        gtid_strict = _var("SELECT @@gtid_strict_mode")
        domain_id = _var("SELECT @@gtid_domain_id")
        log_bin = _var("SELECT @@log_bin")
        log_slave_updates = _var("SELECT @@log_slave_updates")

        placeholders = ",".join(["%s"] * len(IGNORE_DBS))
        cur.execute(
            f"SELECT schema_name FROM information_schema.schemata "
            f"WHERE schema_name NOT IN ({placeholders})",
            tuple(IGNORE_DBS),
        )
        raw_dbs = [x[0] for x in cur.fetchall()]
        # 排除 MariaDB 把数据目录内隐藏子目录编码成的伪库名（如 #mysql50#.xxx）。
        # 物理通道 B 会在源/目标 datadir 内建 .custos_mig_staging_* 暂存，若残留即被当成库，
        # 既不能迁移、也会让 mariadb-dump 报 Incorrect database name。
        databases = [d for d in raw_dbs if d and not d.startswith("#")]

        cur.execute(
            f"SELECT COALESCE(SUM(data_length+index_length),0)/1024/1024 "
            f"FROM information_schema.tables WHERE table_schema NOT IN ({placeholders})",
            tuple(IGNORE_DBS),
        )
        size_mb = float(cur.fetchone()[0] or 0)
        return {
            "ok": True,
            "version": version,
            "gtid_strict_mode": gtid_strict,
            "gtid_domain_id": domain_id,
            "log_bin": log_bin,
            "log_slave_updates": log_slave_updates,
            # 源端产品线：决定 dump 工具与 GTID 参数（两者互斥，绝不能混用）
            "flavor": flavor_of(version),
            "major": major_of(version),
            "databases": databases,
            "size_mb": round(size_mb, 1),
            "gtid_ready": bool(log_bin),
        }
    finally:
        conn.close()


def recommend_channel(probe, channel_pref=None):
    size_gb = (probe.get("size_mb") or 0) / 1024.0
    flavor = (probe.get("flavor") or "mariadb")
    if channel_pref in ("logical", "physical"):
        chosen = channel_pref
    else:
        chosen = "logical"  # 2.4.1 起取消高级模式（物理通道），仅保留通用模式（逻辑通道）
    if chosen == "physical":
        # 物理通道（mariabackup）仅 MariaDB 源可用：目标恒为 Custos MariaDB，
        # 跨引擎物理备份不可行（MySQL 8 起无 .frm；capabilities 也已把 physical 置 False）。
        if flavor != "mariadb":
            chosen = "logical"
            reason = ("源端为 %s，物理通道（mariabackup）仅支持 MariaDB 源；已改推逻辑通道（通用模式）。"
                      % ("MySQL" if flavor == "mysql" else flavor))
            return {"channel": chosen, "reason": reason, "size_gb": round(size_gb, 2)}
        reason = ("数据量 %.1f GB ≥ 阈值 %d GB，推荐物理通道（mariabackup 恢复更快，适合大库）"
                  % (size_gb, SIZE_THRESHOLD_GB))
    else:
        reason = ("数据量 %.1f GB < 阈值 %d GB，推荐逻辑通道（通用/干净/跨大版本安全，默认）"
                  % (size_gb, SIZE_THRESHOLD_GB))
    return {"channel": chosen, "reason": reason, "size_gb": round(size_gb, 2)}


# ---------------------------------------------------------------------------
# 引擎适配层：工具与 GTID 参数必须按【源端产品线】选择
#
# 实测结论（切勿凭印象改回）：
#   mariadb-dump --set-gtid-purged=ON → "unknown variable 'set-gtid-purged=ON'"
#   mysqldump    --gtid               → "unknown option '--gtid'"
#   两者参数集互斥 ⇒ 必须分流。且 MariaDB 的 --gtid 必须配合 --master-data/-dump-slave
#   才会真正输出 GTID 位点（单独传 --gtid 完全不起作用）。
#   MySQL 源写入 MariaDB 目标时必须 --set-gtid-purged=OFF，否则导入端
#   ERROR 1193 Unknown system variable 'GTID_PURGED'（MariaDB 没有该变量）。
# ---------------------------------------------------------------------------
def _which(p):
    return shutil.which(p)


def flavor_of(version):
    """由 VERSION() 判断产品线：mariadb / mysql。"""
    return "mariadb" if "mariadb" in str(version or "").lower() else "mysql"


def major_of(version):
    head = str(version or "").strip().split(".")[0]
    return int(head) if head.isdigit() else 0


def engine_adapter(flavor):
    """源是什么产品线就用什么客户端（官方明确：不要用 mariadb-dump 去导 MySQL）。"""
    if flavor == "mysql":
        return {"dump": "mysqldump", "cli": "mysql", "backup": "xtrabackup"}
    return {"dump": "mariadb-dump", "cli": "mariadb", "backup": "mariabackup"}


_DIALECT_CACHE = {}


def tool_dialect(binary_path):
    """判定 dump 二进制的【参数方言】——只能问二进制，不能看文件名，也不能看源端产品线。

    实测踩坑：Debian 的 mysql-client 只是指向 mariadb-client 的过渡包，
    于是 /usr/bin/mysqldump 实际是 MariaDB 版本，传 --set-gtid-purged 会直接
    "unknown variable"。因此必须以 --help 里真实存在的参数为准：
      有 --set-gtid-purged ⇒ MySQL 方言
      只有 --gtid          ⇒ MariaDB 方言
    """
    path = str(binary_path or "")
    if path in _DIALECT_CACHE:
        return _DIALECT_CACHE[path]
    dialect = None
    try:
        out = subprocess.run([path, "--help"], capture_output=True, text=True,
                             timeout=20).stdout or ""
        if "--set-gtid-purged" in out:
            dialect = "mysql"
        elif "--gtid" in out:
            dialect = "mariadb"
    except Exception:
        pass
    if dialect is None:  # 兜底时才看文件名
        name = os.path.basename(path)
        dialect = "mysql" if ("mysql" in name and "mariadb" not in name) else "mariadb"
    _DIALECT_CACHE[path] = dialect
    return dialect


def resolve_client_tools(flavor):
    """按产品线挑客户端，缺失直接报错（静默降级正是 bug 温床）。

    返回 (dump, cli, dialect)：dialect 是真实二进制的参数方言。
    """
    want = engine_adapter(flavor)
    dump = _which(want["dump"]) or _which("mysqldump") or _which("mariadb-dump")
    cli = _which(want["cli"]) or _which("mysql") or _which("mariadb")
    if not dump or not cli:
        raise RuntimeError(
            "管理端缺少数据库客户端（%s / %s）。请在 Custos 镜像内安装数据库客户端后重试。"
            % (want["dump"], want["cli"]))
    return dump, cli, tool_dialect(dump)


def dump_gtid_args(flavor, want_gtid=True):
    """生成 dump 侧的 GTID 参数 —— 两个产品线的写法完全不同，绝不可混。"""
    if flavor == "mysql":
        # 目标若是 MariaDB，则绝不能输出 GTID_PURGED 行（会 1193）
        return ["--set-gtid-purged=OFF"]
    # MariaDB：--gtid 需配合 --master-data=1 才会输出 GTID 位点
    return ["--gtid", "--master-data=1"] if want_gtid else []


def ssl_args(dialect):
    """自签名证书场景：跳过证书校验。

    MySQL 5.7 会自动生成自签名证书，而新版 MariaDB 客户端【默认校验】服务器证书，
    连接时直接报 TLS/SSL error 2026 "Certificate verification failure"。
    迁移是在内网可信环境搬运数据，跳过校验既可用又不会带来额外风险。
    """
    return ["--ssl-verify-server-cert=0"] if dialect == "mariadb" else []


def capabilities(src_flavor, src_major, tgt_flavor, tgt_major=None):
    """能力矩阵 —— 全部通道门禁的唯一真源。

    表达方式刻意用「能力标志」而非「品牌分支」：MySQL 5.7 因为没有那两个必炸项
    （caching_sha2 / utf8mb4_0900_ai_ci），天然与 MariaDB 落进同一个「宽松桶」，
    这就解释了为何「只有 5.7 能跟 MariaDB 混迁」——无需硬编码例外。
    """
    same_engine = (src_flavor == tgt_flavor)
    cross_engine = not same_engine
    return {
        "same_engine": same_engine,
        "logical": True,
        # 物理通道跨产品线不可行：MySQL 8 起删 .frm 改全局数据字典，MariaDB 读不懂
        "physical": same_engine,
        # 桥接限定 MariaDB→MariaDB（我们的复制语句是 MariaDB GTID 语义）
        "bridge": same_engine and tgt_flavor == "mariadb" and src_flavor == "mariadb",
        # 同引擎可追平；跨引擎只能停写拷贝
        "pipeline": "continuous" if (same_engine and tgt_flavor == "mariadb") else "stopcopy",
        "cross_engine": cross_engine,
        # MySQL 8.0+ → MariaDB：按用户决策暂时放弃（需先做排序规则/认证插件换算）
        "refused": cross_engine and src_flavor == "mysql" and src_major >= 8,
        "flags": {
            "native_auth_only": src_flavor == "mariadb" or src_major < 8,
        },
    }


# ---------------------------------------------------------------------------
# 步骤实现
# ---------------------------------------------------------------------------
def _sql_lit(s):
    """SQL 字符串字面量转义（账号名 / 口令可能含引号、反斜杠）。

    注意：不可命名为 _q —— precheck 内部已有同名局部函数 _q(conn, sql, args)。
    """
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _ensure_source_repl_account(task):
    """在【源】MariaDB 上创建最小权限复制账号（迁移完 cleanup 可回收）。

    降级策略：若源连接账号无 CREATE USER 权限（桥接会报 1227），则不再建专用账号，
    改为**直接用源连接账号本身**作为复制账号（前提是源账号允许从目标主机连入，
    通常 @'%' 即可）。返回 used_src_creds 供 bridge 选择 CHANGE MASTER 的账号。
    """
    cfg = task["config"]
    conn = connect(cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"], timeout=8)
    try:
        cur = conn.cursor()
        u, p = _sql_lit(cfg["repl_user"]), _sql_lit(cfg["repl_pass"])
        try:
            # ⚠ 关键坑（实测坐实）：CREATE USER IF NOT EXISTS 在用户【已存在】时
            #   **不会更新口令**。迁移反复重装后，源库残留旧的 replication 账号（旧口令），
            #   而新任务生成了新口令 ⇒ 该语句静默跳过、不报错、也不触发下面的降级分支，
            #   于是目标拿着新口令连源 → "Access denied ... (using password: YES)"、
            #   IO 线程永远起不来（追平检测报「IO 线程未运行」）。
            #   故必须显式 ALTER USER 强制同步口令，保证幂等。
            cur.execute(f"CREATE USER IF NOT EXISTS '{u}'@'%'")
            cur.execute(f"ALTER USER '{u}'@'%' IDENTIFIED BY '{p}'")
            cur.execute(f"GRANT REPLICATION SLAVE ON *.* TO '{u}'@'%'")
            cur.execute("FLUSH PRIVILEGES")
            return {"used_src_creds": False}
        except Exception as e:
            err = str(e)
            # 1227 Access denied / 缺少 CREATE USER：源账号无权建账号 → 降级复用源账号本身
            if "1227" in err or "Access denied" in err or "CREATE USER" in err:
                cur.execute("SELECT CURRENT_USER()")
                who = cur.fetchone()[0]
                return {"used_src_creds": True,
                        "note": (f"源连接账号 {who} 无 CREATE USER 权限，已改用源账号本身作为复制账号；"
                                 f"请确保该账号允许从目标 {cfg['tgt_host']} 连入（如改为 @'%'）")}
            raise RuntimeError(f"在源库创建复制账号失败: {err}")
    finally:
        conn.close()


def _pw_arg(pw):
    """空口令时必须整个省略 -p，否则传 'p' 会变成交互等待输入，进程直接挂死。"""
    return [f"-p{pw}"] if pw else []


def _ensure_target_accounts(cfg, stage=""):
    """seed / 桥接后【立刻】补齐目标节点上的 Custos 账号（幂等，失败不阻断）。

    根因（v2.4.0 追平测试实测，203/213 集群）：物理通道 copy-back 会用【源库数据目录】
    整库覆盖目标 datadir，`mysql.user` 一并被替换 ⇒ 目标上原有的 ha_monitor /
    replication / app 账号**全部消失**；而旧逻辑只在 cutover 才 `_align_target_creds`
    补账号 ⇒ 从 seed 到 cutover 的整个桥接/追平窗口内，目标处于「无 Custos 账号」状态：

      · 同集群第二节点（213）连新主（203）报 1045
        ``Access denied for user 'replication'@'192.168.1.213'``（账号根本不存在）；
      · 管理器自身连不上目标（ha_monitor 不存在）⇒ 仪表盘复制/IO 全是 “—”；
      · 又因「迁移锁定」让 ensure_topology 整轮跳过（见 failover），这段窗口内
        没有任何机制能把它修回来，只能等 cutover。

    对策：seed 完成（目标实例就绪）后立刻按目标所属集群的凭证重建账号，并在 bridge()
    开头再兜底一次。目标尚未登记进任何集群时无可对齐的凭证 → 跳过（由 cutover 统一处理）。
    """
    try:
        rows = query("SELECT cluster_id FROM nodes WHERE ip=%s AND port=%s",
                     (cfg["tgt_host"], int(cfg["tgt_port"])))
        cid = (rows[0].get("cluster_id") if rows else None)
        if not cid:
            return {"ok": True, "skipped": "目标尚未登记进 Custos 集群，留待 cutover 对齐凭证"}
        cc = query("SELECT ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass "
                   "FROM clusters WHERE id=%s", (cid,))
        if not cc:
            return {"ok": True, "skipped": "目标所属集群凭证缺失"}
        c = cc[0]
        mip = get_manager_public_ip()
        msub = get_manager_docker_subnet_wildcard()
        ensure_ha_creds(cfg["tgt_host"], int(cfg["tgt_port"]),
                        _tgt_user(cfg), cfg["tgt_root_pass"],
                        c["ha_user"], c["ha_pass"], c["repl_user"], c["repl_pass"],
                        c["app_user"], c["app_pass"], mip, msub)
        return {"ok": True, "cluster_id": cid, "stage": stage,
                "note": "已按集群 #%s 凭证重建目标 Custos 账号" % cid}
    except Exception as e:
        # 不阻断迁移主线：账号可由 cutover / monitor 再次自愈，但必须如实回传原因
        return {"ok": False, "stage": stage, "error": str(e)}


def _snapshot_src_gtid(cfg):
    """在同引擎可桥接时，先取源 GreatTID 位点。

    刻意【不用】 --master-data：它隐含 FLUSH TABLES WITH READ LOCK 且需 SUPER/RELOAD，
    与 --single-transaction/--skip-lock-tables 语义打架。直接读变量更安全、无锁。
    """
    try:
        conn = connect(cfg["src_host"], int(cfg["src_port"]), cfg["src_user"],
                       cfg["src_pass"], timeout=8)
        try:
            cur = conn.cursor()
            cur.execute("SELECT @@gtid_binlog_pos")
            row = cur.fetchone()
            return (row[0] or "") if row else ""
        finally:
            conn.close()
    except Exception:
        return ""


# 数据种子的卡死判定阈值（秒）：超过这么久没有任何字节推进即判定假死并主动中止。
# 为什么必须有兜底：线程一旦永久阻塞就再也不会退出，_RUNNING 也就永不释放，
# 并发门禁会把后续【所有】迁移任务全部挡死（只能重启管理器才能解开）。
SEED_STALL_SEC = 300

# 用户主动中止的任务 id 集合（前端「中止任务」写入，步骤循环在安全点检测并退出）
_CANCEL = set()


def seed_logical(task):
    """逻辑通道：仅用户库导出 → 流式导入目标（排除系统库 / Custos 库）。

    工具与 GTID 参数按源端产品线分流（详见 engine_adapter / dump_gtid_args 注释）。
    支持真实字节级进度：以探测阶段得到的数据量为分母统计已传输字节。
    """
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"])
    databases = [d for d in (cfg.get("databases") or []) if d and not d.startswith("#")]
    if not databases:
        raise ValueError("没有可迁移的用户库（仅系统库，无需迁移）")

    src_flavor = cfg.get("src_flavor") or "mariadb"
    tgt_flavor = cfg.get("tgt_flavor") or "mariadb"
    cap = capabilities(src_flavor, int(cfg.get("src_major") or 0), tgt_flavor)

    dump, cli, dialect = resolve_client_tools(src_flavor)
    _tick(task, 2, stage="准备导出（工具=" + os.path.basename(dump) + "）",
          log="开始逻辑迁移：%d 个库，工具=%s" % (len(databases), os.path.basename(dump)))

    # ⚠ 绝不能把 GTID 位点写进 dump。
    #   实测：mariadb-dump --gtid --master-data=1 会输出 SET GLOBAL gtid_slave_pos='…'，
    #   而当源端此刻尚无 GTID 位点时该值为空，导入目标会触发
    #   ERROR 1948（与目标已有 binlog GTID 冲突）。
    #   正确做法：位点单独采样，留给 bridge 阶段显式设置，不进数据流。
    src_gtid = _snapshot_src_gtid(cfg) if cap["bridge"] else ""
    # 方言跟随真实二进制（见 tool_dialect 注释），避免把 MySQL 参数喂给 mariadb-dump
    gtid_args = dump_gtid_args(dialect, want_gtid=False)

    dump_args = [
        dump, "-h", src[0], "-P", str(src[1]), "-u", src[2],
    ] + _pw_arg(src[3]) + [
        # 官方迁移建议：single-transaction 一致快照、hex-blob 保二进制安全、
        # routines/triggers/events 带存储过程与事件器
        "--single-transaction", "--routines", "--triggers", "--events",
        "--hex-blob", "--default-character-set=utf8mb4",
        "--skip-lock-tables", "--databases",
    ] + databases + gtid_args + ssl_args(dialect)
    # 导入端同样需要跳证书校验（MariaDB 客户端默认校验自签名证书会失败）
    tgt_dialect = tool_dialect(cli)
    load_args = ([cli, "-h", tgt[0], "-P", str(tgt[1]), "-u", tgt[2]]
                 + _pw_arg(tgt[3]) + ssl_args(tgt_dialect))

    total_bytes = float(cfg.get("size_mb") or 0) * 1024 * 1024
    p1 = subprocess.Popen(dump_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p2 = subprocess.Popen(load_args, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 登记子进程：卡死时自愈守护可以直接杀掉它们，让阻塞在管道上的线程脱离
    _STEP_PROCS.setdefault(task.get("id"), []).extend([p1, p2])

    # ⚠ 所有「本进程不主动消费」的管道都必须有线程持续排空，否则写满 64KB 即死锁。
    # 实测就是栽在这里：只排空了 dump 端 stderr，导入端 stdout/stderr 没人读 ⇒
    # 客户端一输出就阻塞 ⇒ 不再读 stdin ⇒ 主进程写 p2.stdin 跟着阻塞 ⇒
    # 界面永远「执行中、进度 0%」的假死（传输量恰好卡在 64KB，即管道缓冲大小）。
    derr_box, lout_box, lerr_box = [], [], []
    t_err  = threading.Thread(target=lambda: derr_box.append(p1.stderr.read()), daemon=True)
    t_lout = threading.Thread(target=lambda: lout_box.append(p2.stdout.read()), daemon=True)
    t_lerr = threading.Thread(target=lambda: lerr_box.append(p2.stderr.read()), daemon=True)
    for _t in (t_err, t_lout, t_lerr):
        _t.start()

    read_bytes = 0
    next_report = 0
    stall = {"msg": None}
    last_ts = [time.time()]     # 最近一次真正推进字节的时刻（供看门狗判定卡死）

    def _watchdog():
        # 卡死兜底：长时间一个字节都没推进 ⇒ 杀掉两端让主循环脱离阻塞并如实报错，
        # 否则线程永远不退出，_RUNNING 永不释放，后续所有迁移都被并发门禁挡死。
        while stall["msg"] is None:
            time.sleep(10)
            if time.time() - last_ts[0] > SEED_STALL_SEC:
                stall["msg"] = ("数据种子超过 %d 秒没有任何字节推进（已传输 %.1f MB），"
                                "判定为卡死并中止" % (SEED_STALL_SEC, read_bytes / 1048576.0))
                for _p in (p1, p2):
                    try:
                        _p.kill()
                    except Exception:
                        pass
                return
    t_wd = threading.Thread(target=_watchdog, daemon=True)
    t_wd.start()

    try:
        while True:
            if task.get("id") in _CANCEL:
                raise RuntimeError("任务已被用户中止")
            chunk = p1.stdout.read(65536)
            if not chunk:
                break
            read_bytes += len(chunk)
            last_ts[0] = time.time()
            # 写入端慢会在此自然阻塞 ⇒ 进度真实反映落地速度，而非纯导出速度
            p2.stdin.write(chunk)
            if total_bytes > 0 and read_bytes >= next_report:
                next_report = read_bytes + 8 * 1024 * 1024
                pct = min(95.0, read_bytes * 100.0 / total_bytes)
                # 保留 1 位小数：整 MB 会让小库长期显示「0 / N MB」，看着像没动
                _tick(task, pct, stage="已传输 %.1f / %.1f MB（%d 个库）"
                                       % (read_bytes / 1048576.0, total_bytes / 1048576.0, len(databases)))
    except BrokenPipeError:
        pass
    finally:
        try:
            p2.stdin.close()
        except Exception:
            pass
        try:
            p1.stdout.close()
        except Exception:
            pass
        # 关键：stdin 关闭后必须置 None，否则 communicate() 会再次 flush 该管道，
        #      抛 "ValueError: flush of closed file"（实测踩到的经典坑）。
        p2.stdin = None

    _, err2 = p2.communicate()
    try:
        p1.wait(timeout=60)
    except Exception:
        pass
    for _t in (t_err, t_lout, t_lerr):
        _t.join(timeout=5)
    derr = derr_box[0] if derr_box else b""
    lerr = lerr_box[0] if lerr_box else b""

    # 卡死中止：把导入端真实输出一并报出，否则用户只看到「进度 0%」根本无从下手
    if stall["msg"]:
        _CANCEL.discard(task.get("id"))
        raise RuntimeError(stall["msg"] + "。导入端输出："
                           + (lerr or b"").decode(errors="replace")[:500])

    if p2.returncode != 0 or p1.returncode != 0:
        detail = (err2 or b"").decode(errors="replace")[:2000] \
            or (lerr or b"").decode(errors="replace")[:2000] \
            or (derr or b"").decode(errors="replace")[:2000]
        raise RuntimeError("逻辑导入失败（退出码 dump=%s load=%s）: %s"
                           % (p1.returncode, p2.returncode, detail))

    # ── 防静默漏表 ────────────────────────────────────────────────────────
    # 导入端（mysql/mariadb 客户端）批量执行时，若某张表的 CREATE TABLE 失败会
    # 【继续往下走、且可能返回退出码 0】，于是部分表没建出来却报成功（实测 33→203
    # 漏了 certd.pi_template / certd.sys_passkey 两张表）。这里强制对照源/目标
    # 表清单；有缺表先单独补救一次，仍失败则硬报错，杜绝「数据没迁全还显示成功」。
    _missing = _reconcile_seed_tables(src, tgt, databases)
    if _missing:
        _imp_err = (lerr or err2 or b"").decode(errors="replace")
        _missing = _retry_missing_tables(task, cfg, src, tgt, databases, _missing)
        if _missing:
            _detail = "; ".join("%s.%s" % (d, t) for d, t in _missing)
            raise RuntimeError(
                "逻辑导入缺表且补救失败（源有、目标无）：%s\n导入端报错：%s"
                % (_detail, _imp_err[:1500]))

    # 导入可能覆盖/重建 mysql 库（全库导入场景）⇒ 目标账号有被冲掉的风险，
    # 导入完成后立刻按集群凭证重建一次（幂等；未登记集群时自动跳过）。
    _acct = _ensure_target_accounts(cfg, stage="seed_logical")
    if not _acct.get("ok"):
        print("[migration] 目标账号重建失败（不阻断）：%s" % _acct.get("error"))
    _tick(task, 100, stage="逻辑导入完成", log="导入完成，共传输 %.1f MB" % (read_bytes / 1048576.0))
    return {"ok": True, "dump_tool": os.path.basename(dump), "databases": databases,
            "accounts": _acct,
            "bytes": read_bytes, "src_gtid": src_gtid, "capabilities": {
                k: cap[k] for k in ("same_engine", "cross_engine", "pipeline", "bridge")}}


def _tbl_set(conn, db):
    """取某库 BASE TABLE 集合（出错返回空集，不阻断对账）。"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (db,))
        return set(x[0] for x in cur.fetchall())
    except Exception:
        return set()


def _reconcile_seed_tables(src, tgt, databases):
    """seed 后对照源/目标 BASE TABLE 清单，返回（库,表）缺表列表（源有、目标无）。"""
    try:
        sc = connect(*src, timeout=8)
        tc = connect(*tgt, timeout=8)
    except Exception as e:
        # ⚠ 不再静默 return []：连接异常曾被吞成「无缺失」，导致数据没迁全却判定成功。
        raise RuntimeError("缺表对账无法连接源/目标库，无法确认数据完整性: %s" % e)
    missing = []
    try:
        for db in databases:
            s = _tbl_set(sc, db); t = _tbl_set(tc, db)
            for tbl in sorted(s - t):
                missing.append((db, tbl))
    finally:
        for c in (sc, tc):
            try: c.close()
            except Exception: pass
    return missing


def _retry_missing_tables(task, cfg, src, tgt, databases, missing):
    """对仍缺失的表单独重新导出+导入一次（绕开首次批量流里被吞掉的单表错误）。
    返回补救后依旧缺失的（库,表）列表。若根因是 DDL 不兼容（如 DATA DIRECTORY/
    TABLESPACE 路径在目标不存在），补救同样会失败，此时由上层抛出并附带导入端报错。"""
    by_db = {}
    for db, tbl in missing:
        by_db.setdefault(db, []).append(tbl)
    src_flavor = cfg.get("src_flavor") or "mariadb"
    try:
        dump, cli, dialect = resolve_client_tools(src_flavor)
    except Exception as e:
        print("[migration] 缺表补救工具解析失败: %s" % e)
        return missing
    tgt_dialect = tool_dialect(cli)
    for db, tbls in by_db.items():
        try:
            dump_args = ([dump, "-h", src[0], "-P", str(src[1]), "-u", src[2]]
                         + _pw_arg(src[3]) + [
                             "--single-transaction", "--hex-blob",
                             "--default-character-set=utf8mb4", "--skip-lock-tables",
                             "--databases", db, "--tables"] + tbls
                         + dump_gtid_args(dialect, want_gtid=False) + ssl_args(dialect))
            load_args = ([cli, "-h", tgt[0], "-P", str(tgt[1]), "-u", tgt[2]]
                         + _pw_arg(tgt[3]) + ssl_args(tgt_dialect))
            p1 = subprocess.Popen(dump_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            p2 = subprocess.Popen(load_args, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            data = p1.stdout.read()
            p1.stdout.close()
            try: p1.wait(timeout=300)
            except Exception: pass
            try:
                p2.stdin.write(data); p2.stdin.close()
            except Exception as e:
                print("[migration] 缺表补救写入失败 %s: %s" % (db, e))
            try: p2.wait(timeout=300)
            except Exception: pass
            if p1.returncode != 0:
                print("[migration] 缺表补救导出失败 %s.%s: %s"
                      % (db, ",".join(tbls), (p1.stderr.read() or b"").decode(errors="replace")[:300]))
            if p2.returncode != 0:
                print("[migration] 缺表补救导入失败 %s.%s: %s"
                      % (db, ",".join(tbls), (p2.stderr.read() or b"").decode(errors="replace")[:300]))
        except Exception as e:
            print("[migration] 缺表补救异常 %s: %s" % (db, e))
    return _reconcile_seed_tables(src, tgt, databases)


# ---------------------------------------------------------------------------
# 迁移前兼容性预检（dry-run）—— 把成功率从「赌一把」变成「先验 + 后置校验」
#
# 两层检查：
#   ① 元数据层：把源端用到的排序规则 / 存储引擎 / JSON / 生成列，与目标端实际支持的
#      列表对比（SHOW COLLATION / SHOW ENGINES），提前揪出 Unknown collation 之类必炸项；
#   ② 结构试运行：--no-data 只导结构，改名落到目标端 scratch schema 真实导入再删除，
#      这一步能抓到元数据层看不出来的 DDL 失败（含 MySQL 5.7 的 WITH PARSER ngram 等）。
# ---------------------------------------------------------------------------
_PRECHECK_PREFIX = "prechk_"


def precheck(task_id):
    task = get_task(task_id)
    if not task:
        return {"ok": False, "error": "任务不存在"}
    cfg = task["config"]
    cap = _cap_of(task)
    if cap.get("refused"):
        return {"ok": False, "blocked": True,
                "error": "该组合暂不支持：MySQL 8.0+ → MariaDB 需先做排序规则与认证插件换算。"}

    databases = [d for d in (cfg.get("databases") or []) if d and not d.startswith("#")]
    findings = []
    src_flavor = cfg.get("src_flavor") or "mariadb"
    dump, cli, dialect = resolve_client_tools(src_flavor)
    ph = ",".join(["%s"] * len(databases)) if databases else "NULL"

    def _q(conn, sql, args=()):
        cur = conn.cursor()
        cur.execute(sql, tuple(args))
        return cur.fetchall()

    src = connect(cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"], timeout=8)
    tgt = connect(cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"], timeout=8)
    try:
        # ① 源账号建复制账号权限预检（桥接会在源建 repl@'%'，需 CREATE USER + GRANT OPTION）
        try:
            cur = src.cursor()
            cur.execute("SELECT CURRENT_USER()")
            who = cur.fetchone()[0]
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            grants = "\n".join(r[0] for r in cur.fetchall())
            can_create = (("CREATE USER" in grants)
                          or ("GRANT OPTION" in grants and "*.*" in grants)
                          or ("ALL PRIVILEGES" in grants and "*.*" in grants))
            if not can_create:
                findings.append({
                    "level": "blocker", "kind": "src_create_user",
                    "detail": (f"源连接账号 {who} 缺少 CREATE USER 权限，桥接步骤无法在源库创建复制账号"
                               f"（将报 1227）。请在源库执行："
                               f"GRANT CREATE USER, REPLICATION SLAVE, REPLICATION CLIENT ON *.* "
                               f"TO CURRENT_USER() WITH GRANT OPTION; FLUSH PRIVILEGES;"),
                    "sample": [],
                })
        except Exception:
            # 无法判定（旧版本/权限受限）不阻断，留给桥接步骤实测
            pass
        tgt_coll = {r[0] for r in _q(tgt, "SHOW COLLATION")}
        tgt_eng = {(r[0] or "").upper() for r in _q(tgt, "SHOW ENGINES")
                   if (str(r[1] or "")).upper() in ("YES", "DEFAULT")}
        # ① 排序规则缺口
        miss_coll = {}
        for db_, tb_, col_, _cs, col in _q(
                src, f"SELECT table_schema,table_name,column_name,character_set_name,collation_name "
                     f"FROM information_schema.columns WHERE table_schema IN ({ph}) "
                     f"AND collation_name IS NOT NULL", databases):
            if col and col not in tgt_coll:
                miss_coll.setdefault(col, []).append(f"{db_}.{tb_}.{col_}")
        for c, hits in miss_coll.items():
            findings.append({"level": "blocker", "kind": "collation",
                             "detail": f"目标端缺少排序规则 {c}（{len(hits)} 列）",
                             "sample": hits[:5]})
        # ① 存储引擎缺口
        miss_eng = {}
        for db_, tb_, eng in _q(
                src, f"SELECT table_schema,table_name,engine FROM information_schema.tables "
                     f"WHERE table_schema IN ({ph})", databases):
            if eng and str(eng).upper() not in tgt_eng:
                miss_eng.setdefault(str(eng).upper(), []).append(f"{db_}.{tb_}")
        for e, hits in miss_eng.items():
            findings.append({"level": "blocker", "kind": "engine",
                             "detail": f"目标端缺少存储引擎 {e}（{len(hits)} 表）", "sample": hits[:5]})
        # ① JSON / 生成列（提示级）
        n_json = len(_q(src, f"SELECT 1 FROM information_schema.columns "
                             f"WHERE table_schema IN ({ph}) AND LOWER(data_type)='json'", databases))
        if n_json:
            findings.append({"level": "warn", "kind": "json",
                             "detail": f"存在 {n_json} 个 JSON 列：MySQL 为二进制类型，"
                                       f"MariaDB 实为 LONGTEXT+CHECK，建议做应用层回归"})
        # ② 全文解析器（MySQL 5.7 的 ngram 在 MariaDB 没有对应实现）
        ft = _q(src, f"SELECT DISTINCT table_schema,table_name FROM information_schema.statistics "
                     f"WHERE table_schema IN ({ph}) AND index_type='FULLTEXT'", databases)
        ngram_hits = []
        for db_, tb_ in ft:
            row = _q(src, "SHOW CREATE TABLE `%s`.`%s`" % (db_, tb_))
            if row and "WITH PARSER" in str(row[0][1]).upper():
                ngram_hits.append(f"{db_}.{tb_}")
        if ngram_hits:
            findings.append({"level": "blocker", "kind": "fulltext_parser",
                             "detail": "使用了 MySQL 特有的全文解析器（WITH PARSER），MariaDB 无对应实现",
                             "sample": ngram_hits[:5]})
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            tgt.close()
        except Exception:
            pass

    # ② 结构试运行：只导结构，改名落到目标 scratch schema，真实执行一遍再删除
    trial = {"attempted": 0, "failed": [], "errors": []}
    if databases:
        dump_args = [dump, "-h", cfg["src_host"], "-P", str(int(cfg["src_port"])),
                     "-u", cfg["src_user"]] + _pw_arg(cfg["src_pass"]) + [
            "--no-data", "--routines", "--triggers", "--events",
            "--skip-lock-tables", "--databases",
        ] + databases + dump_gtid_args(dialect, want_gtid=False) + ssl_args(dialect)
        load_args = ([cli, "-h", cfg["tgt_host"], "-P", str(int(cfg["tgt_port"])), "-u", "root"]
                     + _pw_arg(cfg["tgt_root_pass"]) + ssl_args(tool_dialect(cli)))
        try:
            p1 = subprocess.Popen(dump_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            p2 = subprocess.Popen(load_args, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            data = p1.stdout.read()
            p1.stdout.close()
            p1.wait(timeout=300)
            if p1.returncode != 0:
                raise RuntimeError("结构导出失败: " + (p1.stderr.read() or b"").decode(errors="replace")[:800])
            # 把 `db` 改写成 `prechk_db`，避免污染真实库名
            for d in databases:
                data = data.replace(b"`%s`" % d.encode(), b"`%s%s`" % (_PRECHECK_PREFIX.encode(), d.encode()))
            try:
                p2.stdin.write(data)
                p2.stdin.close()
            except BrokenPipeError:
                pass
            # 同 seed_logical：关闭后置 None，避免 communicate() 二次 flush 抛 ValueError
            p2.stdin = None
            _, e2 = p2.communicate(timeout=300)
            trial["attempted"] = len(databases)
            if p2.returncode != 0:
                trial["errors"].append((e2 or b"").decode(errors="replace")[:1500])
        except Exception as e:
            trial["errors"].append(f"{type(e).__name__}: {e}")
        finally:
            # 无论成败都清理 scratch schema
            try:
                c2 = connect(cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"], timeout=8)
                try:
                    cur = c2.cursor()
                    for d in databases:
                        cur.execute(f"DROP DATABASE IF EXISTS `{_PRECHECK_PREFIX}{d}`")
                finally:
                    c2.close()
            except Exception:
                pass
    if trial["errors"]:
        findings.append({"level": "blocker", "kind": "structural_trial",
                         "detail": "结构试运行导入失败（目标无法接受该 DDL）",
                         "sample": trial["errors"][:2]})

    blockers = [f for f in findings if f["level"] == "blocker"]
    return {"ok": not blockers, "blockers": len(blockers),
            "warnings": len([f for f in findings if f["level"] == "warn"]),
            "findings": findings, "trial": trial,
            "capabilities": {k: cap[k] for k in ("same_engine", "cross_engine", "pipeline",
                                                 "bridge", "physical")}}


def _tgt_user(cfg):
    """目标库账号：默认 root。

    旧任务配置里没有 tgt_user 字段（历史上目标端写死 root），这里 or "root" 保持兼容；
    新任务由向导「目标账号」写入。注意：目标端若填非 root，桥接/注册节点/切换/收尾等
    步骤需要 SUPER/CREATE USER/REPLICATION SLAVE 等全局权限，否则会在对应步骤失败
    （见 audit_target_privileges 的分层提示）。
    """
    return cfg.get("tgt_user") or "root"


def _tgt_datadir(cfg):
    """物理通道 B 的目标数据目录：优先用向导显式传入的 tgt_datadir，
    否则按 node_type 取 Custos 默认（与 node_gen 数据目录约定同源）：
    裸机 mariadb → /data/mariadb；容器 mariadb → /data/custos-ha/mariadb12；
    MySQL 系列 → /data/custos-ha/mysql8。"""
    if cfg.get("tgt_datadir"):
        return cfg["tgt_datadir"].rstrip("/")
    nt = cfg.get("node_type", "mariadb-docker")
    if nt == "mariadb":
        return "/data/mariadb"
    if nt == "mariadb-docker":
        return node_gen.MARIADB_DOCKER_DATA_DIR
    return node_gen.MYSQL_DOCKER_DATA_DIR


# ---------------------------------------------------------------------------
# 物理通道 B 的 mariabackup 编排
#
# 根因（务必遵守）：mariabackup 只能读取【本地】数据目录，无法备份远程服务器，
# 所以「备份」必须在【源节点】执行、「恢复(copy-back)」必须在【目标节点】执行。
# 管理器/宿主机一律不预装 mariabackup（既装不上匹配版本、也读不到远端 datadir），
# 管理器只需 openssh-client + rsync + sshpass 做 SSH 编排与文件传输。
#
# 容器节点：经 SSH 到宿主机后 docker exec / docker compose run 进容器执行
# （官方 mariadb 镜像自带 mariabackup）；裸机节点：SSH 直连宿主机执行。
#
# 暂存目录放置约定（务必遵守 —— 物理通道反复失败的根因即在此）：
#   · 源节点：容器节点把暂存放在「数据目录挂载点之内」，保证对容器内 mariabackup 可见
#     （容器只挂了数据目录，如 /data/custos-ha/mariadb12 → 容器内 /var/lib/mysql），
#     docker exec 才能读写到该 target-dir。
#   · 目标节点：**必须放在数据目录【之外】的兄弟路径**。mariabackup --copy-back 明确要求
#     --datadir 为空，若暂存目录位于 datadir 之内，datadir 必然非空，copy-back 直接失败
#     「Original data directory /var/lib/mysql is not empty!」并拒绝写入，结果就是目标
#     数据目录被清空、容器起不来（已实测坐实）。故目标暂存一律用 datadir 的兄弟目录。
# ---------------------------------------------------------------------------

def _tail(s, n=2000):
    return (s or "")[-n:]


def _sh_quote(a):
    return "'" + str(a).replace("'", "'\\''") + "'"


def _default_src_datadir(snt):
    if str(snt).endswith("-docker"):
        return "/data/custos-ha/mariadb12"
    if str(snt).startswith("mariadb"):
        return "/data/mariadb"
    return "/var/lib/mysql"


def _mig_staging_path(node_type, data_dir_host, task_id):
    """源节点暂存目录（宿主机路径）。容器节点放在数据目录挂载点之内（见上方说明）。"""
    if str(node_type).endswith("-docker"):
        return os.path.join(data_dir_host.rstrip("/"), ".custos_mig_staging_%s" % task_id)
    return "/data/custos_mig_staging_%s" % task_id


def _mig_staging_path_tgt(node_type, data_dir_host, task_id):
    """目标节点暂存目录（宿主机路径）——**必须落在数据目录之外**。

    根因（2026-09-20 实测坐实，切勿改回 datadir 之内）：
      mariabackup --copy-back 的第一步就是校验目标 --datadir 为空，否则直接：
        [00] Original data directory /var/lib/mysql is not empty!
      此前目标暂存被放在 <datadir>/.custos_mig_staging_<id>，copy-back 前把暂存「搬回」
      新建的空 datadir，datadir 立刻非空 → copy-back 拒绝写入且 set -e 中断，
      目标 datadir 停留在空目录、容器被 stop，节点直接不可用（这就是物理通道反复失败的根因）。
      改为 datadir 的兄弟目录后，datadir 在 copy-back 时真正为空，实测 COPYBACK_RC=0。
    容器/裸机均适用：该路径只是宿主机上的一个临时目录，copy-back 时以 -v 挂到 /backup。
    """
    return "%s_mig_staging_%s" % (data_dir_host.rstrip("/"), task_id)


def _container_staging(staging_host):
    """宿主机暂存路径 → 容器内路径（数据目录在容器内固定为 /var/lib/mysql）。

    只取暂存目录【最后一段】(.custos_mig_staging_xxx) 拼到 /var/lib/mysql 下，**不依赖**
    「data_dir_host 是 staging_host 前缀」的脆弱字符串切片——此前挂载源路径与暂存路径
    不完全对齐时会拼出 /var/lib/mysqlmariadb/db/data/... 这类乱码路径
    （mariabackup 报 Can't change dir to ...）。只要 src_dd 是 /var/lib/mysql 的真实
    挂载源，basename 映射就恒定成立：<挂载源>/<basename> ↔ 容器内 /var/lib/mysql/<basename>。
    """
    base = os.path.basename(staging_host.rstrip("/"))
    return "/var/lib/mysql/" + base


def _docker_datadir_host_mount(cfg, host, cid, use_src_cred, node_type):
    """探测容器 /var/lib/mysql 在宿主机的真实挂载源路径（bind 或 named volume 均可）。

    docker 节点的「宿主机数据目录」不能写死（1panel 等安装的数据目录并不在
    /data/custos-ha/mariadb12）——必须按实际挂载。rsync 在宿主机层拉取备份，
    只能访问真实挂载源。返回宿主机绝对路径或 None。
    """
    if not cid:
        return None
    sh = _ssh_run(cfg, host,
                  "docker inspect -f '{{range .Mounts}}{{if eq .Destination \"/var/lib/mysql\"}}"
                  "{{.Source}}{{end}}{{end}}' %s 2>/dev/null" % cid,
                  use_src_cred, node_type)
    if sh.returncode == 0:
        p = sh.stdout.strip()
        if p:
            return p
    return None


def _docker_image(cfg, host, cid, use_src_cred, node_type):
    """取 MariaDB 容器的镜像名（copy-back 用 --volumes-from 时复用其镜像）。"""
    sh = _ssh_run(cfg, host, "docker inspect -f '{{.Config.Image}}' %s 2>/dev/null" % cid,
                  use_src_cred, node_type)
    if sh.returncode == 0:
        p = sh.stdout.strip()
        if p:
            return p
    return "mariadb"


# agent-exec 的 HTTP 等待上限（与 SSH 同量级；mariabackup 可能跑很久）
AGENT_EXEC_TIMEOUT = 3600 * 6


def _ssh_creds(cfg, use_src_cred):
    """返回 (user, pwd)。源端用 src_ssh_*；目标端用 tgt_ssh_*（默认 root）。

    根因：目标端原本写死 root + 默认密钥，但管理器从不为节点装公钥、节点 compose 也无
    sshd 之外的 SSH 通道，导致目标 SSH 必失败 → 被静默吞成「没容器」。现统一走账号/密码
    （sshpass），与源端对称。
    """
    if use_src_cred:
        return cfg.get("src_ssh_user") or "root", cfg.get("src_ssh_pass") or ""
    return cfg.get("tgt_ssh_user") or "root", cfg.get("tgt_ssh_pass") or ""


def _ssh_port(cfg, use_src_cred):
    """源端用 src_ssh_port，目标端用 tgt_ssh_port，默认 22（生产环境 SSH 端口未必是 22）。"""
    if use_src_cred:
        return int(cfg.get("src_ssh_port") or 22)
    return int(cfg.get("tgt_ssh_port") or 22)


def _ssh_connect_failed(cp):
    """SSH 是否「连不上 / 认证失败」（而非命令本身执行失败）。用于决定是否退回 agent-exec。"""
    if cp is None or cp.returncode == 0:
        return False
    err = (getattr(cp, "stderr", "") or "") + (getattr(cp, "stdout", "") or "")
    keys = ("Permission denied", "Connection refused", "Connection timed out",
            "No route to host", "Connection reset", "Name or service not known",
            "Host key verification failed", "ssh: connect", "timed out", "Timeout")
    low = err.lower()
    return any(k.lower() in low for k in keys)


def _agent_exec(host, script):
    """经节点 agent 的 /exec 端点在【宿主】执行命令（容器节点经 docker.sock 跑 docker）。

    返回 subprocess.CompletedProcess，或 None（agent 不可达 / 无 /exec / 未授权 / 异常）。
    仅作为 SSH 连不上时的兜底；裸机节点因 agent 容器无法写宿主文件系统，仍应走 SSH。
    """
    if not NODE_AGENT_PORT:
        return None
    try:
        url = "http://%s:%s/exec" % (host, NODE_AGENT_PORT)
        data = json.dumps({"script": script}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if NODE_AGENT_TOKEN:
            headers["Authorization"] = "Bearer %s" % NODE_AGENT_TOKEN
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=AGENT_EXEC_TIMEOUT) as resp:
            obj = json.loads(resp.read().decode("utf-8"))
        if not obj.get("ok"):
            return None
        return subprocess.CompletedProcess(
            args=script, returncode=int(obj.get("rc", 1)),
            stdout=obj.get("stdout", "") or "", stderr=obj.get("stderr", "") or "")
    except Exception:
        return None


def _ssh_opt_string(cfg, host, use_src_cred):
    """构造 rsync 的 ssh 传输串（仅 ssh 选项与可选密码，不内含 host）。

    源/目标的 user@host 由调用方拼到 rsync 的 host:path 参数里，避免 -e 与 host:path
    双重带 host 导致 rsync 把 host 当作远端要执行的命令（远端 zsh 报
    "command not found: <host>"）。
    """
    user, pwd = _ssh_creds(cfg, use_src_cred)
    ssh = "ssh -p %d -o StrictHostKeyChecking=no -o ConnectTimeout=15" % _ssh_port(cfg, use_src_cred)
    if pwd:
        return "sshpass -p %s rsync -az --delete -e %s" % (_sh_quote(pwd), _sh_quote(ssh))
    return "rsync -az --delete -e %s" % _sh_quote(ssh)


def _rsync_pull(cfg, host, remote_path, local_path):
    os.makedirs(local_path, exist_ok=True)
    user, _ = _ssh_creds(cfg, True)
    cmd = "%s %s@%s:%s %s/" % (_ssh_opt_string(cfg, host, True),
                                user, host, _sh_quote(remote_path), _sh_quote(local_path))
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600)


def _rsync_push(cfg, local_path, host, remote_path):
    user, _ = _ssh_creds(cfg, False)
    cmd = "%s %s/ %s@%s:%s" % (_ssh_opt_string(cfg, host, False),
                                _sh_quote(local_path), user, host, _sh_quote(remote_path))
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600)


def _ssh_run(cfg, host, script, use_src_cred, node_type=None):
    """在节点上执行 shell 脚本：先试 SSH（源/目标各自账号密码），连不上则退回 agent-exec（容器节点）。

    node_type 用于判断是否为容器节点（agent-exec 仅对容器节点可靠）。返回 subprocess.CompletedProcess。
    """
    user, pwd = _ssh_creds(cfg, use_src_cred)
    base = ["ssh", "-p", str(_ssh_port(cfg, use_src_cred)), "-o", "StrictHostKeyChecking=no",
            "-o", "BatchMode=no", "-o", "ConnectTimeout=15", "%s@%s" % (user, host)]
    cp = None
    try:
        if pwd:
            cp = subprocess.run(["sshpass", "-p", pwd] + base + [script],
                                capture_output=True, text=True, timeout=3600 * 6)
        else:
            cp = subprocess.run(base + [script], capture_output=True, text=True, timeout=3600 * 6)
    except Exception:
        cp = None
    # SSH 连不上 / 认证失败 → 退回 agent-exec（仅容器节点可靠）
    if (cp is None or _ssh_connect_failed(cp)) and node_type and str(node_type).endswith("-docker"):
        ap = _agent_exec(host, script)
        if ap is not None:
            return ap
    if cp is None:
        raise RuntimeError("无法 SSH 到 %s（命令执行异常），且 agent-exec 不可用。%s"
                           % (host, _ssh_help(use_src_cred)))
    return cp


def _find_mariadb_container(host, cfg, use_src_cred, node_type=None):
    """在宿主机定位运行中的 MariaDB 容器，返回 (cid, diag)。

    diag 取值：'ok'（找到）/'ssh_fail'（SSH 不可达）/'docker_missing'（docker 不可用）
              /'no_container'（确实无匹配容器）。
    关键：把「SSH 失败 / docker 缺失」与「容器不存在」区分开，避免把 SSH 不通误报成「没容器」
    （实测 33 节点：MySQL 连通性预检通过 ≠ SSH 能跑 docker ps，二者是两套凭证/通道）。
    匹配：优先 compose 服务 mariadb；否则按 容器名/镜像 含 'mariadb' 选取（不依赖发布端口/精确名/compose 路径）。
    """
    sh = _ssh_run(cfg, host,
                  "command -v docker >/dev/null 2>&1 && echo DOCKER_OK || echo DOCKER_NO; "
                  "cd /data/custos-ha 2>/dev/null && docker compose ps -q mariadb 2>/dev/null; "
                  "docker ps --filter status=running --format '{{.ID}}|{{.Names}}|{{.Image}}'",
                  use_src_cred, node_type)
    if sh.returncode != 0:
        return "", "ssh_fail"
    out = sh.stdout
    nl = out.find("\n")
    marker = out[:nl].strip() if nl != -1 else out.strip()
    if marker != "DOCKER_OK":
        return "", "docker_missing"
    rest = out[nl + 1:] if nl != -1 else ""
    compose_id = ""
    for line in rest.splitlines():
        line = line.strip()
        if not line:
            continue
        if "|" not in line:
            if not compose_id:
                compose_id = line          # compose ps 输出：纯容器 id（无 '|'）
            continue
        cid, names, image = (line.split("|") + ["", ""])[:3]
        if "mariadb" in (names + " " + image).lower():
            return cid, "ok"
    if compose_id:
        return compose_id, "ok"
    return "", "no_container"


def _resolve_mb(host, cfg, use_src_cred, node_type=None):
    """解析裸机节点上的 mariabackup 路径。

    裸机靠 tarball 装在 /opt/mariadb/bin，常不在 PATH；容器节点在镜像 PATH 内。
    找不到时回退 'mariabackup'（缺失由 _check_node_mb 前置探测拦截，这里只保证路径解析）。
    """
    sh = _ssh_run(cfg, host,
                  "MB=$(command -v /opt/mariadb/bin/mariabackup 2>/dev/null); "
                  "[ -z \"$MB\" ] && MB=$(command -v mariabackup 2>/dev/null); "
                  "printf '%s' \"$MB\"", use_src_cred, node_type)
    if sh.returncode == 0:
        p = sh.stdout.strip()
        if p:
            return p
    return "mariabackup"


# ---------------------------------------------------------------------------
# 裸机节点的 datadir / systemd 单元 / my.cnf —— 多候选 + 保底，绝不猜单一名字或单一位置
#
# 根因（2026-09-20 实测坐实，203 节点）：
#   Install-ha-mariadb-Node.sh 把权威值写进 /etc/ha-mysql-node/install-state-mariadb：
#       DATA_DIR=/data/custos-ha/mariadb      SYSTEMD_UNIT=mariadb-ha.service
#   而迁移代码此前把裸机 datadir 猜成 /data/mariadb、systemd 单元猜成 mariadb.service，
#   于是 copy-back 停服一步直接失败：「Failed to stop mariadb.service: Unit mariadb.service not loaded.」
#
# 但【Custos 安装只是其中一种形态】：用户也可能是标准包管理安装（/etc/my.cnf + /var/lib/mysql +
# mysql.service / mysqld.service / mariadb.service）、tarball 自编译（/opt/mariadb + /data/... +
# 自定义单元名），容器化（另走 docker inspect 分支）。所以检测必须多路取证、层层兜底：
#   ① Custos 状态文件 /etc/ha-mysql-node/install-state-*（装了 Custos 就有）
#   ② **运行中的 mariadbd/mysqld 进程**（--datadir / --defaults-file）—— 最通用、最真实：
#      标准包管理、tarball、自定义端口/目录都能命中，且反映「当前真正在跑的那个实例」
#   ③ systemd 单元：**枚举**名字含 maria/mysql 的服务，再用 ExecStart 是否指向服务端二进制确认
#      ⇒ mariadb-ha.service / mariadb.service / mysqld.service / mysql.service 全都能命中，
#        不依赖任何硬编码单元名
#   ④ my.cnf 兜底 grep datadir（进程没带 --datadir 时）
# 仍全部落空才用内置默认值。整段只在远端 shell 跑一次，不增加 SSH 往返。
# ---------------------------------------------------------------------------
_BARE_STATE_DIR = "/etc/ha-mysql-node"
_BARE_UNIT_FALLBACK = {"mysql": "mysql.service", "mariadb": "mariadb-ha.service"}

# 远端探测脚本（不要对它做 % 格式化：内含 printf '%s=%s\n'）。
_BARE_PROBE = r'''
emit() { printf '%s=%s\n' "$1" "$2"; }
DD=""; UNIT=""; CNF=""; SRC=""

# ① Custos 安装状态文件（存在即用，但仅作为候选之一）
#    必须用 find 而不是 "for f in .../install-state-*" —— 节点登录 shell 可能是 zsh，
#    zsh 在 glob 无匹配时是【致命错误】会直接中止整个脚本（实测：没装 Custos 的节点上
#    探测直接返回空，什么兜底都走不到）。find 无匹配只是输出为空，安全。
for f in $(find /etc/ha-mysql-node -maxdepth 1 -name 'install-state-*' 2>/dev/null); do
  [ -f "$f" ] || continue
  . "$f" 2>/dev/null || true
done
[ -n "${DATA_DIR:-}" ] && { DD="$DATA_DIR"; SRC="custos-state"; }
[ -n "${SYSTEMD_UNIT:-}" ] && UNIT="$SYSTEMD_UNIT"

# ② 运行中的服务端进程（最通用的保底来源）
PCD=""; PID=""
for p in $(ls /proc 2>/dev/null | grep -E '^[0-9]+$'); do
  c=$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null) || continue
  case "$c" in
    *mysqld_safe*|*mariabackup*|*mariadb-backup*|*mysqldump*|*mysqlbackup*|*xtrabackup*|*exporter*) continue ;;
    *mariadbd*|*mysqld*) PCD="$c"; PID="$p"; break ;;
  esac
done
if [ -n "$PCD" ]; then
  emit SRV_PID "$PID"
  d=$(printf '%s' "$PCD" | tr ' ' '\n' | sed -n 's/^--datadir=//p' | head -1)
  [ -n "$d" ] && { DD="$d"; SRC="process"; }
  c=$(printf '%s' "$PCD" | tr ' ' '\n' | sed -n 's/^--defaults-file=//p' | head -1)
  [ -n "$c" ] && CNF="$c"
fi

# ③ systemd 单元：枚举 + 用 ExecStart 指向服务端二进制来确认（不硬编码单元名）
if [ -z "$UNIT" ]; then
  for u in $(systemctl list-unit-files --type=service --no-legend --no-pager 2>/dev/null | awk '{print $1}' | grep -iE 'maria|mysql'); do
    es=$(systemctl show -p ExecStart --value "$u" 2>/dev/null)
    case "$es" in
      *mariadbd*|*mysqld*) UNIT="$u"; emit UNIT_EXEC "$es"; break ;;
    esac
  done
fi

# ④ 配置文件兜底（进程没带 --datadir 时）
if [ -z "$DD" ]; then
  for c in $CNF /etc/my.cnf /etc/mysql/my.cnf /etc/my.cnf.d/server.cnf /etc/mysql/mariadb.conf.d/50-server.cnf; do
    [ -n "$c" ] || continue
    [ -f "$c" ] || continue
    d=$(sed -n 's/^[[:space:]]*datadir[[:space:]]*=[[:space:]]*//p' "$c" 2>/dev/null | head -1 | tr -d '"' | tr -d "'")
    [ -n "$d" ] && { DD="$d"; [ -n "$CNF" ] || CNF="$c"; SRC="mycnf"; break; }
  done
fi

emit DATA_DIR "$DD"
emit SYSTEMD_UNIT "$UNIT"
emit MYCNF "$CNF"
emit DD_SOURCE "$SRC"
emit PROBE_DONE 1
'''


def _bare_kind(node_type):
    """裸机形态归类：mysql* → mysql，其余（mariadb*）→ mariadb。"""
    return "mysql" if str(node_type).startswith("mysql") else "mariadb"


def _node_bare_state(kind, host, cfg, use_src_cred, node_type):
    """探测裸机节点的 datadir / systemd 单元 / my.cnf（多候选 + 保底，见上方说明）。

    返回 {'DATA_DIR':.., 'SYSTEMD_UNIT':.., 'MYCNF':.., 'DD_SOURCE':..}；
    SSH 不通或全部落空时对应键缺省，由调用方回退到内置默认值。
    """
    try:
        sh = _ssh_run(cfg, host, _BARE_PROBE, use_src_cred, node_type)
    except Exception:
        return {}
    st = {}
    for line in (getattr(sh, "stdout", "") or "").splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        if k in ("DATA_DIR", "SYSTEMD_UNIT", "MYCNF", "DD_SOURCE", "SRV_PID"):
            st[k] = v.strip().strip('"').strip("'")   # printf %q / 引号兜底
    return st


def _ssh_help(use_src_cred):
    """SSH 不可达/认证失败时的可操作指引（用户明确反馈：原提示没说要到哪里去开 agent-exec）。"""
    side = "源机" if use_src_cred else "目标机"
    return (
        "\n【怎么修】\n"
        "  1) 回到迁移向导「高级模式（物理通道 B）」第 2 步，填写「%s SSH 账号」/「%s SSH 密码」"
        "——它要的是该机器【宿主机 root】的 SSH 密码，不是数据库 root 密码。\n"
        "     注意：本管理器默认【不】给节点下发公钥，所以 SSH 密码留空【不会】自动改走密钥认证，"
        "会直接失败；请务必填写（或你事先自行配好了免密）。\n"
        "  2) 若该节点是 Custos 纳管节点、且确实不方便给 SSH：到「节点管理」页找到该节点，"
        "点该行的「重新生成」（多个节点可用「批量重新生成」）重新下发节点包。\n"
        "     重新下发后节点 agent 会提供 /exec 通道，管理器即可【免 SSH】在该节点执行 docker/mariabackup"
        " —— 这就是错误里说的 agent-exec。\n"
        "  3) 若该节点【不是】Custos 纳管节点（例如外部库 / 1panel 装的源库）：它没有 agent，"
        "只能填 SSH 密码，没有第二条路。\n"
        % (side, side)
    )


def _check_node_mb(node_type, host, port, cfg, use_src_cred):
    """物理通道前置探测：确认源/目标节点存在 mariabackup，否则抛清晰错误 + 安装指引。"""
    is_docker = str(node_type).endswith("-docker")
    if is_docker:
        cid, diag = _find_mariadb_container(host, cfg, use_src_cred, node_type)
        if diag == "ssh_fail":
            raise RuntimeError("物理通道前置检查失败：无法 SSH 到 %s（SSH 不可达或认证失败）。%s"
                               % (host, _ssh_help(use_src_cred)))
        if diag == "docker_missing":
            raise RuntimeError("物理通道前置检查失败：%s 上 docker 不可用（command -v docker 为空），"
                               "请确认该节点已安装并启动 Docker。" % host)
        if not cid:
            raise RuntimeError("物理通道前置检查失败：未在 %s 找到运行中的 MariaDB 容器"
                               "（已按 compose 服务 / 容器名 / 镜像 匹配均无）。"
                               "请确认该节点已部署并运行 Custos MariaDB 容器。" % host)
        sh = _ssh_run(cfg, host, "docker exec %s command -v mariabackup || true" % cid,
                      use_src_cred, node_type)
        present = bool(sh.stdout.strip())
    else:
        # 裸机：原实现只看 stdout 是否为空 ⇒ SSH 本身失败时会被误报成「未找到 mariabackup」，
        # 把用户引到装包方向去（错方向）。这里先区分「SSH 不通」与「真没装」。
        sh = _ssh_run(cfg, host,
                      "command -v /opt/mariadb/bin/mariabackup 2>/dev/null "
                      "|| command -v mariabackup 2>/dev/null || true", use_src_cred, node_type)
        if _ssh_connect_failed(sh):
            raise RuntimeError("物理通道前置检查失败：无法 SSH 到 %s（SSH 不可达或认证失败）。%s"
                               % (host, _ssh_help(use_src_cred)))
        present = bool(sh.stdout.strip())
    if not present:
        where = ("容器 %s：请改用自带 mariabackup 的官方 mariadb 镜像" % host
                 if is_docker else
                 "裸机 %s：tarball 安装通常在 /opt/mariadb/bin/mariabackup；"
                 "若缺失请执行 apt-get install -y mariadb-backup 或 dnf install -y MariaDB-backup" % host)
        raise RuntimeError("物理通道前置检查失败：节点 %s 未找到 mariabackup。%s" % (host, where))
    return True


def _remote_mb(node_type, host, cfg, op, staging_host, src=None, src_data_dir=None,
               datadir=None, mb_bin=None, svc_unit=None, src_my_cnf=None):
    """在 node_type 节点上执行 mariabackup 的某步（backup/prepare/copyback）。

    容器节点经 SSH + docker 执行；裸机经 SSH 直连执行。返回 CompletedProcess。
    mb_bin 为已解析的 mariabackup 路径（容器用镜像内 PATH 的 'mariabackup'）。
    """
    is_docker = str(node_type).endswith("-docker")
    if mb_bin is None:
        mb_bin = "mariabackup" if is_docker else _resolve_mb(host, cfg, use_src_cred=(op != "copyback"),
                                                              node_type=node_type)
    if op == "backup":
        mb = [mb_bin, "--backup",
              "--host=%s" % src[0], "--port=%s" % src[1],
              "--user=%s" % src[2], "--password=%s" % src[3],
              "--target-dir=%s" % staging_host]
        # 裸机：源 my.cnf 在宿主机真实路径，mariabackup 直连读取；
        # 容器节点里 mariabackup 在容器内执行（docker exec），应读容器自带 my.cnf（自动），
        # 传宿主机路径会因容器内不存在而报 Could not open required defaults file。
        # 裸机源：优先用向导填的 --src_my_cnf；未填则用自动探测到的（运行进程的 --defaults-file），
        # 这样「标准编译/tarball 用了自定义 defaults-file」的节点也能正确读到 datadir/socket。
        _cnf = src_my_cnf or cfg.get("src_my_cnf")
        if _cnf and not is_docker:
            mb.insert(1, "--defaults-file=%s" % _cnf)
        # 裸机才传 --datadir（宿主机真实数据目录）；容器节点里 mariabackup 的数据目录
        # 固定为 /var/lib/mysql，且下方 docker 分支会把 --target-dir 改写为容器内路径
        if src_data_dir and not is_docker:
            mb.append("--datadir=%s" % src_data_dir)
    elif op == "prepare":
        mb = [mb_bin, "--prepare", "--target-dir=%s" % staging_host]
    elif op == "copyback":
        # 容器目标的 copy-back 在下方 is_docker 分支用「显式 -v 挂载 + 空 datadir」执行，
        # 不会用到这里的 mb；此处仅供裸机使用，必须是宿主机真实路径。
        mb = [mb_bin, "--copy-back",
              "--target-dir=%s" % staging_host,
              "--datadir=%s" % (datadir or "/var/lib/mysql")]
    else:
        raise RuntimeError("未知 mariabackup 操作: %s" % op)

    if is_docker:
        cid, diag = _find_mariadb_container(host, cfg, op != "copyback", node_type)
        if diag == "ssh_fail":
            raise RuntimeError("无法 SSH 到节点 %s（SSH 不可达或认证失败），请核对向导 SSH 账号/密码。" % host)
        if diag == "docker_missing":
            raise RuntimeError("节点 %s 上 docker 不可用，无法执行该操作。" % host)
        if not cid:
            raise RuntimeError("未在节点 %s 找到运行中的 MariaDB 容器"
                               "（已按 compose 服务 / 容器名 / 镜像 匹配均无）。" % host)
        if op == "copyback":
            # 目标 docker：直接用容器 cid 操作，**不依赖 compose 项目目录**（Custos 节点 compose
            # 未必在 /data/custos-ha，硬编码会导致 docker compose 找不到项目→静默失败→假成功）。
            # 流程：停实例 → 旧数据目录改名留作回滚点 → 重建【空】数据目录 →
            #       显式 -v 挂载 datadir=/var/lib/mysql + 暂存=/backup 执行 copy-back →
            #       chown 到 999 → 启动 → 清理暂存。
            # 两个关键点（缺一即失败，均已实测坐实）：
            #  ① 暂存目录必须在 datadir **之外**（由 _mig_staging_path_tgt 保证）。若在 datadir 内，
            #     copy-back 会报「Original data directory /var/lib/mysql is not empty!」拒绝写入，
            #     结果是数据目录被清空、容器被 stop，节点直接不可用 —— 这是物理通道反复失败的根因。
            #  ② 临时恢复容器以 root(--user 0:0) 运行并显式 -v 挂载，避免镜像默认 uid 999
            #     写不进 root 属主目录；恢复后统一 chown 999（官方镜像默认用户），使目标容器
            #     无论以 0:0 还是 999 运行都能读写。
            # 失败即【自动回滚】（把 _pre_restore_bak 挪回并重启容器），绝不把目标节点留在
            # 「空数据目录 + 容器已停」的死状态。
            image = _docker_image(cfg, host, cid, op != "copyback", node_type)
            data_dir = datadir or "/var/lib/mysql"
            bak = data_dir.rstrip("/") + "_pre_restore_bak"
            q_dd, q_bak, q_stg = (_sh_quote(data_dir), _sh_quote(bak),
                                  _sh_quote(staging_host))
            script = (
                "set -e; docker stop %s || true; "
                "rm -rf %s; mv %s %s; mkdir -p %s; "
                "if ! docker run --rm --user 0:0 --entrypoint mariabackup "
                "-v %s:/var/lib/mysql -v %s:/backup %s "
                "--copy-back --target-dir=/backup --datadir=/var/lib/mysql; then "
                "echo 'copy-back FAILED, rolling back data dir' >&2; "
                "rm -rf %s; mv %s %s; docker start %s; exit 1; "
                "fi; "
                "chown -R 999:999 %s; docker start %s; rm -rf %s"
                % (cid, q_bak, q_dd, q_bak, q_dd,
                   q_dd, q_stg, _sh_quote(image),
                   q_dd, q_bak, q_dd, cid,
                   q_dd, cid, q_stg)
            )
            return _ssh_run(cfg, host, script, use_src_cred=False, node_type=node_type)
        # backup / prepare：容器运行中，docker exec 进容器（暂存放在数据目录挂载内）
        inner_staging = _container_staging(staging_host)
        # 仅备份前清空 target-dir（mariabackup 要求为空/不存在）；prepare/copyback 不可清，
        # 否则会把刚备份好的目录删掉（prepare 报 No such file or directory）。
        if op == "backup":
            _ssh_run(cfg, host, "docker exec %s rm -rf %s" % (cid, _sh_quote(inner_staging)),
                     use_src_cred=True, node_type=node_type)
        mb[-1] = "--target-dir=%s" % inner_staging
        inner = " ".join(_sh_quote(a) for a in mb)
        return _ssh_run(cfg, host, "docker exec %s %s" % (cid, inner), use_src_cred=True, node_type=node_type)
    # 裸机：直接 SSH 执行
    if op == "copyback":
        # 裸机目标与容器目标遵守同一条铁律：copy-back 时 datadir 必须为空，且必须留回滚点。
        # 时序：停服务 → 旧 datadir 改名(_pre_restore_bak) → 建空 datadir → copy-back →
        #       失败则自动回滚并把服务拉回 → chown 回属主 → 启动服务 → 清理暂存。
        # 停服失败直接中断（绝不在服务仍运行时改名 datadir，否则数据损坏）。
        # 单元名必须用【真实】systemd 单元（由 seed_physical 从安装状态文件取，见 _node_bare_state）：
        # 猜成 mariadb.service 会直接报 "Unit mariadb.service not loaded."（203 实测踩过）。
        svc = svc_unit or _BARE_UNIT_FALLBACK.get(_bare_kind(node_type), "mariadb-ha.service")
        q_svc = _sh_quote(svc)
        ddv = (datadir or "/var/lib/mysql").rstrip("/")
        q_dd = _sh_quote(ddv)
        q_bak = _sh_quote(ddv + "_pre_restore_bak")
        inner = " ".join(_sh_quote(a) for a in mb)
        script = (
            "set -e; "
            "systemctl cat %s >/dev/null 2>&1 || { "
            "echo '找不到 systemd 单元 %s：该裸机实例可能不是按 Custos 安装脚本部署的' >&2; exit 1; }; "
            "systemctl stop %s; "
            "rm -rf %s; mv %s %s; mkdir -p %s; "
            "if ! %s; then "
            "echo 'copy-back FAILED, rolling back data dir' >&2; "
            "rm -rf %s; mv %s %s; systemctl start %s || true; exit 1; "
            "fi; "
            "chown -R mysql:mysql %s 2>/dev/null || true; "
            "systemctl start %s; rm -rf %s"
            % (q_svc, q_svc, q_svc, q_bak, q_dd, q_bak, q_dd, inner,
               q_dd, q_bak, q_dd, q_svc, q_dd, q_svc, _sh_quote(staging_host))
        )
        return _ssh_run(cfg, host, script, use_src_cred=False, node_type=node_type)
    if op == "backup":
        _ssh_run(cfg, host, "rm -rf %s" % _sh_quote(staging_host),
                 use_src_cred=(op != "copyback"), node_type=node_type)
    inner = " ".join(_sh_quote(a) for a in mb)
    return _ssh_run(cfg, host, inner, use_src_cred=(op != "copyback"), node_type=node_type)


_TGT_READY_TIMEOUT = 300   # copy-back 后目标可能需 mariadb-upgrade（跨大版本实测数十秒）


def _wait_target_ready(cfg, timeout=None):
    """物理 copy-back 后等待目标实例真正可连。

    根因（2026-09-20 实测坐实）：copy-back 后 docker start 会触发 entrypoint 的
    mariadb-upgrade（源/目标版本不同时 Phase 1~8，实测约 1 分钟），期间端口未就绪。
    若不等就立刻进入桥接阶段，会以 Connection refused 报错、被误判成迁移失败。
    凭据先试【源端】（物理恢复后目标的 mysql.user 表整体来自源），再退回向导里的目标 root 密码。
    """
    timeout = timeout or _TGT_READY_TIMEOUT
    cands = [(cfg.get("src_user") or "root", cfg.get("src_pass") or ""),
             (_tgt_user(cfg), cfg.get("tgt_root_pass") or "")]
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        for u, p in cands:
            try:
                c = connect(cfg["tgt_host"], int(cfg["tgt_port"]), u, p, timeout=5)
                c.close()
                return True
            except Exception as e:
                last = e
        time.sleep(3)
    raise RuntimeError(
        "物理恢复已写入数据，但等待目标 %s:%s 就绪超时（%ds）：%s。"
        "请查看目标 MariaDB 容器日志（可能仍在 mariadb-upgrade，或启动失败）后重试。"
        % (cfg["tgt_host"], cfg["tgt_port"], timeout, last))


def _sql_version(host, port, cands, timeout=8):
    """连上去取 VERSION()（多组凭据依次尝试）。失败返回 ("", "")。"""
    for u, p in cands:
        try:
            conn = connect(host, int(port), u, p, timeout=timeout)
            try:
                cur = conn.cursor()
                cur.execute("SELECT VERSION()")
                row = cur.fetchone()
                v = str((row[0] if row else "") or "")
            finally:
                conn.close()
            low = v.lower()
            return ("mariadb" if "mariadb" in low else "mysql"), v
        except Exception:
            continue
    return "", ""


def _ver_tuple(v):
    """从版本串取数字元组，用于比较升级/降级（如 '12.3.3-MariaDB' → (12,3,3)）。"""
    nums = []
    for part in str(v).split("-")[0].split("."):
        d = "".join(ch for ch in part if ch.isdigit())
        if d == "":
            break
        nums.append(int(d))
    return tuple(nums) if nums else (0,)


def _assert_physical_compatible(cfg):
    """物理通道（高级模式）的兼容性门禁 —— 开跑前实测两端，避免跑到深处才报看不懂的错。

    实测口径（2026-09-20，177 MariaDB 10.11 → 203 MariaDB 12.3.3 跑通）：
      · 物理通道基于 **mariabackup**，它是 MariaDB 自带工具 —— MySQL 上根本没有该命令
        （MySQL 要用 xtrabackup/mysqlbackup），且 MySQL 8 起改用全局数据字典，MariaDB 读不懂
        其数据文件。故物理通道**只能 MariaDB → MariaDB**。
      · MariaDB 之间**跨版本可以**，但有方向性：mariabackup 备份**不能恢复到更旧的版本**
        （升级方向 OK，降级不行）。故 src 版本 > tgt 版本时明确拒绝，避免用户拿到半死不活的实例。
    说明：向导里 tgt_flavor 目前硬编码 mariadb，若源是 MySQL 会被误判成"跨引擎"、MySQL→MySQL 又会被
    误判成"同引擎"而放行 —— 所以这里以 SQL 实测 VERSION() 为准，而不是相信配置里的品牌字段。
    """
    sf, sv = _sql_version(cfg["src_host"], int(cfg["src_port"]),
                          [(cfg.get("src_user") or "root", cfg.get("src_pass") or "")])
    tf, tv = _sql_version(cfg["tgt_host"], int(cfg["tgt_port"]),
                          [(_tgt_user(cfg), cfg.get("tgt_root_pass") or ""),
                           (cfg.get("src_user") or "root", cfg.get("src_pass") or "")])
    if sf == "mysql" or tf == "mysql":
        side = "源库" if sf == "mysql" else "目标库"
        ver = sv if sf == "mysql" else tv
        raise RuntimeError(
            "物理通道（高级模式）只能 MariaDB → MariaDB：实测【%s】是 MySQL（VERSION()=%s）。\n"
            "mariabackup 是 MariaDB 自带工具，MySQL 上没有该命令（MySQL 需 xtrabackup/mysqlbackup）；"
            "且 MySQL 8 起改用全局数据字典，MariaDB 无法读取其数据文件。\n"
            "【请改用标准模式（逻辑通道）】：MariaDB ⇄ MySQL 5.7 的互迁是支持的；"
            "MySQL 8.0+ → MariaDB 目前不支持（需先做排序规则/认证插件换算）。\n"
            "若两端都是 MariaDB（可跨版本），物理通道是支持的。"
            % (side, ver or "?"))
    if sf == "mariadb" and tf == "mariadb" and sv and tv:
        if _ver_tuple(sv) > _ver_tuple(tv):
            raise RuntimeError(
                "物理通道（高级模式）不能把 mariabackup 备份恢复到【更旧】的 MariaDB：\n"
                "  源 %s：%s\n  目标 %s：%s\n"
                "物理备份只能向上兼容（低版本备份 → 高版本实例），降级恢复官方不支持，"
                "强行恢复会得到无法启动或数据损坏的实例。\n"
                "如需往低版本迁移，请改用【标准模式（逻辑通道）】（跨版本可用逻辑导出/导入）。"
                % (cfg["src_host"], sv, cfg["tgt_host"], tv))
    return {"src_flavor": sf, "src_version": sv, "tgt_flavor": tf, "tgt_version": tv}


def seed_physical(task):
    """物理通道：源节点 mariabackup 热备 → prepare → 传到目标 → 目标节点 copy-back。

    关键时序（务必遵守）：mariabackup 只能读取【本地】数据目录，无法备份远程服务器，
    所以「备份 / prepare」在【源节点】执行（源自带 mariabackup，版本天然匹配源），
    「copy-back」在【目标节点】执行（目标自带 mariaback，版本匹配目标）。
    管理器/宿主机不预装 mariabackup，仅需 openssh-client + rsync + sshpass 做编排。

    版本兼容：**MariaDB 各版本之间可用**（实测 10.11.17 → 12.3.3 跑通），方向必须是
    低版本→高版本；MySQL 不适用（见 _assert_physical_compatible）。

    源端 my.cnf / 数据目录非标安装时，须显式填写 src_my_cnf / src_data_dir。
    """
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    nt = cfg.get("node_type", "mariadb-docker")            # 目标形态
    snt = cfg.get("src_type", nt)                           # 源形态（默认同目标）
    # 裸机节点解析 mariabackup 完整路径（tarball 装在 /opt/mariadb/bin，常不在 PATH）；容器用镜像内 PATH
    src_mb = ("mariabackup" if str(snt).endswith("-docker")
              else _resolve_mb(cfg["src_host"], cfg, use_src_cred=True, node_type=snt))
    tgt_mb = ("mariabackup" if str(nt).endswith("-docker")
              else _resolve_mb(cfg["tgt_host"], cfg, use_src_cred=False, node_type=nt))
    relay = f"/tmp/custos_mig_relay_{task['id']}"            # 管理器中转（避免源→目标直连 ssh）
    os.makedirs(relay, exist_ok=True)
    # 提前初始化：finally 里要按这些路径清理远端暂存，若前面某步就抛错（如前置探测失败），
    # 引用未赋值变量会抛 UnboundLocalError，**把真正的失败原因整个盖掉**（此前日志只剩 START
    # 就是这个原因，排查被严重误导）。故先置 None，finally 判空跳过。
    dd = src_dd = staging = src_staging = None
    try:
        # 迁移锁：从 seed 起就锁定目标节点，覆盖整个迁移（含 copy-back 停实例、桥接追平），
        # 避免 monitor 在恢复/桥接期间误判节点离线/数据分叉，或去动该节点
        # （failover.ensure_topology 对「当前主被锁定」的集群整轮跳过）。锁带 6h 自动释放保险丝；
        # cutover 成功会显式 unlock_target。
        lock_target(cfg["tgt_host"], int(cfg["tgt_port"]))
        # 0) 兼容性门禁：物理通道只能 MariaDB→MariaDB，且不能降级恢复（实测 VERSION() 判定）。
        #    放在最前面，避免用户等完一整套备份/传输才拿到一个看不懂的错。
        _assert_physical_compatible(cfg)
        # 0.1) 物理通道前置探测：源/目标节点须存在 mariabackup，否则提前给清晰错误 + 安装指引
        #      （先验证 SSH/docker 连通性，确保下方 docker inspect 探测挂载源时 SSH 已可用）
        _check_node_mb(snt, cfg["src_host"], int(cfg["src_port"]), cfg, use_src_cred=True)
        _check_node_mb(nt, cfg["tgt_host"], int(cfg["tgt_port"]), cfg, use_src_cred=False)
        _tick(task, 3, stage="解析源/目标数据目录（宿主机路径）…",
              log="物理通道前置校验通过，开始解析数据目录")
        # 1) 解析源/目标数据目录（宿主机路径）
        #    docker 节点：容器 /var/lib/mysql 的真实宿主机挂载源【必须实测】，不能依赖硬编码默认
        #    （1panel 等安装的数据目录并不在 /data/custos-ha/mariadb12）。探测不到就显式报错，
        #    绝不静默回退到错误默认路径（否则 rsync change_dir 失败 / 乱码容器路径）。
        dd = _tgt_datadir(cfg)
        src_dd = cfg.get("src_data_dir") or _default_src_datadir(snt)
        if str(snt).endswith("-docker"):
            scid, _ = _find_mariadb_container(cfg["src_host"], cfg, True, snt)
            m = _docker_datadir_host_mount(cfg, cfg["src_host"], scid, True, snt)
            if m:
                src_dd = m
            else:
                raise RuntimeError(
                    "无法探测源节点 %s 容器 /var/lib/mysql 的宿主机挂载源（docker inspect 未返回）。"
                    "请确认源为运行中的 MariaDB 容器，或在向导「源端数据目录」显式填写容器 "
                    "/var/lib/mysql 对应的宿主机绝对路径（如 1panel 的 "
                    "/opt/1panel/apps/mariadb/mariadb/data）。" % cfg["src_host"])
        if str(nt).endswith("-docker"):
            tcid, _ = _find_mariadb_container(cfg["tgt_host"], cfg, False, nt)
            m = _docker_datadir_host_mount(cfg, cfg["tgt_host"], tcid, False, nt)
            if m:
                dd = m
            else:
                raise RuntimeError(
                    "无法探测目标节点 %s 容器 /var/lib/mysql 的宿主机挂载源（docker inspect 未返回）。"
                    "请确认目标为运行中的 MariaDB 容器，或在向导「目标数据目录」显式填写容器 "
                    "/var/lib/mysql 对应的宿主机绝对路径（如 1panel 的 "
                    "/opt/1panel/apps/mariadb/mariadb/data）。" % cfg["tgt_host"])
        # 裸机节点：datadir / systemd 单元 / my.cnf 全部【多候选探测】，绝不用内置默认值猜
        # （猜错就是 203 实测的「Unit mariadb.service not loaded」+ 指向不存在的数据目录）。
        # 探测顺序见 _node_bare_state：Custos 状态文件 → 运行中进程 → 枚举 systemd 单元 → my.cnf。
        tgt_unit = None
        src_my_cnf = cfg.get("src_my_cnf") or ""
        if not str(snt).endswith("-docker"):
            st = _node_bare_state(_bare_kind(snt), cfg["src_host"], cfg, True, snt)
            if st.get("DATA_DIR") and not cfg.get("src_data_dir"):
                src_dd = st["DATA_DIR"]
            if not src_my_cnf and st.get("MYCNF"):
                src_my_cnf = st["MYCNF"]
            task["_bare_src"] = st
        if not str(nt).endswith("-docker"):
            st = _node_bare_state(_bare_kind(nt), cfg["tgt_host"], cfg, False, nt)
            if st.get("DATA_DIR") and not cfg.get("tgt_datadir"):
                dd = st["DATA_DIR"]
            tgt_unit = st.get("SYSTEMD_UNIT") or _BARE_UNIT_FALLBACK[_bare_kind(nt)]
            task["_bare_tgt"] = st
        staging = _mig_staging_path_tgt(nt, dd, task["id"])      # 目标机暂存：必须在 datadir 之外
        src_staging = _mig_staging_path(snt, src_dd, task["id"]) # 源机暂存（容器节点须在 datadir 内）
        # 1) 源节点热备（读源本地 datadir；--host 仅用于连接源做协调，不读远端文件）
        _tick(task, 8, stage="① 源节点 mariabackup 热备中（耗时取决于数据量，请耐心等待；"
                      "进度条将在备份结束后推进）", log="开始备份源节点数据目录（mariabackup）")
        bk = _remote_mb(snt, cfg["src_host"], cfg, "backup", src_staging,
                        src=src, src_data_dir=src_dd, mb_bin=src_mb, src_my_cnf=src_my_cnf)
        if bk.returncode != 0:
            raise RuntimeError("源节点 mariabackup 备份失败: " + _tail(bk.stderr))
        _tick(task, 40, stage="① 源节点备份完成，正在 prepare（回放 redo 日志，使其一致）",
              log="源节点 mariabackup 备份完成")
        # 2) 源节点 prepare（回放 redo，使其一致）
        pr = _remote_mb(snt, cfg["src_host"], cfg, "prepare", src_staging, mb_bin=src_mb)
        if pr.returncode != 0:
            raise RuntimeError("源节点 mariabackup prepare 失败: " + _tail(pr.stderr))
        _tick(task, 52, stage="② 正在拉取备份到管理器（rsync 中转）", log="prepare 完成，开始拉取备份")
        # 3) 拉取源机暂存 → 管理器中转 → 推到目标机暂存
        pull = _rsync_pull(cfg, cfg["src_host"], src_staging + "/", relay)
        if pull.returncode != 0:
            raise RuntimeError("拉取源备份失败(rsync): " + _tail(pull.stderr))
        _tick(task, 66, stage="③ 正在推送备份到目标节点（rsync）", log="已拉取到管理器，开始推送")
        push = _rsync_push(cfg, relay, cfg["tgt_host"], staging + "/")
        if push.returncode != 0:
            raise RuntimeError("推送备份到目标失败(rsync): " + _tail(push.stderr))
        _tick(task, 80, stage="④ 目标节点 copy-back 恢复中（停实例→恢复→授权→启动）",
              log="已推送到目标节点，开始 copy-back 恢复")
        # 4) 目标节点 copy-back（停实例→恢复→授权→启动）
        cb = _remote_mb(nt, cfg["tgt_host"], cfg, "copyback", staging, datadir=dd,
                        mb_bin=tgt_mb, svc_unit=tgt_unit)
        if cb.returncode != 0:
            raise RuntimeError("目标节点 copy-back 失败: " + _tail(cb.stderr))
        _tick(task, 95, stage="④ copy-back 完成，等待目标实例就绪（跨版本升级可能需约 1 分钟）",
              log="copy-back 完成，等待目标实例就绪")
        # 4.1) 等目标实例就绪：copy-back 完成时容器刚 docker start，entrypoint 可能仍在
        #      mariadb-upgrade（跨大版本实测约 1 分钟），此时端口不可达；不等就进桥接会
        #      Connection refused，被误判成迁移失败（实测）。
        _wait_target_ready(cfg)
        # 4.2) copy-back 已用源库数据目录整库覆盖目标（mysql.user 一并被替换），
        #      此刻必须立刻重建 Custos 账号，否则第二节点/管理器在整个追平窗口内都连不上目标。
        _acct = _ensure_target_accounts(cfg, stage="seed_physical")
        if not _acct.get("ok"):
            print("[migration] 目标账号重建失败（不阻断，cutover 会重试）：%s" % _acct.get("error"))
        _tick(task, 100, stage="物理恢复完成，可继续执行「建立桥接」", log="目标实例就绪，物理种子完成")
    finally:
        shutil.rmtree(relay, ignore_errors=True)
        # 清理远端暂存目录：源节点容器暂存放在 datadir 内的 .custos_mig_staging_*（不清理会被
        # MariaDB 当成 #mysql50# 伪库）；目标暂存是 datadir 的兄弟目录。无论成败都尽量删掉。
        # docker 节点的暂存是 bind-mount 的宿主机路径，须走宿主机 SSH 删除（传裸机 node_type
        # 绕过 docker-exec 路由）。_st 为空（前面某步就失败、路径未解析）时跳过，避免
        # UnboundLocalError/空路径误删掩盖真实错误。
        for _h, _st, _src in ((cfg["src_host"], src_staging, True),
                               (cfg["tgt_host"], staging, False)):
            if not _st:
                continue
            try:
                _ssh_run(cfg, _h, "rm -rf " + _sh_quote(_st),
                         use_src_cred=_src, node_type="mariadb")
            except Exception:
                pass
        # 即便 rm -rf 因权限/容器挂载失败，源库上仍会残留 MariaDB 把 datadir 内
        # .custos_mig_staging_* 编码成的【伪库】`#mysql50#.custos_mig_staging_<id>`
        # （实测 177 上残留了 62 / 1007 两个），会被库列表误选、让后续迁移报
        # "Incorrect database name"。这里再 best-effort DROP 一次。
        try:
            _c = connect(cfg["src_host"], int(cfg["src_port"]),
                         cfg["src_user"], cfg["src_pass"], timeout=8)
            try:
                _cur = _c.cursor()
                _cur.execute("DROP DATABASE IF EXISTS `%s`" %
                             ("#mysql50#.custos_mig_staging_%s" % task["id"]).replace("`", ""))
                _c.commit()
            finally:
                _c.close()
        except Exception:
            pass
    return {"ok": True, "datadir": dd, "staging": staging,
            "note": "物理恢复完成，请将目标实例作为新主继续桥接"}


# ---------------------------------------------------------------------------
# 迁移期间「目标节点锁定」
#
# 根因（2026-09-18 实测坐实，切勿删除本机制）：
#   若目标（203）已被 Custos 纳管为某集群的【当前主】，monitor 的 ensure_topology
#   每 ~2s 会对它调用 promote_to_master；而 promote_to_master 的「只读快检」要求
#   「read_only=0 且无复制通道」才跳过 —— 一旦桥接建立（出现复制行），就会执行
#   STOP REPLICA + RESET REPLICA ALL，**把 177→203 的桥接通道清掉**。
#   于是桥接活不过一个监控周期，追平检测必然永远「无复制通道」。
#   典型触发场景：用户先把 203/213 装成新集群（203 是当前主），再用迁移向导灌数据。
#
# 对策：桥接期间锁定目标节点，让 ensure_topology 整轮跳过，不再干扰。
# ---------------------------------------------------------------------------
_MIG_LOCKS = {}            # "ip:port" -> 过期时间戳（monotonic），内存快路径
_MIG_LOCK_TTL = 6 * 3600   # 保险丝：极端情况下忘记解锁也能自动恢复（6 小时）
_mig_lock = threading.Lock()
_MIG_LOCK_DB_TTL = _MIG_LOCK_TTL  # 持久锁保险丝（秒），与内存 TTL 对齐


def _ensure_locks_table():
    """迁移锁持久化表：抗管理器重建/重启（内存锁重启即失，会导致 monitor 清掉桥接通道）。

    仅在首次访问时创建，失败静默（退化回内存锁，不阻断主流程）。
    """
    try:
        execute("CREATE TABLE IF NOT EXISTS migration_locks ("
                "host VARCHAR(64) NOT NULL, port INT NOT NULL, "
                "expires DATETIME NOT NULL, PRIMARY KEY (host, port))")
    except Exception:
        pass


def lock_target(host, port):
    """锁定迁移目标节点：暂停 monitor 对它（作为当前主）的拓扑维护。

    同时写入内存与持久表 —— 后者保证管理器重建/重启后锁定不丢失，
    否则 ensure_topology 会立刻 RESET REPLICA ALL 把桥接通道清掉（issue #3/#4 根因）。
    """
    if not host:
        return
    host, port = str(host), int(port)
    with _mig_lock:
        _MIG_LOCKS["%s:%d" % (host, port)] = time.monotonic() + _MIG_LOCK_TTL
    try:
        _ensure_locks_table()
        execute("REPLACE INTO migration_locks (host, port, expires) VALUES (%s,%s,"
                " DATE_ADD(NOW(), INTERVAL %s SECOND))", (host, port, _MIG_LOCK_DB_TTL))
    except Exception:
        pass


def unlock_target(host, port):
    """解除锁定（停桥之后交还 monitor 接管）。"""
    if not host:
        return
    host, port = str(host), int(port)
    with _mig_lock:
        _MIG_LOCKS.pop("%s:%d" % (host, port), None)
    try:
        execute("DELETE FROM migration_locks WHERE host=%s AND port=%s", (host, port))
    except Exception:
        pass


def force_clear_node(host, port, clear_tasks=True):
    """强制清除某节点的迁移残留状态（锁 + 可选的 running 任务）。

    场景：节点被重装/重建后，迁移锁（migration_locks）与指向它的 running 任务行
    仍残留，导致仪表盘永久显示「迁移中」，且 failover.ensure_topology 持续跳过该
    节点拓扑维护。本函数提供代码级一键清理，免手敲 SQL（等价于对管理库执行
    DELETE FROM migration_locks / migration_tasks）。

    ⚠ 仅应在确认该节点已不可作为迁移目标（如已重装、迁移线程必死）时调用，
    否则可能中断一个仍在正常运行的迁移。
    """
    host, port = str(host), int(port)
    info = {"unlocked": False, "tasks_deleted": 0}
    # 1) 清锁（内存 + 持久表）
    if is_target_locked(host, port):
        unlock_target(host, port)
        info["unlocked"] = True
    # 2) 清指向该节点的 running 任务（并清内存集合，避免孤儿线程干扰 cleanup_stale）
    if clear_tasks:
        try:
            rows = query("SELECT id, config FROM migration_tasks WHERE status='running'")
            dead = []
            for r in rows:
                try:
                    cfg = json.loads(r.get("config") or "{}")
                except Exception:
                    continue
                if (str(cfg.get("tgt_host")) == host
                        and int(cfg.get("tgt_port") or 0) == port):
                    dead.append(int(r["id"]))
            if dead:
                execute("DELETE FROM migration_tasks WHERE id IN %s", (tuple(dead),))
                for tid in dead:
                    _RUNNING.discard(tid)
                    _REP.pop(tid, None)
                    _CANCEL.discard(tid)
                    _STEP_HB.pop(tid, None)
                    _STEP_PROCS.pop(tid, None)
                    _FORCE_TS.pop(tid, None)
                info["tasks_deleted"] = len(dead)
        except Exception as e:
            info["task_error"] = f"{type(e).__name__}: {e}"
    return info


def is_target_locked(host, port):
    """目标是否处于迁移锁定中 —— 由 failover.ensure_topology 调用。

    内存命中优先（快）；未命中（含管理器刚重启、内存锁丢失）回退持久表，
    避免重启后 monitor 把桥接通道清掉导致从库断开主库 / 追平检测失败（issue #3/#4）。
    """
    if not host:
        return False
    host, port = str(host), int(port)
    key = "%s:%d" % (host, port)
    with _mig_lock:
        exp = _MIG_LOCKS.get(key)
        if exp is not None:
            if time.monotonic() <= exp:
                return True
            _MIG_LOCKS.pop(key, None)
    try:
        _ensure_locks_table()
        rows = query("SELECT expires FROM migration_locks WHERE host=%s AND port=%s",
                    (host, port))
        if rows:
            exp_db = rows[0].get("expires")
            if exp_db and exp_db > datetime.now():
                with _mig_lock:  # 命中后回填内存，减少后续 DB 访问
                    _MIG_LOCKS[key] = time.monotonic() + _MIG_LOCK_TTL
                return True
    except Exception:
        pass
    return False


def is_node_migrating(host, port):
    """该节点是否正处于迁移中（目标被锁定，或存在指向它的进行中迁移任务）。

    供 monitor 正确展示「迁移中」状态、避免把 seed/桥接/追平期的节点误判为离线/数据分叉。
    优先用内存锁（快），并兜底查 running 任务（覆盖锁未设的窗口，如 seed 刚开始瞬间）。
    """
    if is_target_locked(host, port):
        return True
    try:
        rows = query("SELECT config FROM migration_tasks WHERE status='running'")
        for r in rows:
            try:
                cfg = json.loads(r.get("config") or "{}")
            except Exception:
                continue
            if (str(cfg.get("tgt_host")) == str(host)
                    and int(cfg.get("tgt_port") or 0) == int(port)):
                return True
    except Exception:
        pass
    return False


def _check_source_binlog(cfg):
    """桥接前置检查：源库必须开启 binlog、server_id 非 0。

    否则 CHANGE MASTER + START REPLICA 依然会「成功」，但 IO 线程立刻报
    1236 'Binary log is not open' —— 用户只能去仪表盘才发现，体验等同于静默失败。
    故在此提前拦截，并给出可操作的修复指引。
    """
    conn = connect(cfg["src_host"], int(cfg["src_port"]),
                   cfg["src_user"], cfg["src_pass"], timeout=8)
    try:
        cur = conn.cursor()

        def _v(sql):
            try:
                cur.execute(sql)
                r = cur.fetchone()
                return r[0] if r else None
            except Exception:
                return None

        log_bin = _v("SELECT @@log_bin")
        server_id = _v("SELECT @@server_id")
        lb = str(log_bin).strip().upper()
        if lb in ("0", "OFF", "FALSE", "NONE", ""):
            raise RuntimeError(
                "源库 %s:%s 未开启 binlog（@@log_bin=%s），无法建立复制桥接——"
                "IO 线程会直接报 1236 'Binary log is not open'。\n"
                "【方案① 想继续桥接追平】在源库 my.cnf 的 [mysqld] 增加并【重启 MariaDB】：\n"
                "    server_id = <唯一且非 0 的整数>\n"
                "    log_bin   = /path/to/mysql-bin\n"
                "    binlog_format = ROW\n"
                "  （若源库自身还有从库，另需 log_slave_updates=ON）\n"
                "  改完回到第三步重跑「建立桥接」。\n"
                "【方案② 不方便重启源库】改走停写迁移：源库停写后只跑「数据种子」，"
                "再勾选「源库已停写」跳过追平直接切换。"
                % (cfg["src_host"], cfg["src_port"], log_bin))
        if str(server_id).strip() in ("0", "None", ""):
            raise RuntimeError(
                "源库 %s:%s 的 server_id=%s，复制无法工作（主从 server_id 必须唯一且非 0）。"
                "请在源库 my.cnf 设置 server_id 后重启 MariaDB。"
                % (cfg["src_host"], cfg["src_port"], server_id))
        return {"log_bin": log_bin, "server_id": server_id}
    finally:
        conn.close()


def bridge(task):
    """在目标建立指向源的 MariaDB GTID 复制桥接（持续追平）。"""
    cfg = task["config"]
    # 前置检查：源库未开 binlog 时建复制必然 1236 失败，提前拦截（见函数注释）
    _check_source_binlog(cfg)
    # ⚠ 必须先锁定目标节点再建桥接，否则 monitor 的 promote_to_master 会立刻
    #   RESET REPLICA ALL 把通道清掉（详见上方「迁移锁定」根因注释）。
    lock_target(cfg["tgt_host"], int(cfg["tgt_port"]))
    # 桥接前兜底补齐目标账号：seed 之后若目标又被重装/回滚过（或 seed 时账号重建失败），
    # 这里再补一次，保证桥接与追平窗口内目标始终可被管理器与同集群节点连入。
    _ensure_target_accounts(cfg, stage="bridge")
    acct = _ensure_source_repl_account(task)  # 源端复制账号（最小权限，必要时降级复用源账号）
    use_src = bool(acct.get("used_src_creds"))
    m_user = cfg["src_user"] if use_src else cfg["repl_user"]
    m_pass = cfg["src_pass"] if use_src else cfg["repl_pass"]
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"])
    # 建桥接前先自检：用复制账号从管理端连一次源库，口令/授权不对立刻暴露，
    # 否则要等目标 IO 线程报 Access denied 才被发现（且只在仪表盘看得到）。
    try:
        probe = connect(cfg["src_host"], int(cfg["src_port"]), m_user, m_pass, timeout=8)
        probe.close()
    except Exception as e:
        raise RuntimeError(
            "复制账号 %s 无法连入源库 %s:%s：%s。"
            "请确认源库该账号口令正确、且允许从目标 %s 连入（如 @'%%'）；"
            "若源库残留同名旧账号，本步骤已用 ALTER USER 强制同步口令，可重跑本步骤。"
            % (m_user, cfg["src_host"], cfg["src_port"], e, cfg["tgt_host"]))
    conn = connect(*tgt, timeout=8)
    try:
        cur = conn.cursor()
        # 桥接忽略系统库与 Custos 自有库（best-effort，动态全局变量）
        try:
            cur.execute("SET GLOBAL replicate_ignore_db='mysql,ha_local'")
        except Exception:
            pass
        # 目标必须把「从源收到的更新」写进自己的 binlog，否则下游第二节点（如 213）
        # 永远拿不到迁移过来的数据。
        # ⚠ MariaDB 只有 @@log_slave_updates（SELECT @@log_replica_updates 报 1193），
        #   此前写成 log_replica_updates 会被静默吞掉 ⇒ 第二节点数据落后却不报错。
        #   已修正为 MariaDB 正确名，并保留 MySQL 形态兜底。
        try:
            cur.execute("SET GLOBAL log_slave_updates=ON")
        except Exception:
            try:
                cur.execute("SET GLOBAL log_replica_updates=ON")
            except Exception:
                pass
        # 幂等：本步骤允许重复执行（第三步可重跑「建立桥接」）。
        # 若目标上已有 replica 线程在运行，CHANGE MASTER TO 会被拒绝
        # （MariaDB 要求先停止 replica 线程），故先 STOP REPLICA 再重建通道。
        # 无复制通道时 STOP REPLICA 也会报 “not configured”，忽略即可。
        try:
            cur.execute("STOP REPLICA")
        except Exception:
            try:
                cur.execute("STOP SLAVE")
            except Exception:
                pass
        # ⚠ 必须按目标【真实 flavour】生成挂载 SQL，且 MariaDB 分支必须带 MASTER_SSL=0。
        #   v2.4.0 追平测试实测故障：此前硬编码 MariaDB 语法（MASTER_USE_GTID=slave_pos）
        #   且未关 SSL ⇒ 从库以「要求 SSL」的方式连源（SHOW REPLICA STATUS 显示
        #   Master_SSL_Allowed=Yes），而源库 have_ssl=DISABLED ⇒ IO 线程永久 Connecting：
        #     error connecting to master 'replication@x:3306' ...
        #     message: SSL connection error: SSL is required, but the server does not support it
        #   内网复制无需 TLS，显式关闭即可根治；MySQL 8 目标则走 SOURCE_AUTO_POSITION=1
        #   （并索取服务端公钥，规避 caching_sha2_password 非 TLS 握手失败）。
        _flav = detect_flavour(cur)
        cur.execute(change_replication_sql(_flav, cfg["src_host"], int(cfg["src_port"]),
                                            m_user, m_pass, public_key=(_flav == F_MYSQL8)))
        cur.execute(start_replication_sql(_flav))
        # 进度上报：让前端桥接阶段不再“卡住”（BUG-004）
        _tick(task, 60, stage="复制桥接已启动，目标正在从源追平（MariaDB GTID）")
        note = ("桥接已建立，目标正在从源追平（MariaDB GTID）；"
                "已暂停该目标节点的 Custos 自动拓扑维护，避免桥接被 monitor 清除")
        if use_src:
            note += "（使用源账号本身作为复制账号）"
        _tick(task, 100, stage="桥接完成（持续追平中，可随时做一致性校验）")
        return {"ok": True, "note": note, "used_src_creds": use_src}
    finally:
        conn.close()


def verify(task, deep=False):
    """数据一致性校验：确认源库数据已【完整】复制到新集群主库。

    判据分三层，逐层加强，任一层不达标都会在结果里逐条列出便于定位：
      ① 表级存在性：源库有、目标没有 ⇒ 数据没迁全（最关键，缺表必须暴露）；
      ② 表级行数：逐表 COUNT(*) 比对（快速一致性判据）；
      ③ 表级内容：CHECKSUM TABLE 比对（deep=True 才做；较慢，且跨大版本 / 不同
         引擎行格式可能产生差异，故仅作参考，不推翻 ①② 的结论）。

    ⚠ 为什么不能只回一个 "ok"：用户要看到「哪些表一致、哪些不一致」才敢切换；
      此前只返回行数有差异的表名列表，且前端直接 JSON 打印整体结果，用户难以
      判断数据到底迁没迁全（缺表还会直接抛异常中断整个校验）。
    """
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"])
    databases = cfg.get("databases") or []
    s_conn = connect(*src, timeout=8)
    t_conn = connect(*tgt, timeout=8)
    tables, missing, diffs = [], [], []
    ck_checked = ck_mismatch = ck_unsupported = 0
    try:
        for db in databases:
            def _tables(conn, dbname):
                cur = conn.cursor()
                cur.execute(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (dbname,))
                return set(x[0] for x in cur.fetchall())

            s_tbls = _tables(s_conn, db)
            try:
                t_tbls = _tables(t_conn, db)
            except Exception as e:
                # 目标整库读不到 ⇒ 该库所有表都算缺失，并如实记录原因
                t_tbls = set()
                missing.append({"db": db, "table": "*",
                                "reason": "读取目标库表清单失败：%s" % e})

            # ① 缺表检测：目标库还没有 / 拼错库名等情况在此暴露，不再让 COUNT 抛异常中断
            for tbl in sorted(s_tbls - t_tbls):
                missing.append({"db": db, "table": tbl, "reason": "目标库中不存在该表"})

            for tbl in sorted(s_tbls & t_tbls):
                q = "SELECT COUNT(*) FROM `%s`.`%s`" % (db, tbl)
                row = {"db": db, "table": tbl, "rows_src": None, "rows_tgt": None,
                       "rows_ok": False, "note": ""}
                try:
                    cs = s_conn.cursor(); cs.execute(q); row["rows_src"] = cs.fetchone()[0]
                    ct = t_conn.cursor(); ct.execute(q); row["rows_tgt"] = ct.fetchone()[0]
                    row["rows_ok"] = (row["rows_src"] == row["rows_tgt"])
                except Exception as e:
                    row["note"] = "行数比对失败：%s" % e
                if not row["rows_ok"]:
                    diffs.append({"db": db, "table": tbl, "src": row["rows_src"],
                                  "tgt": row["rows_tgt"], "reason": row["note"]})
                # ③ 内容级校验（可选，逐表 CHECKSUM）
                if deep:
                    try:
                        cq = "CHECKSUM TABLE `%s`.`%s`" % (db, tbl)
                        cs = s_conn.cursor(); cs.execute(cq); r1 = cs.fetchone()
                        ct = t_conn.cursor(); ct.execute(cq); r2 = ct.fetchone()
                        row["ck_src"] = list(r1)[-1] if r1 else None
                        row["ck_tgt"] = list(r2)[-1] if r2 else None
                        if row["ck_src"] is None and row["ck_tgt"] is None:
                            # CHECKSUM TABLE 在部分引擎/版本上对 InnoDB 直接返回 NULL。
                            # 两侧同为 NULL 绝不能判为「一致」——那等于对不支持的表谎报成功，
                            # 必须标记为「不可用」，让用户知道该表没被内容级校验覆盖。
                            row["ck_ok"] = None
                            row["ck_note"] = "该表不支持内容校验（CHECKSUM 返回空）"
                            ck_unsupported += 1
                        else:
                            row["ck_ok"] = (row["ck_src"] == row["ck_tgt"])
                            ck_checked += 1
                            if not row["ck_ok"]:
                                ck_mismatch.append({"db": db, "table": tbl,
                                                    "src": row["ck_src"], "tgt": row["ck_tgt"]})
                    except Exception as e:
                        row["ck_ok"] = None
                        row["ck_note"] = "内容校验不可用：%s" % e
                        ck_unsupported += 1
                tables.append(row)
    finally:
        s_conn.close(); t_conn.close()

    # 复制状态（MariaDB：SHOW REPLICA STATUS，兼容 Replica_/Slave_ 两种列名）
    repl = _replica_status(tgt)
    ok_n = sum(1 for t in tables if t["rows_ok"])
    consistent = (not diffs) and (not missing)
    if consistent:
        summary = "已校验 %d 张表，行数全部一致" % ok_n
    else:
        summary = ("共 %d 张表：%d 张一致，%d 张行数不一致，%d 张目标缺失"
                   % (len(tables) + len(missing), ok_n, len(diffs), len(missing)))
    if deep:
        summary += "；内容级校验 " + ("全部一致" if (ck_checked and not ck_mismatch)
                                 else ("存在差异 %d 处" % len(ck_mismatch) if ck_mismatch
                                       else "不可用"))
    return {
        "ok": True,
        "deep": bool(deep),
        "tables_checked": len(tables),
        "tables_ok": ok_n,
        "diffs": diffs,
        "missing": missing,
        "consistent": consistent,
        "tables": tables,
        "checksum": {"enabled": bool(deep), "checked": ck_checked,
                     "mismatch": ck_mismatch, "unsupported": ck_unsupported},
        "replica": repl,
        "summary": summary,
        "note": summary,
    }


def _replica_status(tgt):
    conn = connect(*tgt, timeout=8)
    try:
        # ha_client.connect 默认是元组游标，SHOW REPLICA STATUS 需按列名取，改用 DictCursor
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SHOW REPLICA STATUS")
        row = cur.fetchone()
        if not row:
            return {"running": False, "note": "无复制通道（尚未建立桥接或已停桥）"}
        # 列名兼容：MariaDB 10.5+ 用 Replica_*，旧别名 Slave_*。
        # ⚠ 此前 io_running / sql_running 都调用同一个 _col("Replica")，而 _col 遍历
        #   (*_IO_Running, *_SQL_Running) 时第一个命中的恒为 IO ⇒ 两个字段实际都取到
        #   IO 线程状态，**SQL 线程状态从未被正确读取**（一致性校验展示失真，
        #   且停桥前追平检测会误判为已追平）。已改为按后缀精确取值。
        def _pick(suffix):
            for p in ("Replica", "Slave"):
                k = f"{p}_{suffix}"
                if k in row:
                    return row[k]
            return None
        return {
            "io_running": _pick("IO_Running"),
            "sql_running": _pick("SQL_Running"),
            "seconds_behind": row.get("Seconds_Behind_Master", row.get("Seconds_Behind_Source")),
            "last_error": row.get("Last_SQL_Error") or row.get("Last_Error"),
        }
    finally:
        conn.close()


def _register_target_node(cluster_id, host, port, root_pass, node_type, mysql_version,
                          root_user="root"):
    """把已装好的目标节点登记进 Custos（插入 nodes/hosts），不重新生成部署文件。"""
    # mysql_version 可能为 SELECT VERSION() 原文，截断到 48 与列宽一致，防 1406。
    mysql_version = (mysql_version or None) and mysql_version[:48]
    server_id = node_gen.get_next_server_id()
    host_compose = f"/home/docker/{host}"
    execute(
        "INSERT INTO nodes (ip,port,root_user,root_pass,server_id,compose_path,cluster_id,node_type,mysql_version) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE port=%s,root_user=%s,root_pass=%s,server_id=%s,compose_path=%s,cluster_id=%s,node_type=%s,mysql_version=%s",
        (host, port, root_user, crypto.encrypt(root_pass), server_id, host_compose, cluster_id, node_type, mysql_version,
         port, root_user, crypto.encrypt(root_pass), server_id, host_compose, cluster_id, node_type, mysql_version),
    )
    execute(
        "INSERT INTO hosts (host,port,role,status,initial_role,cluster_id) "
        "VALUES (%s,%s,'unknown','down',%s,%s) "
        "ON DUPLICATE KEY UPDATE initial_role=%s,cluster_id=%s",
        (host, port, "slave", cluster_id, "slave", cluster_id),
    )


def cutover(task):
    """停桥（STOP REPLICA + RESET REPLICA ALL）→ 凭证对齐 → 登记进 Custos（monitor 接管提升）。

    集群归属策略（修复 BUG-001/002）：若目标节点（203）已在某 Custos 集群中，
    则【沿用】该集群，绝不再新建只含 203 的单节点集群把节点从原集群拽出；
    仅当目标尚未登记时才新建集群。同集群的其余节点（如 213/33）归属不受影响。
    """
    cfg = task["config"]
    tgt_host = cfg["tgt_host"]
    tgt_port = int(cfg["tgt_port"])
    tgt = (tgt_host, tgt_port, _tgt_user(cfg), cfg["tgt_root_pass"])
    conn = connect(*tgt, timeout=8)
    _real_version = None
    try:
        cur = conn.cursor()
        cur.execute("STOP REPLICA")
        cur.execute("RESET REPLICA ALL")  # MariaDB：清除桥接通道，避免 monitor 误判
        # ⚠ 登记前必须【在线探测真实版本】：v2.4.0 实测出现过 nodes 表登记为
        #   docker / 8.0.46，而目标真实是 MariaDB 12.3.3 —— 此后 ha_client /
        #   mysql_compat 会按 MySQL 8 方言生成 SQL（CHANGE REPLICATION SOURCE TO、
        #   caching_sha2 等），在 MariaDB 上全部报错 ⇒ 复制与自愈长期失效且极难定位。
        #   故以 SELECT VERSION() 为准，向导里选的形态只作兜底。
        try:
            cur.execute("SELECT VERSION()")
            _real_version = (cur.fetchone() or [None])[0]
        except Exception:
            pass
    finally:
        conn.close()
    node_type = cfg.get("node_type") or "mariadb-docker"
    mysql_version = _real_version or cfg.get("tgt_version") or None
    if _real_version:
        _is_maria = "mariadb" in str(_real_version).lower()
        if _is_maria and node_type in ("docker", "mysql8", "mysql8-docker"):
            node_type = "mariadb-docker" if str(node_type).endswith("docker") else "mariadb"
        elif (not _is_maria) and node_type in ("mariadb", "mariadb-docker"):
            node_type = "mysql8-docker" if str(node_type).endswith("docker") else "mysql8"
    # 复用已有集群：避免把目标节点从原集群拽出（BUG-001）
    existing = query("SELECT cluster_id FROM nodes WHERE ip=%s AND port=%s",
                    (tgt_host, tgt_port))
    reused = bool(existing and existing[0].get("cluster_id"))
    if reused:
        cluster_id = existing[0]["cluster_id"]
    else:
        # 此刻才建集群：create_cluster 生成 Custos 托管的 ha/repl/app 口令
        cluster_id = create_cluster(name=task["name"], node_type=node_type,
                                   mysql_version=mysql_version)
    # 关键：把目标节点账号刷成 Custos 托管口令（并补齐 ha_local 库），
    # 否则 monitor 接管后因账号口令不匹配而连不上节点。
    _align_target_creds(cluster_id, cfg)
    # 登记节点 + 主机（此刻才让 ensure_topology 提升新主、挂第二节点）。
    # 若目标已在某集群，这里只是原地更新 203 一条记录，不改其它节点（213/33 等）归属。
    _register_target_node(cluster_id, tgt_host, tgt_port, cfg["tgt_root_pass"],
                         node_type, mysql_version,
                         root_user=_tgt_user(cfg))
    # 桥接已在上面 STOP REPLICA + RESET REPLICA ALL 停掉，可解除迁移锁定，
    # 把目标交还给 monitor 正常接管（否则该集群的拓扑维护会一直被跳过）。
    unlock_target(tgt_host, tgt_port)
    # ⚠ 物理通道（mariabackup copy-back）的固有后果：数据是【直接替换数据目录】落地的，
    #   **完全不经过 binlog** ⇒ 同集群其余节点（如 213）永远复制不到这批数据；
    #   更隐蔽的是 copy-back 还会把源库 GTID 集合（如 0-21-47）写进目标的
    #   gtid_current_pos，从库连上后直接"继承"该 GTID ⇒ SHOW REPLICA STATUS 显示
    #   IO/SQL=Yes、Seconds_Behind_Master=0（看起来已追平），**但库/表根本不存在**
    #   （v2.4.0 实测：203 有 piyaotest 47 张表，213 连库都没有，复制却显示健康）。
    #   故物理通道切换后，必须把同集群其余节点【在线重建】一次（逻辑全量 + 重链复制）。
    peers = []
    try:
        rows = query("SELECT ip, port FROM nodes WHERE cluster_id=%s AND NOT (ip=%s AND port=%s)",
                     (cluster_id, tgt_host, tgt_port))
        peers = [(r["ip"], int(r["port"])) for r in (rows or [])]
    except Exception:
        peers = []
    rebuilt = []
    if (cfg.get("channel") == "physical") and peers:
        if str(get_setting("AUTO_REBUILD_AFTER_MIGRATION", "1")).strip() == "1":
            try:
                from . import rebuild as _rebuild
                for h, p in peers:
                    try:
                        _rebuild.start_rebuild(cluster_id, h, p, actor="auto:migration-physical")
                        rebuilt.append("%s:%s" % (h, p))
                    except Exception as _e:
                        print("[migration] 自动重建 %s:%s 失败：%s" % (h, p, _e))
            except Exception as _e:
                print("[migration] 加载 rebuild 模块失败：%s" % _e)
    if reused:
        note = ("已停桥、对齐凭证并登记进 Custos（沿用既有集群，新主不会被移出）；"
                "请在外部手动切换 VIP")
    else:
        note = ("已停桥、对齐凭证并登记进 Custos，monitor 将自动提升新主；"
                "请在外部手动切换 VIP")
    if (cfg.get("channel") == "physical") and peers:
        note += ("。⚠ 物理通道恢复的数据不经 binlog，同集群其余节点 %s 不会自动获得数据："
                 "已%s。请在「节点」页确认其重建完成（大库耗时较长）。"
                 % ("、".join("%s:%s" % (h, p) for h, p in peers),
                    ("自动触发在线重建：" + "、".join(rebuilt)) if rebuilt
                    else "请对其手动执行「从库在线重建」"))
    return {"ok": True, "cluster_id": cluster_id, "reused_cluster": reused, "note": note,
            "peers_need_rebuild": ["%s:%s" % (h, p) for h, p in peers]
            if cfg.get("channel") == "physical" else [],
            "peers_rebuild_started": rebuilt}


def _align_target_creds(cluster_id, cfg):
    """将目标节点账号对齐为 Custos 托管口令（ha_monitor / replication / app / root），
    并补齐 ha_local 库——确保 monitor 接管后能连上并管理该节点（认领非重置）。

    复用 node_gen 的 MariaDB 修复脚本（与 Install-ha-mariadb-Node.sh 同源），
    其同时创建 ha_user@'%'（覆盖管理器任意源 IP）、root@'127.0.0.1'（keepalived 探活）等。
    """
    creds = get_cluster_creds(cluster_id)
    manager_ip = get_mgmt_host() or "127.0.0.1"
    sql = node_gen.render_fix_node_creds_sql_mariadb(
        creds["ha_user"], creds["ha_pass"], creds["repl_user"], creds["repl_pass"],
        _tgt_user(cfg), cfg["tgt_root_pass"], manager_ip, int(cfg["tgt_port"]),
    )
    conn = connect(cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"], timeout=8)
    try:
        cur = conn.cursor()
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s and not s.lstrip().startswith("--"):
                cur.execute(s)
        conn.commit()
    finally:
        conn.close()


def cleanup(task):
    """回收源端桥接复制账号（最小权限，可收回）+ 老主置只读暖备。"""
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    conn = connect(*src, timeout=8)
    done, failed = [], []
    try:
        cur = conn.cursor()
        # ❗此前两项都 try/except 静默吞掉，非 root 账号时会「谎报成功」：
        #   界面显示已收尾，实际账号没回收、老主也没置只读。故逐项记录并如实上报。
        try:
            cur.execute(f"DROP USER IF EXISTS '{cfg['repl_user']}'@'%'")
            done.append("已回收源端复制账号 '%s'@'%%'" % cfg["repl_user"])
        except Exception as e:
            failed.append("回收复制账号失败：%s（需 CREATE USER 权限）" % e)
        try:
            cur.execute("SET GLOBAL read_only=ON")  # 老主置只读暖备，防误写
            done.append("已把老主置为只读(read_only=ON)")
        except Exception as e:
            failed.append("置老主只读失败：%s（需 SUPER 权限）" % e)
    finally:
        conn.close()
    note = "；".join(done) if done else "未执行任何操作"
    if failed:
        note += "。⚠ 未完成项：" + "；".join(failed)
    # ok=True：收尾不是迁移成功的必要条件（数据已迁完），但必须如实告知未完成项。
    return {"ok": True, "done": done, "failed": failed, "partial": bool(done and failed),
            "note": note}


# ---------------------------------------------------------------------------
# 停桥前追平检测 + 跳过收尾
# ---------------------------------------------------------------------------
# 追平阈值（秒）：停桥意味着切断 177→203 的桥接，任何残留延迟都会变成永久数据丢失，
# 故取 0 —— 必须 Seconds_Behind_Master 收敛到 0 才允许切换。
LAG_CAUGHT_UP_SEC = 0


def check_lag(task, skip_acct=False):
    """停桥切换前的「追平检测」：确认目标已从源追平，避免带差切换导致数据丢失。

    纯只读，不改动任何节点。返回 caught_up 供前端放行「执行停桥切换」按钮。
    skip_acct=True 时跳过「复制账号自检」（用于高频追踪采样，避免反复连源库）。
    """
    cfg = task["config"]
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg), cfg["tgt_root_pass"])
    try:
        repl = _replica_status(tgt)
    except Exception as e:
        return {"ok": False, "caught_up": False, "error": f"{type(e).__name__}: {e}"}
    if repl.get("note"):
        # 目标上没有复制通道。仅回一句“不存在”会让人不知道下一步做什么，
        # 故用任务步骤状态区分三种情况，分别给出可操作指引。
        steps = task.get("steps") or {}
        bridge_done = (steps.get("bridge") or {}).get("status") == "done"
        cutover_done = (steps.get("cutover") or {}).get("status") == "done"
        if cutover_done:
            why = "已停桥"
            hint = ("本任务已执行过停桥切换（STOP REPLICA + RESET REPLICA ALL 会清除复制通道），"
                    "此时再检测追平没有意义。若需重新迁移，请新建迁移任务。")
        elif not bridge_done:
            why = "未建立桥接"
            hint = ("需要重做【第三步 → 「建立桥接」】这一步："
                    "回到第三步，点击「建立桥接」那一行的【执行】按钮（支持重复执行），"
                    "成功后目标才会建立 177→203 的复制通道，再回来点「追平检测」。"
                    "若你的源库已停写、不需要追平增量，可勾选下方「源库已停写」跳过本检测。")
        else:
            why = "复制通道已清除"
            hint = ("需要重做【第三步 → 「建立桥接」】这一步（桥接执行过，但目标上的复制通道现在不存在）。"
                    "回到第三步重跑「建立桥接」即可重建通道。"
                    "若目标已被登记进 Custos 集群，monitor 的 ensure_topology 会持续清掉该通道，"
                    "此时应新建迁移任务，而不是重跑桥接。")
        return {"ok": True, "caught_up": False, "io_running": None, "sql_running": None,
                "seconds_behind": None, "last_error": None,
                "reason": "%s：目标上没有复制通道" % why, "why": why, "hint": hint,
                "bridge_done": bridge_done, "cutover_done": cutover_done,
                "need_bridge": (not cutover_done),  # 未停桥时，重跑桥接即可恢复通道
                "replica": repl}
    io_raw = repl.get("io_running")
    sql_raw = repl.get("sql_running")
    io_ok = str(io_raw or "").lower() == "yes"
    sql_ok = str(sql_raw or "").lower() == "yes"
    sb = repl.get("seconds_behind")
    last_err = repl.get("last_error")

    # 附加自检：用复制账号从【管理端】连一次源库。
    # 目的：区分「账号/口令或授权问题」与「203 → 177 网络不通」——两者表现都是 IO 起不来。
    steps_prev = task.get("steps") or {}
    bres = (steps_prev.get("bridge") or {}).get("result") or {}
    use_src = bool(bres.get("used_src_creds"))
    m_user = cfg["src_user"] if use_src else cfg["repl_user"]
    m_pass = cfg["src_pass"] if use_src else cfg["repl_pass"]
    # skip_acct：前端「追平追踪」每 2s 采样一次，若每次都连源库做账号自检，
    # 5 分钟就是上百次连接，对老主是无谓负担 ⇒ 仅首次采样做自检，后续跳过。
    if skip_acct:
        acct_ok, acct_err, m_user = None, "", m_user
    else:
        acct_ok, acct_err = True, ""
        try:
            _c = connect(cfg["src_host"], int(cfg["src_port"]), m_user, m_pass, timeout=8)
            _c.close()
        except Exception as e:
            acct_ok, acct_err = False, str(e)

    hint = ""
    if not sql_ok:
        reason = "SQL 线程未运行（状态=%s，复制未工作）" % (sql_raw or "未知")
    elif not io_ok:
        le = str(last_err or "")
        if "1236" in le or "binary log is not open" in le.lower():
            # 1236 的根因是源库没开 binlog，不能笼统说成“口令/授权不对”误导排障
            reason = ("源库 %s:%s 未开启 binlog，IO 线程报 1236 'Binary log is not open'"
                      "（无从拉取 binlog 事件）——这与账号口令无关"
                      % (cfg["src_host"], cfg["src_port"]))
        else:
            state = str(io_raw or "未知")
            if state.lower() == "connecting":
                reason = ("IO 线程仍在重试连源（状态=Connecting，MariaDB 默认每 60s 重试一次）："
                          "通常是复制账号口令/授权不对，或 %s 无法访问 %s:%s"
                          % (cfg["tgt_host"], cfg["src_host"], cfg["src_port"]))
            else:
                reason = "IO 线程未运行（状态=%s），未从源拉取 binlog" % state
            if last_err:
                reason += "；复制错误：" + le[:300]
        if "1236" in le or "binary log is not open" in le.lower():
            hint = ("源库 %s:%s 未开启 binlog，IO 线程报 1236 'Binary log is not open'。\n"
                    "【方案① 想继续桥接追平】源库 my.cnf 的 [mysqld] 增加 "
                    "server_id=<非0唯一值>、log_bin=/path/to/mysql-bin、binlog_format=ROW "
                    "并【重启 MariaDB】，再回第三步重跑「建立桥接」。\n"
                    "【方案② 不方便重启源库】走停写迁移：源库停写 → 只跑「数据种子」→ "
                    "勾选下方「源库已停写」跳过追平 → 直接停桥切换。"
                    % (cfg["src_host"], cfg["src_port"]))
        elif "SSL" in le.upper() or "2026" in le:
            # 专项：SSL 握手类失败（典型的 "SSL connection error: SSL is required,
            # but the server does not support it"，errno 2026）。它与账号口令完全无关，
            # 笼统提示「口令/授权不对」会把排错带偏（v2.4.0 实测就是这样误导的）。
            hint = ("复制通道被建成了「要求 SSL」，但源库 %s:%s 未启用 SSL"
                    "（have_ssl=DISABLED）⇒ IO 线程握手失败(2026)。\n"
                    "请重跑第三步「建立桥接」：新版会以 MASTER_SSL=0 重建通道"
                    "（内网复制无需 TLS）。\n"
                    "若确实要加密复制，请先在源库启用 SSL（配置 ssl_cert/ssl_key 并重启），"
                    "再重跑「建立桥接」。"
                    % (cfg["src_host"], cfg["src_port"]))
        elif not acct_ok:
            hint = ("复制账号 %s 从管理端连源库 %s:%s 失败：%s。"
                    "⇒ 账号口令或 host 授权有问题。请重跑第三步「建立桥接」"
                    "（会用 ALTER USER 强制把该账号口令同步为当前任务口令），"
                    "并确认源库允许目标 %s 连入（@'%%'）。"
                    % (m_user, cfg["src_host"], cfg["src_port"], acct_err, cfg["tgt_host"]))
        else:
            hint = ("复制账号从管理端能连上源库，但目标 %s 连不上 ⇒ 请检查 %s → %s:%s 的网络连通"
                    "（防火墙 / 端口），以及源库是否允许来自 %s 的连接。"
                    % (cfg["tgt_host"], cfg["tgt_host"], cfg["src_host"], cfg["src_port"],
                       cfg["tgt_host"]))
    elif last_err:
        reason = "存在复制错误：" + str(last_err)[:300]
    elif sb is None:
        reason = "无法获取延迟（Seconds_Behind_Master 为空，复制可能未运行）"
    else:
        try:
            lag = int(sb)
        except Exception:
            lag = 0
        reason = "" if lag <= LAG_CAUGHT_UP_SEC else \
            "尚未追平，仍有 %d 秒延迟（阈值 %ds）" % (lag, LAG_CAUGHT_UP_SEC)
    return {"ok": True, "caught_up": not reason,
            "io_running": io_raw, "sql_running": sql_raw, "seconds_behind": sb,
            "last_error": last_err,
            "acct_ok": acct_ok, "acct_error": acct_err, "acct_user": m_user,
            "reason": reason or "已追平（Seconds_Behind_Master=%s）" % sb,
            "hint": hint, "replica": repl}


# 停写检测采样间隔（秒）：连源库采样两次写入计数器，比较差值。
FREEZE_SAMPLE_SEC = 3


def audit_source_privileges(cfg):
    """源库账号「权限体检」：列出迁移各步骤依赖的权限是否具备，缺了会影响什么。

    目的：迁移大量操作依赖源账号权限（探测/种子/校验需 SELECT，桥接建账号需 CREATE USER，
    停写检测需 PROCESS，收尾置只读需 SUPER）。非 root 时若静默失败会误导用户，
    故在第一步就把「缺什么、影响哪一步、怎么补」讲清楚。
    """
    conn = connect(cfg["src_host"], int(cfg["src_port"]),
                   cfg["src_user"], cfg["src_pass"], timeout=8)
    try:
        cur = conn.cursor()
        who = grants = ""
        try:
            cur.execute("SELECT CURRENT_USER()")
            r = cur.fetchone()
            who = r[0] if r else ""
        except Exception:
            pass
        try:
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            grants = "\n".join(str(x[0]) for x in (cur.fetchall() or []))
        except Exception:
            grants = ""
        g = (grants or "").upper()
        has_all = ("ALL PRIVILEGES" in g) or ("GRANT ALL" in g)

        def _has(p):
            return has_all or (p in g)

        def _ok(sql):
            try:
                cur.execute(sql)
                cur.fetchone()
                return True
            except Exception:
                return False

        sel_ok = _ok("SELECT schema_name FROM information_schema.schemata LIMIT 1")
        proc_ok = False
        try:
            cur.execute("SHOW GLOBAL STATUS LIKE %s", ("Com_insert",))
            r = cur.fetchone()
            proc_ok = bool(r and len(r) > 1 and r[1] is not None)
        except Exception:
            proc_ok = False
        repl_cli_ok = _ok("SHOW REPLICA STATUS")
        checks = [
            {"name": "读取库/表（SELECT）", "ok": sel_ok,
             "used": "第一步探测、数据种子、一致性校验",
             "fix": "授予待迁移库的 SELECT"},
            {"name": "SHOW GLOBAL STATUS（PROCESS）", "ok": proc_ok,
             "used": "停写检测",
             "fix": "GRANT PROCESS ON *.*；否则停写检测会判为「无法判定」而不会误报已停写"},
            {"name": "读复制状态（REPLICATION CLIENT）", "ok": repl_cli_ok,
             "used": "读取 GTID / 复制状态",
             "fix": "GRANT REPLICATION CLIENT ON *.*"},
            {"name": "建复制账号（CREATE USER）", "ok": _has("CREATE USER"),
             "used": "建立桥接（在源库建复制账号）",
             "fix": "GRANT CREATE USER ON *.*；无此权限时自动降级为复用源账号本身（须允许从目标连入，通常 @'%'）"},
            {"name": "置全局变量（SUPER / SET GLOBAL）", "ok": _has("SUPER"),
             "used": "收尾回收（把老主置为只读）",
             "fix": "GRANT SUPER ON *.*；无此权限该步会失败，现已如实上报、不会谎报成功"},
        ]
        return {"ok": True, "user": who, "grants": grants, "checks": checks,
                "missing": [c["name"] for c in checks if not c["ok"]]}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        conn.close()


def audit_target_privileges(cfg):
    """目标库账号「分层权限体检」——把"能导数据"和"能接管"分开讲清楚。

    为什么要分层（用户明确要求：不能啰嗦，但要说清，且给出「建议 root」）：
      目标端各阶段对权限的要求【完全不同】——只做数据导入时 CREATE/INSERT 就够，
      但"桥接追平 / 注册为 Custos 节点 / 提升新主 / 收尾置只读"都需要全局级权限
      （REPLICATION SLAVE、CREATE USER+GRANT ALL、SUPER，MariaDB 10.11+ 还需
      READ_ONLY ADMIN）。若不做分层，用户填个非 root 跑到第 4 步才炸，等于白跑一轮。

    返回 levels：每一项 = 一个迁移阶段是否可用 + 缺什么，前端按"可用/不可用"分层展示。
    """
    host, port = cfg["tgt_host"], int(cfg["tgt_port"])
    user = _tgt_user(cfg)
    pwd = cfg.get("tgt_root_pass") or ""
    conn = connect(host, port, user, pwd, timeout=8)
    try:
        cur = conn.cursor()
        who = ""
        try:
            cur.execute("SELECT CURRENT_USER()")
            r = cur.fetchone()
            who = r[0] if r else ""
        except Exception:
            pass
        grants = ""
        try:
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            grants = "\n".join(str(x[0]) for x in (cur.fetchall() or []))
        except Exception:
            grants = ""
        g = (grants or "").upper()
        has_all = ("ALL PRIVILEGES" in g) or ("GRANT ALL" in g)

        def _has(p):
            return has_all or (p in g)

        def _ok(sql):
            try:
                cur.execute(sql)
                cur.fetchone()
                return True
            except Exception:
                return False

        # 只读探测：能否建库（导入前提）
        create_ok = _has("CREATE") or _has("ALL PRIVILEGES")
        insert_ok = _has("INSERT") or _has("ALL PRIVILEGES")
        repl_ok = _has("REPLICATION SLAVE")
        create_user_ok = _has("CREATE USER")
        super_ok = _has("SUPER")
        # MariaDB 10.11+ 提升主库需 READ_ONLY ADMIN（见 mariadb_matrix.py）
        ro_admin_ok = _has("READ_ONLY ADMIN") or _has("SUPER")

        levels = [
            {"stage": "数据导入", "ok": bool(create_ok and insert_ok),
             "need": "CREATE、INSERT（能建库建表写数据）"},
            {"stage": "桥接追平", "ok": bool(repl_ok),
             "need": "REPLICATION SLAVE"},
            {"stage": "注册为 Custos 节点", "ok": bool(create_user_ok and has_all),
             "need": "CREATE USER + GRANT ALL（建 ha_monitor / 复制账号等）"},
            {"stage": "提升新主 / 停桥切换", "ok": bool(super_ok and ro_admin_ok),
             "need": "SUPER（MariaDB 10.11+ 还需 READ_ONLY ADMIN）"},
        ]
        missing = [l["stage"] for l in levels if not l["ok"]]
        return {"ok": True, "user": who or user, "is_root": (user or "root") == "root",
                "levels": levels, "missing": missing,
                "advice": "建议 root" if missing else "权限齐全"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        conn.close()


def plan_binlog_enable(cfg):
    """源库未开 binlog 时，用已有的 root 连接把「开启方案」的准备工作做到位。

    ⚠ 为什么不能直接用 root 打开 binlog（重要，别再提这个方案）：
      `log_bin` 是【静态变量】，MySQL/MariaDB 均不支持 SET GLOBAL log_bin=ON 在线开启，
      必须修改配置文件并重启 mysqld 进程。而向导拿到的是**数据库 root**（只能执行 SQL），
      既改不了 my.cnf（文件系统），也重启不了 mysqld（进程管理）——除非另给源机的
      SSH 凭据。且自动修改生产库配置并重启属极高危操作（业务中断 + binlog 磁盘规划
      需人工决策），绝不应自动执行。故这里只做「探测 + 生成可直接照做的方案」。
    """
    conn = connect(cfg["src_host"], int(cfg["src_port"]),
                   cfg["src_user"], cfg["src_pass"], timeout=8)
    try:
        cur = conn.cursor()

        def _v(sql):
            try:
                cur.execute(sql)
                r = cur.fetchone()
                return r[0] if r else None
            except Exception:
                return None

        ver = _v("SELECT VERSION()")
        datadir = _v("SELECT @@datadir")
        basedir = _v("SELECT @@basedir")
        server_id = _v("SELECT @@server_id")
        log_bin = _v("SELECT @@log_bin")
        replicas = []
        try:
            cur.execute("SHOW SLAVE HOSTS")
            replicas = list(cur.fetchall() or [])
        except Exception:
            pass
        is_slave = False
        try:
            cur.execute("SHOW REPLICA STATUS")
            is_slave = bool(cur.fetchone())
        except Exception:
            pass
    finally:
        conn.close()

    # ---- 目标 server_id：源与目标的 server_id 必须不同，否则复制会因 id 相同直接失败 ----
    tgt_sid = None
    try:
        if cfg.get("tgt_host"):
            tc = connect(cfg["tgt_host"], int(cfg.get("tgt_port") or 3306), _tgt_user(cfg),
                         cfg.get("tgt_root_pass") or "", timeout=5)
            try:
                tcur = tc.cursor()
                tcur.execute("SELECT @@server_id")
                tr = tcur.fetchone()
                tgt_sid = int(tr[0]) if tr else None
            finally:
                tc.close()
    except Exception:
        tgt_sid = None

    # ---- 部署形态推断：容器化 MariaDB 的宿主机 /etc/my.cnf 常常【并不生效】----
    # 官方镜像版本串形如 10.11.17-MariaDB-ubu2204-log（带发行版后缀、无发行包修订号 ~）；
    # 发行版包安装的形如 10.11.17-MariaDB-1:10.11.17+maria~ubu2204（含 ~）。据此粗判。
    _ver = str(ver or "")
    container_like = ("~" not in _ver) and any(
        k in _ver for k in ("-ubu", "-ubi", "-deb", "-alma", "-rocky", "-centos", "-fedo"))

    try:
        sid = int(server_id)
    except Exception:
        sid = 0
    base = str(datadir or "/var/lib/mysql").rstrip("/")
    need_lsu = bool(is_slave or replicas)
    # server_id：非 0，且与目标不冲突
    final_sid = sid if sid else 1001
    if tgt_sid is not None and final_sid == tgt_sid:
        final_sid = (tgt_sid + 11) or 1001
    snippet = "\n".join([
        "[mysqld]",
        "server_id      = %d" % final_sid,
        "log_bin        = %s/mysql-bin" % base,
        "binlog_format  = ROW",
    ] + (["log_slave_updates = ON   # 该库自身还挂着从库，需把上游变更写进自己的 binlog"]
         if need_lsu else [])
      + ["expire_logs_days = 7       # 按需调整，避免 binlog 占满磁盘（MariaDB 10.6+ 亦可用 binlog_expire_logs_seconds）"])
    steps = [
        "1) 先在源机上定位「真正生效」的配置文件（命令见 config_locator）——容器化部署时"
        "宿主机 /etc/my.cnf 往往并不生效，必须以实际挂载 / mysqld 实际读取的那份为准。",
        "2) 备份该文件后，把下面的片段加进其 [mysqld] 段（建议追加到文件末尾，"
        "可避免被 include 的同名项覆盖）。",
        "3) 确认磁盘空间充足（binlog 会持续增长，务必设置过期清理）。",
        "4) 重启 MariaDB（容器：docker restart <容器名>；系统服务：systemctl restart mariadb）。",
        "5) 回本向导重新探测；确认 log_bin=1 后会自动切回【桥接模式】。",
    ]
    return {
        "ok": True,
        "version": ver, "datadir": datadir, "basedir": basedir,
        "server_id": server_id, "log_bin": log_bin,
        "replicas": len(replicas), "is_slave": is_slave,
        "suggest_server_id": final_sid,
        "tgt_server_id": tgt_sid,
        "binlog_path": base + "/mysql-bin",
        "need_log_slave_updates": need_lsu,
        "container_like": container_like,
        "cnf_candidates": ["/etc/mysql/mariadb.conf.d/50-server.cnf",
                           "/etc/mysql/my.cnf", "/etc/my.cnf", "/etc/my.cnf.d/server.cnf"],
        "config_locator": [
            "# 1) 先看 MariaDB 是不是跑在容器里",
            "docker ps --format '{{.Names}}\\t{{.Image}}\\t{{.Ports}}' | grep -iE 'maria|mysql'",
            "# 2) 若是容器：看它挂载了宿主机的哪份配置（这里的宿主机路径才是真正生效的）",
            "docker inspect <容器名> --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'",
            "# 3) 若是裸机服务：确认 mysqld 实际按顺序读取哪些配置文件",
            "mysqld --verbose --help 2>/dev/null | head -20",
        ],
        "warn_container": ("" if not container_like else
            "⚠ 该实例的版本串形如官方容器镜像，疑似跑在 Docker / 面板（如 1Panel）的容器里："
            "宿主机 /etc/my.cnf 通常【不生效】，必须改容器挂载的那份配置，"
            "否则照着改完、重启后 log_bin 依然为 0。"),
        "snippet": snippet, "steps": steps,
        "note": ("log_bin 为静态变量，必须改配置并重启 mysqld；本向导不会自动修改配置或重启服务"
                 "（数据库 root 也无权这么做）——以下方案供你在源机上执行。"),
    }


def check_write_freeze(task):
    """停写迁移模式下的「停写检测」：实测源库是否已不再产生写入。

    源库未开 binlog ⇒ 没有桥接可追平，数据种子是唯一数据来源；
    种子是**一次性快照**，此后源库的新写入不会进入新集群、切换即丢失。
    故必须确认源库确实停写，而这不该只靠用户口头判断——这里用
    「间隔采样两次写入类状态计数器取差值」来实测。

    注意：采样窗口内为 0 只能证明这 FREEZE_SAMPLE_SEC 秒内没写入，
    不代表此后永不写入，故切换的最终放行仍由用户勾选确认（不做自动放行）。
    """
    cfg = task["config"]
    dml_ct = ["Com_insert", "Com_update", "Com_delete", "Com_replace", "Com_commit"]
    row_ct = ["Innodb_rows_inserted", "Innodb_rows_updated", "Innodb_rows_deleted"]

    def _sample():
        conn = connect(cfg["src_host"], int(cfg["src_port"]),
                       cfg["src_user"], cfg["src_pass"], timeout=8)
        try:
            cur = conn.cursor()
            vals = {}
            missing = []
            for c in dml_ct + row_ct:
                try:
                    cur.execute("SHOW GLOBAL STATUS LIKE %s", (c,))
                    r = cur.fetchone()
                    if r is None or len(r) < 2 or r[1] is None:
                        # 取不到值：多为源账号缺 PROCESS 权限（SHOW GLOBAL STATUS 需 PROCESS）。
                        # 绝不能当成 0 —— 否则两次都 0、差值 0，会被误判为「已停写」。
                        missing.append(c)
                        vals[c] = 0
                    else:
                        vals[c] = int(r[1])
                except Exception:
                    missing.append(c)
                    vals[c] = 0
            ro = sro = None
            try:
                cur.execute("SELECT @@read_only")
                r = cur.fetchone()
                ro = int(r[0]) if r else None
            except Exception:
                pass
            try:
                cur.execute("SELECT @@super_read_only")
                r = cur.fetchone()
                sro = int(r[0]) if r else None
            except Exception:
                pass
            return vals, ro, sro, missing
        finally:
            conn.close()

    try:
        a, ro, sro, miss_a = _sample()
    except Exception as e:
        return {"ok": False, "frozen": False, "error": f"{type(e).__name__}: {e}"}
    time.sleep(FREEZE_SAMPLE_SEC)
    try:
        b, ro, sro, miss_b = _sample()
    except Exception as e:
        return {"ok": False, "frozen": False, "error": f"{type(e).__name__}: {e}"}
    missing = sorted(set(miss_a) | set(miss_b))

    delta = {c: (b.get(c, 0) - a.get(c, 0)) for c in dml_ct + row_ct}
    writes = sum(max(0, delta.get(c, 0)) for c in dml_ct)
    rows = sum(max(0, delta.get(c, 0)) for c in row_ct)
    # 计数器若被 FLUSH STATUS 重置（出现负值），采样即不可靠。
    # 此时若按 0 处理会【误判为已停写】——漏报比误报危险得多（会让用户在仍有写入时切换），
    # 故出现负值时保守判定为 unreliable，绝不置 frozen。
    negative = {c: v for c, v in delta.items() if v < 0}
    # 缺失（取不到值）比回退更危险：全取不到时 delta 恒为 0，会伪装成「已停写」。
    unreliable = bool(negative) or bool(missing)
    frozen = (writes == 0 and rows == 0 and not unreliable)
    if missing:
        hint = (
            "停写检测需要读取源库的「写入活动统计」来判断是否还有业务在写入，"
            "但当前用来连接源库的账号权限不足，读不到这些统计（共 %s 项取不到值），本次检测不可信。\n"
            "原因：读取写入统计需要源库账号具备 PROCESS 权限，而当前这个账号没有该权限。\n\n"
            "如何避免（任选其一）：\n"
            "1) 给源库上「本迁移任务里填写的那个账号」补授予 PROCESS 权限，"
            "以源库管理员(root)身份执行下面语句（主机填你填迁移信息时用的那个）：\n"
            "   GRANT PROCESS ON *.* TO '<源库账号>'@'<对应主机>'; FLUSH PRIVILEGES;\n"
            "   授权后回到本页重新点「停写检测」即可。\n"
            "2) 或重新填写一个已经具备 PROCESS 权限的源库账号，再重新检测。\n\n"
            "若暂时不方便授权：请改用其它方式人工确认源库已停写"
            "（如查看源库是否还有业务连接、binlog 或数据文件是否仍在增长），"
            "确认无误后再勾选「源库已停写」执行切换，切勿仅凭本结果判断。"
            % len(missing))
    elif negative:
        hint = ("采样不可靠：以下计数器出现回退（可能被 FLUSH STATUS 重置）：%s。"
                "无法判断是否真的停写，请勿据此切换，请稍后重新检测。"
                % "、".join(negative.keys()))
    elif frozen:
        hint = ("最近 %.0f 秒采样窗口内源库没有任何写入，可以进行切换。"
                "（仅代表该窗口，切换前请保持业务停写状态）" % FREEZE_SAMPLE_SEC)
    else:
        hint = ("源库仍在写入：窗口内 DML %d 次、行变更 %d 行。"
                "请先停业务写入，并在源库执行 "
                "SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON; 加固后再检测。"
                "否则种子/切换后这部分增量将永久丢失。" % (writes, rows))
    return {"ok": True, "frozen": frozen, "unreliable": unreliable,
            "missing": missing, "negative": list(negative.keys()),
            "sample_sec": FREEZE_SAMPLE_SEC,
            "writes": writes, "rows": rows, "delta": delta,
            "read_only": ro, "super_read_only": sro, "hint": hint}


def skip_cleanup(task_id):
    """跳过「收尾回收」直接完成迁移——**完全不改动源库（老主）**。

    场景：用户无权 / 不愿在老主上做手脚时（不回收源端账号、不置 read_only），
    迁移到此为止：把 cleanup 标记为 skipped 并收尾，使任务进入完成态。
    """
    task = get_task(task_id)
    if not task:
        return {"ok": False, "error": "任务不存在"}
    steps = dict(task.get("steps") or {})
    note = "已跳过收尾回收：未回收源端账号、未置老主只读，源库保持原状。"
    steps["cleanup"] = {"status": "done", "at": _now(), "skipped": True,
                        "result": {"note": note, "skipped": True}}
    execute("UPDATE migration_tasks SET status='done',steps=%s,progress=100,"
            "stage='迁移完成（已跳过收尾回收）',updated_at=NOW() WHERE id=%s",
            (json.dumps(steps, ensure_ascii=False), task_id))
    return {"ok": True, "skipped": True, "note": note}


# ---------------------------------------------------------------------------
# 进度上报 + 后台线程调度
#
# 长任务绝不能在 HTTP 请求里同步执行：TB 级迁移会超时，前端也永远看不到进度。
# 这里把「百分比 / 阶段文字 / 日志」由后台线程直接落库，前端轮询 /status 渲染。
# 非后台调用（脚本直接 call）时 _tick 自动降级为空操作，不改变原有行为。
# ---------------------------------------------------------------------------
STEP_WEIGHTS = {"seed": 45, "bridge": 15, "verify": 20, "cutover": 15, "cleanup": 5}

_REP = {}          # tid -> {"rep": _Reporter, "step": step}
_RUNNING = set()   # 正在执行步骤的 tid（并发门禁）

# ---- 卡死自愈（避免并发门禁被一个卡死的步骤永久占用）----
_STEP_HB = {}       # tid -> 该步骤最近一次进度的时刻（心跳）
_STEP_PROCS = {}    # tid -> 该步骤 spawn 的子进程列表（卡死时杀掉它们即可解除阻塞）
_FORCE_TS = {}      # tid -> 首次触发自愈的时刻（宽限期过后才强制释放）
STEP_STALL_SEC = 900    # 步骤超过这么久没有任何进度 ⇒ 触发自愈
STEP_FORCE_GRACE = 120  # 杀掉子进程后再给这么久，仍不退出才强制释放门禁


class _Reporter:
    def __init__(self, tid):
        self.tid = tid

    def log(self, msg):
        execute("UPDATE migration_tasks SET log=CONCAT(IFNULL(log,''),%s),updated_at=NOW() "
                "WHERE id=%s", (f"[{_now()}] {msg}\n", self.tid))

    def prog(self, pct, stage=None):
        # 心跳：供常驻自愈守护判断「这个步骤还活着吗」
        if self.tid in _RUNNING:
            _STEP_HB[self.tid] = time.time()
        pct = int(max(0, min(100, pct)))
        if stage is None:
            execute("UPDATE migration_tasks SET progress=%s,updated_at=NOW() WHERE id=%s",
                    (pct, self.tid))
        else:
            execute("UPDATE migration_tasks SET progress=%s,stage=%s,updated_at=NOW() WHERE id=%s",
                    (pct, str(stage)[:250], self.tid))


def _step_progress(task, step, inner):
    """把「步骤内百分比」折算为整任务百分比：已完成步骤权重 + 当前步骤权重×inner%。"""
    done = 0
    for s, w in STEP_WEIGHTS.items():
        if s == step:
            continue
        if ((task.get("steps") or {}).get(s) or {}).get("status") == "done":
            done += w
    return done + STEP_WEIGHTS.get(step, 0) * max(0.0, min(100.0, float(inner or 0))) / 100.0


def _tick(task, inner, stage=None, log=None):
    """步骤函数上报统一入口。任务未在后台运行时自动空操作。"""
    ent = _REP.get(task.get("id"))
    if not ent:
        return
    try:
        rep = ent["rep"]
        if log:
            rep.log(log)
        rep.prog(_step_progress(task, ent["step"], inner), stage)
    except Exception:
        pass


def _finish_step(tid, step, ok, payload=None, err=None):
    task = get_task(tid)
    if not task:
        return
    steps = dict(task.get("steps") or {})
    steps[step] = {"status": "done" if ok else "error", "at": _now(),
                   **({"result": payload} if ok else {"error": str(err)})}
    base = sum(w for s, w in STEP_WEIGHTS.items()
               if ((steps.get(s) or {}).get("status") == "done"))
    all_done = all(((steps.get(s) or {}).get("status") == "done") for s in STEPS)
    status = "error" if not ok else ("done" if all_done else "running")
    if ok:
        execute("UPDATE migration_tasks SET status=%s,steps=%s,progress=%s,stage=%s,"
                "updated_at=NOW() WHERE id=%s",
                (status, json.dumps(steps, ensure_ascii=False), int(base),
                 f"{STEP_LABELS.get(step, step)}完成", tid))
    else:
        execute("UPDATE migration_tasks SET status=%s,steps=%s,stage=%s,updated_at=NOW() WHERE id=%s",
                (status, json.dumps(steps, ensure_ascii=False),
                 f"{STEP_LABELS.get(step, step)}失败: {str(err)[:180]}", tid))


def _cap_of(task):
    cfg = task.get("config") or {}
    return capabilities(cfg.get("src_flavor") or "mariadb",
                        int(cfg.get("src_major") or 0),
                        cfg.get("tgt_flavor") or "mariadb")


def _thread_body(tid, step, opts=None):
    rep = _Reporter(tid)
    try:
        rep.prog(0, "正在启动…")
        rep.log("开始步骤：%s" % STEP_LABELS.get(step, step))
        result = _execute_step(tid, step, opts)
        _finish_step(tid, step, True, payload=result)
        rep.log("步骤完成：%s" % STEP_LABELS.get(step, step))
    except Exception as e:
        _finish_step(tid, step, False, err=e)
        try:
            rep.log("步骤失败：%s -> %s" % (STEP_LABELS.get(step, step), e))
        except Exception:
            pass
    finally:
        _REP.pop(tid, None)
        _RUNNING.discard(tid)
        # 清理自愈相关状态（线程正常结束时也应清干净）
        _STEP_HB.pop(tid, None)
        _STEP_PROCS.pop(tid, None)
        _FORCE_TS.pop(tid, None)


_HEALER_ON = {"started": False}


def _ensure_healer():
    """启动常驻自愈守护（进程内只起一次）。"""
    if _HEALER_ON["started"]:
        return
    _HEALER_ON["started"] = True
    threading.Thread(target=_stall_healer, daemon=True).start()


def _stall_healer():
    """常驻守护：步骤长时间无任何进度时自愈，保证并发门禁不会被永久占用。

    Python 无法安全杀死线程，所以不能"杀线程"，只能让它自己退出：
      ① 先杀掉该步骤启动的子进程 —— 阻塞在管道读写上的线程会立刻返回、自行退出，
         随后 finally 正常释放门禁（对 seed 这类管道卡死就是根治手段）；
      ② 若仍不退出（没有子进程 / 卡在别处），宽限期后强制释放门禁并标记失败，
         保证一个卡死的步骤不会把后续所有迁移全部挡死。
    """
    while True:
        time.sleep(30)
        now = time.time()
        for tid in list(_RUNNING):
            ts = _STEP_HB.get(tid)
            if ts is None:            # 还没上报过进度：先记一次心跳，避免误判
                _STEP_HB[tid] = now
                continue
            if now - ts < STEP_STALL_SEC:
                continue
            first = _FORCE_TS.get(tid)
            if first is None:
                # ① 杀掉子进程，解除阻塞
                _FORCE_TS[tid] = now
                killed = 0
                for p in _STEP_PROCS.pop(tid, []):
                    try:
                        p.kill()
                        killed += 1
                    except Exception:
                        pass
                try:
                    _Reporter(tid).log(
                        "自愈：步骤超过 %d 秒没有任何进度，已终止 %d 个子进程以解除阻塞；"
                        "若 %d 秒内仍不退出，将强制释放并发门禁"
                        % (STEP_STALL_SEC, killed, STEP_FORCE_GRACE))
                except Exception:
                    pass
                _STEP_HB[tid] = now    # 给一个宽限期，让它自行收尾退出
                continue
            if now - first > STEP_FORCE_GRACE:
                # ② 兜底：强制释放门禁 —— 绝不让一个卡死步骤挡住后续所有任务
                _RUNNING.discard(tid)
                _REP.pop(tid, None)
                _STEP_HB.pop(tid, None)
                _FORCE_TS.pop(tid, None)
                try:
                    execute("UPDATE migration_tasks SET status='error',stage=%s,updated_at=NOW() "
                            "WHERE id=%s",
                            ("步骤无响应超时，已强制释放并发门禁（后台线程可能仍在运行，"
                             "建议重启管理器彻底清理）", tid))
                except Exception:
                    pass


def start_step(task_id, step, opts=None):
    """异步启动一个步骤，HTTP 立即返回（真正的活交给后台线程）。

    opts：可选附加参数。目前仅 {"deep": true} —— 触发一致性校验的内容级深度比对。
    """
    task = get_task(task_id)
    if not task:
        return {"ok": False, "error": "任务不存在"}
    if step not in STEPS:
        return {"ok": False, "error": f"未知步骤: {step}"}
    # 并发门禁：全局同时只允许一个迁移任务在跑（不同任务会争用同一目标集群，
    # 并发执行会互相踩踏复制通道造成数据错乱）。同一任务重复启动也被此挡住。
    if _RUNNING:
        busy = ", ".join("#%d" % t for t in sorted(_RUNNING))
        return {"ok": False, "error": (
            "已有迁移任务正在执行（%s），同一时间只允许一个迁移任务运行。\n"
            "⚠ 这是【管理器进程内】的并发门禁，删除或清理历史记录【不会】释放它。\n"
            "· 若该任务确实还在跑：等它结束即可；\n"
            "· 若它已卡死（进度长时间不动）：点「中止任务」，"
            "或在管理器所在机器上杀掉卡住的 mariadb-dump / mariadb 子进程；\n"
            "· 仍无法释放：重启管理器进程强制清空。" % busy)}
    _RUNNING.add(task_id)
    _REP[task_id] = {"rep": _Reporter(task_id), "step": step}
    execute("UPDATE migration_tasks SET status='running',updated_at=NOW() WHERE id=%s", (task_id,))
    _ensure_healer()
    threading.Thread(target=_thread_body, args=(task_id, step, opts), daemon=True).start()
    return {"ok": True, "started": True, "step": step}


def request_cancel(tid):
    """请求中止任务：只置标记，由步骤循环在安全点自行退出。

    为什么不直接杀线程：Python 无法安全杀死线程，强杀会留下半截状态
    （桥接建了一半、目标残留复制账号、管道未关闭）。置标记后步骤循环退出，
    线程正常结束，finally 会释放 _RUNNING，并发门禁随之自动解除。

    若步骤正阻塞在外部命令（如 mariadb-dump）上，看门狗会在 SEED_STALL_SEC 内兜底。
    """
    _CANCEL.add(int(tid))
    return {"ok": True, "tid": int(tid),
            "note": ("已请求中止，步骤会在下一个安全点退出；若正阻塞在外部命令上，"
                     "最多再等 %d 秒由看门狗兜底中止。" % SEED_STALL_SEC)}


def run_step(task_id, step):
    """同步执行一步（脚本/测试用；Web 请走 start_step）。"""
    try:
        result = _execute_step(task_id, step)
        _finish_step(task_id, step, True, payload=result)
        return {"ok": True, "step": step, **result}
    except Exception as e:
        _finish_step(task_id, step, False, err=e)
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _execute_step(task_id, step, opts=None):
    """真正干活（不含线程/落库）。opts 为可选附加参数（如 {"deep": true}）。"""
    task = get_task(task_id)
    if not task:
        raise ValueError("任务不存在")
    if step not in STEPS:
        raise ValueError(f"未知步骤: {step}")
    cap = _cap_of(task)

    if cap.get("refused"):
        raise RuntimeError("暂不支持该组合：MySQL 8.0+ → MariaDB 需先做排序规则与认证插件换算，"
                           "当前版本未提供该流程。请使用 MariaDB 源或 MySQL 5.7 源。")

    if step == "seed":
        if task["channel"] == "physical":
            if not cap["physical"]:
                raise RuntimeError("物理通道不支持跨产品线迁移（MySQL 8 起元数据改用全局数据字典，"
                                   "MariaDB 无法读取其数据目录），请改用逻辑通道。")
            return seed_logical(task)  # 2.4.1 起高级模式已取消，物理任务降级为逻辑通道
        return seed_logical(task)
    if step == "bridge":
        if not cap["bridge"]:
            raise RuntimeError("该组合不支持 GTID 桥接（跨产品线 GTID 格式不兼容），"
                               "请在停写窗口内一次性拷贝后直接切换。")
        return bridge(task)
    if step == "verify":
        return verify(task, deep=bool((opts or {}).get("deep")))
    if step == "cutover":
        return cutover(task)
    if step == "cleanup":
        return cleanup(task)
    raise ValueError("未知步骤")
