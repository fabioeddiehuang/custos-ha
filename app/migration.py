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
   ⇒ 迁移期间目标节点【绝不注册进 Custos 受管集群】：2.4.0 重做后迁移改为纯逻辑
     「导出→导入」，不再建立复制桥接，目标仅作为承载导入数据的独立实例，
     monitor 也无需为「切断桥接」而特殊绕过其拓扑维护。
"""
import json
import os
import time
import shutil
import subprocess
import threading
import pymysql
from datetime import datetime, timedelta

from .db import query, execute, get_setting
from . import crypto
from .config import get_mgmt_host, NODE_AGENT_PORT, NODE_AGENT_TOKEN, NODE_AGENT_TIMEOUT
from .ha_client import connect, ensure_ha_creds, heal_replication
from .consistency import _replica_status
from .clusters import create_cluster, get_cluster_creds
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

# 数据量阈值（GB）：大库迁移的 UI 提示阈值（逻辑通道按此估算进度/建议）
SIZE_THRESHOLD_GB = 500

# 向导步骤（顺序即步进条）—— 2.4.0 重做：纯逻辑「导出→导入」，去掉桥接/切换/收尾
#   · seed   ：逐库 mariadb-dump --single-transaction → 目标（同名库先 DROP 整库再导入）
#   · verify ：可选完整性校验（每库表数比对 + 可选 checksum），不阻塞任务完成
STEPS = ["seed", "verify"]
STEP_LABELS = {
    "seed": "导出并导入",
    "verify": "完整性校验(可选)",
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
        "SELECT id, name, channel, status, progress, stage, created_at, updated_at, config "
        "FROM migration_tasks ORDER BY id DESC"
    )
    # 顺带把目标节点地址带出，供前端「解锁迁移锁」按钮直接调用 /api/migration/unlock
    # （迁移锁按 host:port 定位，列表不返回则前端只能手动填）。
    out = []
    for r in rows:
        item = dict(r)
        cfg = {}
        try:
            cfg = json.loads(item.get("config") or "{}")
        except Exception:
            pass
        item["tgt_host"] = cfg.get("tgt_host") or ""
        item["tgt_port"] = int(cfg.get("tgt_port") or 0)
        out.append(item)
    return out


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
    # 2.4.1 起仅逻辑通道（物理通道 mariabackup 已取消）；统一推荐逻辑通道。
    size_gb = (probe.get("size_mb") or 0) / 1024.0
    reason = ("数据量 %.1f GB，采用逻辑通道（通用/干净/跨大版本安全，默认）" % size_gb)
    return {"channel": "logical", "reason": reason, "size_gb": round(size_gb, 2)}


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
        "physical": False,  # 物理(mariabackup)通道已在 2.4.0 取消
        # 桥接限定 MariaDB→MariaDB（我们的复制语句是 MariaDB GTID 语义）
        # 2.4.0 重做：迁移改为纯逻辑导出导入，不再建立复制桥接（server_id 撞车 / GTID 空洞 /
        # 1406 等长链 bug 全部消失），故桥接能力恒为 False。
        "bridge": False,
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

def _pw_arg(pw):
    """空口令时必须整个省略 -p，否则传 'p' 会变成交互等待输入，进程直接挂死。"""
    return [f"-p{pw}"] if pw else []


def _ensure_target_accounts(cfg, stage=""):
    """seed（逻辑导入）完成后【立刻】补齐目标节点上的 Custos 账号（幂等，失败不阻断）。

    根因（v2.4.0 实测，203/213 集群）：全库导入（含 mysql 库 / 整个实例）会
    覆盖目标 datadir，`mysql.user` 一并被替换 ⇒ 目标上原有的 ha_monitor /
    replication / app 账号**全部消失** ⇒ 导入后目标处于「无 Custos 账号」状态：

      · 同集群第二节点（213）连新主（203）报 1045
        ``Access denied for user 'replication'@'192.168.1.213'``（账号根本不存在）；
      · 管理器自身连不上目标（ha_monitor 不存在）⇒ 仪表盘复制/IO 全是 “—”；
      · 又因「迁移锁定」让 ensure_topology 整轮跳过（见 failover），这段窗口内
        现改为：导入完成后立刻重建账号，不再依赖后续步骤兜底。

    对策：seed（逻辑导入）完成后立刻按目标所属集群的凭证重建账号。目标尚未登记进任何集群时无可对齐的凭证 → 跳过（待其登记进集群后再对齐）。
    """
    try:
        rows = query("SELECT cluster_id FROM nodes WHERE ip=%s AND port=%s",
                     (cfg["tgt_host"], int(cfg["tgt_port"])))
        cid = (rows[0].get("cluster_id") if rows else None)
        if not cid:
            return {"ok": True, "skipped": "目标尚未登记进 Custos 集群，留待登记进集群后再对齐凭证"}
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
        # 不阻断迁移主线：账号可由 monitor 再次自愈，但必须如实回传原因
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


def _src_dump_priv_flags(host, port, user, pass_):
    """探测源账号对 事件/触发器/存储过程/视图 的导出权限，决定 dump 时带哪些可选 flag。

    2.4.0 重做（纯逻辑导出导入）：非 root 源账号往往只有 SELECT。若缺对应对象级权限，
    mariadb-dump 会直接 1044 失败（如 'show events'）。故按权限「尽力导出」——
    缺哪个对象就跳过哪个，数据本身照常迁移，仅缺失该部分对象（不阻断）。
    """
    try:
        c = connect(host, int(port), user, pass_, timeout=8)
        cur = c.cursor()
        cur.execute("SHOW GRANTS FOR CURRENT_USER()")
        g = "\n".join(str(x[0]) for x in (cur.fetchall() or [])).upper()
        c.close()
    except Exception:
        g = ""
    allp = ("ALL PRIVILEGES" in g) or ("GRANT ALL" in g)
    return {
        "events":   allp or ("EVENT" in g),
        "triggers": allp or ("TRIGGER" in g),
        "routines": allp or ("SELECT" in g),   # 例程需能读 mysql 系统库（全局 SELECT）
        "views":    allp or ("SHOW VIEW" in g),
    }


def _dump_optional_flags(priv, databases, host, port, user, pass_):
    """根据权限生成 dump 的可选 flag；无 SHOW VIEW 时用 --ignore-table 跳过各库视图。"""
    f = []
    if priv.get("events"):
        f.append("--events")
    if not priv.get("triggers"):
        f.append("--skip-triggers")          # 触发器默认导出，缺权限须显式跳过
    if priv.get("routines"):
        f.append("--routines")
    if not priv.get("views"):
        try:
            c = connect(host, int(port), user, pass_, timeout=8)
            cur = c.cursor()
            for d in databases:
                try:
                    cur.execute("SELECT table_name FROM information_schema.views "
                                "WHERE table_schema=%s", (d,))
                    for (v,) in (cur.fetchall() or []):
                        f.append("--ignore-table=%s.%s" % (d, v))
                except Exception:
                    pass
            c.close()
        except Exception:
            pass
    return f


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

    # ── 目标同名库处理（用户决策 1b/1c：整库替换）────────────────────────────
    # 逐个选中库检查：目标实例已存在同名库 → 先 DROP 整库再导入（保证数据完整、无脏数据残留）；
    # 目标无同名库 → 直接导入（建库由下方 dump 的 --databases 完成）。
    # 系统库 / Custos 库（IGNORE_DBS）绝不触碰，杜绝误删 mysql 等。
    tgt_conn = connect(cfg["tgt_host"], int(cfg["tgt_port"]), _tgt_user(cfg),
                       cfg["tgt_root_pass"], timeout=8)
    try:
        tcur = tgt_conn.cursor()
        for _db in databases:
            if _db in IGNORE_DBS:
                continue
            try:
                tcur.execute("SELECT 1 FROM information_schema.schemata "
                             "WHERE schema_name=%s", (_db,))
                if tcur.fetchone():
                    _tick(task, 1, stage="目标已存在同名库 `%s`，先删除再整库导入" % _db,
                          log="DROP DATABASE `%s`（目标存在同名库，按决策整库替换）" % _db)
                    tcur.execute("DROP DATABASE `%s`" % _db)
            except Exception as _e:
                raise RuntimeError("删除目标同名库 `%s` 失败（已中止，避免部分导入）：%s"
                                   % (_db, _e))
    finally:
        try:
            tgt_conn.close()
        except Exception:
            pass

    dump, cli, dialect = resolve_client_tools(src_flavor)
    _tick(task, 2, stage="准备导出（工具=" + os.path.basename(dump) + "）",
          log="开始逻辑迁移：%d 个库，工具=%s" % (len(databases), os.path.basename(dump)))

    # ⚠ 绝不能把 GTID 位点写进 dump。
    #   实测：mariadb-dump --gtid --master-data=1 会输出 SET GLOBAL gtid_slave_pos='…'，
    #   而当源端此刻尚无 GTID 位点时该值为空，导入目标会触发
    #   ERROR 1948（与目标已有 binlog GTID 冲突）。
    #   正确做法：位点不写进 dump 数据流；纯逻辑导入不建复制桥接（cap["bridge"] 恒 False，
    #   位点留空），无「留给 bridge 阶段显式设置」的旧需求。
    src_gtid = _snapshot_src_gtid(cfg) if cap["bridge"] else ""
    # 方言跟随真实二进制（见 tool_dialect 注释），避免把 MySQL 参数喂给 mariadb-dump
    gtid_args = dump_gtid_args(dialect, want_gtid=False)

    priv = _src_dump_priv_flags(src[0], src[1], src[2], src[3])
    dump_args = [
        dump, "-h", src[0], "-P", str(src[1]), "-u", src[2],
    ] + _pw_arg(src[3]) + [
        # 2.4.0 重做：按源账号权限「尽力导出」事件/触发器/例程/视图；
        # 缺对应权限则自动跳过该对象（不阻断数据迁移，仅缺失该部分对象）。
        "--single-transaction",
    ] + _dump_optional_flags(priv, databases, src[0], src[1], src[2], src[3]) + [
        "--hex-blob", "--default-character-set=utf8mb4",
        "--skip-lock-tables", "--databases",
    ] + databases + gtid_args + ssl_args(dialect)
    _skipped = [k for k in ("events", "triggers", "routines", "views")
                if not priv.get(k)]
    if _skipped:
        _tick(task, 1, log="源账号缺少 %s 权限，已自动跳过对应对象导出（数据本身照常迁移）；"
                    "如需完整迁移这些对象，请授予相应权限后重跑。" % "/".join(_skipped))
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
        # 2.4.0 重做：纯逻辑导入无需在源库建复制账号（桥接已移除），故删除「源账号 CREATE USER」预检。
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
        _priv = _src_dump_priv_flags(cfg["src_host"], cfg["src_port"], cfg["src_user"], cfg["src_pass"])
        dump_args = [dump, "-h", cfg["src_host"], "-P", str(int(cfg["src_port"])),
                     "-u", cfg["src_user"]] + _pw_arg(cfg["src_pass"]) + [
            "--no-data",
        ] + _dump_optional_flags(_priv, databases, cfg["src_host"], cfg["src_port"], cfg["src_user"], cfg["src_pass"]) + [
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


def _has_live_migration_task(host, port):
    """是否存在指向该节点、且【确实有后台线程在跑】的迁移任务。

    为什么必须同时校验内存集合 _RUNNING：migration_tasks.status='running' 只是数据库
    记录，管理器重启 / 线程异常退出后记录会残留成【孤儿任务】。若只认 status，节点会被
    永久判定为「迁移中」，而 failover.ensure_topology 会跳过迁移中节点的复制维护，
    结果是【该节点再也建不上主从】（2026-09-23「容器版 MariaDB 无法主从」的根因）。
    _RUNNING 是内存集合、随线程启动写入、重启即空，正好等价于"真的在跑"。
    """
    try:
        rows = query("SELECT id, config FROM migration_tasks WHERE status='running'")
    except Exception:
        return False
    for r in rows:
        try:
            if int(r.get("id")) not in _RUNNING:
                continue          # 孤儿任务：没有活线程，不算数
        except (TypeError, ValueError):
            continue
        try:
            cfg = json.loads(r.get("config") or "{}")
        except Exception:
            continue
        if (str(cfg.get("tgt_host")) == str(host)
                and int(cfg.get("tgt_port") or 0) == int(port)):
            return True
    return False


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
    # 兜底②：仍有指向该节点、且【确实有活线程在跑】的迁移任务 ⇒ 任务期间始终锁定，
    #   杜绝第3步→第4步间隔超过 6h、锁过期后 monitor 的 ensure_topology 把桥接通道
    #   清掉（RESET REPLICA ALL）导致追平检测永远「复制通道已清除」（issue #3/#4 的延续）。
    #
    # ⚠ 这里【绝不能】调用 is_node_migrating()：它反过来会调 is_target_locked()，
    #   二者互相递归 → RecursionError → 被下面的 except 吞掉 ⇒ 整个兜底静默失效。
    # ⚠ 且必须要求任务「真有活线程」：只认 status='running' 会让管理器重启/线程异常
    #   留下的孤儿任务把节点永久锁死 → ensure_topology 永久跳过 → 再也建不上主从。
    try:
        if _has_live_migration_task(host, port):
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
    # 覆盖「锁还没落库」的窗口（如 seed 刚开始的瞬间）。
    # 注意：只认【有活线程】的任务，孤儿 running 任务不再让节点永远显示/停留在迁移中。
    return _has_live_migration_task(host, port)


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
    tables, missing, diffs, lag_diffs = [], [], [], []
    ck_checked = ck_mismatch = ck_unsupported = 0
    # 复制状态提前取：桥接模式下目标是【仍在追平的活从库】，COUNT(*) 与 CHECKSUM
    # 在不同时刻读取会受复制延迟影响；据此把「行数瞬时差 1 行」区分成
    # 「复制追平中（非数据不一致）」还是「真正的行数不一致」，避免误报（根治反复出现的
    # "行数不一致但内容校验全一致" 自相矛盾提示）。
    repl = _replica_status(t_conn)
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

    # ---- 行数差异归因：区分「复制追平中（瞬时延迟）」与「真正的行数不一致」 ----
    # 桥接模式目标为活从库：复制仍在追平，COUNT(*) 在两个时刻读到不同行数属正常，
    # 不应报成数据不一致。判定逻辑：
    #   ① 开了内容级校验且 CHECKSUM 一致 ⇒ 内容完全相同，行数差必为瞬时读取竞态 ⇒ 非差异；
    #   ② 复制健康且在追平（SQL 线程 Running、无错误、延迟非 0）⇒ 行数差是复制延迟 ⇒ 追平中；
    #   ③ 其余（已追平却仍差 / 复制中断报错）⇒ 真正的行数不一致。
    sql_running = (repl or {}).get("sql_running")
    repl_err = (repl or {}).get("last_error")
    sbm = (repl or {}).get("seconds_behind")
    healthy_lag = (sql_running == "Yes") and (not repl_err) and (sbm not in (0, None))
    real_diffs = []
    for d in diffs:
        t = next((x for x in tables if x["db"] == d["db"] and x["table"] == d["table"]), None)
        if t and deep and t.get("ck_ok") is True:
            # CHECKSUM 已证明内容一致 ⇒ 行数差是读取竞态，撤销差异标记（根治核心）
            t["rows_ok"] = True
            t["note"] = "内容校验(CHECKSUM)已通过，行数瞬时差异系复制延迟，非数据不一致"
            continue
        if healthy_lag:
            # 复制健康追平中 ⇒ 行数差为延迟导致，标记「追平中」而非失败
            if t:
                t["lag"] = True
                t["note"] = "复制追平中，行数暂不一致（复制延迟，非数据不一致，待延迟归零后复测即可）"
            lag_diffs.append(d)
            continue
        real_diffs.append(d)
    diffs = real_diffs

    ok_n = sum(1 for t in tables if t["rows_ok"])
    consistent = (not diffs) and (not missing)
    if consistent:
        if lag_diffs:
            summary = ("已校验 %d 张表，行数全部一致；%d 张表复制追平中（行数暂不一致，"
                       "待复制延迟归零后复测即可）" % (ok_n, len(lag_diffs)))
        else:
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
        "lag_diffs": lag_diffs,
        "consistent": consistent,
        "tables": tables,
        "checksum": {"enabled": bool(deep), "checked": ck_checked,
                     "mismatch": ck_mismatch, "unsupported": ck_unsupported},
        "replica": repl,
        "summary": summary,
        "note": summary,
    }


# ---------------------------------------------------------------------------


def audit_source_privileges(cfg):
    """源库账号「权限体检」：列出逻辑导出导入依赖的权限是否具备，缺了会影响什么。

    目的：2.4.0 重做后迁移为纯逻辑「导出→导入」（逐库 dump --single-transaction → 目标），
    源库只需能导出数据（SELECT / SHOW VIEW / TRIGGER / EVENT），不再需要复制 / 建账号 /
    置只读等权限。非 root 时若静默失败会误导用户，故在第一步把「缺什么、影响导出、怎么补」讲清楚。
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

        # 2.4.0 重做：纯逻辑「导出→导入」，源库只需能导出数据，无需复制/建账号/置只读权限。
        # dump 已带 --skip-lock-tables（单事务快照），故不要求 LOCK TABLES / RELOAD。
        sel_ok = _ok("SELECT schema_name FROM information_schema.schemata LIMIT 1")
        checks = [
            {"name": "读取库/表（SELECT）", "ok": sel_ok,
             "used": "第一步探测、数据种子导出、一致性校验",
             "fix": "授予待迁移库的 SELECT 权限"},
            {"name": "导出视图（SHOW VIEW）", "ok": _has("SHOW VIEW"),
             "used": "导出视图定义（缺失则自动跳过视图，不阻断数据迁移）",
             "fix": "GRANT SHOW VIEW（可选；缺失只跳过视图）"},
            {"name": "导出触发器（TRIGGER）", "ok": _has("TRIGGER"),
             "used": "导出触发器（--triggers，缺失则自动跳过，不阻断）",
             "fix": "GRANT TRIGGER（可选；缺失只跳过触发器）"},
            {"name": "导出事件（EVENT）", "ok": _has("EVENT"),
             "used": "导出事件调度器（--events，缺失则自动跳过，不阻断）",
             "fix": "GRANT EVENT（可选；缺失只跳过事件）"},
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
      2.4.0 重做（纯逻辑导出导入）：目标端真正需要的两件事——
      ① 数据导入：CREATE + INSERT（建库建表写数据）；
      ② 重建集群账号：导入后按集群凭证重建 ha_monitor / 复制账号（需 CREATE USER + GRANT）。
      实际导入与账号重建均用目标 root（tgt_root_pass）执行，故目标填 root 即可。

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
        create_user_ok = _has("CREATE USER")
        # 2.4.0 重做：纯逻辑导入，目标端真正需要的两件事——
        #  ① 数据导入：CREATE + INSERT（建库建表写数据）；
        #  ② 重建集群账号：导入后按集群凭证重建 ha_monitor / 复制账号（需 CREATE USER + GRANT）。
        # 注：实际导入与账号重建均用目标 root（tgt_root_pass）执行，故目标填 root 即可；
        #     此处列出非 root 时的能力边界，便于用户判断。
        levels = [
            {"stage": "数据导入", "ok": bool(create_ok and insert_ok),
             "need": "CREATE、INSERT（能建库建表写数据）"},
            {"stage": "重建集群账号", "ok": bool(create_user_ok and has_all),
             "need": "CREATE USER + GRANT ALL（导入后重建 ha_monitor / 复制账号等）"},
        ]
        missing = [l["stage"] for l in levels if not l["ok"]]
        return {"ok": True, "user": who or user, "is_root": (user or "root") == "root",
                "levels": levels, "missing": missing,
                "advice": "建议 root" if missing else "权限齐全"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 进度上报 + 后台线程调度
#
# 长任务绝不能在 HTTP 请求里同步执行：TB 级迁移会超时，前端也永远看不到进度。
# 这里把「百分比 / 阶段文字 / 日志」由后台线程直接落库，前端轮询 /status 渲染。
# 非后台调用（脚本直接 call）时 _tick 自动降级为空操作，不改变原有行为。
# ---------------------------------------------------------------------------
STEP_WEIGHTS = {"seed": 80, "verify": 20}

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
    # 2.4.0 重做：seed 是完成性步骤（整库导入完成即迁移成功）；verify 为可选校验，
    # 不阻塞任务完成。故「全部完成」以 seed 为准。
    all_done = ((steps.get("seed") or {}).get("status") == "done")
    if ok:
        status = "done" if all_done else "running"
        execute("UPDATE migration_tasks SET status=%s,steps=%s,progress=%s,stage=%s,"
                "updated_at=NOW() WHERE id=%s",
                (status, json.dumps(steps, ensure_ascii=False), int(base),
                 f"{STEP_LABELS.get(step, step)}完成", tid))
    else:
        if step == "seed":
            status = "error"            # 仅 seed 失败才判任务失败
        else:
            # verify 等可选步骤失败不回退已成功的迁移（seed 已成功视为迁移完成）
            status = (task.get("status") or "running")
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
        return seed_logical(task)
    if step == "verify":
        return verify(task, deep=bool((opts or {}).get("deep")))
    # 2.4.0 重做：桥接 / 停桥切换 / 收尾 步骤已移除（纯逻辑导出导入，不再建复制通道）
    raise ValueError("未知步骤: %s（桥接/切换/收尾步骤已在 2.4.0 重做中移除）" % step)
