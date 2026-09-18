"""migration.py — 既有 MariaDB 集群 → Custos 托管集群 数据迁移向导后端。

⚠ 本模块**专注 MariaDB**：所有复制 / GTID / 客户端语法均使用 MariaDB 形态，
   不混入 MySQL 的 START SLAVE / RESET SLAVE / MASTER_AUTO_POSITION /
   CHANGE REPLICATION SOURCE TO / xtrabackup 等语法：
     · 复制命令：START REPLICA / STOP REPLICA / RESET REPLICA ALL / SHOW REPLICA STATUS
     · 复制建立：CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos   （MariaDB GTID 语义）
     · GTID 体系：gtid_domain_id / gtid_current_pos / gtid_slave_pos / @@gtid_strict_mode
     · 客户端：mariadb-dump / mariadb（10.5+），降级 mysqldump / mysql
     · 物理备份：mariabackup（MariaDB 版，非 Percona xtrabackup）

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
import pymysql
from datetime import datetime

from .db import query, execute
from . import crypto
from .ha_client import connect
from .clusters import create_cluster, get_cluster_creds
from . import node_gen
from .config import get_mgmt_host

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
    "cleanup": "收尾回收",
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
    }


def list_tasks():
    rows = query(
        "SELECT id, name, channel, status, created_at, updated_at FROM migration_tasks ORDER BY id DESC"
    )
    return rows


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
        databases = [x[0] for x in cur.fetchall()]

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
    if channel_pref in ("logical", "physical"):
        chosen = channel_pref
    else:
        chosen = "physical" if size_gb >= SIZE_THRESHOLD_GB else "logical"
    if chosen == "physical":
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
        u, p = cfg["repl_user"], cfg["repl_pass"]
        try:
            cur.execute(f"CREATE USER IF NOT EXISTS '{u}'@'%' IDENTIFIED BY '{p}'")
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


def seed_logical(task):
    """逻辑通道：仅用户库导出 → 流式导入目标（排除系统库 / Custos 库）。

    工具与 GTID 参数按源端产品线分流（详见 engine_adapter / dump_gtid_args 注释）。
    支持真实字节级进度：以探测阶段得到的数据量为分母统计已传输字节。
    """
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
    databases = cfg.get("databases") or []
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

    # 单独线程读源端 stderr，防止管道撑满导致死锁
    derr_box = []
    t_err = threading.Thread(target=lambda: derr_box.append(p1.stderr.read()), daemon=True)
    t_err.start()

    read_bytes = 0
    next_report = 0
    try:
        while True:
            chunk = p1.stdout.read(65536)
            if not chunk:
                break
            read_bytes += len(chunk)
            # 写入端慢会在此自然阻塞 ⇒ 进度真实反映落地速度，而非纯导出速度
            p2.stdin.write(chunk)
            if total_bytes > 0 and read_bytes >= next_report:
                next_report = read_bytes + 8 * 1024 * 1024
                pct = min(95.0, read_bytes * 100.0 / total_bytes)
                _tick(task, pct, stage="已传输 %.0f / %.0f MB（%d 个库）"
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
    t_err.join(timeout=5)
    derr = derr_box[0] if derr_box else b""

    if p2.returncode != 0 or p1.returncode != 0:
        detail = (err2 or b"").decode(errors="replace")[:2000] \
            or (derr or b"").decode(errors="replace")[:2000]
        raise RuntimeError("逻辑导入失败（退出码 dump=%s load=%s）: %s"
                           % (p1.returncode, p2.returncode, detail))

    _tick(task, 100, stage="逻辑导入完成", log="导入完成，共传输 %.1f MB" % (read_bytes / 1048576.0))
    return {"ok": True, "dump_tool": os.path.basename(dump), "databases": databases,
            "bytes": read_bytes, "src_gtid": src_gtid, "capabilities": {
                k: cap[k] for k in ("same_engine", "cross_engine", "pipeline", "bridge")}}


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

    databases = [d for d in (cfg.get("databases") or []) if d]
    findings = []
    src_flavor = cfg.get("src_flavor") or "mariadb"
    dump, cli, dialect = resolve_client_tools(src_flavor)
    ph = ",".join(["%s"] * len(databases)) if databases else "NULL"

    def _q(conn, sql, args=()):
        cur = conn.cursor()
        cur.execute(sql, tuple(args))
        return cur.fetchall()

    src = connect(cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"], timeout=8)
    tgt = connect(cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"], timeout=8)
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
                c2 = connect(cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"], timeout=8)
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


def seed_physical(task):
    """物理通道：mariabackup（MariaDB 版）热备源 → prepare → 恢复到目标。

    注意：物理备份须使用【匹配源主版本】的 mariabackup（同 MariaDB 大版本）。
    源→目标跨大版本时，目标 12.3.3 首次启动会自动 mysql_upgrade，向导会监控其结果。

    源端 my.cnf / 数据目录因人而异（裸机 / 容器 / 自定义路径），非标安装须在向导中
    显式填写 src_my_cnf（--defaults-file）与 src_data_dir（--datadir），否则
    mariabackup 无法定位源数据目录。
    """
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
    if not _which("mariabackup"):
        raise RuntimeError("管理端未安装 mariabackup（MariaDB 物理备份工具），无法使用物理通道")
    tmp = f"/tmp/custos_mig_backup_{int(time.time())}"
    os.makedirs(tmp, exist_ok=True)
    # 1) 热备（对源几乎无锁）
    # 源端 my.cnf / 数据目录因人而异（裸机/容器/自定义），非标安装必须显式指定：
    #   --defaults-file 放在最前（mariabackup 要求）；--datadir 覆盖探测到的数据目录。
    bk_args = ["mariabackup"]
    if cfg.get("src_my_cnf"):
        bk_args.append(f"--defaults-file={cfg['src_my_cnf']}")
    bk_args += ["--backup", f"--host={src[0]}", f"--port={src[1]}",
                f"--user={src[2]}", f"--password={src[3]}", f"--target-dir={tmp}"]
    if cfg.get("src_data_dir"):
        bk_args.append(f"--datadir={cfg['src_data_dir']}")
    bk = subprocess.run(
        bk_args,
        capture_output=True, text=True, timeout=3600 * 6,
    )
    if bk.returncode != 0:
        raise RuntimeError("mariabackup 备份失败: " + bk.stderr[:2000])
    # 2) prepare（回放 redo，使其一致）
    pr = subprocess.run(
        ["mariabackup", "--prepare", f"--target-dir={tmp}"],
        capture_output=True, text=True, timeout=3600,
    )
    if pr.returncode != 0:
        raise RuntimeError("mariabackup prepare 失败: " + pr.stderr[:2000])
    # 3) 恢复到目标：先把已 prepare 的备份传到目标「暂存目录」，再在目标机
    #    copy-back 到目标数据目录（显式 --datadir 落盘，不依赖目标 my.cnf）。
    #    注意：绝不能直接 rsync 进目标数据目录再 copy-back（会循环/落错位置）。
    dd = _tgt_datadir(cfg)
    nt = cfg.get("node_type", "mariadb-docker")
    staging = f"/data/custos_mig_staging_{task['id']}"
    try:
        r = subprocess.run(
            ["rsync", "-az", "--delete", f"{tmp}/", f"{tgt[0]}:{staging}/"],
            capture_output=True, text=True, timeout=3600,
        )
        if r.returncode != 0:
            raise RuntimeError(r.stderr[:1500])
        # 先停实例→清空目标数据目录→copy-back（--datadir 显式落盘）→授权→启动；
        # 按形态分支：裸机 systemctl，容器 docker compose（数据目录为宿主机挂载）。
        if nt.endswith("-docker"):
            cb = (f"cd /data/custos-ha && docker compose stop mariadb; "
                  f"rm -rf {dd} && mkdir -p {dd} && "
                  f"mariabackup --copy-back --target-dir={staging} --datadir={dd} && "
                  f"chown -R root:root {dd} && "
                  f"docker compose start mariadb; "
                  f"rm -rf {staging}")
        else:
            svc = "mariadb" if nt.startswith("mariadb") else "mysql"
            cb = (f"systemctl stop {svc}; "
                  f"rm -rf {dd} && mkdir -p {dd} && "
                  f"mariabackup --copy-back --target-dir={staging} --datadir={dd} && "
                  f"chown -R mysql:mysql {dd} && systemctl start {svc}; "
                  f"rm -rf {staging}")
        rb = subprocess.run(["ssh", tgt[0], cb], capture_output=True, text=True, timeout=3600)
        if rb.returncode != 0:
            raise RuntimeError("远程 copy-back 失败: " + rb.stderr[:1500])
    except FileNotFoundError as e:
        if nt.endswith("-docker"):
            stop_hint, start_hint, chown = (
                "cd /data/custos-ha && docker compose stop mariadb",
                "docker compose start mariadb", "root:root")
        else:
            svc = "mariadb" if nt.startswith("mariadb") else "mysql"
            stop_hint, start_hint, chown = (
                f"systemctl stop {svc}", f"systemctl start {svc}", "mysql:mysql")
        raise RuntimeError(
            "未找到 rsync/ssh，无法自动恢复到目标。请手动执行：\n"
            f"  rsync -az --delete {tmp}/ {tgt[0]}:{staging}/\n"
            f"  目标机执行: {stop_hint}; rm -rf {dd} && mkdir -p {dd} && "
            f"mariabackup --copy-back --target-dir={staging} --datadir={dd} && "
            f"chown -R {chown} {dd} && {start_hint}; rm -rf {staging}\n"
            f"（原始错误: {e}）"
        )
    finally:
        # 清理管理器本机临时备份目录（TB 级，避免撑爆管理端磁盘）
        shutil.rmtree(tmp, ignore_errors=True)
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
_MIG_LOCKS = {}            # "ip:port" -> 过期时间戳（monotonic）
_MIG_LOCK_TTL = 6 * 3600   # 保险丝：极端情况下忘记解锁也能自动恢复（6 小时）
_mig_lock = threading.Lock()


def lock_target(host, port):
    """锁定迁移目标节点：暂停 monitor 对它（作为当前主）的拓扑维护。"""
    if not host:
        return
    with _mig_lock:
        _MIG_LOCKS["%s:%d" % (host, int(port))] = time.monotonic() + _MIG_LOCK_TTL


def unlock_target(host, port):
    """解除锁定（停桥之后交还 monitor 接管）。"""
    if not host:
        return
    with _mig_lock:
        _MIG_LOCKS.pop("%s:%d" % (host, int(port)), None)


def is_target_locked(host, port):
    """目标是否处于迁移锁定中 —— 由 failover.ensure_topology 调用。"""
    if not host:
        return False
    key = "%s:%d" % (host, int(port))
    with _mig_lock:
        exp = _MIG_LOCKS.get(key)
        if exp is None:
            return False
        if time.monotonic() > exp:  # 保险丝到期自动释放
            _MIG_LOCKS.pop(key, None)
            return False
        return True


def bridge(task):
    """在目标建立指向源的 MariaDB GTID 复制桥接（持续追平）。"""
    cfg = task["config"]
    # ⚠ 必须先锁定目标节点再建桥接，否则 monitor 的 promote_to_master 会立刻
    #   RESET REPLICA ALL 把通道清掉（详见上方「迁移锁定」根因注释）。
    lock_target(cfg["tgt_host"], int(cfg["tgt_port"]))
    acct = _ensure_source_repl_account(task)  # 源端复制账号（最小权限，必要时降级复用源账号）
    use_src = bool(acct.get("used_src_creds"))
    m_user = cfg["src_user"] if use_src else cfg["repl_user"]
    m_pass = cfg["src_pass"] if use_src else cfg["repl_pass"]
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
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
            pass
        # MariaDB GTID 桥接：MASTER_USE_GTID=slave_pos
        cur.execute(
            "CHANGE MASTER TO MASTER_HOST=%s, MASTER_PORT=%s, "
            "MASTER_USER=%s, MASTER_PASSWORD=%s, MASTER_USE_GTID=slave_pos",
            (cfg["src_host"], int(cfg["src_port"]), m_user, m_pass),
        )
        cur.execute("START REPLICA")
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


def verify(task):
    """逐表行数比对（源 vs 目标）+ 复制状态。"""
    cfg = task["config"]
    src = (cfg["src_host"], int(cfg["src_port"]), cfg["src_user"], cfg["src_pass"])
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
    databases = cfg.get("databases") or []
    diffs = []
    tables_checked = 0
    s_conn = connect(*src, timeout=8)
    t_conn = connect(*tgt, timeout=8)
    try:
        for db in databases:
            cs = s_conn.cursor()
            cs.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (db,))
            tbls = [x[0] for x in cs.fetchall()]
            for tbl in tbls:
                tables_checked += 1
                q = f"SELECT COUNT(*) FROM `{db}`.`{tbl}`"
                cs.execute(q)
                n1 = cs.fetchone()[0]
                ct = t_conn.cursor()
                ct.execute(q)
                n2 = ct.fetchone()[0]
                if n1 != n2:
                    diffs.append({"db": db, "table": tbl, "src": n1, "tgt": n2})
    finally:
        s_conn.close()
        t_conn.close()

    # 复制状态（MariaDB：SHOW REPLICA STATUS，兼容 Replica_/Slave_ 两种列名）
    repl = _replica_status(tgt)
    return {
        "ok": True,
        "tables_checked": tables_checked,
        "diffs": diffs,
        "consistent": len(diffs) == 0,
        "replica": repl,
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


def _register_target_node(cluster_id, host, port, root_pass, node_type, mysql_version):
    """把已装好的目标节点登记进 Custos（插入 nodes/hosts），不重新生成部署文件。"""
    server_id = node_gen.get_next_server_id()
    host_compose = f"/home/docker/{host}"
    execute(
        "INSERT INTO nodes (ip,port,root_user,root_pass,server_id,compose_path,cluster_id,node_type,mysql_version) "
        "VALUES (%s,%s,'root',%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE port=%s,root_pass=%s,server_id=%s,compose_path=%s,cluster_id=%s,node_type=%s,mysql_version=%s",
        (host, port, crypto.encrypt(root_pass), server_id, host_compose, cluster_id, node_type, mysql_version,
         port, crypto.encrypt(root_pass), server_id, host_compose, cluster_id, node_type, mysql_version),
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
    tgt = (tgt_host, tgt_port, "root", cfg["tgt_root_pass"])
    conn = connect(*tgt, timeout=8)
    try:
        cur = conn.cursor()
        cur.execute("STOP REPLICA")
        cur.execute("RESET REPLICA ALL")  # MariaDB：清除桥接通道，避免 monitor 误判
    finally:
        conn.close()
    node_type = cfg.get("node_type", "mariadb-docker")
    mysql_version = cfg.get("tgt_version") or None
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
                         node_type, mysql_version)
    # 桥接已在上面 STOP REPLICA + RESET REPLICA ALL 停掉，可解除迁移锁定，
    # 把目标交还给 monitor 正常接管（否则该集群的拓扑维护会一直被跳过）。
    unlock_target(tgt_host, tgt_port)
    if reused:
        note = ("已停桥、对齐凭证并登记进 Custos（沿用既有集群，新主不会被移出）；"
                "请在外部手动切换 VIP")
    else:
        note = ("已停桥、对齐凭证并登记进 Custos，monitor 将自动提升新主；"
                "请在外部手动切换 VIP")
    return {"ok": True, "cluster_id": cluster_id, "reused_cluster": reused, "note": note}


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
        "root", cfg["tgt_root_pass"], manager_ip, int(cfg["tgt_port"]),
    )
    conn = connect(cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"], timeout=8)
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
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"DROP USER IF EXISTS '{cfg['repl_user']}'@'%'")
        except Exception:
            pass
        try:
            cur.execute("SET GLOBAL read_only=ON")  # 老主置只读暖备，防误写
        except Exception:
            pass
    finally:
        conn.close()
    return {"ok": True, "note": "已回收源端复制账号，老主已置只读暖备（保留期后下线）"}


# ---------------------------------------------------------------------------
# 停桥前追平检测 + 跳过收尾
# ---------------------------------------------------------------------------
# 追平阈值（秒）：停桥意味着切断 177→203 的桥接，任何残留延迟都会变成永久数据丢失，
# 故取 0 —— 必须 Seconds_Behind_Master 收敛到 0 才允许切换。
LAG_CAUGHT_UP_SEC = 0


def check_lag(task):
    """停桥切换前的「追平检测」：确认目标已从源追平，避免带差切换导致数据丢失。

    纯只读，不改动任何节点。返回 caught_up 供前端放行「执行停桥切换」按钮。
    """
    cfg = task["config"]
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
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
    io_ok = str(repl.get("io_running") or "").lower() == "yes"
    sql_ok = str(repl.get("sql_running") or "").lower() == "yes"
    sb = repl.get("seconds_behind")
    last_err = repl.get("last_error")
    if not sql_ok:
        reason = "SQL 线程未运行（复制未工作）"
    elif not io_ok:
        reason = "IO 线程未运行（未从源拉取 binlog）"
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
    return {"ok": True, "caught_up": not reason, "io_running": repl.get("io_running"),
            "sql_running": repl.get("sql_running"), "seconds_behind": sb,
            "last_error": last_err,
            "reason": reason or "已追平（Seconds_Behind_Master=%s）" % sb, "replica": repl}


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


class _Reporter:
    def __init__(self, tid):
        self.tid = tid

    def log(self, msg):
        execute("UPDATE migration_tasks SET log=CONCAT(IFNULL(log,''),%s),updated_at=NOW() "
                "WHERE id=%s", (f"[{_now()}] {msg}\n", self.tid))

    def prog(self, pct, stage=None):
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


def _thread_body(tid, step):
    rep = _Reporter(tid)
    try:
        rep.prog(0, "正在启动…")
        rep.log("开始步骤：%s" % STEP_LABELS.get(step, step))
        result = _execute_step(tid, step)
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


def start_step(task_id, step):
    """异步启动一个步骤，HTTP 立即返回（真正的活交给后台线程）。"""
    task = get_task(task_id)
    if not task:
        return {"ok": False, "error": "任务不存在"}
    if step not in STEPS:
        return {"ok": False, "error": f"未知步骤: {step}"}
    if task_id in _RUNNING:
        return {"ok": False, "error": "该任务已有步骤正在执行，请等待完成后再操作"}
    _RUNNING.add(task_id)
    _REP[task_id] = {"rep": _Reporter(task_id), "step": step}
    execute("UPDATE migration_tasks SET status='running',updated_at=NOW() WHERE id=%s", (task_id,))
    threading.Thread(target=_thread_body, args=(task_id, step), daemon=True).start()
    return {"ok": True, "started": True, "step": step}


def run_step(task_id, step):
    """同步执行一步（脚本/测试用；Web 请走 start_step）。"""
    try:
        result = _execute_step(task_id, step)
        _finish_step(task_id, step, True, payload=result)
        return {"ok": True, "step": step, **result}
    except Exception as e:
        _finish_step(task_id, step, False, err=e)
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _execute_step(task_id, step):
    """真正干活（不含线程/落库）。"""
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
            return seed_physical(task)
        return seed_logical(task)
    if step == "bridge":
        if not cap["bridge"]:
            raise RuntimeError("该组合不支持 GTID 桥接（跨产品线 GTID 格式不兼容），"
                               "请在停写窗口内一次性拷贝后直接切换。")
        return bridge(task)
    if step == "verify":
        return verify(task)
    if step == "cutover":
        return cutover(task)
    if step == "cleanup":
        return cleanup(task)
    raise ValueError("未知步骤")
