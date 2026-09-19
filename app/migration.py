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
    acct = _ensure_source_repl_account(task)  # 源端复制账号（最小权限，必要时降级复用源账号）
    use_src = bool(acct.get("used_src_creds"))
    m_user = cfg["src_user"] if use_src else cfg["repl_user"]
    m_pass = cfg["src_pass"] if use_src else cfg["repl_pass"]
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
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
    tgt = (cfg["tgt_host"], int(cfg["tgt_port"]), "root", cfg["tgt_root_pass"])
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
            tc = connect(cfg["tgt_host"], int(cfg.get("tgt_port") or 3306), "root",
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
        return {"ok": False, "error": "已有迁移任务正在执行（%s），请等待其完成后再操作" % busy}
    _RUNNING.add(task_id)
    _REP[task_id] = {"rep": _Reporter(task_id), "step": step}
    execute("UPDATE migration_tasks SET status='running',updated_at=NOW() WHERE id=%s", (task_id,))
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
            return seed_physical(task)
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
