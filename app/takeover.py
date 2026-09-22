"""takeover.py — 旧集群接管（v2.4.1）。

接管 = 把既有的、非 Custos 管理的 MySQL/MariaDB 集群纳入 Custos 托管，并配置 Custos VIP。
两种模式（产品决策已锁定）：
  A 平行新建+切换：新建干净 Custos 集群作目标，旧→新 复制追平后 Custos 接管 VIP（复用 migration 流程）。
  B 原地纳管：Custos 就地纳管旧节点（拓扑发现 + 装 sidecar(agent+keepalived) + 登记 + 配 VIP + 接管复制/切换）。

本模块实现「公共骨架」（两模式共用）：
  · 任务持久化（独立 takeover_tasks 表，不污染 migration_tasks）
  · 旧集群连接 / 拓扑发现（主库看从库清单）
  · 服务器开关审计（audit_server_settings，复制/GTID/可观测必需项）
  · 能力门禁（takeover_capabilities：同版本、GTID、拒 Galera/组复制、外部 HA 复核）

A/B 两模式的「部署 sidecar / 调迁移 / 配 VIP / 停写翻转」在后续阶段接入。
"""
import json
import os
import shlex
import subprocess
import tempfile
import threading

from .db import query, execute
from . import crypto
from . import migration as _migration
from . import node_gen as _node_gen
from .ha_client import (
    connect, ensure_ha_creds, make_slave_of, set_read_only, set_local_master_flag, get_status,
)
from .mysql_compat import (
    detect_flavour, detect_syntax, is_mariadb, parse_version, is_mysql_node, node_family,
)

_lock = threading.Lock()

# Custos 自己的库，接管发现用户库时排除
SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys", "ndbinfo"}
IGNORE_DBS = SYSTEM_DBS | {"ha_local"}


# ---------------------------------------------------------------------------
# 任务持久化（独立 takeover_tasks 表）
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


def save_task(task):
    cfg = _enc(task.get("config", {}))
    steps = json.dumps(task.get("steps", {}), ensure_ascii=False)
    if task.get("id"):
        execute(
            "UPDATE takeover_tasks SET mode=%s,status=%s,config=%s,steps=%s,log=%s,updated_at=NOW() "
            "WHERE id=%s",
            (task.get("mode"), task["status"], cfg, steps, task.get("log", ""), task["id"]),
        )
    else:
        new_id = execute(
            "INSERT INTO takeover_tasks (name, mode, status, config, steps, log, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,'',NOW(),NOW())",
            (task["name"], task.get("mode"), task["status"], cfg, steps),
        )
        task["id"] = new_id


def get_task(tid):
    rows = query("SELECT * FROM takeover_tasks WHERE id=%s", (tid,))
    if not rows:
        return None
    t = rows[0]
    return {
        "id": t["id"], "name": t["name"], "mode": t.get("mode") or "A",
        "status": t.get("status") or "draft", "progress": int(t.get("progress") or 0),
        "stage": t.get("stage") or "", "log": t.get("log") or "",
        "config": _dec(t.get("config")), "steps": json.loads(t.get("steps") or "{}"),
        "created_at": t.get("created_at") or "", "updated_at": t.get("updated_at") or "",
    }


def list_tasks():
    return query(
        "SELECT id,name,mode,status,progress,stage,created_at,updated_at "
        "FROM takeover_tasks ORDER BY id DESC"
    )


def delete_task(tid):
    execute("DELETE FROM takeover_tasks WHERE id=%s", (tid,))


# ---------------------------------------------------------------------------
# 旧集群连接 / 拓扑发现
# ---------------------------------------------------------------------------
def _connect_old(cfg):
    return connect(cfg["old_host"], int(cfg["old_port"]),
                   cfg["old_user"], cfg["old_pass"], timeout=cfg.get("old_timeout", 8))


def probe_old_cluster(cfg):
    """连旧集群主库，探测版本/flavor/GTID domain/拓扑/开关设置，返回结构化报告。"""
    conn = _connect_old(cfg)
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
        flavour = detect_flavour(cur)
        syntax = detect_syntax(cur)
        major = parse_version(version)
        family = "mariadb" if is_mariadb(version) else "mysql"

        def _var(sql):
            try:
                cur.execute(sql)
                r = cur.fetchone()
                return r[0] if r else None
            except Exception:
                return None

        info = {
            "ok": True,
            "version": version,
            "flavour": flavour,
            "family": family,
            "major": list(major),
            "gtid_domain_id": _var("SELECT @@gtid_domain_id") if family == "mariadb" else None,
            "gtid_strict_mode": _var("SELECT @@gtid_strict_mode") if family == "mariadb" else None,
            "gtid_mode": _var("SELECT @@gtid_mode") if family != "mariadb" else None,
            "wsrep_on": _var("SELECT @@wsrep_on"),
            "server_id": _var("SELECT @@server_id"),
            "log_bin": _var("SELECT @@log_bin"),
        }
        # 拓扑发现：主库看从库清单（MySQL 用 SHOW REPLICAS，MariaDB 用 SHOW SLAVE HOSTS）
        replicas = []
        try:
            if syntax == "new":
                cur.execute("SHOW REPLICAS")
            else:
                cur.execute("SHOW SLAVE HOSTS")
            cols = [d[0].lower() for d in cur.description]
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                try:
                    port = int(d.get("port") or 3306)
                except Exception:
                    port = 3306
                replicas.append({
                    "host": d.get("host"),
                    "port": port,
                    "server_id": d.get("server_id"),
                })
        except Exception:
            pass
        info["replicas"] = replicas
        info["replica_count"] = len(replicas)
        # 服务器开关审计
        info["settings"] = audit_server_settings(cur, family)
        return info
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 服务器开关审计（MySQL / MariaDB 名映射）
# ---------------------------------------------------------------------------
# 通用必需项：（var, 期望判定, 说明, 缺失自助/自动动作, required）
_COMMON_SETTINGS = [
    ("log_bin", lambda v: str(v).upper() == "ON", "二进制日志开启（复制必需）", "my.cnf 配置 log-bin 并重启"),
    ("binlog_format", lambda v: str(v).upper() == "ROW", "binlog 行格式（GTID/复制一致）", "SET GLOBAL binlog_format=ROW"),
    ("log_slave_updates", lambda v: str(v).upper() == "ON", "从库写 binlog（级联复制 / Custos 重指）", "SET GLOBAL log_slave_updates=ON"),
    ("relay_log_recovery", lambda v: str(v).upper() == "ON", "中继日志崩溃恢复", "SET GLOBAL relay_log_recovery=ON"),
    ("binlog_row_image", lambda v: str(v).upper() in ("FULL", "1"), "binlog 行镜像完整", "SET GLOBAL binlog_row_image=FULL"),
    ("slow_query_log", lambda v: str(v).upper() == "ON", "慢查询日志（Custos 慢日志分析）", "SET GLOBAL slow_query_log=ON"),
    ("performance_schema", lambda v: str(v).upper() == "ON", "性能 schema（诊断）", "需 my.cnf 开启并重启"),
    ("default_storage_engine", lambda v: str(v).upper() == "INNODB", "默认引擎 InnoDB", "SET GLOBAL default_storage_engine=INNODB"),
    ("skip_name_resolve", lambda v: str(v).upper() == "ON", "跳过 DNS 解析（账号 host 解析稳定）", "SET GLOBAL skip_name_resolve=ON"),
]

# MariaDB 专用 GTID 项
_MARIADB_GTID = [
    ("gtid_strict_mode", lambda v: str(v).upper() == "ON", "MariaDB GTID 严格模式", "SET GLOBAL gtid_strict_mode=ON"),
]

# MySQL 专用 GTID 项
_MYSQL_GTID = [
    ("gtid_mode", lambda v: str(v).upper() == "ON", "MySQL GTID 模式开启", "SET GLOBAL gtid_mode=ON（需逐级升级）"),
    ("enforce_gtid_consistency", lambda v: str(v).upper() == "ON", "MySQL GTID 一致性强制", "SET GLOBAL enforce_gtid_consistency=ON"),
]

# Custos 接管后自管，无需手动（仅展示）
_CUSTOS_MANAGED = [
    ("read_only", None, "只读（Custos 接管后按角色管理，当前视原集群角色而定）", "由 Custos 管理，无需手动"),
    ("super_read_only", None, "超级只读（Custos 接管后从库置位）", "由 Custos 管理，无需手动"),
]


def audit_server_settings(cur, family):
    """读源库 SHOW VARIABLES 当前值，逐项比对，给出满足/缺失报告。"""
    vars_map = {}
    try:
        cur.execute("SHOW VARIABLES")
        for name, val in cur.fetchall():
            vars_map[name.lower()] = val
    except Exception:
        return []
    spec = list(_COMMON_SETTINGS)
    spec += _MARIADB_GTID if family == "mariadb" else _MYSQL_GTID
    spec += _CUSTOS_MANAGED
    report = []
    for var, check, desc, action in spec:
        cur_val = vars_map.get(var.lower())
        if check is None:
            report.append({"var": var, "current": cur_val, "ok": None,
                           "desc": desc, "action": action, "required": False})
            continue
        ok = check(cur_val) if cur_val is not None else False
        report.append({"var": var, "current": cur_val, "ok": ok,
                       "desc": desc, "action": action, "required": True})
    return report


# ---------------------------------------------------------------------------
# 能力门禁
# ---------------------------------------------------------------------------
def takeover_capabilities(probe):
    """基于旧集群探测结果判定可否接管。返回 {ok, reasons[], warnings[], checks{}}。"""
    checks = {}
    reasons = []
    warnings = []

    # 1) Galera / 组复制 → 拒绝
    if str(probe.get("wsrep_on")).upper() == "ON":
        reasons.append("检测到 Galera/组复制（wsrep_on=ON），当前版本不支持接管，请先退出 Galera 集群。")
        checks["galera"] = False
    else:
        checks["galera"] = True

    # 2) GTID 必需
    fam = probe.get("family")
    checks["family"] = fam
    if fam == "mariadb":
        # ⚠ MariaDB 的 gtid_domain_id 默认就是 0（永远 is not None），不能只判断它；
        # 真正判定 GTID 是否启用必须看 gtid_strict_mode=ON（make_slave_of 用
        # MASTER_AUTO_POSITION=1 强依赖 GTID，非 GTID 主库会复制起不来）。
        checks["gtid"] = bool(probe.get("log_bin")
                              and str(probe.get("gtid_strict_mode")).upper() == "ON")
    else:
        checks["gtid"] = str(probe.get("gtid_mode")).upper() == "ON"
    if not checks["gtid"]:
        if fam == "mariadb":
            reasons.append("未启用 GTID（需 log_bin=ON 且 gtid_strict_mode=ON），接管要求 GTID 复制；"
                           "请先在旧主 SET GLOBAL gtid_strict_mode=ON 并确认 binlog 已开启。")
        else:
            reasons.append("未启用 GTID（需 gtid_mode=ON + enforce_gtid_consistency=ON），接管要求 GTID 复制。")

    # 3) 外部 HA（MHA/keepalived）需 SSH 复核 —— 首版给 pending 警告
    warnings.append("旧集群若自带 MHA/keepalived 等外部 HA，请在部署接管前卸载，否则 VIP 冲突；"
                    "可在「部署 sidecar」步骤经 SSH 复核。")

    # 4) 逐节点版本一致性 —— 首版仅记录主库 family，从库一致性在部署阶段逐节点复核
    warnings.append("同集群各节点须同版本/同 flavor（MariaDB↔MariaDB 或 MySQL↔MySQL，禁混）；"
                    "从库一致性将在部署阶段逐节点复核。")

    ok = (not reasons)
    return {"ok": ok, "reasons": reasons, "warnings": warnings, "checks": checks}


# ---------------------------------------------------------------------------
# 任务创建
# ---------------------------------------------------------------------------
def create_task(cfg):
    task = {
        "name": cfg.get("name") or "未命名接管",
        "mode": cfg.get("mode") or "A",
        "status": "draft",
        "progress": 0,
        "stage": "已创建",
        "config": cfg,
        "steps": {},
        "log": "",
    }
    save_task(task)
    return task


# ---------------------------------------------------------------------------
# SSH 部署助手（接管初期旧节点无 agent，只能 SSH；agent-exec 仅 SSH 失败兜底）
# ---------------------------------------------------------------------------
def ssh_port_for(node, cfg):
    """SSH 端口优先级：节点显式 > 任务级 old_ssh_port > 默认 22。"""
    return int(node.get("ssh_port") or cfg.get("old_ssh_port") or 22)


def _ssh_run(host, script, ssh_user, ssh_pass, ssh_port):
    """经 sshpass SSH 到节点执行脚本；失败抛 RuntimeError。"""
    if not ssh_pass:
        raise RuntimeError("缺少 SSH 密码，无法推送 sidecar（接管初期旧节点无 agent）")
    cmd = [
        "sshpass", "-p", ssh_pass, "ssh",
        "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=15", "-p", str(ssh_port),
        f"{ssh_user}@{host}", "bash -s",
    ]
    p = subprocess.run(cmd, input=script.encode("utf-8"), capture_output=True, timeout=600)
    if p.returncode != 0:
        raise RuntimeError("SSH 执行失败(%s:%s): %s" % (host, ssh_port, p.stderr.decode("utf-8", "replace")[:800]))
    return p.stdout.decode("utf-8", "replace")


def _rsync_push(local_dir, host, remote_dir, ssh_user, ssh_pass, ssh_port):
    """rsync 整个节点目录推到目标机（含 keepalived + agent + sidecar compose）。"""
    if not ssh_pass:
        raise RuntimeError("缺少 SSH 密码，无法推送 sidecar")
    cmd = [
        "sshpass", "-p", ssh_pass, "rsync", "-az", "--delete",
        "-e", "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p %s" % ssh_port,
        local_dir + "/", f"{ssh_user}@{host}:{remote_dir}/",
    ]
    p = subprocess.run(cmd, capture_output=True, timeout=600)
    if p.returncode != 0:
        raise RuntimeError("rsync 推送失败(%s): %s" % (host, p.stderr.decode("utf-8", "replace")[:800]))


def _node_dir(ip):
    """取 generate_node 写出的节点目录（与 node_gen.NODES_DIR 一致）。"""
    from pathlib import Path
    base = Path(__file__).resolve().parent.parent / "generated_nodes"
    return base / ip


# ---------------------------------------------------------------------------
# Custos 集群登记（Mode B 接管时新建；Mode A 复用既有目标集群）
# ---------------------------------------------------------------------------
def _create_custos_cluster(name, vip, family, port=3306, vip_prefix="24", interface="eth0", vrid="51"):
    from .config import rand_secret
    cid = execute(
        "INSERT INTO clusters (name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass, "
        "vip, vip_prefix, interface, vrid, vip_port, node_type, created_at) "
        "VALUES (%s,'ha_monitor',%s,'replication',%s,'app',%s,%s,%s,%s,%s,%s,%s,NOW())",
        (name, rand_secret(16), rand_secret(16), rand_secret(16), vip, vip_prefix, interface, vrid,
         port, family),
    )
    return cid


# ---------------------------------------------------------------------------
# 部署 sidecar：把 Custos agent + keepalived（漂移 Custos VIP）推到旧节点
# ---------------------------------------------------------------------------
def deploy_sidecar(task):
    cfg = task["config"]
    cid = cfg.get("cluster_id")
    if not cid:
        raise RuntimeError("接管缺少目标 Custos 集群（cluster_id）")
    nodes = cfg.get("nodes") or []
    if not nodes:
        raise RuntimeError("接管缺少旧集群节点清单（config.nodes）")
    logs = []
    for n in nodes:
        host = n["host"]
        port = int(n.get("port") or 3306)
        family = node_family(n.get("node_type") or ("mariadb" if n.get("family") == "mariadb" else "mysql"))
        nt = normalize_node_type(n.get("node_type") or ("mariadb" if family == "mariadb" else "mysql"))
        try:
            # 生成 sidecar 文件（adopt：不建库、不跑 Install，仅 agent+keepalived+VIP）
            _node_gen.generate_node(
                host, port, n.get("user", "root"), n.get("pass", ""),
                initial_role=("master" if n.get("is_master") else "slave"),
                cluster_id=cid, node_type=nt, adopt=True,
                mysql_version=n.get("mysql_version"),
                gtid_domain_id=cfg.get("gtid_domain_id"),
            )
            d = _node_dir(host)
            ssh_user = n.get("ssh_user") or cfg.get("old_ssh_user") or "root"
            ssh_pass = n.get("ssh_pass") or cfg.get("old_ssh_pass") or ""
            ssh_port = ssh_port_for(n, cfg)
            remote = "/home/docker/%s" % host
            _rsync_push(str(d), host, remote, ssh_user, ssh_pass, ssh_port)
            # 启动 sidecar（keepalived + agent，host 网络漂移 Custos VIP）
            _ssh_run(host, "mkdir -p %s && cd %s && docker compose up -d\n" % (remote, remote),
                     ssh_user, ssh_pass, ssh_port)
            logs.append("✓ %s sidecar 已部署" % host)
        except Exception as e:
            logs.append("✗ %s 部署失败: %s" % (host, e))
            raise
    return {"ok": True, "logs": logs}


# ---------------------------------------------------------------------------
# 模式 A：平行新建 + 切换（复用 migration：seed→bridge→verify→cutover）
# ---------------------------------------------------------------------------
def run_mode_a(task):
    cfg = task["config"]
    cid = cfg.get("target_cluster_id")
    if not cid:
        raise RuntimeError("模式 A 需指定目标 Custos 集群（target_cluster_id）")
    rows = query("SELECT id, ip, port, root_user, root_pass FROM nodes WHERE cluster_id=%s ORDER BY id", (cid,))
    if not rows:
        raise RuntimeError("目标 Custos 集群尚无节点，请先添加节点")
    tgt = rows[0]
    tgt_host, tgt_port = tgt["ip"], int(tgt["port"])
    mcfg = {
        "src_host": cfg["old_host"], "src_port": int(cfg["old_port"]),
        "src_user": cfg["old_user"], "src_pass": cfg["old_pass"],
        "src_gtid": True,
        "tgt_host": tgt_host, "tgt_port": tgt_port,
        "tgt_root_pass": (crypto.decrypt(tgt["root_pass"]) if tgt["root_pass"] else ""),
        "tgt_user": tgt.get("root_user") or "root",
        "tgt_version": cfg.get("old_version"), "src_flavor": cfg.get("old_family"),
        "tgt_flavor": cfg.get("target_family") or cfg.get("old_family"),
        "node_type": normalize_node_type(cfg.get("target_node_type") or "mariadb-docker"),
        "databases": cfg.get("databases") or [],
        "repl_user": "replication", "repl_pass": "",
        "src_ssh_port": cfg.get("old_ssh_port", 22),
        # 目标集群是独立的 Custos 集群，SSH 端口未必和旧集群相同，须独立配置
        "tgt_ssh_port": cfg.get("target_ssh_port", cfg.get("old_ssh_port", 22)),
    }
    from .config import rand_secret
    mcfg["repl_pass"] = rand_secret(16)
    mt = {"name": task["name"] + "-mig", "channel": cfg.get("channel", "logical"),
          "status": "draft", "config": mcfg, "steps": {}, "log": ""}
    _migration.save_task(mt)
    mtid = mt["id"]
    cfg["migration_task_id"] = mtid
    save_task(task)
    # 顺序驱动迁移各步（迁移自身为后台线程，这里轮询其状态）
    for step in ("seed", "bridge", "verify", "cutover"):
        _migration.start_step(mtid, step)
        while True:
            st = _migration.get_task(mtid)
            s = (st.get("steps") or {}).get(step) or {}
            if s.get("status") == "done":
                break
            if s.get("status") == "error":
                raise RuntimeError("迁移步骤 %s 失败: %s" % (step, s.get("error")))
            import time as _t
            _t.sleep(3)
    # 目标集群其余节点接成新主（tgt）的从库，避免「只迁到 1 个节点、其余节点空转」
    logs = []
    if len(rows) > 1:
        cc = query("SELECT ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass "
                   "FROM clusters WHERE id=%s", (cid,))
        if cc:
            cc0 = cc[0]
            from .config import get_manager_public_ip, get_manager_docker_subnet_wildcard
            mip = get_manager_public_ip(); msub = get_manager_docker_subnet_wildcard()
            for tn in rows[1:]:
                try:
                    _tp = crypto.decrypt(tn["root_pass"]) if tn["root_pass"] else ""
                    _tu = tn.get("root_user") or "root"
                    ensure_ha_creds(tn["ip"], int(tn["port"]), _tu, _tp,
                                    cc0["ha_user"], cc0["ha_pass"], cc0["repl_user"],
                                    cc0["repl_pass"], cc0["app_user"], cc0["app_pass"], mip, msub)
                    make_slave_of(tn["ip"], int(tn["port"]), _tu, _tp,
                                  tgt_host, tgt_port, cc0["repl_user"], cc0["repl_pass"],
                                  _tu, _tp)
                    logs.append("✓ 目标节点 %s 已接为新主 %s 的从库" % (tn["ip"], tgt_host))
                except Exception as e:
                    logs.append("✗ 目标节点 %s 接从库失败: %s" % (tn["ip"], e))
            task["log"] = (task.get("log") or "") + "\n" + "\n".join(logs)
            save_task(task)
    return {"ok": True, "migration_task_id": mtid, "logs": logs}


# ---------------------------------------------------------------------------
# 模式 B：原地纳管（登记 + 建 Custos 集群 + 部署 sidecar + 接管复制/切换）
# ---------------------------------------------------------------------------
def run_mode_b(task):
    cfg = task["config"]
    cid = cfg.get("cluster_id")
    if not cid:
        cid = _create_custos_cluster(
            cfg.get("cluster_name") or (task["name"] + "-custos"),
            cfg["new_vip"], cfg.get("old_family") or "mariadb",
            port=int(cfg.get("old_port") or 3306),
        )
        cfg["cluster_id"] = cid
    # 1) 登记旧节点 + 部署 sidecar
    deploy_sidecar(task)
    nodes = cfg.get("nodes") or []
    # 2) 接管复制/切换：注入账号、从库只读、主库持有 VIP
    master = next((n for n in nodes if n.get("is_master")), None)
    if not master:
        raise RuntimeError("未指定主库节点（is_master=true）")
    creds = query("SELECT ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass, vip "
                  "FROM clusters WHERE id=%s", (cid,))
    if not creds:
        raise RuntimeError("目标 Custos 集群凭证缺失，无法注入账号")
    c = creds[0]
    from .config import get_manager_public_ip, get_manager_docker_subnet_wildcard
    mip = get_manager_public_ip(); msub = get_manager_docker_subnet_wildcard()
    logs = []
    # 2.1) 【关键】每个旧节点（主+从）都要注入 Custos 账号。只建主库的后果是从库
    #      无 ha_monitor/app → 管理器无法监控/故障切换从库（从库失管）。
    for n in nodes:
        try:
            ensure_ha_creds(n["host"], int(n["port"]), n.get("user", "root"), n.get("pass", ""),
                            c["ha_user"], c["ha_pass"], c["repl_user"], c["repl_pass"],
                            c["app_user"], c["app_pass"], mip, msub)
            logs.append("✓ %s 已注入 Custos 账号(ha_monitor/replication/app)" % n["host"])
        except Exception as e:
            logs.append("✗ %s 账号注入失败: %s" % (n["host"], e))
    # 2.2) 从库只读 + 指向主库（重建 Custos 可管理的复制通道）
    for n in nodes:
        if n.get("is_master"):
            continue
        try:
            set_read_only(n["host"], int(n["port"]), n.get("user", "root"), n.get("pass", ""), True,
                         n.get("user", "root"), n.get("pass", ""))
            make_slave_of(n["host"], int(n["port"]), n.get("user", "root"), n.get("pass", ""),
                          master["host"], int(master["port"]), c["repl_user"], c["repl_pass"],
                          n.get("user", "root"), n.get("pass", ""))
            logs.append("✓ %s 已设为从库并指向主库 %s" % (n["host"], master["host"]))
        except Exception as e:
            # 不再静默吞：记录到任务日志，便于排错（旧集群已有复制则提示即可）
            logs.append("✗ %s 重指从库失败(需人工确认): %s" % (n["host"], e))
    # 2.3) 主库持有 Custos VIP：设置本地主库标志（keepalived 据此漂移 VIP）
    try:
        set_local_master_flag(master["host"], int(master["port"]), c["ha_user"], c["ha_pass"],
                              "%s:%s" % (master["host"], master["port"]),
                              master.get("user", "root"), master.get("pass", ""))
        logs.append("✓ 主库 %s 已设置本地主库标志（keepalived 据此漂移 VIP）" % master["host"])
    except Exception as e:
        logs.append("✗ 主库标志设置失败: %s" % e)
    task["log"] = (task.get("log") or "") + "\n" + "\n".join(logs)
    save_task(task)
    return {"ok": True, "cluster_id": cid, "logs": logs}


# ---------------------------------------------------------------------------
# VIP 翻转（秒级）：确保 Custos VIP 漂到当前主库（模式 B）；模式 A 由迁移 cutover 完成
# ---------------------------------------------------------------------------
def flip_vip(task):
    cfg = task["config"]
    if (cfg.get("mode") or "A") == "A":
        return {"ok": True, "note": "模式 A 的 VIP 翻转由迁移 cutover 完成"}
    master = next((n for n in (cfg.get("nodes") or []) if n.get("is_master")), None)
    if not master:
        raise RuntimeError("未指定主库节点")
    ssh_user = master.get("ssh_user") or cfg.get("old_ssh_user") or "root"
    ssh_pass = master.get("ssh_pass") or cfg.get("old_ssh_pass") or ""
    ssh_port = ssh_port_for(master, cfg)
    remote = "/home/docker/%s" % master["host"]
    _ssh_run(master["host"], "cd %s && docker compose up -d keepalived\n" % remote,
             ssh_user, ssh_pass, ssh_port)
    return {"ok": True, "master": master["host"]}


# ---------------------------------------------------------------------------
# 撤销接管（可逆）：卸载 sidecar + 删除 Custos 账号 + 注销登记
# ---------------------------------------------------------------------------
def undo_takeover(task):
    cfg = task["config"]
    logs = []
    for n in cfg.get("nodes") or []:
        host = n["host"]
        ssh_user = n.get("ssh_user") or cfg.get("old_ssh_user") or "root"
        ssh_pass = n.get("ssh_pass") or cfg.get("old_ssh_pass") or ""
        ssh_port = ssh_port_for(n, cfg)
        remote = "/home/docker/%s" % host
        try:
            # 停止并移除 sidecar（keepalived + agent），保留旧集群数据库
            _ssh_run(host, "cd %s && docker compose down 2>/dev/null; rm -rf %s\n" % (remote, remote),
                     ssh_user, ssh_pass, ssh_port)
            # 删除 Custos 接管注入的账号。
            # ⚠ 不能用写死的 '@%'（账号策略可能按网段/172.28/127.0.0.1 建号，
            # 也可能 legacy 用 '%'），必须按 mysql.user 实际 host 逐个删除，
            # 否则子网下建的账号删不干净 → 旧库残留 Custos 账号。
            try:
                conn = connect(host, int(n["port"]), n.get("user", "root"), n.get("pass", ""),
                               timeout=5)
                cur = conn.cursor()
                for u in ("ha_monitor", "replication", "app"):
                    try:
                        cur.execute("SELECT host FROM mysql.user WHERE user=%s", (u,))
                        for (h,) in cur.fetchall():
                            cur.execute("DROP USER IF EXISTS '%s'@'%s'"
                                        % (u.replace("'", "''"), str(h).replace("'", "''")))
                    except Exception as e:
                        logs.append("  (清理账号 %s@* 失败: %s)" % (u, e))
                conn.commit(); conn.close()
            except Exception:
                pass
            logs.append("✓ %s 已撤销接管（sidecar 卸载 + Custos 账号删除）" % host)
        except Exception as e:
            logs.append("✗ %s 撤销失败: %s" % (host, e))
    cid = cfg.get("cluster_id")
    if cid:
        execute("DELETE FROM nodes WHERE cluster_id=%s", (cid,))
        execute("DELETE FROM hosts WHERE cluster_id=%s", (cid,))
        execute("DELETE FROM clusters WHERE id=%s", (cid,))
        logs.append("✓ 已注销 Custos 集群 #%s 登记" % cid)
    return {"ok": True, "logs": logs}


# ---------------------------------------------------------------------------
# 步骤编排（前端逐步调用，天然支持重试/暂停；后台线程执行 + 轮询进度）
# ---------------------------------------------------------------------------
STEPS_BY_MODE = {
    "A": ["prepare_target", "seed", "bridge", "verify", "cutover", "vip"],
    "B": ["register", "deploy_sidecar", "takeover_repl", "vip"],
}

_STEP_FN = {
    "prepare_target": None,          # 模式 A：目标集群由用户在「集群管理」预先创建并选入
    "seed": run_mode_a,              # 模式 A 由 run_mode_a 内部顺序驱动全部迁移步
    "bridge": None, "verify": None, "cutover": None,  # 这些在 run_mode_a 内完成
    "register": run_mode_b,          # 模式 B：建集群 + 登记 + 部署 sidecar + 接管复制
    "deploy_sidecar": None,          # 在 run_mode_b 内完成
    "takeover_repl": None,           # 在 run_mode_b 内完成
    "vip": flip_vip,
}


def _run_step_thread(task_id, step):
    try:
        t = get_task(task_id)
        mode = t["mode"]
        # 模式 A：seed 一步即触发整条迁移链；其余 A 步骤为占位（已在 seed 内完成）
        fn = _STEP_FN.get(step)
        if fn is None:
            # 占位步骤：标记完成（实际工作由关联步骤完成）
            _mark_step(t, step, "done", {"note": "由关联步骤完成"})
        else:
            res = fn(t)
            _mark_step(t, step, "done", res)
        # 进度 = 已完成步 / 总步
        total = len(STEPS_BY_MODE.get(mode, []))
        done = sum(1 for s in (t["steps"] or {}).values() if s.get("status") == "done")
        t["progress"] = int(done / total * 100) if total else 0
        if t["progress"] >= 100:
            t["status"] = "done"
            t["stage"] = "接管完成"
        save_task(t)
    except Exception as e:
        t = get_task(task_id)
        _mark_step(t, step, "error", {"error": str(e)})
        t["status"] = "error"
        t["stage"] = "步骤失败: %s" % step
        t["log"] = (t.get("log") or "") + "\n[error] %s: %s" % (step, e)
        save_task(t)


def _mark_step(task, step, status, result):
    steps = task.get("steps") or {}
    steps[step] = {"status": status, "at": _now(), "result": result}
    task["steps"] = steps


def _now():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def start_step(task_id, step):
    t = get_task(task_id)
    if not t:
        return {"ok": False, "error": "任务不存在"}
    mode = t["mode"]
    if step not in STEPS_BY_MODE.get(mode, []):
        return {"ok": False, "error": "该模式无步骤 %s" % step}
    t["status"] = "running"
    t["stage"] = "执行: %s" % step
    save_task(t)
    threading.Thread(target=_run_step_thread, args=(task_id, step), daemon=True).start()
    return {"ok": True, "started": True, "step": step}


def normalize_node_type(node_type):
    from .mysql_compat import normalize_node_type as _n
    return _n(node_type)


def render_agent_install_sh(manager_base, family="mariadb"):
    """生成接管 sidecar 的「统一安装源」脚本：节点 curl 后 bash 执行。

    自判 OS（apt/yum/dnf）确保 python3 → 从管理器拉取 node_agent.py 与 systemd 服务
    → 安装并启用 custos-node-agent。keepalived（VIP 漂移）由节点包经 SSH 推送的
    compose 接管；本脚本只负责 agent 这一侧，作为 scp 失败时的兜底统一源。
    """
    return (
        "#!/bin/bash\n"
        "# Custos 接管 sidecar 安装（agent 侧，统一源）。keepalived 由节点包 compose 提供。\n"
        "set -eu\n"
        "MGR='" + (manager_base or "") + "'\n"
        "FAM='" + (family or "mariadb") + "'\n"
        "AGENT_DIR=/opt/custos-node-agent\n"
        "echo \"[custos-agent] 安装节点 agent（管理器 $MGR）...\"\n"
        "# 1) 确保 python3\n"
        "if ! command -v python3 >/dev/null 2>&1; then\n"
        "  if command -v apt-get >/dev/null 2>&1; then apt-get update -y && apt-get install -y python3; \n"
        "  elif command -v dnf >/dev/null 2>&1; then dnf install -y python3; \n"
        "  elif command -v yum >/dev/null 2>&1; then yum install -y python3; \n"
        "  else echo '未识别包管理器，请手动安装 python3'; exit 1; fi\n"
        "fi\n"
        "mkdir -p $AGENT_DIR\n"
        "# 2) 拉取 agent 与 systemd 服务\n"
        "curl -fsSL \"${MGR}/agent/asset/node_agent.py\" -o $AGENT_DIR/node_agent.py\n"
        "curl -fsSL \"${MGR}/agent/asset/node-agent.service\" -o /etc/systemd/system/custos-node-agent.service\n"
        "chmod 0755 $AGENT_DIR/node_agent.py\n"
        "# 3) 启用并启动\n"
        "systemctl daemon-reload\n"
        "systemctl enable custos-node-agent\n"
        "systemctl restart custos-node-agent\n"
        "echo '[custos-agent] 完成。节点 agent 已启动，管理器可经 28889 端口探活与下发。'\n"
    )
