# -*- coding: utf-8 -*-
"""selfcheck.py — 启动期自检闸门（v2.5 改造，Phase 3，稳定+安全）。

管理器/节点启动后跑最小自检；不过则明确报错并阻断「就绪/可写」，避免带病运行。
对应 MR3/MR4：初始化与运行时一致、权限齐备、弱口令暴露。
"""
import logging
import time

from app.config import is_weak_password, WEB_PASS
from app.db import get_conn
from app.ha_client import connect
from app.ha_client.db_ops import has_privilege
from app.bootstrap import available_images

log = logging.getLogger(__name__)

# 节点接入必须齐备的特权（能力探测，不按版本硬编码）
NODE_REQUIRED_PRIVS = (
    "REPLICATION SLAVE ADMIN", "READ ONLY ADMIN", "BINLOG ADMIN", "REPLICATION SLAVE",
)


def selfcheck_manager():
    """服务端启动自检：返回 [(level, msg)]，level ∈ {error, warn, ok}。"""
    issues = []
    if is_weak_password(WEB_PASS):
        issues.append(("warn", "WEB_PASS 为弱口令/默认值（如 admin），首次登录将被强制改密；"
                               "生产环境建议设为强口令"))
    try:
        conn = get_conn()
        try:
            with conn.cursor() as c:
                c.execute("SELECT 1 FROM users LIMIT 1")
        finally:
            conn.close()
    except Exception as e:
        issues.append(("error", "管理库不可连或缺失 users 表：%s" % e))
    # 节点镜像托管自检（v2.5.0 F2）：所有节点形态的 keepalived/agent 都依赖
    # 服务端托管的 ./images/*.tar。管理器容器若缺 ./images 挂载或未构建镜像包，
    # 节点引导会在「校正 keepalived 镜像标签」一步失败——必须在启动时就暴露，
    # 而不是等节点装到一半才报「未找到 custos-node 镜像」。
    try:
        import os as _os
        from .config import IMAGES_DIR
        if not _os.path.isdir(IMAGES_DIR):
            issues.append(("warn",
                           "管理器容器内 %s 不存在：节点引导将拿不到 custos-node/agent "
                           "镜像包（多半是 docker-compose.yml 的 ./images 挂载未生效，"
                           "老容器未重建）。请执行 ./scripts/deploy.sh up -d "
                           "--force-recreate custos-server 重建容器" % IMAGES_DIR))
        elif not available_images():
            issues.append(("warn",
                           "%s 目录为空：未托管任何节点镜像包。请在服务端宿主机执行 "
                           "scripts/build_node_images.sh 生成 custos-node-<版本>.tar "
                           "与 python-3-slim.tar" % IMAGES_DIR))
    except Exception:
        pass
    if not issues:
        issues.append(("ok", "服务端启动自检通过"))
    return issues


def selfcheck_node(host, port, user, password, retries=3, retry_delay=3):
    """节点接入自检：关键特权齐备。返回 (ok, [(level, msg)])。

    连接阶段带【有限重试】：刚装完 / 刚重启的节点，mariadbd 可能仍在初始化或正被
    systemd 重启，此时连接会被掐断，典型报错正是
    `(2013, 'Lost connection to MySQL server during query')`（或 2003）。
    这属【瞬时故障】而非真不可连——一次失败就判 error，会在节点 ready 回执时刷出
    误导性报错（节点其实已装好），甚至让 deploy_state 停在 registered。
    故连接失败按固定间隔重试若干次再定性；健康节点首次即成功，不增加任何开销。
    """
    issues = []
    conn = None
    last_err = None
    _n = max(1, int(retries or 1))
    for attempt in range(1, _n + 1):
        try:
            conn = connect(host, port, user, password)
            break
        except Exception as e:
            last_err = e
            if attempt < _n:
                log.warning("节点 %s 自检连接失败（第 %d/%d 次：%s），%ss 后重试",
                            host, attempt, _n, e, retry_delay)
                time.sleep(retry_delay)
    if conn is None:
        return False, [("error", "节点不可连（已重试 %d 次）：%s" % (_n, last_err))]
    try:
        cur = conn.cursor()
        for priv in NODE_REQUIRED_PRIVS:
            if not has_privilege(cur, priv):
                issues.append(("warn", "节点缺少特权 %s（部分功能可能受限）" % priv))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    ok = not any(l == "error" for l, _ in issues)
    if ok and not issues:
        issues.append(("ok", "节点自检通过"))
    return ok, issues


# 授权行里可能出现的特权 token（用于解析 SHOW GRANTS 输出）
_GRANT_TOKEN_RE = __import__("re").compile(r"GRANT\s+(.+?)\s+ON\s")
# 多词语义特权：从 grants_spec 单一真相源（MariaDB+MySQL 并集）派生，
# 避免解析规则与任一风味的授权定义分叉（MR3）。
from app.grants_spec import ALL_PRIVS_UNION
_MULTI_WORD_PRIVS = tuple(sorted({p.upper() for p in ALL_PRIVS_UNION if " " in p})) or (
    "REPLICATION SLAVE ADMIN", "REPLICATION CLIENT", "REPLICATION SLAVE",
    "REPLICATION MASTER ADMIN", "READ ONLY ADMIN", "BINLOG ADMIN", "SLAVE MONITOR",
    "CREATE TEMPORARY TABLES", "CREATE USER", "SHOW VIEW", "SYSTEM_VARIABLES_ADMIN",
    "ALL PRIVILEGES",
)


def _parse_grant_privs(grant_row):
    """从一行 SHOW GRANTS 解析出特权集合（大写），best-effort。"""
    m = _GRANT_TOKEN_RE.search(grant_row or "")
    if not m:
        return set()
    body = m.group(1).upper()
    privs = set()
    for multi in _MULTI_WORD_PRIVS:
        if multi in body:
            privs.add(multi)
    # 单 token（逗号分隔）
    for tok in body.replace("ALL PRIVILEGES", "").split(","):
        tok = tok.strip()
        if tok and " " not in tok and tok not in ("GRANT", "OPTION", "WITH", "ALL"):
            privs.add(tok)
    return privs


def _probe_host_specs():
    """授权自检要探测的 host 名单 —— 与 ensure_ha_creds **建号口径完全一致**。

    取 _account_hosts()（ACCOUNT_HOST_MODE 决定：legacy=['%'] / additive=['%','<段>.%'] /
    segment=['<段>.%']），再补 localhost / 127.0.0.1 两个本机变体。
    绝不硬编码：探测名单与建号名单一旦分叉，就会把「账号存在」误判成「未找到」
    （见 selfcheck_cluster_grants 内注释）。
    """
    try:
        from app.ha_client.credentials import _account_hosts
        from app.config import get_manager_public_ip
        hosts = list(_account_hosts(get_manager_public_ip()))
    except Exception:
        hosts = ["%"]
    for extra in ("localhost", "127.0.0.1"):
        if extra not in hosts:
            hosts.append(extra)
    return hosts


def selfcheck_cluster_grants(cluster_id, master_host, master_port, creds):
    """按 grants_spec 单一真相源核验集群主库账号授权，**MariaDB/MySQL 各自用正确集**。

    与 ensure_ha_creds 形成「边自愈边观测」双保险：自愈负责补，本函数负责看得见漂移。
    **风味感知**：先 detect_flavour 再选对应特权集，MySQL 集群不会误报 MariaDB 专有特权、
    也不会漏检 MySQL 专有特权（如 SYSTEM_VARIABLES_ADMIN）。
    **能力感知**：期望集再按服务器真实支持的特权（SHOW PRIVILEGES）过滤，老版本缺特权
    不误报。best-effort：无 SHOW GRANTS FOR 权限时降级为提示，绝不阻断监控。

    **分级（L2 噪音治理，2026-10-12）**：本函数是「观测」，不是「告警通道」——
    自愈负责补齐、send_alert 负责上报。因此这里发现的漂移**全部按 info 返回**：
    本函数只在启动时跑一次（`_startup_selfcheck` 有 `_STARTUP_CHECKED` 守卫），
    每条 warn 都会恰好刷在启动日志最显眼处，误判成本极高；而这些漂移几乎都能被
    ensure_ha_creds 在下一轮补上。真正补不上的情况由 `repair_ha_shadow_grants`
    的 send_alert（真告警路径）上报，不会因本处降级而丢失。

    返回 (ok, [(level, msg)])，level ∈ {ok, info}。
    """
    issues = []
    ha_user = creds.get("ha_user") or "ha_monitor"
    ha_pass = creds.get("ha_pass") or ""
    try:
        conn = connect(str(master_host), int(master_port), ha_user, ha_pass)
    except Exception as e:
        # 拿不到主库连接不阻塞监控，仅记录（自愈/监控本身也连不上时会各自降级）
        # 分级 info（L2 噪音治理）：主库不可连本身由monitor/心跳等**真告警路径**负责上报，
        # 此处再报一次 warn 属重复告警，且自检只跑一次、必然刷在启动日志最显眼处。
        return True, [("info", "授权自检跳过（主库不可连：%s）" % e)]
    try:
        from app.grants_spec import REQUIRED_ACCOUNTS, required_privileges
        from app.mariadb_matrix import server_privileges
        from app.mysql_compat import detect_flavour, F_MARIADB
        cur = conn.cursor()
        try:
            flavour = detect_flavour(cur) or F_MARIADB
        except Exception:
            flavour = F_MARIADB  # 探测失败按 MariaDB（项目主线）处理
        available = server_privileges(cur)  # None=探测失败，按全集比对
        # —— 探测口径必须与【创建口径】一致（单一真相源）——
        # 旧实现硬编码 ("%","localhost","127.0.0.1")，但节点上的账号是按
        # _account_hosts() 建的：ACCOUNT_HOST_MODE ∈ {additive,segment} 时为
        # ['%','<段>.%'] 或 ['<段>.%']。于是账号明明存在于 '<段>.%'，却被判
        # 「未找到授权记录」→ warn 假阳性（真机 2026-10-12：账号 app 恒报此warn）。
        # 这与 repair_ha_shadow_grants 的 1133 同源：**MySQL/MariaDB 按 host 精确度
        # 优先匹配**，探测名单与真实建号名单分叉就会误判。
        # 注：本函数以 ha_monitor 连接，**无权读 mysql.user**（不像 repair_ 走 root），
        # 故不能像那里一样直接枚举真实 host，只能对齐「配置口径」。
        host_specs = _probe_host_specs()
        for acct in REQUIRED_ACCOUNTS:
            actual = set()
            for host_spec in host_specs:
                try:
                    cur.execute("SHOW GRANTS FOR %s@%s", (acct, host_spec))
                    for row in cur.fetchall() or []:
                        actual |= _parse_grant_privs((row[0] if row else "") or "")
                    if actual:
                        break
                except Exception:
                    continue
            if not actual:
                issues.append(("info", "账号 %s 在 %s 上未找到授权记录"
                               "（ensure_ha_creds 会自愈补建）"
                               % (acct, "/".join(host_specs))))
                continue
            # 期望集：先按风味选对（MariaDB/MySQL 不同），再按服务器能力过滤，
            # 避免老版本缺特权被误报、也避免跨风味误报。
            expected = required_privileges(acct, flavour)
            if available:
                expected = [p for p in expected if p.upper() in available]
            missing = sorted(set(expected) - set(p.upper() for p in actual))
            if missing:
                # 分级 info（L2 噪音治理）：本条属「有兜底」——ensure_ha_creds 每轮都会
                # 重授，下一轮即消失；真正补不上的情况由 repair_ha_shadow_grants 的
                # send_alert 治本告警（真告警路径）上报，不会因本处降级而丢失。
                issues.append(("info", "账号 %s 缺少特权 %s（授权漂移，下一轮 ensure_ha_creds 将补齐）"
                               % (acct, ",".join(missing))))
    except Exception as e:
        issues.append(("info", "授权自检异常(可忽略): %s" % e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if not issues:
        issues.append(("ok", "集群 %s 授权与单一真相源一致" % cluster_id))
    return True, issues
