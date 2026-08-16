"""生成「一键启动」节点部署文件（mysql-server + keepalived 合一）。

每个被添加的节点会在 NODES_DIR/<ip>/ 下生成：
  docker-compose.yaml   # mysql:8.0 + keepalived(本地 build)
  init.sql              # 首次启动即创建与「所属集群」一致的 ha_monitor / replication / app
  keepalived/
    Dockerfile
    check_mysql_master.sh
    keepalived.conf     # 含同集群全部对等节点（unicast_peer）
    ha_check.conf       # 含管理库连接与节点凭证

用户只需在目标主机上 `cd <目录> && docker compose up -d` 即可完成节点安装与基础配置。
每个节点归属一个集群（cluster_id），其账号/VIP 均取自该集群配置。
"""
import logging
import os
import shutil
from pathlib import Path

from .db import get_setting, query, execute
from .config import MGMT, NODES_DIR, NODES_HOST_BASE, get_mgmt_host, VIP, VIP_PREFIX, INTERFACE, VRID, VIP_PORT
from .ha_client import get_status, ensure_ha_creds
from .clusters import get_cluster_creds

logger = logging.getLogger("node_gen")

NODE_ASSETS = Path(__file__).parent / "node_assets" / "keepalived"


def _valid_vip(vip):
    """校验 VIP 是否为合法 IPv4。

    防止空值 / 非法值进入 keepalived.conf 的 virtual_ipaddress，否则 keepalived
    会静默报 “No VIP specified” / 解析到无效 IP “/24”，导致 VIP 永远不上浮。
    """
    if not vip or not str(vip).strip():
        return False
    parts = str(vip).strip().split(".")
    if len(parts) != 4:
        return False
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except (TypeError, ValueError):
        return False


def _shq(s):
    """把值安全地写成 shell 赋值（单引号包裹，转义内嵌单引号）。

    防止密码/连接串含 # $ 空格 等特殊字符被 shell 解析破坏，导致
    check_mysql_master.sh 连不上 MySQL、静默返回 1、VIP 永远不上浮。
    """
    s = "" if s is None else str(s)
    return "'" + s.replace("'", "'\\''") + "'"


def get_next_server_id():
    base = 0
    try:
        r = query("SELECT MAX(server_id) AS m FROM hosts")
        if r and r[0]["m"]:
            base = max(base, int(r[0]["m"]))
    except Exception:
        pass
    try:
        r = query("SELECT MAX(server_id) AS m FROM nodes")
        if r and r[0]["m"]:
            base = max(base, int(r[0]["m"]))
    except Exception:
        pass
    return base + 1


def _peer_ips(self_ip, cluster_id):
    """同一集群内的对等节点（用于 keepalived unicast_peer）。"""
    peers = set()
    if not cluster_id:
        return sorted(peers)
    try:
        for r in query("SELECT host FROM hosts WHERE cluster_id=%s AND host<>%s",
                       (cluster_id, self_ip)):
            peers.add(r["host"])
    except Exception:
        pass
    try:
        for r in query("SELECT ip FROM nodes WHERE cluster_id=%s AND ip<>%s",
                       (cluster_id, self_ip)):
            peers.add(r["ip"])
    except Exception:
        pass
    return sorted(peers)


def _render_keepalived_conf(local_ip, peers, vip, vip_prefix, interface, vrid, router_id):
    # 硬性校验：VIP 非法时拒绝生成（避免写出 “/24” 这类坏配置让 keepalived 静默失败）
    if not _valid_vip(vip):
        raise ValueError("VIP 非法（%r），拒绝生成 keepalived.conf，请先在集群管理设置正确 VIP" % vip)
    peer_block = "\n".join(f"        {p}" for p in peers)
    return f"""global_defs {{
    router_id {router_id}
    enable_script_security
}}

vrrp_script chk_mysql_master {{
    script "/etc/keepalived/check_mysql_master.sh"
    interval 3
    timeout 2
    fall 2
    rise 2
    user root
}}

vrrp_instance VI_MYSQL {{
    state BACKUP
    interface {interface}
    virtual_router_id {vrid}
    priority 100
    nopreempt
    advert_int 1
    unicast_src_ip {local_ip}
    unicast_peer {{
{peer_block}
    }}
    track_script {{
        chk_mysql_master
    }}
    virtual_ipaddress {{
        {vip}/{vip_prefix}
    }}
}}
"""


def _render_compose(node_name, root_pass, port, server_id, vip_port=None):
    # 注意：YAML 无花括号，可用 f-string。root_pass 含特殊字符时建议改用 .env，此处直接内联。
    if vip_port is None:
        vip_port = VIP_PORT
    # 端口映射：节点自身端口 + VIP 端口都映射到容器 3306，
    # 使「业务连 VIP:VIP_PORT」在任意主库节点上均可达（VIP 浮动到哪台，哪台就提供该端口）。
    port_lines = f'      - "{port}:3306"\n'
    if str(vip_port) != str(port):
        port_lines += f'      - "{vip_port}:3306"\n'
    return f"""# 自动生成：MySQL 8.0 + keepalived 合一节点部署
# 前置：目标主机需支持 --network host 与 NET_ADMIN/NET_RAW（Linux）；首次启动会创建 ha_monitor。
# 运行： cd 本目录 && docker compose up -d
services:
  mysql:
    image: mysql:8.0
    container_name: {node_name}
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: "{root_pass}"
    command:
      - --server-id={server_id}
      - --gtid-mode=ON
      - --enforce-gtid-consistency=ON
      - --log-bin=mysql-bin
      - --log-slave-updates=ON
      - --relay-log=relay-bin
      - --relay-log-recovery=ON
      - --sync-binlog=1
      - --innodb-flush-log-at-trx-commit=1
      - --read-only=ON
    ports:
{port_lines}
    volumes:
      # MySQL 数据目录：统一落在节点目录内的 ./MySQL-data（自包含，可直接拷到 /home/docker/<ip>/ 运行）
      - ./MySQL-data:/var/lib/mysql
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      # 慢日志覆盖配置（slow_query_log=1 + log_output=TABLE），供 slow_log 采集 mysql.slow_log。
      # 必须挂载，否则节点慢查询不入库，/api/slow_log 永远空（节点默认 slow_query_log=OFF）。
      - ./ha-overrides.cnf:/etc/mysql/conf.d/ha-overrides.cnf:ro
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-p{root_pass}"]
      interval: 5s
      timeout: 3s
      retries: 10

  keepalived:
    # 模板化镜像：优先使用已编译并导入的镜像 mysql-ha-keepalived:latest（无需联网编译）。
    # 若目标主机无法编译（如离线），可在可编译的机器上 `docker compose build keepalived`
    # 后 `docker save mysql-ha-keepalived:latest -o keepalived.tar` 拷贝过来
    # `docker load -i keepalived.tar`，再 `docker compose up --no-build` 启动。
    image: mysql-ha-keepalived:latest
    build: ./keepalived
    restart: always
    network_mode: host
    cap_add:
      - NET_ADMIN
      - NET_BROADCAST
      - NET_RAW
    depends_on:
      - mysql
    volumes:
      - ./keepalived/keepalived.conf:/etc/keepalived/keepalived.conf:ro
      - ./keepalived/check_mysql_master.sh:/etc/keepalived/check_mysql_master.sh:ro
      - ./keepalived/notify_master.sh:/etc/keepalived/notify_master.sh:ro
      - ./keepalived/notify_backup.sh:/etc/keepalived/notify_backup.sh:ro
      - ./keepalived/ha_check.conf:/etc/keepalived/ha_check.conf:ro
"""


def render_my_cnf_fragment(server_id, port, mysql_version=None):
    """生成裸机 my.cnf [mysqld] 片段（供用户在宝塔/编辑器中追加并重启 MySQL 生效）。

    原因：宝塔等面板重启会重写 my.cnf，SET PERSIST 写入的 mysqld-auto.cnf 可能被覆盖；
    故 server_id / gtid / binlog 等关键复制参数走配置文件最稳。
    """
    return (
        "# ---- MySQL HA Manager 裸机节点参数（追加到 my.cnf 的 [mysqld] 段，重启 MySQL 生效）----\n"
        f"server-id = {server_id}\n"
        "gtid_mode = ON\n"
        "enforce_gtid_consistency = ON\n"
        "log_bin = mysql-bin\n"
        "log_slave_updates = ON\n"
        "relay_log = relay-bin\n"
        "relay_log_recovery = ON\n"
        "sync_binlog = 1\n"
        "innodb_flush_log_at_trx_commit = 1\n"
        "read_only = ON\n"
        "slow_query_log = 1\n"
        "long_query_time = 0.5\n"
        "log_output = TABLE\n"
        "log_queries_not_using_indexes = 0\n"
        f"# 业务/复制端口（修改后须与管理器节点端口 {port} 一致）\n"
    )


def render_init_baremetal_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass):
    """裸机 init.sql：创建 ha_monitor / replication / app 账号 + ha_local 状态库。

    不含 server_id 等参数（走 my.cnf 片段）。用户须在「应用 my.cnf 片段并重启 MySQL」后手动执行。
    """
    return _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass)


def render_baremetal_compose(node_name, vip_port=None):
    """裸机节点：只跑 keepalived 客户端（docker 模式），MySQL 由宿主机既有实例提供。

    keepalived 容器使用 host 网络，连接 127.0.0.1:<port> 探活本机 MySQL。
    """
    if vip_port is None:
        vip_port = VIP_PORT
    return (
        "# 自动生成：裸机 MySQL + keepalived-only 客户端节点部署\n"
        "# 前置：目标机已运行 MySQL（宝塔等），已应用 my.cnf 片段并创建账号（init_baremetal.sql）。\n"
        "# 运行： cd 本目录 && docker compose up -d\n"
        "services:\n"
        "  keepalived:\n"
        "    image: mysql-ha-keepalived:latest\n"
        "    build: ./keepalived\n"
        "    restart: always\n"
        "    network_mode: host\n"
        "    cap_add:\n"
        "      - NET_ADMIN\n"
        "      - NET_BROADCAST\n"
        "      - NET_RAW\n"
        "    volumes:\n"
        "      - ./keepalived/keepalived.conf:/etc/keepalived/keepalived.conf:ro\n"
        "      - ./keepalived/check_mysql_master.sh:/etc/keepalived/check_mysql_master.sh:ro\n"
        "      - ./keepalived/notify_master.sh:/etc/keepalived/notify_master.sh:ro\n"
        "      - ./keepalived/notify_backup.sh:/etc/keepalived/notify_backup.sh:ro\n"
        "      - ./keepalived/ha_check.conf:/etc/keepalived/ha_check.conf:ro\n"
    )


def render_docker_my_cnf():
    """Docker 节点 MySQL 覆盖配置：开启慢日志（log_output=TABLE），供 slow_log 采集。

    挂载到容器 /etc/mysql/conf.d/ha-overrides.cnf。对 v2.0 已部署集群无副作用，
    仅在该节点重新生成并部署后生效。
    """
    return (
        "[mysqld]\n"
        "slow_query_log = 1\n"
        "long_query_time = 0.5\n"
        "log_output = TABLE\n"
        "log_queries_not_using_indexes = 0\n"
    )


def _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass):
    """渲染 init.sql（首次启动即创建与所属集群一致的 ha_monitor / replication / app 账号）。

    抽成模块级函数，供 generate_node（首次生成）与 sync_node_creds（密码变更后重生成）
    共用，保证「节点部署文件」中的凭证与集群当前凭证始终一致——这样即使重部署
    （清空 MySQL-data 后 init.sql 重新执行）也不会因旧密码导致无法连接。
    """
    return (
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* TO '%s'@'%%';\n"
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%';\n"
        # 业务最小权限账号：供测试/应用通过 VIP 连接，仅授予常用读写与管理权限，
        # 不含 SUPER/REPLICATION 等运维权限，避免应用直接用 ha_monitor/replication。
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, "
        "REFERENCES, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON *.* TO '%s'@'%%';\n"
        # 本地状态库：管理端在切换主库时把「当前主」写入各节点本机 ha_local.state，
        # keepalived 检查脚本只读本机判定 VIP 归属，完全不依赖节点回连管理库
        # （避免管理库不可达时 VIP 不漂移、被 nopreempt 锁死在首节点）。
        "CREATE DATABASE IF NOT EXISTS ha_local;\n"
        "CREATE TABLE IF NOT EXISTS ha_local.state (k VARCHAR(32) PRIMARY KEY, v VARCHAR(255));\n"
        "FLUSH PRIVILEGES;\n"
    ) % (ha_user, ha_pass, ha_user, repl_user, repl_pass, repl_user,
         app_user, app_pass, app_user)


def _write(path: Path, text: str, mode=0o600):
    path.write_text(text)
    os.chmod(path, mode)


def _refresh_all_keepalived_conf(cluster_id=None):
    """任一节点增删后，重算对应集群所有节点的 unicast_peer 并刷新 keepalived.conf。

    cluster_id 为 None 时刷新全部集群。
    """
    try:
        if cluster_id:
            rows = query("SELECT ip, port, server_id FROM nodes WHERE cluster_id=%s",
                         (cluster_id,))
        else:
            rows = query("SELECT ip, port, server_id FROM nodes")
    except Exception:
        return
    for r in rows:
        ip = r["ip"]
        cid = r.get("cluster_id")
        peers = _peer_ips(ip, cid)
        creds = get_cluster_creds(cid) if cid else None
        vip = (creds or {}).get("vip") or get_setting("VIP") or VIP
        vip_prefix = (creds or {}).get("vip_prefix") or VIP_PREFIX
        interface = (creds or {}).get("interface") or INTERFACE
        vrid = (creds or {}).get("vrid") or VRID
        try:
            conf = _render_keepalived_conf(
                ip, peers, vip, vip_prefix, interface, vrid,
                f"ha_mysql_{ip.replace('.', '_')}",
            )
        except ValueError as e:
            logger.warning("跳过刷新 %s 的 keepalived.conf：%s", ip, e)
            continue
        p = _safe_node_path(ip) / "keepalived" / "keepalived.conf"
        if p.exists():
            _write(p, conf)


def regenerate_cluster_compose(cluster_id):
    """集群 VIP 端口(vip_port)变更后，重渲染该集群所有节点的 docker-compose；
    并补拷缺失的 notify 脚本（早期版本生成节点时不拷贝 notify 脚本，会导致
    keepalived 配置指向不存在的脚本、VIP 不上浮）。

    仅写入节点部署文件，不影响正在运行的容器；需到各节点执行 docker compose up -d 生效。
    """
    creds = get_cluster_creds(cluster_id) if cluster_id else None
    vip_port = (creds or {}).get("vip_port") or VIP_PORT
    vip = (creds or {}).get("vip") or ""
    vip_prefix = (creds or {}).get("vip_prefix") or VIP_PREFIX
    interface = (creds or {}).get("interface") or INTERFACE
    vrid = (creds or {}).get("vrid") or VRID
    rows = query(
        "SELECT ip, port, root_pass, server_id, node_type FROM nodes WHERE cluster_id=%s",
        (cluster_id,))
    for r in rows:
        ip = r["ip"]
        node_type = r.get("node_type") or "docker"
        kd = _safe_node_path(ip) / "keepalived"
        if not kd.exists():
            continue
        # 确保 notify 脚本存在（早期版本缺失，导致 keepalived 起不来 / VIP 不上浮）
        for nf in ("notify_master.sh", "notify_backup.sh"):
            src = NODE_ASSETS / nf
            if src.exists() and not (kd / nf).exists():
                _write(kd / nf, src.read_text(), mode=0o755)
        # 重渲染 keepalived.conf：同步最新 VIP / 网卡 / VRID / unicast_peer。
        # 修复此前改 VIP 只刷新 docker-compose、漏刷 keepalived.conf，导致节点配置文件里
        # virtual_ipaddress 仍为空（只剩 /24），keepalived 报 “No VIP specified” / 解析到
        # 无效 IP “/24”，VIP 永远不上浮。VIP 非法时跳过写入（避免覆盖为坏配置）。
        peers = _peer_ips(ip, cluster_id)
        try:
            conf = _render_keepalived_conf(
                ip, peers, vip, vip_prefix, interface, vrid,
                f"ha_mysql_{ip.replace('.', '_')}",
            )
            _write(kd / "keepalived.conf", conf)
        except ValueError as e:
            logger.warning("集群 %s 节点 %s 的 keepalived.conf 渲染跳过（%s）；"
                           "请先在集群管理设置正确 VIP 后再刷新", cluster_id, ip, e)
        p = kd.parent / "docker-compose.yaml"
        if not p.exists():
            p = kd.parent / "docker-compose.yml"
        if not p.exists():
            continue
        node_name = f"mysql-ha-node-{ip.replace('.', '-')}"
        if node_type == "baremetal":
            content = render_baremetal_compose(node_name, vip_port)
        else:
            content = _render_compose(
                node_name, r.get("root_pass") or "", int(r["port"]),
                int(r.get("server_id") or 0), vip_port)
            _write(kd.parent / "ha-overrides.cnf", render_docker_my_cnf(), mode=0o644)
        _write(p, content)


def _clean_generated_dir(d: Path):
    """重新生成节点前清理目录中旧的自生成文件；保留 MySQL-data 数据卷，绝不删除。"""
    if not d.exists():
        return
    for item in d.iterdir():
        if item.name == "MySQL-data":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def _safe_node_path(ip):
    """把节点 ip 解析为 NODES_DIR 下的安全绝对路径，杜绝路径穿越导致的误删/越权写。

    节点地址是 MySQL 主机 IP/主机名，本不应含路径分隔符或 `..`。即使传入 '/etc'、
    '../../etc' 这类字符串，也必须在拼接并 resolve 后断言路径仍位于 NODES_DIR 之内，
    否则直接拒绝——避免 _clean_generated_dir / delete_node_dir 递归删除宿主任意目录，
    或 sync_node_creds / _refresh_all_keepalived_conf 越权读写宿主文件系统。
    """
    if not ip or any(sep in ip for sep in ("/", "\\")) or ".." in ip:
        raise ValueError(f"非法的节点地址（疑似路径穿越）: {ip!r}")
    base = Path(NODES_DIR).resolve()
    d = (base / ip).resolve()
    if d != base and not str(d).startswith(str(base) + os.sep):
        raise ValueError(f"非法的节点路径（越过部署根目录）: {ip!r}")
    return d


def generate_node(ip, port, root_user, root_pass, initial_role="slave", cluster_id=None,
                  node_type="docker", mysql_version=None):
    ip = (ip or "").strip()
    if not ip:
        raise ValueError("节点 IP 不能为空")
    port = int(port)
    if not cluster_id:
        # 缺省归入首个集群
        from .clusters import get_default_cluster
        d = get_default_cluster()
        if not d:
            raise ValueError("尚未创建任何集群，请先在「集群管理」中创建集群")
        cluster_id = d["id"]

    creds = get_cluster_creds(cluster_id)
    if not creds:
        raise ValueError("集群不存在或凭证缺失")
    ha_user = creds["ha_user"] or "ha_monitor"
    ha_pass = creds["ha_pass"]
    if not ha_pass:
        # 兜底：集群未设密码时自动生成并写回（保证添加节点始终可用）
        from .config import rand_secret
        ha_pass = rand_secret(16)
        from .clusters import update_cluster
        update_cluster(cluster_id, ha_pass=ha_pass)
        creds["ha_pass"] = ha_pass
    repl_user = creds["repl_user"] or "replication"
    repl_pass = creds["repl_pass"]
    if not repl_pass:
        from .config import rand_secret
        repl_pass = rand_secret(16)
        from .clusters import update_cluster
        update_cluster(cluster_id, repl_pass=repl_pass)
        creds["repl_pass"] = repl_pass
    app_user = creds["app_user"] or "app"
    app_pass = creds["app_pass"]
    if not app_pass:
        from .config import rand_secret
        app_pass = rand_secret(16)
        from .clusters import update_cluster
        update_cluster(cluster_id, app_pass=app_pass)
        creds["app_pass"] = app_pass

    vip = creds["vip"]
    if not vip:
        raise ValueError("请先在「集群管理」中为该集群设置 VIP（节点同网段空闲 IP）后再添加节点")
    if not _valid_vip(vip):
        raise ValueError("集群 VIP 格式非法（%r），应为合法 IPv4，如 192.168.1.10" % vip)
    vip_prefix = creds["vip_prefix"] or VIP_PREFIX
    interface = creds["interface"] or INTERFACE
    vrid = creds["vrid"] or VRID
    vip_port = creds.get("vip_port") or VIP_PORT
    mgmt_host = get_mgmt_host()
    mgmt_port = MGMT["port"]
    mgmt_user = MGMT["user"]
    mgmt_pass = MGMT["password"]
    mgmt_db = MGMT["db"]
    server_id = get_next_server_id()
    node_name = f"mysql-ha-node-{ip.replace('.', '-')}"
    peers = _peer_ips(ip, cluster_id)
    # 裸机模式 keepalived 容器在 host 网络，连本机 MySQL 用 127.0.0.1；
    # Docker 模式连节点 IP（容器已把端口发布到宿主机）。
    local_host = "127.0.0.1" if node_type == "baremetal" else ip

    d = _safe_node_path(ip)
    d.mkdir(parents=True, exist_ok=True)
    _clean_generated_dir(d)
    kd = d / "keepalived"
    kd.mkdir(parents=True, exist_ok=True)

    # 1) init.sql / compose（按节点类型分支）
    init_sql = _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass)
    extra_files = {}
    if node_type == "baremetal":
        # 裸机：不生成 MySQL 容器，仅生成「手工应用文件」（init_baremetal.sql + my.cnf 片段）。
        # 管理器不自动连 root 执行 init（避免生产 root 凭据被用于远程推送）。
        _write(d / "init_baremetal.sql", init_sql, mode=0o644)
        my_cnf = render_my_cnf_fragment(server_id, port, mysql_version)
        _write(d / "my.cnf.ha-fragment", my_cnf, mode=0o644)
        extra_files["init_baremetal_sql"] = f"{NODES_HOST_BASE}/{ip}/init_baremetal.sql"
        extra_files["my_cnf_fragment"] = f"{NODES_HOST_BASE}/{ip}/my.cnf.ha-fragment"
    else:
        # init.sql 会被 MySQL 容器以非 root 的 mysql 用户读取执行，
        # 若权限过严（0600）会因 Permission denied 失败；改为 0644 保证可读。
        _write(d / "init.sql", init_sql, mode=0o644)
        # 慢日志覆盖配置（挂载到容器 /etc/mysql/conf.d）
        _write(d / "ha-overrides.cnf", render_docker_my_cnf(), mode=0o644)

    # 2) keepalived.conf
    keep_conf = _render_keepalived_conf(
        ip, peers, vip, vip_prefix, interface, vrid, f"ha_mysql_{ip.replace('.', '_')}"
    )
    _write(kd / "keepalived.conf", keep_conf)

    # 3) ha_check.conf（含管理库连接与节点凭证；限制权限）
    #    LOCAL_ROOT_USER/LOCAL_ROOT_PASS 用于 keepalived 本地探活（root 密码在容器创建时
    #    固定，不随 HA_PASS 漂移，避免 ha_monitor 密码脱节导致 VIP 不漂移）。
    ha_check = (
        f"LOCAL_HOST={_shq(local_host)}\n"
        f"LOCAL_PORT={port}\n"
        f"LOCAL_USER={_shq(ha_user)}\n"
        f"LOCAL_PASS={_shq(ha_pass)}\n"
        f"LOCAL_ROOT_USER={_shq(root_user)}\n"
        f"LOCAL_ROOT_PASS={_shq(root_pass)}\n"
        f"MGMT_HOST={_shq(mgmt_host)}\n"
        f"MGMT_PORT={mgmt_port}\n"
        f"MGMT_USER={_shq(mgmt_user)}\n"
        f"MGMT_PASS={_shq(mgmt_pass)}\n"
        f"MGMT_DB={_shq(mgmt_db)}\n"
    )
    _write(kd / "ha_check.conf", ha_check)

    # 4) 复制静态资源：keepalived Dockerfile + 检查脚本 + 启动入口
    # check_mysql_master.sh 通过 :ro 绑定挂载进 keepalived 容器，其可执行位由宿主机文件
    # 权限决定；若不可执行，keepalived 会禁用 track_script，导致 split-brain。故此处必须 0755。
    for f in ("Dockerfile", "check_mysql_master.sh", "entrypoint.sh",
              "notify_master.sh", "notify_backup.sh"):
        src = NODE_ASSETS / f
        if src.exists():
            mode = 0o755 if f.endswith(".sh") else 0o644
            _write(kd / f, src.read_text(), mode=mode)

    # 5) docker-compose.yaml（裸机仅 keepalived 客户端）
    if node_type == "baremetal":
        compose = render_baremetal_compose(node_name, vip_port)
    else:
        compose = _render_compose(node_name, root_pass, port, server_id, vip_port)
    _write(d / "docker-compose.yaml", compose)

    # 6) 登记到 nodes 表 + hosts 表（供监控自动接入）
    #    compose_path 记录「宿主机路径」（便于 Web 展示与到目标主机执行），
    #    实际文件仍写入容器内的 NODES_DIR（由 docker-compose 挂载到 ./generated_nodes）。
    #    注意：每个 <ip> 子目录是自包含部署单元，拷贝到目标主机的 /home/docker/<ip>/ 即可运行。
    #    root_pass 一并入库：节点 MySQL root 密码在容器创建时固定，作为 ha_monitor 凭证
    #    自愈与切换回退的权威凭据（不随 HA_PASS 漂移）。
    host_compose = f"{NODES_HOST_BASE}/{ip}"
    execute(
        "INSERT INTO nodes (ip, port, root_user, root_pass, server_id, compose_path, cluster_id, node_type, mysql_version) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE port=%s, root_user=%s, root_pass=%s, server_id=%s, "
        "compose_path=%s, cluster_id=%s, node_type=%s, mysql_version=%s",
        (ip, port, root_user, root_pass, server_id, host_compose, cluster_id, node_type, mysql_version,
         port, root_user, root_pass, server_id, host_compose, cluster_id, node_type, mysql_version),
    )
    execute(
        "INSERT INTO hosts (host, port, role, status, initial_role, cluster_id) "
        "VALUES (%s,%s,'unknown','down',%s,%s) "
        "ON DUPLICATE KEY UPDATE initial_role=%s, cluster_id=%s",
        (ip, port, initial_role, cluster_id, initial_role, cluster_id),
    )
    # 若指定为初始主库，则登记为当前主库，使 keepalived VIP 选举偏向上位
    if initial_role == "master":
        execute("UPDATE hosts SET is_current_master=0 WHERE cluster_id=%s", (cluster_id,))
        execute("UPDATE hosts SET is_current_master=1 WHERE cluster_id=%s AND host=%s AND port=%s",
                (cluster_id, ip, port))
        from .state import set_current_master
        set_current_master(cluster_id, ip, port)

    # 7) 新增节点后刷新同集群所有节点的对等列表
    _refresh_all_keepalived_conf(cluster_id)

    return dict(
        dir=host_compose, node_name=node_name, server_id=server_id, peers=peers,
        node_type=node_type,
        run_cmd=f"cd {host_compose} && docker compose up -d",
        **extra_files,
    )


def sync_node_creds(cluster_id=None):
    """把指定集群（或全局）的 HA/复制/业务账号密码同步到其在线节点。

    触发时机：「集群管理」保存且该集群 HA/复制/业务密码发生变化时。
    作用：
      1) 用节点 MySQL **root** 账号执行 ALTER USER，将 ha_monitor / replication / app
         账号密码校正为当前设定值（权威自愈，不依赖 ha_monitor 是否可达）。
      2) 重新生成各节点 keepalived/ha_check.conf，保持与节点一致。
    """
    if cluster_id:
        clusters = [cluster_id]
        creds_map = {cluster_id: get_cluster_creds(cluster_id)}
    else:
        cls = query("SELECT id FROM clusters")
        clusters = [c["id"] for c in cls]
        creds_map = {c["id"]: get_cluster_creds(c["id"]) for c in cls}

    mgmt_host = get_mgmt_host()
    mgmt_port = MGMT["port"]
    mgmt_user = MGMT["user"]
    mgmt_pass = MGMT["password"]
    mgmt_db = MGMT["db"]

    synced = 0
    for cid in clusters:
        creds = creds_map.get(cid)
        if not creds:
            continue
        ha_user = creds["ha_user"] or "ha_monitor"
        ha_pass = creds["ha_pass"] or ""
        repl_user = creds["repl_user"] or "replication"
        repl_pass = creds["repl_pass"] or ""
        app_user = creds["app_user"] or "app"
        app_pass = creds["app_pass"] or ""
        if not ha_pass or not repl_pass:
            continue
        rows = query(
            "SELECT ip, port, root_user, root_pass, node_type, mysql_version FROM nodes WHERE cluster_id=%s", (cid,))
        for n in rows:
            ip = n["ip"]
            port = int(n["port"])
            root_user = n.get("root_user") or "root"
            root_pass = n.get("root_pass") or ""
            node_type = n.get("node_type") or "docker"

            # 用 root 权威自愈账号密码（不依赖 ha_monitor 是否可达）
            ok, msg = ensure_ha_creds(
                ip, port, root_user, root_pass,
                ha_user, ha_pass, repl_user, repl_pass,
                app_user, app_pass,
            )
            if not ok:
                # root 不可达（节点离线/密码不符），跳过（恢复后重新生成部署文件）
                continue

            # 重新生成该节点 ha_check.conf，保持与节点一致
            # 裸机节点 keepalived 探活连 127.0.0.1（宿主机既有 MySQL）；docker 节点连宿主机 IP。
            local_host = "127.0.0.1" if node_type == "baremetal" else ip
            kd = _safe_node_path(ip) / "keepalived"
            kd.mkdir(parents=True, exist_ok=True)
            ha_check = (
                f"LOCAL_HOST={local_host}\n"
                f"LOCAL_PORT={port}\n"
                f"LOCAL_USER={ha_user}\n"
                f"LOCAL_PASS={ha_pass}\n"
                f"LOCAL_ROOT_USER={root_user}\n"
                f"LOCAL_ROOT_PASS={root_pass}\n"
                f"MGMT_HOST={mgmt_host}\n"
                f"MGMT_PORT={mgmt_port}\n"
                f"MGMT_USER={mgmt_user}\n"
                f"MGMT_PASS={mgmt_pass}\n"
                f"MGMT_DB={mgmt_db}\n"
            )
            _write(kd / "ha_check.conf", ha_check)
            # 同步重生成 init 凭证 SQL（仅凭证相关），保证后续「重部署」后执行时使用的仍是当前密码。
            # 裸机节点写 init_baremetal.sql（不含 server_id 参数，需配合 my.cnf 片段）；docker 节点写 init.sql。
            if node_type == "baremetal":
                init_sql = render_init_baremetal_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass)
                _write(_safe_node_path(ip) / "init_baremetal.sql", init_sql, mode=0o644)
            else:
                init_sql = _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass)
                _write(_safe_node_path(ip) / "init.sql", init_sql, mode=0o644)
            synced += 1
    return synced


def list_nodes(cluster_id=None):
    try:
        if cluster_id:
            return query(
                "SELECT n.ip, n.port, n.root_user, n.root_pass, n.server_id, n.compose_path, n.created_at, "
                "n.node_type, n.mysql_version, "
                "h.initial_role, h.role, h.status, n.cluster_id "
                "FROM nodes n LEFT JOIN hosts h ON h.host = n.ip AND h.cluster_id=n.cluster_id "
                "WHERE n.cluster_id=%s ORDER BY n.ip",
                (cluster_id,),
            )
        return query(
            "SELECT n.ip, n.port, n.root_user, n.root_pass, n.server_id, n.compose_path, n.created_at, "
            "n.node_type, n.mysql_version, "
            "h.initial_role, h.role, h.status, n.cluster_id "
            "FROM nodes n LEFT JOIN hosts h ON h.host = n.ip AND h.cluster_id=n.cluster_id "
            "ORDER BY n.cluster_id, n.ip"
        )
    except Exception:
        return []


def delete_node_dir(ip):
    """仅删除生成目录（不含 MySQL-data 数据卷）。"""
    import shutil as _sh
    d = _safe_node_path(ip)
    if d.exists():
        _sh.rmtree(d, ignore_errors=True)


def delete_node(ip):
    # 查询该节点所属集群，便于仅清理对应集群的 current_master 与 keepalived 对等列表
    cid = None
    try:
        r = query("SELECT cluster_id FROM nodes WHERE ip=%s", (ip,))
        if r:
            cid = r[0]["cluster_id"]
    except Exception:
        pass
    try:
        execute("DELETE FROM nodes WHERE ip=%s", (ip,))
        execute("DELETE FROM hosts WHERE host=%s", (ip,))
        # 若被删节点恰是其所在集群的当前主，清除该集群 current_master 以便监控重新选举
        if cid:
            cur = query("SELECT host FROM current_master WHERE cluster_id=%s", (cid,))
            if cur and cur[0]["host"] == ip:
                execute("DELETE FROM current_master WHERE cluster_id=%s", (cid,))
    except Exception as e:
        raise ValueError(f"删除节点失败: {e}")
    _refresh_all_keepalived_conf(cid)
