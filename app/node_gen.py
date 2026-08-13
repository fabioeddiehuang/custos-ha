"""生成「一键启动」节点部署文件（mysql-server + keepalived 合一）。

每个被添加的节点会在 NODES_DIR/<ip>/ 下生成：
  docker-compose.yaml   # mysql:8.0 + keepalived(本地 build)
  init.sql              # 首次启动即创建与全局一致的 ha_monitor
  keepalived/
    Dockerfile
    check_mysql_master.sh
    keepalived.conf     # 含全部对等节点（unicast_peer）
    ha_check.conf       # 含管理库连接与各节点凭证

用户只需在目标主机上 `cd <目录> && docker compose up -d` 即可完成节点安装与基础配置。
"""
import os
import shutil
from pathlib import Path

from .db import get_setting, query, execute, upsert_setting
from .config import MGMT, NODES_DIR, NODES_HOST_BASE, get_mgmt_host, VIP, VIP_PREFIX, INTERFACE, VRID, rand_secret
from .ha_client import get_status, ensure_ha_creds

NODE_ASSETS = Path(__file__).parent / "node_assets" / "keepalived"


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


def _peer_ips(self_ip):
    peers = set()
    try:
        for r in query("SELECT ip FROM nodes WHERE ip<>%s", (self_ip,)):
            peers.add(r["ip"])
    except Exception:
        pass
    try:
        for r in query("SELECT host FROM hosts WHERE host<>%s", (self_ip,)):
            peers.add(r["host"])
    except Exception:
        pass
    return sorted(peers)


def _render_keepalived_conf(local_ip, peers, vip, vip_prefix, interface, vrid, router_id):
    peer_block = "\n".join(f"        {p}" for p in peers)
    return f"""global_defs {{
    router_id {router_id}
    enable_script_security
}}

vrrp_script chk_mysql_master {{
    script "/etc/keepalived/check_mysql_master.sh"
    interval 2
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


def _render_compose(node_name, root_pass, port, server_id):
    # 注意：YAML 无花括号，可用 f-string。root_pass 含特殊字符时建议改用 .env，此处直接内联。
    return f"""# 自动生成：MySQL 8.0 + keepalived 合一节点部署
# 前置：目标主机需支持 --network host 与 NET_ADMIN/NET_RAW（Linux）；首次启动会创建 ha_monitor。
# 运行： cd 本目录 && docker compose up -d
services:
  mysql:
    image: mysql:8.0
    container_name: {node_name}
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
      - --read-only=ON
    ports:
      - "{port}:3306"
    volumes:
      # MySQL 数据目录：统一落在节点目录内的 ./MySQL-data（自包含，可直接拷到 /home/docker/<ip>/ 运行）
      - ./MySQL-data:/var/lib/mysql
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-p{root_pass}"]
      interval: 5s
      timeout: 3s
      retries: 10

  keepalived:
    build: ./keepalived
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
      - ./keepalived/ha_check.conf:/etc/keepalived/ha_check.conf:ro
"""


def _write(path: Path, text: str, mode=0o600):
    path.write_text(text)
    os.chmod(path, mode)


def _refresh_all_keepalived_conf():
    """任一节点增删后，重算所有节点的 unicast_peer 并刷新 keepalived.conf。"""
    try:
        rows = query("SELECT ip, port, server_id FROM nodes")
    except Exception:
        return
    for r in rows:
        ip = r["ip"]
        peers = _peer_ips(ip)
        conf = _render_keepalived_conf(
            ip, peers, get_setting("VIP") or VIP, get_setting("VIP_PREFIX") or VIP_PREFIX,
            get_setting("INTERFACE") or INTERFACE, get_setting("VRID") or VRID,
            f"ha_mysql_{ip.replace('.', '_')}",
        )
        p = Path(NODES_DIR) / ip / "keepalived" / "keepalived.conf"
        if p.exists():
            _write(p, conf)


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


def generate_node(ip, port, root_user, root_pass, initial_role="slave"):
    ip = (ip or "").strip()
    if not ip:
        raise ValueError("节点 IP 不能为空")
    port = int(port)
    ha_user = get_setting("HA_USER") or "ha_monitor"
    ha_pass = get_setting("HA_PASS")
    if not ha_pass:
        # 兜底：服务端配置未手动设置时，自动生成并保存（保证添加节点始终可用）
        ha_pass = rand_secret(16)
        upsert_setting("HA_PASS", ha_pass)
    repl_user = get_setting("REPL_USER") or "repl"
    repl_pass = get_setting("REPL_PASS")
    if not repl_pass:
        repl_pass = rand_secret(16)
        upsert_setting("REPL_PASS", repl_pass)
    # 业务最小权限账号：测试/应用连接 VIP 使用，避免直接用 ha_monitor/replication
    app_user = get_setting("APP_USER") or "app"
    app_pass = get_setting("APP_PASS")
    if not app_pass:
        app_pass = rand_secret(16)
        upsert_setting("APP_PASS", app_pass)

    vip = get_setting("VIP") or VIP
    if not vip:
        raise ValueError("请先在「服务端配置」中设置 VIP（节点同网段空闲 IP）后再添加节点")
    vip_prefix = get_setting("VIP_PREFIX") or VIP_PREFIX
    interface = get_setting("INTERFACE") or INTERFACE
    vrid = get_setting("VRID") or VRID
    mgmt_host = get_mgmt_host()
    mgmt_port = MGMT["port"]
    mgmt_user = MGMT["user"]
    mgmt_pass = MGMT["password"]
    mgmt_db = MGMT["db"]
    server_id = get_next_server_id()
    node_name = f"mysql-ha-node-{ip.replace('.', '-')}"
    peers = _peer_ips(ip)

    d = Path(NODES_DIR) / ip
    d.mkdir(parents=True, exist_ok=True)
    _clean_generated_dir(d)
    kd = d / "keepalived"
    kd.mkdir(parents=True, exist_ok=True)

    # 1) init.sql：首次启动即创建 ha_monitor 与复制账号（与全局 HA/复制凭证保持一致）
    #    因此「服务端配置」中自动生成的账号密码会直接落地到节点，无需手动建账号。
    init_sql = (
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER ON *.* TO '%s'@'%%';\n"
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
    # init.sql 会被 MySQL 容器以非 root 的 mysql 用户读取执行，
    # 若权限过严（0600）会因 Permission denied 失败；改为 0644 保证可读。
    _write(d / "init.sql", init_sql, mode=0o644)

    # 2) keepalived.conf
    keep_conf = _render_keepalived_conf(
        ip, peers, vip, vip_prefix, interface, vrid, f"ha_mysql_{ip.replace('.', '_')}"
    )
    _write(kd / "keepalived.conf", keep_conf)

    # 3) ha_check.conf（含管理库连接与节点凭证；限制权限）
    #    LOCAL_ROOT_USER/LOCAL_ROOT_PASS 用于 keepalived 本地探活（root 密码在容器创建时
    #    固定，不随 HA_PASS 漂移，避免 ha_monitor 密码脱节导致 VIP 不漂移）。
    ha_check = (
        f"LOCAL_HOST={ip}\n"
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

    # 4) 复制静态资源：keepalived Dockerfile + 检查脚本 + 启动入口
    # check_mysql_master.sh 通过 :ro 绑定挂载进 keepalived 容器，其可执行位由宿主机文件
    # 权限决定；若不可执行，keepalived 会禁用 track_script，导致 split-brain。故此处必须 0755。
    for f in ("Dockerfile", "check_mysql_master.sh", "entrypoint.sh"):
        src = NODE_ASSETS / f
        if src.exists():
            mode = 0o755 if f in ("check_mysql_master.sh", "entrypoint.sh") else 0o644
            _write(kd / f, src.read_text(), mode=mode)

    # 5) docker-compose.yaml
    compose = _render_compose(node_name, root_pass, port, server_id)
    _write(d / "docker-compose.yaml", compose)

    # 6) 登记到 nodes 表 + hosts 表（供监控自动接入）
    #    compose_path 记录「宿主机路径」（便于 Web 展示与到目标主机执行），
    #    实际文件仍写入容器内的 NODES_DIR（由 docker-compose 挂载到 ./generated_nodes）。
    #    注意：每个 <ip> 子目录是自包含部署单元，拷贝到目标主机的 /home/docker/<ip>/ 即可运行。
    #    root_pass 一并入库：节点 MySQL root 密码在容器创建时固定，作为 ha_monitor 凭证
    #    自愈与切换回退的权威凭据（不随 HA_PASS 漂移）。
    host_compose = f"{NODES_HOST_BASE}/{ip}"
    execute(
        "INSERT INTO nodes (ip, port, root_user, root_pass, server_id, compose_path) "
        "VALUES (%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE port=%s, root_user=%s, root_pass=%s, server_id=%s, compose_path=%s",
        (ip, port, root_user, root_pass, server_id, host_compose,
         port, root_user, root_pass, server_id, host_compose),
    )
    execute(
        "INSERT INTO hosts (host, port, role, status, initial_role) "
        "VALUES (%s,%s,'unknown','down',%s) "
        "ON DUPLICATE KEY UPDATE initial_role=%s",
        (ip, port, initial_role, initial_role),
    )
    # 若指定为初始主库，则登记为 current_master，使 keepalived VIP 选举偏向上位
    if initial_role == "master":
        execute("UPDATE hosts SET is_current_master=0")
        execute("UPDATE hosts SET is_current_master=1 WHERE host=%s AND port=%s", (ip, port))
        execute(
            "INSERT INTO current_master (id, host, port) VALUES (1,%s,%s) "
            "ON DUPLICATE KEY UPDATE host=%s, port=%s",
            (ip, port, ip, port),
        )

    # 7) 新增节点后刷新所有节点的对等列表
    _refresh_all_keepalived_conf()

    return dict(
        dir=host_compose, node_name=node_name, server_id=server_id, peers=peers,
        run_cmd=f"cd {host_compose} && docker compose up -d",
    )


def sync_node_creds():
    """把当前 HA_PASS / REPL_PASS / APP_PASS 同步到所有已登记节点。

    触发时机：「服务端配置」保存且 HA_PASS / REPL_PASS 发生变化时。
    作用：
      1) 用节点 MySQL **root** 账号（root 密码在容器创建时固定，不随 HA_PASS 漂移）
         执行 ALTER USER，将 ha_monitor / replication / app 账号密码校正为当前设定值。
         这是权威自愈手段——无论 ha_monitor 当前密码是什么（哪怕已完全脱节），
         都能连上节点并校正，避免「节点 MySQL 账号与管理端脱节 → 监控探活失败 /
         keepalived 本地检查认证失败 / VIP 不跟随真实主库漂移」的坑（此前 213/177 踩过）。
      2) 重新生成各节点 keepalived/ha_check.conf（含 LOCAL_ROOT_PASS），保持与节点一致。

    注意：仅对「root 可达」的节点执行；离线节点需在恢复后用最新配置重新生成部署文件。
    仅改密码（账号名不变）；若改了账号名，需重新生成节点部署文件并初始化。
    """
    ha_user = get_setting("HA_USER") or "ha_monitor"
    ha_pass = get_setting("HA_PASS") or ""
    repl_user = get_setting("REPL_USER") or "repl"
    repl_pass = get_setting("REPL_PASS") or ""
    app_user = get_setting("APP_USER") or "app"
    app_pass = get_setting("APP_PASS") or ""
    if not ha_pass or not repl_pass:
        return 0
    mgmt_host = get_mgmt_host()
    mgmt_port = MGMT["port"]
    mgmt_user = MGMT["user"]
    mgmt_pass = MGMT["password"]
    mgmt_db = MGMT["db"]

    synced = 0
    for n in list_nodes():
        ip = n["ip"]
        port = int(n["port"])
        root_user = n.get("root_user") or "root"
        root_pass = n.get("root_pass") or ""

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
        kd = Path(NODES_DIR) / ip / "keepalived"
        kd.mkdir(parents=True, exist_ok=True)
        ha_check = (
            f"LOCAL_HOST={ip}\n"
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
        synced += 1
    return synced


def list_nodes():
    try:
        return query(
            "SELECT ip, port, root_user, root_pass, server_id, compose_path, created_at "
            "FROM nodes ORDER BY ip"
        )
    except Exception:
        return []


def delete_node(ip):
    try:
        execute("DELETE FROM nodes WHERE ip=%s", (ip,))
        execute("DELETE FROM hosts WHERE host=%s", (ip,))
        # 若被删节点恰是当前主，清除 current_master 以便监控重新选举
        cur = query("SELECT host, port FROM current_master WHERE id=1")
        if cur and cur[0]["host"] == ip:
            execute("DELETE FROM current_master WHERE id=1")
    except Exception as e:
        raise ValueError(f"删除节点失败: {e}")
    _refresh_all_keepalived_conf()
