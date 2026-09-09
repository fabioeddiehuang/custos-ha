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
import time
from pathlib import Path

import pymysql

from .db import get_setting, query, execute
from .mysql_compat import default_auth_plugin, is_mariadb_node
from .config import APP_VERSION, NODES_DIR, NODES_HOST_BASE, get_manager_public_ip, get_manager_docker_subnet_wildcard, VIP, VIP_PREFIX, INTERFACE, VRID, VIP_PORT, is_semi_sync_enabled
from .ha_client import get_status, ensure_ha_creds
from .clusters import get_cluster_creds

logger = logging.getLogger("node_gen")

# 节点 keepalived 镜像 tag 所用版本号，与管理器镜像 tag 保持一致。
# 唯一来源为 .env 的 VERSION=（经 config.APP_VERSION 读取）；
# 仅当取不到时兜底 v2.0.1，避免 tag 变成空值。
PKG_VERSION = APP_VERSION or "v2.0.1"

# VIP 非法（如集群尚未设置 VIP）时，keepalived.conf 刷新会被跳过并告警。
# 该告警每周期都打会刷屏，这里按 ip 做冷却（默认 5 分钟最多提示一次）。
_vip_warn_ts = {}

NODE_ASSETS = Path(__file__).parent / "node_assets" / "keepalived"
NODE_SCRIPTS = Path(__file__).parent / "node_assets"

# 容器化 MariaDB 节点（node_type='mariadb-docker'）使用的镜像。
#
# 选型原则：**统一使用 Debian 构建的官方镜像**，以同时兼容 Anolis(RHEL 系) 与 Ubuntu(Debian 系)。
# UBI 构建（mariadb:12.3.3-ubi）有两个跨发行版兼容性大坑，实测正是 Ubuntu 节点异常的诱因：
#   1) UBI 镜像默认 USER=mysql(uid 999)：宿主数据目录属主为 root 时直接
#      "Errcode: 13 Permission denied" 起不来，必须靠 --user root + 入口脚本 chown 兜底；
#   2) UBI 镜像没有 /etc/mysql/my.cnf，mariadbd 只读 /etc/my.cnf（!includedir /etc/my.cnf.d），
#      与 Debian 构建的 /etc/mysql/conf.d 路径不同，配置挂载易失效。
# 另：Debian 构建的 12.3.3 不要求 UBI 的 x86-64-v3 指令集，【老 CPU 兼容性已优于 UBI】；
# 12.3.2 作为同系列（Debian 构建、版本更保守）的备用，拉取失败或个别老 CPU 不兼容时自动回退。
# 口径与容器化 MariaDB 部署保持一致（以 12.3.3 为主、12.3.2 为备）。
MARIADB_DOCKER_IMAGE = "mariadb:12.3.3"
MARIADB_DOCKER_IMAGE_FALLBACK = "mariadb:12.3.2"

# 宿主数据目录：统一收敛到 /data/custos-ha/<引擎>，便于备份/巡检时按引擎识别。
MARIADB_DOCKER_DATA_DIR = "/data/custos-ha/mariadb12"
MYSQL_DOCKER_DATA_DIR = "/data/custos-ha/mysql8"
MARIADB_DOCKER_CONTAINER = "mariadb-ha-node"

# ---- 统一日志目录（三形态一致）----
# MySQL docker / MariaDB 裸机 / MariaDB docker 的日志统一收敛到宿主
#   /data/custos-ha/logs/<数据目录基名>/  例：logs/mysql8、logs/mariadb12、logs/mariadb
# 容器内统一挂到 LOG_DIR_CT。背景：MySQL 旧配置把 error.log 写在 /var/lib/mysql-files，
# 该路径**未挂载到宿主**，容器一销毁/重建日志就没了，是排错时最难查的一类问题。
LOG_ROOT_HOST = "/data/custos-ha/logs"
LOG_DIR_CT = "/var/log/custos-ha"


def _log_dir_host(data_dir):
    """由数据目录推导宿主日志目录：/data/custos-ha/mysql8 -> /data/custos-ha/logs/mysql8。

    按「数据目录基名」分目录，天然区分同机混部的不同形态（mysql8 / mariadb12 / mariadb）。
    """
    base = os.path.basename((data_dir or "").rstrip("/")) or "default"
    return "%s/%s" % (LOG_ROOT_HOST, base)

# 节点侧 keepalived 客户端镜像名。项目由 mysql-ha-manager 更名为 Custos 后，
# 镜像名也必须同步（否则新拉起的节点仍去找已废弃的 mysql-ha-keepalived:<版本>）。
KEEPALIVED_IMAGE_NAME = "custos-node"

# Node OS metrics collector agent (custos-node-agent) sidecar, deployed with the node compose.
# Reads real host metrics: pid: host shares host PID ns for /proc; read-only mount / -> /host
# with --root /host for real disk stats; network_mode: host lets manager reach <nodeIP>:28889.
# node_agent.py is copied into the node unit dir (./node_agent.py) by generate_node/regenerate.
AGENT_SERVICE = r"""
  custos-node-agent:
    image: python:3-slim
    container_name: custos-node-agent
    command:
      - python3
      - /app/node_agent.py
      - --port
      - '28889'
      - --bind
      - 0.0.0.0
      - --root
      - /host
    restart: always
    pid: host
    network_mode: host
    volumes:
      - /:/host:ro
      - ./node_agent.py:/app/node_agent.py:ro
"""

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
    # 脚本顺序执行 4 次 mysql 探活（SELECT 1 / read_only / super_read_only / current_master），
    # 单次内部上限 timeout 2s，最坏累计约 8s（见 check_mysql_master.sh）。keepalived 的 timeout
    # 必须明显大于脚本最坏耗时，否则节点一忙/重启恢复时脚本尚未返回就被误杀，触发
    # “failed to terminate after kill / timed out, expect idle - skipping run”，健康检查冻死。
    interval 12
    timeout 10
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
    # 端口映射：节点自身端口 + VIP 端口都映射到容器内同一端口（{port}，与 my.cnf 的 port= 一致），
    # 使「业务连 VIP:VIP_PORT」在任意主库节点上均可达（VIP 浮动到哪台，哪台就提供该端口）。
    port_lines = f'      - "{port}:{port}"\n'
    if str(vip_port) != str(port):
        port_lines += f'      - "{vip_port}:{port}"\n'
    return f"""# 自动生成：MySQL 8.0 + keepalived 合一节点部署
# 前置：目标主机需支持 --network host 与 NET_ADMIN/NET_RAW（Linux）；首次启动会创建 ha_monitor。
# 运行： cd 本目录 && docker compose up -d
services:
  mysql:
    image: mysql:8.0
    container_name: custos-mysql
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: "{root_pass}"
      # 全站统一中国时间：节点 MySQL 的 NOW()/错误日志时间均为北京时间(UTC+8)
      TZ: "Asia/Shanghai"
      # 跳过官方镜像在初始化阶段对 /usr/share/zoneinfo 的全量装载（耗时数秒且会刷一堆
      # "Unable to load ... as time zone" 警告；仅需系统时区，无需命名时区表）。
      MYSQL_INITDB_SKIP_TZINFO: "1"
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
      # I/O 相关（避免沿用 mysql:8.0 镜像 128MB buffer pool / 200 io_capacity 的默认极小值，
      # 否则副本追日志、主库刷脏时会把磁盘 util 打满）。buffer pool 可按宿主机内存再调大。
      - --innodb-buffer-pool-size=1G
      - --innodb-io-capacity=2000
      - --innodb-io-capacity-max=4000
      # 并行复制：加快从库回放，避免高写入/大事务下复制延迟持续累积（如单节点长期滞后）。
      # LOGICAL_CLOCK 保证提交顺序一致；preserve_commit_order=ON 维持从库提交序与主机一致。
      - --slave-parallel-workers=4
      - --slave-parallel-type=LOGICAL_CLOCK
      - --slave-preserve-commit-order=ON
    ports:
{port_lines}
    volumes:
      # MySQL 数据目录：统一 /data/custos-ha/mysql8（与 MariaDB 节点的 mariadb12 对齐）。
      # :z 供 Anolis 等 SELinux=Enforcing 系统重打标签；未启用 SELinux 的 Ubuntu 会忽略该标记，
      # 故同一份 compose 同时兼容两类系统。
      - {MYSQL_DOCKER_DATA_DIR}:/var/lib/mysql:z
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      # 慢日志覆盖配置（slow_query_log=1 + log_output=TABLE），供 slow_log 采集 mysql.slow_log。
      # 必须挂载，否则节点慢查询不入库，/api/slow_log 永远空（节点默认 slow_query_log=OFF）。
      - ./ha-overrides.cnf:/etc/mysql/conf.d/ha-overrides.cnf:ro
      # 统一日志目录：error.log 等全部落到这里（宿主 logs/<基名> -> 容器 /var/log/custos-ha）。
      # 旧值 /var/lib/mysql-files 未挂到宿主，容器一销毁日志就没了，是排错时最难查的一类问题。
      # 首次部署前需先在宿主建目录并 chown 给容器内 mysql(uid 999)：见同目录 prepare-logs.sh
      - {_log_dir_host(MYSQL_DOCKER_DATA_DIR)}:/var/log/custos-ha:z
    logging:
      # 容器 stdout 上限：防崩溃循环时 docker 日志刷爆磁盘（与 error.log 的 logrotate 互补）
      driver: json-file
      options:
        max-size: "50m"
        max-file: "3"
    depends_on:
      init-logs:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-p{root_pass}"]
      interval: 5s
      timeout: 3s
      retries: 10

  # 一次性初始化：把【数据目录】与【统一日志目录】的属主改为容器内 mysql(uid 999)。
  # 宿主挂载目录默认由 root 创建（0755 root:root），mysqld 以 mysql 运行写不了 error.log，
  # 表现为 Could not open file '/var/log/custos-ha/error.log' ... Permission denied → 启动即退出。
  # 复用节点已有的 mysql 镜像（无需联网）；service_completed_successfully 保证先于 mysqld 完成。
  init-logs:
    image: mysql:8.0
    container_name: custos-init-logs
    user: "0:0"
    entrypoint:
      - sh
      - -c
      - "chown -R 999:999 /var/lib/mysql /var/log/custos-ha && echo logs-dir-ready"
    volumes:
      - {MYSQL_DOCKER_DATA_DIR}:/var/lib/mysql:z
      - {_log_dir_host(MYSQL_DOCKER_DATA_DIR)}:/var/log/custos-ha:z
    restart: "no"

  keepalived:
    # 模板化镜像：优先使用已编译并导入的镜像 custos-node:<VERSION>（无需联网编译）。
    # 若目标主机无法编译（如离线），可在可编译的机器上 `docker compose build keepalived`
    # 后 `docker save custos-node:latest -o custos-node.tar` 拷贝过来
    # `docker load -i custos-node.tar`，再 `docker compose up --no-build` 启动。
    image: {KEEPALIVED_IMAGE_NAME}:{PKG_VERSION}
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
    """.replace("{KEEPALIVED_IMAGE_NAME}", KEEPALIVED_IMAGE_NAME) + AGENT_SERVICE


def _semisync_block():
    """半同步复制配置片段（仅 SEMI_SYNC=1 时返回非空，否则空串）。

    写进 my.cnf 使半同步随实例启动即生效、重启不丢——根治「升版/重启后
    SET GLOBAL 清零导致半同步静默退化为异步」的反复问题。MariaDB 半同步插件
    为内置，plugin_load_add 保证启动时加载，变量即可设置，不会因插件缺失
    导致节点启动失败。
    """
    if not is_semi_sync_enabled():
        return ""
    return (
        "# ---- 半同步复制（SEMI_SYNC=1：插件内置，随启动生效，重启不丢）----\n"
        "plugin_load_add = rpl_semi_sync_master;rpl_semi_sync_slave\n"
        "rpl_semi_sync_master_enabled = 1\n"
        "rpl_semi_sync_slave_enabled = 1\n"
        "rpl_semi_sync_master_timeout = 10000\n"
        "rpl_semi_sync_master_wait_point = AFTER_SYNC\n"
    )


def _inject_semisync(cnf):
    """把半同步配置片段注入到 [mysqld] 段开头（仅 SEMI_SYNC=1 时插入）。"""
    blk = _semisync_block()
    if blk:
        cnf = cnf.replace("[mysqld]\n", "[mysqld]\n" + blk, 1)
    return cnf


def _mysql_version_tuple(v):
    """解析 MySQL 版本；项目 MySQL 集群仅支持 8.0.46，解析不出时按 8.0.46 处理。

    注意（踩过的坑）：v=None 时 str(None)=="None" 解析出空列表、补零后得到 (0,0,0)，
    **不会抛异常**，所以不能只靠 except 兜底——必须显式判断空列表。
    旧逻辑因此把「版本留空」判成远古版本，生成 8.0.46 上不存在的半同步旧命名。
    """
    try:
        parts = [int(x) for x in str(v).split(".")[:3] if x.strip().isdigit()]
        if not parts:
            return (8, 0, 46)
        while len(parts) < 3:
            parts.append(0)
        return tuple(parts[:3])
    except Exception:
        # 版本未知/解析失败时一律按 8.0.26+ 处理（官方 mysql:8.0 镜像现为 8.0.4x）。
        # MySQL 8.0.26 起半同步插件与变量由 master/slave 改名为 source/replica，
        # 旧 soname（semisync_master.so）在 8.0.26+ 已随改名被移除，写进 my.cnf 会导致
        # mysqld 启动即失败、容器 restart:always 崩溃循环（集群起不来）。未知版本走新命名。
        return (8, 0, 26)


def _semisync_block_mysql(mysql_version=None):
    """MySQL(Docker) 节点的半同步 my.cnf 片段（仅 SEMI_SYNC=1 返回非空）。

    写进 ha-overrides.cnf，随容器启动即加载插件并开启半同步——根治「节点重启后
    SET GLOBAL 清零导致半同步静默退化为异步」以及「远程 root 无 SUPER/INSERT 权限、
    运行时 INSTALL PLUGIN 被拒」的问题（插件改为随启动加载，无需运行时安装）。
    MySQL 8.0.26+ 插件/变量改名为 source/replica；更早 8.0 用 master/slave。
    """
    if not is_semi_sync_enabled():
        return ""
    # 项目 MySQL 集群锁定 8.0.46（属 8.0.26+ 命名体系），固定使用 source/replica，
    # 不再按版本分支生成 master/slave 旧命名：8.0.46 上 rpl_semi_sync_master_* 等
    # 旧变量名已废弃/不存在，写进 my.cnf 会让 mysqld 启动即失败、容器崩溃循环。
    names = "semisync_source.so;semisync_replica.so"
    src, rep = "rpl_semi_sync_source", "rpl_semi_sync_replica"
    return (
        "# ---- 半同步复制（SEMI_SYNC=1：随启动加载插件，重启不丢）----\n"
        f"plugin_load_add = {names}\n"
        "# rpl_semi_sync_*_enabled 不写进静态 my.cnf：mysqld --initialize 期间插件未加载，"
        "# 写它们会报 unknown variable 并中止初始化（容器崩溃循环）。"
        "# 开关由管理器运行时 SET GLOBAL 开启（ensure_ha_creds），随自愈周期持久化。"
    )


def render_mariadb_my_cnf(server_id, port, gtid_domain_id=None, data_dir="/data/mariadb", install_dir="/opt/mariadb"):
    """生成 MariaDB 裸机节点的【完整 my.cnf】（非片段，可直接用）。

    MariaDB 的 GTID 机制与 MySQL【根本不兼容】：MariaDB 用 gtid_domain_id + 基于 server_id
    的序列 GTID（形如 0-1-100），没有 MySQL 的 gtid_mode 变量，复制挂载用
    MASTER_USE_GTID=slave_pos（不是 AUTO_POSITION=1）。

    该配置文件由 Install-ha-mariadb-Node.sh 在节点上全量落盘，确保「配置一致性」：
    每个 MariaDB 节点的 server_id / gtid_domain_id / port / 数据目录 / binlog 完全一致风格，
    避免手动改配置带来的漂移（用户要求：配置一致性由服务端生成保证）。
    """
    if gtid_domain_id is None:
        # 默认域：统一使用 0（不推荐；generate_node 现已显式传入 server_id 作为非 0 域）
        gtid_domain_id = 0
    _cnf = (
        "# ---- Custos 生成的 MariaDB 节点配置（完整 my.cnf，Install 脚本落盘）----\n"
        "[mysqld]\n"
        f"server-id = {server_id}\n"
        f"gtid_domain_id = {gtid_domain_id}\n"
        "# MariaDB 默认启用 GTID，无需 gtid_mode（与 MySQL 不兼容，请勿添加）\n"
        "# gtid_strict_mode=ON：强制每个 domain 内 GTID 单调递增；默认 OFF 时切换后从库可能因\n"
        "# 乱序 GTID 停在旧位点而漏事务，HA 场景必须开。（已存在乱序 GTID 的老集群需先核对再开）\n"
        "gtid_strict_mode = ON\n"
        "log_bin = mysql-bin\n"
        "log_slave_updates = ON\n"
        "relay_log = relay-bin\n"
        "relay_log_recovery = ON\n"
        "sync_binlog = 1\n"
        "innodb_flush_log_at_trx_commit = 1\n"
        "binlog_format = ROW\n"
        "read_only = ON\n"
        "# 复制/性能（与 docker 节点保持一致，避免单节点复制延迟累积）\n"
        "innodb_buffer_pool_size = 1G\n"
        "innodb_io_capacity = 2000\n"
        "innodb_io_capacity_max = 4000\n"
        "slave_parallel_threads = 4\n"
        "slave_parallel_mode = optimistic\n"
        "# 慢日志（供观测/排错）\n"
        "slow_query_log = 1\n"
        "long_query_time = 0.5\n"
        "log_output = TABLE\n"
        "log_queries_not_using_indexes = 0\n"
        # 错误日志落盘到统一日志目录（/data/custos-ha/logs/<基名>），而非数据目录：
        # 与 MySQL docker / MariaDB docker 两形态保持一致，排错时按同一路径找；
        # MariaDB 不支持错误日志写表(mysql.error_log 仅 MySQL 8.0 支持)，只能走文件；
        # secure_file_priv='' 放开 LOAD_FILE 路径限制，使面板在容器/裸机都能读到。
        f"log_error = {_log_dir_host(data_dir)}/error.log\n"
        "secure_file_priv = ''\n"
        f"port = {port}\n"
        f"datadir = {data_dir}\n"
        f"socket = {data_dir}/mysql.sock\n"
        f"pid_file = {data_dir}/mariadb.pid\n"
        "bind_address = 0.0.0.0\n"
        "skip-name-resolve\n"
        "# 字符集\n"
        "character_set_server = utf8mb4\n"
        "collation_server = utf8mb4_general_ci\n"
        "# 允许 root 从管理器 IP 连入（Install 脚本会创建 root@'<管理器IP>'）\n"
        "# 认证插件统一 mysql_native_password，便于老驱动/管理器连接\n"
        "\n"
        "# 客户端默认连接参数：使 `mysql -uroot -p` 无需指定 -h/-P/-S 即可登录\n"
        "[client]\n"
        f"port = {port}\n"
        f"socket = {data_dir}/mysql.sock\n"
        "default-character-set = utf8mb4\n"
    )
    return _inject_semisync(_cnf)


def render_mariadb_docker_my_cnf(server_id, port, gtid_domain_id=None):
    """容器化 MariaDB 节点的配置片段（docker-my.cnf，由 Install 脚本挂载进容器）。

    与裸机 my.cnf 的**唯一**差别：不写 datadir / socket / pid_file —— 这三个路径
    在官方镜像内是 /var/lib/mysql 与 /run/mariadb/mariadb.sock，若沿用宿主的
    /data/mariadb 会导致容器起不来。其余（server-id / gtid_domain_id / port /
    bind-address / 复制与慢日志参数）与裸机完全一致，保证两种形态同集群混部时
    复制行为一致。

    落盘位置由 compose（render_mariadb_compose）决定：同时挂载到
      - /etc/my.cnf.d/99-ha.cnf      （UBI 构建：/etc/my.cnf 里 !includedir /etc/my.cnf.d）
      - /etc/mysql/conf.d/99-ha.cnf  （Debian 构建：/etc/mysql/my.cnf 里 !includedir）
    两条都挂，使 12.3.3-ubi 与 12.3.2 任一镜像都能读到；99- 前缀保证最后加载、覆盖默认。
    """
    if gtid_domain_id is None:
        gtid_domain_id = server_id
    _cnf = (
        "# ---- Custos 生成的容器化 MariaDB 节点配置（docker-my.cnf，由 Install 脚本挂载进容器）----\n"
        "# 注意：容器内数据目录固定 /var/lib/mysql、socket 固定 /run/mariadb/mariadb.sock，\n"
        "#       故本文件【不设】datadir / socket / pid_file，请到 Install 脚本改 MARIA_DATA_DIR。\n"
        "[mysqld]\n"
        f"server-id = {server_id}\n"
        f"gtid_domain_id = {gtid_domain_id}\n"
        "# MariaDB 默认启用 GTID，无需 gtid_mode（与 MySQL 不兼容，请勿添加）\n"
        "# gtid_strict_mode=ON：强制每个 domain 内 GTID 单调递增；默认 OFF 时切换后从库可能因\n"
        "# 乱序 GTID 停在旧位点而漏事务，HA 场景必须开。（已存在乱序 GTID 的老集群需先核对再开）\n"
        "gtid_strict_mode = ON\n"
        "log_bin = mysql-bin\n"
        "log_slave_updates = ON\n"
        "relay_log = relay-bin\n"
        "relay_log_recovery = ON\n"
        "sync_binlog = 1\n"
        "innodb_flush_log_at_trx_commit = 1\n"
        "binlog_format = ROW\n"
        "read_only = ON\n"
        "# 复制/性能（与裸机 MariaDB 节点保持一致）\n"
        "# 缓冲池默认 512M（而非官方/裸机模板的 1G）：小内存主机（如 2G）用 1G 极易 OOM。\n"
        "# 内存充足可调大：compose 场景改 MARIADB_INNODB_BUFFER_POOL_SIZE 环境变量即可覆盖。\n"
        "innodb_buffer_pool_size = 512M\n"
        "innodb_io_capacity = 2000\n"
        "innodb_io_capacity_max = 4000\n"
        "slave_parallel_threads = 4\n"
        "slave_parallel_mode = optimistic\n"
        "# 慢日志（供观测/排错）\n"
        "slow_query_log = 1\n"
        "long_query_time = 0.5\n"
        "log_output = TABLE\n"
        "log_queries_not_using_indexes = 0\n"
        "# 错误日志落盘到统一日志目录（容器内 /var/log/custos-ha，由 compose 挂载到宿主\n"
        "# /data/custos-ha/logs/<基名>）：官方镜像默认写 stdout（@@log_error 为空），会导致\n"
        "# 管理器「错误日志」面板 LOAD_FILE 读不到、永远显示为空；统一目录既持久又跨形态一致。\n"
        f"log_error = {LOG_DIR_CT}/error.log\n"
        "# 放开 secure_file_priv，使管理器 LOAD_FILE 可读错误日志文件（容器镜像可能限制路径），"
        "# 作为「错误日志」面板读取 mysql.error_log 表失败时的兜底；仅放开文件导入/导出，HA 账号不受影响。"
        "secure_file_priv = ''\n"
        f"port = {port}\n"
        "bind_address = 0.0.0.0\n"
        "skip-name-resolve\n"
        "# 字符集\n"
        "character_set_server = utf8mb4\n"
        "collation_server = utf8mb4_general_ci\n"
    )
    return _inject_semisync(_cnf)


def _q(s):
    """转义 SQL 字符串字面量（反斜杠 + 单引号），供所有把凭证拼进 SQL 的模板使用。

    背景（重要）：集群的账号名/密码允许管理员自定义，一旦含 `'` 或 `\\`，此前直接以
    `'%s'` 拼进 `CREATE/ALTER USER ... IDENTIFIED BY '...'` 会**破坏整条语句的语法**，
    该语句执行失败 -> 这个账号就静默建不出来。表现为：
      - 「我自己设置的用户名/密码怎么都登不上」（账号根本不存在）；
      - 安装脚本账号校验报「账号未创建成功」。
    注意：仅用于字符串字面量；主机名/掩码等标识符不在此列。
    """
    return str(s if s is not None else "").replace("\\", "\\\\").replace("'", "''")


def render_mariadb_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass):
    """MariaDB 节点的 init.sql：创建 ha_monitor / replication / app 账号 + ha_local 状态库。

    与 MySQL init.sql 的差异：
      - MariaDB 无 gtid_mode 变量，跳过 SET GLOBAL gtid_mode 那几行。
      - GTID 由 my.cnf 的 gtid_domain_id 保证，无需在线开启。
      - CREATE/ALTER USER 用 mysql_native_password（MariaDB 10.4+ 默认 ed25519，
        显式指定以确保管理器用 native 密码协议连接稳定）。
    """
    return (
        # MariaDB 默认已开启 GTID（gtid_domain_id 在 my.cnf 指定），无需 SET gtid_mode。
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # 注意：MariaDB 中 ALTER USER / DROP USER **不是**可授权权限——它们由 CREATE USER 权限
        # 隐含覆盖（CREATE USER 特权即允许对任意用户 CREATE/ALTER/DROP USER）。若把它们写进
        # GRANT 会直接报 1064 语法错误，导致 init 整段失败。故只授 CREATE USER。
        # BINLOG ADMIN 必须显式授予：MariaDB 10.5+ 把 `SET SESSION sql_log_bin` 从 SUPER 中拆出，
        # 缺它时管理器写 ha_local.state（需禁 binlog）会报 1227，主库标记一直写不进去、
        # keepalived 只能拿到空状态退化判定。
        "GRANT CREATE USER, CREATE, INSERT, UPDATE, DELETE, REPLICATION SLAVE, "
        "REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, SLAVE MONITOR, READ_ONLY ADMIN, BINLOG ADMIN, "
        "REPLICATION CLIENT, PROCESS, "
        "SELECT, RELOAD, SUPER ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # replication 复制账号（从库从其自身 IP 连，必须 @'%%'）
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT REPLICATION SLAVE, REPLICATION SLAVE ADMIN ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # 业务最小权限账号
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, "
        "REFERENCES, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # 本地状态库：切换主库时写入当前主，keepalived 检查脚本据此持有/释放 VIP
        "CREATE DATABASE IF NOT EXISTS ha_local;\n"
        "CREATE TABLE IF NOT EXISTS ha_local.state (k VARCHAR(32) PRIMARY KEY, v VARCHAR(255));\n"
        "FLUSH PRIVILEGES;\n"
    ) % (
        _q(ha_user), _q(ha_pass), _q(ha_user),
        _q(ha_user), _q(ha_pass),
        _q(repl_user), _q(repl_pass), _q(repl_user),
        _q(repl_user), _q(repl_pass),
        _q(app_user), _q(app_pass), _q(app_user),
        _q(app_user), _q(app_pass),
    )


def render_baremetal_compose(node_name, vip_port=None):
    """裸机 MariaDB 节点（node_type=mariadb）的 keepalived-only compose（容器化 MariaDB 走 render_mariadb_compose），只跑 keepalived 客户端。

    - 裸机 MariaDB：mysqld 由宿主 systemd 提供；
    - 容器化 MariaDB（node_type=mariadb-docker）已改为纯 compose（见 render_mariadb_compose），
       本函数只服务裸机形态（keepalived-only）；需要容器化请创建节点时直接选 mariadb-docker（纯 compose）。

    keepalived 容器使用 host 网络，连接 127.0.0.1:<port> 探活本机 MariaDB。
    """
    if vip_port is None:
        vip_port = VIP_PORT
    return (
        "# 自动生成：MariaDB 节点（裸机 / 容器化）的 keepalived-only 客户端部署\n"
        "# 前置：目标机 MariaDB 已就绪并创建账号（init_mariadb.sql）——\n"
        "#       裸机形态：由 Install-ha-mariadb-Node.sh 全量安装（宿主 systemd 管理 mysqld）；\n"
        "#       （注：容器化 MariaDB 节点 node_type=mariadb-docker 已改用 render_mariadb_compose 纯 compose，不走本函数）。\n"
        "# 运行： cd 本目录 && docker compose up -d\n"
        "services:\n"
        "  keepalived:\n"
        "    image: {KEEPALIVED_IMAGE_NAME}:{PKG_VERSION}\n"
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
    ).replace("{KEEPALIVED_IMAGE_NAME}", KEEPALIVED_IMAGE_NAME) \
     .replace("{PKG_VERSION}", PKG_VERSION) + AGENT_SERVICE


def render_mariadb_compose(node_name, root_pass, port, server_id, vip_port=None,
                           data_dir=None, image=None, buffer_pool="512M"):
    """容器化 MariaDB 节点的【纯 compose】部署文件（推荐方式）。

    与 render_baremetal_compose()（只跑 keepalived、MariaDB 由安装脚本另起）不同，
    本函数把 MariaDB 容器与 keepalived 容器一并纳入 compose，节点部署即一条
    `docker compose up -d`，与 MySQL 8.0 容器节点（_render_compose）的形式完全对齐。

    关键设计（对应「兼容 Anolis / Ubuntu / Anolis 7.9 / 老 CPU / 低内存」）：
      1) 端口：容器内 MariaDB 监听 port（由 docker-my.cnf 的 port= 决定，与裸机形态一致），
         宿主同时把「节点端口」与「VIP 端口」映射到容器同一端口 —— 业务连 VIP:VIP_PORT
         无需在节点上配 iptables/firewalld 转发（避开 firewalld 与 ufw/iptables 差异，
         Ubuntu 与 Anolis 行为统一）。
      2) 配置：docker-my.cnf 同时挂到 /etc/my.cnf.d 与 /etc/mysql/conf.d，
         Debian 构建与 UBI 构建任一镜像都能读到（99- 前缀保证最后加载、覆盖默认值）。
      3) 权限：以 root(uid 0) 启动，官方 entrypoint 先 chown 数据目录再 gosu 降权为 mysql，
         规避 UBI 构建默认 USER=mysql(uid 999) 时宿主目录属主不符导致的 Errcode 13。
      4) 低内存：buffer pool 默认 512M（官方默认与裸机模板的 1G 在小内存机易 OOM），
         可按宿主机内存调整 MARIADB_INNODB_BUFFER_POOL_SIZE。
      5) 挂载 :z 兼顾 Anolis 的 SELinux=Enforcing；未启用 SELinux 的 Ubuntu 会忽略该标记。
    """
    if vip_port is None:
        vip_port = VIP_PORT
    if data_dir is None:
        data_dir = MARIADB_DOCKER_DATA_DIR
    if image is None:
        image = MARIADB_DOCKER_IMAGE
    # 端口映射：节点自身端口 + VIP 端口 -> 容器内同一端口
    port_lines = f'      - "{port}:{port}"\n'
    if str(vip_port) != str(port):
        port_lines += f'      - "{vip_port}:{port}"\n'
    return f"""# 自动生成：MariaDB 容器 + keepalived 合一节点部署（纯 compose）
# 前置：目标主机已安装 Docker / Docker Compose（无需预装 MariaDB 及任何依赖）。
# 运行： cd 本目录 && docker compose up -d
# 镜像：{image}（Debian 构建，同时兼容 Anolis 与 Ubuntu；老 CPU 主机请勿改用 UBI 构建）
services:
  mariadb:
    image: {image}
    container_name: custos-mariadb
    restart: always
    # 以 root 启动：官方 entrypoint 会先把数据目录 chown 给 mysql，再 gosu 降权运行 mariadbd。
    # 规避 UBI 构建默认 USER=mysql(uid 999) 时宿主数据目录属主为 root 导致的 Errcode 13。
    # 入口前先修一次统一日志目录属主（宿主挂载目录默认 root:root，mariadbd 写不了 error.log），
    # 把原本独立的 init-logs 一次性容器职责合并进来，避免 restart:no + depends_on
    # service_completed_successfully 在 docker compose down/up 重建时卡住、导致节点起不来。
    user: "0:0"
    # 关键：Docker Compose v2 在用【列表】覆盖 entrypoint 时不会把镜像默认 CMD(mariadbd)
    # 追加进来，若这里只写官方入口而不带 mariadbd，最终 exec 拿到空参数 -> 入口 _main
    # 落到末尾 `exec "$@"`(空) -> 立即退出码 0、零日志、容器 Restarting 卡死。
    # 故把 mariadbd 显式写进 entrypoint 数组，确保 $@ 始终含 mariadbd（不再依赖被丢弃的 CMD）。
    entrypoint:
      - sh
      - -c
      - "chown -R 999:999 /var/log/custos-ha 2>/dev/null || chmod -R 777 /var/log/custos-ha 2>/dev/null; exec $@"
      - wrap
      - /usr/local/bin/docker-entrypoint.sh
      - mariadbd
    environment:
      MARIADB_ROOT_PASSWORD: "{root_pass}"
      # 全站统一中国时间：节点 NOW()/日志时间均为北京时间(UTC+8)
      TZ: "Asia/Shanghai"
      # 低内存主机友好：官方默认与裸机模板为 1G，小内存机易 OOM。内存充足可调至 1G/2G。
      MARIADB_INNODB_BUFFER_POOL_SIZE: "{buffer_pool}"
      # 跨小版本升级时自动执行 mariadb-upgrade，避免数据字典未升级导致起不来
      MARIADB_AUTO_UPGRADE: "1"
    ports:
{port_lines}    volumes:
      # 数据目录：统一 /data/custos-ha/mariadb12
      - {data_dir}:/var/lib/mysql:z
      # 配置双挂：Debian 构建读 /etc/mysql/conf.d，UBI 构建读 /etc/my.cnf.d，两条都挂
      # 可让任一镜像都生效；99- 前缀保证最后加载、覆盖镜像默认值。
      - ./docker-my.cnf:/etc/my.cnf.d/99-ha.cnf:ro,z
      - ./docker-my.cnf:/etc/mysql/conf.d/99-ha.cnf:ro,z
      # 初始化 SQL：仅在数据目录为空的首次启动执行；已初始化节点改由 fix-node-creds.sql
      # 或管理器 ensure_ha_creds 自愈校正账号（不会重复执行导致报错）。
      - ./init_mariadb.sql:/docker-entrypoint-initdb.d/init_mariadb.sql:ro,z
      # 统一日志目录：error.log 等全部落到这里（宿主 logs/<基名> -> 容器 /var/log/custos-ha）
      - {_log_dir_host(data_dir)}:/var/log/custos-ha:z
    logging:
      # 容器 stdout 上限：防崩溃循环时 docker 日志刷爆磁盘（与 error.log 的 logrotate 互补）
      driver: json-file
      options:
        max-size: "50m"
        max-file: "3"
    healthcheck:
      # 用镜像自带 healthcheck.sh：正确处理账密，规避密码含特殊字符时的解析问题
      test: ["CMD", "healthcheck.sh", "--connect", "--innodb_initialized"]
      interval: 5s
      timeout: 5s
      retries: 12
      start_period: 60s

  # （日志目录属主修正已合并进上面的 mariadb 服务入口；取消独立 init-logs 容器，
  #  避免 docker compose down/up 重建时 restart:no + depends_on 卡住节点。）


  keepalived:
    image: {KEEPALIVED_IMAGE_NAME}:{PKG_VERSION}
    build: ./keepalived
    restart: always
    network_mode: host
    cap_add:
      - NET_ADMIN
      - NET_BROADCAST
      - NET_RAW
    depends_on:
      - mariadb
    volumes:
      - ./keepalived/keepalived.conf:/etc/keepalived/keepalived.conf:ro
      - ./keepalived/check_mysql_master.sh:/etc/keepalived/check_mysql_master.sh:ro
      - ./keepalived/notify_master.sh:/etc/keepalived/notify_master.sh:ro
      - ./keepalived/notify_backup.sh:/etc/keepalived/notify_backup.sh:ro
      - ./keepalived/ha_check.conf:/etc/keepalived/ha_check.conf:ro
""" + AGENT_SERVICE


def render_docker_my_cnf(mysql_version=None, port=3306):
    """Docker 节点 MySQL 覆盖配置：开启慢日志（log_output=TABLE），供 slow_log 采集。

    挂载到容器 /etc/mysql/conf.d/ha-overrides.cnf。对 v2.0 已部署集群无副作用，
    仅在该节点重新生成并部署后生效。SEMI_SYNC=1 时额外注入半同步插件加载与开启参数，
    使半同步随容器启动即生效、重启不丢（无需运行时 INSTALL PLUGIN）。
    """
    cnf = (
        "[mysqld]\n"
        "bind-address = 0.0.0.0\n"
        f"port = {port}\n"
        "# 错误日志写入 mysql.error_log 表（log_sink_table 为 MySQL 8.0 内置组件，无需装插件），\n"
        "# 管理器「错误日志」面板直接 SELECT 该表，容器环境下不再依赖 LOAD_FILE / secure_file_priv。\n"
        "log_error_services = log_filter_internal; log_sink_internal\n"
        # 统一日志目录：容器内固定 /var/log/custos-ha（compose 挂到宿主
        # /data/custos-ha/logs/<基名>）。旧值 /var/lib/mysql-files 未挂载到宿主，
        # 容器销毁即丢日志，是排错时最难查的一类问题。
        f"log_error = {LOG_DIR_CT}/error.log\n"
        # 放开 secure_file_priv：使管理器「错误日志」面板能用 LOAD_FILE(@@log_error)
        # 读到上述新路径（MySQL 容器默认仅允许 /var/lib/mysql-files）。
        "secure_file_priv = ''\n"
        "skip-name-resolve\n"
        "slow_query_log = 1\n"
        "long_query_time = 0.5\n"
        "log_output = TABLE\n"
        "log_queries_not_using_indexes = 0\n"
    )
    blk = _semisync_block_mysql(mysql_version)
    if blk:
        cnf = cnf.replace("[mysqld]\n", "[mysqld]\n" + blk, 1)
    return cnf


def render_logrotate_conf():
    """生成 logrotate 配置（三形态共用）：统一日志目录下的 *.log 按「天数 + 大小上限」轮转。

    为什么用 copytruncate 而不是默认的 rename+新建：mysqld 会长期持有 error.log 的句柄，
    默认「改名 + 新建」会让 mysqld 继续往**已改名**的旧文件写，表现为轮转后日志不再更新。
    copytruncate 复制后截断原文件，句柄/属主/权限不变，也不会写丢、无需重启 mysqld。
    """
    return f"""# Custos 节点日志轮转（自动生成，三形态共用）
# 覆盖：{LOG_ROOT_HOST}/*/ 下所有 .log（mysql8 / mariadb12 / mariadb 各一个目录）
#
# 安装（节点宿主机执行）：
#     cp custos-ha-logrotate.conf /etc/logrotate.d/custos-ha
#     （裸机形态由 Install 脚本自动安装；容器形态请在宿主机手动执行上面这条）
# 校验：logrotate -d /etc/logrotate.d/custos-ha
{LOG_ROOT_HOST}/*/*.log {{
    daily
    rotate 7
    # 大小上限：超过 100M 立即轮转（不等 daily）——节点崩溃循环时几分钟就能刷爆磁盘
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    su root root
}}
"""


def render_prepare_logs(node_type, log_dir):
    """生成日志目录初始化脚本：首次部署前建目录并修正属主。

    必须执行：compose 挂载的宿主目录由 root 自动创建(0755 root:root)，容器内 mysqld 以
    mysql(uid 999) 运行，无权写 error.log —— 表现为 mysqld 起不来或日志恒为空。
    """
    if node_type == "mariadb":
        chown_line = 'if id -u mysql >/dev/null 2>&1; then chown -R mysql:mysql "$LOG_DIR"; fi'
        who = "mysql（系统用户，不存在则跳过 chown）"
    else:
        chown_line = 'chown -R 999:999 "$LOG_DIR"'
        who = "uid/gid 999（容器内 mysql 用户）"
    return f"""#!/bin/sh
# 自动生成：初始化统一日志目录（首次部署 / 重建节点前执行一次即可）
# 统一根目录：{LOG_ROOT_HOST}   属主：{who}
set -eu
LOG_DIR="{log_dir}"
mkdir -p "$LOG_DIR"
{chown_line}
chmod 0755 "$LOG_DIR"
echo "OK: $LOG_DIR ready -> $(stat -c '%U:%G %a' "$LOG_DIR" 2>/dev/null || echo unknown)"
"""

def render_update_node():
    """节点更新/重装脚本（纯 compose，保留数据与日志目录）。

    等价于 `docker compose down && docker compose up -d`：停掉并移除现有容器，
    再按当前目录最新的部署文件重新拉起。管理器随后在数分钟内自动自愈
    （账号 / 复制 / 半同步），无需手动干预。
    """
    return r"""#!/bin/sh
# 自动生成：节点更新 / 重装（纯 compose，数据保留）
# 用法： cd <节点目录> && sh update-node.sh
# 行为：停止并移除现有容器（保留数据卷 / 绑定目录）→ 重新拉起 →
#       管理器在数分钟内自动自愈（账号 / 复制 / 半同步），无需手动干预。
set -e
cd "$(dirname "$0")"
echo "[update-node] 停止并移除现有容器（保留数据与日志目录）..."
docker compose down
echo "[update-node] 重新拉起节点..."
docker compose up -d
echo "[update-node] 完成。节点将在数十秒内启动，管理器自动自愈复制与半同步。"
"""


def _auth_plugin_pref():
    """全局认证插件偏好（settings.AUTH_PLUGIN：auto / native / caching_sha2）。"""
    try:
        return (get_setting("AUTH_PLUGIN") or "auto").strip().lower()
    except Exception:
        return "auto"


def _auth_plugin_for(mysql_version=None, node_type=None):
    """按节点版本选认证插件：MySQL 8.0.34+ 用 caching_sha2_password。

    消除节点 error log 里的
    ``[MY-013360] Plugin mysql_native_password reported: '...is deprecated...'``；
    MariaDB 与 MySQL 8.0.33- 仍用 mysql_native_password。
    """
    try:
        return default_auth_plugin(mysql_version, node_type, _auth_plugin_pref())
    except Exception:
        return "mysql_native_password"


def _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                     auth_plugin="mysql_native_password"):
    """渲染 init.sql（首次启动即创建与所属集群一致的 ha_monitor / replication / app 账号）。

    抽成模块级函数，供 generate_node（首次生成）与 sync_node_creds（密码变更后重生成）
    共用，保证「节点部署文件」中的凭证与集群当前凭证始终一致——这样即使重部署
    （清空 MySQL-data 后 init.sql 重新执行）也不会因旧密码导致无法连接。

    auth_plugin：CREATE/ALTER USER 的认证插件，由 _auth_plugin_for() 按节点版本给出。
    """
    return (
        # 启用 GTID 一致性（复制/切换必需）。gtid_mode 已在 compose 启动参数
        # --gtid-mode=ON 中静态开启（MySQL 启动即 ON），此处不再做运行时逐级过渡——
        # 否则节点已为 ON 时再执行 SET OFF_PERMISSIVE 会触发 1788（不可一步跳变）并报错，
        # 导致 init.sql 在该行中断、后续账号创建语句不执行（节点连不上、长时间离线）。
        # enforce_gtid_consistency 静态配置也已开启，这里仅做幂等确认（已 ON 时为 no-op）。
        "SET GLOBAL enforce_gtid_consistency=ON;\n"
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        # 注意：CREATE/ALTER/DROP USER 这三项**不能**写进静态 GRANT——MySQL 5.7 / MariaDB
        # 不认识这些动态权限名（GRANT 直接报 1064 语法错误），会导致 init 整段失败。
        # 8.0 上由 ensure_ha_creds 自愈时再容错补授（失败忽略，兼容低版本）。
        "GRANT CREATE USER, REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* "
        "TO '%s'@'%%';\n"
        # 关键：CREATE 后 ALTER 强制密码，即使账号此前已存在（旧密码漂移）也校正回集群当前值；
        # 否则重装时 CREATE IF NOT EXISTS 不覆盖、从库连主库报 Access denied。ALTER USER 在
        # 5.7.6+/MariaDB 10.2+ 均支持，版本安全。
        "ALTER USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        # 业务最小权限账号：供测试/应用通过 VIP 连接，仅授予常用读写与管理权限，
        # 不含 SUPER/REPLICATION 等运维权限，避免应用直接用 ha_monitor/replication。
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, "
        "REFERENCES, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        # 本地状态库：管理端在切换主库时把「当前主」写入各节点本机 ha_local.state，
        # keepalived 检查脚本只读本机判定 VIP 归属，完全不依赖节点回连管理库
        # （避免管理库不可达时 VIP 不漂移、被 nopreempt 锁死在首节点）。
        "CREATE DATABASE IF NOT EXISTS ha_local;\n"
        "CREATE TABLE IF NOT EXISTS ha_local.state (k VARCHAR(32) PRIMARY KEY, v VARCHAR(255));\n"
        "FLUSH PRIVILEGES;\n"
    ) % (
        _q(ha_user), _q(ha_pass), _q(ha_user),
        _q(ha_user), _q(ha_pass),
        _q(repl_user), _q(repl_pass), _q(repl_user),
        _q(repl_user), _q(repl_pass),
        _q(app_user), _q(app_pass), _q(app_user),
        _q(app_user), _q(app_pass),
    )
    return _apply_auth_plugin(content, auth_plugin)


def _apply_auth_plugin(sql, auth_plugin):
    """把模板里写死的 mysql_native_password 换成目标插件（默认即 native，直接返回）。"""
    if not auth_plugin or auth_plugin == "mysql_native_password":
        return sql
    return sql.replace("mysql_native_password", auth_plugin)


def render_fix_node_creds_sql(ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass, manager_ip,
                              manager_subnet, auth_plugin="mysql_native_password"):
    """生成「节点账号/权限修复脚本」，当节点 ha_monitor 权限被面板/重装回退（只剩 USAGE）、
    或 root@'<管理器IP>' 密码漂移导致管理器连不上时使用。注入真实凭证。"""
    content = (
        "-- 节点账号/权限修复脚本（由管理器生成，已注入真实凭证）\n"
        "-- ⚠ 无需手动执行：Install-ha-mariadb-Node.sh 在节点准备阶段【自动重放】本文件，\n"
        "--   用于校正 ha_monitor 权限被面板/重装回退、或 root@'<管理器IP>' 密码漂移。\n"
        "--   仅当自动重放失败（日志有提示）时，才需在本机手动跑：\n"
        "--   mysql -uroot -p'<节点真实root密码>' < fix-node-creds.sql\n"
        "SET SQL_LOG_BIN=0;\n"
        # 1) ha_monitor：同时校正 @'%' 与 @'<管理器IP>' 两种 host 的密码。
        #    关键：MySQL 会优先匹配「特定 host」账号，若节点上残留 ha_monitor@'<管理器IP>'
        #    且密码漂移，管理器从管理器 IP 连入会命中该特定 host 而认证失败；把两种 host
        #    都设为同一正确密码可消除歧义。CREATE/ALTER/DROP USER 三项不写进静态 GRANT
        #    （5.7/MariaDB 会报 1064），8.0 由 ensure_ha_creds 容错补授。
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT CREATE USER, REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* TO '%s'@'%%';\n"
        "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "ALTER USER '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT CREATE USER, REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* TO '%s'@'%s';\n"
        # 2) root@'<管理器IP>'：修正为管理器 nodes.root_pass，使管理器 root 连接可用
        "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "ALTER USER '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;\n"
        #    root@'127.0.0.1'：keepalived 本机 TCP 探活账号，同 MariaDB 版说明——
        #    节点 root 口令变更后必须同步刷这个账号，否则探活 1045、VIP 不上浮。
        "CREATE USER IF NOT EXISTS '%s'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "ALTER USER '%s'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT ALL PRIVILEGES ON *.* TO '%s'@'127.0.0.1' WITH GRANT OPTION;\n"
        # 3) replication：复制通道账号（从库从其自身 IP 连，必须 @'%'）。
        #    ⚠ 关键：CREATE 之后必须再 ALTER 强制密码——否则节点曾用旧 repl_pass 安装、
        #    重装/跑修复脚本时 CREATE IF NOT EXISTS 不覆盖已存在账号 → 密码仍是旧的 →
        #    从库连主库报 Access denied。ALTER 兜底可彻底校正回集群当前 repl_pass。
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';\n"
        "FLUSH PRIVILEGES;\n"
        "SET SQL_LOG_BIN=1;\n"
    ) % (
        _q(ha_user), _q(ha_pass), _q(ha_user), _q(ha_pass), _q(ha_user),
        _q(ha_user), _q(manager_ip), _q(ha_pass), _q(ha_user), _q(manager_ip), _q(ha_pass), _q(ha_user), _q(manager_ip),
        _q(root_user), _q(manager_ip), _q(root_pass),
        _q(root_user), _q(manager_ip), _q(root_pass),
        _q(root_user), _q(manager_ip),
        _q(root_user), _q(root_pass), _q(root_user), _q(root_pass), _q(root_user),
        _q(repl_user), _q(repl_pass), _q(repl_user), _q(repl_user), _q(repl_pass),
    )
    if manager_subnet:
        # 同机（管理器容器与本节点同宿主机）场景：连接源是 docker 网关/容器 IP，
        # 落在管理器所在 docker 子网，需额外放行 root@'<子网通配符>' 覆盖 c/s 同机。
        content += (
            "-- 同机场景额外放行：root@'<docker 子网>'（覆盖管理器容器连接源）\n"
            "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '%s';\n"
            "ALTER USER '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '%s';\n"
            "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;\n"
        ) % (_q(root_user), _q(manager_subnet), _q(root_pass),
             _q(root_user), _q(manager_subnet), _q(root_pass),
             _q(root_user), _q(manager_subnet))
    return _apply_auth_plugin(content, auth_plugin)


def render_fix_node_creds_sql_mariadb(ha_user, ha_pass, repl_user, repl_pass,
                                      root_user, root_pass, manager_ip, port,
                                      manager_subnet=None):
    """MariaDB 的节点账号/权限修复脚本（语法与 MySQL 不同）。

    差异：
      - MariaDB 10.4+ 默认认证插件是 ed25519，显式用 IDENTIFIED VIA mysql_native_password
        并 USING PASSWORD('<哈希>') 注入——但此处我们直接给明文密码（INSTALL 脚本本机跑，
        非经主从复制，无明文穿越复制链路风险）。等价写法是 `IDENTIFIED BY`，保持与 init 一致。
      - MariaDB 无 REPLICATION_SLAVE_ADMIN / SYSTEM_VARIABLES_ADMIN（或用旧名），用通用权限集。
      - gtid_domain_id 不在本脚本处理（已由 my.cnf 固定）。
    """
    # 注意：这里必须赋值给 content 而非直接 return —— 否则下方「同机子网放行」
    # 这段代码永远不会执行（此前正是因此成了死代码）。
    content = (
        "-- MariaDB 节点账号/权限修复脚本（由管理器生成，已注入真实凭证）\n"
        "-- ⚠ 无需手动执行：Install-ha-mariadb-Node.sh 在节点准备阶段【自动重放】本文件。\n"
        "--   仅当自动重放失败（日志有提示）时，才需在本机手动跑：\n"
        "--   mysql -uroot -p'<节点真实root密码>' < fix-node-creds.sql\n"
        "SET SQL_LOG_BIN=0;\n"
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # BINLOG ADMIN：见 render_mariadb_init_sql 的说明，缺它 ha_local.state 写不进去。
        "GRANT CREATE USER, CREATE, INSERT, UPDATE, DELETE, REPLICATION SLAVE, "
        "REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, SLAVE MONITOR, READ_ONLY ADMIN, BINLOG ADMIN, "
        "REPLICATION CLIENT, PROCESS, "
        "SELECT, RELOAD, SUPER ON *.* TO '%s'@'%%';\n"
        "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "ALTER USER '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT CREATE USER, CREATE, INSERT, UPDATE, DELETE, REPLICATION SLAVE, "
        "REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, SLAVE MONITOR, READ_ONLY ADMIN, BINLOG ADMIN, "
        "REPLICATION CLIENT, PROCESS, "
        "SELECT, RELOAD, SUPER ON *.* TO '%s'@'%s';\n"
        "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "ALTER USER '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;\n"
        # root@'127.0.0.1'：keepalived 的 check_mysql_master.sh 是容器内的本机 TCP 探活
        # （LOCAL_HOST=127.0.0.1），命中这个账号。安装脚本按当时密码建过它，之后节点
        # root 口令一旦变更（改密/重新生成），该账号就会口令漂移 → 探活 1045 → VIP 不上浮。
        # 故每次重放修复脚本都必须把它刷成节点当前 root 口令。
        "CREATE USER IF NOT EXISTS '%s'@'127.0.0.1' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "ALTER USER '%s'@'127.0.0.1' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT ALL PRIVILEGES ON *.* TO '%s'@'127.0.0.1' WITH GRANT OPTION;\n"
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT REPLICATION SLAVE, REPLICATION SLAVE ADMIN ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "CREATE DATABASE IF NOT EXISTS ha_local;\n"
        "CREATE TABLE IF NOT EXISTS ha_local.state (k VARCHAR(32) PRIMARY KEY, v VARCHAR(255));\n"
        "FLUSH PRIVILEGES;\n"
        "SET SQL_LOG_BIN=1;\n"
    ) % (
        _q(ha_user), _q(ha_pass), _q(ha_user), _q(ha_pass), _q(ha_user),
        _q(ha_user), _q(manager_ip), _q(ha_pass), _q(ha_user), _q(manager_ip), _q(ha_pass), _q(ha_user), _q(manager_ip),
        _q(root_user), _q(manager_ip), _q(root_pass),
        _q(root_user), _q(manager_ip), _q(root_pass),
        _q(root_user), _q(manager_ip),
        _q(root_user), _q(root_pass), _q(root_user), _q(root_pass), _q(root_user),
        _q(repl_user), _q(repl_pass), _q(repl_user), _q(repl_user), _q(repl_pass),
    )
    if manager_subnet:
        # 同机（管理器容器与本节点同宿主机）场景：连接源是 docker 网关/容器 IP，
        # 落在管理器所在 docker 子网，需额外放行 root@'<子网通配符>' 覆盖 c/s 同机。
        content += (
            "-- 同机场景额外放行：root@'<docker 子网>'（覆盖管理器容器连接源）\n"
            "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
            "ALTER USER '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
            "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION;\n"
        ) % (_q(root_user), _q(manager_subnet), _q(root_pass),
             _q(root_user), _q(manager_subnet), _q(root_pass),
             _q(root_user), _q(manager_subnet))
    return content


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
            now = time.time()
            last = _vip_warn_ts.get(ip, 0)
            if now - last > 300:
                logger.warning("跳过刷新 %s 的 keepalived.conf：%s", ip, e)
                _vip_warn_ts[ip] = now
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
        # 刷新 keepalived 运行期脚本。
        # v2.2.12 关键：改为【总是覆盖】而非「仅在缺失时补齐」。
        # 原因：把节点置为可写的动作在 notify_master.sh 里，而旧版把错误全部静默吞掉
        # （`>/dev/null 2>&1 || true`），导致 VIP 停留在只读节点、业务写入报 1290。
        # 若只在脚本缺失时才补，升级后存量节点仍跑旧脚本 —— 修复不生效、故障照旧复现
        # （这正是「修改管理器代码后再重装，1290 依旧」的原因：真凶在节点本地脚本）。
        for nf in ("notify_master.sh", "notify_backup.sh",
                   "check_mysql_master.sh", "entrypoint.sh", "Dockerfile"):
            src = NODE_ASSETS / nf
            if src.exists():
                _write(kd / nf, src.read_text(),
                       mode=0o755 if nf.endswith(".sh") else 0o644)
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
        if is_mariadb_node(node_type):
            # 两种 MariaDB 形态的 compose 都只含 keepalived 客户端
            content = render_baremetal_compose(node_name, vip_port)
        else:
            content = _render_compose(
                node_name, r.get("root_pass") or "", int(r["port"]),
                int(r.get("server_id") or 0), vip_port)
            _write(kd.parent / "ha-overrides.cnf", render_docker_my_cnf(r.get("mysql_version"), int(r.get("port") or 3306)), mode=0o644)
        _write(p, content)
        # sync node agent script (OS metrics sidecar deployed with node compose)
        agent_src = Path(__file__).resolve().parent.parent / "scripts" / "node_agent.py"
        if agent_src.exists():
            _write(kd.parent / "node_agent.py", agent_src.read_text(), mode=0o644)


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
        raise ValueError("集群 VIP 格式非法（%r），应为合法 IPv4，如 192.0.2.10" % vip)
    vip_prefix = creds["vip_prefix"] or VIP_PREFIX
    interface = creds["interface"] or INTERFACE
    vrid = creds["vrid"] or VRID
    vip_port = creds.get("vip_port") or VIP_PORT
    manager_public_ip = get_manager_public_ip()
    manager_subnet = get_manager_docker_subnet_wildcard()
    server_id = get_next_server_id()
    node_name = f"mysql-ha-node-{ip.replace('.', '-')}"
    peers = _peer_ips(ip, cluster_id)
    # MariaDB 两种形态（裸机 / 容器 host 网络）的 keepalived 容器都在 host 网络，
    # 连本机 MariaDB 用 127.0.0.1；MySQL Docker 模式连节点 IP（桥接网络靠端口发布）。
    local_host = "127.0.0.1" if is_mariadb_node(node_type) else ip

    d = _safe_node_path(ip)
    d.mkdir(parents=True, exist_ok=True)
    _clean_generated_dir(d)
    kd = d / "keepalived"
    kd.mkdir(parents=True, exist_ok=True)

    # 1) init.sql / compose（按节点类型分支）
    init_sql = _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                                auth_plugin=_auth_plugin_for(mysql_version, node_type))
    extra_files = {}
    if is_mariadb_node(node_type):
        # MariaDB 两种形态（裸机 / 容器）共用：init_mariadb.sql + fix-node-creds.sql。
        # 差异仅在「配置文件」与「部署脚本」，故此处先写公共部分，再按形态分支。
        _write(d / "init_mariadb.sql", render_mariadb_init_sql(
            ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass), mode=0o644)
        # 账号/权限修复脚本（MariaDB 语法，native 密码）
        fix_sql = render_fix_node_creds_sql_mariadb(
            ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass, manager_public_ip, port,
            manager_subnet)
        _write(d / "fix-node-creds.sql", fix_sql, mode=0o644)
        extra_files["init_mariadb_sql"] = f"{NODES_HOST_BASE}/{ip}/init_mariadb.sql"
        # gtid_domain_id 显式指定为与 server_id 一致的非 0 值：MariaDB 一主多从/切换场景下，
        # 每个节点用各自独立域，避免切换后双主写入产生 GTID 冲突（预检不再报 domain_id=0）。
        if node_type == "mariadb-docker":
            # 容器化 MariaDB：纯 compose 部署（render_mariadb_compose 已含 mariadb + keepalived），
            # 故只交付配置片段（不含 datadir/socket）+ 卸载脚本，不再生成安装脚本
            # （部署入口即 docker compose up -d）；安装脚本仅作「裸机装不上 tarball」回退在裸机分支交付。
            _write(d / "docker-my.cnf",
                   render_mariadb_docker_my_cnf(server_id, port, gtid_domain_id=server_id),
                   mode=0o644)
            extra_files["docker_my_cnf"] = f"{NODES_HOST_BASE}/{ip}/docker-my.cnf"
            extra_files["uninstall_script"] = f"{NODES_HOST_BASE}/{ip}/Uninstall-mariadb-docker-Node.sh"
            src = NODE_SCRIPTS / "Uninstall-mariadb-docker-Node.sh"
            if src.exists():
                _write(d / "Uninstall-mariadb-docker-Node.sh", src.read_text(), mode=0o755)
        else:
            # 裸机 MariaDB：完全独立安装（脱离宝塔），由 Install 脚本全量安装 tarball。
            # 完整 my.cnf（Install 脚本落盘到 ${MARIADB_INSTALL_DIR}/my.cnf 或 /etc/my.cnf）
            my_cnf = render_mariadb_my_cnf(server_id, port, gtid_domain_id=server_id)
            _write(d / "my.cnf", my_cnf, mode=0o644)
            extra_files["my_cnf"] = f"{NODES_HOST_BASE}/{ip}/my.cnf"
            extra_files["install_script"] = f"{NODES_HOST_BASE}/{ip}/Install-ha-mariadb-Node.sh"
            extra_files["uninstall_script"] = f"{NODES_HOST_BASE}/{ip}/Uninstall-mariadb-Node.sh"
            # 裸机包【只适配裸机】：不再交付 Install-mariadb-docker-Node.sh 容器回退脚本。
            # 原因：该脚本把裸机完整 my.cnf（仅剔除 datadir/socket/pid_file）喂给官方镜像容器，
            # 残留裸机专属参数（缺失 log_error → 官方镜像默认写 stdout，管理器「错误日志」
            # 面板读不到恒为空；buffer pool 1G 等），且部署机制分裂为
            # 「docker run --network host + keepalived-only compose」，与 mariadb-docker 的
            # 纯 compose 形成两条并存路径，必然再次漂移。
            # 需要容器化（如 glibc 过低装不上 tarball）时，创建节点直接选 mariadb-docker 即可：
            # node_family 允许两种 MariaDB 形态同集群混部，且数据目录同为
            # /data/custos-ha/mariadb12，既有回退安装可无损迁移到纯 compose。
            for sf in ("Install-ha-mariadb-Node.sh", "Uninstall-mariadb-Node.sh"):
                src = NODE_SCRIPTS / sf
                if src.exists():
                    _write(d / sf, src.read_text(), mode=0o755)
    else:
        # init.sql 会被 MySQL 容器以非 root 的 mysql 用户读取执行，
        # 若权限过严（0600）会因 Permission denied 失败；改为 0644 保证可读。
        _write(d / "init.sql", init_sql, mode=0o644)
        # 慢日志覆盖配置（挂载到容器 /etc/mysql/conf.d）
        _write(d / "ha-overrides.cnf", render_docker_my_cnf(mysql_version, port), mode=0o644)

    # 2) keepalived.conf
    keep_conf = _render_keepalived_conf(
        ip, peers, vip, vip_prefix, interface, vrid, f"ha_mysql_{ip.replace('.', '_')}"
    )
    _write(kd / "keepalived.conf", keep_conf)

    # 3) ha_check.conf（仅含节点本地探活凭证 + 管理器服务端后台 IP；限制权限）
    #    - 管理端后台 MySQL（MGMT_MYSQL_HOST，如 192.0.2.88）仅管理器自身存储使用，
    #      绝不作为集群节点，也不写入此处（节点脚本不会、也不应回连管理库）。
    #    - MANAGER_IP 是「运行管理器的服务端后台」的 IP，节点只允许该 IP 连入；
    #      它与管理库 IP 是两回事，请勿混淆。
    #    - LOCAL_ROOT_USER/LOCAL_ROOT_PASS 用于 keepalived 本地探活（root 密码在容器创建时
    #      固定，不随 HA_PASS 漂移，避免 ha_monitor 密码脱节导致 VIP 不漂移）。
    ha_check = (
        f"LOCAL_HOST={_shq(local_host)}\n"
        f"LOCAL_PORT={port}\n"
        # VIP_PORT：业务连接「VIP:VIP_PORT」的端口。MariaDB 节点形态下 keepalived 只做
        # 三层 IP 漂移、不做端口转换，若业务端口与本机 MySQL 端口不同（如 VIP 3307 /
        # 本机 3306），VIP 上根本无人监听 —— 表现为「VIP 能 ping 通但连不上库」。
        # 安装脚本据此在本机加一条端口转发（firewalld forward-port 或 iptables REDIRECT）。
        f"VIP_PORT={vip_port}\n"
        f"NODE_IP={_shq(ip)}\n"
        f"MANAGER_IP={_shq(manager_public_ip)}\n"
        f"LOCAL_USER={_shq(ha_user)}\n"
        f"LOCAL_PASS={_shq(ha_pass)}\n"
        f"LOCAL_ROOT_USER={_shq(root_user)}\n"
        f"LOCAL_ROOT_PASS={_shq(root_pass)}\n"
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

    # 4.5) copy node agent script (OS metrics sidecar deployed with node compose).
    #      The rendered compose includes custos-node-agent service mounting ./node_agent.py.
    agent_src = Path(__file__).resolve().parent.parent / "scripts" / "node_agent.py"
    if agent_src.exists():
        _write(d / "node_agent.py", agent_src.read_text(), mode=0o644)

    # 5) docker-compose.yaml
    #    MariaDB 裸机（mariadb）：compose 只跑 keepalived 客户端（mysqld 由宿主 systemd 起）；
    #    MariaDB 容器化（mariadb-docker）：纯 compose（mariadb + keepalived 合一）；
    #    仅 MySQL Docker 才在 compose 内单独含 MySQL 容器。
    if node_type == "mariadb":
        compose = render_baremetal_compose(node_name, vip_port)
    elif node_type == "mariadb-docker":
        compose = render_mariadb_compose(node_name, root_pass, port, server_id, vip_port)
    else:
        compose = _render_compose(node_name, root_pass, port, server_id, vip_port)
    _write(d / "docker-compose.yaml", compose)

    # 5.5) 统一日志目录：logrotate 配置 + 目录初始化脚本（三形态一致）
    #      排错时按固定路径 /data/custos-ha/logs/<基名>/ 找日志，不再逐个形态猜。
    _data_dir_for_log = {
        "mariadb": "/data/mariadb",              # 裸机默认（与 render_mariadb_my_cnf 默认值一致）
        "mariadb-docker": MARIADB_DOCKER_DATA_DIR,
    }.get(node_type, MYSQL_DOCKER_DATA_DIR)
    _write(d / "custos-ha-logrotate.conf", render_logrotate_conf(), mode=0o644)
    _write(d / "prepare-logs.sh",
           render_prepare_logs(node_type, _log_dir_host(_data_dir_for_log)), mode=0o755)
    _write(d / "update-node.sh", render_update_node(), mode=0o755)

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

    # MariaDB 裸机节点：宿主机已原生运行 MariaDB，只需拉起 keepalived + 节点 agent 容器
    # （compose 不含数据库服务，故不会另起一个 MySQL 容器抢占 3306；agent 即 custos-node-agent）。
    # 容器化 MariaDB 节点（mariadb-docker）：纯 compose 部署，入口即 docker compose up -d
    # （mariadb + keepalived 合一，无需安装脚本）。
    # MySQL docker 节点：MySQL 与 keepalived 都在容器里，整套一起拉起。
    if node_type == "mariadb":
        run_cmd = f"cd {host_compose} && docker compose up -d keepalived custos-node-agent"
    else:
        run_cmd = f"cd {host_compose} && docker compose up -d"

    return dict(
        dir=host_compose, node_name=node_name, server_id=server_id, peers=peers,
        node_type=node_type,
        image=(MARIADB_DOCKER_IMAGE if node_type == "mariadb-docker" else ""),
        run_cmd=run_cmd,
        **extra_files,
    )


def _node_is_read_only(ip, port, root_user, root_pass, ha_user, ha_pass):
    """检测节点当前是否为只读副本（read_only / super_read_only = ON）。

    只读副本的 ha_monitor / replication / app / root 账号密码由主库经复制自动同步，
    **不应**在本节点直接执行 CREATE/ALTER USER。否则会与复制线程争用 mysql.* 系统表
    的元数据锁，导致复制线程死锁 / 长时间等待
    （'Waiting for dependent transaction to commit'），复制卡死、Seconds_Behind_Source
    持续增大、磁盘 I/O 居高不下。返回 True 表示应跳过直接账号同步。
    """
    last_err = None
    for (u, p) in ((root_user, root_pass), (ha_user, ha_pass)):
        try:
            c = pymysql.connect(host=ip, port=int(port), user=u, password=p,
                                connect_timeout=4, cursorclass=pymysql.cursors.DictCursor)
        except Exception as e:  # 账号不可达，尝试下一个
            last_err = e
            continue
        try:
            cur = c.cursor()
            cur.execute("SELECT @@read_only AS ro, @@super_read_only AS sro")
            r = cur.fetchone()
            return bool(r and (r.get("ro") or r.get("sro")))
        except Exception as e:
            last_err = e
        finally:
            try:
                c.close()
            except Exception:
                pass
    if last_err:
        logger.warning("只读副本检测失败 %s:%s: %s", ip, port, last_err)
    return False


def sync_node_creds(cluster_id=None, old_ha_user=None, old_repl_user=None, old_repl_pass=None, old_app_user=None):
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

    manager_public_ip = get_manager_public_ip()
    manager_subnet = get_manager_docker_subnet_wildcard()

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
        # 业务端口必须下发到节点：节点侧 setup_vip_port_forward() 从 ha_check.conf 读
        # VIP_PORT 来配置「VIP 端口 -> 本机 MySQL 端口」转发。此前本路径未写 VIP_PORT，
        # 导致重新生成节点目录后转发被跳过 -> 业务连 VIP:VIP_PORT 无监听
        #（表现为 VIP 能 ping 通，但业务账号连不上库）。
        vip_port = creds.get("vip_port") or VIP_PORT
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
            mysql_version = n.get("mysql_version") or ""

            # 只读副本：账号密码由主库经复制自动同步，严禁在副本上直接执行
            # CREATE/ALTER USER —— 否则会与复制线程争用 mysql.* 元数据锁，造成复制线程
            # 死锁 / 长时间等待（'Waiting for dependent transaction to commit'），
            # 复制卡死、Seconds_Behind_Source 持续增大、磁盘 I/O 居高不下。
            if _node_is_read_only(ip, port, root_user, root_pass, ha_user, ha_pass):
                logger.info("节点 %s:%s 为只读副本，跳过直接账号同步（凭证由复制同步）", ip, port)
                ok, msg = True, "replica-skip"
            else:
                # 优先用 ha_monitor（已知正确）自愈账号密码，并把 root@'<管理器IP>' 密码
                # 校正为库存值，打破「root 密码漂移 -> 连不上 -> 自愈不跑」死循环。
                ok, msg = ensure_ha_creds(
                    ip, port, root_user, root_pass,
                    ha_user, ha_pass, repl_user, repl_pass,
                    app_user, app_pass,
                    manager_ip=manager_public_ip,
                    manager_subnet=manager_subnet,
                    old_ha_user=old_ha_user,
                    old_repl_user=old_repl_user,
                    old_app_user=old_app_user,
                )
            if not ok:
                # root 与 ha_monitor 均不可达（节点离线），跳过（恢复后重新生成部署文件）
                continue

            # 重新生成该节点 ha_check.conf，保持与节点一致
            # MariaDB 两种形态的 keepalived 都在 host 网络，探活连 127.0.0.1；
            # MySQL docker 节点走桥接端口发布，连宿主机 IP。
            local_host = "127.0.0.1" if is_mariadb_node(node_type) else ip
            kd = _safe_node_path(ip) / "keepalived"
            kd.mkdir(parents=True, exist_ok=True)
            # 口令必须用 _shq() 做 shell 转义！旧实现裸写，口令含 空格/#/$/引号 时
            # 生成的 `LOCAL_ROOT_PASS=abc def#123` 被 source 后会被截断或整行变成注释，
            # 变量实际为空 -> notify 脚本以 MYSQL_PWD="" 连接，报
            #   ERROR 1045: Access denied for user 'root'@'localhost' (using password: NO)
            # 置可写失败 -> 节点保持只读 -> 业务经 VIP 写入报 1290。
            # 注意：LOCAL_PORT 是数字且要与 check 脚本做 `${MY_NODE}:${LOCAL_PORT}` 字符串比较，
            #       加引号会变成 '3306' 导致比较失败，故这里【不能】加引号。
            ha_check = (
                f"LOCAL_HOST={_shq(local_host)}\n"
                f"LOCAL_PORT={port}\n"
                f"NODE_IP={_shq(ip)}\n"
                f"MANAGER_IP={_shq(manager_public_ip)}\n"
                f"LOCAL_USER={_shq(ha_user)}\n"
                f"LOCAL_PASS={_shq(ha_pass)}\n"
                f"LOCAL_ROOT_USER={_shq(root_user)}\n"
                f"LOCAL_ROOT_PASS={_shq(root_pass)}\n"
                # VIP_PORT 为纯数字端口，不加引号（与 LOCAL_PORT 一致，供端口转发使用）
                f"VIP_PORT={vip_port}\n"
            )
            _write(kd / "ha_check.conf", ha_check)
            # 同步重生成 init 凭证 SQL（仅凭证相关），保证后续「重部署」后执行时使用的仍是当前密码。
            # MariaDB 节点（裸机/容器）写 init_mariadb.sql（MariaDB 语法）；docker 节点写 init.sql。
            if is_mariadb_node(node_type):
                init_sql = render_mariadb_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass)
                _write(_safe_node_path(ip) / "init_mariadb.sql", init_sql, mode=0o644)
            else:
                init_sql = _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                                            auth_plugin=_auth_plugin_for(mysql_version, node_type))
                _write(_safe_node_path(ip) / "init.sql", init_sql, mode=0o644)
            # 同步修复脚本（凭证变更/权限回退时，可在节点本机重放）
            # MariaDB 用 MariaDB 语法修复脚本（native 密码，无 MySQL8 专有特权）；其余用通用版。
            if is_mariadb_node(node_type):
                fix_sql = render_fix_node_creds_sql_mariadb(
                    ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass, manager_public_ip, port,
                    manager_subnet)
            else:
                fix_sql = render_fix_node_creds_sql(
                    ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass, manager_public_ip,
                    manager_subnet, auth_plugin=_auth_plugin_for(mysql_version, node_type))
            _write(_safe_node_path(ip) / "fix-node-creds.sql", fix_sql, mode=0o644)
            synced += 1

    # 复制账号改名/改密后，从库仍用旧 repl_user 连主库；主库 DROP 旧账号前必须先把
    # 从库重指向新 repl_user/repl_pass，否则主库删除旧账号后复制断（Access Denied）。
    if (old_repl_user and old_repl_user != repl_user) or \
       (old_repl_pass is not None and old_repl_pass != repl_pass):
        try:
            _cm = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cluster_id,))
        except Exception:
            _cm = []
        if _cm:
            _mh, _mp = _cm[0]["host"], int(_cm[0]["port"])
            _ns = query("SELECT ip, port, root_user, root_pass FROM nodes WHERE cluster_id=%s", (cluster_id,))
            for _n in _ns:
                if "%s:%s" % (_n["ip"], int(_n["port"])) == "%s:%s" % (_mh, _mp):
                    continue  # 主库自身不重指向
                try:
                    from .ha_client import make_slave_of
                    make_slave_of(
                        _n["ip"], int(_n["port"]),
                        _n.get("root_user") or "root", _n.get("root_pass") or "",
                        _mh, _mp, repl_user, repl_pass,
                        _n.get("root_user") or "root", _n.get("root_pass") or "")
                except Exception as _e:
                    logger.warning("副本 %s:%s 重指向新复制账号失败: %s", _n["ip"], _n["port"], _e)
        else:
            logger.warning("集群 %s 无 current_master，跳过从库重指向", cluster_id)

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
