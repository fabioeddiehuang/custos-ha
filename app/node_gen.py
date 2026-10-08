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
import hashlib
import json
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import pymysql

from .db import get_setting, query, execute
from .mysql_compat import default_auth_plugin, is_mariadb_node
from .config import (APP_VERSION, NODES_DIR, NODES_HOST_BASE, NODE_INSTALL_BASE, NODE_DIR_NAME,
                     node_install_dest, get_manager_public_ip,
                     get_manager_docker_subnet_wildcard, VIP, VIP_PREFIX, INTERFACE, VRID,
                     VIP_PORT, is_semi_sync_enabled)
from . import mariadb_matrix
from . import crypto as _crypto
from .ha_client import get_status, ensure_ha_creds, db_uptime, repair_ha_shadow_grants
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

# 节点包内数据库配置/脚本归整目录名（节点部署目录归整改造：cnf/sql 进 <db>/conf，脚本进 <db>/scripts）
MYSQL_NODE_CONF_DIR = "mysql8.0"
# 旧常量：仅作兼容回退（bootstrap 等模块仍引用）；新逻辑一律走 _node_conf_dir(node_type, version)
# 按版本动态取名（mariadb12.3.3 / mariadb11.4.13 / mariadb10.6.28 ...）。
MARIADB_NODE_CONF_DIR = "mariadb12.3.3"
AGENT_DIR = "agent"

# 主线版本：节点与集群都没登记版本时的唯一回退值。
MARIADB_MAINLINE_VERSION = "12.3.3"


def mariadb_version_tag(mysql_version=None):
    """把各形态的 MariaDB 版本串归一化成「主.次.补丁」（用于镜像 tag 与节点包目录名）。

    常见输入：'11.4.13-MariaDB-ubu2404-log'（容器）、'10.6.28-MariaDB-log'（裸机）、
    '12.3.3'。取不到、非法、或主版本 < 10（如 MySQL 风格的 '8.0'/'5.7'）时回退主线
    MARIADB_MAINLINE_VERSION。

    ⚠ 关键修复（2026-10-08）：本函数仅用于 MariaDB 节点，MariaDB 在 Docker Hub 的 tag
    自 10.x 起，不存在 mariadb:8.0 这类镜像。此前若节点版本被误填为 MySQL 风味的 '8.0'，
    会原样拼出 mariadb:8.0 → 镜像拉取失败 → 节点容器 restart:always 崩溃循环。
    故主版本 < 10 一律视为非法 MariaDB 版本并回退主线（项目支持矩阵为 10.6/11.4/12.3）。
    """
    v = (mysql_version or "").strip()
    if not v:
        return MARIADB_MAINLINE_VERSION
    head = v.split("-")[0]
    parts = [p for p in head.split(".") if p.isdigit()]
    if len(parts) >= 2:
        # 主版本 < 10（MySQL 8.0/5.7 等）对 MariaDB 无效：回退主线而非拼出不存在的镜像。
        try:
            if int(parts[0]) < 10:
                return MARIADB_MAINLINE_VERSION
        except ValueError:
            return MARIADB_MAINLINE_VERSION
        if len(parts) >= 3:
            return ".".join(parts[:3])
        return ".".join(parts[:2])
    return MARIADB_MAINLINE_VERSION


def mariadb_image_for_version(mysql_version=None):
    """容器化 MariaDB 节点镜像：**跟随版本**（如 mariadb:11.4.13）。

    旧逻辑写死 mariadb:12.3.3 —— 接管 10.6/11.4 老库后新增从库会被强塞 12.3.3，
    而跨大版本复制不是受支持组合、页面也无从选择。现在按「节点显式版本 → 集群登记
    版本 → 主线」的顺序取镜像。
    """
    return "mariadb:%s" % mariadb_version_tag(mysql_version)


def _node_conf_dir(node_type, mysql_version=None):
    """节点包内数据库配置目录名（mysql8.0 / mariadb<版本>，如 mariadb11.4.13）。"""
    if is_mariadb_node(node_type):
        return "mariadb%s" % mariadb_version_tag(mysql_version)
    return MYSQL_NODE_CONF_DIR

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
def agent_env_yaml(ip, token, manager_url, unit_dir, heartbeat=30):
    """custos-node-agent 的心跳环境变量块（v2.5.0 F5）。

    心跳方向是 agent → manager，所以节点必须知道：
      - 自己是谁（NODE_IP）
      - 管理器在哪（MANAGER_URL）
      - 凭什么认证（NODE_AGENT_TOKEN，每节点独立）
    缺任何一个都不启用心跳（agent 退化为纯被动，与旧版一致）。
    """
    if not (ip and token and manager_url):
        return ""
    return (
        "    environment:\n"
        f"      NODE_IP: '{ip}'\n"
        f"      NODE_AGENT_TOKEN: '{token}'\n"
        f"      NODE_AGENT_HEARTBEAT: '{int(heartbeat)}'\n"
        f"      MANAGER_URL: '{manager_url}'\n"
        f"      NODE_UNIT_DIR: '{unit_dir}'\n"
    )


def render_agent_service(log_dir_host):
    """渲染 custos-node-agent sidecar service（日志落盘到统一日志目录，便于 <ip>/logs 软链接入）。"""
    return f"""
  custos-node-agent:
    image: python:3-slim
    container_name: custos-node-agent
    # 保持 exec 列表（流式数组）形式，语义与原多段 block 列表完全一致。
    command: ["python3", "/app/node_agent.py", "--port", "28889", "--bind", "0.0.0.0",
              "--root", "/host", "--log", "/var/log/custos-ha/node_agent.log"]
    restart: always
    pid: host
    network_mode: host
    volumes:
      # 注意：宿主根挂到 /host 必须是【读写】。F4/F5 需要把配置文件（如 ha_check.conf）
      # 真实写入节点单元目录、并被对应容器挂载生效；若只读则 agent 容器无 docker CLI、
      # 且无法落盘。agent 已持有 docker.sock（宿主上帝权限），只读挂载对威胁模型无实质
      # 缓解（已可用 docker run -v / 拿到宿主写），故此处放宽为读写以支撑功能。
      - /:/host
      - ./agent/node_agent.py:/app/node_agent.py:ro
      - {log_dir_host}:/var/log/custos-ha:z
      # Plan B：迁移物理通道经 agent 在宿主跑 docker/mariabackup，需宿主 docker.sock。
      # 同时挂到 /host/var/run/docker.sock 供 chroot /host 后的脚本解析。
      - /var/run/docker.sock:/var/run/docker.sock
      - /var/run/docker.sock:/host/var/run/docker.sock
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


def server_id_conflict(sid, exclude_ip=None, exclude_port=None):
    """检查 server_id 是否已被**其他**节点占用；被占用返回占用者 "ip:port"，空闲返回 None。

    用于自定义 server_id 的撞 id 防护：server_id 在复制拓扑里必须全局唯一，
    两个节点撞 id 会导致 GTID 混乱（从库认错事务来源、复制反复中断）。
    exclude_ip/port 用于排除「节点自身」，使同一节点重新生成时不算冲突。
    """
    try:
        sid = int(sid)
    except (TypeError, ValueError):
        return None
    for tbl, hcol in (("nodes", "ip"), ("hosts", "host")):
        try:
            rows = query("SELECT %s AS h, port FROM %s WHERE server_id=%%s" % (hcol, tbl), (sid,))
        except Exception:
            continue
        for r in rows or []:
            if exclude_ip is not None and str(r.get("h")) == str(exclude_ip) \
                    and int(r.get("port") or 0) == int(exclude_port or 0):
                continue
            return "%s:%s" % (r.get("h"), r.get("port"))
    return None


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
    log_file /var/log/custos-ha/keepalived.log
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
    # 启动参数：保持 exec 列表（流式数组）形式——绝不能改成字符串，否则 MySQL 官方镜像
    # 会在参数前再拼一个 mysqld 导致双命令报错，且多一层 sh -c 会让 SIGTERM 优雅关闭失效。
    # 写成单行流式数组仅作可读性优化，与原多段 block 列表语义完全一致。
    command: ["--server-id={server_id}", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
              "--log-bin=mysql-bin", "--log-slave-updates=ON", "--relay-log=relay-bin",
              "--relay-log-recovery=ON", "--sync-binlog=1", "--innodb-flush-log-at-trx-commit=1",
              "--read-only=ON", "--innodb-buffer-pool-size=320M", "--innodb-io-capacity=2000",
              "--innodb-io-capacity-max=4000", "--slave-parallel-workers=4",
              "--slave-parallel-type=LOGICAL_CLOCK", "--slave-preserve-commit-order=ON"]
    ports:
{port_lines}
    volumes:
      # MySQL 数据目录：统一 /data/custos-ha/mysql8（与 MariaDB 节点的 mariadb12 对齐）。
      # :z 供 Anolis 等 SELinux=Enforcing 系统重打标签；未启用 SELinux 的 Ubuntu 会忽略该标记，
      # 故同一份 compose 同时兼容两类系统。
      - {MYSQL_DOCKER_DATA_DIR}:/var/lib/mysql:z
      - ./mysql8.0/conf/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      # 慢日志覆盖配置（slow_query_log=1 + log_output=TABLE），供 slow_log 采集 mysql.slow_log。
      # 必须挂载，否则节点慢查询不入库，/api/slow_log 永远空（节点默认 slow_query_log=OFF）。
      - ./mysql8.0/conf/ha-overrides.cnf:/etc/mysql/conf.d/ha-overrides.cnf:ro
      # 统一日志目录：error.log 等全部落到这里（宿主 logs/<基名> -> 容器 /var/log/custos-ha）。
      # 旧值 /var/lib/mysql-files 未挂到宿主，容器一销毁日志就没了，是排错时最难查的一类问题。
      # 首次部署前需先在宿主建目录并 chown 给容器内 mysql(uid 999)：见 <db>/scripts/prepare-logs.sh
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
    entrypoint: ["sh", "-c", "chown -R 999:999 /var/lib/mysql /var/log/custos-ha && echo logs-dir-ready"]
    volumes:
      - {MYSQL_DOCKER_DATA_DIR}:/var/lib/mysql:z
      - {_log_dir_host(MYSQL_DOCKER_DATA_DIR)}:/var/log/custos-ha:z
    restart: "no"

  keepalived:
    # keepalived 镜像 custos-node:<VERSION> 由【服务端】本地构建并托管在 ./images/，
    # 节点经 curl 引导脚本从服务端下载 tar 后 `docker load` 载入（F2 离线镜像方案）。
    # 【严禁】保留 build: ./keepalived 兜底——当服务端 tar 未载入时，节点会退化到公网拉取
    # debian:bookworm-slim 基础镜像再编译，恰是「keepalived 从公网 pull」的来源。此处只声明
    # image，镜像必须已由服务端下发并 docker load 成功；否则 docker compose up 会明确报
    # "image not found"（故障清晰、不静默走公网）。镜像缺失时在服务端跑 build_node_images.sh。
    image: {KEEPALIVED_IMAGE_NAME}:{PKG_VERSION}
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
      - {_log_dir_host(MYSQL_DOCKER_DATA_DIR)}:/var/log/custos-ha:z
    """.replace("{KEEPALIVED_IMAGE_NAME}", KEEPALIVED_IMAGE_NAME) + render_agent_service(_log_dir_host(MYSQL_DOCKER_DATA_DIR))


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
        # v2.3.0：插件名由 mariadb_matrix 提供。MariaDB 全系列沿用 master/slave 命名，
        # 11.5+ 新增的 source/replica 只是**别名**、且旧名仍有效，故不做版本分支——
        # 切勿照 MySQL 8.0.26+ 改成 source/replica，会让 10.6~11.4 节点启动即崩。
        "plugin_load_add = " + mariadb_matrix.semisync_plugin_load() + "\n"
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


# ---------------------------------------------------------------------------
# 内存封顶（根治 MariaDB 容器/裸机内存膨胀）
# ---------------------------------------------------------------------------
# 为什么要显式写死这些值（而不是依赖官方镜像/发行版默认）：
#   1) 各构建的默认值不一致，且 key_buffer_size(默认 128M) / aria_pagecache_buffer_size(默认 128M)
#      这类「固定大缓存」对以 InnoDB 为主的业务纯属浪费；显式给小值即可省下约 200M/节点。
#   2) performance_schema 若被镜像/发行版默认打开，会随运行时间**持续增长**（可达数百 MB~GB），
#      是容器内存悄悄变大的头号嫌疑；本产品不依赖它（备份/一致性巡检均显式排除该库），显式关闭。
#   3) 会话级缓冲（sort/join/read/tmp_table…）是【每连接】的，连接数一多会成倍放大——
#      必须显式封顶，避免「连接数 × 缓冲」把内存顶穿。
# 注意：以下取值均为「不超过发行版默认」的保守上限，只封顶、不放大，故不会增加内存占用。
_MEMORY_CNF_BLOCK = (
    "# ---- 内存封顶（显式设置，避免随默认值/连接数/运行时长膨胀）----\n"
    "# performance_schema 纯 InnoDB 业务不需要，且开启后会随运行时间持续占用内存：显式关闭。\n"
    "performance_schema = OFF\n"
    "# MyISAM/Aria 固定大缓存瘦身（默认各 128M，对 InnoDB 业务基本无用）\n"
    "key_buffer_size = 8M\n"
    "aria_pagecache_buffer_size = 16M\n"
    "# InnoDB 其它缓冲\n"
    "innodb_log_buffer_size = 8M\n"
    "innodb_buffer_pool_instances = 1\n"
    "# 表缓存 / 线程缓存 / 连接数（够用即可）\n"
    "table_open_cache = 500\n"
    "table_definition_cache = 500\n"
    "thread_cache_size = 8\n"
    "max_connections = 100\n"
    "# 会话级缓冲（每连接，逐个封顶，防「连接数 × 缓冲」顶穿内存）\n"
    "sort_buffer_size = 512K\n"
    "join_buffer_size = 256K\n"
    "read_buffer_size = 128K\n"
    "read_rnd_buffer_size = 256K\n"
    "tmp_table_size = 8M\n"
    "max_heap_table_size = 8M\n"
    "binlog_cache_size = 32K\n"
    "binlog_stmt_cache_size = 32K\n"
    "net_buffer_length = 16K\n"
)


# ---------------------------------------------------------------------------
# IO / CPU 调优块：**降低读写延迟但不增加内存**（与上面的内存封顶互补）
# ---------------------------------------------------------------------------
# 为什么重点调刷盘策略而不是一味加缓冲池：默认 innodb_flush_log_at_trx_commit=1
# 且 sync_binlog=1 时，**每个事务提交都要 fsync 两次**（redo + binlog），
# 这是写延迟与 IOPS 的头号瓶颈；放松它带来的延迟下降，远大于同等内存的缓冲池增量。
_IO_TUNE_BLOCK_COMMON = (
    "# ---- IO 调优（降延迟，不增内存）----\n"
    "# 刷盘策略：默认 1 / 1 会让每次提交 fsync 两次。\n"
    "#   innodb_flush_log_at_trx_commit=2 → 提交只写 OS 页缓存、每秒 fsync 一次；\n"
    "#       进程崩溃【不丢数据】（OS 已持有），仅整机断电/内核崩溃最多丢约 1 秒。\n"
    "#   sync_binlog=100 → 每 100 个事务才 fsync 一次 binlog，写 IOPS 降一个数量级。\n"
    "#   本产品有半同步复制兜底：单机宕机切从库时数据不丢。\n"
    "# ⚠ 若要求「单机断电也零丢失」，把这两项改回 1 / 1（代价：写延迟显著升高）。\n"
    "innodb_flush_log_at_trx_commit = 2\n"
    "sync_binlog = 100\n"
    "# LRU 扫描深度：默认 1024~1536 会让 page cleaner 持续空转耗 CPU；缓冲池不大时\n"
    "# 512 足够，可明显降低后台 CPU 占用。\n"
    "innodb_lru_scan_depth = 512\n"
    "# SSD 上关闭「邻接页合并刷」：减少随机写场景的无效 IO\n"
    "innodb_flush_neighbors = 0\n"
)

# MySQL 8.0.30+ 用 innodb_redo_log_capacity 管重做日志（官方默认仅 100M），调大可显著
# 减少「检查点刷脏风暴」造成的写延迟抖动。innodb_flush_method：官方镜像默认 fsync，
# 会让数据在 OS 页缓存与 InnoDB 缓冲池【双份驻留】，O_DIRECT 绕过页缓存，
# 降延迟同时省内存。
_IO_TUNE_BLOCK_MYSQL = _IO_TUNE_BLOCK_COMMON + (
    "innodb_redo_log_capacity = 256M\n"
    "innodb_flush_method = O_DIRECT\n"
)

# MariaDB 用 innodb_log_file_size 表示重做日志大小（无 innodb_redo_log_capacity，
# 写进去会 unknown variable 导致启动失败）。O_DIRECT 同上，避免双份缓存。
_IO_TUNE_BLOCK_MARIADB = _IO_TUNE_BLOCK_COMMON + (
    "innodb_log_file_size = 256M\n"
    "innodb_flush_method = O_DIRECT\n"
)


def render_mariadb_my_cnf(server_id, port, gtid_domain_id=None, data_dir="/data/custos-ha/mariadb", install_dir="/opt/mariadb"):
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
        "binlog_format = ROW\n# binlog 清理：避免复制 binlog 无限累积撑爆 /data（历史版本未设导致磁盘告警）\nexpire_logs_days = 7\nmax_binlog_size = 256M\n"
        "read_only = ON\n"
        "# 复制/性能（与 docker 节点保持一致，避免单节点复制延迟累积）\n"
        "innodb_buffer_pool_size = 320M\n"
        "innodb_io_capacity = 2000\n"
        "innodb_io_capacity_max = 4000\n"
        "slave_parallel_threads = 4\n"
        "slave_parallel_mode = optimistic\n"
        + _MEMORY_CNF_BLOCK + _IO_TUNE_BLOCK_MARIADB +
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
        "binlog_format = ROW\n# binlog 清理：避免复制 binlog 无限累积撑爆 /data（历史版本未设导致磁盘告警）\nexpire_logs_days = 7\nmax_binlog_size = 256M\n"
        "read_only = ON\n"
        "# 复制/性能（与裸机 MariaDB 节点保持一致）\n"
        "# 缓冲池默认 512M（而非官方/裸机模板的 1G）：小内存主机（如 2G）用 1G 极易 OOM。\n"
        "# 内存充足可调大：compose 场景改 MARIADB_INNODB_BUFFER_POOL_SIZE 环境变量即可覆盖。\n"
        "innodb_buffer_pool_size = 320M\n"
        "innodb_io_capacity = 2000\n"
        "innodb_io_capacity_max = 4000\n"
        "slave_parallel_threads = 4\n"
        "slave_parallel_mode = optimistic\n"
        + _MEMORY_CNF_BLOCK + _IO_TUNE_BLOCK_MARIADB +
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
    # v2.3.0：特权集统一由 mariadb_matrix 提供，与 ha_client.ensure_ha_creds 同源，
    # 消除「node_gen 有 REPLICATION MASTER ADMIN、ha_client 没有」的两处不一致。
    _HA_PRIVS = mariadb_matrix.ha_privs()
    _REPL_PRIVS = mariadb_matrix.repl_privs()
    _APP_PRIVS = mariadb_matrix.app_privs()
    return (
        # MariaDB 默认已开启 GTID（gtid_domain_id 在 my.cnf 指定），无需 SET gtid_mode。
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # 注意：MariaDB 中 ALTER USER / DROP USER **不是**可授权权限——它们由 CREATE USER 权限
        # 隐含覆盖（CREATE USER 特权即允许对任意用户 CREATE/ALTER/DROP USER）。若把它们写进
        # GRANT 会直接报 1064 语法错误，导致 init 整段失败。故只授 CREATE USER。
        # BINLOG ADMIN 必须显式授予：MariaDB 10.5+ 把 `SET SESSION sql_log_bin` 从 SUPER 中拆出，
        # 缺它时管理器写 ha_local.state（需禁 binlog）会报 1227，主库标记一直写不进去、
        # keepalived 只能拿到空状态退化判定。
        "GRANT %s ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # replication 复制账号（从库从其自身 IP 连，必须 @'%%'）
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT %s ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # 业务最小权限账号
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT %s ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # 本地状态库：切换主库时写入当前主，keepalived 检查脚本据此持有/释放 VIP
        "CREATE DATABASE IF NOT EXISTS ha_local;\n"
        "CREATE TABLE IF NOT EXISTS ha_local.state (k VARCHAR(32) PRIMARY KEY, v VARCHAR(255));\n"
        "FLUSH PRIVILEGES;\n"
    ) % (
        _q(ha_user), _q(ha_pass), _HA_PRIVS, _q(ha_user),
        _q(ha_user), _q(ha_pass),
        _q(repl_user), _q(repl_pass), _REPL_PRIVS, _q(repl_user),
        _q(repl_user), _q(repl_pass),
        _q(app_user), _q(app_pass), _APP_PRIVS, _q(app_user),
        _q(app_user), _q(app_pass),
    )


def render_baremetal_compose(node_name, vip_port=None, data_dir=None):
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
        "      - {LOG_DIR_HOST_VOL}:/var/log/custos-ha:z\n"
    ).replace("{KEEPALIVED_IMAGE_NAME}", KEEPALIVED_IMAGE_NAME) \
     .replace("{PKG_VERSION}", PKG_VERSION) \
     .replace("{LOG_DIR_HOST_VOL}", _log_dir_host(data_dir or "/data/custos-ha/mariadb")) + render_agent_service(_log_dir_host(data_dir or "/data/custos-ha/mariadb"))


def render_mariadb_compose(node_name, root_pass, port, server_id, vip_port=None,
                           data_dir=None, image=None, buffer_pool="320M",
                           mysql_version=None, conf_dir=None):
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
        # 镜像跟随版本：集群/节点登记 11.4.13 就用 mariadb:11.4.13，不再一律 12.3.3。
        image = mariadb_image_for_version(mysql_version)
    if conf_dir is None:
        conf_dir = _node_conf_dir("mariadb-docker", mysql_version)
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
    # 入口前先修一次统一日志目录属主（宿主挂载目录默认 root:root，mariadbd 写不了 error.log），
    # 把原本独立的 init-logs 容器职责合并进本入口，避免 restart:no + depends_on 卡住节点。
    # 保持 exec 列表（流式数组）形式：exec $@ 最终落到 docker-entrypoint.sh mariadbd，语义不变。
    entrypoint: ["sh", "-c",
                "chown -R 999:999 /var/log/custos-ha 2>/dev/null || chmod -R 777 /var/log/custos-ha 2>/dev/null; exec $@",
                "wrap", "/usr/local/bin/docker-entrypoint.sh", "mariadbd"]
    environment:
      MARIADB_ROOT_PASSWORD: "{root_pass}"
      # 全站统一中国时间：节点 NOW()/日志时间均为北京时间(UTC+8)
      TZ: "Asia/Shanghai"
      # 低内存主机友好：官方默认与裸机模板为 1G，小内存机易 OOM。内存充足可调至 1G/2G。
      MARIADB_INNODB_BUFFER_POOL_SIZE: "{buffer_pool}"
      # glibc malloc 的 arena 数量上限：容器内多线程进程会为每个线程建独立 arena，
      # 释放后常驻不归还 OS，典型表现是「RSS 只涨不降、容器内存越来越大」（可达数百 MB）。
      # 限制为 2 可显著抑制该膨胀，是容器化 MySQL/MariaDB 内存治理的常规手段。
      MALLOC_ARENA_MAX: "2"
      # 跨小版本升级时自动执行 mariadb-upgrade，避免数据字典未升级导致起不来
      MARIADB_AUTO_UPGRADE: "1"
    ports:
{port_lines}    volumes:
      # 数据目录：统一 /data/custos-ha/mariadb12
      - {data_dir}:/var/lib/mysql:z
      # 配置双挂：Debian 构建读 /etc/mysql/conf.d，UBI 构建读 /etc/my.cnf.d，两条都挂
      # 可让任一镜像都生效；99- 前缀保证最后加载、覆盖镜像默认值。
      - ./{conf_dir}/conf/docker-my.cnf:/etc/my.cnf.d/99-ha.cnf:ro,z
      - ./{conf_dir}/conf/docker-my.cnf:/etc/mysql/conf.d/99-ha.cnf:ro,z
      # 初始化 SQL：仅在数据目录为空的首次启动执行；已初始化节点改由 fix-node-creds.sql
      # 或管理器 ensure_ha_creds 自愈校正账号（不会重复执行导致报错）。
      - ./{conf_dir}/conf/init_mariadb.sql:/docker-entrypoint-initdb.d/init_mariadb.sql:ro,z
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
      - {_log_dir_host(data_dir or "/data/custos-ha/mariadb")}:/var/log/custos-ha:z
""" + render_agent_service(_log_dir_host(data_dir or "/data/custos-ha/mariadb"))


# ---------------------------------------------------------------------------
# MySQL 8.0 专用内存封顶（**不可复用上面的 MariaDB 块**：MySQL 没有 aria_* 变量，
# 写进去会报 unknown variable 导致容器启动失败/崩溃循环）
# ---------------------------------------------------------------------------
# 为什么必须显式关 performance_schema：MySQL 8 官方镜像默认开启，且会按
# max_connections / table_open_cache 等参数**自动放大**，实测在 mysql:8.0.46 上
# 占到 1.4~1.5G（超过 InnoDB 缓冲池本身），是容器内存第一大户。
# 本产品不依赖它：慢日志走 mysql.slow_log、错误日志走 mysql.error_log 表。
_MEMORY_CNF_BLOCK_MYSQL = (
    "# ---- 内存封顶（MySQL 8.0 专用）----\n"
    "performance_schema = OFF\n"
    "innodb_buffer_pool_size = 320M\n"
    "innodb_buffer_pool_instances = 1\n"
    "innodb_log_buffer_size = 8M\n"
    "key_buffer_size = 8M\n"
    "table_open_cache = 500\n"
    "table_definition_cache = 500\n"
    "thread_cache_size = 8\n"
    "max_connections = 100\n"
    "sort_buffer_size = 512K\n"
    "join_buffer_size = 256K\n"
    "read_buffer_size = 128K\n"
    "read_rnd_buffer_size = 256K\n"
    "tmp_table_size = 8M\n"
    "max_heap_table_size = 8M\n"
    "binlog_cache_size = 32K\n"
    "binlog_stmt_cache_size = 32K\n"
    "net_buffer_length = 16K\n"
)


def render_docker_my_cnf(mysql_version=None, port=3306):
    """Docker 节点 MySQL 覆盖配置：开启慢日志（log_output=TABLE），供 slow_log 采集。

    挂载到容器 /etc/mysql/conf.d/ha-overrides.cnf。对 v2.0 已部署集群无副作用，
    仅在该节点重新生成并部署后生效。SEMI_SYNC=1 时额外注入半同步插件加载与开启参数，
    使半同步随容器启动即生效、重启不丢（无需运行时 INSTALL PLUGIN）。
    """
    cnf = (
        "[mysqld]\n"
        "bind-address = 0.0.0.0\n"
        + _MEMORY_CNF_BLOCK_MYSQL + _IO_TUNE_BLOCK_MYSQL +
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

# ---- 版本跟随后清理旧版本配置目录（P1，2026-10-02）----
# 接管 / 升级导致集群版本变化时，节点目录里会残留旧版本目录（如 mariadb12.3.3），
# 而新包只带当前版本目录（mariadb11.4.13 / mariadb10.6.28 ...）。旧目录不再被任何
# 服务引用，留着只会增加排错混淆、还可能被误当成「节点实际版本」。故：保留包内
# 当前那个配置目录，其余 mariadb*/mysql8.0 一律清掉。
_conf=""
for d in mariadb* mysql8.0; do
  [ -d "$d" ] || continue
  _conf="$d"
  break
done
if [ -n "$_conf" ]; then
  for d in mariadb* mysql8.0; do
    [ -d "$d" ] || continue
    [ "$d" = "$_conf" ] && continue
    echo "[update-node] 清理旧版本配置目录 $d"
    rm -rf "$d"
  done
fi

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
        "--   mysql -uroot -p'<节点真实root密码>' < mariadb12.3.3/conf/fix-node-creds.sql\n"
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
    # v2.3.0：特权集与 init SQL / ha_client.ensure_ha_creds 同源（mariadb_matrix）。
    _HA_PRIVS = mariadb_matrix.ha_privs()
    _REPL_PRIVS = mariadb_matrix.repl_privs()
    content = (
        "-- MariaDB 节点账号/权限修复脚本（由管理器生成，已注入真实凭证）\n"
        "-- ⚠ 无需手动执行：Install-ha-mariadb-Node.sh 在节点准备阶段【自动重放】本文件。\n"
        "--   仅当自动重放失败（日志有提示）时，才需在本机手动跑：\n"
        "--   mysql -uroot -p'<节点真实root密码>' < mariadb12.3.3/conf/fix-node-creds.sql\n"
        "SET SQL_LOG_BIN=0;\n"
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        # v2.3.0：特权集由 mariadb_matrix 提供（与 init SQL / ensure_ha_creds 同源）。
        "GRANT %s ON *.* TO '%s'@'%%';\n"
        "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "ALTER USER '%s'@'%s' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "GRANT %s ON *.* TO '%s'@'%s';\n"
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
        "GRANT %s ON *.* TO '%s'@'%%';\n"
        "ALTER USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s');\n"
        "CREATE DATABASE IF NOT EXISTS ha_local;\n"
        "CREATE TABLE IF NOT EXISTS ha_local.state (k VARCHAR(32) PRIMARY KEY, v VARCHAR(255));\n"
        "FLUSH PRIVILEGES;\n"
        "SET SQL_LOG_BIN=1;\n"
    ) % (
        _q(ha_user), _q(ha_pass), _q(ha_user), _q(ha_pass), _HA_PRIVS, _q(ha_user),
        _q(ha_user), _q(manager_ip), _q(ha_pass), _q(ha_user), _q(manager_ip), _q(ha_pass), _HA_PRIVS, _q(ha_user), _q(manager_ip),
        _q(root_user), _q(manager_ip), _q(root_pass),
        _q(root_user), _q(manager_ip), _q(root_pass),
        _q(root_user), _q(manager_ip),
        _q(root_user), _q(root_pass), _q(root_user), _q(root_pass), _q(root_user),
        _q(repl_user), _q(repl_pass), _REPL_PRIVS, _q(repl_user), _q(repl_user), _q(repl_pass),
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    os.chmod(path, mode)


def _refresh_all_keepalived_conf(cluster_id=None):
    """任一节点增删后，重算对应集群所有节点的 unicast_peer 并刷新 keepalived.conf。

    cluster_id 为 None 时刷新全部集群。
    """
    try:
        if cluster_id:
            rows = query("SELECT ip, port, server_id, cluster_id, interface FROM nodes WHERE cluster_id=%s",
                         (cluster_id,))
        else:
            # F8（2026-09-16 真机）：此前 SELECT **漏选 cluster_id**，而下方 `cid = r.get("cluster_id")
            # 恒为 None → `_peer_ips(ip, None)` 直接返回 [] → 重新生成的 keepalived.conf 里
            # `unicast_peer {}` 恒为空 → 节点间 VRRP 互不知情（脑裂 / VIP 不切换）。
            # 实测 33 的 generated_nodes 配置 peer 为空即此因。
            rows = query("SELECT ip, port, server_id, cluster_id, interface FROM nodes")
    except Exception:
        return
    for r in rows:
        ip = r["ip"]
        cid = r.get("cluster_id")
        peers = _peer_ips(ip, cid)
        creds = get_cluster_creds(cid) if cid else None
        vip = (creds or {}).get("vip") or get_setting("VIP") or VIP
        vip_prefix = (creds or {}).get("vip_prefix") or VIP_PREFIX
        # 节点级 interface 优先（v2.5.0），缺省回退集群级 / 全局
        interface = r.get("interface") or (creds or {}).get("interface") or INTERFACE
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
    vrid = (creds or {}).get("vrid") or VRID
    rows = query(
        "SELECT ip, port, root_pass, server_id, node_type, interface FROM nodes WHERE cluster_id=%s",
        (cluster_id,))
    for r in rows:
        ip = r["ip"]
        node_type = r.get("node_type") or "mariadb-docker"
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
        # 节点级 interface 优先（v2.5.0），缺省回退集群级 / 全局
        interface = r.get("interface") or (creds or {}).get("interface") or INTERFACE
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
            _write(kd.parent / MYSQL_NODE_CONF_DIR / "conf" / "ha-overrides.cnf", render_docker_my_cnf(r.get("mysql_version"), int(r.get("port") or 3306)), mode=0o644)
        _write(p, content)
        # sync node agent script (OS metrics sidecar deployed with node compose)
        agent_src = Path(__file__).resolve().parent.parent / "scripts" / "node_agent.py"
        if agent_src.exists():
            _write(kd.parent / AGENT_DIR / "node_agent.py", agent_src.read_text(), mode=0o644)


def _clean_generated_dir(d: Path):
    """重新生成节点前清理目录中旧的自生成文件；保留 MySQL-data 数据卷，绝不删除。"""
    if not d.exists():
        return
    for item in d.iterdir():
        if item.name in ("MySQL-data", "logs"):
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


# ---------------------------------------------------------------------------
# v2.3.0：变更生效动作 —— 把「哪些文件变了」翻译成「运维要在节点上做什么」
#
# 为什么需要：Web 上改配置/regen 后页面只说「已重新生成」，不说接下来做什么。
# 而不同文件的生效方式**完全不同**且容易踩坑：
#   - my.cnf 类的**静态参数**改了，只 `docker compose restart` 根本不生效，必须重建容器；
#   - keepalived 配置/脚本改了，只需 restart keepalived；
#   - init SQL **只在数据目录为空的首次启动**执行，已初始化的实例改它毫无效果；
#   - fix-node-creds.sql 必须**登录节点本机**用 root 执行（含真实凭证，不经过管理器）。
# 这里把差异固化成规则表，regen 后直接给出可复制的命令，杜绝「以为改了其实没生效」。
# ---------------------------------------------------------------------------

DEPLOY_LEVEL_LABEL = {
    "recreate": "必须重建容器",
    "restart": "重启服务",
    "exec": "节点执行一次",
    "manual": "人工确认",
}


def _db_service(node_type):
    """compose 中数据库服务的服务名；裸机形态返回 None（由 systemd 管理，不在 compose 内）。"""
    if node_type == "mariadb-docker":
        return "mariadb"
    if node_type in ("docker", "mysql", "mysql-docker", ""):
        return "mysql"
    return None


def _snapshot_dir(d):
    """读取节点生成目录下所有文件的内容（用于变更比对）。失败返回空 dict。"""
    out = {}
    try:
        for f in Path(d).rglob("*"):
            if f.is_file():
                rel = str(f.relative_to(d)).replace("\\", "/")
                try:
                    out[rel] = f.read_bytes()
                except Exception:
                    out[rel] = b""
    except Exception:
        pass
    return out


def _fingerprint(snapshot):
    """生成文件内容指纹（路径 + 内容），用于判断「配置是否又变了」。"""
    h = hashlib.md5()
    for k in sorted(snapshot):
        h.update(k.encode("utf-8"))
        h.update(snapshot[k])
    return h.hexdigest()


def deploy_actions(changed_files, node_type, host_dir, fresh=False):
    """把变更文件列表翻译成「需要在节点上执行的动作清单」。

    changed_files: 相对节点目录的文件路径列表（如 "docker-my.cnf"、"keepalived/keepalived.conf"）
    fresh:         是否为首次生成（旧目录为空）—— 首次生成给「首次部署」引导，不逐文件提示。

    返回 [{level, level_label, title, why, cmd, files}]，level 取值：
      recreate —— 必须重建容器才生效（数据库静态配置 / compose 编排 / 镜像）
      restart  —— 重启对应服务即可（keepalived / node-agent）
      exec     —— 需登录节点本机执行一次（prepare-logs.sh / fix-node-creds.sql）
      manual   —— 需人工确认（首次部署；init SQL 仅对空数据目录生效）
    """
    d = host_dir
    db = _db_service(node_type)
    acts = []

    def add(level, title, why, cmd, files, verify="manual"):
        acts.append({
            "level": level,
            "level_label": DEPLOY_LEVEL_LABEL.get(level, level),
            "title": title,
            "why": why,
            "cmd": cmd or "",
            "files": files or [],
            # verify：管理器「自动核验」该待办是否已在节点落实的方式（见 verify_node_pending）：
            #   db_uptime    —— 数据库进程重启（Uptime 归零）即视为落实（重建/重启数据库类）
            #   proc:<svc>   —— 节点 agent 报告的服务进程启动时间晚于 pending_since（重启 keepalived/agent）
            #   manual       —— 节点侧一次性动作，无法远程观测，仍由运维点「已执行」
            "verify": verify,
        })

    files = [str(f).replace("\\", "/") for f in (changed_files or [])]

    def hit(*names):
        out = []
        for f in files:
            base = f.rsplit("/", 1)[-1]
            for n in names:
                if "/" in n:
                    if f == n:
                        out.append(f)
                elif base == n:
                    out.append(f)
        return out

    if fresh:
        if node_type == "mariadb":
            add("manual", "首次部署：安装 MariaDB 并拉起 keepalived",
                "先跑安装脚本（全量安装 MariaDB + 初始化账号），再拉起 keepalived 与节点 agent。",
                "cd %s && sudo bash mariadb12.3.3/scripts/Install-ha-mariadb-Node.sh "
                "&& docker compose up -d keepalived custos-node-agent" % d,
                [], "db_alive")
        else:
            add("manual", "首次部署：启动节点容器（数据库 + keepalived 合一）",
                "把整个目录拷到目标机后执行一次即可；数据目录为空时会自动执行 init SQL 初始化账号。",
                "cd %s && docker compose up -d" % d, [], "db_alive")
        return acts

    # 1) 数据库配置（my.cnf 类）：静态参数，必须重建/重启数据库
    cnf = hit("docker-my.cnf", "my.cnf", "ha-overrides.cnf")
    if cnf:
        if db:
            add("recreate", "数据库配置已变更：必须重建数据库容器（在节点安装目录下执行）",
                "my.cnf 中的 server_id / 半同步插件 / buffer pool / 日志路径等均为"
                "**静态参数**，容器启动时读取，restart 不会重新加载——必须 "
                "--force-recreate 才生效。下方命令的 `cd` 路径即**节点安装目录**，请先进入该目录再执行。",
                "cd %s && docker compose up -d --force-recreate %s" % (d, db), cnf, "db_uptime")
        else:
            add("restart", "数据库配置已变更：需重启 MariaDB 服务",
                "裸机 MariaDB 由 systemd 管理（单元名 mariadb-ha.service，二进制 mariadbd），"
                "改 my.cnf 后需重启该单元生效（注意不是 mysqld/mariadb，那是官方包的服务名）。",
                "sudo systemctl restart mariadb-ha.service", cnf, "db_uptime")

    # 2) compose 编排（镜像版本 / 端口 / 挂载）
    cf = hit("docker-compose.yaml", "docker-compose.yml")
    if cf:
        add("recreate", "compose 编排已变更：需按新编排重建（在节点安装目录下执行）",
            "docker-compose.yaml 变更（镜像版本 / 端口 / 挂载 / 服务定义）必须重新 up 才会应用；"
            "镜像版本变更需先 docker compose pull。下方命令的 `cd` 路径即**节点安装目录**。",
            "cd %s && docker compose pull && docker compose up -d" % d, cf, "db_uptime")

    # 3) keepalived 配置与脚本
    kf = hit("keepalived/keepalived.conf", "keepalived/ha_check.conf",
             "keepalived/check_mysql_master.sh", "keepalived/notify_master.sh",
             "keepalived/notify_backup.sh", "keepalived/entrypoint.sh",
             "keepalived/Dockerfile")
    if kf:
        add("restart", "keepalived 配置/脚本已变更：重启 keepalived",
            "keepalived.conf、ha_check.conf 及探活/通知脚本均为容器启动时读取"
            "（脚本还是 :ro 挂载），需重启 keepalived 容器生效。",
            "cd %s && docker compose restart keepalived" % d, kf, "proc:keepalived")

    # 4) init SQL：只对空数据目录生效（最常见的「改了没用」）
    sf = hit("init.sql", "init_mariadb.sql")
    if sf:
        add("manual", "初始化 SQL 已变更：仅对空数据目录的首次启动生效",
            "init SQL 只在数据目录为空时执行一次。已初始化的实例**不会**被覆盖；"
            "若确需重刷账号/权限，请在节点本机执行 fix-node-creds.sql。",
            "", sf)

    # 5) 账号修复脚本：需登录节点本机执行
    fx = hit("fix-node-creds.sql")
    if fx:
        add("exec", "账号/权限修复脚本已更新：需在节点本机执行",
            "该文件含节点真实凭证，不经管理器下发，需登录节点用 root 执行"
            "（命令会交互式提示输入 root 密码）。",
            "cd %s && mysql -uroot -p < %s" % (d, fx[0]), fx)

    # 6) 节点 Agent（OS 指标采集）
    af = hit("node_agent.py")
    if af:
        add("restart", "节点 Agent 已更新：重启 custos-node-agent",
            "agent 脚本为 :ro 挂载，重启容器即可加载新版本。",
            "cd %s && docker compose restart custos-node-agent" % d, af, "proc:node-agent")

    # 7) 日志目录 / logrotate
    lf = hit("prepare-logs.sh", "custos-ha-logrotate.conf")
    if lf:
        add("exec", "日志目录/logrotate 配置已更新：需在节点执行一次",
            "统一日志目录需预先创建并把属主改为容器内 mysql(uid 999)，否则 "
            "mariadbd/mysqld 写 error.log 会 Permission denied 启动失败。",
            # ⚠ _node_conf_dir() 返回的是**字符串**，不能直接 `/` 拼路径（str / str 会抛
            # TypeError: unsupported operand type(s) for /: 'str' and 'str'）。
            # 实测（2026-10-02）：该异常只在「首次生成节点包」时触发（此时 prepare-logs.sh
            # 才属于变更文件、才会走到这一行），表现为「页面报 str/str 错误，但节点其实
            # 已经建成」，重复点一次（文件不再是变更项）就正常 —— 极难定位。
            "cd %s && sudo bash %s" % (d, Path(_node_conf_dir(node_type)) / "conf" / "scripts" / "prepare-logs.sh"), lf)

    # 8) 安装/卸载脚本：仅在重装/卸载场景执行
    inf = hit("Install-ha-mariadb-Node.sh", "Uninstall-mariadb-Node.sh",
              "Uninstall-mariadb-docker-Node.sh")
    if inf:
        add("manual", "安装/卸载脚本已更新：仅在重装或卸载时需要执行",
            "正常运行中的节点无需执行；重装/改密重跑 Install，下线节点跑 Uninstall。",
            "", inf)

    # update-node.sh 为工具脚本（按需手动用），不产生待办动作
    return acts


def node_pending_actions(ip):
    """读取节点当前的「配置待生效」动作清单（无待办返回 []）。

    v2.3.7：读取前先自动核验一次（verify_node_pending），已落实的动作被自动清除，
    因此页面看到的就是「真实仍未生效」的待办，无需运维每次手动点「已执行」。
    """
    try:
        verify_node_pending(ip)
    except Exception:
        pass
    try:
        rows = query("SELECT pending_actions FROM nodes WHERE ip=%s", (ip,))
    except Exception:
        return []
    if not rows:
        return []
    raw = (rows[0] or {}).get("pending_actions") or ""
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def ack_node_pending(ip, force=False):
    """运维确认已在节点执行完动作后调用，清除「配置待生效」提示。

    v2.3.8 修正（此前是盲清，是「反复点修复却没效果」的直接根因）：
      旧实现只有一句 ``UPDATE nodes SET pending_actions=''`` —— 不连节点、不核验、
      不区分条目，点一下徽标就消失。运维据此以为配置已生效，实际节点上根本没变，
      于是问题复现 → 再 regen → 再点，陷入死循环。UI 把「我知道了」呈现成了「已完成」。

    现在的语义：
      - manual 类（首次部署、执行 SQL 脚本等无法远程观测的一次性动作）→ 允许清除；
      - db_uptime / proc:* 类 → **只有自动核验确认已落实才清除**；未落实的一律保留，
        并回传 reason（为什么判定未生效），让运维看到真实原因而不是干瞪眼；
      - force=True 是人工兜底：当核验方式确实不适用于该环境（例如数据库在 regen
        之前就重启过、或节点压根没部署 custos-node-agent）时可强制清除，
        避免待办因核验条件永不满足而永久悬挂。

    返回 {"cleared": [标题...], "kept": [{"title","reason"}...], "forced": bool}。
    """
    try:
        acts = node_pending_actions(ip) or []   # 内部已先自动核验，已落实的会被移除
    except Exception:
        acts = []
    if not acts:
        try:
            execute("UPDATE nodes SET pending_actions='', pending_since=NULL WHERE ip=%s", (ip,))
        except Exception:
            pass
        return {"cleared": [], "kept": [], "forced": bool(force)}

    cleared, kept = [], []
    for a in acts:
        v = str((a or {}).get("verify") or "manual")
        auto = (v == "db_uptime") or v.startswith("proc:")
        if force or not auto:
            cleared.append((a or {}).get("title") or "")
        else:
            kept.append(a)

    if not kept:
        try:
            execute("UPDATE nodes SET pending_actions='', pending_since=NULL WHERE ip=%s", (ip,))
        except Exception:
            pass
    else:
        _save_pending(ip, kept)      # 只保留「确实还没生效」的
    return {
        "cleared": cleared,
        "kept": [{"title": (a or {}).get("title") or "",
                  "reason": (a or {}).get("reason") or "自动核验未通过"} for a in kept],
        "forced": bool(force),
    }


def _node_creds(ip):
    """取节点连接数据库所需的端口与 root 凭证（root_pass 由 db.query 透明解密）。"""
    try:
        rows = query("SELECT port, root_user, root_pass FROM nodes WHERE ip=%s", (ip,))
    except Exception:
        return None
    if not rows:
        return None
    r = rows[0] or {}
    if not r.get("port"):
        return None
    return {
        "port": int(r["port"]),
        "root_user": (r.get("root_user") or "root"),
        "root_pass": (r.get("root_pass") or ""),
    }


def _agent_svc_started(agent, svc):
    """从节点 agent /stats 负载里取某服务进程的启动 epoch 秒；取不到返回 None。"""
    if not isinstance(agent, dict):
        return None
    if svc == "node-agent":
        v = agent.get("agent_started_at")
    elif svc == "keepalived":
        v = agent.get("keepalived_started_at")
    else:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _save_pending(ip, acts):
    """写回剩余待办；acts 为空则同时清空 pending_since（页面徽标随之消失）。"""
    try:
        if acts:
            execute("UPDATE nodes SET pending_actions=%s WHERE ip=%s",
                    (json.dumps(acts, ensure_ascii=False), ip))
        else:
            execute("UPDATE nodes SET pending_actions='', pending_since=NULL WHERE ip=%s", (ip,))
    except Exception as e:
        logger.warning("写回节点[%s]待生效清单失败(可忽略): %s", ip, e)


def verify_node_pending(ip):
    """自动核验节点「配置待生效」中哪些已在节点上落实，落实的自动移除（徽标随之消失）。

    落实判定（与 deploy_actions 写入的 verify 字段一一对应）：
      - db_uptime  ：数据库进程 Uptime < 待办产生后的经过秒数 → 数据库已被重建/重启；
      - proc:svc   ：节点 agent 报告的服务进程启动时间晚于 pending_since → 该服务已被重启；
      - manual     ：exec / 首次部署等节点侧一次性动作，无法远程观测，保留（仍由运维点「已执行」）。

    全程只读探测（DB 查 Uptime / 节点 agent 读 /proc），不改动节点任何配置；
    仅在确认落实时更新管理库 nodes 表。
    """
    try:
        rows = query(
            "SELECT pending_actions, pending_since, "
            "TIMESTAMPDIFF(SECOND, pending_since, NOW()) AS elapsed "
            "FROM nodes WHERE ip=%s", (ip,))
    except Exception:
        return
    if not rows:
        return
    row = rows[0] or {}
    raw = row.get("pending_actions") or ""
    if not raw:
        return
    try:
        acts = json.loads(raw)
    except Exception:
        return
    if not isinstance(acts, list) or not acts:
        return

    # 待办产生时间与「已过去秒数」。elapsed 由管理库用同一时钟算出（TIMESTAMPDIFF），
    # 与数据库 Uptime 同源，避免管理器容器与管理库时区不一致造成误判。
    ps_dt = None
    ps = row.get("pending_since")
    if ps:
        try:
            ps_dt = ps if isinstance(ps, datetime) else datetime.strptime(str(ps), "%Y-%m-%d %H:%M:%S")
        except Exception:
            ps_dt = None
    try:
        elapsed = int(row.get("elapsed")) if row.get("elapsed") is not None else None
    except (TypeError, ValueError):
        elapsed = None

    # 按需取证：只在确有对应类型待办时才连 DB / 拉 agent，避免无谓开销。
    db_up, agent = None, None
    if any(a.get("verify") in ("db_uptime", "db_alive") for a in acts):
        c = _node_creds(ip)
        if c:
            db_up = db_uptime(ip, c["port"], c["root_user"], c["root_pass"],
                              c["root_user"], c["root_pass"])
    if any(str(a.get("verify", "")).startswith("proc:") for a in acts):
        try:
            from .os_metrics import fetch_node_stats
            agent = fetch_node_stats(ip)
        except Exception:
            agent = None

    remain = []
    for a in acts:
        v = a.get("verify")
        if v == "db_uptime":
            # 数据库在待办产生之后被重建/重启过（Uptime 比经过时间短）即视为落实。
            if elapsed is not None and db_up is not None and db_up < elapsed - 2:
                logger.info("节点[%s] 待办已落实(数据库已重启, Uptime=%ss<%.0fs)：%s",
                            ip, db_up, elapsed, a.get("title"))
                continue
            # 未落实：写明原因，避免运维只看到一个徽标却不知道差在哪一步。
            if db_up is None:
                a["reason"] = ("连不上节点数据库、取不到 Uptime，无法确认是否已重启"
                               "（多为 root 凭证/网络/端口问题）")
            else:
                a["reason"] = ("数据库自待办产生后未重启：当前 Uptime=%ss，待办已产生 %ss。"
                               "请在该节点执行重建/重启命令后再看（生效后会自动消失）"
                               % (db_up, elapsed))
            remain.append(a)
        elif v == "db_alive":
            # 首次部署类动作：只要节点数据库能连上，就说明该节点已安装/拉起完成。
            # （此前是 manual，永远等不到自动消除，导致「配置待生效」徽标长期挂死。）
            if db_up is not None:
                logger.info("节点[%s] 待办已落实(节点数据库已可连接, Uptime=%ss)：%s",
                            ip, db_up, a.get("title"))
                continue
            a["reason"] = ("连不上该节点的数据库，尚未完成部署（或 root 凭证/端口/网络不通）；"
                           "在节点执行安装/启动命令后会自动消失")
            remain.append(a)
        elif isinstance(v, str) and v.startswith("proc:"):
            svc = v.split(":", 1)[1]
            started = _agent_svc_started(agent, svc)
            if started is not None and ps_dt is not None and started > ps_dt.timestamp() - 2:
                logger.info("节点[%s] 待办已落实(%s 已于待办后重启)：%s",
                            ip, svc, a.get("title"))
                continue
            if not isinstance(agent, dict) or agent.get("error") or agent is None:
                a["reason"] = ("节点 custos-node-agent 不可达，读不到 %s 进程启动时间，无法自动核验；"
                               "请在节点部署/重启 agent（老节点需 regen 后重启 custos-node-agent）" % svc)
            elif started is None:
                a["reason"] = "节点 agent 未上报 %s 的启动时间，无法自动核验" % svc
            else:
                a["reason"] = ("%s 自待办产生后未重启，请在节点执行重启命令；"
                               "若管理器与该节点时钟不一致也会导致误判，可点「仍然清除」人工确认" % svc)
            remain.append(a)
        else:
            # manual / exec：节点侧一次性动作，保留待运维确认或后续人工处理。
            a["reason"] = "该动作需在节点上手工执行，无法远程核验，需人工确认"
            remain.append(a)

    if len(remain) != len(acts):
        _save_pending(ip, remain)


def verify_pending_all():
    """后台监控周期调用：对全部「有待办」的节点跑一次自动核验。"""
    try:
        rows = query("SELECT ip FROM nodes WHERE pending_actions IS NOT NULL AND pending_actions != ''")
    except Exception:
        return
    for r in rows:
        ip = (r or {}).get("ip")
        if not ip:
            continue
        try:
            verify_node_pending(ip)
        except Exception as e:
            logger.warning("自动核验节点[%s]待生效异常(可忽略): %s", ip, e)


def _auto_push_changed(ip, changed, unit_dir):
    """把重新生成后**真正变化的文件**经 agent 自动下发到节点（v2.5.0 F4）。

    以前重新生成只在页面留一句「请在节点执行 update-node.sh」，节点侧的配置
    永远是旧的，而页面看起来一切正常——这正是「配置漂移」。现在直接下发：
      - keepalived/* 变化 → 重启 keepalived
      - agent/* 变化      → 重启 agent
    下发失败也不影响主流程（仍可按「待生效操作」手工执行）。
    """
    import base64
    from pathlib import Path
    from . import agent_hub

    # 文件要写到【该节点真实的安装目录】。写死默认目录会导致自定义目录的节点
    # 被下发到 /home/docker/custos-node（agent 建出一套不生效的副本，配置漂移依旧）。
    try:
        _cp = query("SELECT compose_path FROM nodes WHERE ip=%s", (ip,))
        _dest = ((_cp[0] or {}).get("compose_path") if _cp else "") or ""
    except Exception:
        _dest = ""
    _dest = node_install_dest(_dest) if _dest else node_install_dest(None)

    files, restart = {}, set()
    for rel in changed or []:
        p = Path(unit_dir) / rel
        if not p.is_file():
            continue
        try:
            files["%s/%s" % (_dest, rel)] = base64.b64encode(p.read_bytes()).decode()
        except OSError:
            continue
        r = str(rel)
        if r.startswith("keepalived"):
            restart.add("keepalived")
        elif r.startswith("agent"):
            restart.add("agent")
    if not files:
        return None
    return agent_hub.enqueue(
        ip, "update_files",
        {"files": files, "restart": sorted(restart)},
        actor="node_gen", ttl_sec=3600,
        dedupe_key="drift_%s_%s" % (ip, abs(hash(tuple(sorted(files)))) % 10**8),
    )


def generate_node(ip, port, root_user, root_pass, initial_role="slave", cluster_id=None,
                  node_type="mariadb-docker", mysql_version=None, server_id=None,
                  install_dir=None):
    ip = (ip or "").strip()
    if not ip:
        raise ValueError("节点 IP 不能为空")
    # 节点在目标机上的最终安装目录（install_dir 视为【基目录】→ 统一落到 <基>/custos-node）。
    # 本函数内所有「节点侧绝对路径」都必须用它，避免自定义目录时仍写死默认路径。
    _install_dest = node_install_dest(install_dir)
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

    # ---- 版本跟随：节点未显式指定版本时，跟随集群已登记的版本 ----
    # 场景：接管了 10.6 / 11.4 老库之后再添加从库。旧逻辑镜像写死 12.3.3，会把跨大版本
    # 节点塞进老集群（非受支持组合且页面无从选择）。此处补齐缺省版本，使后续
    # 「镜像 / 节点包目录名 / my.cnf 版本分支」全部跟随集群版本。
    if not (mysql_version or "").strip():
        try:
            from .clusters import get_cluster as _get_cluster
            _cv = ((_get_cluster(cluster_id) or {}).get("mysql_version") or "").strip()
        except Exception:
            _cv = ""
        if _cv:
            mysql_version = _cv

    vip = creds["vip"]
    if not vip:
        raise ValueError("请先在「集群管理」中为该集群设置 VIP（节点同网段空闲 IP）后再添加节点")
    if not _valid_vip(vip):
        raise ValueError("集群 VIP 格式非法（%r），应为合法 IPv4，如 192.0.2.10" % vip)
    vip_prefix = creds["vip_prefix"] or VIP_PREFIX
    interface = creds["interface"] or INTERFACE
    # ---- 节点级 interface 优先（v2.5.0）：多网卡主机混部的集群，各节点网卡名可能
    # 不同，必须按节点自身网卡上浮 VIP，否则 VIP 落错网卡（漂不起来）。节点未登记
    # 时回退集群级 interface / 全局 INTERFACE。 ---
    try:
        _nif = query(
            "SELECT interface FROM nodes WHERE ip=%s AND port=%s", (ip, int(port))
        )
        if _nif and _nif[0].get("interface"):
            interface = _nif[0]["interface"]
    except Exception:
        pass
    vrid = creds["vrid"] or VRID
    vip_port = creds.get("vip_port") or VIP_PORT
    manager_public_ip = get_manager_public_ip()
    manager_subnet = get_manager_docker_subnet_wildcard()
    # ---- server_id：支持页面自定义，规避「自动分配撞 id」的风险 ----
    # 自定义值必须全局唯一（复制拓扑里撞 id 会导致 GTID 混乱、复制反复中断），
    # 故先做范围与占用校验；未填则沿用原策略。
    if server_id not in (None, "", 0):
        try:
            _sid = int(server_id)
        except (TypeError, ValueError):
            raise ValueError("server_id 必须是整数，收到 %r" % (server_id,))
        if _sid < 1 or _sid > 4294967295:
            raise ValueError("server_id 必须落在 1~4294967295（MySQL/MariaDB 合法范围）")
        _used = server_id_conflict(_sid, exclude_ip=ip, exclude_port=port)
        if _used:
            raise ValueError("server_id %d 已被节点 %s 占用，请换一个" % (_sid, _used))
        server_id = _sid
    else:
        server_id = get_next_server_id()
        # 【修复 2026-09-24】server_id 必须与该节点自身的 GTID 历史保持一致。
        # 同一节点(ip+port)重新生成配置时若换成新的 server_id，其 data 目录里原有的
        # GTID（domain / server 取自旧 server_id）会与新 domain 混杂，主从随即报
        #   1236 '... missing the GTID x-y-N requested by the slave
        #         (even though both a prior and a subsequent sequence number does exist)'
        # —— 即主库 binlog 出现「空洞」。故已存在的节点一律复用原 server_id，
        #    只有在首次添加（nodes 表无记录）时才分配新的。
        try:
            _rows = query("SELECT server_id FROM nodes WHERE ip=%s AND port=%s", (ip, int(port)))
            if _rows and _rows[0].get("server_id"):
                server_id = int(_rows[0]["server_id"])
        except Exception:
            pass
    node_name = f"mysql-ha-node-{ip.replace('.', '-')}"
    peers = _peer_ips(ip, cluster_id)
    # MariaDB 两种形态（裸机 / 容器 host 网络）的 keepalived 容器都在 host 网络，
    # 连本机 MariaDB 用 127.0.0.1；MySQL Docker 模式连节点 IP（桥接网络靠端口发布）。
    local_host = "127.0.0.1" if is_mariadb_node(node_type) else ip

    d = _safe_node_path(ip)
    # v2.3.0：清空前先快照旧文件，用于生成后 diff 出「变更了什么」，
    # 进而算出「需要在节点上执行哪些动作」给运维看。
    _old_snap = _snapshot_dir(d) if d.exists() else {}
    d.mkdir(parents=True, exist_ok=True)
    _clean_generated_dir(d)
    kd = d / "keepalived"
    kd.mkdir(parents=True, exist_ok=True)

    # 1) init.sql / compose（按节点类型分支）
    init_sql = _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                                auth_plugin=_auth_plugin_for(mysql_version, node_type))
    extra_files = {}
    # 节点包内数据库配置目录名：跟随版本（mariadb11.4.13 / mariadb10.6.28 / mariadb12.3.3 ...）
    _mconf = _node_conf_dir(node_type, mysql_version)
    if is_mariadb_node(node_type):
        # MariaDB 两种形态（裸机 / 容器）共用：init_mariadb.sql + fix-node-creds.sql。
        # 差异仅在「配置文件」与「部署脚本」，故此处先写公共部分，再按形态分支。
        _write(d / _mconf /"conf" / "init_mariadb.sql", render_mariadb_init_sql(
            ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass), mode=0o644)
        # 账号/权限修复脚本（MariaDB 语法，native 密码）
        fix_sql = render_fix_node_creds_sql_mariadb(
            ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass, manager_public_ip, port,
            manager_subnet)
        _write(d / _mconf /"conf" / "fix-node-creds.sql", fix_sql, mode=0o644)
        extra_files["init_mariadb_sql"] = f"{_install_dest}/{_mconf}/conf/init_mariadb.sql"
        # gtid_domain_id 显式指定为与 server_id 一致的非 0 值：MariaDB 一主多从/切换场景下，
        # 每个节点用各自独立域，避免切换后双主写入产生 GTID 冲突（预检不再报 domain_id=0）。
        if node_type == "mariadb-docker":
            # 容器化 MariaDB：纯 compose 部署（render_mariadb_compose 已含 mariadb + keepalived），
            # 故只交付配置片段（不含 datadir/socket）+ 卸载脚本，不再生成安装脚本
            # （部署入口即 docker compose up -d）；安装脚本仅作「裸机装不上 tarball」回退在裸机分支交付。
            _write(d / _mconf /"conf" / "docker-my.cnf",
                   render_mariadb_docker_my_cnf(server_id, port, gtid_domain_id=server_id),
                   mode=0o644)
            extra_files["docker_my_cnf"] = f"{_install_dest}/{_mconf}/conf/docker-my.cnf"
            extra_files["uninstall_script"] = f"{_install_dest}/{_mconf}/scripts/Uninstall-mariadb-docker-Node.sh"
            src = NODE_SCRIPTS / "Uninstall-mariadb-docker-Node.sh"
            if src.exists():
                _write(d / _mconf /"scripts" / "Uninstall-mariadb-docker-Node.sh", src.read_text(), mode=0o755)
        else:
            # 裸机 MariaDB：完全独立安装，由 Install 脚本全量安装 tarball。
            # 完整 my.cnf（Install 脚本落盘到 ${MARIADB_INSTALL_DIR}/my.cnf 或 /etc/my.cnf）
            my_cnf = render_mariadb_my_cnf(server_id, port, gtid_domain_id=server_id)
            _write(d / _mconf /"conf" / "my.cnf", my_cnf, mode=0o644)
            extra_files["my_cnf"] = f"{_install_dest}/{_mconf}/conf/my.cnf"
            extra_files["install_script"] = f"{_install_dest}/{_mconf}/scripts/Install-ha-mariadb-Node.sh"
            extra_files["uninstall_script"] = f"{_install_dest}/{_mconf}/scripts/Uninstall-mariadb-Node.sh"
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
                    _write(d / _mconf /"scripts" / sf, src.read_text(), mode=0o755)
    else:
        # init.sql 会被 MySQL 容器以非 root 的 mysql 用户读取执行，
        # 若权限过严（0600）会因 Permission denied 失败；改为 0644 保证可读。
        _write(d / MYSQL_NODE_CONF_DIR / "conf" / "init.sql", init_sql, mode=0o644)
        # 慢日志覆盖配置（挂载到容器 /etc/mysql/conf.d）
        _write(d / MYSQL_NODE_CONF_DIR / "conf" / "ha-overrides.cnf", render_docker_my_cnf(mysql_version, port), mode=0o644)

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
        _write(d / AGENT_DIR / "node_agent.py", agent_src.read_text(), mode=0o644)
    svc_src = Path(__file__).resolve().parent.parent / "scripts" / "node-agent.service"
    if svc_src.exists():
        _write(d / AGENT_DIR / "node-agent.service", svc_src.read_text(), mode=0o644)
    # 节点包自带 custosctl（供 `custosctl node install` 自装到 /usr/bin/custosctl）
    ctl_src = Path(__file__).resolve().parent.parent / "custosctl"
    if ctl_src.exists():
        _write(d / "custosctl", ctl_src.read_text(), mode=0o755)

    # 5) docker-compose.yaml
    #    MariaDB 裸机（mariadb）：compose 只跑 keepalived 客户端（mysqld 由宿主 systemd 起）；
    #    MariaDB 容器化（mariadb-docker）：纯 compose（mariadb + keepalived 合一）；
    #    仅 MySQL Docker 才在 compose 内单独含 MySQL 容器。
    if node_type == "mariadb":
        compose = render_baremetal_compose(node_name, vip_port)
    elif node_type == "mariadb-docker":
        compose = render_mariadb_compose(node_name, root_pass, port, server_id, vip_port,
                                         mysql_version=mysql_version, conf_dir=_mconf)
    else:
        compose = _render_compose(node_name, root_pass, port, server_id, vip_port)

    # ---- v2.5.0 F5：每节点独立 agent token + 心跳环境变量 ----
    # regen 时**保留既有 token**：换 token 会让已经在线的 agent 立刻认证失败、心跳断连，
    # 而重新下发单元/重启容器并不是总能同时发生。
    try:
        _tk_rows = query("SELECT agent_token FROM nodes WHERE ip=%s", (ip,))
        agent_token = ((_tk_rows[0] or {}).get("agent_token") if _tk_rows else "") or ""
    except Exception:
        agent_token = ""
    if not agent_token:
        from .config import rand_secret as _rand_secret
        agent_token = _rand_secret(24)
    from .config import WEB_PORT as _WEB_PORT
    _mgr_url = "http://%s:%d" % (get_manager_public_ip() or "", int(_WEB_PORT))
    _unit_dir = _install_dest
    _anchor = "  custos-node-agent:\n    image: python:3-slim"
    if _anchor in compose:
        compose = compose.replace(
            _anchor,
            _anchor + "\n" + agent_env_yaml(ip, agent_token, _mgr_url, _unit_dir).rstrip("\n"),
            1)
    else:
        # 模板结构若变化，宁可显式告警也不要静默丢掉心跳配置
        print("[warn] 未能在 compose 中找到 custos-node-agent 服务块，agent 心跳未配置", flush=True)

    _write(d / "docker-compose.yaml", compose)

    # 5.5) 统一日志目录：logrotate 配置 + 目录初始化脚本（三形态一致）
    #      排错时按固定路径 /data/custos-ha/logs/<基名>/ 找日志，不再逐个形态猜。
    _data_dir_for_log = {
        "mariadb": "/data/mariadb",              # 裸机默认（与 render_mariadb_my_cnf 默认值一致）
        "mariadb-docker": MARIADB_DOCKER_DATA_DIR,
    }.get(node_type, MYSQL_DOCKER_DATA_DIR)
    # 目录名必须跟随版本（与上面的 _mconf 同源）：否则 11.4/10.6 节点会把
    # logrotate / prepare-logs.sh / update-node.sh 写进 mariadb12.3.3/，
    # 而数据库配置在 mariadb11.4.13/ —— 安装脚本里的日志初始化路径随之错位。
    conf_dir = _mconf
    _write(d / conf_dir / "conf" / "custos-ha-logrotate.conf", render_logrotate_conf(), mode=0o644)
    _write(d / conf_dir / "scripts" / "prepare-logs.sh",
           render_prepare_logs(node_type, _log_dir_host(_data_dir_for_log)), mode=0o755)
    _write(d / conf_dir / "scripts" / "update-node.sh", render_update_node(), mode=0o755)

    # 6) 登记到 nodes 表 + hosts 表（供监控自动接入）
    #    compose_path 记录「宿主机路径」（便于 Web 展示与到目标主机执行），
    #    实际文件仍写入容器内的 NODES_DIR（由 docker-compose 挂载到 ./generated_nodes）。
    #    注意：每个 <ip> 子目录是自包含部署单元，拷贝到目标主机的
    #    <NODE_INSTALL_BASE>/<NODE_DIR_NAME>（默认 /home/docker/custos-node）即可运行。
    #    root_pass 一并入库：节点 MySQL root 密码在容器创建时固定，作为 ha_monitor 凭证
    #    自愈与切换回退的权威凭据（不随 HA_PASS 漂移）。
    # 节点安装目录：添加节点时可按节点指定（install_dir 为【基目录】，绝对路径），否则默认。
    # 统一走 node_install_dest：基目录 /opt → 实际目录 /opt/custos-node（不把文件散在 /opt 根）。
    # 该值仅作为「宿主机部署目录」元数据（compose_path）与引导命令默认目录，不影响管理器本地生成路径。
    host_compose = _install_dest
    # v2.3.1：root 口令落库前加密（后续写 ha_check.conf 等仍用局部明文变量，不受影响）
    _root_pass_enc = _crypto.encrypt(root_pass)
    execute(
        # agent_token 只在「为空」时写入：regen 不覆盖既有 token，
        # 否则已在线节点的 agent 会因 token 变更而认证失败、心跳断连。
        "INSERT INTO nodes (ip, port, root_user, root_pass, server_id, compose_path, cluster_id, node_type, mysql_version, agent_token) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE port=%s, root_user=%s, root_pass=%s, server_id=%s, "
        "compose_path=%s, cluster_id=%s, node_type=%s, mysql_version=%s, "
        "agent_token=IF(agent_token IS NULL OR agent_token='', VALUES(agent_token), agent_token)",
        (ip, port, root_user, _root_pass_enc, server_id, host_compose, cluster_id, node_type, mysql_version, agent_token,
         port, root_user, _root_pass_enc, server_id, host_compose, cluster_id, node_type, mysql_version),
    )
    execute(
        "INSERT INTO hosts (host, port, role, status, initial_role, cluster_id) "
        "VALUES (%s,%s,'unknown','down',%s,%s) "
        "ON DUPLICATE KEY UPDATE initial_role=%s, cluster_id=%s",
        (ip, port, initial_role, cluster_id, initial_role, cluster_id),
    )
    # ---- v2.3.0：变更生效提示 ----
    # 只有指纹真的变了才刷新待办（幂等：重复 regen 不会把已确认的待办又刷回来，
    # 也不会因为「无变化」而误清空运维尚未执行的动作）。
    _new_snap = _snapshot_dir(d)
    _changed = sorted(k for k in (set(_new_snap) | set(_old_snap))
                      if _new_snap.get(k) != _old_snap.get(k))
    # v2.5.0 F4：把变化的文件经 agent 自动下发，节点配置不再停留在旧版本。
    # 失败不阻断生成（页面仍保留「待生效操作」供人工执行）。
    if _changed:
        try:
            _auto_push_changed(ip, _changed, d)
        except Exception as _e:
            print("[warn] 自动下发变更文件失败（可改用「待生效操作」手工执行）：%s" % _e,
                  flush=True)
    _fp = _fingerprint(_new_snap)
    try:
        _prev = query("SELECT gen_fingerprint FROM nodes WHERE ip=%s", (ip,))
        _prev_fp = ((_prev[0] or {}).get("gen_fingerprint") or "") if _prev else ""
    except Exception:
        _prev_fp = ""
    # fresh（首次部署）必须同时满足：生成目录为空 **且** 库中无历史指纹（此前从未生成过）。
    # 否则一旦生成目录被清空/迁移后重新生成，已上线节点会被误报「首次部署」。
    _actions = deploy_actions(_changed, node_type, host_compose,
                              fresh=(not _old_snap) and (not _prev_fp))
    if _fp != _prev_fp:
        try:
            execute("UPDATE nodes SET gen_fingerprint=%s, pending_actions=%s, "
                    "pending_since=NOW() WHERE ip=%s",
                    (_fp, json.dumps(_actions, ensure_ascii=False) if _actions else "", ip))
        except Exception:
            pass
    # ⚠ 不再「添加节点即任命主库」：initial_role 只是【偏好】，不是任命。
    # 旧行为在此直接把 current_master 写成该节点，可它还根本没装/没上线；
    # 而 ensure_topology 只在 current_master 为空时才选主 ⇒ 部署期被永久钉在一个
    # 不可达的主库上（其余节点复制全挂），同时先起来的节点没人置只读、变成可写野主。
    # 现在仅由 hosts.initial_role 记录偏好，真正选主交给 ensure_topology 的部署期逻辑：
    # 预设主库【上线后】才被选为主；上线前其余节点等待且强制只读。

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
        image=(mariadb_image_for_version(mysql_version) if node_type == "mariadb-docker" else ""),
        run_cmd=run_cmd,
        actions=_actions,
        changed_files=_changed,
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
            # F12（2026-09-16 真机）：MariaDB 12.3.3 **没有** super_read_only 变量，把两者写进
            # 同一条 SELECT 会整条报 1193 → 检测失败 → 只读副本被误判为「可写」，后果：
            #   ① 破坏「副本跳过直接账号同步」的保护（会与复制线程争用 mysql.* 元数据锁）；
            #   ② 副本永远进不了只读分支 → 本轮 F11 的影子账号修复也永不执行。
            # 实测 33 号节点 read_only=ON 却被判为可写。故拆成两条、各自容错。
            ro = 0
            try:
                cur.execute("SELECT @@read_only AS ro")
                _r = cur.fetchone()
                if _r:
                    ro = _r.get("ro") if isinstance(_r, dict) else _r[0]
            except Exception:
                pass
            sro = 0
            try:
                cur.execute("SELECT @@super_read_only AS sro")
                _r2 = cur.fetchone()
                if _r2:
                    sro = _r2.get("sro") if isinstance(_r2, dict) else _r2[0]
            except Exception:
                sro = 0      # MariaDB 无 super_read_only（1193）：忽略，仅以 read_only 判定
            return bool(ro or sro)
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
            node_type = n.get("node_type") or "mariadb-docker"
            mysql_version = n.get("mysql_version") or ""

            # 只读副本：账号密码由主库经复制自动同步，严禁在副本上直接执行
            # CREATE/ALTER USER —— 否则会与复制线程争用 mysql.* 元数据锁，造成复制线程
            # 死锁 / 长时间等待（'Waiting for dependent transaction to commit'），
            # 复制卡死、Seconds_Behind_Source 持续增大、磁盘 I/O 居高不下。
            # F11（2026-09-16 真机）：无论主/从，都修复 ha_monitor「精确 host 影子账号」
            # 的权限落差 —— 否则管理器连得上却没权限（1227），复制无法自愈、切换中止。
            # repair_ha_shadow_grants 用 root 执行 GRANT（需 GRANT OPTION）、只授权不建号
            # 不改密 + sql_log_bin=0，因而对只读副本也安全（这正是副本无法被管理时的解药）。
            try:
                _rep, _rmsg = repair_ha_shadow_grants(
                    ip, port, ha_user, ha_pass, root_user, root_pass,
                    manager_ip=manager_public_ip)
                if _rep:
                    logger.info("节点 %s:%s 影子账号权限已修复：%s", ip, port, _rmsg)
            except Exception as _e:
                logger.info("节点 %s:%s 影子账号修复跳过：%s", ip, port, _e)
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
            # 落盘到 <db>/conf/ 子目录（与 generate_node 一致；install 脚本与 compose 均从 conf/ 读取）。
            if is_mariadb_node(node_type):
                init_sql = render_mariadb_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass)
                _write(_safe_node_path(ip) / _node_conf_dir(node_type, mysql_version) / "conf" / "init_mariadb.sql", init_sql, mode=0o644)
            else:
                init_sql = _render_init_sql(ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                                            auth_plugin=_auth_plugin_for(mysql_version, node_type))
                _write(_safe_node_path(ip) / MYSQL_NODE_CONF_DIR / "conf" / "init.sql", init_sql, mode=0o644)
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
            _write(_safe_node_path(ip) / _node_conf_dir(node_type, mysql_version) / "conf" / "fix-node-creds.sql", fix_sql, mode=0o644)
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
                "n.node_type, n.mysql_version, n.pending_actions, n.pending_since, "
                "h.initial_role, h.role, h.status, n.cluster_id "
                "FROM nodes n LEFT JOIN hosts h ON h.host = n.ip AND h.cluster_id=n.cluster_id "
                "WHERE n.cluster_id=%s ORDER BY n.ip",
                (cluster_id,),
            )
        return query(
            "SELECT n.ip, n.port, n.root_user, n.root_pass, n.server_id, n.compose_path, n.created_at, "
            "n.node_type, n.mysql_version, n.pending_actions, n.pending_since, "
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
