import os
import socket
import secrets


def env(name, default=None):
    return os.environ.get(name, default)


def rand_secret(n=16):
    """生成 URL 安全的随机密钥（用于自动生成账号密码）。"""
    return secrets.token_urlsafe(n)


# 管理 MySQL（独立容器，存配置/拓扑/切换历史/账号）
# 默认库用户/密码/库名统一为 "ha"（compose 也以 ha 作为默认值）
MGMT = dict(
    host=env("MGMT_MYSQL_HOST", "192.168.1.88"),
    port=int(env("MGMT_MYSQL_PORT", 3306)),
    user=env("MGMT_MYSQL_USER", "ha"),
    password=env("MGMT_MYSQL_PASS", "ha"),
    db=env("MGMT_MYSQL_DB", "ha"),
)

WEB_PORT = int(env("WEB_PORT", 28888))
WEB_USER = env("WEB_USER", "admin")
WEB_PASS = env("WEB_PASS", "admin")  # 为空则首次启动随机生成并打印
SECRET_KEY = env("SECRET_KEY", "change-me-in-production")

# 检测与切换
CHECK_INTERVAL = int(env("CHECK_INTERVAL", 2))   # 探活间隔(秒)
FAIL_THRESHOLD = int(env("FAIL_THRESHOLD", 2))    # 连续失败次数阈值
# 单节点探活超时（秒）：主库服务停止时会立即收到 RST（连接拒绝很快返回），
# 仅当主机断电/网络黑洞时才走到该超时。调小可加快「主机失联」判定（配合 FAIL_THRESHOLD）。
PROBE_TIMEOUT = int(env("PROBE_TIMEOUT", 2))
# 注：主库失联切换延迟 ≈ FAIL_THRESHOLD×PROBE_TIMEOUT（当前 2×2=4s），已为安全下限，
# 再调小易误切。如确需更快可临时设环境变量覆盖，不建议写死更小值。

# 节点生成目录（一键启动 compose 落地目录；建议挂卷持久化）
NODES_DIR = env("NODES_DIR", "/app/generated_nodes")

# 节点生成目录在宿主机上的「显示路径」：docker-compose 将项目根下的
# ./generated_nodes 挂载到容器内的 /app/generated_nodes，因此节点文件在宿主机上的
# 实际落盘路径为 <项目根>/generated_nodes/<ip>。每个 <ip> 子目录是自包含的部署单元
# （docker-compose.yaml / init.sql / keepalived/ 均相对本目录，MySQL 数据落在 ./MySQL-data），
# 可直接拷贝到目标主机的 /home/docker/<ip>/ 运行，路径不会越级。
# 该值用于 Web 展示与「到目标主机执行」命令，可通过 NODES_HOST_BASE 覆盖（如项目目录改名时）。
NODES_HOST_BASE = env("NODES_HOST_BASE", "mysql-ha-manager/generated_nodes")

# VIP / 网络（供节点生成使用，可在 Web「服务端配置」中设置）
# VIP 必须由用户填写（需为节点同网段空闲 IP，无法自动探测），故默认留空；
# 网卡与 VRID 提供默认值（仍可在 Web 覆盖）。
VIP = env("VIP", "")
VIP_PREFIX = env("VIP_PREFIX", "24")
INTERFACE = env("INTERFACE", "eth0")
VRID = env("VRID", "51")
# VIP 对外访问端口：业务连 VIP:VIP_PORT 访问当前主库。默认 3307（与节点默认端口一致）。
# 该端口会在生成节点时一并映射到容器内 3306，使任意主库节点上 VIP:VIP_PORT 均可达。
VIP_PORT = int(env("VIP_PORT", "3307"))


def get_mgmt_host():
    """管理端（管理 MySQL）对外可达地址。

    优先使用显式配置的 MGMT_MYSQL_HOST；若配置为回环/空/auto，则自动探测本机
    主网卡 IPv4 —— 因为你永远不知道管理端装在哪一台机器上。
    """
    h = (MGMT["host"] or "").strip()
    if h and h not in ("127.0.0.1", "localhost", "", "auto"):
        return h
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


# 管理器对外 IP（裸机节点实际看到的【连接来源 IP】）。
# 这是「运行管理器的服务端后台」的 IP —— 节点只允许该 IP 连入；
# 它与「管理端自建的后台 MySQL（MGMT_MYSQL_HOST，如 192.168.1.88）」是**两回事**：
# 管理库仅用于管理器自身存储，绝不作为集群节点、也绝不写入节点配置。
# 生产环境强烈建议显式指定（docker 部署填运行 docker 那台宿主机的对外 IP，
# 即节点看到的连接源 IP），避免 docker SNAT / 多网卡下自动探测到错误地址。
MANAGER_PUBLIC_IP = (env("MANAGER_PUBLIC_IP", "") or "").strip()

# 慢日志采集 / 观测中心开关。默认 False（暂停）：节点侧 mysql.slow_log 清空需 ha_monitor
# 持 DELETE 权限，未授权时每周期会报 1556 并重复采集刷日志；v2.1 起默认暂停，避免干扰。
# 如需开启采集，设置 SLOWLOG_ENABLED=1 并在节点以真实 root 跑 fix-node-creds.sql 授权 DELETE。
SLOWLOG_ENABLED = str(env("SLOWLOG_ENABLED", "0") or "0").strip() in ("1", "true", "yes", "on")


def get_manager_public_ip():
    """管理器对外 IP —— 裸机节点应允许此 IP 连接、并写入节点部署配置。

    这是「服务端后台（管理器应用）」的 IP，节点只允许该 IP 连入；
    它与「管理端后台 MySQL（MGMT_MYSQL_HOST）」无关 —— 管理库只供管理器自身
    存储，绝不能作为集群节点、也不应出现在任何节点配置里。

    优先用显式 MANAGER_PUBLIC_IP（生产强烈建议显式设置）；未设置时自动探测本机
    主网卡 IPv4 并打印告警提示显式配置（docker 桥接网络下探测到的是容器 IP，
    与节点看到的宿主机 IP 不同，此时必须显式设置 MANAGER_PUBLIC_IP，否则
    root@'<IP>' / ha_monitor@'<IP>' 授权会错位，导致管理器连不上节点）。
    """
    import logging
    if MANAGER_PUBLIC_IP and MANAGER_PUBLIC_IP not in ("127.0.0.1", "localhost", "auto"):
        return MANAGER_PUBLIC_IP
    _log = logging.getLogger("ha.config")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    _log.warning(
        "MANAGER_PUBLIC_IP 未显式设置，已自动探测为 %s。若以 docker 部署，节点看到的连接"
        "来源是宿主机 IP，请显式设置 MANAGER_PUBLIC_IP 为管理器宿主机的对外 IP，"
        "否则 root@'<IP>' / ha_monitor@'<IP>' 授权错位，管理器连不上节点。", ip)
    return ip
