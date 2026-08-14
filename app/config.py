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
CHECK_INTERVAL = int(env("CHECK_INTERVAL", 3))   # 探活间隔(秒)
FAIL_THRESHOLD = int(env("FAIL_THRESHOLD", 2))    # 连续失败次数阈值
# 单节点探活超时（秒）：主库服务停止时会立即收到 RST（连接拒绝很快返回），
# 仅当主机断电/网络黑洞时才走到该超时。调小可加快「主机失联」判定（配合 FAIL_THRESHOLD）。
PROBE_TIMEOUT = int(env("PROBE_TIMEOUT", 3))

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
