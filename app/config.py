import os
import socket
import secrets
import hashlib
import hmac
import logging


def env(name, default=None):
    return os.environ.get(name, default)


def _resolve_app_version():
    """版本号唯一来源：.env 的 VERSION=。

    优先级：环境变量 VERSION（docker compose 已从 .env 注入）→ 仓库根 .env 文件 → 空串。
    注意：v2.2.20 起 app/VERSION 文件已删除废弃，**任何代码都不要再读该文件**，
    否则会静默回退到错误版本（曾导致节点 keepalived 镜像 tag 取到 v2.0.1）。
    """
    raw = (env("VERSION") or "").strip()
    if raw:
        return raw
    try:
        env_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
        )
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("VERSION="):
                    return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return ""


APP_VERSION = _resolve_app_version()

# 节点账号 host 策略（安全开关）
#   legacy   : 仅创建 @'%'
#   additive : 追加 @'<网段>.%'，同时**保留** @'%' 兜底（灰度第一步，最安全）
#   segment  : 仅 @'<网段>.%'（灰度终态）。注意：本模式只停止维护 @'%'，
#              **不会自动 DROP**，故随时可改回 additive 回滚，确认无误后再人工清理。
# 默认 additive：开启账号 host 收敛（保留 @'%' 兜底，绝不破坏现有连接），
# 消除「任意来源可连 ha_monitor/replication」的暴露面。改回 legacy 可关闭。
ACCOUNT_HOST_MODE = (env("ACCOUNT_HOST_MODE", "additive") or "additive").strip().lower()


def segment_of(ip):
    """由 IPv4 推导 C 段通配：192.168.1.100 -> 192.168.1.%。

    非 IPv4 或无法推导时返回 None —— 调用方必须回退 @'%'，绝不能猜测网段：
    猜错会让管理器连不上所有节点，比保留 % 危险得多。
    """
    try:
        parts = str(ip or "").strip().split(".")
        if len(parts) != 4 or not all(x.isdigit() for x in parts):
            return None
        return ".".join(parts[:3]) + ".%"
    except Exception:
        return None


# 用户角色：admin=管理员（全部权限）；viewer=只读（任何写操作一律拒绝）
ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"


def rand_secret(n=16):
    """生成 URL 安全的随机密钥（用于自动生成账号密码）。"""
    return secrets.token_urlsafe(n)


# 管理 MySQL（独立容器，存配置/拓扑/切换历史/账号）
# 默认库用户/密码/库名统一为 "ha"（compose 也以 ha 作为默认值）
MGMT = dict(
    host=env("MGMT_MYSQL_HOST", "192.0.2.88"),
    port=int(env("MGMT_MYSQL_PORT", 3306)),
    user=env("MGMT_MYSQL_USER", "ha"),
    password=env("MGMT_MYSQL_PASS", "ha"),
    db=env("MGMT_MYSQL_DB", "ha"),
)

WEB_PORT = int(env("WEB_PORT", 28888))
WEB_USER = env("WEB_USER", "admin")
WEB_PASS = env("WEB_PASS", "admin")  # 为空则首次启动随机生成并打印
DEFAULT_SECRET_KEY = "change-me-in-production"
# 密钥持久化目录（建议挂卷，使容器重建后仍复用同一密钥，避免会话全部失效）。
DATA_DIR = env("DATA_DIR", "/app/data")
SECRET_KEY_FILE = os.path.join(DATA_DIR, "secret_key")


def _resolve_secret_key():
    """解析 SECRET_KEY：

    - 显式设置了不安全值（默认值 'change-me-in-production' 或长度<16）→ 明确拒绝，不静默覆盖。
    - 显式设置了强密钥（.env / docker-compose 的 SECRET_KEY）→ 直接使用。
    - 留空/未设置 → 优先复用已持久化的密钥文件（容器重建不丢失会话）；
      否则生成随机密钥并持久化到 DATA_DIR/secret_key，并打印日志提示。
    """
    raw = env("SECRET_KEY", "")
    if raw:
        if raw == DEFAULT_SECRET_KEY or len(raw) < 16:
            raise RuntimeError(
                "显式设置的 SECRET_KEY 为默认值 'change-me-in-production' 或长度不足 16，"
                "存在会话伪造风险。请改为强随机值，或删除该设置让系统自动生成。"
            )
        return raw
    # 未设置：尝试复用已持久化的密钥
    try:
        if os.path.exists(SECRET_KEY_FILE):
            with open(SECRET_KEY_FILE, "r") as f:
                k = f.read().strip()
            if k and len(k) >= 16:
                return k
    except Exception:
        pass
    # 生成并持久化
    k = secrets.token_hex(32)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(SECRET_KEY_FILE, "w") as f:
            f.write(k)
        os.chmod(SECRET_KEY_FILE, 0o600)
        import sys
        print(
            "[config] SECRET_KEY 未显式设置，已自动生成随机密钥并持久化到 %s（容器重建不丢失）；"
            "如需固定，可在 .env 设置 SECRET_KEY=<强随机值>。" % SECRET_KEY_FILE,
            file=sys.stderr,
        )
    except Exception as e:
        logging.getLogger("config").warning(
            "无法持久化 SECRET_KEY 到 %s（%s），已使用内存随机密钥，重启后会话将失效。", SECRET_KEY_FILE, e
        )
    return k


SECRET_KEY = _resolve_secret_key()


def validate_config():
    """启动期安全校验：SECRET_KEY 必须非空且长度≥16（留空会自动生成随机密钥并持久化）。"""
    if not SECRET_KEY or len(SECRET_KEY) < 16:
        raise RuntimeError("SECRET_KEY 生成/配置失败（长度不足 16），启动中止。")


def hash_pwd(p):
    """加盐密码哈希，格式 `salt$hash`（salt 为随机十六进制）。

    校验用 verify_pwd；旧格式（无 `$` 分隔）按 sha256(p+SECRET_KEY) 兼容，
    以便 SECRET_KEY 不变时存量账号可平滑过渡。
    """
    salt = secrets.token_hex(16)
    digest = hashlib.sha256((salt + SECRET_KEY + p).encode()).hexdigest()
    return f"{salt}${digest}"


def verify_pwd(stored, p):
    if not stored:
        return False
    if "$" in stored:
        salt, h = stored.split("$", 1)
        digest = hashlib.sha256((salt + SECRET_KEY + p).encode()).hexdigest()
        return hmac.compare_digest(h, digest)
    # 兼容旧格式（无盐）：sha256(p + SECRET_KEY)
    return hmac.compare_digest(
        stored, hashlib.sha256((p + SECRET_KEY).encode()).hexdigest()
    )

# 检测与切换
CHECK_INTERVAL = int(env("CHECK_INTERVAL", 2))   # 探活间隔(秒)
FAIL_THRESHOLD = int(env("FAIL_THRESHOLD", 2))    # 连续失败次数阈值
# 故障切换冷却（秒）：同集群两次切换最小间隔，防止频繁切换产生抖动（split-brain 风险）。
# 注意：该冷却【不影响】主库失联的紧急自动切换（那由 CHECK_INTERVAL/FAIL_THRESHOLD 驱动），
# 仅约束 ①手动切换 与 ②「高延迟/压力」自动切换（后者默认关闭）。故 120s 已足够安全。
FAILOVER_COOLDOWN_SEC = int(env("FAILOVER_COOLDOWN_SEC", 120))
# 半同步复制开关（"1" 启用）。启用后主库提交等待至少一个从库 ACK，压缩故障切换数据丢失窗口。
# 默认关闭：避免在未确认插件可用性的存量集群上强行开启导致复制异常；需显式开启并经测试。
SEMI_SYNC = env("SEMI_SYNC", "0")
# 单节点探活超时（秒）：主库服务停止时会立即收到 RST（连接拒绝很快返回），
# 仅当主机断电/网络黑洞时才走到该超时。调小可加快「主机失联」判定（配合 FAIL_THRESHOLD）。
PROBE_TIMEOUT = int(env("PROBE_TIMEOUT", 2))
# 注：主库失联切换延迟 ≈ FAIL_THRESHOLD×PROBE_TIMEOUT（当前 2×2=4s），已为安全下限，
# 再调小易误切。如确需更快可临时设环境变量覆盖，不建议写死更小值。

# 节点「最后心跳」(hosts.last_seen) 的最小落库间隔（秒）。
# 稳态下探活结果全部不变，唯独 last_seen=NOW() 每次都变，是空闲时管理库持续写盘的主因。
# 设为 0 = 完全不落库 last_seen（真正零写入，代价是页面「最后心跳」不再刷新）。
HEARTBEAT_INTERVAL = int(env("HEARTBEAT_INTERVAL", 30))

# 指标快照 / 配置漂移巡检的采集间隔（秒）。0 = 沿用原「按周期倍数」逻辑。
# 这两个采集会向管理库 INSERT 历史行；空闲时值与上次完全一致则不写（见各模块内的变更检测）。
METRICS_INTERVAL = int(env("METRICS_INTERVAL", 60))
DRIFT_INTERVAL = int(env("DRIFT_INTERVAL", 300))

# 节点 OS 指标采集 agent（填补 agentless 架构下「无 OS 级指标」的缺口）。
# 在每个 MySQL 节点宿主机上运行 `scripts/node_agent.py`（轻量 HTTP 服务，stdlib-only），
# 管理器定时拉取其 /stats 接口拿到真实 CPU/内存/磁盘占用，补进 dashboard。
# 该能力完全可选：agent 不可达时管理器静默降级（dashboard 仅不显示 OS 指标，不影响 HA 主逻辑）。
NODE_AGENT_PORT = int(env("NODE_AGENT_PORT", 28889))        # 节点 agent 监听端口
NODE_AGENT_TOKEN = (env("NODE_AGENT_TOKEN", "") or "").strip()  # 可选：Bearer/查询参数校验
NODE_AGENT_TIMEOUT = float(env("NODE_AGENT_TIMEOUT", 3.0))  # 单次拉取超时（秒）
NODE_AGENT_CACHE_TTL = int(env("NODE_AGENT_CACHE_TTL", 15))  # 拉取结果缓存秒数（避免频繁打节点）

# 节点生成目录（一键启动 compose 落地目录；建议挂卷持久化）
NODES_DIR = env("NODES_DIR", "/app/generated_nodes")

# 节点生成目录在宿主机上的「显示路径」：docker-compose 将项目根下的
# ./generated_nodes 挂载到容器内的 /app/generated_nodes，因此节点文件在宿主机上的
# 实际落盘路径为 <项目根>/generated_nodes/<ip>。每个 <ip> 子目录是自包含的部署单元
# （docker-compose.yaml / init.sql / keepalived/ 均相对本目录，MySQL 数据落在 ./MySQL-data），
# 可直接拷贝到目标主机的 /home/docker/<ip>/ 运行，路径不会越级。
# 该值用于 Web 展示与「到目标主机执行」命令，可通过 NODES_HOST_BASE 覆盖（如项目目录改名时）。
# 仓库目录改名为 custos-ha 后此处默认值同步更新；也可在 docker-compose.yml 里显式设置
# NODES_HOST_BASE 覆盖，无需改代码。
NODES_HOST_BASE = env("NODES_HOST_BASE", "custos-ha/generated_nodes")

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
# 它与「管理端自建的后台 MySQL（MGMT_MYSQL_HOST，如 192.0.2.88）」是**两回事**：
# 管理库仅用于管理器自身存储，绝不作为集群节点、也绝不写入节点配置。
# 生产环境强烈建议显式指定（docker 部署填运行 docker 那台宿主机的对外 IP，
# 即节点看到的连接源 IP），避免 docker SNAT / 多网卡下自动探测到错误地址。
MANAGER_PUBLIC_IP = (env("MANAGER_PUBLIC_IP", "") or "").strip()

# 慢日志采集 / 观测中心开关。默认 False（暂停）：节点侧 mysql.slow_log 清空需 ha_monitor
# 持 DELETE 权限，未授权时每周期会报 1556 并重复采集刷日志；v2.1 起默认暂停，避免干扰。
# 如需开启采集，设置 SLOWLOG_ENABLED=1 并在节点以真实 root 跑 fix-node-creds.sql 授权 DELETE。
SLOWLOG_ENABLED = str(env("SLOWLOG_ENABLED", "0") or "0").strip() in ("1", "true", "yes", "on")


def get_manager_public_ip_source():
    """返回 (ip, source)，source ∈ {'env','db','auto'}。

    【为什么需要 source】此前 get_manager_public_ip() 只看环境变量，页面「系统设置 →
    管理器内网 IP」写入 monitor_settings 的值**从未被读取**，导致「填不填都无差异」。
    现在明确三级优先级：环境变量（部署态事实）> 页面设置 > 自动探测，
    并把「当前值来自哪一级」暴露给页面，避免运维以为页面改了却不生效。
    """
    if MANAGER_PUBLIC_IP and MANAGER_PUBLIC_IP not in ("127.0.0.1", "localhost", "auto"):
        return MANAGER_PUBLIC_IP, "env"
    # 页面设置（仅当环境变量未显式指定时生效）
    try:
        from .db import get_setting  # 延迟导入：db 依赖 config，避免导入期循环
        v = (get_setting("MANAGER_PUBLIC_IP", "") or "").strip()
        if v and v not in ("127.0.0.1", "localhost", "auto"):
            return v, "db"
    except Exception:
        pass
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip, "auto"


def get_manager_public_ip():
    """管理器对外 IP —— 裸机节点应允许此 IP 连接、并写入节点部署配置。

    这是「服务端后台（管理器应用）」的 IP，节点只允许该 IP 连入；
    它与「管理端后台 MySQL（MGMT_MYSQL_HOST）」无关 —— 管理库只供管理器自身
    存储，绝不能作为集群节点、也不应出现在任何节点配置里。

    优先级：环境变量 MANAGER_PUBLIC_IP > 页面设置(monitor_settings) > 自动探测。
    自动探测仅作兜底并打印告警（docker 桥接网络下探测到的是容器 IP，与节点看到的
    宿主机 IP 不同，此时必须显式设置，否则 root@'<IP>' / ha_monitor@'<IP>'
    授权会错位，导致管理器连不上节点）。
    """
    import logging
    ip, source = get_manager_public_ip_source()
    if source == "env":
        return ip
    if source == "db":
        return ip
    _log = logging.getLogger("ha.config")
    _log.warning(
        "MANAGER_PUBLIC_IP 未显式设置，已自动探测为 %s。若以 docker 部署，节点看到的连接"
        "来源是宿主机 IP，请显式设置 MANAGER_PUBLIC_IP 为管理器宿主机的对外 IP，"
        "否则 root@'<IP>' / ha_monitor@'<IP>' 授权错位，管理器连不上节点。", ip)
    return ip


def get_manager_docker_subnet_wildcard():
    """探测管理器容器所在 docker 网络的子网通配符（如 '172.18.%'）。

    用途：让「同机」节点（裸机 MariaDB / Docker 节点）允许管理器容器的连接来源
    （docker 网关或容器 IP，均位于该子网），实现 c/s 同机；部署到任意主机时
    自动适配，无需硬编码网段（解决「换台机器部署又不通」的问题）。

    - 仅当管理器确在容器内（/.dockerenv 存在）才返回；裸机部署返回 None。
    - 取本容器出口 IP 前两段拼 'a.b.%'：docker 默认 /16（或更小），网关与容器
      IP 都在 'a.b.0.0/16' 内，通配前两段即可覆盖同机来源。
    调用方据此在节点 root 授权中追加 root@'<该通配符>'，与 root@'<MANAGER_PUBLIC_IP>'
    形成「双白名单」：跨机走真实 IP，同机走 docker 子网。
    """
    if not os.path.exists("/.dockerenv"):
        return None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        return None
    finally:
        s.close()
    parts = (ip or "").split(".")
    if len(parts) != 4:
        return None
    return f"{parts[0]}.{parts[1]}.%"
