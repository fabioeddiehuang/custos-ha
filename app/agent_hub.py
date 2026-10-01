"""agent_hub.py — 节点 agent 心跳与指令通道（v2.5.0 F5）。

为什么需要它
------------
现状 agent 是**纯被动**的：只能由管理器主动连 node:28889 拉指标/日志/执行。
这带来两个硬伤：
  1. 管理器停机期间，节点上发生的任何事都无人知晓，恢复后也没有补做机制；
  2. 新节点/接管节点没有 agent 时，管理器无从下手（只能人工上机）。

改为 **agent 心跳出向**（agent → manager）：agent 定期上报，管理器在心跳响应里
**带回待执行指令**。这样：
  - 只需节点能出网到管理器（NAT / 单向防火墙都能过）；
  - 节点上线即自注册，不必手工登记；
  - 指令落库 + 过期时间 + 分批下发，管理器停机恢复后不会惊群。

安全模型（关键）
----------------
agent 是 root + `chroot /host` + `docker.sock`，等于节点上的**上帝权限**。
因此：
  1. 心跳必须带 token 校验（每节点独立 token，缺失时回退全局）；
  2. 指令只允许**白名单动词**，参数严格校验——绝不放任任意命令；
  3. 通用 `exec` 是逃生舱，默认关闭（agent 侧 `NODE_AGENT_ALLOW_SHELL=1` 才放行）；
  4. 所有指令与执行结果落库可审计。
"""
import json
import logging
import time
from datetime import datetime, timedelta

from .db import query, execute
from .config import NODE_AGENT_TOKEN

log = logging.getLogger(__name__)

# 允许的指令动词（白名单）。agent 侧对同一份白名单做二次校验，
# 两端一致才能执行——服务端被篡改也不能让节点跑任意命令。
V_UPDATE_FILES = "update_files"
V_RESTART_SERVICE = "restart_service"
V_SET_CREDENTIAL = "set_credential"
V_RUN_BUILTIN = "run_builtin_script"
V_STOP_SERVICE = "stop_service"        # F8 滚动升级用
V_START_SERVICE = "start_service"
V_WIPE_DATA = "wipe_data"              # L3 纯净重建：清空数据目录（高危）
V_EXEC = "exec"                        # 逃生舱：默认关闭

ALLOWED_VERBS = (V_UPDATE_FILES, V_RESTART_SERVICE, V_SET_CREDENTIAL,
                 V_RUN_BUILTIN, V_STOP_SERVICE, V_START_SERVICE, V_WIPE_DATA,
                 V_EXEC)

# 允许被 run_builtin_script 调用的内置脚本（随部署单元下发，不接受任意内容）
ALLOWED_SCRIPTS = ("update-node.sh", "prepare-logs.sh")

# 允许被 restart/stop/start 操作的服务
ALLOWED_SERVICES = ("keepalived", "agent", "mariadb")

# 允许被 wipe_data 清理的目标（只能是数据卷/数据目录，绝不可能是系统路径）
ALLOWED_WIPE_TARGETS = ("mariadb_data",)

# 每次心跳最多下发几条指令：管理器长时间停机后恢复，靠它限流防惊群
DISPATCH_LIMIT_PER_HEARTBEAT = 3

# 指令默认有效期（秒）。过期不再下发，避免"几天前的指令突然被执行"。
DEFAULT_CMD_TTL = 3600

# 单条指令载荷上限（字节）。下发节点变更文件时整包 base64 后写入 payload，
# 列已扩容为 LONGTEXT，但仍需挡住异常大的载荷（避免管理库被撑爆 / 复制卡顿）。
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024


def _now():
    return datetime.now()


def node_token(ip):
    """取节点 agent token：优先节点独立 token，缺失时回退全局配置。"""
    try:
        rows = query("SELECT agent_token FROM nodes WHERE ip=%s", (ip,))
        t = (rows[0].get("agent_token") if rows else "") or ""
        if t:
            return t
    except Exception:
        pass       # agent_token 列可能尚未迁移：忽略，走全局回退
    return NODE_AGENT_TOKEN or ""


def verify_agent(ip, token):
    """校验 agent 身份。

    用 compare_digest 防时序侧信道；节点无 token 且全局也未配置时**拒绝**
    （不能因为"没配"就放行，那等于任何人都可伪装成节点下发指令）。
    """
    import hmac
    expected = node_token(ip)
    if not expected:
        return False
    return hmac.compare_digest(str(expected), str(token or ""))


# ------------------------------------------------------------------ 指令入队

def enqueue(ip, verb, payload=None, actor="", ttl_sec=DEFAULT_CMD_TTL,
            dedupe_key=""):
    """下发一条指令（只入队，由心跳拉取）。

    dedupe_key：同一 key 且仍 pending/sent 的指令不会重复入队，
    避免"点两次按钮下发两遍"或"配置漂移反复下发同一批文件"。
    """
    if verb not in ALLOWED_VERBS:
        raise ValueError("不允许的指令动词：%s（白名单：%s）"
                         % (verb, "/".join(ALLOWED_VERBS)))
    ip = str(ip).strip()
    if not ip:
        raise ValueError("节点 IP 不能为空")
    if verb == V_EXEC and not _exec_allowed():
        raise ValueError("exec 逃生舱未开启：需在 agent 侧设置 NODE_AGENT_ALLOW_SHELL=1")

    if dedupe_key:
        try:
            dup = query("SELECT id FROM agent_commands WHERE ip=%s AND dedupe_key=%s "
                        "AND status IN ('pending','sent') LIMIT 1", (ip, dedupe_key))
            if dup:
                return dup[0]["id"]
        except Exception:
            pass

    exp = _now() + timedelta(seconds=int(ttl_sec or DEFAULT_CMD_TTL))
    _body = json.dumps(payload or {}, ensure_ascii=False)
    # 体积保护：单条指令设上限，超了宁可明确报错，也不要把几十 MB 塞进管理库。
    if len(_body.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise ValueError(
            "指令载荷 %.1f MB 超过上限 %d MB（verb=%s ip=%s）：请减少单次下发的文件数，"
            "或分批下发" % (len(_body.encode("utf-8")) / 1048576.0,
                            MAX_PAYLOAD_BYTES // 1048576, verb, ip))
    try:
        new_id = execute(
            "INSERT INTO agent_commands (ip, verb, payload, actor, expires_at, dedupe_key) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (ip, verb, _body, actor or "", exp, dedupe_key or ""),
        )
    except Exception as e:
        # 1406 = Data too long：payload 仍是 TEXT(64KB) 的老库会遇到。给出可执行指引，
        # 而不是让调用方只看到一句 MySQL 原文（自动下发就此静默失败）。
        if "1406" in str(e):
            raise RuntimeError(
                "指令写入失败：agent_commands.payload 列仍是 TEXT(64KB)，装不下本次载荷。"
                "请重启服务端触发自动扩容（init_db 会把它 ALTER 成 LONGTEXT）。原始错误：%s" % e)
        raise
    return new_id


def _exec_allowed():
    """exec 逃生舱是否开启（开关在 agent 侧，服务端只做记录性判断）。"""
    import os
    return os.environ.get("NODE_AGENT_ALLOW_SHELL", "") == "1"


def expire_stale():
    """把过期仍未执行的指令标记为 expired（不再下发）。"""
    try:
        execute("UPDATE agent_commands SET status='expired' "
                "WHERE status='pending' AND expires_at IS NOT NULL AND expires_at < NOW()")
    except Exception as e:
        log.debug("清理过期指令失败：%s", e)


# ------------------------------------------------------------------ 心跳与分发

def on_heartbeat(ip, payload=None):
    """处理一次心跳：更新节点在线状态，并返回待执行指令。

    返回 {"ok":..., "commands":[{id, verb, payload}], "server_time":...}
    指令被下发时标记为 sent（agent 执行后回报结果）。
    """
    payload = payload or {}
    ip = str(ip).strip()
    if not ip:
        return {"ok": False, "error": "缺少节点 IP"}

    # 节点上线即自注册：状态置 online，并记录最近心跳时间
    try:
        execute("UPDATE nodes SET deploy_state='online', last_ready_at=NOW() WHERE ip=%s",
                (ip,))
    except Exception as e:
        log.debug("更新节点在线状态失败（字段可能未迁移）：%s", e)

    expire_stale()
    cmds = []
    try:
        rows = query(
            "SELECT id, verb, payload FROM agent_commands "
            "WHERE ip=%s AND status='pending' "
            "AND (expires_at IS NULL OR expires_at > NOW()) "
            "ORDER BY id LIMIT %s",
            (ip, int(DISPATCH_LIMIT_PER_HEARTBEAT)),
        )
        for r in rows:
            try:
                pl = json.loads(r.get("payload") or "{}")
            except Exception:
                pl = {}
            cmds.append({"id": r["id"], "verb": r["verb"], "payload": pl})
        if rows:
            ids = [int(r["id"]) for r in rows]
            execute(
                "UPDATE agent_commands SET status='sent', sent_at=NOW() "
                "WHERE id IN (%s)" % ",".join(["%s"] * len(ids)), ids)
    except Exception as e:
        log.debug("拉取待发指令失败：%s", e)

    return {"ok": True, "commands": cmds, "server_time": int(time.time())}


def on_result(ip, cmd_id, ok, message="", payload=None):
    """agent 执行完指令后回报结果。"""
    try:
        execute("UPDATE agent_commands SET status=%s, done_at=NOW(), result=%s "
                "WHERE id=%s AND ip=%s",
                ("done" if ok else "failed",
                 (message or "")[:2000], int(cmd_id), ip))
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True}


def list_commands(ip=None, limit=30):
    """查询指令（页面/排障用）。"""
    try:
        if ip:
            return query("SELECT id, ip, verb, status, actor, created_at, sent_at, "
                         "done_at, expires_at, result FROM agent_commands "
                         "WHERE ip=%s ORDER BY id DESC LIMIT %s", (ip, int(limit)))
        return query("SELECT id, ip, verb, status, actor, created_at, sent_at, "
                     "done_at, expires_at, result FROM agent_commands "
                     "ORDER BY id DESC LIMIT %s", (int(limit),))
    except Exception:
        return []


def pending_count(ip=None):
    try:
        if ip:
            rows = query("SELECT COUNT(*) c FROM agent_commands "
                         "WHERE ip=%s AND status IN ('pending','sent')", (ip,))
        else:
            rows = query("SELECT COUNT(*) c FROM agent_commands "
                         "WHERE status IN ('pending','sent')")
        return int((rows[0] or {}).get("c") or 0)
    except Exception:
        return 0
