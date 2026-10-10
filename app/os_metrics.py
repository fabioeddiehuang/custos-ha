"""节点 OS 指标采集（管理器侧拉取）。

填补 agentless 架构下「无 OS 级指标」的缺口：在每个 MySQL 节点宿主机上运行
`scripts/node_agent.py`（轻量 HTTP 服务），管理器定时拉取其 /stats 接口拿到真实
CPU/内存/磁盘占用，补进 dashboard。

完全可选：agent 不可达时静默降级（返回 ok=False + error），dashboard 仅不显示 OS 指标，
不影响 HA 主逻辑。
"""
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from .config import (
    NODE_AGENT_PORT, NODE_AGENT_TOKEN, NODE_AGENT_TIMEOUT, NODE_AGENT_CACHE_TTL,
)
from .db import query

_cache = {}          # {(cluster_id, ip): (ts, payload)}
_cache_lock = threading.Lock()


def _token_for(ip):
    """取访问该节点 agent 的令牌：优先「每节点独立 token」，缺失时回退全局配置。

    必须与心跳通道同源（agent_hub.node_token）：节点 compose 里注入的
    NODE_AGENT_TOKEN 是 nodes.agent_token（每节点独立随机值），若这里仍拿全局
    NODE_AGENT_TOKEN 去请求，agent 侧 _allow() 一律 401，表现为「心跳在线、
    但 OS 指标 / 节点日志统统显示 agent 不可达」。
    """
    try:
        from . import agent_hub
        return agent_hub.node_token(ip) or ""
    except Exception:
        return NODE_AGENT_TOKEN or ""


def _fetch(ip):
    """拉取单个节点 agent 的 /stats；失败返回含 error 的 dict。"""
    token = _token_for(ip)
    url = f"http://{ip}:{NODE_AGENT_PORT}/stats"
    if token:
        # 令牌是随机串，可能含 + / = 等字符，必须 urlencode
        # （否则 query 里的 + 会被解析成空格，agent 侧字符串比对失败）
        url += "?" + urllib.parse.urlencode({"token": token})
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    try:
        with urllib.request.urlopen(req, timeout=NODE_AGENT_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def fetch_node_stats(ip):
    """拉取节点 agent /stats 原始负载。

    供「配置待生效」自动核验使用：负载里含 agent / keepalived 等进程的启动时间
    （agent_started_at / keepalived_started_at），据此判断对应服务是否已在节点上重启。
    失败时返回含 error 的 dict。
    """
    return _fetch(ip)


def fetch_node_log(ip, name: str = "error", lines: int = 400, path: str = "",
                   timeout: int = 8):
    """通过节点 agent 的 /logs 读取节点宿主机上的日志文件（只读）。

    为什么需要这条通道：MariaDB 没有 mysql.error_log 表，管理器只能靠 SQL 的
    LOAD_FILE 读 @@log_error 指向的文件；但 mysqld 生成的 error.log 权限是 0660，
    LOAD_FILE 对 0660 文件恒返回 NULL（实测 660→NULL、644→有内容），且还受
    secure_file_priv 限制。节点 agent 以 root 直接读宿主机文件，绕开全部 SQL 侧限制。

    返回 agent 原始 dict：
      成功 {"ok": True, "path": <宿主机路径>, "lines": [...], "size": n, "truncated": bool}
      失败 {"ok": False, "error": "..."}
    """
    q = {"name": name, "lines": str(int(lines))}
    if path:
        q["path"] = path
    token = _token_for(ip)
    if token:
        q["token"] = token
    url = f"http://{ip}:{NODE_AGENT_PORT}/logs?" + urllib.parse.urlencode(q)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": f"HTTP {e.code}"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _node_ips(cluster_id):
    try:
        rows = query("SELECT ip FROM nodes WHERE cluster_id=%s", (cluster_id,))
        return [r["ip"] for r in rows if r.get("ip")]
    except Exception:
        return []


def get_cluster_os_metrics(cluster_id, force=False):
    """返回 [{ip, ok, data|error}, ...]，data 为 agent 的 /stats 内容。"""
    if not cluster_id:
        return []
    ips = _node_ips(cluster_id)
    now = time.time()
    out = []
    for ip in ips:
        key = (cluster_id, ip)
        cached = None
        with _cache_lock:
            if key in _cache:
                ts, payload = _cache[key]
                if not force and now - ts < NODE_AGENT_CACHE_TTL:
                    cached = payload
        if cached is None:
            data = _fetch(ip)
            cached = {"ip": ip, "ok": "error" not in data, "data": data if "error" not in data else None,
                      "error": data.get("error") if "error" in data else None}
            with _cache_lock:
                _cache[key] = (now, cached)
        else:
            cached = dict(cached)
            cached["ip"] = ip
        out.append(cached)
    return out


def get_node_os_metrics(cluster_id, ip, force=False):
    """返回单个节点的 OS 指标 dict（含 ok / data / error）。"""
    if not ip:
        return {"ip": ip, "ok": False, "error": "ip 为空"}
    now = time.time()
    key = (cluster_id, ip)
    with _cache_lock:
        if key in _cache and not force:
            ts, payload = _cache[key]
            if now - ts < NODE_AGENT_CACHE_TTL:
                return dict(payload)
    data = _fetch(ip)
    payload = {"ip": ip, "ok": "error" not in data,
               "data": data if "error" not in data else None,
               "error": data.get("error") if "error" in data else None}
    with _cache_lock:
        _cache[key] = (now, payload)
    return payload
