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
import urllib.request
import urllib.error

from .config import (
    NODE_AGENT_PORT, NODE_AGENT_TOKEN, NODE_AGENT_TIMEOUT, NODE_AGENT_CACHE_TTL,
)
from .db import query

_cache = {}          # {(cluster_id, ip): (ts, payload)}
_cache_lock = threading.Lock()


def _fetch(ip):
    """拉取单个节点 agent 的 /stats；失败返回含 error 的 dict。"""
    url = f"http://{ip}:{NODE_AGENT_PORT}/stats"
    if NODE_AGENT_TOKEN:
        url += f"?token={NODE_AGENT_TOKEN}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {NODE_AGENT_TOKEN}"} if NODE_AGENT_TOKEN else {})
    try:
        with urllib.request.urlopen(req, timeout=NODE_AGENT_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


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
