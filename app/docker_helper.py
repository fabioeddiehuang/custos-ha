"""通过宿主 /var/run/docker.sock 读取兄弟容器日志。

管理器自身运行在容器内，镜像未安装 docker CLI。直接经 Unix Socket 调用
Docker HTTP API，无需在镜像内安装任何额外二进制，也无需 docker 命令行。

用于「系统设置 -> 容器日志」读取管理端侧容器（keepalived / mysql-ha-manager /
ha-manager-monitor / mysql-ha-manager-mgmt 等）的日志，支持 since 增量以实现实时刷新。
"""
import os
import time
import json
import struct
import socket
import urllib.parse

SOCK = "/var/run/docker.sock"


def docker_available():
    return os.path.exists(SOCK)


def _http_get(path):
    """经 Unix Socket 发送一次 HTTP/1.1 GET，返回 (status_line, body_bytes)。"""
    if not docker_available():
        return None, b""
    req = (
        f"GET {path} HTTP/1.1\r\n"
        "Host: docker\r\n"
        "Accept: */*\r\n"
        "Connection: close\r\n\r\n"
    )
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(15)
    try:
        s.connect(SOCK)
        s.sendall(req.encode())
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    except Exception:
        return None, b""
    finally:
        try:
            s.close()
        except Exception:
            pass
    sep = data.find(b"\r\n\r\n")
    if sep < 0:
        return None, b""
    status = data[:data.find(b"\r\n")].decode(errors="replace")
    body = data[sep + 4:]
    return status, body


def list_containers():
    status, body = _http_get("/containers/json?all=1")
    if status is None or "200" not in status:
        return []
    try:
        return json.loads(body.decode("utf-8", "replace"))
    except Exception:
        return []


def container_logs(token, since_seconds=86400):
    """返回名称匹配 token 的容器日志文本（最近 since_seconds 秒）。无则返回 None。"""
    if not docker_available():
        return None
    token = (token or "").strip()
    if not token:
        return None
    # 查找名称匹配的容器（docker API 用容器名/id，compose 服务名需自行匹配）
    ctr_id = None
    for c in list_containers():
        names = [n.lstrip("/") for n in (c.get("Names") or [])]
        if token in names or any(token in n for n in names):
            ctr_id = c.get("Id")
            break
    if not ctr_id:
        return None
    try:
        since_ts = int(time.time()) - int(since_seconds)
    except (TypeError, ValueError):
        since_ts = int(time.time()) - 86400
    qs = urllib.parse.urlencode({"stdout": "1", "stderr": "1", "since": since_ts})
    status, body = _http_get(f"/containers/{ctr_id}/logs?{qs}")
    if status is None or "200" not in status:
        return None
    # 解码 multiplexed stream（Docker >= 1.13：每帧 8 字节头 [类型,0,0,0,长度(4BE)]）
    out = []
    i = 0
    n = len(body)
    while i + 8 <= n:
        size = struct.unpack(">I", body[i + 4:i + 8])[0]
        i += 8
        if i + size > n:
            size = n - i
        out.append(body[i:i + size].decode("utf-8", "replace").rstrip("\n"))
        i += size
    return "\n".join(out)
