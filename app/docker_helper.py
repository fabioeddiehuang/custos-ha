"""通过宿主 /var/run/docker.sock 读取兄弟容器日志。

管理器自身运行在容器内，镜像未安装 docker CLI。直接经 Unix Socket 调用
Docker HTTP API，无需在镜像内安装任何额外二进制，也无需 docker 命令行。

用于「系统设置 -> 容器日志」读取管理端侧容器（keepalived / custos /
custos-monitor / custos-mgmt 等）的日志，支持 since 增量以实现实时刷新。
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


def _dechunk(data):
    """解码 HTTP chunked 传输编码。

    Docker API 对 /containers/json、/containers/{id}/logs 等接口普遍使用
    Transfer-Encoding: chunked。不解码的话：JSON 解析失败（表现为「查不到容器」）、
    日志正文混入 chunk 长度前缀（表现为「读不到日志」）。
    """
    out = bytearray()
    i, n = 0, len(data)
    while i < n:
        j = data.find(b"\r\n", i)
        if j < 0:
            break
        line = data[i:j].strip()
        semi = line.find(b";")          # 忽略 chunk 扩展
        if semi >= 0:
            line = line[:semi]
        try:
            size = int(line, 16)
        except ValueError:
            break
        i = j + 2
        if size == 0:                   # 结束块
            break
        out += data[i:i + size]
        i += size + 2                   # 跳过数据后的 CRLF
    return bytes(out)


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
    head = data[:sep]
    status = head[:head.find(b"\r\n")].decode(errors="replace")
    body = data[sep + 4:]
    if b"chunked" in head.lower():
        body = _dechunk(body)
    return status, body


def list_containers():
    status, body = _http_get("/containers/json?all=1")
    if status is None or "200" not in status:
        return []
    try:
        return json.loads(body.decode("utf-8", "replace"))
    except Exception:
        return []


def _http_post(path, body):
    """经 Unix Socket 发送一次 HTTP/1.1 POST，返回 (status_line, body_bytes)。"""
    if not docker_available():
        return None, b""
    req = (
        f"POST {path} HTTP/1.1\r\n"
        "Host: docker\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    )
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(15)
    try:
        s.connect(SOCK)
        s.sendall(req.encode() + body)
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
    head = data[:sep]
    status = head[:head.find(b"\r\n")].decode(errors="replace")
    body = data[sep + 4:]
    if b"chunked" in head.lower():
        body = _dechunk(body)
    return status, body


def _decode_mux(body):
    """解码 Docker 多路复用流（每帧 8 字节头）。"""
    out, i, n = [], 0, len(body)
    while i + 8 <= n:
        size = struct.unpack(">I", body[i + 4:i + 8])[0]
        i += 8
        if i + size > n:
            size = n - i
        out.append(body[i:i + size].decode("utf-8", "replace"))
        i += size
    return "".join(out)


def _docker_exec(ctr_id, cmd):
    """在容器内执行命令并返回合并输出（stdout+stderr）。"""
    body = json.dumps({"Cmd": ["sh", "-c", cmd], "AttachStdout": True,
                       "AttachStderr": True, "Tty": False}).encode()
    status, resp = _http_post(f"/containers/{ctr_id}/exec", body)
    if status is None or "200" not in status:
        return ""
    try:
        j = json.loads(resp.decode("utf-8", "replace"))
        exec_id = j.get("Id")
    except Exception:
        return ""
    if not exec_id:
        return ""
    _, out = _http_post(f"/exec/{exec_id}/start",
                        json.dumps({"Detach": False, "Tty": False}).encode())
    return _decode_mux(out)


# 兜底：容器把日志写进文件而非 stdout 时，读取这些常见路径
_LOG_FILES_CMD = (
    "for f in /app/data/logs/*.log /var/log/keepalived_chk.log "
    "/var/log/keepalived.log /var/log/messages /var/log/syslog; do "
    "if [ -f \"$f\" ]; then echo \"===== $f =====\"; tail -n 1500 \"$f\"; fi; done"
)


def container_logs_by_id(ctr_id, since_seconds=86400, file_fallback=False):
    """按容器 ID 读取日志文本。

    返回约定：
      - None  -> 容器不存在或 Docker API 不可用（调用方提示「本机无该容器」）
      - ""    -> 容器存在但时间窗内无输出（调用方提示「暂无日志」）
      - 文本  -> 日志内容
    """
    if not ctr_id or not docker_available():
        return None
    try:
        since_ts = int(time.time()) - int(since_seconds)
    except (TypeError, ValueError):
        since_ts = int(time.time()) - 86400
    qs = urllib.parse.urlencode({"stdout": "1", "stderr": "1", "since": since_ts})
    status, body = _http_get(f"/containers/{ctr_id}/logs?{qs}")
    if status is None or "200" not in status:
        return None
    logs = _decode_mux(body)
    if logs.strip():
        return logs
    if file_fallback:
        try:
            out = _docker_exec(ctr_id, _LOG_FILES_CMD)
            if out and out.strip():
                return out
        except Exception:
            pass
    return ""


def find_container(token):
    """按容器名 / ID 前缀匹配容器，返回 (id, name)；无匹配返回 (None, None)。"""
    token = (token or "").strip()
    if not token:
        return None, None
    for c in list_containers():
        names = [n.lstrip("/") for n in (c.get("Names") or [])]
        cid = c.get("Id") or ""
        if token in names or (cid and cid.startswith(token)) \
                or any(token in n for n in names):
            return cid, (names[0] if names else cid[:12])
    return None, None


def own_container_id():
    """返回本容器自身的容器 ID。

    Docker 默认把容器 hostname 设为容器 ID 前 12 位，据此定位自身，
    避免依赖 compose 项目名（不同部署目录生成的容器名不同）。
    """
    if not docker_available():
        return None
    try:
        host = socket.gethostname()
    except Exception:
        return None
    if not host:
        return None
    for c in list_containers():
        cid = c.get("Id") or ""
        if cid and (cid.startswith(host) or host.startswith(cid[:12])):
            return cid
    return None


def list_units():
    """列出宿主上所有容器（含已停止），供「容器日志」下拉动态选择。"""
    out = []
    for c in list_containers():
        names = [n.lstrip("/") for n in (c.get("Names") or [])]
        out.append({
            "id": (c.get("Id") or "")[:12],
            "name": names[0] if names else (c.get("Id") or "")[:12],
            "image": c.get("Image") or "",
            "state": c.get("State") or "",
            "status": c.get("Status") or "",
        })
    return out


def container_logs(token, since_seconds=86400, file_fallback=True):
    """返回名称匹配 token 的容器日志文本（最近 since_seconds 秒）。

    无匹配容器返回 None；容器存在但 stdout/stderr 为空时：
      - file_fallback=True 则兜底读取容器内常见日志文件（tail）；
      - 否则返回空串 ""。
    """
    if not docker_available():
        return None
    ctr_id, _ = find_container(token)
    if not ctr_id:
        return None
    return container_logs_by_id(ctr_id, since_seconds, file_fallback=file_fallback)
