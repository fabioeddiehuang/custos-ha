#!/usr/bin/env python3
# Custos 节点 OS 指标采集 agent
#
# 背景：Custos-HA 为 agentless 架构（管理器不 SSH/不装 agent 到节点），dashboard
# 「主库状态」只能用 MySQL 内部指标近似（连接数%/缓冲池%/数据量/QPS），拿不到真实
# CPU/内存/磁盘占用。本 agent 补这个缺口：在每个 MySQL 节点**宿主机**上跑一个轻量
# HTTP 服务，管理器定时拉取 /stats 即可拿到真实 OS 指标。
#
# 设计原则：
#   - 纯标准库，无第三方依赖（节点缺网也能跑）；若环境有 psutil 则优先用（更准）。
#   - 默认只暴露只读采集；额外的 POST /exec（受同一 NODE_AGENT_TOKEN 保护）仅用于
#     迁移物理通道在宿主执行 docker/mariabackup，属可控的受信写端点，非公开能力。
#   - 完全可选：管理器侧 agent 不可达时静默降级，不影响 HA 主逻辑。
#
# 用法：
#   python3 node_agent.py                       # 默认 0.0.0.0:28889
#   python3 node_agent.py --port 28889 --bind 0.0.0.0
#   NODE_AGENT_TOKEN=xxxx python3 node_agent.py # 开启令牌校验（Bearer 或 ?token=）
#   python3 node_agent.py --disks /,/data       # 额外汇报这些挂载点的磁盘占用
#
# systemd 部署见同目录 node-agent.service。

import argparse
import glob
import json
import logging
import os
import socket
import subprocess
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import psutil  # 可选：更准；缺失则回退 /proc
except Exception:
    psutil = None


# ---------------------------------------------------------------------------
# 指标采集（带缓存，避免每次请求都读 /proc 造成抖动）
# ---------------------------------------------------------------------------
class Collector:
    def __init__(self, extra_disks=None, root="/"):
        self.extra_disks = [d.strip() for d in (extra_disks or []) if d.strip()]
        # root 为「要统计磁盘占用的根挂载点」。裸机默认 "/"；
        # 容器内部署时把宿主机 / 挂载到 /host 并传 --root /host，才能拿到宿主机真实磁盘占用。
        self.root = root or "/"
        self._lock = threading.Lock()
        self._cpu_prev = None          # (idle, total) 上次 /proc/stat 采样
        self._cache = {"ts": 0, "data": None}
        self._cache_ttl = 2.0          # 同机缓存 2s，防止短间隔重复采样 CPU

    # ---- CPU ----
    def _cpu_sample(self):
        if psutil is not None:
            # psutil.cpu_percent 自带间隔采样，但这里我们手动算差值更可控
            t = psutil.cpu_times()
            idle = t.idle
            total = sum(t)  # 所有字段求和
            return idle, total
        # /proc/stat 回退
        with open("/proc/stat") as f:
            line = f.readline()
        parts = [float(x) for x in line.split()[1:]]
        idle = parts[3]
        total = sum(parts)
        return idle, total

    def cpu_percent(self):
        cur = self._cpu_sample()
        with self._lock:
            prev = self._cpu_prev
            self._cpu_prev = cur
        if prev is None:
            return 0.0
        idle_delta = cur[0] - prev[0]
        total_delta = cur[1] - prev[1]
        if total_delta <= 0:
            return 0.0
        return round((1.0 - idle_delta / total_delta) * 100.0, 1)

    # ---- 内存 ----
    def mem_info(self):
        if psutil is not None:
            vm = psutil.virtual_memory()
            return {
                "total_mb": round(vm.total / 1024 / 1024, 1),
                "used_mb": round((vm.total - vm.available) / 1024 / 1024, 1),
                "avail_mb": round(vm.available / 1024 / 1024, 1),
                "percent": round(vm.percent, 1),
            }
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, v = line.partition(":")
                info[k.strip()] = int(v.strip().split()[0])  # KB
        total = info.get("MemTotal", 0)
        avail = info.get("MemAvailable", info.get("MemFree", 0))
        used = total - avail
        pct = round(used / total * 100.0, 1) if total else 0.0
        return {
            "total_mb": round(total / 1024.0, 1),
            "used_mb": round(used / 1024.0, 1),
            "avail_mb": round(avail / 1024.0, 1),
            "percent": pct,
        }

    # ---- 磁盘 ----
    def disk_info(self, path):
        try:
            st = os.statvfs(path)
        except Exception:
            return None
        total = st.f_blocks * st.f_frsize
        free = st.f_bavail * st.f_frsize
        used = total - free
        pct = round(used / total * 100.0, 1) if total else 0.0
        return {
            "path": path,
            "total_gb": round(total / 1024 / 1024 / 1024.0, 2),
            "used_gb": round(used / 1024 / 1024 / 1024.0, 2),
            "avail_gb": round(free / 1024 / 1024 / 1024.0, 2),
            "percent": pct,
        }

    def disks(self):
        paths = [self.root] + self.extra_disks
        out = []
        seen = set()
        for p in paths:
            # 归一到该路径所在文件系统挂载点
            rp = os.path.realpath(p)
            if rp in seen:
                continue
            seen.add(rp)
            d = self.disk_info(rp)
            if d:
                out.append(d)
        return out

    # ---- 负载 / 主机 ----
    def host_info(self):
        info = {
            "hostname": socket.gethostname(),
            "uptime_sec": 0,
            "load1": 0.0,
            "load5": 0.0,
            "load15": 0.0,
            "cpus": os.cpu_count() or 0,
        }
        try:
            with open("/proc/uptime") as f:
                info["uptime_sec"] = int(float(f.readline().split()[0]))
        except Exception:
            pass
        try:
            with open("/proc/loadavg") as f:
                l1, l5, l15 = f.readline().split()[:3]
            info["load1"] = float(l1)
            info["load5"] = float(l5)
            info["load15"] = float(l15)
        except Exception:
            pass
        return info

    def collect(self):
        now = time.time()
        with self._lock:
            if self._cache["data"] and now - self._cache["ts"] < self._cache_ttl:
                return self._cache["data"]
        data = {
            "ts": int(now),
            "cpu_percent": self.cpu_percent(),
            "mem": self.mem_info(),
            "disks": self.disks(),
            "host": self.host_info(),
            "agent": "custos-node-agent",
            "version": 2,
            # 进程启动时间（epoch 秒）：管理器据此自动核验 keepalived / node-agent
            # 是否已在节点上重启（「配置待生效」自动监测，无需人工点「已执行」）。
            "agent_started_at": _self_started_at(self.root),
            "keepalived_started_at": _proc_started_at(self.root, "keepalived"),
        }
        with self._lock:
            self._cache = {"ts": now, "data": data}
        return data


# ---------------------------------------------------------------------------
# 只读日志读取（管理器日志面板的兜底通道）
# ---------------------------------------------------------------------------
# 背景：MariaDB 没有 mysql.error_log 表，管理器只能靠 SQL 的 LOAD_FILE 读
# @@log_error 指向的文件；而 mysqld 生成的 error.log 权限是 0660，LOAD_FILE
# 对 0660 文件恒返回 NULL（实测 660→NULL、644→有内容），且还受 secure_file_priv
# 限制。agent 以 root 身份直接读宿主机文件，绕开全部 SQL 侧限制。
#
# 安全约束：只读；只允许读取白名单目录下的文件；不接受任何写/执行参数。
_LOG_NAME_CANDIDATES = (
    "{root}/data/custos-ha/logs/*/{name}.log",
    "{root}/data/custos-ha/logs/*/{name}.log.*",
    "{root}/data/*/logs/*/{name}.log",
    "{root}/var/log/custos-ha/{name}.log",
    "{root}/var/log/mysql/{name}.log",
    "{root}/var/log/mariadb/{name}.log",
    "{root}/var/lib/mysql/{name}.err",
)
_LOG_ALLOW_PREFIX = (
    "/data/",
    "/var/log/",
    "/var/lib/mysql/",
    "/var/lib/mariadb/",
)
_LOG_TAIL_MAX_BYTES = 2 * 1024 * 1024


def _host_join(root, host_path):
    """宿主机绝对路径 -> agent 视角的真实路径（容器里宿主机挂在 /host）。"""
    r = (root or "/").rstrip("/")
    if not r or r == "/":
        return host_path
    return r + "/" + host_path.lstrip("/")


def _host_strip(root, real_path):
    """agent 视角路径 -> 宿主机绝对路径（回显给管理器/前端）。"""
    r = (root or "/").rstrip("/")
    if r and r != "/" and real_path.startswith(r + "/"):
        return real_path[len(r):]
    return real_path


def _log_allowed(root, real_path):
    """只允许读取白名单目录下的文件（避免变成任意文件读取接口）。"""
    host_path = _host_strip(root, os.path.realpath(real_path))
    if ".." in host_path:
        return False
    return any(host_path.startswith(p) for p in _LOG_ALLOW_PREFIX)


def find_log_file(root, name="error"):
    """按常见布局搜索日志文件，返回 mtime 最新的那个真实路径（找不到返回 None）。"""
    best = None
    base = (root or "/").rstrip("/")
    for pat in _LOG_NAME_CANDIDATES:
        for p in glob.glob(pat.format(root=base, name=name)):
            try:
                if not os.path.isfile(p):
                    continue
                m = os.path.getmtime(p)
            except OSError:
                continue
            if best is None or m > best[0]:
                best = (m, p)
    return best[1] if best else None


def tail_lines(real_path, lines=400, max_bytes=_LOG_TAIL_MAX_BYTES):
    """读文件末尾最多 max_bytes 再取最后 lines 行（只读）。返回 (行列表, 大小, 是否截断)。"""
    size = os.path.getsize(real_path)
    start = max(0, size - max_bytes)
    with open(real_path, "rb") as f:
        if start:
            f.seek(start)
        data = f.read()
    arr = data.decode("utf-8", "replace").splitlines()
    if start and arr:
        arr = arr[1:]           # 丢弃可能被截断的首行
    return arr[-lines:], size, bool(start)


# ---------------------------------------------------------------------------
# 进程启动时间（供管理器「配置待生效」自动核验：比对服务重启时间）
# ---------------------------------------------------------------------------
# 纯只读：仅读宿主机 /proc（容器内 root=/host 且 pid: host，即宿主机 /proc），
# 不挂 docker.sock、不接受任何参数，延续 agent「只读」原则。
# 为什么需要：keepalived / node-agent 重启属容器生命周期动作，管理器无法从 DB 侧观测，
# 只能靠「进程启动时间晚于待办产生时间」来判断该待办是否已落实。
_PROC_HZ = 100.0   # Linux USER_HZ（主流 x86_64/aarch64 为 100）


def _proc_btime(root):
    """宿主机开机时间（epoch 秒），读 /proc/stat 的 btime；失败返回 None。"""
    try:
        with open(os.path.join(root, "proc", "stat")) as f:
            for line in f:
                if line.startswith("btime "):
                    return int(line.split()[1])
    except Exception:
        return None
    return None


def _proc_starttime_epoch(root, pid):
    """某 pid 的启动 epoch 秒（btime + starttime ticks / HZ）；失败返回 None。"""
    btime = _proc_btime(root)
    if btime is None:
        return None
    try:
        with open(os.path.join(root, "proc", str(pid), "stat")) as f:
            s = f.read()
    except Exception:
        return None
    rp = s.rfind(")")
    if rp < 0:
        return None
    rest = s[rp + 1:].split()
    # /proc/<pid>/stat 字段（1-based）：3=state … 22=starttime；
    # comm 被括号包裹（可能含空格），故按最后一个 ')' 切分，rest 从 state 起算 → starttime=rest[19]。
    if len(rest) < 20:
        return None
    try:
        return btime + int(rest[19]) / _PROC_HZ
    except (ValueError, IndexError):
        return None


def _proc_started_at(root, comm):
    """返回名为 comm 的进程启动 epoch 秒（取第一个匹配）；找不到返回 None。"""
    proc = os.path.join(root, "proc")
    try:
        for pid in os.listdir(proc):
            if not pid.isdigit():
                continue
            try:
                with open(os.path.join(proc, pid, "stat")) as f:
                    s = f.read()
            except Exception:
                continue
            # /proc/<pid>/stat 形如 `4159013 (keepalived) S 1 ...`：
            # comm 必须取【第一个 '(' 与最后一个 ')' 之间】的内容。
            # 此前误写为 s[1:rp]（只跳过了 pid 的首位数字），实际得到的却是
            # `159013 (keepalived`，与 comm 永远不相等 —— 于是
            # keepalived_started_at 恒为 None，管理器的 proc:* 自动核验
            # 永远判定「agent 未上报启动时间」，「配置待生效」徽标无法自动消失。
            lp = s.find("(")
            rp = s.rfind(")")
            if lp < 0 or rp <= lp or s[lp + 1:rp] != comm:
                continue
            return _proc_starttime_epoch(root, pid)
    except Exception:
        return None
    return None


def _self_started_at(root):
    """本 agent 进程自身的启动 epoch 秒（读 /proc/self/stat）。"""
    return _proc_starttime_epoch(root, "self")


# ---------------------------------------------------------------------------
# 宿主命令执行（Plan B：迁移物理通道 agent-exec 兜底）
# ---------------------------------------------------------------------------
def exec_on_host(script):
    """在宿主文件系统执行 shell 脚本，返回 {ok, rc, stdout, stderr}。

    容器节点：agent 容器挂了 /:/host:ro 且 pid:host，故 chroot /host 后执行，
    使脚本里的宿主路径（如 /data/custos-ha）与宿主 docker（/host/usr/bin/docker +
    挂载的 /host/var/run/docker.sock）都正确解析；docker 命令实际在宿主 dockerd 上跑，
    写入发生在目标容器内，不写宿主只读层。裸机节点：/host 即宿主根，等同原生执行。
    仅迁移场景使用；与 /stats 同样受 NODE_AGENT_TOKEN 保护。
    """
    env = dict(os.environ)
    if os.path.isdir("/host") and (os.path.exists("/host/usr/bin/docker")
                                   or os.path.exists("/host/bin/docker")):
        env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        args = ["chroot", "/host", "/bin/sh", "-c", script]
    else:
        args = ["/bin/sh", "-c", script]
    try:
        cp = subprocess.run(args, env=env, capture_output=True, text=True, timeout=3600 * 6)
        return {"ok": True, "rc": cp.returncode, "stdout": cp.stdout, "stderr": cp.stderr}
    except Exception as e:
        return {"ok": False, "rc": 1, "stdout": "", "stderr": str(e)}


# ---------------------------------------------------------------------------
# HTTP 服务
# ---------------------------------------------------------------------------
def make_handler(collector, token):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _allow(self):
            if not token:
                return True
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                return auth[len("Bearer "):].strip() == token
            q = self.path.split("?", 1)[1] if "?" in self.path else ""
            for kv in q.split("&"):
                if kv.startswith("token="):
                    return kv[6:] == token
            return False

        def _send(self, code, obj):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path in ("/healthz", "/ping"):
                self._send(200, {"ok": True})
                return
            if path == "/stats":
                if not self._allow():
                    self._send(401, {"error": "unauthorized"})
                    return
                try:
                    self._send(200, collector.collect())
                except Exception as e:
                    self._send(500, {"error": str(e)})
                return
            if path == "/logs":
                if not self._allow():
                    self._send(401, {"error": "unauthorized"})
                    return
                try:
                    raw_q = self.path.split("?", 1)[1] if "?" in self.path else ""
                    qs = dict(urllib.parse.parse_qsl(raw_q))
                    name = (qs.get("name") or "error").strip() or "error"
                    try:
                        n = int(qs.get("lines") or 400)
                    except ValueError:
                        n = 400
                    n = max(1, min(2000, n))
                    rp = None
                    if qs.get("path"):
                        cand = _host_join(collector.root, qs["path"])
                        if os.path.isfile(cand):
                            rp = cand
                    if rp is None:
                        rp = find_log_file(collector.root, name)
                    if not rp or not os.path.isfile(rp):
                        self._send(200, {"ok": False,
                                         "error": f"未找到 {name} 日志文件",
                                         "root": collector.root})
                        return
                    if not _log_allowed(collector.root, rp):
                        self._send(403, {"ok": False,
                                         "error": "路径不在允许的日志目录内"})
                        return
                    real = os.path.realpath(rp)
                    arr, size, trunc = tail_lines(real, n)
                    self._send(200, {
                        "ok": True,
                        "path": _host_strip(collector.root, real),
                        "size": size,
                        "truncated": trunc,
                        "mtime": int(os.path.getmtime(real)),
                        "lines": arr,
                    })
                except Exception as e:
                    self._send(500, {"ok": False, "error": str(e)})
                return
            self._send(404, {"error": "not found"})

        def do_POST(self):
            path = self.path.split("?", 1)[0]
            if path == "/exec":
                if not self._allow():
                    self._send(401, {"error": "unauthorized"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", 0) or 0)
                    raw = self.rfile.read(length) if length else b"{}"
                    body = json.loads(raw.decode("utf-8") or "{}")
                    script = body.get("script") or ""
                    if not script:
                        self._send(400, {"error": "missing script"})
                        return
                    self._send(200, exec_on_host(script))
                except Exception as e:
                    self._send(500, {"error": str(e)})
                return
            self._send(404, {"error": "not found"})

        def log_message(self, *a):  # 静默默认访问日志
            pass

    return Handler


def main():
    ap = argparse.ArgumentParser(description="Custos 节点 OS 指标采集 agent")
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=int(os.environ.get("NODE_AGENT_PORT", 28889)))
    ap.add_argument("--token", default=os.environ.get("NODE_AGENT_TOKEN", ""))
    ap.add_argument("--disks", default=os.environ.get("NODE_AGENT_DISKS", ""),
                    help="逗号分隔的额外挂载点，如 /data,/backup")
    ap.add_argument("--root", default="/",
                    help="要统计磁盘占用的根挂载点。裸机默认 /；容器部署时配合 -v /:/host:ro 传 --root /host")
    ap.add_argument("--log", default=os.environ.get("NODE_AGENT_LOG", ""),
                    help="日志文件路径（如 /var/log/custos-ha/node_agent.log）；不指定则仅 stdout")
    args = ap.parse_args()

    log = None
    if args.log:
        logging.basicConfig(
            filename=args.log, level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s")
        log = logging.getLogger("node_agent")

    collector = Collector(extra_disks=args.disks.split(",") if args.disks else [], root=args.root)
    handler = make_handler(collector, args.token.strip())
    srv = ThreadingHTTPServer((args.bind, args.port), handler)
    msg = ("[node_agent] listening on %s:%s" % (args.bind, args.port)) + \
          (" (token required)" if args.token else " (no auth)")
    if log is not None:
        log.info(msg)
    else:
        print(msg, flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        if log is not None:
            log.info("stopped by KeyboardInterrupt")
    except Exception:
        if log is not None:
            logging.exception("node_agent crashed")
        raise
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
