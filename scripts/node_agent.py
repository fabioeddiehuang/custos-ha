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
#   - 只暴露只读采集，绝不接受任何写/执行请求。
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
import json
import os
import socket
import threading
import time
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
            "version": 1,
        }
        with self._lock:
            self._cache = {"ts": now, "data": data}
        return data


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
    args = ap.parse_args()

    collector = Collector(extra_disks=args.disks.split(",") if args.disks else [], root=args.root)
    handler = make_handler(collector, args.token.strip())
    srv = ThreadingHTTPServer((args.bind, args.port), handler)
    print(f"[node_agent] listening on {args.bind}:{args.port}"
          + (f" (token required)" if args.token else " (no auth)"), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
