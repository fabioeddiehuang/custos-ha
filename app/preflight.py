# -*- coding: utf-8 -*-
"""preflight.py — 安装/升级前预检（v2.5 改造，Phase 3，稳定+安全）。

装前跑清单；任一不过则明确报错并给出修复命令，而非「装完才发现坏」。
离线优先（F2）在此校验：所需镜像必须已 load，运行期零外网/零 registry 依赖。
"""
import logging
import os
import shutil
import socket

log = logging.getLogger(__name__)


def preflight_install(spec):
    """spec: dict(device_free_gb_min, ports[list], vip, image_loaded, flavour_supported)。

    返回 (ok, [(level, msg)])；ok=False 时不应继续安装。
    """
    issues = []
    # 1) 磁盘可用空间
    try:
        base = "/app/data" if os.path.isdir("/app/data") else "."
        free = shutil.disk_usage(base).free / (1024 ** 3)
        if free < spec.get("device_free_gb_min", 5):
            issues.append(("error",
                            "磁盘可用空间 %.1fGB 低于阈值 %.1fGB（df -h %s 查看；清理或扩容后重试）"
                            % (free, spec["device_free_gb_min"], base)))
    except Exception as e:
        issues.append(("warn", "磁盘检查失败：%s" % e))

    # 2) 所需端口未被占用
    for p in spec.get("ports", []):
        s = socket.socket()
        s.settimeout(0.3)
        try:
            s.bind(("0.0.0.0", int(p)))
        except Exception:
            issues.append(("error", "端口 %s 已被占用（lsof -i:%s 查看占用进程；释放后重试）" % (p, p)))
        finally:
            s.close()

    # 3) 离线镜像已 load（无外网/registry 依赖）
    if spec.get("image_loaded") is False:
        issues.append(("error", "所需镜像未载入（离线环境请先 `docker load -i <tar>`；"
                               "确认 tar 在 ./images 且 .dockerignore 已排除）"))

    # 4) 风味在支持矩阵内（TODO 1.2 矩阵）
    if spec.get("flavour_supported") is False:
        issues.append(("error", "节点风味不在支持矩阵（仅 MariaDB 10.11+/11.4+/12.x 与 MySQL 8.0）"))

    ok = not any(l == "error" for l, _ in issues)
    if ok and not issues:
        issues.append(("ok", "安装预检通过"))
    return ok, issues
