# -*- coding: utf-8 -*-
"""ha_client 模块级常量（拆分后集中放置；各子模块 `from ._const import *` 使用，含下划线名）。"""
import os as _os
import time as _time

_IDEMPOTENT_SQL_ERRNOS = frozenset({1007, 1050, 1061, 1396, 1133})

_DATA_CONFLICT_SQL_ERRNOS = frozenset({1062, 1032, 1146})

_SKIPPABLE_SQL_ERRNOS = _IDEMPOTENT_SQL_ERRNOS | _DATA_CONFLICT_SQL_ERRNOS



_AUTH_IO_MARKERS = (
    "caching_sha2_password",
    "authentication requires secure connection",
    "authentication plugin",
    "access denied for user",
    "error connecting to source",
    "error connecting to master",
    # WolfSSL(裸机 tarball) 客户端 → OpenSSL(官方镜像) 服务端的 TLS 握手失败会报这个，
    # 属于「连接类」错误，同样靠重建通道（新 SQL 带 MASTER_SSL=0）自愈。
    "ssl connection error",
    "2061",
    "1045",
)

_VIPGUARD_FENCE_GRACE = float(_os.environ.get("VIPGUARD_FENCE_GRACE", "60"))  # 秒

_FENCE_FIRST_SEEN = {}



_alter_memo = {}

_ALL_PRIV_SIG = {
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD",
    "SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER", "SUPER",
    "CREATE USER", "TRIGGER",
}

_RETENTION_DAYS = 7

_RETENTION_SEC = 7 * 24 * 3600  # 604800，MySQL 8.0 侧使用

_MAX_BINLOG_SIZE = 268435456    # 256M

_BINLOG_TOTAL_SIZE_CAP = 20 * 1024 * 1024 * 1024



# —— 后台自愈日志限频（L2 噪音治理，2026-10-12）——
# 背景：Custos 的自愈是「静默成功、失败才记日志」。同一个失败每轮 monitor 都会再记一条，
# 而它往往下一轮就被自愈修掉 ⇒ 日志里堆满 warn，用户误判为「系统有问题」。
# 这里提供唯一限频入口：**同一 key 的同类噪音在 interval 秒内只记第一次**。
_WARN_THROTTLE = {}
WARN_THROTTLE_INTERVAL = 6 * 3600  # 6 小时，与项目既有告警限频惯例一致


def warn_throttled(key, interval=WARN_THROTTLE_INTERVAL):
    """限频闸门：True=本次应记录；False=窗口内已记过，应静默。

    刻意不做线程安全：最坏情况只是多记一条日志，绝不丢告警 —— 噪音治理不能
    以牺牲「可观测性」为代价。key 应包含 节点(host,port) + 故障类别 + 账号。
    """
    now = _time.time()
    last = _WARN_THROTTLE.get(key)
    if last is not None and (now - last) < interval:
        return False
    _WARN_THROTTLE[key] = now
    return True


__all__ = ["_IDEMPOTENT_SQL_ERRNOS", "_DATA_CONFLICT_SQL_ERRNOS", "_SKIPPABLE_SQL_ERRNOS", "_AUTH_IO_MARKERS", "_VIPGUARD_FENCE_GRACE", "_FENCE_FIRST_SEEN", "_alter_memo", "_ALL_PRIV_SIG", "_RETENTION_DAYS", "_RETENTION_SEC", "_MAX_BINLOG_SIZE", "_BINLOG_TOTAL_SIZE_CAP", "warn_throttled", "WARN_THROTTLE_INTERVAL"]