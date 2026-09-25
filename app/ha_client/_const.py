# -*- coding: utf-8 -*-
"""ha_client 模块级常量（拆分后集中放置；各子模块 `from ._const import *` 使用，含下划线名）。"""
import os as _os

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



__all__ = ["_IDEMPOTENT_SQL_ERRNOS", "_DATA_CONFLICT_SQL_ERRNOS", "_SKIPPABLE_SQL_ERRNOS", "_AUTH_IO_MARKERS", "_VIPGUARD_FENCE_GRACE", "_FENCE_FIRST_SEEN", "_alter_memo", "_ALL_PRIV_SIG", "_RETENTION_DAYS", "_RETENTION_SEC", "_MAX_BINLOG_SIZE", "_BINLOG_TOTAL_SIZE_CAP"]