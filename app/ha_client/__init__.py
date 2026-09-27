# -*- coding: utf-8 -*-
"""ha_client 公共 API（包 facade）。

实现按职责拆分在 app/ha_client/ 子模块（connection / status / gtid /
credentials / local_state / replication / _const）。对外
`from app.ha_client import <name>` 与原单文件 app/ha_client.py 完全等价。
"""
from app.ha_client.connection import *
from app.ha_client.status import *
from app.ha_client.gtid import *
from app.ha_client.credentials import *
from app.ha_client.local_state import *
from app.ha_client.replication import *
from app.ha_client.db_ops import (ident_clause, set_read_only, gtid_slave_is_subset,
                                  has_privilege)
# 内部下划线辅助函数也显式再导出，保持与原单文件 app/ha_client.py 完全等价
from app.ha_client.connection import (_pymysql_at_least, _tiny_sleep, _one_dict, _esc_sql)
from app.ha_client.gtid import (_derive_stuck_gtid, _io_error_is_auth, _start_io_thread, _stop_io_thread, _stop_replica)
from app.ha_client.credentials import (_native_password_hash, _parse_grant_privs, _granted_privs,
                                       _can_login, _ensure_account, _has_all_privileges,
                                       _ensure_local_state_grant, _account_hosts, _as_int)
from app.ha_client.local_state import (_addr_eq, _local_state_self_master)
from app.ha_client.replication import (_fence_readonly,)
