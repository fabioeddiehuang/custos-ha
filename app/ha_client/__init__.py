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
from app.ha_client.db_ops import (ident_clause, gtid_slave_is_subset,
                                  has_privilege)
# ★ set_read_only 必须导出【主机级】版本（status.py: host, port, user, password,
#   on, root_user, root_pass），且必须在 db_ops 导入之后显式再绑定——因为
#   `from ...replication import *` / db_ops 里都有同名【游标级】函数
#   (cur, on, flavour)，会把它遮蔽。
#   此前导出的是游标级：failover.py / monitor.py 的写入隔离调用报
#   “set_read_only() takes from 2 to 3 positional arguments but 7 were given”
#   → except 吞掉 → fenced=False → 防脑裂见证发现主库仍存活 → **切换被中止**
#   （表现为「手动切主失败」）。需要游标级时请显式
#   `from app.ha_client.db_ops import set_read_only`（syncgate.py 即如此）。
from app.ha_client.status import (set_read_only,)
# 内部下划线辅助函数也显式再导出，保持与原单文件 app/ha_client.py 完全等价
from app.ha_client.connection import (_pymysql_at_least, _tiny_sleep, _one_dict, _esc_sql)
from app.ha_client.gtid import (_derive_stuck_gtid, _io_error_is_auth, _start_io_thread, _stop_io_thread, _stop_replica)
from app.ha_client.credentials import (_native_password_hash, _parse_grant_privs, _granted_privs,
                                       _can_login, _ensure_account, _has_all_privileges,
                                       _ensure_local_state_grant, _account_hosts, _as_int)
from app.ha_client.local_state import (_addr_eq, _local_state_self_master)
from app.ha_client.replication import (_fence_readonly,)
