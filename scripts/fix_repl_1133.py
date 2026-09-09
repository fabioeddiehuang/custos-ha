#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快速修复从库 SQL 线程因 1133 等幂等冲突而中断的复制。

场景：管理器对主库执行 `GRANT ... TO 'ha_monitor'@'<manager_ip>'`，该 GRANT 经 binlog
复制到从库，而从库上该精确 host 账号不存在，重放报 1133 导致 SQL 线程卡死。

本脚本复用 app.ha_client.heal_replication（已支持 1133 自动跳过）：检测 Slave_SQL_Running=No
且错误为可跳过冲突时，注入空事务 / SET GLOBAL sql_slave_skip_counter=1 跳过并重起复制。

用法:
  python3 scripts/fix_repl_1133.py --host 192.168.1.33 --port 3307 --root-pass <节点root密码>
  # 可加入 cron 定时巡检：对各从库每分钟跑一次（无冲突时直接返回“复制正常”）。
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.ha_client import heal_replication


def main():
    ap = argparse.ArgumentParser(description="修复从库 SQL 线程 1133 等幂等复制冲突")
    ap.add_argument("--host", required=True, help="从库 IP")
    ap.add_argument("--port", type=int, default=3306, help="从库端口")
    ap.add_argument("--root-user", default="root")
    ap.add_argument("--root-pass", default=os.environ.get("ROOT_PASS", ""),
                    help="节点 root 密码（跳过事务需 SUPER）")
    ap.add_argument("--ha-user", default="ha_monitor")
    ap.add_argument("--ha-pass", default=os.environ.get("HA_PASS", ""))
    args = ap.parse_args()
    ok, msg = heal_replication(args.host, args.port, args.ha_user, args.ha_pass,
                               args.root_user, args.root_pass)
    print(("OK: " if ok else "FAIL: ") + msg)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
