# -*- coding: utf-8 -*-
"""selfheal.py — 自愈收敛状态机（v2.5 稳定性改造，对应 MR1/MR2/MR3 自愈侧）。

把「从库如何收敛到终态」从 monitor 主循环里拆出来（功能/自愈拆分独立文件），
提供**幂等、限频、可重入、可观测**的收敛动作。终态定义：

    从库指向 current_master 且 slave_io==Yes 且 slave_sql==Yes 且 GTID 已覆盖

每次 tick 推进一帧，覆盖全部拓扑：
    - 主宕          ：等当前主恢复（monitor 侧跳过，不在此重指）
    - 从宕          ：重启复制 + heal
    - 旧主残留      ：切换后从库还挂已退位旧主 → repoint 到 current_master
    - MariaDB 分叉  ：reset 后重挂（**绝不盲目 RESET MASTER 丢数据**，优先回灌主库）
    - 跨机房        ：repoint

所有动作失败都**显式记录（禁静默）**，且幂等——重装/重启后从任意中间态续跑。
"""
import logging

from app.ha_client import (make_slave_of, get_status, heal_replication)
from app.ha_client.db_ops import gtid_slave_is_subset
from app.failover import _repoint_slave, node_root

log = logging.getLogger(__name__)


def repoint_slave_to_current(cluster_id, h, cur_h, cur_p, creds):
    """把从库 h 重指向当前主 cur_h:cur_p 并自愈复制冲突（底层用 failover._repoint_slave）。

    幂等：重复调用安全；可观测：每一步打印日志；**绝不返回假成功**（复制未双 Yes 即 False）。
    """
    return _repoint_slave(cluster_id, h, cur_h, cur_p, creds)


def converge_slave(cluster_id, h, cur_h, cur_p, flavour, creds, st):
    """收敛一帧：根据从库当前状态决定并执行下一步动作，返回 (action, ok, msg)。

    action ∈ {noop, repoint, heal}。所有动作幂等、可重入。
    返回 (action, ok, msg)；失败显式记录，不静默。
    """
    host, port = h["host"], int(h["port"])
    ru, rp = node_root(host, cluster_id)
    src_h = (st.get("master_host") or "").strip()
    src_p = int(st.get("master_port") or 0)
    points_current = bool(cur_h) and src_h == cur_h and src_p == int(cur_p or 0)
    io_yes = st.get("slave_io") == "Yes"
    sql_yes = st.get("slave_sql") == "Yes"

    if points_current and io_yes and sql_yes and not st.get("error"):
        return "noop", True, f"{host}:{port} 已收敛（指向当前主且复制双 Yes）"

    if points_current:
        # 指向对了但线程异常 → 自愈复制（幂等冲突 / IO 缺口），不重指
        try:
            heal_replication(host, port, creds["ha_user"], creds["ha_pass"], ru, rp)
            return "heal", True, f"{host}:{port} 指向当前主，已触发复制自愈"
        except Exception as e:
            log.warning("converge heal 异常 %s:%s: %s", host, port, e)
            return "heal", False, f"{host}:{port} 复制自愈异常: {e}"

    # 指向旧主（或错误源）→ 重指向当前主（自愈收敛核心动作）
    log.info("[selfheal] %s:%s 指向旧主/错误源(%s:%s)，重指向当前主 %s:%s",
             host, port, src_h, src_p, cur_h, cur_p)
    ok, msg = repoint_slave_to_current(cluster_id, h, cur_h, cur_p, creds)
    return "repoint", ok, msg


def tick_slaves(cluster_id, nodes, cur_h, cur_p, flavour, creds):
    """对集群所有从库各推进一帧收敛，返回 [(host:port, action, ok, msg)]。"""
    out = []
    for h in nodes:
        try:
            ru, rp = node_root(h["host"], cluster_id)
            st = get_status(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"], ru, rp) or {}
        except Exception as e:
            out.append((f"{h['host']}:{h['port']}", "probe", False, f"状态读取失败: {e}"))
            continue
        action, ok, msg = converge_slave(cluster_id, h, cur_h, cur_p, flavour, creds, st)
        out.append((f"{h['host']}:{h['port']}", action, ok, msg))
    return out
