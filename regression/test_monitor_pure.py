"""monitor 纯逻辑单测：复制状态分类（数据交互状态）、复制错误是否「源库不可达」。

_classify_repl_state 是 v2.2.20「数据交互状态」展示与后端采样的唯一真相来源，
分类错会把「卡死」显示成「正常」、把「需重建」显示成「中断」——直接误导运维。
_repl_err_is_connectivity 决定自愈是否跳过（CPU 打满类故障），误判会空耗资源。
"""
from app import monitor


def test_classify_offline():
    assert monitor._classify_repl_state({"alive": False}, False) == "offline"
    # 主库不可达也归 offline（alive 在前置探活里已置 False）
    assert monitor._classify_repl_state({"alive": False, "ro": 0}, True) == "offline"


def test_classify_master():
    assert monitor._classify_repl_state({"alive": True, "ro": 1}, True) == "orphan"
    assert monitor._classify_repl_state({"alive": True, "ro": 0}, True) == "primary"
    # 主库即使复制线程异常也只判 primary，不应误归 repl_*
    assert monitor._classify_repl_state(
        {"alive": True, "ro": 0, "slave_io": "No", "slave_sql": "No"}, True) == "primary"


def test_classify_slave_states():
    assert monitor._classify_repl_state({"alive": True, "purged": True}, False) == "repl_purged"
    assert monitor._classify_repl_state({"alive": True, "stuck": True}, False) == "repl_stuck"
    assert monitor._classify_repl_state(
        {"alive": True, "slave_io": "No", "slave_sql": "Yes"}, False) == "repl_broken"
    # 优先级：purged > stuck > broken > ok
    assert monitor._classify_repl_state(
        {"alive": True, "purged": True, "stuck": True, "slave_io": "No"}, False) == "repl_purged"
    assert monitor._classify_repl_state(
        {"alive": True, "stuck": True, "slave_io": "No"}, False) == "repl_stuck"
    assert monitor._classify_repl_state(
        {"alive": True, "slave_io": "Yes", "slave_sql": "Yes"}, False) == "repl_ok"


def test_repl_err_is_connectivity():
    assert monitor._repl_err_is_connectivity(
        {"slave_io_err": "2003 (HY000): Can't connect to MySQL server"}) is True
    assert monitor._repl_err_is_connectivity({"error": "Lost connection to master"}) is True
    assert monitor._repl_err_is_connectivity({"slave_io_err": "connection refused"}) is True
    assert monitor._repl_err_is_connectivity(
        {"slave_io_err": "Host is blocked because of many connection errors"}) is True
    assert monitor._repl_err_is_connectivity({"slave_sql_err": "timeout"}) is True
    # 1045 凭证漂移不算不可达（由凭证自愈负责）
    assert monitor._repl_err_is_connectivity(
        {"slave_io_err": "Access denied for user"}) is False
    # 1236 清理类不算不可达（走重建流程）
    assert monitor._repl_err_is_connectivity(
        {"slave_io_err": "Got fatal error 1236 ... purged"}) is False
    assert monitor._repl_err_is_connectivity({}) is False


def test_classify_fork_when_upstream_not_current_master():
    """F14：从库双线程 Yes 但上游并非当前主库 = 静默数据分叉，必须判 repl_fork。

    本轮真机：33 顶着早已消失的幽灵主库，双线程 Yes 却持续写错数据，
    旧逻辑只验 slave_io/slave_sql 报 repl_ok 掩盖了分叉。
    """
    st = {"alive": True, "slave_io": "Yes", "slave_sql": "Yes",
          "master_host": "192.0.2.203", "master_port": 3307}
    # 上游 != 当前主库 -> 分叉（而非 repl_ok）
    assert monitor._classify_repl_state(st, False, "192.0.2.177:3307") == "repl_fork"
    # 上游 == 当前主库 -> 正常
    assert monitor._classify_repl_state(st, False, "192.0.2.203:3307") == "repl_ok"
    # 无当前主库可比对 -> 退化为旧语义（仅看线程），不误报分叉
    assert monitor._classify_repl_state(st, False) == "repl_ok"
    # 线程已断 -> 仍是 broken，不因上游校验被改成 fork
    assert monitor._classify_repl_state(
        {"alive": True, "slave_io": "No", "slave_sql": "Yes",
         "master_host": "192.0.2.203", "master_port": 3307},
        False, "192.0.2.177:3307") == "repl_broken"
    # 端口不一致也应判分叉（防 master_port 漂移型误对齐）
    assert monitor._classify_repl_state(
        {"alive": True, "slave_io": "Yes", "slave_sql": "Yes",
         "master_host": "192.0.2.177", "master_port": 3308},
        False, "192.0.2.177:3307") == "repl_fork"
