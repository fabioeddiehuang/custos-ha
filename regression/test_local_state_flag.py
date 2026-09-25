"""F16 回归：ha_local.state 本地主库标识的「写入 / 核验 / 自愈」链路。

背景（2026-09-16 真机事故）：
keepalived 的 check_mysql_master.sh 第 3 关**只读节点本地** ha_local.state.current_master，
据此决定是否持有 VIP；vip-guard（make_slave_of）也读同一份记录决定是否允许把节点降级为从库。
因此这条链路上任何一环**静默失败**，后果都是「真主拒绝持有 VIP」或「回归的旧主拒绝降级」，
实测表现为：故障切换后两个节点同时放弃 VIP → VIP 凭空消失，且要等满 5 分钟兜底才自愈。

本文件把修好的三条不变量焊死，防止回归：
  1. set_local_master_flag_ex 必须能说清失败原因，不能再静默返回 False；
  2. push_current_master_to_nodes 按**节点**记账（失败下周期立即重试），
     且当前主库**每周期都核验**（不进 5 分钟缓存）——它是 VIP 落点；
  3. make_slave_of 的 VIP 守卫先**就地纠正**过期的本地状态，只有纠正失败才拒绝降级。
"""
from conftest import FakeConnection, FakeCursor

import app.failover as failover
import app.ha_client as ha_client
import app.alerts as alerts


# ---------------------------------------------------------------- 工具

def _push_setup(monkeypatch, nodes, results):
    """把 push_current_master_to_nodes 的外部依赖全部替换为可观测替身。

    nodes:   [(host, port)]  —— 会被 query() 返回的集群成员
    results: {(host, port): (ok, err) | Exception}  —— 每个节点的写入结果
    返回 (calls, alert_msgs)：calls 记录每次写入尝试，alert_msgs 记录告警文案。
    """
    calls = []
    alert_msgs = []

    monkeypatch.setattr(failover, "load_creds", lambda cid=None: {
        "ha_user": "ha_monitor", "ha_pass": "p",
        "repl_user": "replication", "repl_pass": "rp",
        "app_user": "app", "app_pass": "ap",
    })
    monkeypatch.setattr(failover, "query",
                        lambda sql, args=None: [{"host": h, "port": p} for h, p in nodes])
    monkeypatch.setattr(failover, "node_root", lambda ip, cid=None: ("root", "rpw"))

    def fake_set(host, port, user, password, master_str, root_user="root", root_pass=""):
        calls.append((host, port, master_str))
        r = results.get((host, port), (True, None))
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(failover, "set_local_master_flag_ex", fake_set)
    monkeypatch.setattr(failover, "send_alert",
                        lambda title, msg=None, **k: alert_msgs.append(title + "|" + str(msg)))
    monkeypatch.setattr(failover, "_LAST_PUSH", {})
    monkeypatch.setattr(failover, "_PUSH_WARN_AT", {})
    return calls, alert_msgs


class _ExplodingCursor(FakeCursor):
    """在写入 ha_local.state 时抛错，用于模拟「就地纠正也失败」（如 1142 权限不足）。"""

    def execute(self, sql, args=None):
        if "REPLACE INTO ha_local.state" in (sql or ""):
            raise RuntimeError("1142 command denied to user 'ha_monitor'")
        return super().execute(sql, args)


def _guard_setup(monkeypatch, cur):
    """构造 make_slave_of 最小连库环境：本机自认主(10.0.0.2) + 当前可写 + MariaDB。"""
    conn = FakeConnection()
    conn._cur = cur
    monkeypatch.setattr(ha_client, "connect_robust", lambda *a, **k: conn)
    monkeypatch.setattr(alerts, "send_alert", lambda *a, **k: None)
    cur.add("@@global.read_only", [(0,)])
    cur.add("select v from ha_local.state", [("10.0.0.2:3306",)])
    cur.add("select version()", [("12.3.3-MariaDB",)])
    return conn


def _executed(cur):
    return " || ".join(cur.executed)


# ---------------------------------------------------------------- 1. 写入层可诊断

def test_set_local_master_flag_reports_connect_failure(monkeypatch):
    """连不上时必须给出原因，而不是静默 False（静默正是本次事故长期隐身的原因）。"""
    monkeypatch.setattr(ha_client, "connect_robust", lambda *a, **k: None)
    ok, err = ha_client.set_local_master_flag_ex("10.0.0.1", 3306, "u", "p", "M:3306")
    assert ok is False
    assert err and "无法连接" in err


def test_set_local_master_flag_ex_skips_write_when_value_equal(monkeypatch, fake_connection):
    """值已正确时不得产生 REPLACE —— 保住「空闲零写入」特性。"""
    fake_connection._cur.add("select v from ha_local.state", [("10.0.0.1:3306",)])
    monkeypatch.setattr(ha_client, "connect_robust", lambda *a, **k: fake_connection)

    ok, err = ha_client.set_local_master_flag_ex("10.0.0.1", 3306, "u", "p", "10.0.0.1:3306")
    assert ok is True and err is None
    assert "REPLACE INTO ha_local.state" not in _executed(fake_connection._cur)


def test_set_local_master_flag_ex_writes_when_value_differs(monkeypatch, fake_connection):
    """值过期时必须真的写下去（真主拿不到 VIP 的直接原因就是这里没写成功）。"""
    fake_connection._cur.add("select v from ha_local.state", [("10.9.9.9:3306",)])
    monkeypatch.setattr(ha_client, "connect_robust", lambda *a, **k: fake_connection)

    ok, err = ha_client.set_local_master_flag_ex("10.0.0.1", 3306, "u", "p", "10.0.0.1:3306")
    assert ok is True and err is None
    cur = fake_connection._cur
    assert "REPLACE INTO ha_local.state" in _executed(cur)
    # 必须禁 binlog：ha_local 是节点本地状态表，进了 binlog 会在从库造成 1062 卡复制
    assert "SET SESSION sql_log_bin=0" in _executed(cur)
    # 写毕必须恢复 binlog
    assert "SET SESSION sql_log_bin=1" in _executed(cur)


def test_set_local_master_flag_still_returns_bool(monkeypatch, fake_connection):
    """向后兼容：旧签名仍返回裸 bool（有其它调用方按 bool 使用）。"""
    fake_connection._cur.add("select v from ha_local.state", [("10.0.0.1:3306",)])
    monkeypatch.setattr(ha_client, "connect_robust", lambda *a, **k: fake_connection)
    assert ha_client.set_local_master_flag("10.0.0.1", 3306, "u", "p", "10.0.0.1:3306") is True


# ---------------------------------------------------------------- 2. 推送层按节点记账

def test_push_retries_failed_node_on_next_cycle(monkeypatch):
    """失败节点不得被集群级时间戳掩盖，必须下个周期立即重试。

    用三节点把两条不变量分开验证：10.0.0.1=当前主库（每周期核验）、
    10.0.0.2=非主库且推送成功（应被缓存跳过）、10.0.0.3=推送失败（必须重试）。
    """
    nodes = [("10.0.0.1", 3306), ("10.0.0.2", 3306), ("10.0.0.3", 3306)]
    results = {("10.0.0.3", 3306): (False, "access denied 1227")}
    calls, alert_msgs = _push_setup(monkeypatch, nodes, results)

    failover.push_current_master_to_nodes(1, "10.0.0.1:3306")
    assert sorted(h for h, _, _ in calls) == ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
    assert len(alert_msgs) == 1, "失败必须告警，不能静默"

    calls.clear()
    failover.push_current_master_to_nodes(1, "10.0.0.1:3306")
    got = sorted(h for h, _, _ in calls)
    assert got == ["10.0.0.1", "10.0.0.3"], \
        "当前主库每周期核验 + 失败节点重试；已成功的非主库走缓存跳过"
    assert len(alert_msgs) == 1, "同一失败集合在冷却期内不重复告警"


def test_push_verifies_current_master_every_cycle(monkeypatch):
    """当前主库是 VIP 落点：不得走 5 分钟缓存，必须每周期核验（哨兵实验证明的缺陷）。

    哨兵实验（真机）：向节点注入错误的 current_master 后，旧实现 90s 内完全无人纠正，
    因为该节点被记过「已推送」；而真主一旦处于这种状态，keepalived 会主动放弃 VIP。
    """
    nodes = [("10.0.0.1", 3306), ("10.0.0.2", 3306)]
    calls, _ = _push_setup(monkeypatch, nodes, {})

    # 首轮：主库标识变化 → 全部重推
    failover.push_current_master_to_nodes(1, "10.0.0.1:3306")
    assert sorted(h for h, _, _ in calls) == ["10.0.0.1", "10.0.0.2"]

    # 次轮：主库标识未变 → 当前主库仍每周期核验；非主库被缓存跳过（纯 I/O 优化）
    calls.clear()
    failover.push_current_master_to_nodes(1, "10.0.0.1:3306")
    assert [h for h, _, _ in calls] == ["10.0.0.1"], \
        "当前主库必须每周期核验（值正确时写入层会自行短路，零写盘）"


def test_push_force_ignores_cache(monkeypatch):
    """切换后必须 force 重推，忽略一切缓存，确保新主立刻落到每个节点。"""
    nodes = [("10.0.0.1", 3306), ("10.0.0.2", 3306)]
    calls, _ = _push_setup(monkeypatch, nodes, {})
    failover.push_current_master_to_nodes(1, "10.0.0.1:3306")
    calls.clear()
    failover.push_current_master_to_nodes(1, "10.0.0.1:3306", force=True)
    assert sorted(h for h, _, _ in calls) == ["10.0.0.1", "10.0.0.2"]


def test_push_captures_exception_and_alerts(monkeypatch):
    """抛出（非返回 False）也必须被捕获并告警，不得静默吞掉。"""
    nodes = [("10.0.0.1", 3306), ("10.0.0.2", 3306)]
    results = {("10.0.0.2", 3306): RuntimeError("connection refused")}
    _, alert_msgs = _push_setup(monkeypatch, nodes, results)

    failover.push_current_master_to_nodes(1, "10.0.0.1:3306")
    assert len(alert_msgs) == 1
    assert "10.0.0.2:3306" in alert_msgs[0]
    assert "connection refused" in alert_msgs[0]


def test_reset_topo_throttle_clears_per_node_push_cache(monkeypatch):
    """切换/手动切换后必须清掉推送缓存（含其它集群不受影响）。"""
    nodes = [("10.0.0.1", 3306)]
    _push_setup(monkeypatch, nodes, {})
    failover._LAST_PUSH[(1, "10.0.0.1", 3306)] = ("10.0.0.1:3306", 0.0)
    failover._LAST_PUSH[(2, "10.0.0.9", 3306)] = ("10.0.0.9:3306", 0.0)
    failover._PUSH_WARN_AT[(1, "m", ())] = 0.0

    failover.reset_topo_throttle(1)
    assert (1, "10.0.0.1", 3306) not in failover._LAST_PUSH
    assert (2, "10.0.0.9", 3306) in failover._LAST_PUSH, "不得误清其它集群"
    assert (1, "m", ()) not in failover._PUSH_WARN_AT


# ---------------------------------------------------------------- 3. VIP 守卫自愈

def test_vip_guard_selfheals_stale_local_state(monkeypatch):
    """本机自认主、但管理器已认定别的主 → 就地纠正并放行降级（旧实现是死锁）。"""
    cur = FakeCursor()
    _guard_setup(monkeypatch, cur)

    try:
        ok, msg = ha_client.make_slave_of("10.0.0.2", 3306, "u", "p",
                                          "10.0.0.1", 3306, "replication", "rp")
    except Exception:
        ok, msg = None, ""   # 下游复制 SQL 在替身上可能失败，与本用例无关

    assert "REPLACE INTO ha_local.state" in _executed(cur), \
        "必须就地纠正本地 current_master"
    assert "拒绝把" not in (msg or ""), "纠正成功后不得再拒绝降级"


def test_vip_guard_refuses_when_selfheal_fails(monkeypatch):
    """只有「就地纠正也失败」才允许拒绝，且必须把失败原因暴露出来。"""
    cur = _ExplodingCursor()
    _guard_setup(monkeypatch, cur)

    ok, msg = ha_client.make_slave_of("10.0.0.2", 3306, "u", "p",
                                      "10.0.0.1", 3306, "replication", "rp")
    assert ok is False
    assert "拒绝把" in msg
    assert "就地纠正该记录也未成功" in msg
    assert "ha_monitor" in msg or "1142" in msg, "应带出真实失败原因"


def test_vip_guard_allows_already_readonly_node(monkeypatch):
    """已被 fencing 置只读的节点：降级不会让业务更糟，应放行以恢复复制。"""
    cur = FakeCursor()
    conn = _guard_setup(monkeypatch, cur)
    cur.add("@@global.read_only", [(1,)])   # 只读

    try:
        ok, msg = ha_client.make_slave_of("10.0.0.2", 3306, "u", "p",
                                          "10.0.0.1", 3306, "replication", "rp")
    except Exception:
        ok, msg = None, ""

    assert "REPLACE INTO ha_local.state" not in _executed(cur), \
        "只读场景无需改写本地状态"
    assert "拒绝把" not in (msg or "")
