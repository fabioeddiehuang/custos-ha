"""mariadb_matrix 单测：版本支持矩阵、版本准入、特权集按服务器能力过滤、半同步命名。

这些逻辑是「MariaDB 纳管正确性」的地基：版本误判会让节点被错误拒绝/接受，
特权名硬编码会刷 1064/1227。覆盖它们等于守住一大类「纳管即失败」的坑。
"""
from app import mariadb_matrix as mm


def test_parse():
    assert mm.parse("12.3.3-MariaDB") == (12, 3, 3)
    assert mm.parse("10.6.21-MariaDB-log") == (10, 6, 21)
    assert mm.parse("") == (0, 0, 0)
    assert mm.parse(None) == (0, 0, 0)
    assert mm.parse("garbage") == (0, 0, 0)


def test_series_of():
    assert mm.series_of("12.3.3-MariaDB") == (12, 3)
    assert mm.series_of("nope") == (0, 0)


def test_support_level():
    # 未探测
    assert mm.support_level("") == mm.LEVEL_UNKNOWN
    # 低于最低支持线 -> 不支持
    assert mm.support_level("10.5.10") == mm.LEVEL_UNSUPPORTED
    # 兼容区
    assert mm.support_level("10.6.21") == mm.LEVEL_LEGACY
    assert mm.support_level("10.11.8") == mm.LEVEL_LEGACY
    # 支持区
    assert mm.support_level("11.4.3") == mm.LEVEL_SUPPORTED
    assert mm.support_level("11.8.1") == mm.LEVEL_SUPPORTED
    assert mm.support_level("12.0.1") == mm.LEVEL_SUPPORTED
    assert mm.support_level("12.2.0") == mm.LEVEL_SUPPORTED
    # 主线
    assert mm.support_level("12.3.3") == mm.LEVEL_MAINLINE
    # 向前兼容：矩阵未更新的更新系列不应误判为不支持
    assert mm.support_level("12.4.0") == mm.LEVEL_SUPPORTED
    assert mm.support_level("13.1.0") == mm.LEVEL_SUPPORTED


def test_support_check_shape_and_admission():
    c = mm.support_check("9.6.0")  # 不支持
    assert set(c) >= {"level", "label", "supported", "message", "advice", "version", "series"}
    assert c["supported"] is False
    assert c["level"] == mm.LEVEL_UNSUPPORTED

    leg = mm.support_check("10.6.21")
    assert leg["supported"] is True  # 兼容区仍纳管，仅提示
    assert leg["level"] == mm.LEVEL_LEGACY


def test_filter_privs_returns_all_when_probe_unavailable():
    privs = ("REPLICATION SLAVE", "BINLOG ADMIN", "SLAVE MONITOR")
    assert mm.filter_privs(privs, None) == list(privs)


def test_filter_privs_drops_unsupported_names():
    available = {"REPLICATION SLAVE", "REPLICATION CLIENT"}  # 大小写归一
    out = mm.filter_privs(("REPLICATION SLAVE", "BINLOG ADMIN", "SLAVE MONITOR"), available)
    assert "REPLICATION SLAVE" in out
    assert "BINLOG ADMIN" not in out
    assert "SLAVE MONITOR" not in out


def test_ha_privs_respects_server_capability():
    # 全集包含 SLAVE MONITOR（老版本可能没有）
    assert "SLAVE MONITOR" in mm.ha_privs(None)
    # 当服务器不支持时，该特权被滤掉
    out = mm.ha_privs({"REPLICATION SLAVE", "REPLICATION SLAVE ADMIN", "SUPER", "SELECT", "PROCESS"})
    assert "SLAVE MONITOR" not in out
    assert "REPLICATION SLAVE" in out


def test_ha_privs_includes_execute():
    """F15：GTID 分叉检测用 GTID_SUBSET()，MariaDB 需 EXECUTE 特权。

    缺 EXECUTE 时该查询报 1370 被 make_slave_of 的「分叉检测跳过」静默吞掉，
    等于最危险的场景（数据分叉）当前没被真正检测。EXECUTE 为通用特权，
    所有版本均支持，故运行时按服务器能力过滤后仍须保留。
    """
    assert "EXECUTE" in mm.HA_PRIVS
    assert "EXECUTE" in mm.ha_privs(None)
    assert "EXECUTE" in mm.ha_privs({"SELECT", "EXECUTE", "REPLICATION SLAVE"})


def test_optional_privs_filtering():
    out = mm.optional_privs({"REPLICATION MASTER ADMIN"})
    assert "REPLICATION MASTER ADMIN" in out
    assert "SLAVE MONITOR" not in out  # 不在服务器能力集内


def test_semisync_naming_is_master_slave_for_all_mariadb():
    # MariaDB 全系列沿用 master/slave，绝不做版本分支（否则 10.6~11.4 启动即崩）
    assert mm.semisync_prefixes("10.6.21") == ("rpl_semi_sync_master", "rpl_semi_sync_slave")
    assert mm.semisync_prefixes("12.3.3") == ("rpl_semi_sync_master", "rpl_semi_sync_slave")
    assert mm.semisync_plugin_load() == "rpl_semi_sync_master;rpl_semi_sync_slave"


def test_server_privileges_probes_real_names():
    cur = __import__("tests.conftest", fromlist=["FakeCursor"]).FakeCursor()
    cur.add("SHOW PRIVILEGES", [("REPLICATION SLAVE",), ("BINLOG ADMIN",), ("SELECT",)])
    out = mm.server_privileges(cur)
    assert out == {"REPLICATION SLAVE", "BINLOG ADMIN", "SELECT"}


def test_server_privileges_returns_none_on_failure():
    class Boom:
        def execute(self, *a, **k):
            raise RuntimeError("no permission")
        def fetchall(self):
            return []
    assert mm.server_privileges(Boom()) is None


def test_matrix_rows_contains_mainline_and_unsupported_floor():
    rows = mm.matrix_rows()
    series = {r["series"] for r in rows}
    assert "12.3" in series
    assert "< 10.6" in series
    levels = {r["level"] for r in rows}
    assert mm.LEVEL_MAINLINE in levels
    assert mm.LEVEL_UNSUPPORTED in levels
