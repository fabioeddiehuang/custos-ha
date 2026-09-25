"""mysql_compat 单测：跨风味复制 SQL 生成 + 风味分派。

这是 TODO 1.3「MySQL 侧冻结策略与回归清单」的核心落点：mysql_compat 是
MySQL 8.0 与 MariaDB 两条复制路径的**共享分派层**，改 MariaDB 极易误伤 MySQL。
本文件同时构造两套输入，确保同一逻辑在两条路径都不回归（双路径回归）。
"""
from app import mysql_compat as mc
from regression.conftest import FakeCursor


# ---------------------------------------------------------------- 版本/风味判定

def test_parse_version():
    assert mc.parse_version("8.0.36-cluster") == (8, 0, 36)
    assert mc.parse_version("12.3.3-MariaDB") == (12, 3, 3)
    assert mc.parse_version("") == (0, 0, 0)
    assert mc.parse_version(None) == (0, 0, 0)


def test_is_mariadb():
    assert mc.is_mariadb("12.3.3-MariaDB") is True
    assert mc.is_mariadb("8.0.36") is False
    assert mc.is_mariadb("5.7.40") is False


def test_flavour_from_version():
    assert mc.flavour_from_version("12.3.3-MariaDB") == mc.F_MARIADB
    assert mc.flavour_from_version("8.0.36") == mc.F_MYSQL8      # >=8.0.22 新语法
    assert mc.flavour_from_version("8.0.21") == mc.F_OLD         # <8.0.22 旧语法
    assert mc.flavour_from_version("5.7.40") == mc.F_OLD


def test_flavour_from_node_type():
    assert mc.flavour_from_node_type("docker") == mc.F_MYSQL8
    assert mc.flavour_from_node_type("mariadb") == mc.F_MARIADB
    assert mc.flavour_from_node_type("mariadb-docker") == mc.F_MARIADB
    assert mc.flavour_from_node_type("unknown") == mc.F_OLD


def test_node_family_groups_mariadb_forms():
    assert mc.node_family("mariadb") == "mariadb"
    assert mc.node_family("mariadb-docker") == "mariadb"
    assert mc.node_family("docker") == "mysql"


def test_is_mariadb_node():
    assert mc.is_mariadb_node("mariadb") is True
    assert mc.is_mariadb_node("mariadb-docker") is True
    assert mc.is_mariadb_node("docker") is False


def test_use_new_syntax():
    assert mc.use_new_syntax(None) is False
    assert mc.use_new_syntax("12.3.3-MariaDB") is False
    assert mc.use_new_syntax("8.0.36") is True
    assert mc.use_new_syntax("8.0.21") is False


def test_detect_flavour_via_cursor():
    maria = FakeCursor().add("SELECT VERSION()", [("12.3.3-MariaDB-Log",)])
    assert mc.detect_flavour(maria) == mc.F_MARIADB
    mysql = FakeCursor().add("SELECT VERSION()", [("8.0.36",)])
    assert mc.detect_flavour(mysql) == mc.F_MYSQL8
    # 异常时回退 old，绝不抛
    class Boom:
        def execute(self, *a, **k):
            raise RuntimeError()
        def fetchone(self):
            return None
    assert mc.detect_flavour(Boom()) == mc.F_OLD


# ---------------------------------------------------------------- 复制挂载 SQL 双路径

def test_change_replication_sql_mysql8():
    sql = mc.change_replication_sql(mc.F_MYSQL8, "10.0.0.2", 3306, "repl", "pw")
    assert "CHANGE REPLICATION SOURCE TO" in sql
    assert "SOURCE_AUTO_POSITION=1" in sql
    assert "SOURCE_HOST='10.0.0.2'" in sql
    assert "SOURCE_PORT=3306" in sql
    assert "SOURCE_USER='repl'" in sql
    assert "SOURCE_PASSWORD='pw'" in sql
    assert "MASTER_USE_GTID" not in sql


def test_change_replication_sql_mariadb():
    sql = mc.change_replication_sql(mc.F_MARIADB, "10.0.0.2", 3306, "repl", "pw")
    assert "CHANGE MASTER TO" in sql
    assert "MASTER_USE_GTID=slave_pos" in sql
    assert "MASTER_SSL=0" in sql           # WolfSSL/OpenSSL 互操作必须关 TLS
    assert "MASTER_HOST='10.0.0.2'" in sql
    assert "MASTER_PORT=3306" in sql
    # MariaDB 绝不用 MySQL 8 的 SOURCE 术语
    assert "SOURCE_" not in sql
    assert "AUTO_POSITION" not in sql


def test_change_replication_sql_old_mysql():
    sql = mc.change_replication_sql(mc.F_OLD, "10.0.0.2", 3306, "repl", "pw")
    assert "CHANGE MASTER TO" in sql
    assert "MASTER_AUTO_POSITION=1" in sql
    assert "MASTER_USE_GTID" not in sql


def test_change_replication_sql_public_key_and_anon():
    sql = mc.change_replication_sql(mc.F_MYSQL8, "h", 3306, "u", "p",
                                     public_key=True, assign_anon_gtids=True)
    assert "SOURCE_PUBLIC_KEY=1" in sql
    assert "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL" in sql
    # MariaDB 忽略 public_key
    sql_m = mc.change_replication_sql(mc.F_MARIADB, "h", 3306, "u", "p", public_key=True)
    assert "PUBLIC_KEY" not in sql_m


def test_change_replication_sql_channel():
    sql = mc.change_replication_sql(mc.F_MYSQL8, "h", 3306, "u", "p", channel="ch")
    assert "FOR CHANNEL 'ch'" in sql


# ---------------------------------------------------------------- 复制启停/状态 SQL

def test_replication_verb_terms_by_flavour():
    assert mc.start_replication_sql(mc.F_MYSQL8) == "START REPLICA"
    assert mc.start_replication_sql(mc.F_MARIADB) == "START SLAVE"
    assert mc.stop_replication_sql(mc.F_MARIADB) == "STOP SLAVE"
    assert mc.reset_replica_sql(mc.F_MYSQL8) == "RESET REPLICA ALL"
    assert mc.reset_replica_sql(mc.F_OLD, all_=False) == "RESET SLAVE"
    assert mc.show_replica_status_sql(mc.F_MYSQL8) == "SHOW REPLICA STATUS"
    assert mc.show_replica_status_sql(mc.F_MARIADB) == "SHOW SLAVE STATUS"
    assert mc.show_replicas_sql(mc.F_MYSQL8) == "SHOW REPLICAS"
    assert mc.show_replicas_sql(mc.F_MARIADB) == "SHOW SLAVE HOSTS"


def test_change_replication_filter_mariadb_is_none():
    # MariaDB 根本没有 CHANGE REPLICATION FILTER 语句，必须返回 None（否则每条都刷 1064）
    assert mc.change_replication_filter_sql(["ha_local"], flavour=mc.F_MARIADB) is None
    sql = mc.change_replication_filter_sql(["ha_local"], flavour=mc.F_MYSQL8)
    assert sql and "CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB" in sql


# ---------------------------------------------------------------- 认证插件/账号子句

def test_default_auth_plugin():
    assert mc.default_auth_plugin("12.3.3-MariaDB") == "mysql_native_password"
    assert mc.default_auth_plugin("8.0.33") == "mysql_native_password"   # <8.0.34 仍用 native
    assert mc.default_auth_plugin("8.0.34") == "caching_sha2_password"   # >=8.0.34 切换 caching_sha2
    assert mc.default_auth_plugin("8.4.0") == "caching_sha2_password"
    # prefer 强制覆盖
    assert mc.default_auth_plugin("8.0.36", prefer="native") == "mysql_native_password"
    assert mc.default_auth_plugin("8.0.36", prefer="caching_sha2") == "caching_sha2_password"
    # 版本未知：MariaDB 节点按 node_type 判定，docker 沿用既有行为
    assert mc.default_auth_plugin(None, node_type="mariadb") == "mysql_native_password"
    assert mc.default_auth_plugin(None, node_type="docker") == "mysql_native_password"


def test_identify_clause_mariadb_uses_using_password():
    m = mc.identify_clause("mysql_native_password", "pw", mariadb=True)
    assert "IDENTIFIED VIA mysql_native_password USING PASSWORD('pw')" in m
    n = mc.identify_clause("mysql_native_password", "pw", mariadb=False)
    assert "IDENTIFIED WITH mysql_native_password BY 'pw'" in n


# ---------------------------------------------------------------- GTID 解析双路径

def test_parse_gtid_set_mysql():
    out = mc.parse_gtid_set("uuid1:1-5,uuid1:7,uuid2:3-9")
    assert out == {"uuid1": {1, 2, 3, 4, 5, 7}, "uuid2": set(range(3, 10))}


def test_parse_gtid_set_mariadb_sequence():
    out = mc.parse_gtid_set("0-1-100,0-2-30")
    assert out == {(0, 1): {100}, (0, 2): {30}}


def test_parse_gtid_set_auto_detect_mariadb_format():
    # 无 flavour 时按格式自动判别：纯三段数字 -> MariaDB
    out = mc.parse_gtid_set("0-1-100")
    assert out == {(0, 1): {100}}


def test_looks_like_mariadb_gtid():
    assert mc._looks_like_mariadb_gtid("0-1-100,0-2-30") is True
    assert mc._looks_like_mariadb_gtid("uuid:1-5") is False
