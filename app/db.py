"""管理库连接与表结构初始化。

注意：每次 get_conn() 返回「新连接」，避免多线程（监控线程 + Web 线程池）共享
同一 PyMySQL 连接导致协议错乱。调用方负责在使用后关闭连接。
"""
import pymysql
from pymysql.cursors import DictCursor
from .config import MGMT


def _create():
    return pymysql.connect(
        host=MGMT["host"], port=MGMT["port"], user=MGMT["user"],
        password=MGMT["password"], database=MGMT["db"], charset="utf8mb4",
        cursorclass=DictCursor, connect_timeout=5,
    )


def get_conn():
    """返回一个新的管理库连接。管理库不可用时抛异常，由调用方处理降级。"""
    return _create()


SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS hosts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        host VARCHAR(64) NOT NULL,
        port INT NOT NULL,
        role VARCHAR(16) DEFAULT 'unknown',
        server_id BIGINT,
        status VARCHAR(8) DEFAULT 'unknown',
        is_current_master TINYINT(1) DEFAULT 0,
        initial_role VARCHAR(8) DEFAULT 'slave',
        err TEXT,
        repl_delay INT DEFAULT NULL,
        slave_io VARCHAR(16),
        slave_sql VARCHAR(16),
        last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_host (host, port)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS current_master (
        id TINYINT PRIMARY KEY,
        host VARCHAR(64) NOT NULL,
        port INT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS switch_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        from_host VARCHAR(64),
        from_port INT,
        to_host VARCHAR(64),
        to_port INT,
        reason VARCHAR(255),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS settings (
        `key` VARCHAR(64) PRIMARY KEY,
        value TEXT
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(64) UNIQUE,
        password_hash VARCHAR(128),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS nodes (
        ip VARCHAR(64) PRIMARY KEY,
        port INT NOT NULL,
        root_user VARCHAR(64),
        root_pass VARCHAR(255) DEFAULT '',
        server_id INT,
        compose_path VARCHAR(255),
        cluster_id INT DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS clusters (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(64) NOT NULL,
        ha_user VARCHAR(64),
        ha_pass VARCHAR(255) DEFAULT '',
        repl_user VARCHAR(64),
        repl_pass VARCHAR(255) DEFAULT '',
        app_user VARCHAR(64),
        app_pass VARCHAR(255) DEFAULT '',
        vip VARCHAR(45),
        vip_prefix VARCHAR(4) DEFAULT '24',
        interface VARCHAR(32),
        vrid VARCHAR(8),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS alert_emails (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        is_default TINYINT(1) DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_email (email)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS alert_email_map (
        email_id INT NOT NULL,
        cluster_id INT NOT NULL,
        PRIMARY KEY (email_id, cluster_id)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：指标历史快照（monitor 周期落库，供趋势图）----
    """
    CREATE TABLE IF NOT EXISTS metric_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        seconds_behind INT, slave_io VARCHAR(16), slave_sql VARCHAR(16),
        ro TINYINT(1), connections INT, repl_delay INT,
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_host (cluster_id, host, port, ts)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：慢日志采集（节点 mysql.slow_log 采集后 TRUNCATE，保持节点侧极小）----
    """
    CREATE TABLE IF NOT EXISTS slow_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        query_time_ms INT, lock_time_ms INT, rows_sent INT, rows_examined INT,
        db VARCHAR(64), sql_text MEDIUMTEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_host (cluster_id, host, port, ts)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：跨节点配置一致性巡检结果（定时/手动跑）----
    """
    CREATE TABLE IF NOT EXISTS config_drift (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        var_name VARCHAR(64), expected VARCHAR(255), actual VARCHAR(255),
        status VARCHAR(8),
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_cid (cluster_id, ts)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：操作审计日志 ----
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        actor VARCHAR(64), action VARCHAR(64), target VARCHAR(128),
        detail TEXT, cluster_id INT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_ts (created_at)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：自愈/告警状态记录（供告警「已自愈/未恢复」状态标记）----
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        alert_type VARCHAR(32),
        status VARCHAR(16),
        message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_host (cluster_id, host, port)
    ) ENGINE=InnoDB
    """,
]


def init_schema():
    conn = pymysql.connect(
        host=MGMT["host"], port=MGMT["port"], user=MGMT["user"],
        password=MGMT["password"], charset="utf8mb4", cursorclass=DictCursor,
    )
    try:
        with conn.cursor() as c:
            try:
                # 外部管理库账号常无 CREATE DATABASE 权限；库已存在则忽略
                c.execute(f"CREATE DATABASE IF NOT EXISTS `{MGMT['db']}`")
            except Exception:
                pass
        try:
            conn.select_db(MGMT["db"])
        except Exception:
            raise SystemExit(
                f"[init_db] 管理库 `{MGMT['db']}` 不存在且当前账号无法创建，"
                f"请先手动建库或授予建库权限。"
            )
        with conn.cursor() as c:
            for st in SCHEMA_SQL:
                c.execute(st)
            # 兼容旧版 schema：将 port 为 NULL 的历史行补默认，并确保 NOT NULL
            try:
                c.execute("UPDATE hosts SET port=3306 WHERE port IS NULL")
                c.execute("ALTER TABLE hosts MODIFY port INT NOT NULL DEFAULT 3306")
            except Exception:
                pass
            # 兼容目录调整：将已生成节点的 compose_path 统一刷新为新基路径
            # （generated_nodes 移到项目根目录后，旧的 data/Nodes-compose/<ip> 路径已失效）
            try:
                from .config import NODES_HOST_BASE
                for r in query("SELECT ip FROM nodes"):
                    execute(
                        "UPDATE nodes SET compose_path=%s WHERE ip=%s",
                        (f"{NODES_HOST_BASE}/{r['ip']}", r["ip"]),
                    )
            except Exception:
                pass
            # 兼容旧 schema：新增 initial_role 列（记录节点生成时指定的初始主/从）
            try:
                c.execute(
                    "ALTER TABLE hosts ADD COLUMN initial_role VARCHAR(8) DEFAULT 'slave'"
                )
            except Exception:
                pass
            # 兼容旧 schema：新增 err 列（记录节点探活失败原因，便于前端展示）
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN err TEXT")
            except Exception:
                pass
            # 兼容旧 schema：nodes 表新增 root_pass（节点 MySQL root 密码，用于 ha_monitor
            # 凭证自愈与切换的 root 回退；root 密码在容器创建时固定，不随 HA_PASS 漂移）
            try:
                c.execute("ALTER TABLE nodes ADD COLUMN root_pass VARCHAR(255) DEFAULT ''")
            except Exception:
                pass
            # 兼容旧 schema：switch_history 增加 cluster_id（按集群区分切换记录）
            try:
                c.execute("ALTER TABLE switch_history ADD COLUMN cluster_id INT DEFAULT NULL")
            except Exception:
                pass
            # ---- 多集群支持：新增 clusters 表 + hosts/nodes 的 cluster_id ----
            try:
                c.execute(
                    """CREATE TABLE IF NOT EXISTS clusters (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(64) NOT NULL,
                        ha_user VARCHAR(64),
                        ha_pass VARCHAR(255) DEFAULT '',
                        repl_user VARCHAR(64),
                        repl_pass VARCHAR(255) DEFAULT '',
                        app_user VARCHAR(64),
                        app_pass VARCHAR(255) DEFAULT '',
                        vip VARCHAR(45),
                        vip_prefix VARCHAR(4) DEFAULT '24',
                        interface VARCHAR(32),
                        vrid VARCHAR(8),
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB"""
                )
            except Exception:
                pass
            # clusters 增加 vip_port（VIP 对外访问端口，业务连 VIP:VIP_PORT 访问主库）
            try:
                c.execute("ALTER TABLE clusters ADD COLUMN vip_port INT NOT NULL DEFAULT 3307")
            except Exception:
                pass
            # ---- v2.1：节点/集群支持裸机（node_type）与 MySQL 大版本（mysql_version）----
            # 默认 node_type='docker' 行为完全不变；mysql_version=NULL 表示沿用旧复制语法。
            for _tbl, _col, _def in (
                ("nodes", "node_type", "varchar(16) NOT NULL DEFAULT 'docker'"),
                ("nodes", "mysql_version", "varchar(16) DEFAULT NULL"),
                ("clusters", "node_type", "varchar(16) NOT NULL DEFAULT 'docker'"),
                ("clusters", "mysql_version", "varchar(16) DEFAULT NULL"),
            ):
                try:
                    c.execute(f"ALTER TABLE {_tbl} ADD COLUMN {_col} {_def}")
                except Exception:
                    pass
            # hosts 增加 cluster_id
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN cluster_id INT DEFAULT NULL")
            except Exception:
                pass
            # nodes 增加 cluster_id
            try:
                c.execute("ALTER TABLE nodes ADD COLUMN cluster_id INT DEFAULT NULL")
            except Exception:
                pass
            # current_master 改为按集群（cluster_id 主键），兼容旧版单列(id=1)结构。
            # 仅当 current_master 仍是旧结构（含 id 列、无 cluster_id 列）时迁移一次，
            # 避免每次重启都把当前主数据清空。
            try:
                c.execute("SHOW COLUMNS FROM current_master")
                cm_cols = [r["Field"] for r in c.fetchall()]
            except Exception:
                cm_cols = []
            if "cluster_id" not in cm_cols and "id" in cm_cols:
                try:
                    c.execute(
                        """CREATE TABLE IF NOT EXISTS current_master_new (
                            cluster_id INT PRIMARY KEY,
                            host VARCHAR(64) NOT NULL,
                            port INT NOT NULL,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB"""
                    )
                    c.execute("SELECT id, host, port FROM current_master")
                    for r in c.fetchall():
                        c.execute(
                            "INSERT IGNORE INTO current_master_new (cluster_id, host, port) VALUES (%s,%s,%s)",
                            (1, r["host"], r["port"]),
                        )
                    c.execute("DROP TABLE IF EXISTS current_master")
                    c.execute("ALTER TABLE current_master_new RENAME TO current_master")
                except Exception as e:
                    print(f"[init_db] 迁移 current_master 失败(可忽略): {e}")
            # 首次迁移：把既有 hosts/nodes 归入默认集群(id=1)，并从旧 settings 迁移凭证
            try:
                c.execute("SELECT COUNT(*) AS n FROM clusters")
                if (c.fetchone() or {}).get("n", 0) == 0:
                    # 从 settings 读取旧凭证
                    def _s(k):
                        c.execute("SELECT value FROM settings WHERE `key`=%s", (k,))
                        row = c.fetchone()
                        return row["value"] if row else None
                    name = _s("CLUSTER_NAME") or "默认集群"
                    ha_user = _s("HA_USER") or "ha_monitor"
                    ha_pass = _s("HA_PASS")
                    repl_user = _s("REPL_USER") or "replication"
                    repl_pass = _s("REPL_PASS")
                    app_user = _s("APP_USER") or "app"
                    app_pass = _s("APP_PASS")
                    vip = _s("VIP")
                    vip_prefix = _s("VIP_PREFIX") or "24"
                    interface = _s("INTERFACE") or "eth0"
                    vrid = _s("VRID") or "51"
                    c.execute(
                        """INSERT INTO clusters
                        (id, name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                         vip, vip_prefix, interface, vrid)
                        VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                         vip, vip_prefix, interface, vrid),
                    )
                    c.execute("UPDATE hosts SET cluster_id=1 WHERE cluster_id IS NULL")
                    c.execute("UPDATE nodes SET cluster_id=1 WHERE cluster_id IS NULL")
            except Exception as e:
                print(f"[init_db] 迁移默认集群失败(可忽略): {e}")
        # v2.1：保留期默认值（仅首次写入，已存在则不动）
        for _k, _v in (("METRIC_RETENTION_DAYS", "30"), ("SLOWLOG_RETENTION_DAYS", "7")):
            if get_setting(_k) is None:
                upsert_setting(_k, _v)
        conn.commit()
    finally:
        conn.close()


def query(sql, args=None):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute(sql, args)
            return c.fetchall()
    finally:
        conn.close()


def execute(sql, args=None):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute(sql, args)
        conn.commit()
    finally:
        conn.close()


def upsert_setting(key, value):
    execute(
        "INSERT INTO settings (`key`, value) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE value=%s",
        (key, value, value),
    )


def get_setting(key, default=None):
    rows = query("SELECT value FROM settings WHERE `key`=%s", (key,))
    if rows:
        return rows[0]["value"]
    return default
