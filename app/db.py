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
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
