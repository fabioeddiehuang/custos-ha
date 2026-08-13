# -*- coding: utf-8 -*-
"""
与 MySQL 节点交互的客户端工具。

重要设计：所有操作都支持「root 回退」。
- 业务/监控使用 ha_monitor 账号，但其密码可能因改 HA_PASS 而漂移。
- 节点 MySQL 的 root 密码在容器创建时固定（compose 的 MYSQL_ROOT_PASSWORD），
  不随 HA_PASS 漂移。因此当 ha_monitor 连不上时，回退用 root 执行，
  保证只读探活/切换/复制搭建/账号自愈都能成功，VIP 不再因 ha_monitor
  密码漂移而卡在某一个节点。
"""
import pymysql
from pymysql import OperationalError, ProgrammingError, DatabaseError


def connect(host, port, user, password, timeout=5):
    """建立连接；失败时抛 RuntimeError（带原始异常信息）。"""
    try:
        return pymysql.connect(
            host=host, port=int(port), user=user, password=password,
            connect_timeout=timeout, autocommit=True,
            charset="utf8mb4",
        )
    except Exception as e:
        raise RuntimeError(str(e))


def connect_robust(host, port, user, password, root_user="root", root_pass="", timeout=5):
    """
    优先用 ha_monitor(user/password) 连接；
    失败（账号漂移/拒绝）时回退用 root(root_user/root_pass) 连接。
    """
    try:
        return connect(host, port, user, password, timeout=timeout)
    except Exception:
        if root_pass:
            return connect(host, port, root_user, root_pass, timeout=timeout)
        raise


def get_status(host, port, user, password, root_user="root", root_pass="", timeout=3):
    """探活并返回节点状态字典（供 monitor/failover 使用）。

    返回字段：alive, mode(master/slave/unknown), ro(0/1/None), role,
    seconds_behind, slave_io, slave_sql, master_host, master_port, error。
    """
    result = dict(
        alive=False, mode="unknown", ro=None, role="unknown",
        seconds_behind=None, slave_io="No", slave_sql="No",
        master_host=None, master_port=None, error=None,
    )
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception as e:
        result["error"] = str(e)
        return result
    cur = conn.cursor()
    try:
        try:
            cur.execute("SELECT @@read_only")
            result["ro"] = cur.fetchone()[0]
        except Exception:
            result["ro"] = None
        try:
            cur.execute("SHOW SLAVE STATUS")
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                d = dict(zip(cols, row))
                result["mode"] = "slave"
                result["role"] = "slave"
                result["slave_io"] = d.get("Slave_IO_Running")
                result["slave_sql"] = d.get("Slave_SQL_Running")
                result["seconds_behind"] = d.get("Seconds_Behind_Master")
                result["master_host"] = d.get("Master_Host")
                result["master_port"] = d.get("Master_Port")
            else:
                result["mode"] = "master"
                result["role"] = "master"
        except Exception:
            pass
        result["alive"] = True
        return result
    except Exception as e:
        result["error"] = str(e)
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def set_read_only(host, port, user, password, on=True, root_user="root", root_pass=""):
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        cur = conn.cursor()
        cur.execute("SET GLOBAL read_only=%s", (1 if on else 0,))
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def promote_to_master(host, port, user, password, root_user="root", root_pass=""):
    """提升为可写主库：先清除残留复制通道，再关闭 read_only。

    必须先 STOP SLAVE + RESET SLAVE ALL，否则本节点仍挂着 SHOW SLAVE STATUS（指向旧主），
    monitor 会把它误判为 slave，且可能与新主形成环形复制（多次手动切换后「全变成从库」的根因）。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        try:
            cur.execute("STOP SLAVE")
            cur.execute("RESET SLAVE ALL")
        except Exception:
            pass
        cur.execute("SET GLOBAL read_only=0")
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def make_slave_of(host, port, user, password, master_host, master_port,
                  repl_user, repl_pass, root_user="root", root_pass=""):
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        cur = conn.cursor()
        cur.execute("STOP SLAVE")
        cur.execute(
            "CHANGE MASTER TO MASTER_HOST=%s, MASTER_PORT=%s, "
            "MASTER_USER=%s, MASTER_PASSWORD=%s, "
            "MASTER_AUTO_POSITION=1, MASTER_SSL=0",
            (master_host, int(master_port), repl_user, repl_pass),
        )
        cur.execute("START SLAVE")
        # 从库设为只读，避免双写脑裂：仅当前主库可写。
        try:
            cur.execute("SET GLOBAL read_only=1")
        except Exception:
            pass
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def stop_slave(host, port, user, password, root_user="root", root_pass=""):
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        conn.cursor().execute("STOP SLAVE")
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def start_slave(host, port, user, password, root_user="root", root_pass=""):
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        conn.cursor().execute("START SLAVE")
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def reset_slave(host, port, user, password, root_user="root", root_pass=""):
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        cur = conn.cursor()
        cur.execute("STOP SLAVE")
        cur.execute("RESET SLAVE ALL")
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def set_local_master_flag(host, port, user, password, master_str, root_user="root", root_pass=""):
    """把「当前主库 host:port」写入本节点本地 MySQL 的 ha_local.state。

    由管理端在切换主库时调用，供 keepalived 检查脚本本地读取判定 VIP 归属。
    完全本地、不依赖节点回连管理库，彻底规避管理库不可达导致 VIP 不漂移。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS ha_local")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS ha_local.state "
            "(k VARCHAR(32) PRIMARY KEY, v VARCHAR(255))"
        )
        cur.execute(
            "REPLACE INTO ha_local.state (k, v) VALUES ('current_master', %s)",
            (master_str,),
        )
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def alter_user_pass(host, port, user, password, target_user, target_pass,
                    root_user="root", root_pass=""):
    """
    用 (user/password) 连接后，将 target_user@'%' 的密码改为 target_pass。
    回退：若 user 连不上，则用 root 连接执行。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        cur = conn.cursor()
        cur.execute(
            "ALTER USER %s@'%%' IDENTIFIED WITH mysql_native_password BY %s",
            (target_user, target_pass),
        )
        cur.execute("FLUSH PRIVILEGES")
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_ha_creds(host, port, root_user, root_pass,
                    ha_user, ha_pass, repl_user, repl_pass,
                    app_user="", app_pass=""):
    """
    用 root 将节点上的 ha_monitor / replication / app 账号密码「自愈」为当前设定值。
    这是防止 ha_monitor 密码漂移的权威手段：root 密码在容器创建时固定，
    无论 ha_monitor 当前密码是什么，都能连上并校正。
    返回 (ok, msg)。
    """
    try:
        conn = connect(host, port, root_user, root_pass, timeout=5)
    except Exception as e:
        return False, "root 连接失败: %s" % e
    cur = conn.cursor()
    try:
        pairs = [(ha_user, ha_pass), (repl_user, repl_pass)]
        if app_user and app_pass:
            pairs.append((app_user, app_pass))
        for u, p in pairs:
            cur.execute(
                "ALTER USER %s@'%%' IDENTIFIED WITH mysql_native_password BY %s",
                (u, p),
            )
        cur.execute("FLUSH PRIVILEGES")
        return True, "ok"
    except Exception as e:
        return False, "校正失败: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass
