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
import re
import pymysql
from pymysql import OperationalError, ProgrammingError, DatabaseError


# 可安全做「空事务跳过」的复制错误码集合（均为幂等类冲突：对象/记录已存在或已不存在，
# 说明该事务的效果在当前节点实际上已经完成，跳过不会造成数据丢失）。
_SKIPPABLE_SQL_ERRNOS = frozenset({1007, 1050, 1061, 1062, 1396, 1032, 1146})


def _one_dict(cur):
    """把当前结果集的第一行转成 dict（按列名索引）。

    关键：connect() 默认使用元组游标，直接用 row[key] 会失败；
    务必先按 cur.description 转 dict，否则读不到 Slave_SQL_Running 等字段。
    """
    if not cur.description:
        return None
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def heal_replication(host, port, user, password, root_user="root", root_pass=""):
    """自愈复制：当 Slave_SQL_Running=No 且错误为幂等冲突时，注入空事务跳过该 GTID。

    典型场景：旧主重入 / 切换后，某节点物理数据已包含某事务（如 CREATE DATABASE）
    但其 GTID 执行集合未记录该事务，新主重放时触发 1007 等「已存在」错误导致
    SQL 线程中断。此时用 SET GTID_NEXT='<冲突GTID>'; BEGIN; COMMIT; 显式标记该
    GTID 已执行，再 START SLAVE 即可继续，无需 RESET MASTER（RESET MASTER 会清空
    GTID 集合、反而让同名事务被再次重放）。

    返回 (ok, msg)。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        cur.execute("SHOW SLAVE STATUS")
        row = _one_dict(cur)
        if not row:
            return False, "无复制通道（该节点非从库）"

        def g(key):
            return row.get(key)

        slave_sql = g("Slave_SQL_Running") or g("Replica_SQL_Running") or ""
        last_err = (g("Last_SQL_Error") or g("Replica_SQL_Error") or "") or ""
        last_code = g("Last_SQL_Errno") or g("Replica_SQL_Errno") or 0
        try:
            last_code = int(last_code)
        except (TypeError, ValueError):
            last_code = 0

        if slave_sql == "Yes":
            return True, "复制正常，无需自愈"
        if not last_err:
            # SQL 线程停了但没有错误信息，尝试直接重启
            try:
                cur.execute("START SLAVE")
                return True, "已尝试重启复制线程"
            except Exception as e:
                return False, str(e)
        if last_code and last_code not in _SKIPPABLE_SQL_ERRNOS:
            return False, f"复制错误码 {last_code} 非幂等冲突，需人工处理：{last_err}"
        # 从错误信息中提取冲突事务的 GTID（如 'uuid:N'）
        m = re.search(r"executing transaction '([0-9a-fA-F-]+:\d+)'", last_err)
        if not m:
            try:
                cur.execute("START SLAVE")
                return True, "已尝试重启复制线程(无 GTID 可跳过)"
            except Exception as e:
                return False, str(e)
        gtid = m.group(1)
        try:
            cur.execute("STOP SLAVE")
            cur.execute(f"SET GTID_NEXT='{gtid}'")
            cur.execute("BEGIN")
            cur.execute("COMMIT")
            cur.execute("SET GTID_NEXT='AUTOMATIC'")
            cur.execute("START SLAVE")
            return True, f"已跳过冲突事务 {gtid}，复制恢复"
        except Exception as e:
            return False, str(e)
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def parse_gtid_set(s):
    """解析 GTID 集合字符串为 {uuid: set([ints])}。

    支持形如 'uuid1:1-33,uuid2:1-19' 或 'uuid:1-5:7:9-11' 的写法。
    """
    res = {}
    if not s:
        return res
    for part in s.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        uuid, ranges = part.split(":", 1)
        nums = set()
        for r in ranges.split(":"):
            r = r.strip()
            if not r:
                continue
            if "-" in r:
                a, b = r.split("-", 1)
                try:
                    nums.update(range(int(a), int(b) + 1))
                except ValueError:
                    pass
            else:
                try:
                    nums.add(int(r))
                except ValueError:
                    pass
        res[uuid] = nums
    return res


def heal_io_gtid_gap(slave_host, slave_port, ha_user, ha_pass,
                     slave_root_user="root", slave_root_pass="",
                     master_host=None, master_port=None,
                     master_root_user="root", master_root_pass=""):
    """自愈复制 IO 线程 13114/1236：从库报「Replica has more GTIDs than the source has」。

    根因：复制源（主库）自身丢失了本应存在的 GTID（binlog 被截断/回滚，或源端 RESET
    MASTER），而从库已执行过这些 GTID。修复须在【源端/主库】为这些缺失 GTID 注入空事务，
    从库 IO 线程即可恢复重连。与 SQL 线程冲突跳过（作用在从库）不同，此修复作用在「主库」。

    返回 (acted, msg)：acted=True 表示已执行注入/恢复；False 表示无需处理或失败。
    """
    sconn = connect_robust(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user, slave_root_pass, timeout=5)
    if sconn is None:
        return False, "无法连接从库(ha_monitor/root均失败)"
    try:
        scur = sconn.cursor()
        scur.execute("SHOW REPLICA STATUS")
        row = _one_dict(scur)
        if not row:
            return False, "从库无复制通道，无需处理"
        def g(key):
            return row.get(key)
        io_err = (g("Last_IO_Error") or "") or ""
        # 仅处理「从库比源多 GTID」这一类 IO 错误
        if ("more GTIDs than the source" not in io_err
                and "13114" not in io_err and "1236" not in io_err):
            # 透出真实 IO 错误文本，便于从库侧直接看到连不上主库的真实原因
            return False, ("IO 错误非 GTID 缺失类，跳过" + (("：" + io_err) if io_err else ""))
        # 待修复的源端 = 从库实际连接的源（优先从复制状态读取，回退到传入的主库）
        target_host = g("Source_Host") or g("Master_Host") or master_host
        try:
            target_port = int(g("Source_Port") or g("Master_Port") or master_port or 0)
        except (TypeError, ValueError):
            target_port = 0
        source_uuid = g("Source_UUID") or g("Master_UUID") or ""
        executed = g("Executed_Gtid_Set") or ""
        if not source_uuid or not target_host or not target_port:
            return False, "无法确定源端标识，跳过"
        gtids = parse_gtid_set(executed)
        if source_uuid not in gtids or not gtids[source_uuid]:
            return False, "从库未执行源端 GTID，无需注入"
        # 连源端（主库），读其 gtid_executed，仅注入主库缺失的部分
        mconn = connect_robust(target_host, target_port, ha_user, ha_pass,
                               master_root_user, master_root_pass, timeout=5)
        if mconn is None:
            return False, f"无法连接源端主库 {target_host}:{target_port}(ha_monitor/root均失败)"
        try:
            mcur = mconn.cursor()
            mcur.execute("SELECT @@GLOBAL.gtid_executed")
            master_executed = (mcur.fetchone() or (None,))[0] or ""
            master_has = parse_gtid_set(master_executed).get(source_uuid, set())
            missing = sorted(n for n in gtids[source_uuid] if n not in master_has)
            if missing:
                try:
                    for n in missing:
                        mcur.execute(f"SET GTID_NEXT='{source_uuid}:{n}'")
                        mcur.execute("BEGIN")
                        mcur.execute("COMMIT")
                    mcur.execute("SET GTID_NEXT='AUTOMATIC'")
                except Exception as e:
                    try:
                        mcur.execute("SET GTID_NEXT='AUTOMATIC'")
                    except Exception:
                        pass
                    return False, f"在源端注入空事务失败: {e}"
            # 重启从库 IO 线程，立即清除报错（Connect_Retry 默认 60s，主动重启更快）
            try:
                scur.execute("STOP REPLICA IO_THREAD")
                scur.execute("START REPLICA IO_THREAD")
            except Exception:
                try:
                    scur.execute("STOP SLAVE IO_THREAD")
                    scur.execute("START SLAVE IO_THREAD")
                except Exception:
                    pass
            return True, (f"已在源端 {target_host}:{target_port} 为 {source_uuid} "
                          f"补齐 {len(missing)} 个缺失 GTID，并重启从库 IO 线程")
        finally:
            try:
                mconn.close()
            except Exception:
                pass
    except Exception as e:
        return False, str(e)
    finally:
        try:
            sconn.close()
        except Exception:
            pass


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
        seconds_behind=None, slave_io=None, slave_sql=None,
        slave_io_err="", slave_sql_err="",
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
                result["slave_io"] = d.get("Slave_IO_Running") or d.get("Replica_IO_Running")
                result["slave_sql"] = d.get("Slave_SQL_Running") or d.get("Replica_SQL_Running")
                result["slave_io_err"] = (d.get("Last_IO_Error") or d.get("Last_IO_Error") or "").strip()
                result["slave_sql_err"] = (d.get("Last_SQL_Error") or d.get("Last_SQL_Error") or "").strip()
                # 注意：复制延迟为 0（健康、已追平）时不能用 `or` 回退——
                # `0 or Seconds_Behind_Source(None)` 会把健康的 0 误判为 None，
                # 导致仪表盘「复制延迟」全部显示「—」。必须用 None 哨兵判断。
                sb = d.get("Seconds_Behind_Master")
                if sb is None:
                    sb = d.get("Seconds_Behind_Source")
                result["seconds_behind"] = sb
                result["master_host"] = d.get("Master_Host") or d.get("Source_Host")
                result["master_port"] = d.get("Master_Port") or d.get("Source_Port")
            else:
                # 无复制通道：主库或尚未建立复制。slave_io/sql 保持 None，避免被误判为 No/No 故障
                result["mode"] = result.get("mode") or "master"
                result["role"] = result.get("role") or "master"
                result["slave_io"] = None
                result["slave_sql"] = None
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


def get_gtid_executed(host, port, user, password, root_user="root", root_pass="", timeout=5):
    """读取节点 @@GLOBAL.gtid_executed（主从均适用），用于选主时比较复制进度。

    返回字符串；连接失败或查询异常返回 None。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.gtid_executed")
            row = cur.fetchone()
            return (row[0] if row and row[0] else "") or ""
    except Exception:
        return None
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
    """把本节点改造为 master_host 的从库（GTID 自动定位）。

    重指向前先 RESET SLAVE ALL 清掉残留复制通道，再以 MASTER_AUTO_POSITION=1
    重新挂载到新主库。GTID 自动定位会跳过本节点「已经执行过」的事务，
    因此【严禁 RESET MASTER】：一旦清空本机 GTID 执行集合，新主库会把它
    曾发过的同名事务（如 CREATE DATABASE）当作新事务重放，从而触发
    「database exists / duplicate entry」等 1007/1062 错误，导致
    Slave_SQL_Running=No。依赖 GTID 自动定位即可干净重入，无需 RESET MASTER。

    返回 (ok, msg)。
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
        # 注意：这里不再 RESET MASTER，避免 GTID 执行集合被清空后
        # 旧事务被新主库重放而报「已存在」类错误。
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


def is_stale_master(status):
    """判断一个节点是否为「残留的独立主库」（非当前主、但自己是主且无复制通道）。

    用于 ensure_topology 决定是否需要对旧主上线做 RESET MASTER 干净重入。
    """
    if not status:
        return False
    return status.get("mode") == "master" and not status.get("master_host")


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
