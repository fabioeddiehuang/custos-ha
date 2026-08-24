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
import time
import pymysql
from pymysql import OperationalError, ProgrammingError, DatabaseError

# get_server_public_key 仅 PyMySQL >= 1.0.0 支持；老版本（如 0.10.x）传此参数会
# 直接抛 TypeError（Connection.__init__() got an unexpected keyword argument ...）。
# 裸机 root 若用 caching_sha2_password 且非 TLS 连接才需要它；不支持时忽略即可
# （老版本本身也无法用该方式完成 caching_sha2 认证，属预期降级，无新增回归）。
def _pymysql_at_least(major, minor):
    try:
        parts = [int(x) for x in pymysql.__version__.split(".")[:2]]
    except Exception:
        return False
    return tuple(parts) >= (major, minor)

_PYMYSQL_SUPPORTS_PUBKEY = _pymysql_at_least(1, 0)
from .mysql_compat import (detect_syntax, detect_flavour, flavour_from_node_type,
                           change_replication_sql, start_replication_sql,
                           stop_replication_sql, reset_replica_sql, show_replica_status_sql,
                           F_MYSQL8, F_MARIADB)


# 可安全做「空事务跳过」的复制错误码集合（均为幂等类冲突：对象/记录已存在或已不存在，
# 说明该事务的效果在当前节点实际上已经完成，跳过不会造成数据丢失）。
_SKIPPABLE_SQL_ERRNOS = frozenset({1007, 1050, 1061, 1062, 1396, 1032, 1146})


def _tiny_sleep(seconds=0.3):
    """自愈循环里重启复制后给 SQL 线程一点时间应用/再次停下，便于下一轮检测冲突。"""
    try:
        time.sleep(seconds)
    except Exception:
        pass


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
        syntax = detect_syntax(cur)
        # 循环跳过所有冲突事务：SQL 线程可能因多个事务连续冲突而反复停止（如 911 跳过后又
        # 在 912 处卡住）。仅处理首个冲突会导致从库 GTID 集合始终追不上主库，进而使
        # heal_anonymous_gtid_io 的安全校验失败、主库含匿名事务的 binlog 永不被清理，
        # 从库 IO 线程持续报 1236。故这里反复跳过，直到 SQL 线程持续运行或无可跳过的冲突。
        skipped = []
        for _ in range(200):
            cur.execute(show_replica_status_sql(syntax))
            row = _one_dict(cur)
            if not row:
                return False, "无复制通道（该节点非从库）"

            def g(key):
                return row.get(key)

            slave_sql = g("Slave_SQL_Running") or g("Replica_SQL_Running") or ""
            if slave_sql == "Yes":
                if skipped:
                    return True, f"已跳过冲突事务 {', '.join(skipped)}，复制恢复"
                return True, "复制正常，无需自愈"
            last_err = (g("Last_SQL_Error") or g("Replica_SQL_Error") or "") or ""
            last_code = g("Last_SQL_Errno") or g("Replica_SQL_Errno") or 0
            try:
                last_code = int(last_code)
            except (TypeError, ValueError):
                last_code = 0
            # 复制是否使用 GTID 自动定位（SHOW REPLICA STATUS 的 Auto_Position 列）。
            #  - AUTO_POSITION=1：用「SET GTID_NEXT='<冲突GTID>'+BEGIN/COMMIT」注入空事务跳过，
            #    这是最精确的方式，且不破坏 GTID 连续性。
            #  - AUTO_POSITION=0（传统 binlog 坐标复制 / 部分 MariaDB GTID 场景）：
            #    SET GTID_NEXT 不生效（本环境正是此情况——monitor 自愈静默失败、复制永久停，
            #    只能靠运维手动 `SET GLOBAL sql_slave_skip_counter=1` 才恢复）。此场景必须用
            #    sql_slave_skip_counter 跳过，否则复制永远卡在 Slave_SQL_Running=No。
            auto_pos = g("Auto_Position")
            try:
                auto_pos = int(auto_pos) if auto_pos is not None else 1
            except (TypeError, ValueError):
                auto_pos = 1
            if not last_err:
                # SQL 线程停了但没有错误信息，尝试直接重启后继续检查
                try:
                    cur.execute(start_replication_sql(syntax))
                except Exception as e:
                    return False, str(e)
                _tiny_sleep()
                continue
            # 若错误文本已点名冲突事务 GTID，则按「跳过该 GTID 空事务」处理——即使错误码不在
            # 幂等集合（如 1827：coordinator 仅转述 worker 失败，错误文本已写明
            # 'failed executing transaction <uuid>:<n>'）。这样可自愈 203 主库场景下
            # 'Worker 1 failed executing transaction 9da5e304:4' 这类复制中断。
            m = re.search(r"executing transaction '([0-9a-fA-F-]+:\d+)'", last_err)
            # GTID 自动定位 + 错误点名具体 GTID：走空事务注入（最精确，不丢 GTID 连续性）。
            if m and auto_pos == 1:
                gtid = m.group(1)
                try:
                    cur.execute(stop_replication_sql(syntax))
                    cur.execute(f"SET GTID_NEXT='{gtid}'")
                    cur.execute("BEGIN")
                    cur.execute("COMMIT")
                    cur.execute("SET GTID_NEXT='AUTOMATIC'")
                    cur.execute(start_replication_sql(syntax))
                    skipped.append(gtid)
                except Exception as e:
                    return False, str(e)
                _tiny_sleep()
                continue
            # 非 GTID 自动定位，或无法解析具体 GTID：用 sql_slave_skip_counter 跳过。
            # 兼容传统 binlog 坐标复制，避免 SET GTID_NEXT 不生效导致自愈失败、复制卡死。
            if last_code and last_code not in _SKIPPABLE_SQL_ERRNOS and not m:
                return False, f"复制错误码 {last_code} 非幂等冲突，需人工处理：{last_err}"
            try:
                cur.execute(stop_replication_sql(syntax))
                cur.execute("SET GLOBAL sql_slave_skip_counter = 1")
                cur.execute(start_replication_sql(syntax))
                skipped.append("skip_counter")
            except Exception as e:
                return False, f"跳过冲突事务失败(sql_slave_skip_counter): {e}"
            _tiny_sleep()
        return False, (f"跳过冲突事务次数过多（>200，含 {', '.join(skipped)}），"
                       f"可能存在结构性冲突，需人工处理")
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
        ssyntax = detect_syntax(scur)
        scur.execute(show_replica_status_sql(ssyntax))
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


def heal_anonymous_gtid_io(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user="root", slave_root_pass="",
                           master_host=None, master_port=None,
                           master_root_user="root", master_root_pass=""):
    """自愈 IO 错误 1236 'Cannot replicate anonymous transaction when AUTO_POSITION=1'。

    根因：源端(主库) binlog 含【匿名事务】(gtid_mode 曾为 OFF 时写入，或经 OFF_PERMISSIVE
    过渡写入)，而 AUTO_POSITION=1 无法复制匿名事务 → 从库 IO 线程每次重连都从 binlog 头部
    读到该匿名事务而报 1236。该匿名事务通常已被从库执行过（位于从库 Executed_Gtid_Set 内），
    故可安全清除，无需 RESET MASTER（会丢 GTID 集合）。

    安全步骤：
      1) 先让从库 SQL 线程把已接收的 GTID 全部应用（heal_replication 跳过冲突事务），
         确保从库 Executed ⊇ 主库即将清理的 binlog 区间；
      2) 主库 FLUSH BINARY LOGS 生成全新(纯 GTID)的 binlog；
      3) 主库 PURGE BINARY LOGS TO 当前 binlog（删掉含匿名事务的旧 binlog；
         安全前提：步骤1已保证从库执行过其中全部 GTID，purge 前再做一次集合校验）；
      4) 从库 STOP REPLICA + START REPLICA，AUTO_POSITION 从新 binlog 续传，不再遇匿名事务。

    返回 (acted, msg)。
    """
    sconn = connect_robust(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user, slave_root_pass, timeout=5)
    if sconn is None:
        return False, "无法连接从库(ha_monitor/root均失败)"
    try:
        scur = sconn.cursor()
        ssyntax = detect_syntax(scur)
        scur.execute(show_replica_status_sql(ssyntax))
        row = _one_dict(scur)
        if not row:
            return False, "从库无复制通道，无需处理"
        io_err = (row.get("Last_IO_Error") or row.get("Last_IO_Error") or "") or ""
        if "anonymous transaction" not in io_err:
            return False, "非匿名事务类 IO 错误，跳过"
        target_host = row.get("Source_Host") or row.get("Master_Host") or master_host
        try:
            target_port = int(row.get("Source_Port") or row.get("Master_Port")
                              or master_port or 0)
        except (TypeError, ValueError):
            target_port = 0
        if not target_host or not target_port:
            return False, "无法确定源端(主库)地址，跳过"
        # 步骤1：让从库应用已接收的 GTID（修复 SQL 冲突，如 1827 点名的事务）
        try:
            heal_replication(slave_host, slave_port, ha_user, ha_pass,
                             slave_root_user, slave_root_pass)
        except Exception as e:
            return False, f"从库 SQL 自愈失败: {e}"
        # 取从库执行集合，用于 purge 前安全校验
        scur.execute("SELECT @@GLOBAL.gtid_executed")
        slave_executed = (scur.fetchone() or (None,))[0] or ""
        # 步骤2/3：主库轮转 + 清理含匿名事务的旧 binlog
        mconn = connect_robust(target_host, target_port, ha_user, ha_pass,
                               master_root_user, master_root_pass, timeout=5)
        if mconn is None:
            return False, (f"无法连接源端主库 {target_host}:{target_port}"
                           f"(ha_monitor/root均失败)")
        try:
            mcur = mconn.cursor()
            mcur.execute("SELECT @@GLOBAL.gtid_executed")
            master_executed = (mcur.fetchone() or (None,))[0] or ""
            # 安全校验：从库必须已执行主库全部 GTID，否则 purge 会丢数据
            slave_g = parse_gtid_set(slave_executed)
            master_g = parse_gtid_set(master_executed)
            for uuid, nums in master_g.items():
                if not nums <= slave_g.get(uuid, set()):
                    return False, (f"从库尚未执行主库全部 GTID（{uuid} 缺失），"
                                   f"不安全清理主库 binlog，请先让复制追平后再试")
            mcur.execute("FLUSH BINARY LOGS")
            mcur.execute("SHOW BINARY LOGS")
            logs = mcur.fetchall()
            cur_bin = None
            for lg in logs or []:
                if isinstance(lg, dict):
                    cur_bin = lg.get("Log_name") or cur_bin
                elif isinstance(lg, (list, tuple)):
                    cur_bin = lg[0] if lg else cur_bin
            if not cur_bin:
                return False, "无法确定主库当前 binlog"
            mcur.execute(f"PURGE BINARY LOGS TO '{cur_bin}'")
        finally:
            try:
                mconn.close()
            except Exception:
                pass
        # 步骤4：从库重启复制线程，从新(干净的) binlog 续传
        try:
            scur.execute(stop_replication_sql(ssyntax))
            scur.execute(start_replication_sql(ssyntax))
            return True, (f"已在主库 {target_host}:{target_port} 轮转并清理含匿名事务的 binlog，"
                          f"从库 {slave_host}:{slave_port} 已重连续传")
        except Exception as e:
            return False, f"主库 binlog 已清理，但从库重启复制失败: {e}"
    except Exception as e:
        return False, str(e)
    finally:
        try:
            sconn.close()
        except Exception:
            pass


def reset_replication_creds_on_master(master_host, master_port, ha_user, ha_pass,
                                       root_user, root_pass, repl_user, repl_pass,
                                       slave_ip=None):
    """在主库上重置复制账号密码，解决从库 IO 报
    ``Access denied for user 'replication'@...`` 的凭证漂移问题。

    根因通常是主库的 ``replication@'%'`` 密码与集群库存 ``repl_pass`` 不一致（多为
    该主库之前未以真实 root 跑过 ``fix-node-creds.sql``，ha_monitor 仅 USAGE 无法自愈）。
    从库侧密码是对的（make_slave_of 已用库存 repl_pass 重指），所以必须把主库账号
    密码改回库存值。

    MySQL 8.0 默认 caching_sha2_password，非 TLS 复制会失败；强制 mysql_native_password
    可彻底规避。同时按从库 IP 建同名同密账号，避免 host 精确匹配把连接导向别的 host
    模式账号导致密码被遮蔽。

    优先 ha_monitor，连上但缺权限（典型 USAGE-only）时回退 root。返回 (ok, msg)。
    """
    def _do(cur):
        hosts = ["'%'"]
        if slave_ip and slave_ip != master_host:
            hosts.append("'%s'" % slave_ip)
        for h in hosts:
            try:
                cur.execute(
                    "CREATE USER IF NOT EXISTS '%s'@%s IDENTIFIED WITH mysql_native_password BY '%s'"
                    % (repl_user, h, repl_pass))
            except Exception:
                # 5.7/MariaDB 不支持 CREATE USER 作 GRANT 项等：忽略，后面用 ALTER 兜底
                pass
            cur.execute(
                "ALTER USER '%s'@%s IDENTIFIED WITH mysql_native_password BY '%s'"
                % (repl_user, h, repl_pass))
            cur.execute(
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@%s"
                % (repl_user, h))
        cur.execute("FLUSH PRIVILEGES")

    who = None
    conn = None
    try:
        conn = connect(master_host, master_port, ha_user, ha_pass, timeout=8)
        who = "ha_monitor"
    except Exception as e:
        if not root_pass:
            return False, "主库 ha_monitor 连接失败: %s" % e
        try:
            conn = connect(master_host, master_port, root_user, root_pass, timeout=8)
            who = "root(%s)" % root_user
        except Exception as e2:
            return False, ("主库 ha_monitor 连接失败: %s；root 回退也失败: %s" % (e, e2))
    cur = conn.cursor()
    try:
        _do(cur)
        return True, ("已在主库 %s:%s（用 %s）重置复制账号 %s 密码为 mysql_native_password"
                      % (master_host, master_port, who, repl_user))
    except Exception as e:
        # ha_monitor 连上了但缺 CREATE/ALTER USER 权限（典型：init 未授予、仅 USAGE），
        # 改不动 replication 密码。此时回退 root 重做（root 必然有权限）。
        if who == "ha_monitor" and root_pass:
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = connect(master_host, master_port, root_user, root_pass, timeout=8)
                cur = conn.cursor()
                _do(cur)
                who = "root(%s)(回退)" % root_user
                return True, ("已在主库 %s:%s（用 %s）重置复制账号 %s 密码为 mysql_native_password"
                              % (master_host, master_port, who, repl_user))
            except Exception as e2:
                return False, ("主库重置复制密码失败(ha_monitor: %s；root 回退也失败: %s)"
                               % (e, e2))
        return False, "主库重置复制密码失败(%s): %s" % (who, e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def restart_slave_io(host, port, user, password, root_user="root", root_pass="", timeout=8):
    """重启从库 IO 线程，使其用新密码立即重连主库（无需等 60s 重试窗口）。"""
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception as e:
        return False, "重启从库 IO 线程：连接失败: %s" % e
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(stop_replication_sql(syntax) + " IO_THREAD")
        cur.execute(start_replication_sql(syntax) + " IO_THREAD")
        return True, "已重启从库 %s:%s IO 线程" % (host, port)
    except Exception as e:
        return False, "重启从库 IO 线程失败: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def connect(host, port, user, password, timeout=5):
    """建立连接；失败时抛 RuntimeError（带原始异常信息）。

    注意：get_server_public_key 仅 PyMySQL >= 1.0.0 支持。但实测某些容器内的
    PyMySQL「版本号 ≥1.0.0 却实际不支持该参数」（镜像源/缓存调包或安装不干净），
    光靠版本号判断不可靠。故：先按版本守卫尝试携带该参数；若仍抛 TypeError
    （无论版本号怎么报），去掉该参数重试，保证原生密码等常规连接不受影响。
    """
    base = dict(
        host=host, port=int(port), user=user, password=password,
        connect_timeout=timeout, autocommit=True,
        charset="utf8mb4",
    )
    attempts = [base]
    if _PYMYSQL_SUPPORTS_PUBKEY:
        attempts.insert(0, {**base, "get_server_public_key": True})
    last_err = None
    for kwargs in attempts:
        try:
            return pymysql.connect(**kwargs)
        except TypeError as e:
            # 老版本 / 被调包的 PyMySQL 不识别 get_server_public_key：去掉后重试。
            if "get_server_public_key" in str(e):
                last_err = e
                continue
            raise
        except Exception as e:
            raise RuntimeError(str(e))
    raise RuntimeError(str(last_err))


def connect_robust(host, port, user, password, root_user="root", root_pass="", timeout=5):
    """
    优先用 ha_monitor(user/password) 连接；
    失败（账号漂移/拒绝）时回退用 root(root_user/root_pass) 连接。

    两者都失败时合并错误信息——既指出 ha_monitor（首选账号，缺失/密码漂移才是根因），
    也保留 root 回退结果，避免只显示 root 的 1045、掩盖「init 未跑/ha_monitor 缺失」真相。
    """
    ha_err = None
    try:
        return connect(host, port, user, password, timeout=timeout)
    except Exception as e:
        ha_err = e
    if root_pass:
        try:
            return connect(host, port, root_user, root_pass, timeout=timeout)
        except Exception as e2:
            raise RuntimeError(
                "ha_monitor(%s) 连接失败: %s；root(%s) 回退也失败: %s"
                % (user, ha_err, root_user, e2)
            )
    raise ha_err


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
        connections=None, threads_running=None, qps=None,
    )
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception as e:
        result["error"] = str(e)
        return result
    cur = conn.cursor()
    try:
        syntax = detect_syntax(cur)
        try:
            cur.execute("SELECT @@read_only")
            result["ro"] = cur.fetchone()[0]
        except Exception:
            result["ro"] = None
        try:
            cur.execute(show_replica_status_sql(syntax))
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
        # 轻量采集实时运行指标（供观测中心趋势图使用）。
        # 失败不影响核心探活结果，故整体包 try 吞掉。
        try:
            cur.execute("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            rc = cur.fetchone()
            result["connections"] = int(rc[1]) if rc else None
            cur.execute("SHOW GLOBAL STATUS LIKE 'Threads_running'")
            rr = cur.fetchone()
            result["threads_running"] = int(rr[1]) if rr else None
            # QPS：用两次 Questions 差值估算（此处仅取当前瞬时值，由调用方做差更准；
            # 采集线程已在 metrics.py 用相邻样本做差，这里提供原始值即可）。
            cur.execute("SHOW GLOBAL STATUS LIKE 'Queries'")
            rq = cur.fetchone()
            result["qps"] = int(rq[1]) if rq else None
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
        syntax = detect_syntax(cur)
        try:
            cur.execute(stop_replication_sql(syntax))
            cur.execute(reset_replica_sql(syntax))
        except Exception:
            pass
        cur.execute("SET GLOBAL read_only=0")
        # 轮转 binlog：若该节点此前以从库/独立主形态运行，其当前 binlog 可能是在
        # gtid_mode=OFF/OFF_PERMISSIVE 时期写入、含匿名事务。提升为主库后，新从库以
        # AUTO_POSITION=1 来拉取时会因该匿名事务报 1236。轮转后新 binlog 仅含 GTID 事务，
        # 从根上避免「提升为主后从库匿名事务 1236」这类偶发故障（heal_anonymous_gtid_io
        # 负责清理仍被从库需要的旧 binlog）。
        try:
            cur.execute("FLUSH BINARY LOGS")
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
        flavour = detect_flavour(cur)
        # MariaDB 与 MySQL 双路径完全独立：
        #  - MySQL 8.0：用 MASTER_AUTO_POSITION=1，要求 gtid_mode=ON；匿名事务用
        #    ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL 根治 1236。
        #  - MariaDB：用 MASTER_USE_GTID=slave_pos（基于 gtid_domain_id 的序列 GTID，
        #    自动定位、天然无匿名事务 1236 问题），**不检查 gtid_mode**（MariaDB 无此变量）。
        syntax = "new" if flavour == F_MYSQL8 else "old"
        if flavour == F_MARIADB:
            # MariaDB：无需 gtid_mode 检查，GTID 由 my.cnf 的 gtid_domain_id 保证
            pass
        else:
            # GTID 自动定位（MASTER_AUTO_POSITION=1）要求本节点 gtid_mode=ON；
            # 若节点未开启 GTID，CHANGE REPLICATION SOURCE 会直接失败并每周期刷屏。
            try:
                cur.execute("SELECT @@gtid_mode")
                row = cur.fetchone()
                gm = row[0] if row else None
            except Exception:
                gm = None
            if gm is not None and gm != "ON":
                return False, (f"本节点 gtid_mode={gm}（应为 ON），无法使用 GTID 自动定位。"
                               f"请重新运行 Install-ha-mariadb-Node.sh 开启 GTID 后重入集群。")
        try:
            cur.execute(stop_replication_sql(flavour))
            cur.execute(reset_replica_sql(flavour))
        except Exception:
            pass
        # 注意：这里不再 RESET MASTER，避免 GTID 执行集合被清空后
        # 旧事务被新主库重放而报「已存在」类错误。
        # 先带 PUBLIC_KEY 选项（MySQL 8.0 caching_sha2 / MariaDB 非 TLS 复制的官方修复）；
        # 若服务端不识别该选项则去掉重试。
        # assign_anon_gtids 仅 MySQL 8.0 生效（根治匿名事务 1236）；MariaDB 无需（走 slave_pos）。
        try:
            cur.execute(
                change_replication_sql(flavour, master_host, int(master_port),
                                       repl_user, repl_pass, public_key=True,
                                       assign_anon_gtids=(flavour == F_MYSQL8)),
            )
        except Exception as e_pub:
            pub_err = str(e_pub)
            try:
                cur.execute(
                    change_replication_sql(flavour, master_host, int(master_port),
                                           repl_user, repl_pass, public_key=False,
                                           assign_anon_gtids=(flavour == F_MYSQL8)),
                )
            except Exception as e_no_pub:
                try:
                    cur.execute(
                        change_replication_sql(flavour, master_host, int(master_port),
                                               repl_user, repl_pass, public_key=False,
                                               assign_anon_gtids=False),
                    )
                except Exception as e2:
                    raise Exception(
                        f"CHANGE REPLICATION SOURCE 失败(pub:{pub_err}; nopub:{e_no_pub}; "
                        f"compat:{e2})")
        # 双保险：ha_local 是节点本地状态表，不参与复制（写时已 SET sql_log_bin=0）。
        # 再从库侧加复制过滤，确保即便有遗漏也不会因 ha_local 事件 1062 卡死复制。
        try:
            cur.execute("CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = (ha_local)")
        except Exception as e_flt:
            print(f"[repl] 设置 REPLICATE_IGNORE_DB=ha_local 失败(可忽略): {e_flt}")
        cur.execute(start_replication_sql(flavour))
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


def fix_node_mysql_params(host, port, root_user="root", root_pass="",
                          ha_user=None, ha_pass=None, target_server_id=None):
    """自愈单节点复制关键参数（仅 SET PERSIST，不重启、不改 my.cnf）。

    解决的问题：裸机 MySQL 的 my.cnf 片段常被面板/重启覆盖，导致 gtid_mode 反复回退为
    OFF、server_id 退回默认值 1（多节点重复），每次重装都要手动改配置。

    SET PERSIST 会把参数写入 mysqld-auto.cnf，MySQL 启动后（在 my.cnf 之后）自动加载，
    因此【重启依然生效】，且会覆盖被面板改写的 my.cnf 片段，从根上消除反复回退。

    仅处理可在线修改的动态参数：
    - enforce_gtid_consistency=ON（须先于 gtid_mode=ON）
    - gtid_mode 经 OFF_PERMISSIVE -> ON_PERMISSIVE -> ON
    - server_id=<唯一值>

    log_bin=OFF 无法在线开启（需改 my.cnf + 重启），若检测到则只告警不处理。

    返回 (ok, report_dict)。report_dict: {changes:[...], warns:[...], before:{...}, error:...}
    """
    # 自愈是特权操作（SET PERSIST），优先用 root；root 连不上（如 root@'<IP>' 未建）
    # 再回退 ha_monitor（docker 模板已授予 SYSTEM_VARIABLES_ADMIN/SUPER）。
    conn = None
    try:
        conn = connect(host, port, root_user, root_pass, timeout=5)
    except Exception:
        conn = None
    if conn is None:
        conn = connect_robust(host, port, ha_user or "root", ha_pass or "",
                              root_user, root_pass)
    if conn is None:
        return False, {"error": f"无法以 root/ha_monitor 连入 {host}:{port}"
                              f"（请先确认已创建 root@'<管理器IP>' 且密码正确）"}
    cur = conn.cursor()
    report = {"changes": [], "warns": [], "before": {}}
    try:
        # MariaDB 与 MySQL 双路径完全独立：
        #  - MariaDB 没有 gtid_mode 变量（只有 gtid_domain_id），@@gtid_mode 会直接报错；
        #    故 MariaDB 跳过 gtid_mode/enforce_gtid_consistency 整段，仅校验 server_id 与 log_bin。
        #  - MySQL 才需要 gtid_mode 三阶段过渡 + enforce_gtid_consistency。
        flavour = detect_flavour(cur)
        if flavour == F_MARIADB:
            report["before"] = {"flavour": "mariadb"}
            try:
                cur.execute("SELECT @@server_id,@@log_bin")
                sid, lb = cur.fetchone()
                report["before"]["server_id"] = sid
                report["before"]["log_bin"] = lb
            except Exception as e:
                report["warns"].append(f"读取 server_id/log_bin 失败: {e}")
            # server_id 唯一化（MariaDB 同样靠 server_id + gtid_domain_id 区分 GTID 源）
            if target_server_id and int(sid or 0) != int(target_server_id):
                try:
                    cur.execute(f"SET GLOBAL server_id={int(target_server_id)}")
                    report["changes"].append(f"server_id={target_server_id}")
                except Exception as e:
                    report["warns"].append(f"server_id 设置失败: {e}")
            # log_bin 无法在线开启，仅告警
            if int(lb or 0) != 1:
                report["warns"].append(
                    "log_bin=OFF：复制需要 binlog，需重启 MariaDB（裸机请重跑 Install 脚本或 "
                    "在 my.cnf 增加 log_bin 后重启）")
            conn.commit()
            return True, report
        # ---- 以下为 MySQL 路径 ----
        cur.execute("SELECT @@gtid_mode,@@enforce_gtid_consistency,@@server_id,@@log_bin")
        gm, egc, sid, lb = cur.fetchone()
        report["before"] = {"gtid_mode": gm, "server_id": sid, "log_bin": lb}
        # enforce_gtid_consistency 必须先于 gtid_mode=ON
        if str(egc).upper() != "ON":
            try:
                cur.execute("SET PERSIST enforce_gtid_consistency=ON")
                report["changes"].append("enforce_gtid_consistency=ON")
            except Exception as e:
                report["warns"].append(f"enforce_gtid_consistency 设置失败: {e}")
        # gtid_mode 三阶段过渡（不可一步到 ON）
        if str(gm).upper() != "ON":
            ok_gtid = True
            for stp in ("OFF_PERMISSIVE", "ON_PERMISSIVE", "ON"):
                try:
                    cur.execute(f"SET PERSIST gtid_mode={stp}")
                except Exception as e:
                    ok_gtid = False
                    report["warns"].append(f"gtid_mode={stp} 设置失败: {e}")
                    break
            if ok_gtid:
                report["changes"].append("gtid_mode=ON")
                # 轮转 binlog：gtid_mode 刚从 OFF/OFF_PERMISSIVE 转为 ON，当前 binlog 可能
                # 仍含匿名事务（旧主库切换时会导致从库报 1236「Cannot replicate anonymous
                # transaction」）。轮转后新 binlog 仅含 GTID 事务，从根上避免该错误。
                try:
                    cur.execute("FLUSH BINARY LOGS")
                except Exception as e:
                    report["warns"].append(f"FLUSH BINARY LOGS 失败: {e}")
        # server_id 唯一化
        if target_server_id and int(sid) != int(target_server_id):
            try:
                cur.execute(f"SET PERSIST server_id={int(target_server_id)}")
                report["changes"].append(f"server_id={target_server_id}")
            except Exception as e:
                report["warns"].append(f"server_id 设置失败: {e}")
        # log_bin 无法在线开启，仅告警。
        # 注意：@@log_bin 返回整型 0/1（非 'ON'/'OFF' 字符串），故按真值判断，
        # 避免把正常的 log_bin=1 误报成 log_bin=OFF。
        if int(lb or 0) != 1:
            report["warns"].append(
                "log_bin=OFF：复制需要 binlog，SET PERSIST 无法在线开启；"
                "需在 my.cnf 增加 log_bin 后重启 MySQL（裸机请跑 Install 脚本或手动追加片段后重启）")
        conn.commit()
        return True, report
    except Exception as e:
        return False, {"error": f"{type(e).__name__}: {e}"}
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
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(stop_replication_sql(syntax))
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
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(start_replication_sql(syntax))
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
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(stop_replication_sql(syntax))
        cur.execute(reset_replica_sql(syntax))
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
        # ha_local 是节点本地状态表（keepalived 检查脚本读本机 current_master 用），
        # 绝不参与复制：用 sql_log_bin=0 防止写入进 binlog 被复制到从库。
        # 否则管理端连主库写入 current_master 进 binlog，从库复制线程再 INSERT 同主键
        # 会报 1062 Duplicate entry 卡死复制（Write_rows 而非 REPLACE 语义）。
        cur.execute("SET SESSION sql_log_bin=0")
        try:
            cur.execute("CREATE DATABASE IF NOT EXISTS ha_local")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS ha_local.state "
                "(k VARCHAR(32) PRIMARY KEY, v VARCHAR(255))"
            )
            cur.execute(
                "REPLACE INTO ha_local.state (k, v) VALUES ('current_master', %s)",
                (master_str,),
            )
        finally:
            cur.execute("SET SESSION sql_log_bin=1")
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
                    app_user="", app_pass="", manager_ip=""):
    """
    将节点上的 ha_monitor / replication / app / root@'<管理器IP>' 账号**完整自愈**为当前设定值。

    与旧版（仅 ALTER USER、且必须用 root 连）不同：
    - 优先用 **ha_monitor**（init 创建、密码已知正确、且已授予 CREATE/ALTER/DROP USER）
      连接并修正账号；root 仅作为最后回退。
    - 这会打破「root 密码漂移 -> 用错误 root 连不上 -> 自愈不跑 -> 永远连不上 root」
      的死循环：只要 ha_monitor 仍能连上（节点在线），就能把 root@'<管理器IP>' 密码
      校正回管理器库存值，后续 root 连接自动恢复、复制随后建立。
    - 若账号不存在（裸机未跑 init.sql / 被面板清空 / 密码漂移）也能自动 CREATE + GRANT，
      避免从库连主库报 1130「Host ... not allowed」。

    返回 (ok, msg)。
    """
    # 优先 ha_monitor（已知正确），失败再回退 root。
    conn = None
    used_for_connect = None
    last_err = None
    for (cu, cp, tag) in ((ha_user, ha_pass, "ha_monitor"),
                          (root_user, root_pass, "root")):
        if not cp:
            continue
        try:
            conn = connect(host, port, cu, cp, timeout=5)
            used_for_connect = tag
            break
        except Exception as e:
            last_err = e
    if conn is None:
        return False, "连接失败(ha_monitor 与 root 均不可达): %s" % last_err
    def _sync(cur):
        # 判定风味：MariaDB 与 MySQL 的账号/权限语法【不兼容】，必须分派。
        # MariaDB 无 SYSTEM_VARIABLES_ADMIN 等 MySQL 8.0 专有特权（直接 GRANT 会 1064）；
        # 但 MariaDB 10.5+ 提供 REPLICATION SLAVE ADMIN，12.x 执行 CHANGE MASTER/START SLAVE
        # 必须有该特权，故 MariaDB 分支需授予 REPLICATION SLAVE ADMIN（不可省略）。
        try:
            flavour = detect_flavour(cur)
        except Exception:
            flavour = F_MYSQL8
        if flavour == F_MARIADB:
            # MariaDB：认证插件显式 mysql_native_password（兼容管理器/老驱动）。
            # MariaDB 10.6 不支持 `IDENTIFIED VIA ... BY '明文'`，须用
            # `IDENTIFIED VIA mysql_native_password USING PASSWORD('明文')`
            # （PASSWORD() 函数对明文再哈希，与 init_mariadb.sql 一致，已验证可用）。
            idn = "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
            idn_root = "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
            # 注：MariaDB 10.5+（含 11/12.x）提供 REPLICATION SLAVE ADMIN / SLAVE MONITOR 特权：
            #  - 12.x 执行 CHANGE MASTER TO / START SLAVE 必须有 REPLICATION SLAVE ADMIN（仅 REPLICATION SLAVE 不够，报 1227）；
            #  - 12.x 执行 SHOW REPLICA STATUS / 读取复制状态必须有 SLAVE MONITOR（监控探活读复制状态用），否则报 1227；
            #  10.x 对这两项不强制，但显式授予对 10.5+ 兼容无害。
            ha_grant = ("GRANT CREATE USER, CREATE, INSERT, UPDATE, DELETE, "
                        "REPLICATION SLAVE, REPLICATION SLAVE ADMIN, SLAVE MONITOR, "
                        "READ_ONLY ADMIN, REPLICATION CLIENT, PROCESS, SELECT, RELOAD, SUPER ON *.* "
                        "TO '%s'@'%%'" % ha_user)
            repl_grant = ("GRANT REPLICATION SLAVE, REPLICATION SLAVE ADMIN ON *.* "
                          "TO '%s'@'%%'" % repl_user)
        else:
            idn = "IDENTIFIED WITH mysql_native_password BY %s"
            idn_root = "IDENTIFIED WITH mysql_native_password BY %s"
            ha_grant = ("GRANT REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
                        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* "
                        f"TO '{ha_user}'@'%'")
            repl_grant = f"GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'%'"
        accts = [(ha_user, ha_pass, ha_grant), (repl_user, repl_pass, repl_grant)]
        if app_user and app_pass:
            app_grant = (f"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, "
                         f"REFERENCES, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON *.* "
                         f"TO '{app_user}'@'%'")
            accts.append((app_user, app_pass, app_grant))
        for u, p, grant_sql in accts:
            cur.execute("CREATE USER IF NOT EXISTS %s@'%%' " + idn, (u, p))
            cur.execute("ALTER USER %s@'%%' " + idn, (u, p))
            cur.execute(grant_sql)
        # 关键：把 root@'<管理器IP>' 与 root@'%' 密码校正为管理器库存 root_pass。
        #  - manager_ip 的 root 用于管理器自愈回退；
        #  - root@'%' 用于内网 Navicat/其它主机远程连接（用户要求可远程登录）。
        # manager_ip 为空时跳过该条，但 root@'%' 始终创建（便于内网运维）。
        root_targets = []
        if manager_ip:
            root_targets.append(manager_ip)
        root_targets.append("%")  # 内网远程访问
        for rh in root_targets:
            cur.execute("CREATE USER IF NOT EXISTS %s@%s " + idn_root, (root_user, rh, root_pass))
            cur.execute("ALTER USER %s@%s " + idn_root, (root_user, rh, root_pass))
        # 授予 ha_monitor CREATE USER 权限，使其能持续自愈账号（CREATE USER 特权同时隐含
        # 允许 ALTER/DROP USER）。8.0 部分小版本不认 ALTER USER/DROP USER 作为可授予权限名
        # （GRANT 报 1064），故只授 CREATE USER 这一稳定有效的特权；5.7/MariaDB 同样支持。
        try:
            cur.execute("GRANT CREATE USER ON *.* TO '%s'@'%%'" % ha_user)
        except Exception:
            pass
        # MariaDB 10.11+（含 11/12.x）执行 SET GLOBAL read_only=0/1 需要 READ_ONLY ADMIN
        # 特权（旧版仅 SUPER 即可，12.x 强化后缺它会 1227 导致手动/自动切换失败）。
        # 该特权在 10.11 之前不存在，GRANT 会报 1064，故 try/except 忽略以兼容旧版。
        try:
            cur.execute("GRANT READ_ONLY ADMIN ON *.* TO '%s'@'%%'" % ha_user)
        except Exception:
            pass

    cur = conn.cursor()
    used = used_for_connect
    try:
        _sync(cur)
    except Exception as e:
        # ha_monitor 连上了但缺 CREATE/ALTER USER 权限（典型：init 未授予、仅 USAGE），
        # 改不动 replication 密码。此时回退 root 重做整段账号同步（root 必然有权限）。
        # 这是「复制 Access denied」能被管理器自愈的关键路径。
        if used_for_connect == "ha_monitor" and root_pass:
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = connect(host, port, root_user, root_pass, timeout=5)
                cur = conn.cursor()
                _sync(cur)
                used = "root(回退)"
            except Exception as e2:
                return False, ("校正失败(经 ha_monitor: %s；root 回退也失败: %s)"
                               % (e, e2))
        else:
            return False, "校正失败(经 %s): %s" % (used_for_connect, e)
    try:
        cur.execute("FLUSH PRIVILEGES")
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    return True, "ok (via %s)" % used
