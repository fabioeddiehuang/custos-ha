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
import hashlib
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
                           change_replication_sql, change_replication_filter_sql,
                           start_replication_sql,
                           stop_replication_sql, reset_replica_sql, show_replica_status_sql,
                           default_auth_plugin, F_MYSQL8, F_MARIADB)
from .config import get_manager_public_ip


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


# IO 线程「认证类」错误特征：MySQL 8.0 的 caching_sha2_password 在明文 TCP、
# 又未索取服务端 RSA 公钥时，会报 “Authentication requires secure connection”；
# 连同账号密码不对 / 连不上源端一并归入此类，统一走「重建通道」自愈。
_AUTH_IO_MARKERS = (
    "caching_sha2_password",
    "authentication requires secure connection",
    "authentication plugin",
    "access denied for user",
    "error connecting to source",
    "error connecting to master",
    "2061",
    "1045",
)


def _io_error_is_auth(io_err: str) -> bool:
    s = (io_err or "").lower()
    return any(m in s for m in _AUTH_IO_MARKERS)


def _start_io_thread(cur):
    """启动 IO 线程（MySQL 8.0 / 5.7 两种语法都试）。"""
    for s in ("START REPLICA IO_THREAD", "START SLAVE IO_THREAD"):
        try:
            cur.execute(s)
            return True
        except Exception:
            continue
    return False


def _stop_io_thread(cur):
    """停止 IO 线程（失败可忽略：未运行时本就会报错）。"""
    for s in ("STOP REPLICA IO_THREAD", "STOP SLAVE IO_THREAD"):
        try:
            cur.execute(s)
            return True
        except Exception:
            continue
    return False


def heal_io_auth(slave_host, slave_port, ha_user, ha_pass,
                 slave_root_user="root", slave_root_pass="",
                 repl_user=None, repl_pass=None,
                 master_host=None, master_port=None,
                 master_root_user="root", master_root_pass=""):
    """自愈复制 IO 线程的【认证类】错误。

    典型报错（MySQL 8.0 + caching_sha2_password + 非 TLS 复制）：
        Authentication plugin 'caching_sha2_password' reported error:
        Authentication requires secure connection.

    根因：从库以明文 TCP 连主库，caching_sha2_password 在没有 TLS、也没有索取
    服务端 RSA 公钥的情况下无法完成密码交换。原先这类错误会被
    heal_io_gtid_gap 判为「非 GTID 缺失类」直接跳过，导致一键修复无效。

    修复分两级：
      1) 重建复制通道并带 SOURCE_PUBLIC_KEY=1（官方推荐：向服务端索取 RSA 公钥）；
      2) 若仍失败（服务端未开启 RSA 自动密钥 / 不支持该选项），到【源端】把复制
         账号改为 mysql_native_password，再重启 IO 线程。

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

        def g(k):
            return row.get(k)

        io_err = (g("Last_IO_Error") or "") or ""
        if not _io_error_is_auth(io_err):
            return False, "IO 错误非认证类，跳过" + (("：" + io_err) if io_err else "")

        target_host = g("Source_Host") or g("Master_Host") or master_host
        try:
            target_port = int(g("Source_Port") or g("Master_Port") or master_port or 0)
        except (TypeError, ValueError):
            target_port = 0
        if not target_host or not target_port or not repl_user:
            return False, "缺少源端地址或复制账号，无法重建复制通道"

        flavour = detect_flavour(scur)
        notes = []

        # ---------- 第一级：带 SOURCE_PUBLIC_KEY=1 重建通道 ----------
        _stop_io_thread(scur)
        try:
            scur.execute(change_replication_sql(
                flavour, target_host, target_port, repl_user, repl_pass or "",
                public_key=True, assign_anon_gtids=(flavour == F_MYSQL8)))
            notes.append("已带 SOURCE_PUBLIC_KEY=1 重建复制通道")
        except Exception as e_pub:
            notes.append(f"带公钥重建失败({e_pub})，改为不带公钥重建")
            try:
                scur.execute(change_replication_sql(
                    flavour, target_host, target_port, repl_user, repl_pass or "",
                    public_key=False))
            except Exception as e2:
                return False, f"重建复制通道失败: {e2}"
        _start_io_thread(scur)

        _tiny_sleep(1.5)
        scur.execute(show_replica_status_sql(ssyntax))
        row2 = _one_dict(scur) or {}
        if row2.get("Slave_IO_Running") == "Yes" or row2.get("Replica_IO_Running") == "Yes":
            return True, "；".join(notes) + f"；IO 线程已恢复（源 {target_host}:{target_port}）"

        io_err2 = (row2.get("Last_IO_Error") or "") or ""

        # ---------- 第二级：到源端把复制账号降级为 mysql_native_password ----------
        mconn = connect_robust(target_host, target_port, ha_user, ha_pass,
                               master_root_user, master_root_pass, timeout=5)
        if mconn is None:
            return False, (f"重建通道后 IO 仍未恢复，且无法连接源端 {target_host}:{target_port} "
                           f"做降级处理（当前错误：{io_err2 or '未知'}）")
        try:
            mcur = mconn.cursor()
            quser = str(repl_user).replace("'", "''")
            qpass = str(repl_pass or "").replace("'", "''")
            last_err = None
            # 常见两种 host 形态：'%' 与从库 IP
            for host_part in ("'%'", f"'{slave_host}'"):
                try:
                    mcur.execute(
                        f"ALTER USER '{quser}'@{host_part} "
                        f"IDENTIFIED WITH mysql_native_password BY '{qpass}'")
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
            if last_err is not None:
                # 账号可能根本不存在：直接按 native 插件创建并授权
                try:
                    mcur.execute(
                        f"CREATE USER '{quser}'@'%' "
                        f"IDENTIFIED WITH mysql_native_password BY '{qpass}'")
                    mcur.execute(f"GRANT REPLICATION SLAVE ON *.* TO '{quser}'@'%'")
                    last_err = None
                except Exception as e2:
                    return False, (f"源端改用 mysql_native_password 失败: {last_err} / {e2}"
                                   f"（当前错误：{io_err2 or '未知'}）")
            notes.append(f"已在源端把复制账号 {repl_user} 改为 mysql_native_password")
        finally:
            try:
                mconn.close()
            except Exception:
                pass

        # 源端改完后重建通道（此时不再需要公钥）并重启 IO 线程
        _stop_io_thread(scur)
        try:
            scur.execute(change_replication_sql(
                flavour, target_host, target_port, repl_user, repl_pass or "",
                public_key=False))
        except Exception as e:
            notes.append(f"重建通道失败({e})")
        _start_io_thread(scur)

        _tiny_sleep(1.5)
        scur.execute(show_replica_status_sql(ssyntax))
        row3 = _one_dict(scur) or {}
        if row3.get("Slave_IO_Running") == "Yes" or row3.get("Replica_IO_Running") == "Yes":
            return True, "；".join(notes) + f"；IO 线程已恢复（源 {target_host}:{target_port}）"
        return False, ("；".join(notes) + "；IO 仍未恢复："
                       + (row3.get("Last_IO_Error") or "未知错误"))
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


# c/s 同机：始终用节点真实 IP 连接。管理器容器连同机节点时源 IP 是 docker 网桥网关
# (172.x.x.x)，节点侧已授权 root@'<MANAGER_PUBLIC_IP>' + root@'<docker子网通配符>'
# 双白名单，且防火墙放行 docker 网段到 MySQL 端口，故无需改写 host（摒弃
# host.docker.internal + extra_hosts 临时方案，裸机 MariaDB 上该主机名无法解析）。
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


def is_read_only(host, port, ha_user, ha_pass, root_user="root", root_pass="", timeout=5):
    """判断节点是否只读（副本）。

    用于自愈时跳过只读副本上的「直接账号 DDL / 改全局参数 DDL」：副本的账号与
    gtid_mode/server_id 均来自主库经复制同步，管理器在副本上直接执行这些 DDL 会与其
    复制来的同名语句争用元数据锁并漂权限（表现为 1045/1227 告警）。副本账号由主库
    ensure_ha_creds 周期性 binlog 出去自动纠正。
    """
    try:
        conn = connect_robust(host, port, ha_user, ha_pass, root_user, root_pass, timeout)
    except Exception:
        return False
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT @@global.read_only")
        row = cur.fetchone()
        return bool(row and row[0])
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
        # 压力代理指标（宿主机资源在远程节点上拿不到，改用 MySQL 侧压力信号）
        row_lock_waits=None, slow_queries=None,
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
            # 压力代理指标：均为累计计数器，由调用方按相邻样本做差得到「周期增量」
            cur.execute("SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'")
            rlk = cur.fetchone()
            result["row_lock_waits"] = int(rlk[1]) if rlk else None
            cur.execute("SHOW GLOBAL STATUS LIKE 'Slow_queries'")
            rsq = cur.fetchone()
            result["slow_queries"] = int(rsq[1]) if rsq else None
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

    【空闲零写入】ensure_topology 每个监控周期都会调用本函数。旧实现对已经是主库的节点
    仍会执行 STOP REPLICA / RESET REPLICA ALL / **FLUSH BINARY LOGS**；其中
    FLUSH BINARY LOGS 会新建并 fsync 一个 binlog 文件（节点模板为 sync_binlog=1），
    是「系统空闲却在节点上持续产生磁盘写入」的主因。
    故先做一次**只读**快检：已「可写且无残留复制通道」时直接返回，不执行任何写操作。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        # 只读快检：不产生任何写盘 / 不进 binlog
        try:
            cur.execute("SELECT @@GLOBAL.read_only")
            _r = cur.fetchone()
            if _r is not None and int(_r[0]) == 0:
                cur.execute(show_replica_status_sql(syntax))
                if cur.fetchone() is None:
                    # 已是可写且无残留复制通道的主库：无需再做任何写操作
                    return True, None
        except Exception:
            pass
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
        # 注意：MySQL 8.0 语法为 `REPLICATE_IGNORE_DB = (db)`，MariaDB 12.x 为
        # `REPLICATE_IGNORE_DB=(db)`（无空格），必须走 mysql_compat 分支函数，不可硬编码。
        try:
            cur.execute(change_replication_filter_sql(["ha_local"], flavour=flavour))
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
    # 自愈是特权操作（SET PERSIST server_id/gtid_mode），需 SYSTEM_VARIABLES_ADMIN/SUPER。
    # 优先用 ha_monitor（节点模板已授予该特权，主库与副本均具备）；仅在 ha_monitor 不可用
    # （账号缺失/漂移）时回退 root（容器 root 密码固定，不随 HA_PASS 漂移）。
    conn = None
    if ha_pass:
        try:
            conn = connect(host, port, ha_user or "root", ha_pass, timeout=5)
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
            # 建库建表仅作兜底：ha_local.state 通常在节点初始化时已建好。
            # ha_monitor 一般不持有 CREATE 权限，而 MySQL 对
            # CREATE DATABASE/TABLE IF NOT EXISTS 也会先校验权限再判存在，
            # 于是在「表已存在但账号无 CREATE」的节点上必然抛 1044，
            # 被外层 except 吞掉后整个状态写入静默失败（ha_local.state 永远为空，
            # keepalived 检查脚本读不到 current_master → VIP 无法归属）。
            # 故这两句单独容错，失败不阻断后续的 SELECT/REPLACE。
            try:
                cur.execute("CREATE DATABASE IF NOT EXISTS ha_local")
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS ha_local.state "
                    "(k VARCHAR(32) PRIMARY KEY, v VARCHAR(255))"
                )
            except Exception:
                pass
            # 空闲优化：当前主库未变化时不再 REPLACE，避免每个监控周期都在所有节点产生
            # 一次 InnoDB 提交（redo 强制刷盘 → 磁盘每秒写入）。仅值变化时写。
            cur.execute("SELECT v FROM ha_local.state WHERE k='current_master'")
            row = cur.fetchone()
            if row and row[0] == master_str:
                return True
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


def _native_password_hash(password):
    """计算 MySQL mysql_native_password 存储的 authentication_string（*+SHA1(SHA1(pwd))）。

    用于自愈时比对「当前密码哈希」与「期望密码哈希」：仅当不一致才 ALTER USER，
    避免每周期无条件重写密码 -> 进 binlog -> 副本重放+再写自身 binlog -> 空闲也持续写盘。
    """
    if password is None:
        return None
    s1 = hashlib.sha1(password.encode("utf-8")).digest()
    # 注意：二次哈希要取 hexdigest（str），不能用 digest() 后再 .hexdigest()
    # —— bytes 没有 .hexdigest()，会抛 'bytes' object has no attribute 'hexdigest'，
    # 导致整个账号自愈链路失效。
    s2 = hashlib.sha1(s1).hexdigest()
    return "*" + s2.upper()


def _parse_grant_privs(grant_sql):
    """从 GRANT 语句里解析出权限集合（用于与账号已有权限比对，判断是否需重授权）。"""
    if not grant_sql:
        return set()
    m = re.match(r"\s*GRANT\s+(.+?)\s+ON\s", grant_sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return set()
    return {p.strip().upper() for p in m.group(1).split(",") if p.strip()}


def _granted_privs(cur, user, host):
    """读取账号当前已授予的全局权限集合（用于幂等判断，避免重复 GRANT 写 binlog）。"""
    try:
        cur.execute("SHOW GRANTS FOR %s@%s", (user, host))
    except Exception:
        return set()
    privs = set()
    for (g,) in cur.fetchall():
        privs |= _parse_grant_privs(g)
    return privs


def _can_login(host, port, user, password, timeout=3):
    """实连校验账号密码。

    caching_sha2_password 的 authentication_string 形如 ``$A$005$<随机盐><摘要>``，
    盐每次 ALTER 都变，无法在管理端本地复算比对，故改用「真的连一次」判断是否一致。
    """
    if not host or not port:
        return False
    conn = None
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
        return True
    except Exception:
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# 「插件已迁移、但实连校验仍失败」时去重用的记忆表，避免每周期重复 ALTER 写 binlog。
# key 含 passwd：密码一变 key 就变，仍能第一时间修正。
_alter_memo = {}
_ALTER_MEMO_TTL = 3600


def _ensure_account(cur, user, host, passwd, idn_expr, grant_sql=None,
                    conn_host=None, conn_port=None, plugin=None):
    """幂等确保账号存在、密码正确、权限正确；无变化时完全不执行写操作（不产生 binlog）。

    这是根治「主库每周期被 ALTER/GRANT 刷写、副本经复制持续写盘」的关键：
    稳定态下本函数只做 SELECT 比对，0 条写语句 -> 主库 binlog 静默 -> 副本空闲即静默。

    plugin 为目标认证插件；当它非 mysql_native_password（如 caching_sha2_password）时，
    本地无法复算哈希，改为「插件一致 + 实连校验通过」即判定密码正确、跳过 ALTER。
    """
    try:
        cur.execute("SELECT plugin, authentication_string FROM mysql.user "
                    "WHERE user=%s AND host=%s", (user, host))
        row = cur.fetchone()
    except Exception:
        # 极旧版本 mysql.user 无 plugin 列：退回只查 authentication_string
        cur.execute("SELECT NULL, authentication_string FROM mysql.user "
                    "WHERE user=%s AND host=%s", (user, host))
        row = cur.fetchone()
    want = _native_password_hash(passwd)

    need_alter = False
    if row is None:
        # 账号不存在：创建（CREATE 同时设密码），随后跳过 ALTER
        cur.execute("CREATE USER IF NOT EXISTS %s@%s " + idn_expr, (user, host, passwd))
    else:
        cur_plugin, cur_hash = row[0], row[1]
        if plugin and cur_plugin and cur_plugin != plugin:
            need_alter = True                      # 插件迁移（native -> caching_sha2）
        elif plugin and plugin != "mysql_native_password":
            need_alter = not _can_login(conn_host, conn_port, user, passwd)
        else:
            need_alter = (cur_hash != want)

    if need_alter:
        memo_key = (conn_host, conn_port, user, host, passwd)
        now = time.time()
        last = _alter_memo.get(memo_key, 0)
        if plugin and plugin != "mysql_native_password" and now - last < _ALTER_MEMO_TTL:
            need_alter = False                     # 刚改过仍连不上，本周期不再重复写 binlog
        else:
            _alter_memo[memo_key] = now
    if need_alter:
        cur.execute("ALTER USER %s@%s " + idn_expr, (user, host, passwd))
    if grant_sql:
        want_privs = _parse_grant_privs(grant_sql)
        have = _granted_privs(cur, user, host)
        if want_privs and not want_privs.issubset(have):
            # ALL PRIVILEGES 需单独判定：MySQL 8 的 SHOW GRANTS 会把它**展开**成一长串
            # 具体权限（不含 "ALL PRIVILEGES" 字面量），直接做子集判断会误判为「权限缺失」，
            # 导致每周期重复 GRANT -> 重新引入「主库空闲也持续写 binlog、副本跟着写盘」。
            # MariaDB 则直接给出 "ALL PRIVILEGES" 字面量，两种都必须覆盖。
            if "ALL PRIVILEGES" in want_privs and _has_all_privileges(cur, user, host):
                return
            cur.execute(grant_sql)


# 判定「已拥有全部权限」的签名集：这些权限只有 ALL PRIVILEGES 才会同时具备，
# 用于识别 MySQL 8 展开式的 SHOW GRANTS（无法靠 "ALL PRIVILEGES" 字面量匹配）。
_ALL_PRIV_SIG = {
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD",
    "SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER", "SUPER",
    "CREATE USER", "TRIGGER",
}


def _has_all_privileges(cur, user, host):
    """判断账号是否已拥有全部权限（兼容 MySQL / MariaDB 两种 SHOW GRANTS 形式）。"""
    try:
        cur.execute("SHOW GRANTS FOR %s@%s", (user, host))
        rows = cur.fetchall()
    except Exception:
        return False
    have = set()
    for (g,) in rows:
        if "ALL PRIVILEGES" in (g or "").upper():
            return True
        have |= _parse_grant_privs(g)
    return _ALL_PRIV_SIG.issubset(have)


def _ensure_local_state_grant(cur, ha_user):
    """确保 ha_monitor 能读写节点本地状态库 ha_local（keepalived VIP 检查脚本依赖）。

    根因：MySQL 8.0 分支授予 ha_monitor 的 *.* 权限里没有 INSERT/CREATE，
    于是 set_local_master_flag 的 `REPLACE INTO ha_local.state` 报 1142，
    被上层 `except: pass` 吞掉 —— ha_local.state 永远为空，
    节点上的 keepalived 检查脚本读不到 current_master，VIP 无法归属/漂移。

    这里按**最小权限**只授 ha_local 库（不放大 *.* 权限）。
    返回是否授权成功（失败通常意味着当前连接无 GRANT OPTION）。
    """
    try:
        cur.execute("CREATE DATABASE IF NOT EXISTS ha_local")
    except Exception:
        pass   # 无 CREATE 权限时，库通常已在节点初始化阶段建好
    try:
        cur.execute(
            "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON ha_local.* TO '%s'@'%%'" % ha_user)
        return True
    except Exception:
        return False


def ensure_ha_creds(host, port, root_user, root_pass,
                    ha_user, ha_pass, repl_user, repl_pass,
                    app_user="", app_pass="", manager_ip="", manager_subnet=None):
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
    local_state_ok = [False]

    def _sync(cur):
        # 判定风味：MariaDB 与 MySQL 的账号/权限语法【不兼容】，必须分派。
        # MariaDB 无 SYSTEM_VARIABLES_ADMIN 等 MySQL 8.0 专有特权（直接 GRANT 会 1064）；
        # 但 MariaDB 10.5+ 提供 REPLICATION SLAVE ADMIN，12.x 执行 CHANGE MASTER/START SLAVE
        # 必须有该特权，故 MariaDB 分支需授予 REPLICATION SLAVE ADMIN（不可省略）。
        try:
            flavour = detect_flavour(cur)
        except Exception:
            flavour = F_MYSQL8
        # 认证插件：MySQL >= 8.0.34 用 caching_sha2_password，消除
        # "[MY-013360] mysql_native_password is deprecated" 告警（8.4 起该插件默认不可用）。
        # MariaDB 始终用自带的 mysql_native_password（无废弃问题）。
        version_str = ""
        try:
            cur.execute("SELECT VERSION()")
            _vr = cur.fetchone()
            version_str = (_vr[0] if _vr else "") or ""
        except Exception:
            version_str = ""
        try:
            from .db import get_setting            # 局部导入，避免模块级循环依赖
            _prefer = (get_setting("AUTH_PLUGIN") or "auto")
        except Exception:
            _prefer = "auto"
        auth_plugin = default_auth_plugin(version_str, None, _prefer)
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
            idn = "IDENTIFIED WITH " + auth_plugin + " BY %s"
            idn_root = "IDENTIFIED WITH " + auth_plugin + " BY %s"
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
        # 幂等：账号/密码/权限无变化则不写任何语句（见 _ensure_account），
        # 彻底消除「主库每周期 ALTER/GRANT -> binlog -> 副本重放再写盘」的空闲写盘。
        for u, p, grant_sql in accts:
            _ensure_account(cur, u, "%", p, idn, grant_sql,
                            conn_host=host, conn_port=port, plugin=auth_plugin)
        # 关键：把 root@'<管理器IP>' 与 root@'%' 密码校正为管理器库存 root_pass。
        #  - manager_ip 的 root 用于管理器自愈回退；
        #  - root@'%' 用于内网 Navicat/其它主机远程连接（用户要求可远程登录）。
        # manager_ip 为空时跳过该条，但 root@'%' 始终创建（便于内网运维）。
        root_targets = []
        if manager_ip:
            root_targets.append(manager_ip)
        if manager_subnet:
            # c/s 同机：管理器容器连【同机】节点时源 IP 是 docker 网关/容器网段，
            # 并非 manager_ip（真实宿主机 IP），故追加 docker 子网通配符白名单。
            root_targets.append(manager_subnet)
        root_targets.append("%")  # 内网远程访问
        for rh in root_targets:
            # 必须与 node_gen 生成的 fix-node-creds.sql 保持一致：
            # root 远程账号必须是 ALL PRIVILEGES + WITH GRANT OPTION。
            # 否则 root 只有 USAGE（CREATE USER 默认仅 USAGE）：一旦 ha_monitor 权限退化
            # （自身无 GRANT OPTION，无法自行补齐），root 回退也救不了，
            # 账号自愈就彻底锁死成死循环。
            root_grant = ("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION"
                          % (root_user, rh))
            _ensure_account(cur, root_user, rh, root_pass, idn_root, root_grant,
                            conn_host=host, conn_port=port, plugin=auth_plugin)
        # 补授 ha_monitor 的额外特权（已授权时跳过，避免每周期重复 GRANT 写 binlog）。
        #  - CREATE USER：MySQL 8.0 与 MariaDB 均需要（MariaDB 中 CREATE USER 特权即隐含
        #    覆盖 ALTER USER / DROP USER，无需也不能单独 GRANT ALTER/DROP USER——后者在
        #    MariaDB 不是可授权权限，GRANT 会报 1064）。
        #  - READ_ONLY ADMIN：MariaDB 10.11+ / MySQL 8.0 需要，提升主库时关 read_only 用。
        for extra in ("CREATE USER", "READ_ONLY ADMIN"):
            try:
                if extra.upper() not in _granted_privs(cur, ha_user, "%"):
                    cur.execute("GRANT %s ON *.* TO '%s'@'%%'" % (extra, ha_user))
            except Exception:
                pass
        # 节点本地状态库 ha_local 的读写授权（见 _ensure_local_state_grant 说明）
        local_state_ok[0] = _ensure_local_state_grant(cur, ha_user)

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
    # ha_local 的库级 GRANT 需要 GRANT OPTION。_sync 若走的是 ha_monitor（通常没有
    # GRANT OPTION），上面那步会静默失败，这里再用 root 补一次，确保 VIP 状态可写。
    if not local_state_ok[0] and used_for_connect == "ha_monitor" and root_pass:
        try:
            _rc = connect(host, port, root_user, root_pass, timeout=5)
            _ensure_local_state_grant(_rc.cursor(), ha_user)
            _rc.close()
        except Exception:
            pass
    try:
        cur.execute("FLUSH PRIVILEGES")
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    return True, "ok (via %s)" % used
