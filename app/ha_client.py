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
from .config import ACCOUNT_HOST_MODE, segment_of, get_manager_public_ip, is_semi_sync_enabled
from . import mariadb_matrix


# 复制错误码分类 —— 两类语义完全不同，绝不可混为一谈：
#
#  ① 幂等类（1007/1050/1061/1396/1133）：库/表/索引/账号「已存在或不存在」，属 DDL/DCL
#     重放冲突（典型场景：旧主重入、切换后新主重放同名 DDL）。该事务的效果在本节点
#     实际上已经达成，跳过**不丢数据**，自动跳过是安全的。
#
#  ② 数据冲突类（1062/1032/1146）：行内容不同 / 行缺失 / 表缺失，属 DML 层面主从
#     数据**已经不一致**。此时「跳过」= 静默接受分歧：主库那条变更永远不会应用到本节点，
#     从库与主库永久不一致，且只留一条 healed 日志，半年后切主才发现数据对不上、
#     已无法追溯。故【默认禁止自动跳过】，应人工核对或从主库重建从库。
_IDEMPOTENT_SQL_ERRNOS = frozenset({1007, 1050, 1061, 1396, 1133})
_DATA_CONFLICT_SQL_ERRNOS = frozenset({1062, 1032, 1146})
# 兼容旧引用（全集）；是否真的可跳过由 heal_replication 的 allow_data_conflict 决定
_SKIPPABLE_SQL_ERRNOS = _IDEMPOTENT_SQL_ERRNOS | _DATA_CONFLICT_SQL_ERRNOS


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


def _derive_stuck_gtid(cur, syntax):
    """从 SHOW REPLICA STATUS 的 Retrieved/Executed GTID 集合推导当前卡住的事务 GTID。

    仅当存在「已接收但未执行」的事务时有效，返回 MariaDB GTID 格式 'domain-server-seq'。
    MySQL 8.0 的 uuid:seq 格式不在本函数范围（其错误文本通常已含 GTID，由上层正则捕获）。
    """
    cur.execute(show_replica_status_sql(syntax))
    row = _one_dict(cur)
    if not row:
        return None
    r = row.get("Retrieved_Gtid_Set") or ""
    e = row.get("Executed_Gtid_Set") or ""

    def _parse(s):
        d = {}
        for part in s.split(","):
            part = part.strip()
            if not part or "-" not in part:
                continue
            dom, rest = part.split("-", 1)
            sid, seqs = rest.split("-", 1)
            try:
                seq = max(int(x) for x in seqs.split(":"))
                d[int(dom)] = (int(sid), seq)
            except (ValueError, TypeError):
                continue
        return d

    rd, ed = _parse(r), _parse(e)
    cands = []
    for dom, (sid, rseq) in rd.items():
        eseq = ed.get(dom, (None, 0))[1]
        if rseq > eseq:
            cands.append((dom, sid, eseq + 1))
    if not cands:
        return None
    cands.sort()
    dom, sid, nxt = cands[0]
    return "%d-%d-%d" % (dom, sid, nxt)


def heal_replication(host, port, user, password, root_user="root", root_pass="",
                     allow_data_conflict=False):
    """自愈复制：当 Slave_SQL_Running=No 且错误为幂等冲突时，注入空事务跳过该 GTID。

    allow_data_conflict：是否允许跳过「数据冲突类」错误（1062/1032/1146）。
    默认 False —— 这类错误意味着主从数据已经不一致，跳过会静默放大分歧，
    应人工核对或重建从库。仅在运维明确接受风险时由 SKIP_DATA_CONFLICT 开启。


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
            # ---- 1133：复制重放 DCL（GRANT/REVOKE）时目标账号在从库不存在 ----
            # MariaDB 多域 GTID 下 sql_slave_skip_counter 被禁（错误 1966），且错误文本不含事务
            # GTID，无法走空事务注入。最稳妥：在从库补建该账号，重放即成功、复制继续，零数据丢失；
            # 账号后续由 ensure_ha_creds 校正密码/权限。
            if last_code == 1133:
                uh = re.search(r"TO\s+'?([^'@\s]+)'?\s*@\s*'?'?([^'\s,)]+)", last_err) \
                     or re.search(r"FROM\s+'?([^'@\s]+)'?\s*@\s*'?'?([^'\s,)]+)", last_err)
                if uh:
                    cu, ch = uh.group(1), uh.group(2)
                    try:
                        cur.execute(stop_replication_sql(syntax))
                        cur.execute("CREATE USER IF NOT EXISTS '%s'@'%s'" % (_esc_sql(cu), _esc_sql(ch)))
                        cur.execute(start_replication_sql(syntax))
                        skipped.append("create_user:%s@%s" % (cu, ch))
                        _tiny_sleep()
                        continue
                    except Exception as e:
                        return False, f"1133 补建账号失败({cu}@{ch}): {e}"
                # 解析不到账号则落到下方 1966 兜底（空事务跳过该事务）
            # 数据冲突（1062/1032/1146）：跳过等于静默接受主从数据永久不一致，
            # 默认拒绝自动自愈；仅在运维显式开启 SKIP_DATA_CONFLICT 时才放行。
            if last_code in _DATA_CONFLICT_SQL_ERRNOS and not allow_data_conflict:
                return False, (
                    f"数据冲突(errno {last_code})：跳过会使本节点与主库数据永久不一致，"
                    f"已拒绝自动跳过，需人工核对或从主库重建本节点。{last_err}"
                    f"（若确认可接受风险，请在「系统设置 → 高级切换/监控」开启「允许跳过数据冲突」）")
            if last_code and last_code not in _SKIPPABLE_SQL_ERRNOS and not m:
                return False, f"复制错误码 {last_code} 非幂等冲突，需人工处理：{last_err}"
            # 非 GTID 自动定位，或无法解析具体 GTID：用 sql_slave_skip_counter 跳过。
            # 兼容传统 binlog 坐标复制，避免 SET GTID_NEXT 不生效导致自愈失败、复制卡死。
            try:
                cur.execute(stop_replication_sql(syntax))
                cur.execute("SET GLOBAL sql_slave_skip_counter = 1")
                cur.execute(start_replication_sql(syntax))
                skipped.append("skip_counter")
            except Exception as e:
                em = str(e)
                # MariaDB 多域 GTID：sql_slave_skip_counter 被禁（错误 1966）——改用「空事务注入」
                # 跳过卡住的事务：从 SHOW REPLICA STATUS 的 Retrieved/Executed GTID 集合推导其 GTID。
                if "1966" in em and auto_pos == 1:
                    try:
                        gx = _derive_stuck_gtid(cur, syntax)
                    except Exception:
                        gx = None
                    if gx:
                        try:
                            cur.execute("SET GTID_NEXT='%s'" % gx)
                            cur.execute("BEGIN")
                            cur.execute("COMMIT")
                            cur.execute("SET GTID_NEXT='AUTOMATIC'")
                            cur.execute(start_replication_sql(syntax))
                            skipped.append("gtid:%s" % gx)
                            _tiny_sleep()
                            continue
                        except Exception as e2:
                            return False, f"空事务跳过失败({gx}): {e2}"
                    return False, f"多域GTID无法跳过(1966)且无可用GTID: {em}"
                return False, f"跳过冲突事务失败(sql_slave_skip_counter): {em}"
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


def is_gtid_purged_error(err):
    """复制 IO 错误是否为「主库 binlog 已清理 / 从库状态过旧」类 1236。

    典型文本：
      Got fatal error 1236 from master when reading data from binary log:
      'Could not find GTID state requested by slave in any binlog files.
       Probably the slave state is too old and required binlog files have been purged.'

    这类故障【无法自愈】：重挂复制（RESET SLAVE + CHANGE MASTER）只会重新请求同一批
    已被清理的 GTID，注入空事务（heal_io_gtid_gap）解决的是相反方向（从库比主库多
    GTID）。唯一解法是从主库重新做一份数据——即「重建从库」。
    """
    e = (err or "").lower()
    if not e:
        return False
    return (
        "could not find gtid state requested by slave" in e
        or "required binlog files have been purged" in e
        or "slave state is too old" in e
        or ("1236" in e and ("purged" in e or "too old" in e))
    )


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
    # WolfSSL(裸机 tarball) 客户端 → OpenSSL(官方镜像) 服务端的 TLS 握手失败会报这个，
    # 属于「连接类」错误，同样靠重建通道（新 SQL 带 MASTER_SSL=0）自愈。
    "ssl connection error",
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


def _stop_replica(cur):
    """完整停掉复制（IO + SQL 两个线程）。

    ⚠ 执行 CHANGE MASTER TO / CHANGE REPLICATION SOURCE TO 之前必须用它，
    只停 IO 线程是不够的：SQL 线程仍在跑时会直接报
        1198: This operation cannot be performed as you have a running slave '';
              run STOP SLAVE '' first
    「一键修复 / 重建复制通道」曾因此整体失败（修复动作看起来毫无效果）。
    """
    for s in ("STOP REPLICA", "STOP SLAVE"):
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
        # 必须完整 STOP SLAVE（含 SQL 线程），否则 CHANGE MASTER TO 报 1198。
        _stop_replica(scur)
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
            quser = _esc_sql(repl_user)
            qpass = _esc_sql(repl_pass or "")
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
        _stop_replica(scur)
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
        # 用参数化执行，避免 repl_pass 含 ' / \ 时破坏语句（自定义密码场景）。
        hosts = ["%"]
        if slave_ip and slave_ip != master_host:
            hosts.append(slave_ip)
        for h in hosts:
            try:
                cur.execute(
                    "CREATE USER IF NOT EXISTS %s@%s "
                    "IDENTIFIED WITH mysql_native_password BY %s",
                    (repl_user, h, repl_pass))
            except Exception:
                # 5.7/MariaDB 不支持 CREATE USER 作 GRANT 项等：忽略，后面用 ALTER 兜底
                pass
            cur.execute(
                "ALTER USER %s@%s IDENTIFIED WITH mysql_native_password BY %s",
                (repl_user, h, repl_pass))
            cur.execute(
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s@%s",
                (repl_user, h))
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


def _esc_sql(s):
    """转义 SQL 字符串字面量（反斜杠 + 单引号），供所有把凭证/辨识名拼进 SQL 的语句使用。

    集群账号名/密码允许管理员自定义，含 `'` 或 `\\` 时若不转义，整条语句语法错误 ->
    账号静默建不出来（表现为「自定义用户名密码登不上」）。参数化 execute 已安全，此处
    仅用于少数无法参数化、必须用字符串拼装的授权/降级语句（用户名无特殊字符但为稳妥也转义）。
    """
    return str(s if s is not None else "").replace("\\", "\\\\").replace("'", "''")


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


def db_uptime(host, port, user, password, root_user="root", root_pass="", timeout=5):
    """返回数据库进程已运行时长（秒）；失败返回 None。

    用于「配置待生效」自动核验：recreate / 重建 / 重启数据库后，进程 Uptime 会重置为
    很小的值；若 Uptime < 待办产生后的经过秒数，说明数据库在 regen 之后被重建或重启过，
    该待办已落实，可自动清除。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception:
        return None
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SHOW STATUS LIKE 'Uptime'")
        row = cur.fetchone()
        if row:
            try:
                return int(row[1])
            except (TypeError, ValueError):
                return None
        return None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
        # 半同步复制健康指标（SEMI_SYNC=0 时变量恒为 OFF，不影响核心探活）
        semi_master_status=None, semi_slave_status=None,
        semi_master_clients=None, semi_no_times=None,
        semi_no_tx=None, semi_yes_tx=None,
        # 复制位点（GTID 执行集合）：用于 monitor 区分「真在追平」与「SQL 线程卡死」
        exec_gtid="",
        # 数据交互状态：主库视角下的下游从库列表（SHOW REPLICAS / SHOW SLAVE HOSTS）
        downstream=[],
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
                # 复制位点（GTID 执行集合）：断电位点不前进 = SQL 线程卡死，而非真落后。
                # MySQL/MariaDB GTID 模式均返回 Executed_Gtid_Set；老 MariaDB 退回 Gtid_Slave_Pos。
                eg = (d.get("Executed_Gtid_Set") or d.get("Gtid_Slave_Pos") or "").strip()
                result["exec_gtid"] = eg
            else:
                # 无复制通道：主库或尚未建立复制。slave_io/sql 保持 None，避免被误判为 No/No 故障
                result["mode"] = result.get("mode") or "master"
                result["role"] = result.get("role") or "master"
                result["slave_io"] = None
                result["slave_sql"] = None
                # 主库视角：列出下游从库（数据推送给谁）。SHOW REPLICAS(>=8.0.22/10.5)
                # / SHOW SLAVE HOSTS 的 Host 取自从库 report_host，可能为空（故仅保留非空项）。
                try:
                    cur.execute("SHOW REPLICAS" if syntax == "new" else "SHOW SLAVE HOSTS")
                    for _r in cur.fetchall():
                        _d = dict(zip([c[0] for c in cur.description], _r))
                        _dh = (_d.get("Host") or "").strip()
                        _dp = _d.get("Port")
                        if _dh:
                            result["downstream"].append(
                                {"host": _dh, "port": int(_dp) if _dp else None})
                except Exception:
                    pass
        except Exception:
            pass
        # 半同步复制健康指标（与复制探测同级；SEMI_SYNC 关闭时变量恒为 OFF，不告警）
        try:
            cur.execute("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync%'")
            ss = dict(cur.fetchall())
            def _si(k):
                try:
                    return int(ss.get(k) or 0)
                except Exception:
                    return None
            # MySQL 8.0.26+ 将半同步状态变量重命名（master→source, slave→replica），
            # 故同时兼容新旧两套命名，确保 MySQL 8.0 与 MariaDB 都能正确采集半同步健康度。
            def _semi_key(old, new):
                return old if old in ss else new
            result["semi_master_status"] = ss.get(_semi_key("Rpl_semi_sync_master_status", "Rpl_semi_sync_source_status"))
            result["semi_slave_status"] = ss.get(_semi_key("Rpl_semi_sync_slave_status", "Rpl_semi_sync_replica_status"))
            result["semi_master_clients"] = _si(_semi_key("Rpl_semi_sync_master_clients", "Rpl_semi_sync_source_clients"))
            result["semi_no_times"] = _si(_semi_key("Rpl_semi_sync_master_no_times", "Rpl_semi_sync_source_no_times"))
            result["semi_no_tx"] = _si(_semi_key("Rpl_semi_sync_master_no_tx", "Rpl_semi_sync_source_no_tx"))
            result["semi_yes_tx"] = _si(_semi_key("Rpl_semi_sync_master_yes_tx", "Rpl_semi_sync_source_yes_tx"))
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
        # MySQL 8.0 专有：super_read_only=ON 会强制 read_only=ON 且【拒绝】直接关 read_only
        # （关只读必须先关 super_read_only）；开只读则一并开，保证从库彻底只读。
        # MariaDB 无该变量，语句报错属正常，忽略即可（不影响后一句 read_only）。
        try:
            cur.execute("SET GLOBAL super_read_only=%s", (1 if on else 0,))
        except Exception:
            pass
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
        # MySQL 8.0 专有：super_read_only=ON 会强制 read_only=ON 并拒绝业务写入（报 1290），
        # 且不关闭它就直接关 read_only 会被拒绝/无效。故必须先关 super_read_only。
        # MariaDB 无该变量，语句报错属正常，忽略即可（不影响后一句执行）。
        try:
            cur.execute("SET GLOBAL super_read_only=0")
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


def _addr_eq(addr, host, port):
    """比较形如 'IP:port' 的字符串与 (host, port) 是否指向同一个实例。"""
    try:
        s = str(addr or "").strip()
        if not s:
            return False
        if ":" in s:
            h, _, p = s.rpartition(":")
        else:
            h, p = s, str(port)
        return h.strip() == str(host).strip() and int(p) == int(port)
    except Exception:
        return False


def _local_state_self_master(cur, host, port):
    """读取节点本机 ha_local.state（keepalived 检查脚本判定 VIP 归属的唯一依据）。

    返回 (is_self, recorded)：recorded 为形如 'IP:port' 的字符串或 None。
    读不到表/无记录时返回 (False, ...)，不阻断正常流程。
    """
    try:
        cur.execute("SELECT v FROM ha_local.state WHERE k='current_master'")
        row = cur.fetchone()
    except Exception:
        return False, None
    rec = (row[0] if row else None)
    return _addr_eq(rec, host, port), rec


def get_node_utc_time(host, port, user, password, root_user="root", root_pass=""):
    """读取节点 UTC 时间（SELECT UTC_TIMESTAMP()），用于时钟漂移检测。

    用 UTC 而非 NOW()：节点与管理器的系统时区可能不同，直接比较本地时间会因
    时区差异产生 8 小时级别的误报；UTC 可完全消除该歧义。

    返回 datetime 或 None。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    except Exception:
        return None
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT UTC_TIMESTAMP()")
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_binlog_usage(host, port, user, password, root_user="root", root_pass=""):
    """读取 binlog 占用总量（无需 SSH / OS 权限，纯 MySQL 侧可得）。

    为什么重要：远程节点没有 agent，OS 级磁盘指标不可得（api.py 注释明确说明），
    而「binlog 堆积撑爆磁盘」是 MySQL 最高频的生产事故之一——复制一断，binlog
    只增不减。这里用 SHOW BINARY LOGS 求和即可拿到占用量。

    返回 (total_bytes, file_count, oldest_file)；失败返回 (None, None, None)。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    except Exception:
        return None, None, None
    if conn is None:
        return None, None, None
    try:
        cur = conn.cursor()
        cur.execute("SHOW BINARY LOGS")
        rows = cur.fetchall()
        if not rows:
            return 0, 0, None
        total = 0
        oldest = None
        for r in rows:
            try:
                total += int(r[1])
            except (TypeError, ValueError, IndexError):
                continue
            if oldest is None:
                oldest = r[0]
        return total, len(rows), oldest
    except Exception:
        return None, None, None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def purge_binlog_before(host, port, user, password, dt_str, root_user="root", root_pass=""):
    """PURGE BINARY LOGS BEFORE <dt_str>。

    ⚠ 危险操作：调用方必须已确认**所有从库都已追平**（否则从库需要的 binlog 被删，
    复制会永久报 1236，只能重建从库）。故仅在 monitor 的 binlog 守卫里、
    且 AUTO_PURGE_BINLOG=1 且全部从库无延迟时才会被调用。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    except Exception as e:
        return False, f"连接失败: {e}"
    if conn is None:
        return False, "无法连接节点(ha_monitor/root 均失败)"
    try:
        cur = conn.cursor()
        cur.execute("PURGE BINARY LOGS BEFORE '%s'" % str(dt_str).replace("'", ""))
        return True, f"已清理 {dt_str} 之前的 binlog"
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
    因此    【严禁 RESET MASTER】：一旦清空本机 GTID 执行集合，新主库会把它
    曾发过的同名事务（如 CREATE DATABASE）当作新事务重放，从而触发
    「database exists / duplicate entry」等 1007/1062 错误，导致
    Slave_SQL_Running=No。依赖 GTID 自动定位即可干净重入，无需 RESET MASTER。

    唯一例外：若本节点「曾为主库」（其 gtid_binlog_pos 非空，含自有 GTID 身份），
    重指向其他主库时该自有 GTID 与新主库同 domain-server 序列在 gtid_strict_mode
    下必然冲突（out-of-order）。此情形必须 RESET MASTER 清空自有 binlog，
    再按已应用集/主库位点重设 gtid_slave_pos——否则干净从库无法建立（见下方 _own_gt 分支）。

    返回 (ok, msg)。
    """
    # ---- 凭证解密失败守卫（必须在连库前拦截）----
    # crypto.decrypt() 在密钥不匹配时会【原样返回密文】（刻意设计：绝不返回空串，
    # 否则空口令会在下次写库时被永久落库、覆盖真实凭证）。但密文一旦被当明文口令
    # 拼进 CHANGE MASTER，MySQL/MariaDB 会报：
    #   1470 - String 'ENC1:...' is too long for MASTER_PASSWORD (should be no longer than 96)
    # 该报错完全看不出「密钥不匹配」这一真实根因，且会把密文写进复制配置，
    # 表现为「复制永久起不来、源库不可达」，极难定位。这里显式拦截并给出可操作结论。
    if isinstance(repl_pass, str) and repl_pass.startswith("ENC1:"):
        return False, (
            f"{host}:{port} 复制口令仍是 ENC1 密文（解密失败），已中止 CHANGE MASTER 以避免"
            f"把密文写进复制配置。根因：凭证密钥与加密时不一致 —— SECRET_KEY / CRED_KEY 变动，"
            f"或 data/secret_key 丢失被重新生成。处理二选一：① 恢复原 SECRET_KEY 后重启管理器"
            f"（凭证自动恢复）；② 在「集群设置」重新录入复制口令，使其按当前密钥重新加密。")
    if isinstance(password, str) and password.startswith("ENC1:"):
        # 不阻断：ha_monitor 失败时 connect_robust 还能回退 root，贸然中断会扩大故障面。
        print(f"[cred] 警告：{host}:{port} 的 ha_monitor 口令仍是 ENC1 密文（解密失败），"
              f"当前连接将回退 root。请检查凭证密钥是否与加密时一致。")
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        flavour = detect_flavour(cur)

        # ---- VIP 守卫（必须先于任何复制改动执行）----
        # ha_local.state 是 keepalived 检查脚本判定 VIP 归属的**唯一**依据。若本节点该表
        # 记录的 current_master 指向自己，说明 VIP 正绑在本机、业务正在经 VIP 写它。
        # 此时若继续把它挂成从库并 SET GLOBAL read_only=1，业务写入会立刻报
        #   1290 - The MariaDB server is running with the --read-only=ON option
        # 这正是「管理器视角的 current_master」与「keepalived 视角」分裂的典型后果：
        # 例如 ha_local.state 写入失败（缺 BINLOG ADMIN / ha_local 库权限 → 1227/1142），
        # 节点便一直自认主、VIP 不释放，而管理器已选了别的主，于是把它当从库压成只读。
        # 分裂时保持其可写并告警，远好于静默压只读导致业务整体不可写。
        _self_m, _rec = _local_state_self_master(cur, host, port)
        _target = "%s:%s" % (master_host, int(master_port))
        if _self_m and not _addr_eq(_target, host, port):
            # ---- 先自愈，再决定是否拦截（F16，2026-09-16）----
            # 本节点的 ha_local.state **只有管理器会写**。既然管理器此刻已认定当前主是
            # _target（另一节点），那么本机这份「我是主」的记录就是**过期错误数据**。
            # 旧实现只做「拒绝降级」，形成死锁：
            #   状态推送失败 → 本机记录过期 → 守卫拒绝降级 → 记录永远得不到纠正；
            # 后果有两层，且互相放大：
            #   ① 回归的旧主既不重入集群、又保留自己的 binlog 继续接受写入 → 静默分叉；
            #   ② 真主（_target）在 keepalived 那侧也可能因同类过期记录而拒绝持有 VIP
            #      → 两节点同时放弃 → VIP 全网消失（实测现象）。
            # 故此处就地把本机 current_master 纠正为 _target，再复读确认；纠正成功即放行。
            try:
                cur.execute("SELECT @@GLOBAL.read_only")
                _ro = cur.fetchone()
                _v = "" if _ro is None else str(_ro[0]).strip().upper()
                _writable = _v in ("0", "OFF", "")
            except Exception:
                _writable = True
            if not _writable:
                # 已被 fencing 置为只读（read_only=ON）：改造为从库不会让情况更糟，
                # 反而能让它重新入集群并恢复复制，故放行（保持原语义）。
                print(f"[vip-guard] {host}:{port} 自认主(ha_local={_rec!r})但已为只读，"
                      f"放行重挂载为 {_target} 的从库以恢复复制。")
                _self_m = False
            else:
                _fixed = False
                _ferr = ""
                try:
                    # 与会话内其它写入保持一致：ha_local 是节点本地状态表，绝不进 binlog，
                    # 否则该写入会被复制到从库、并可能在从库上报 1062 卡死复制线程。
                    cur.execute("SET SESSION sql_log_bin=0")
                    cur.execute(
                        "REPLACE INTO ha_local.state (k, v) VALUES ('current_master', %s)",
                        (_target,))
                    _fixed = True
                except Exception as _e_fix:
                    _ferr = "%s: %s" % (type(_e_fix).__name__, _e_fix)
                finally:
                    try:
                        cur.execute("SET SESSION sql_log_bin=1")
                    except Exception:
                        pass
                if _fixed:
                    print(f"[vip-guard] {host}:{port} 本地 current_master 已由 {_rec!r} "
                          f"就地纠正为 {_target}（自愈），继续挂载为从库。")
                    _self_m = False
                else:
                    print(f"[vip-guard] {host}:{port} 就地纠正本地 current_master 失败：{_ferr}")
        if _self_m and not _addr_eq(_target, host, port):
            msg = (f"[vip-guard] 拒绝把 {host}:{port} 改造为从库：其本机 ha_local.state 仍记录 "
                   f"current_master={_rec!r}（keepalived 据此把 VIP 绑在该节点），与本次要挂载的"
                   f"主库 {_target} 不一致，且就地纠正该记录也未成功。已跳过本次重指向并保留其"
                   f"可写状态，避免业务经 VIP 写入报 1290。请检查该节点 ha_local 库与 "
                   f"ha_monitor 账号权限（需 BINLOG ADMIN 及 ha_local 库的 "
                   f"SELECT/INSERT/UPDATE/CREATE），以及到该节点的网络连通性。")
            print(msg)
            try:
                from .alerts import send_alert   # 局部导入，避免模块级循环依赖
                send_alert("VIP 与主库视角分裂", msg)
            except Exception:
                pass
            return False, msg
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
        # ---- GTID 分叉感知（升版重启/历史切换后从库执行集可能超前或分叉于主库）----
        # 正常重指向：RESET SLAVE ALL + MASTER_USE_GTID=slave_pos 即可（GTID 自动定位跳过已执行事务）。
        # 但若从库「自有 binlog(gtid_binlog_pos) 或 已应用集(gtid_slave_pos)」不是主库
        # gtid_binlog_pos 的子集（如本例 100 曾为主、自有 binlog 含 3-3-17737，主库却发 3-3-1），
        # slave_pos 自动定位失败并触发「out-of-order sequence」(gtid_strict_mode)，复制卡死。
        # 此时 RESET MASTER 清空从库自有 binlog 解除乱序冲突；gtid_slave_pos 默认保留（让复制从
        # 已应用位点续传、重放主库其余 domain 最终一致）。仅当 gtid_slave_pos 本身也非子集
        # （极少见：从库应用过主库没有的事务）才把 gtid_slave_pos 对齐到主库当前位置跳过重放。
        try:
            cur.execute("SELECT @@gtid_slave_pos, @@gtid_binlog_pos")
            _sr = cur.fetchone() or [None, None]
            _slave_gt = _sr[0] or ""
            _own_gt = _sr[1] or ""
            _mconn = connect_robust(master_host, int(master_port), user, password, root_user, root_pass)
            if _mconn is not None:
                _mcur = _mconn.cursor()
                _mcur.execute("SELECT @@gtid_binlog_pos")
                _master_gt = (_mcur.fetchone() or [None])[0] or ""
                _mconn.close()
                if _master_gt and (_slave_gt or _own_gt):
                    _pos_ok = False
                    if _slave_gt:
                        cur.execute("SELECT GTID_SUBSET(%s, %s)", (_slave_gt, _master_gt))
                        _pos_ok = bool((cur.fetchone() or [0])[0])
                    # 从库「自有 binlog GTID」(gtid_binlog_pos) 非空 = 它曾为源（曾是主库）。
                    # 重指向其他主库时，其自有 GTID 与该主库同 domain-server 序列在
                    # gtid_strict_mode 下必然冲突（out-of-order）；而 GTID_SUBSET(自有,主库)
                    # 常判为「子集」会漏掉此场景（同序列、不同源）。故「曾为源」即清空自有
                    # binlog，再按已应用集/主库位点重设 gtid_slave_pos，彻底解除乱序冲突。
                    if _own_gt:
                        print("[gtid] %s:%s 曾为源(自有binlog GTID=%s)，RESET MASTER 解除乱序冲突" % (host, port, _own_gt))
                        try:
                            cur.execute(stop_replication_sql(flavour))
                            cur.execute(reset_replica_sql(flavour))
                            cur.execute("RESET MASTER")
                            if _slave_gt and _master_gt:
                                cur.execute("SELECT GTID_SUBSET(%s, %s)", (_slave_gt, _master_gt))
                                if bool((cur.fetchone() or [0])[0]):
                                    # 已应用集为主库子集：保留以免从头重放报 1062
                                    cur.execute("SET GLOBAL gtid_slave_pos=%s", (_slave_gt,))
                                else:
                                    # 已应用集也分叉：对齐到主库当前位置，跳过重放避免 1236
                                    cur.execute("SET GLOBAL gtid_slave_pos=%s", (_master_gt,))
                                    print("[gtid] %s:%s 已应用集分叉，gtid_slave_pos 已对齐主库当前 %s" % (host, port, _master_gt))
                            elif _slave_gt:
                                cur.execute("SET GLOBAL gtid_slave_pos=%s", (_slave_gt,))
                            elif _master_gt:
                                cur.execute("SET GLOBAL gtid_slave_pos=%s", (_master_gt,))
                        except Exception as e2:
                            print("[gtid] 分叉重建失败(可忽略，继续常规重链): %s" % e2)
                    elif not _pos_ok:
                        # 纯净从库（无自有 binlog GTID）：仅已应用集非主库子集才对齐（原有逻辑）
                        print("[gtid] %s:%s 本地 GTID 非主库子集(分叉)，RESET MASTER 解除冲突后重链" % (host, port))
                        try:
                            cur.execute(stop_replication_sql(flavour))
                            cur.execute(reset_replica_sql(flavour))
                            cur.execute("RESET MASTER")
                            if _slave_gt and not _pos_ok:
                                # 已应用集也分叉：对齐到主库当前位置，跳过重放避免 1236
                                cur.execute("SET GLOBAL gtid_slave_pos=%s", (_master_gt,))
                                print("[gtid] %s:%s 已应用集分叉，gtid_slave_pos 已对齐主库当前 %s" % (host, port, _master_gt))
                        except Exception as e2:
                            print("[gtid] 分叉重建失败(可忽略，继续常规重链): %s" % e2)
        except Exception as e:
            print("[gtid] 分叉检测跳过(可忽略): %s" % e)

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
        # 仅 MySQL 支持 CHANGE REPLICATION FILTER；MariaDB 无此语句（返回 None），
        # 直接跳过——否则每次重链都刷一条 1064 语法错误日志。
        _flt_sql = change_replication_filter_sql(["ha_local"], flavour=flavour)
        if _flt_sql:
            try:
                cur.execute(_flt_sql)
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
    返回 bool；需要失败原因时改用 set_local_master_flag_ex()。
    """
    return set_local_master_flag_ex(host, port, user, password, master_str,
                                    root_user, root_pass)[0]


def set_local_master_flag_ex(host, port, user, password, master_str,
                             root_user="root", root_pass=""):
    """同 set_local_master_flag，但返回 (ok, err)，err 为人类可读的失败原因。

    **这个诊断能力是刻意加的**：本函数是 ha_local.state 的唯一写入点，而 keepalived
    完全依赖这份本地记录判定 VIP 归属。早期实现把所有异常压成 `return False`，
    使「节点本地状态长期停留在幽灵主库 IP」这类致命故障在管理端完全无声——
    实测后果是故障切换后 VIP 在两个节点上被同时放弃（VIP 凭空消失）。
    任何失败都必须能说清是「连不上 / 无 BINLOG ADMIN / 建表被拒 / 写入被拒 / 表不存在」。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root 均失败)"
    try:
        cur = conn.cursor()
        # MariaDB 10.5+：SET SESSION sql_log_bin 需要 BINLOG ADMIN，仅 SUPER 不够。
        # ha_monitor 账号若缺该权限，connect_robust 会以它连上（连接本身成功），
        # 但下面这句报 1227 → 整个写入被静默吞掉 → ha_local.state 永远为空。
        # 这里先自检一次，失败就主动改用 root 连接重来，而不是直接放弃。
        try:
            cur.execute("SET SESSION sql_log_bin=0")
            cur.execute("SET SESSION sql_log_bin=1")
        except Exception as e_binlog:
            try:
                conn.close()
            except Exception:
                pass
            if not root_user or not root_pass:
                return False, ("ha_monitor 无 BINLOG ADMIN（%s），且未提供节点 root 口令，"
                               "无法关闭会话 binlog 写入 ha_local.state" % e_binlog)
            conn2 = None
            try:
                conn2 = pymysql.connect(host=host, port=int(port), user=root_user,
                                        password=root_pass, connect_timeout=4, autocommit=True)
            except Exception as e_root:
                return False, ("ha_monitor 无 BINLOG ADMIN（%s），root 回退连接亦失败：%s"
                               % (e_binlog, e_root))
            if conn2 is None:
                return False, "ha_monitor 无 BINLOG ADMIN，root 回退连接返回空"
            conn = conn2
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
                return True, None
            cur.execute(
                "REPLACE INTO ha_local.state (k, v) VALUES ('current_master', %s)",
                (master_str,),
            )
        finally:
            try:
                cur.execute("SET SESSION sql_log_bin=1")
            except Exception:
                pass
        return True, None
    except Exception as e:
        return False, "%s: %s" % (type(e).__name__, e)
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


def _account_hosts(manager_ip=None):
    """按 ACCOUNT_HOST_MODE 返回要为 ha_monitor / replication / app 创建的 host 列表。

    legacy（默认）: ['%']            仅 @'%'，与历史行为完全一致
    additive      : ['%', '<段>.%']  追加网段账号，**保留 @'%' 兜底**（灰度第一步，最安全）
    segment       : ['<段>.%']       仅网段（灰度终态；不自动 DROP @'%'，可随时改回回滚）

    网段推导失败（manager_ip 为空 / 非 IPv4）时一律回退 ['%']：
    宁可维持现状，也绝不赌网段 —— 猜错会让管理器连不上所有节点，
    这比保留 % 危险得多（参考 config.get_manager_public_ip 关于 docker 桥接
    探测错位的说明）。
    """
    if ACCOUNT_HOST_MODE not in ("additive", "segment"):
        return ["%"]
    seg = segment_of(manager_ip or "")
    if not seg:
        return ["%"]
    return ["%", seg] if ACCOUNT_HOST_MODE == "additive" else [seg]


def ensure_semisync_runtime(host, port, ha_user, ha_pass, root_user=None,
                            root_pass=None, timeout=5):
    """按【节点】开启半同步（运行期 SET GLOBAL）——补齐**副本**侧开关。

    为什么必须单独做这件事（根因）：
        ensure_ha_creds 只在主库执行。账号/权限会经复制同步到副本，
        但 **SET GLOBAL 不会复制**，所以副本的 rpl_semi_sync_*_enabled 永远停在 OFF；
        主库等不到任何半同步副本 -> 主库 source_status 与副本 replica_status 恒为 OFF，
        表现为「半同步明明配了却一直 OFF」。
    本函数对**每个节点**单独执行（含副本）。MySQL 要求修改
    rpl_semi_sync_replica_enabled 后重启复制 IO 线程才会以半同步方式重连，
    故副本上检测到开关刚被打开时会自动重启 IO 线程。

    幂等：开关已 ON 时直接返回，不产生任何写操作/不复制。
    返回 (changed: bool, msg: str)
    """
    if not is_semi_sync_enabled():
        return False, "SEMI_SYNC 未开启，跳过"
    conn = None
    last_err = None
    for cu, cp in ((ha_user, ha_pass), (root_user, root_pass)):
        if not cu:
            continue
        try:
            conn = connect(host, port, cu, cp, timeout=timeout)
            break
        except Exception as e:
            last_err = e
    if conn is None:
        return False, "不可达: %s" % last_err
    try:
        cur = conn.cursor()
        try:
            flavour = detect_flavour(cur)
        except Exception:
            return False, "无法判定数据库类型"
        if flavour == F_MARIADB:
            # 幂等：已开启则直接返回，不产生写操作、也【不刷自愈日志】。
            # monitor 仅在 changed=True 时打印自愈信息；若每周期都返回 True，
            # 每个 MariaDB 节点每个间隔都会刷一行「半同步自愈」（MySQL 分支用 already-on 规避）。
            def _on_m(x):
                return str(x or "").strip().upper() in ("ON", "1")

            try:
                cur.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%_enabled'")
                _pre = {r[0]: r[1] for r in (cur.fetchall() or [])}
            except Exception:
                _pre = {}
            if _on_m(_pre.get("rpl_semi_sync_master_enabled")) \
                    and _on_m(_pre.get("rpl_semi_sync_slave_enabled")):
                return False, "already-on"
            # MariaDB 半同步：插件内置，通常由 node_gen 注入的 my.cnf（plugin_load_add +
            # rpl_semi_sync_*_enabled=1）随启动启用。但若节点生成时未注入该块（早于该修复，
            # 或 SEMI_SYNC 后开未 regen），插件可能未加载、变量恒为 OFF —— 这里运行时补启用，
            # 使「半同步配了却一直 OFF」对 my.cnf 缺块也 resilient，无需重装节点。
            def _mset(var):
                try:
                    cur.execute("SET GLOBAL %s=1" % var)
                    return True, ""
                except Exception as e:
                    return False, str(e)

            m_ok, m_err = _mset("rpl_semi_sync_master_enabled")
            s_ok, s_err = _mset("rpl_semi_sync_slave_enabled")

            def _semi_unk(s):
                return "unknown system variable" in s.lower() or "does not exist" in s.lower()

            if (not m_ok or not s_ok) and _semi_unk(m_err + " " + s_err):
                # 插件未加载：运行时 INSTALL PLUGIN（MariaDB 半同步 .so 内置）
                for _sn, _pn in (("semisync_master.so", "rpl_semi_sync_master"),
                                 ("semisync_slave.so", "rpl_semi_sync_slave")):
                    try:
                        cur.execute("INSTALL PLUGIN %s SONAME '%s'" % (_pn, _sn))
                    except Exception as e:
                        if "already loaded" not in str(e).lower():
                            print("[semi] MariaDB 加载插件 %s 失败: %s" % (_pn, e))
                m_ok, m_err = _mset("rpl_semi_sync_master_enabled")
                s_ok, s_err = _mset("rpl_semi_sync_slave_enabled")
            # 最佳努力：对齐 AFTER_SYNC 等待点 + 超时（与 my.cnf 意图一致）
            try:
                cur.execute("SET GLOBAL rpl_semi_sync_master_wait_point='AFTER_SYNC'")
            except Exception:
                pass
            try:
                cur.execute("SET GLOBAL rpl_semi_sync_master_timeout=10000")
            except Exception:
                pass
            restarted = False
            try:
                cur.execute("SHOW SLAVE STATUS")
                _row = cur.fetchone()
                if _row:
                    _cols = [d[0] for d in cur.description]
                    _d = dict(zip(_cols, _row))
                    if _d.get("Slave_IO_Running") == "Yes":
                        cur.execute("STOP SLAVE")
                        cur.execute("START SLAVE")
                        restarted = True
            except Exception:
                pass
            try:
                conn.commit()
            except Exception:
                pass
            try:
                cur.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%_enabled'")
                _now = {r[0]: r[1] for r in (cur.fetchall() or [])}
            except Exception:
                _now = {}
            _m_on = str(_now.get("rpl_semi_sync_master_enabled", "")).strip().upper() in ("ON", "1")
            _s_on = str(_now.get("rpl_semi_sync_slave_enabled", "")).strip().upper() in ("ON", "1")
            if not (_m_on and _s_on):
                _why = (m_err + " " + s_err).strip() or "SET GLOBAL 未生效"
                return False, "MariaDB 半同步开启失败（复核未 ON）：%s" % _why
            return True, "MariaDB 已启用半同步" + ("并重启复制 IO 线程" if restarted else "")
        # 以下为 MySQL 8 原逻辑
        cur.execute("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%_enabled'")
        vals = dict(cur.fetchall())

        def _v(new, old):
            return vals.get(new, vals.get(old))

        def _on(x):
            return str(x or "").strip().upper() in ("ON", "1")

        if _on(_v("rpl_semi_sync_source_enabled", "rpl_semi_sync_master_enabled")) \
                and _on(_v("rpl_semi_sync_replica_enabled", "rpl_semi_sync_slave_enabled")):
            return False, "already-on"
        # 两套命名都试：MySQL 8.0.26+ 用 source/replica，MariaDB（含 11.4 及以下）只有
        # master/slave（source/replica 在 11.5+ 才是别名）。此前只 SET source/replica，
        # 在 MariaDB 上必然失败却被 except: pass 吞掉 → 每轮都报「已开启半同步」的假成功。
        failed = []
        for var in ("rpl_semi_sync_source_enabled", "rpl_semi_sync_replica_enabled",
                    "rpl_semi_sync_master_enabled", "rpl_semi_sync_slave_enabled"):
            try:
                cur.execute("SET GLOBAL %s=1" % var)
            except Exception as e:
                failed.append("%s(%s)" % (var, (str(e) or "").strip()[:60]))
        # 副本：改完开关需重启 IO 线程才会真正以半同步方式重连主库
        restarted = False
        try:
            cur.execute("SHOW REPLICA STATUS")
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            if row:
                d = dict(zip(cols, row))
                if d.get("Replica_IO_Running") == "Yes" or d.get("Slave_IO_Running") == "Yes":
                    cur.execute("STOP REPLICA")
                    cur.execute("START REPLICA")
                    restarted = True
        except Exception:
            pass
        try:
            conn.commit()
        except Exception:
            pass

        # 复核：只有变量真的变成 ON 才算成功。此前无论 SET 是否成功都 return True，
        # 1227 权限不足/变量不存在被吞掉，导致 monitor 每 60s 记一次「自愈成功」，
        # 自愈统计一片大好、半同步却从未真正开启。
        try:
            cur.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%_enabled'")
            now = {r[0]: r[1] for r in (cur.fetchall() or [])}
        except Exception:
            now = {}

        def _now(new, old):
            return now.get(new, now.get(old))

        src_on = _on(_now("rpl_semi_sync_source_enabled", "rpl_semi_sync_master_enabled"))
        rep_on = _on(_now("rpl_semi_sync_replica_enabled", "rpl_semi_sync_slave_enabled"))
        if not (src_on and rep_on):
            why = "；".join(failed) or "SET GLOBAL 未生效（多为 1227 权限不足，需 SUPER/SYSTEM_VARIABLES_ADMIN）"
            return False, "半同步开启失败（复核仍未 ON）：%s" % why
        return True, "已开启半同步" + ("并重启复制 IO 线程" if restarted else "")
    except Exception as e:
        return False, "异常: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_connect_creds(host, port, ha_user, ha_pass, root_user, root_pass,
                         manager_ip=None, manager_subnet=None, timeout=5):
    """仅校正【本节点】的 ha_monitor 与 root 登录凭证，使管理器能连上该节点。

    与 ensure_ha_creds 的区别：
      - 只处理 ha_monitor@'%'（+ @manager_ip）与 root@<各 host>，不动 replication/app 账号
        （replication 账号是主库权威、由主库经复制下发；app 同），故对副本完全安全；
      - 全程在 SET SESSION sql_log_bin=0 下执行，**不写 binlog、不回灌主库**，
        可在副本上直连校正，杜绝账号 DCL 经复制放大 / 元数据锁争用。

    解决：副本重装后 init SQL 写入的 ha_monitor/root 与管理器库存不一致（节点包生成后
    改过口令 / 重加密等），导致管理器连不上副本 -> 副本永远 "down" -> 不被重指向、
    不自动加入集群。优先用 root 连接（具 SUPER 可先关 binlog），不通再回退 ha_monitor。
    返回 (ok, msg)。
    """
    conn = None
    used = None
    for (cu, cp, tag) in ((root_user, root_pass, "root"), (ha_user, ha_pass, "ha_monitor")):
        if not cu:
            continue
        try:
            conn = connect(host, port, cu, cp, timeout=timeout)
            used = tag
            break
        except Exception:
            pass
    if conn is None:
        return False, "不可达（root/ha_monitor 均失败）"
    try:
        cur = conn.cursor()
        try:
            _flavour = detect_flavour(cur)
        except Exception:
            _flavour = F_MYSQL8
        version_str = ""
        try:
            cur.execute("SELECT VERSION()")
            _vr = cur.fetchone()
            version_str = (_vr[0] if _vr else "") or ""
        except Exception:
            pass
        try:
            from .db import get_setting
            _prefer = (get_setting("AUTH_PLUGIN") or "auto")
        except Exception:
            _prefer = "auto"
        _auth = default_auth_plugin(version_str, None, _prefer)
        if _flavour == F_MARIADB:
            idn = "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
            idn_root = "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
        else:
            idn = "IDENTIFIED WITH " + _auth + " BY %s"
            idn_root = "IDENTIFIED WITH " + _auth + " BY %s"
        # 关 binlog：本节点本地校正，不复制到主库（避免 DCL 回灌 / 放大）
        try:
            cur.execute("SET SESSION sql_log_bin=0")
        except Exception:
            pass
        for _h in ("%", manager_ip):
            if not _h:
                continue
            _ensure_account(cur, ha_user, _h, ha_pass, idn, None,
                           conn_host=host, conn_port=port, plugin=_auth)
        root_targets = []
        if manager_ip:
            root_targets.append(manager_ip)
        root_targets.append("127.0.0.1")
        if manager_subnet:
            root_targets.append(manager_subnet)
        root_targets.append("%")
        for rh in root_targets:
            _g = ("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION"
                  % (_esc_sql(root_user), rh))
            _ensure_account(cur, root_user, rh, root_pass, idn_root, _g,
                           conn_host=host, conn_port=port, plugin=_auth)
        return True, ("已校正本节点连接凭证(ha_monitor/root，binlog 安全，以 %s 连接)" % used)
    except Exception as e:
        return False, "异常: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ensure_ha_creds(host, port, root_user, root_pass,
                    ha_user, ha_pass, repl_user, repl_pass,
                    app_user="", app_pass="", manager_ip="", manager_subnet=None,
                    old_ha_user="", old_repl_user="", old_app_user=""):
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
            # F13（2026-09-16 真机）：MariaDB 分支此前**漏定义 idn_repl**，而下方 accts 循环
            # 在 u==repl_user 时引用它 → NameError → ensure_ha_creds 对**所有 MariaDB 节点**
            # 恒失败（返回 False），凭证漂移自愈整条链路静默失效（表现为日志恒
            # "凭证巡检完成：0 个节点已对齐"、漂移永远自愈不了）。MariaDB 复制账号同样用
            # native 插件 + USING PASSWORD 语法，直接复用 idn 即可。
            idn_repl = idn
            # 注：MariaDB 10.5+（含 11/12.x）提供 REPLICATION SLAVE ADMIN / SLAVE MONITOR 特权：
            #  - 12.x 执行 CHANGE MASTER TO / START SLAVE 必须有 REPLICATION SLAVE ADMIN（仅 REPLICATION SLAVE 不够，报 1227）；
            #  - 12.x 执行 SHOW REPLICA STATUS / 读取复制状态必须有 SLAVE MONITOR（监控探活读复制状态用），否则报 1227；
            #  10.x 对这两项不强制，但显式授予对 10.5+ 兼容无害。
            # BINLOG ADMIN：MariaDB 10.5+ 把 `SET SESSION/LOCAL sql_log_bin` 从 SUPER 中拆出，
            # 缺它时 manager 写 ha_local.state（必须禁 binlog，否则从库 1062）会报 1227，
            # 结果主库标记一直写不进去、keepalived 读不到 current_master 而退化判定。
            # 与 node_gen 的 render_mariadb_init_sql / fix-node-creds.sql 保持一致。
            # v2.3.0：特权集由 mariadb_matrix 单一来源提供，并**按服务器真实支持的
            # 特权名过滤**（SHOW PRIVILEGES）—— 不再按版本号猜，也不再靠 except 吞
            # 1064/1227。此前 ha_client 这份比 node_gen 少了 REPLICATION MASTER
            # ADMIN，导致「init 建号有、自愈补号无」的不一致，一并消除。
            _avail = mariadb_matrix.server_privileges(cur)
            ha_grant = ("GRANT %s ON *.* TO '%s'@'%%'"
                        % (mariadb_matrix.ha_privs(_avail), _esc_sql(ha_user)))
            repl_grant = ("GRANT %s ON *.* TO '%s'@'%%'"
                          % (mariadb_matrix.repl_privs(_avail), _esc_sql(repl_user)))
        else:
            idn = "IDENTIFIED WITH " + auth_plugin + " BY %s"
            idn_root = "IDENTIFIED WITH " + auth_plugin + " BY %s"
            # replication 账号强制 mysql_native_password：MySQL 8.0 下非 SSL 复制 IO 线程
            # 对 caching_sha2_password 的握手与客户端 JDBC/驱动不同，常报 Access Denied，
            # 改为 native 后非 SSL 复制即可稳定连通（与节点 init 配方一致）。
            idn_repl = "IDENTIFIED WITH mysql_native_password BY %s"
            ha_grant = ("GRANT REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
                        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* "
                        f"TO '{_esc_sql(ha_user)}'@'%'")
            repl_grant = f"GRANT REPLICATION SLAVE ON *.* TO '{_esc_sql(repl_user)}'@'%'"
        accts = [(ha_user, ha_pass, ha_grant), (repl_user, repl_pass, repl_grant)]
        if app_user and app_pass:
            app_grant = (f"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, "
                         f"REFERENCES, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON *.* "
                         f"TO '{_esc_sql(app_user)}'@'%'")
            accts.append((app_user, app_pass, app_grant))
        # 幂等：账号/密码/权限无变化则不写任何语句（见 _ensure_account），
        # 彻底消除「主库每周期 ALTER/GRANT -> binlog -> 副本重放再写盘」的空闲写盘。
        # 账号 host 策略（开关 ACCOUNT_HOST_MODE，默认 legacy 仅 @'%'，行为完全不变）。
        # 开启后 MySQL 会按 host 精确度优先匹配：网段账号比 @'%' 更具体、优先生效。
        _acct_hosts = _account_hosts(manager_ip)
        for u, p, grant_sql in accts:
            for _h in _acct_hosts:
                _g = grant_sql if _h == "%" else grant_sql.replace("@'%'", "@'%s'" % _h)
                _acct_idn = idn_repl if u == repl_user else idn
                _acct_plugin = "mysql_native_password" if u == repl_user else auth_plugin
                _ensure_account(cur, u, _h, p, _acct_idn, _g,
                                conn_host=host, conn_port=port, plugin=_acct_plugin)
        # 关键：把 root@'<管理器IP>' 与 root@'%' 密码校正为管理器库存 root_pass。
        #  - manager_ip 的 root 用于管理器自愈回退；
        #  - 127.0.0.1 的 root 供 keepalived 的 check_mysql_master.sh 本机 TCP 探活使用
        #    （安装脚本按当时密码建过它，节点 root 口令一旦变更就会漂移 → 探活 1045
        #     → VIP 不上浮，故自愈必须把它刷成节点当前 root 口令）；
        #  - root@'%' 用于内网数据库客户端/其它主机远程连接（用户要求可远程登录）。
        # manager_ip 为空时跳过该条，但 root@'%' 始终创建（便于内网运维）。
        root_targets = []
        if manager_ip:
            root_targets.append(manager_ip)
        root_targets.append("127.0.0.1")
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
                          % (_esc_sql(root_user), rh))
            _ensure_account(cur, root_user, rh, root_pass, idn_root, root_grant,
                            conn_host=host, conn_port=port, plugin=auth_plugin)
        # 补授 ha_monitor 的额外特权（已授权时跳过，避免每周期重复 GRANT 写 binlog）。
        #  - CREATE USER：MySQL 8.0 与 MariaDB 均需要（MariaDB 中 CREATE USER 特权即隐含
        #    覆盖 ALTER USER / DROP USER，无需也不能单独 GRANT ALTER/DROP USER——后者在
        #    MariaDB 不是可授权权限，GRANT 会报 1064）。
        #  - READ_ONLY ADMIN：MariaDB 10.11+ / MySQL 8.0 需要，提升主库时关 read_only 用。
        #  - BINLOG ADMIN：MariaDB 10.5+ 需要，写 ha_local.state 前关闭会话 binlog 用。
        #    （对 MySQL 8.0 不存在该权限，GRANT 会失败但已被 except 吞掉，无副作用。）
        # 账号 DCL（CREATE USER / GRANT）绝不参与复制：节点级账号由各节点独立授予
        # （管理器逐节点直连写入），若经 binlog 复制到从库，从库上被 GRANT 的精确 host 账号
        # （如 ha_monitor@<manager_ip>）往往不存在，重放即报 1133
        # 「Can't find any matching row in the user table」→ SQL 线程卡死。
        # 故此处关会话 binlog，避免把该 GRANT 复制下去（从库由管理器直连单独授予）。
        _binlog_off = False
        try:
            cur.execute("SET SESSION sql_log_bin=0")
            _binlog_off = True
        except Exception:
            pass  # 缺 BINLOG ADMIN/SUPER（如 ha_monitor 首跑尚未获得）时跳过，由复制自愈兜底
        # v2.3.0：补授列表同样按服务器能力过滤（仅 MariaDB 分支）；MySQL 分支保持
        # 原列表与原有行为不变（不存在的特权依旧被 except 吞掉，无副作用）。
        if flavour == F_MARIADB:
            _extra_privs = mariadb_matrix.optional_privs(
                mariadb_matrix.server_privileges(cur))
        else:
            _extra_privs = ("CREATE USER", "READ_ONLY ADMIN", "BINLOG ADMIN",
                            "REPLICATION MASTER ADMIN")
        for extra in _extra_privs:
            # 同时授予 @'%' 与管理器实际连入用的 @manager_ip 账号（精确 host 优先匹配，
            # 管理器大多以 ha_monitor@<manager_ip> 连入，缺 REPLICATION MASTER ADMIN 会导致
            # enable_semisync 的 SET GLOBAL rpl_semi_sync_master_* 报 Access Denied）。
            for _h in ("%", manager_ip):
                if not _h:
                    continue
                try:
                    if extra.upper() not in _granted_privs(cur, ha_user, _h):
                        cur.execute("GRANT %s ON *.* TO '%s'@'%s'" % (extra, _esc_sql(ha_user), _h))
                except Exception:
                    pass
        # ---- F11：消除「精确 host 影子账号」造成的能力落差（2026-09-16 真机）----
        # 现象：节点上残留 ha_monitor@'<管理器IP>'（历史 fix 脚本交叉污染 / 旧版本遗留），
        #   MariaDB / MySQL 按 host **精确度优先**匹配 —— 管理器从管理器主机 IP 连入时
        #   命中该「影子账号」。若它只有 USAGE（或缺少 REPLICATION SLAVE ADMIN /
        #   SLAVE MONITOR），则表现为「连得上、却没权限」：CHANGE REPLICATION SOURCE
        #   报 1227、读复制状态报 1227 → 复制永久起不来、故障切换中止，而错误信息
        #   完全看不出是精确 host 账号在作祟（这正是 177/33 实测中「复制无法自愈」的根因）。
        # 修复：对 ha_monitor **每一个已存在的 host 变体**（并集 管理器IP ∪ '%'）执行全集
        #   授权，彻底抹平影子账号的能力落差。幂等：已具备全集者跳过，不写 binlog。
        #   刻意只补 ha_monitor：repl/app 不参与管理器连入，无需 per-host 变体。
        try:
            cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (ha_user,))
            _ha_hosts = [r[0] for r in (cur.fetchall() or [])]
        except Exception:
            _ha_hosts = []
        _target_privs = _parse_grant_privs(ha_grant)
        _shadow_hosts = set([h for h in _ha_hosts if h] + ["%"]
                            + ([manager_ip] if manager_ip else []))
        for _h in _shadow_hosts:
            try:
                if _target_privs and _target_privs.issubset(_granted_privs(cur, ha_user, _h)):
                    continue
                _g = (ha_grant if _h == "%"
                      else ha_grant.replace("@'%'", "@'%s'" % _h))
                cur.execute(_g)
                print("[ha-shadow] 已为 ha_monitor@%s 补齐全集特权（消除精确 host 影子账号）"
                      % _h, flush=True)
            except Exception as _e:
                print("[ha-shadow] 修复 ha_monitor@%s 权限失败(可忽略): %s" % (_h, _e))
        # 账号改名：删除节点上残留的旧账号（旧账号可能以多个 host 存在）。
        # 必须在 sql_log_bin=0 作用域内执行，避免 DROP USER 经复制下放到从库。
        def _drop_old(cur, old_user, new_user):
            if not old_user or old_user == new_user:
                return
            try:
                cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (old_user,))
                _hs = [r[0] for r in cur.fetchall()]
            except Exception:
                return
            for _h in _hs:
                try:
                    cur.execute("DROP USER IF EXISTS `%s`@`%s`" % (
                        old_user.replace("`", "``"), _h.replace("`", "``")))
                except Exception as _e:
                    print("[drop-old] 删除旧账号 %s@%s 失败: %s" % (old_user, _h, _e))
        _drop_old(cur, old_ha_user, ha_user)
        _drop_old(cur, old_repl_user, repl_user)
        _drop_old(cur, old_app_user, app_user)

        if _binlog_off:
            try:
                cur.execute("SET SESSION sql_log_bin=1")
            except Exception:
                pass
        # 节点本地状态库 ha_local 的读写授权（见 _ensure_local_state_grant 说明）
        local_state_ok[0] = _ensure_local_state_grant(cur, ha_user)

        # 半同步：rpl_semi_sync_*_enabled 不能写进静态 my.cnf——mysqld --initialize
        # 期间插件未加载，会报 unknown variable 并中止初始化（容器崩溃循环）。
        # 故改在运行时 SET GLOBAL 开启，随管理器自愈周期持久化（节点重启后自动重开）。
        if flavour == F_MYSQL8 and is_semi_sync_enabled():
            try:
                cur.execute("SET GLOBAL rpl_semi_sync_source_enabled=1")
                cur.execute("SET GLOBAL rpl_semi_sync_replica_enabled=1")
            except Exception as _se:
                import logging as _lg
                _lg.getLogger(__name__).warning("运行时开启半同步失败: %s", _se)

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


def repair_ha_shadow_grants(host, port, ha_user, ha_pass, root_user="root", root_pass="",
                            manager_ip=""):
    """修复 ha_monitor「精确 host 影子账号」的权限落差（F11，**可安全用于只读副本**）。

    背景（2026-09-16 真机 177/33）：节点上可能残留 ha_monitor@'<管理器IP>' 等
    「精确 host」账号（历史 fix 脚本交叉污染 / 旧版本遗留 / ACCOUNT_HOST_MODE 切换）。
    MySQL / MariaDB 按 host **精确度优先**匹配 —— 管理器从管理器主机 IP 连入时命中该
    影子账号。若它只有 USAGE，则表现为「连得上、却没权限」：CHANGE REPLICATION SOURCE、
    SHOW REPLICA STATUS、写 ha_local 一律报 1227 → **复制永久无法自愈、故障切换中止**，
    而错误信息完全看不出是精确 host 账号在作祟。实测 33 号节点的复制正是被此卡死。

    与 ensure_ha_creds 的关键区别：本函数**只做 GRANT**（不 CREATE / ALTER USER、不改
    密码），因此可在**只读副本**上安全执行。副本上严禁 CREATE/ALTER USER（会与复制
    线程争用 mysql.* 元数据锁，导致 'Waiting for dependent transaction to commit'、
    复制卡死），但「为既有账号补齐授权」是低风险且**必要**的：否则该副本永远无法被
    管理器管理。GRANT 全程 ``SET SESSION sql_log_bin=0``，不复制下去、不污染主库。

    连接：优先 ha_monitor；若它自身即无权影子账号（连也白连），回退 root
    （root@'<管理器IP>' / root@'%' 恒为 ALL PRIVILEGES）。幂等：账号已具全集时不写库。

    返回 (repaired_count, msg)。
    """
    conn = None
    try:
        # GRANT 需要 **GRANT OPTION**，而 ha_monitor 刻意不带该权限（最小权限原则）。
        # 故本修复**优先用 root**（节点上 root@'<管理器IP>' / root@'%' 均为
        # ALL PRIVILEGES WITH GRANT OPTION）；root 不可达时才退回 ha_monitor
        # （仅当它自身已带 GRANT OPTION 时才有意义，否则会 1044/1045 —— 不影响其它逻辑）。
        if root_pass:
            try:
                conn = connect(host, int(port), root_user, root_pass, timeout=5)
            except Exception:
                conn = None
        if conn is None:
            conn = connect(host, int(port), ha_user, ha_pass, timeout=5)
        if conn is None:
            return 0, "无法连接节点(root/ha_monitor 均失败)"
        cur = conn.cursor()
        flavour = detect_flavour(cur)
        if flavour == F_MARIADB:
            target = mariadb_matrix.ha_privs(mariadb_matrix.server_privileges(cur))
        else:
            target = ("REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
                      "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE, EXECUTE")
        target_set = {p.strip().upper() for p in target.split(",") if p.strip()}
        try:
            cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (ha_user,))
            _hosts = [r[0] for r in (cur.fetchall() or [])]
        except Exception:
            _hosts = []
        cand = set([h for h in _hosts if h] + ["%"]
                   + ([manager_ip] if manager_ip else []))
        _binlog_off = False
        try:
            cur.execute("SET SESSION sql_log_bin=0")
            _binlog_off = True
        except Exception:
            pass
        repaired = 0
        for h in cand:
            try:
                if target_set and target_set.issubset(_granted_privs(cur, ha_user, h)):
                    continue
                cur.execute("GRANT %s ON *.* TO '%s'@'%s'"
                            % (target, _esc_sql(ha_user), h))
                repaired += 1
                print("[ha-shadow] %s:%s 的 ha_monitor@%s 已补齐全集特权"
                      % (host, port, h), flush=True)
            except Exception as e:
                print("[ha-shadow] %s:%s ha_monitor@%s 授权失败(可忽略): %s"
                      % (host, port, h, e))
        if _binlog_off:
            try:
                cur.execute("SET SESSION sql_log_bin=1")
            except Exception:
                pass
        return repaired, ("已修复 %d 个影子账号" % repaired if repaired else "无需修复")
    except Exception as e:
        return 0, "异常: %s" % e
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass