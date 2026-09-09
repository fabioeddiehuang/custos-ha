"""rebuild.py — 从库在线重建（重做数据 + 重链复制）。

适用场景
--------
从库数据与主库发生**真实冲突**（1062 主键冲突 / 1032 找不到行）时，
自愈（注入空事务跳过）无法修复，传统做法只能「停机 + 手工从主库重建」。
本模块基于项目已有的 backup（dump）与 restore 能力在线完成，
不引入 xtrabackup / mydumper 等外部工具依赖。

流程
----
1. 校验：目标必须是集群内**从库**（非当前主库），且集群有明确主库
2. 目标从库 STOP SLAVE，断开复制
3. 对主库执行 dump（backup.run_backup）
4. 恢复到目标从库（restore_backup，关闭会话 binlog 避免污染 GTID）
5. make_slave_of 重新挂载（GTID 自动定位；分叉由其内部判断处理）
6. 回查复制线程状态

安全约束（改动前务必读）
----------------------
- **绝不 RESET MASTER**：make_slave_of 依赖 GTID 自动定位跳过已执行事务，
  擅自清空本机 GTID 会让主库重放同名事务，引发 1007/1062 使 SQL 线程停摆。
- 重建目标禁止是当前主库 —— 那等于用主库数据覆盖主库本身，会抹掉整个集群数据。
- 恢复必须 SET sql_log_bin=0：否则从库 binlog 产生新 GTID 集合，与主库历史
  GTID 冲突，重链时被判分叉并触发 RESET MASTER（见 backup.restore_backup）。
"""
import threading
import time
import uuid

import pymysql
from pymysql.cursors import DictCursor

from .db import query
from .state import get_current_master
from .clusters import get_cluster_creds
from .ha_client import connect_robust, make_slave_of, is_gtid_purged_error
from . import backup, audit

# 任务表（进程内内存态；管理器为单实例，无需跨进程共享）
_TASKS = {}
_LOCK = threading.Lock()
_MAX_TASKS = 50


def _set(task_id, **kw):
    with _LOCK:
        t = _TASKS.get(task_id)
        if t is not None:
            t.update(kw)
            t["updated"] = time.time()


def get_task(task_id):
    with _LOCK:
        t = _TASKS.get(task_id)
        return dict(t) if t else {}


def _prune():
    """只保留最近的任务，避免长期运行的内存增长。"""
    with _LOCK:
        if len(_TASKS) <= _MAX_TASKS:
            return
        for k in sorted(_TASKS, key=lambda x: _TASKS[x].get("started") or 0)[:-_MAX_TASKS]:
            _TASKS.pop(k, None)


def _exec(ip, port, user, pwd, sql):
    conn = connect_robust(ip, port, user, pwd)
    if not conn:
        return False, "无法连接节点"
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        try:
            conn.commit()
        except Exception:
            pass
        return True, ""
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _repl_state(ip, port, user, pwd):
    """回查复制线程状态，返回 (io, sql, delay)。"""
    conn = connect_robust(ip, port, user, pwd)
    if not conn:
        return None, None, None
    try:
        with conn.cursor(DictCursor) as cur:
            cur.execute("SHOW SLAVE STATUS")
            row = cur.fetchone() or {}
        io = row.get("Slave_IO_Running") or row.get("Replica_IO_Running")
        sqlt = row.get("Slave_SQL_Running") or row.get("Replica_SQL_Running")
        d = row.get("Seconds_Behind_Master")
        if d is None:
            d = row.get("Seconds_Behind_Source")
        return io, sqlt, d
    except Exception:
        return None, None, None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _gtid_state(ip, port, user, pwd):
    """读取节点 GTID 位点：MariaDB 取 gtid_binlog_pos，MySQL 8 取 gtid_executed。

    返回 (flavour, gtid)，flavour ∈ {"mariadb", "mysql", ""}（空串=读取失败）。
    """
    conn = connect_robust(ip, port, user, pwd)
    if not conn:
        return "", ""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            ver = str((cur.fetchone() or ("",))[0] or "").lower()
            if "mariadb" in ver:
                cur.execute("SELECT @@gtid_binlog_pos")
                return "mariadb", str((cur.fetchone() or ("",))[0] or "")
            cur.execute("SELECT @@gtid_executed")
            return "mysql", str((cur.fetchone() or ("",))[0] or "")
    except Exception as e:
        print(f"[rebuild] 读取 {ip}:{port} GTID 位点失败(可忽略): {e}")
        return "", ""
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _align_gtid_after_restore(slave_host, slave_port, sru, srp,
                              master_host, master_port, mru, mrp):
    """重建后把从库 GTID 位点对齐到「主库导出结束时」的位置。返回说明文本。

    必要性（本次修复的根因）：
      逻辑重建 = 主库 dump + 从库 restore（且 SET sql_log_bin=0），**不会写入从库
      GTID 状态**。重建完的从库 gtid_slave_pos / gtid_executed 为空，以 GTID 自动
      定位（MASTER_USE_GTID=slave_pos / MASTER_AUTO_POSITION=1）连主库时，从库相当
      于从「时间起点」请求，而主库早期 binlog 多半已清理 → 永久报
        1236 'Could not find GTID state requested by slave in any binlog files.
             Probably the slave state is too old and required binlog files have been purged.'
      表现为「重建任务显示成功，但复制还是断的，仪表盘一直显示『需重建』」。
    处理：数据既已从主库全量恢复，就把位点设为主库导出结束时的位置，从该点续传。
    注意：dump 期间提交的事务可能未全部进入 dump（逻辑备份无一致性快照），这部分会
    被跳过；故重建完成后建议跑一次「一致性校验」确认。
    """
    flavour, m_gtid = _gtid_state(master_host, master_port, mru or "root", mrp or "")
    if not m_gtid:
        return "无法读取主库 GTID 位点，跳过位点对齐（若复制仍报 1236 需人工处理）"
    conn = connect_robust(slave_host, slave_port, sru, srp)
    if not conn:
        return "无法连接从库，跳过位点对齐"
    try:
        with conn.cursor() as cur:
            if flavour == "mariadb":
                cur.execute("SELECT @@gtid_slave_pos")
                s_gtid = str((cur.fetchone() or ("",))[0] or "")
                if s_gtid:
                    cur.execute("SELECT GTID_SUBSET(%s, %s)", (s_gtid, m_gtid))
                    if (cur.fetchone() or (0,))[0]:
                        return f"从库位点已是主库子集，无需对齐（{s_gtid}）"
                cur.execute("SET GLOBAL gtid_slave_pos=%s", (m_gtid,))
                return f"已对齐 gtid_slave_pos → 主库导出结束位点 {m_gtid}"
            cur.execute("SELECT @@gtid_executed")
            s_gtid = str((cur.fetchone() or ("",))[0] or "")
            if s_gtid:
                return f"从库已有 GTID 执行集，跳过对齐（{s_gtid}）"
            cur.execute("SET GLOBAL gtid_purged=%s", (m_gtid,))
            return f"已对齐 gtid_purged → 主库导出结束位点 {m_gtid}"
    except Exception as e:
        return f"GTID 位点对齐失败(可忽略)：{e}"
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _repl_io_err(ip, port, user, pwd):
    """读取从库 Last_IO_Error（用于重建后判断是否真的链上了）。"""
    conn = connect_robust(ip, port, user, pwd)
    if not conn:
        return ""
    try:
        with conn.cursor(DictCursor) as cur:
            cur.execute("SHOW SLAVE STATUS")
            row = cur.fetchone() or {}
        return str(row.get("Last_IO_Error") or "")
    except Exception:
        return ""
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _master_binlog_coords(ip, port, user, pwd):
    """读取主库当前 binlog 文件与位点（GTID 定位失败时的兜底坐标）。

    MariaDB 12 起 `SHOW MASTER STATUS` 改名为 `SHOW BINLOG STATUS`，故两种都试。
    """
    conn = connect_robust(ip, port, user, pwd)
    if not conn:
        return "", 0
    try:
        with conn.cursor() as cur:
            for sql in ("SHOW BINLOG STATUS", "SHOW MASTER STATUS"):
                try:
                    cur.execute(sql)
                    row = cur.fetchone() or ()
                    if row and row[0]:
                        return str(row[0]), int(row[1] or 0)
                except Exception:
                    continue
        return "", 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _node_root(cluster_id, host):
    rows = query("SELECT root_user, root_pass FROM nodes WHERE cluster_id=%s AND ip=%s",
                 (cluster_id, host))
    if rows:
        return (rows[0].get("root_user") or "root"), (rows[0].get("root_pass") or "")
    return "root", ""


def _run(task_id, cluster_id, host, port):
    def step(pct, msg):
        _set(task_id, pct=pct, msg=msg)

    try:
        step(2, "校验目标节点…")
        rows = query("SELECT is_current_master FROM hosts "
                     "WHERE cluster_id=%s AND host=%s AND port=%s",
                     (cluster_id, host, int(port)))
        if not rows:
            return False, f"节点 {host}:{port} 不在该集群内"
        if int(rows[0].get("is_current_master") or 0) == 1:
            return False, "重建目标不能是当前主库（会抹掉整个集群的数据）"

        m_host, m_port = get_current_master(cluster_id)
        m_port = int(m_port or 3306)
        if not m_host or (m_host == host and m_port == int(port)):
            return False, "集群当前主库未知或与目标相同，无法重建"

        creds = get_cluster_creds(cluster_id)
        if not creds:
            return False, "集群凭证不存在"
        root_user, root_pass = _node_root(cluster_id, host)

        step(6, "停止从库复制…")
        _exec(host, port, root_user, root_pass, "STOP SLAVE")

        step(10, f"从主库 {m_host}:{m_port} 导出数据…")
        r = backup.run_backup(cluster_id, m_host, m_port, with_data=True, scope="all")
        if not r.get("ok"):
            return False, f"主库导出失败：{r.get('error') or '未知错误'}"
        fname = r.get("file")

        step(45, "恢复数据到目标从库（关闭会话 binlog）…")
        r2 = backup.restore_backup(cluster_id, host, port, fname, no_binlog=True)
        if not r2.get("ok"):
            err0 = (r2.get("errors") or [""])[0]
            return False, f"数据恢复失败：{r2.get('msg') or r2.get('error')} {err0}"

        step(80, "对齐 GTID 复制位点…")
        mru, mrp = _node_root(cluster_id, m_host)
        flavour, _mgtid = _gtid_state(m_host, m_port, mru, mrp)
        gt_msg = _align_gtid_after_restore(host, port, root_user, root_pass,
                                           m_host, m_port, mru, mrp)
        print(f"[rebuild] {host}:{port} 主库 {m_host}:{m_port} GTID={_mgtid or '未知'}；{gt_msg}")

        step(85, "重新建立复制链路…")
        ok, msg = make_slave_of(host, port, root_user, root_pass,
                                m_host, m_port,
                                creds.get("repl_user"), creds.get("repl_pass"),
                                root_user=root_user, root_pass=root_pass)
        if not ok:
            return False, f"重链复制失败：{msg}"

        step(92, "校验复制状态…")
        time.sleep(3)
        io, sq, d = _repl_state(host, port, root_user, root_pass)
        io_err = _repl_io_err(host, port, root_user, root_pass)

        # ---- 兜底：GTID 自动定位仍报「主库 binlog 已清理」(1236) 时改按【文件位点】重链 ----
        # GTID 定位要求主库能找到从库所请求位点对应的 binlog；主库早期 binlog 一旦被
        # 清理（或位点集合对不上），无论怎么对齐都会反复 1236。此时数据本就来自该主库
        # 的全量导出，直接用主库【当前 binlog 文件+位点】做坐标续传即可，不再依赖历史。
        if io != "Yes" and is_gtid_purged_error(io_err):
            # ① 先用【主库当前 GTID 位点】重新对齐再拉起 IO 线程。
            #    位点可能被 make_slave_of 的分叉处理（RESET MASTER）冲掉，或导出期间
            #    主库又切换过，故这里重新取一次主库位点。
            _flv2, _mgtid2 = _gtid_state(m_host, m_port, mru, mrp)
            if _mgtid2 and flavour == "mariadb":
                _exec(host, port, root_user, root_pass, "STOP SLAVE")
                _ok_a, _err_a = _exec(host, port, root_user, root_pass,
                                      f"SET GLOBAL gtid_slave_pos='{_mgtid2}'")
                _exec(host, port, root_user, root_pass, "START SLAVE")
                time.sleep(4)
                io, sq, d = _repl_state(host, port, root_user, root_pass)
                io_err = _repl_io_err(host, port, root_user, root_pass)
                if io == "Yes":
                    gt_msg += f"；已按主库当前 GTID 位点 {_mgtid2} 重新对齐后恢复"
                elif not _ok_a:
                    print(f"[rebuild] {host}:{port} 重新对齐 GTID 失败：{_err_a}")

            # ② 仍不通 -> 改按【主库当前 binlog 文件+位点】重链（绕开 GTID 历史）
            mfile, mpos = _master_binlog_coords(m_host, m_port, mru, mrp)
            print(f"[rebuild] {host}:{port} GTID 定位失败（{io_err[:160]}），"
                  f"改用文件位点 {mfile}:{mpos} 兜底重链")
            if mfile and mpos:
                _rp = str(creds.get("repl_pass") or "").replace("\\", "\\\\").replace("'", "\\'")
                if flavour == "mariadb":
                    gtid_opt = "MASTER_USE_GTID=no"
                else:
                    gtid_opt = "MASTER_AUTO_POSITION=0"
                _ok_c, _err_c = _exec(
                    host, port, root_user, root_pass,
                    f"STOP SLAVE;")
                _ok_c, _err_c = _exec(
                    host, port, root_user, root_pass,
                    f"CHANGE MASTER TO MASTER_HOST='{m_host}', MASTER_PORT={int(m_port)}, "
                    f"MASTER_USER='{creds.get('repl_user')}', MASTER_PASSWORD='{_rp}', "
                    f"MASTER_LOG_FILE='{mfile}', MASTER_LOG_POS={int(mpos)}, {gtid_opt}")
                if _ok_c:
                    _exec(host, port, root_user, root_pass, "START SLAVE")
                    time.sleep(5)
                    io, sq, d = _repl_state(host, port, root_user, root_pass)
                    io_err = _repl_io_err(host, port, root_user, root_pass)
                    if io == "Yes":
                        gt_msg += f"；GTID 定位失败，已按主库文件位点 {mfile}:{mpos} 兜底重链成功"
                else:
                    print(f"[rebuild] {host}:{port} 文件位点重链失败：{_err_c}")

        # 复制没起来就如实报失败：旧实现一律返回成功，导致「数据恢复了但复制是断的」，
        # 界面却显示「重建完成」，问题被掩盖。
        if io != "Yes" or sq != "Yes":
            return False, (f"已从主库 {m_host}:{m_port} 恢复数据到 {host}:{port}，"
                           f"但复制未起来：IO={io} SQL={sq}；"
                           f"错误：{(io_err or '无')[:300]}；{gt_msg}")
        step(100, "重建完成")
        return True, (f"已从主库 {m_host}:{m_port} 重建 {host}:{port}；"
                      f"复制线程 IO={io} SQL={sq} 延迟={d}；{gt_msg}。"
                      f"（逻辑备份无一致性快照，导出窗口内提交的事务可能未包含，建议重建后执行一次一致性校验）")
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# (cluster_id, host, port) -> 最近一次**成功**重建的 time.time()
_RECENT_OK = {}


def last_rebuilt_at(cluster_id, host, port):
    """该节点最近一次成功重建的时间戳（无则 None）。

    供 monitor 在重建完成后立即解除该节点的自愈退避 / 卡死计数 / 需重建标记，
    避免出现「重建其实已完成，但页面仍显示旧状态」的错觉。
    """
    try:
        return _RECENT_OK.get((int(cluster_id), host, int(port)))
    except (TypeError, ValueError):
        return None


def _worker(task_id, cluster_id, host, port):
    ok, msg = _run(task_id, cluster_id, host, port)
    if ok:
        try:
            _RECENT_OK[(int(cluster_id), host, int(port))] = time.time()
        except (TypeError, ValueError):
            pass
    _set(task_id, status="done" if ok else "failed", msg=msg,
         pct=100 if ok else None, finished=time.time())


def start_rebuild(cluster_id, host, port, actor=""):
    """后台线程启动重建，立即返回 task_id（大库耗时长，不能阻塞请求）。"""
    task_id = uuid.uuid4().hex[:12]
    with _LOCK:
        _TASKS[task_id] = {
            "id": task_id, "cluster_id": int(cluster_id), "host": host,
            "port": int(port), "status": "running", "pct": 0, "msg": "已排队",
            "started": time.time(), "updated": time.time(),
        }
    _prune()
    try:
        audit.add_audit(actor or "-", "rebuild_slave", f"{host}:{port}",
                        "从库在线重建", cluster_id=int(cluster_id))
    except Exception:
        pass
    threading.Thread(target=_worker, args=(task_id, int(cluster_id), host, int(port)),
                     daemon=True).start()
    return task_id
