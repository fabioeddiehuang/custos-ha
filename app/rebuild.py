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
from .ha_client import connect_robust, make_slave_of
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

        step(85, "重新建立复制链路…")
        ok, msg = make_slave_of(host, port, root_user, root_pass,
                                m_host, m_port,
                                creds.get("repl_user"), creds.get("repl_pass"),
                                root_user=root_user, root_pass=root_pass)
        if not ok:
            return False, f"重链复制失败：{msg}"

        step(96, "校验复制状态…")
        io, sq, d = _repl_state(host, port, root_user, root_pass)
        step(100, "重建完成")
        return True, (f"已从主库 {m_host}:{m_port} 重建 {host}:{port}；"
                      f"复制线程 IO={io} SQL={sq} 延迟={d}")
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _worker(task_id, cluster_id, host, port):
    ok, msg = _run(task_id, cluster_id, host, port)
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
