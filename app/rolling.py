"""rolling.py — 滚动升级向导 + 节点修复分档（v2.5.0 F8）。

解决什么痛点
------------
升级节点时要「更新从库 → 切主 → 更新旧主 → 再切 → 更新下一个」，
运维不仅不知道何时能切（F7 已解决），还要手工串起整条流程。本模块把它固化成一条龙。

节点修复分三档
--------------
  L1 重启服务       —— 最轻，agent 的 restart_service
  L2 重建复制       —— 现有 rebuild.start_rebuild（从主库全量重新导入 + GTID 对齐）
  L3 纯净重建       —— 停服务 → 清空数据目录 → 重新初始化 → 全量导入 → 建复制

L3 的两条铁律
-------------
  1. **绝不对当前主库执行**：清空主库 = 整个集群数据全丢。必须集群另有存活主库。
  2. 数据目录路径必须落在数据库数据目录范围内（agent 侧二次校验），
     绝不接触系统路径。

数据源（与 F9 联动）
--------------------
L3 默认不重新做备份，而是用「最近一次已有备份 + binlog 增量」；
若没有可用备份或增量过大，回退「从主库全量 dump」（复用现有 rebuild 通道）。
"""
import logging
import threading
import time
import uuid

from .db import query
from .state import get_current_master
from . import agent_hub, syncgate

log = logging.getLogger(__name__)

A_RESTART = "restart"
A_REBUILD = "rebuild"
A_PURE = "pure"
ACTIONS = (A_RESTART, A_REBUILD, A_PURE)

# 单个节点动作的最长等待（秒）
NODE_ACTION_TIMEOUT = 1800
SYNC_WAIT_TIMEOUT = 900

_lock = threading.Lock()
_TASKS = {}


def _mk(tid, **kw):
    with _lock:
        t = _TASKS.setdefault(tid, {
            "id": tid, "status": "running", "stage": "", "progress": 0,
            "steps": [], "started_at": time.time(), "finished_at": None,
            "cluster_id": None, "error": "",
        })
        t.update(kw)
        return dict(t)


def get_task(tid):
    with _lock:
        t = _TASKS.get(tid or "")
        return dict(t) if t else {"id": tid, "status": "unknown"}


def _cancelled(tid):
    with _lock:
        return bool((_TASKS.get(tid) or {}).get("_cancel"))


def _log_step(tid, msg):
    with _lock:
        t = _TASKS.get(tid)
        if t is not None:
            t["steps"].append(msg)


def _nodes(cluster_id):
    rows = query("SELECT ip, port FROM nodes WHERE cluster_id=%s ORDER BY ip",
                 (cluster_id,))
    return [(r["ip"], int(r["port"])) for r in (rows or [])]


def _wait_cmd(cmd_id, timeout=600):
    """等待一条 agent 指令完成。"""
    from .db import query as _q
    end = time.time() + timeout
    while time.time() < end:
        rows = _q("SELECT status, result FROM agent_commands WHERE id=%s", (int(cmd_id),))
        st = (rows[0] or {}).get("status") if rows else None
        if st in ("done", "failed", "expired"):
            return st == "done", ((rows[0] or {}).get("result") or "")
        time.sleep(3)
    return False, "等待指令超时"


def _wait_rebuild(task_id_rb, timeout=NODE_ACTION_TIMEOUT):
    from . import rebuild
    end = time.time() + timeout
    while time.time() < end:
        t = rebuild.get_task(task_id_rb) or {}
        st = t.get("status")
        if st in ("done", "failed", "error"):
            return st == "done", str(t.get("msg") or t.get("error") or "")
        time.sleep(5)
    return False, "重建超时"


def _apply_action(tid, cluster_id, ip, port, action, actor):
    """对单个节点执行修复动作。返回 (ok, msg)。"""
    if action == A_RESTART:
        cid = agent_hub.enqueue(ip, "restart_service", {"service": "mariadb"},
                                actor=actor, ttl_sec=1800,
                                dedupe_key="restart_%s" % ip)
        ok, msg = _wait_cmd(cid)
        return ok, ("重启完成" if ok else "重启失败：%s" % msg)
    if action in (A_REBUILD, A_PURE):
        if action == A_PURE:
            ok_p, msg_p = _pure_rebuild(cluster_id, ip, port, actor)
            if not ok_p:
                return False, "纯净重建失败：%s" % msg_p
            _log_step(tid, "%s 数据目录已清空并重新初始化" % ip)
        from . import rebuild
        try:
            # start_rebuild 后台启动重建并**直接返回 task_id 字符串**（非字典）。
            task_id_rb = rebuild.start_rebuild(cluster_id, ip, port, actor=actor)
            ok, msg = _wait_rebuild(task_id_rb)
            return ok, ("重建完成" if ok else "重建失败：%s" % msg)
        except Exception as e:
            return False, "重建异常：%s" % e
    return False, "未知动作：%s" % action


def _pure_rebuild(cluster_id, ip, port, actor):
    """L3 纯净重建：停服务 → 清数据目录 → 起服务（数据随后由 rebuild 从主库导入）。

    铁律：绝不对当前主库执行。
    """
    mh, mp = get_current_master(cluster_id)
    if ip == mh and int(port) == int(mp or 0):
        return False, "不能对当前主库执行纯净重建（会抹掉整个集群数据）"
    rows = query("SELECT node_type FROM nodes WHERE ip=%s", (ip,))
    ntype = ((rows[0] or {}).get("node_type") if rows else "") or "mariadb"
    # 容器形态：数据目录在卷内；裸机形态：系统数据目录。
    # 只清理「数据库数据目录」这一种目标（agent 侧还会二次校验路径形态）。
    data_dir = "/var/lib/mysql"
    if ntype in ("mariadb-docker", "docker"):
        data_dir = "/var/lib/mysql"     # 容器内路径，由 agent 在宿主卷上执行
    c1 = agent_hub.enqueue(ip, "stop_service", {"service": "mariadb"},
                           actor=actor, ttl_sec=900, dedupe_key="stop_%s" % ip)
    ok, msg = _wait_cmd(c1, timeout=300)
    if not ok:
        return False, "停止数据库服务失败：%s" % msg
    c2 = agent_hub.enqueue(ip, "wipe_data", {"target": "mariadb_data", "path": data_dir},
                           actor=actor, ttl_sec=900, dedupe_key="wipe_%s" % ip)
    ok, msg = _wait_cmd(c2, timeout=600)
    if not ok:
        return False, "清空数据目录失败：%s" % msg
    c3 = agent_hub.enqueue(ip, "start_service", {"service": "mariadb"},
                           actor=actor, ttl_sec=900, dedupe_key="start_%s" % ip)
    ok, msg = _wait_cmd(c3, timeout=300)
    if not ok:
        return False, "启动数据库服务失败：%s" % msg
    return True, "ok"


def start_upgrade(cluster_id, actions, new_master="", actor="",
                  do_switch=True):
    """启动滚动升级。

    actions: {ip: "restart"|"rebuild"|"pure"}（不含的跳过）
    new_master: 切换到哪个节点（空=第一个处理完的从库）
    """
    tid = uuid.uuid4().hex[:12]
    _mk(tid, cluster_id=cluster_id, actor=actor, stage="初始化", progress=2)
    threading.Thread(target=_run, args=(tid, int(cluster_id), dict(actions or {}),
                                        new_master or "", actor, bool(do_switch)),
                     daemon=True).start()
    return {"task_id": tid}


def _run(tid, cluster_id, actions, new_master, actor, do_switch):
    try:
        mh, mp = get_current_master(cluster_id)
        if not mh:
            _mk(tid, status="failed", error="取不到当前主库", progress=100,
                finished_at=time.time())
            return
        nodes = _nodes(cluster_id)
        slaves = [(h, p) for h, p in nodes if not (h == mh and p == int(mp or 0))]
        # 顺序：先所有从库，最后主库（主库留到切主之后处理）
        order = [(h, p, actions.get(h, A_RESTART)) for h, p in slaves]

        total = len(order) + (1 if do_switch else 0) + 1
        done = 0

        # 1) 逐个处理从库
        for h, p, act in order:
            if _cancelled(tid):
                _mk(tid, status="cancelled", progress=100, finished_at=time.time())
                return
            _mk(tid, stage="处理从库 %s（%s）" % (h, act),
                progress=int(90 * done / max(1, total)))
            ok, msg = _apply_action(tid, cluster_id, h, p, act, actor)
            _log_step(tid, "%s：%s" % (h, msg))
            if not ok:
                _mk(tid, status="failed", error="节点 %s 失败：%s" % (h, msg),
                    progress=100, finished_at=time.time())
                return
            done += 1

        # 2) 等从库追平 + 强制同步（拿到客观就绪判据再切主）
        _mk(tid, stage="等待从库追平", progress=60)
        if not _wait_ready(tid, cluster_id, SYNC_WAIT_TIMEOUT):
            _mk(tid, status="failed", error="等待追平超时，已中止（未切主）",
                progress=100, finished_at=time.time())
            return

        if do_switch:
            target = new_master or (slaves[0][0] if slaves else "")
            if not target:
                _mk(tid, status="failed", error="没有可切换的目标节点",
                    progress=100, finished_at=time.time())
                return
            _mk(tid, stage="强制同步（停写等追平）", progress=70)
            r = syncgate.start_forced_sync(cluster_id)
            fok = _wait_sync(r.get("task_id"), 200)
            if not fok:
                _mk(tid, status="failed",
                    error="强制同步未完成，已中止切换（主库写入已由强制同步自动恢复）",
                    progress=100, finished_at=time.time())
                return
            _mk(tid, stage="切换主库到 %s" % target, progress=80)
            try:
                from .failover import do_failover_to
                do_failover_to(cluster_id, target,
                               int(dict((h, p) for h, p in nodes).get(target, mp or 0)),
                               actor=actor)
            except Exception as e:
                _mk(tid, status="failed", error="切换失败：%s" % e,
                    progress=100, finished_at=time.time())
                return
            _log_step(tid, "已切换主库到 %s" % target)
            done += 1

            # 3) 处理原主库（现在已是从库）
            _mk(tid, stage="处理原主库 %s" % mh, progress=88)
            ok, msg = _apply_action(tid, cluster_id, mh, int(mp or 0),
                                    actions.get(mh, A_RESTART), actor)
            _log_step(tid, "%s：%s" % (mh, msg))
            done += 1
            if not ok:
                _mk(tid, status="failed", error="原主库处理失败：%s" % msg,
                    progress=100, finished_at=time.time())
                return

        _mk(tid, status="done", stage="滚动升级完成", progress=100,
            finished_at=time.time())
    except Exception as e:
        log.exception("滚动升级失败")
        _mk(tid, status="failed", error=str(e), progress=100,
            finished_at=time.time())


def _wait_ready(tid, cluster_id, timeout):
    end = time.time() + timeout
    while time.time() < end:
        if _cancelled(tid):
            return False
        try:
            r = syncgate.readiness(cluster_id)
            if r.get("ready"):
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def _wait_sync(task_id, timeout):
    end = time.time() + timeout
    while time.time() < end:
        t = syncgate.get_task(task_id) or {}
        if t.get("status") in ("ready", "done", "timeout", "failed", "cancelled"):
            return t.get("status") in ("ready", "done")
        time.sleep(2)
    return False


def cancel(tid):
    with _lock:
        t = _TASKS.get(tid)
        if not t:
            return {"ok": False, "error": "任务不存在"}
        t["_cancel"] = True
    return {"ok": True}
