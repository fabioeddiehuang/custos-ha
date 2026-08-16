"""后台监控循环：探活、更新拓扑、触发自动切换、维护拓扑对齐、状态告警（多集群）。"""
import concurrent.futures as _cf
import threading
import time
from .config import CHECK_INTERVAL, FAIL_THRESHOLD, PROBE_TIMEOUT
from .db import query, execute
from .ha_client import get_status, heal_replication, heal_io_gtid_gap
from .state import get_current_master
from .failover import do_failover, ensure_topology, load_creds, node_root
from .alerts import send_alert
from .clusters import list_clusters
from . import metrics, slow_log, config_drift

# 复制延迟告警阈值（秒）。超过即发邮件告警，避免重复刷屏只在「跨阈值的那一刻」触发。
REPL_LAG_ALERT_SEC = 30
# 各节点上一次观测状态（用于状态跃迁告警，避免每个周期重复刷屏）
_PREV = {}
# 各集群每个节点的连续探活失败计数（用于阈值触发自动切换）。
# 该属性挂在 run_once 函数对象上，必须在 def run_once 之后初始化（见下方），
# 否则 AttributeError 会被 run_forever 的 try 吞掉，导致 monitor 每个周期都在同一行
# 崩溃、永远不探活/不建复制/不漂 VIP（即「全离线」根因）。


def _update_host(h, st, is_master=False):
    """把节点探活结果写回 hosts；在线但复制线程中断时，记录真实复制错误便于排错。"""
    err = None
    if not st["alive"]:
        err = st.get("error") or "探活失败"
    elif st.get("slave_io") or st.get("slave_sql"):
        # 在线且有复制通道，但 IO/SQL 任一非 Yes：记录真实错误（而不是留空）
        if st.get("slave_io") != "Yes" or st.get("slave_sql") != "Yes":
            io_err = st.get("slave_io_err") or ""
            sql_err = st.get("slave_sql_err") or ""
            parts = []
            if st.get("slave_io") != "Yes" and io_err:
                parts.append("IO错误: " + io_err)
            if st.get("slave_sql") != "Yes" and sql_err:
                parts.append("SQL错误: " + sql_err)
            err = "；".join(parts) if parts else "复制线程异常(IO/SQL 非 Yes)"
    execute(
        """UPDATE hosts SET status=%s, role=%s, repl_delay=%s, slave_io=%s, slave_sql=%s,
           err=%s, last_seen=NOW() WHERE cluster_id=%s AND host=%s AND port=%s""",
        (("up" if st["alive"] else "down"), st["role"],
         st["seconds_behind"], st["slave_io"], st["slave_sql"],
         err,
         h["cluster_id"], h["host"], int(h["port"])),
    )


def _alert_on_transition(key, status, is_master, repl_ok, delay, cluster_name, cluster_id):
    """根据与上一次观测的差异发送告警，仅在状态跃迁时触发（避免刷屏）。"""
    prev = _PREV.get(key)
    _PREV[key] = dict(status=status, is_master=is_master, repl_ok=repl_ok, delay=delay)
    if prev is None:
        return  # 首次观测只建立基线，不告警
    host = f"{key[0]}:{key[1]}"
    tag = f"【{cluster_name}】" if cluster_name else ""
    try:
        # 1) 节点离线 / 恢复
        if prev["status"] != status:
            if status == "down":
                send_alert("节点离线", f"{tag}节点 {host} 已离线，监控连续探活失败。若为主库将触发自动切换。",
                           cluster_id=cluster_id)
            else:
                send_alert("节点恢复在线", f"{tag}节点 {host} 已恢复在线，若为旧主将自动重新加入集群为从库。",
                           cluster_id=cluster_id)
        # 2) 复制中断（仅非主库且节点在线时；节点离线属于「节点离线」告警，不重复报）
        if status == "up" and not is_master and prev["repl_ok"] and not repl_ok:
            send_alert("复制中断", f"{tag}节点 {host} 复制异常（Slave_IO 或 Slave_SQL 非 Yes），请检查复制链路。",
                       cluster_id=key[2])
        # 3) 复制延迟过高（仅在「跨过阈值」那一刻告警，且仅在线节点）
        if status == "up" and not is_master and delay is not None:
            crossed = delay >= REPL_LAG_ALERT_SEC and (
                prev["delay"] is None or prev["delay"] < REPL_LAG_ALERT_SEC)
            if crossed:
                send_alert("复制延迟过高", f"{tag}节点 {host} 复制延迟 {delay}s，已超过阈值 {REPL_LAG_ALERT_SEC}s。",
                           cluster_id=cluster_id)
    except Exception as e:
        print(f"[monitor] 发送告警失败(可忽略): {e}")


# 互斥锁：后台 monitor 线程与手动「刷新全部」(/api/refresh) 都调用 run_once，
# 二者并发会在节点层重复执行 make_slave_of / START SLAVE 等，可能引发复制抖动。
# 用非阻塞锁保证同一时刻只有一个 run_once 在执行（另一个直接跳过本轮）。
_RUN_ONCE_LOCK = threading.Lock()

# 拓扑维护（切换/自愈/对齐）的后台触发线程守卫：避免手动刷新频繁点击时堆积线程。
_ACT_THREAD = None
_ACT_THREAD_LOCK = threading.Lock()

# 最近一次探活结果缓存：{cid: {(host, port, cid): (st, is_master)}}
# 供 _act_once 复用，避免「探活」与「切换/自愈/拓扑」两次重复连库。
_LAST_STATUS = {}


def run_once():
    # 连续探活失败计数挂在函数对象上（每个集群独立）。延迟到此处初始化，
    # 因为模块加载时函数尚未定义，顶层执行 run_once._fails = {} 会 NameError。
    if not hasattr(run_once, "_fails"):
        run_once._fails = {}
    # ① 探活：无锁，立即把在线/离线状态写库，使仪表盘与手动「刷新全部」
    #    能实时反映设备上下线，而不必等待下方互斥锁（手动刷新不再被跳过）。
    try:
        _probe_all()
    except Exception as e:
        print(f"[monitor] _probe_all 异常(可忽略): {e}")
    # ② 切换/自愈/拓扑：加锁串行，避免与后台 monitor 或手动刷新并发执行
    #    make_slave_of / START SLAVE / RESET 等复制变更操作引发复制抖动。
    if not _RUN_ONCE_LOCK.acquire(blocking=False):
        return
    try:
        _act_once()
    finally:
        _RUN_ONCE_LOCK.release()


# run_once._fails 的模块级初始化（必须在 def run_once 之后，避免导入期 NameError）。
# 保证即使手动「刷新全部」(api_refresh) 在后台 monitor 首个周期之前就直接调用
# _probe_all()，也不会因 run_once._fails 未初始化而抛 AttributeError。
run_once._fails = {}


def request_act_once():
    """非阻塞触发一次拓扑维护（切换/自愈/对齐）。手动「刷新全部」后用于尽快收敛复制/VIP。

    加互斥锁，避免与后台 monitor 并发执行 make_slave_of / START SLAVE 等复制变更操作。
    若锁被占用（monitor 正在执行）则直接跳过——monitor 会在下一个周期补上，无需排队等待。
    """
    if not _RUN_ONCE_LOCK.acquire(blocking=False):
        return
    try:
        _act_once()
    finally:
        _RUN_ONCE_LOCK.release()


def trigger_act_once_async():
    """后台非阻塞触发一次拓扑维护（用于手动刷新后尽快收敛复制/VIP，不拖慢刷新响应）。

    用 _ACT_THREAD 守卫：若已有待执行的拓扑维护线程则跳过，避免频繁点击刷新堆积线程。
    """
    global _ACT_THREAD
    with _ACT_THREAD_LOCK:
        if _ACT_THREAD is not None and _ACT_THREAD.is_alive():
            return
        t = threading.Thread(target=request_act_once, name="act-once", daemon=True)
        t.start()
        _ACT_THREAD = t


def _probe_all():
    """仅探活 + 写回 hosts 状态（up/down/复制线程/延迟/错误），并缓存结果。

    并发探活：所有主机的 MySQL 探活在线程池中并行执行，总耗时≈单个超时
    （PROBE_TIMEOUT）而非 N×超时，因此「刷新全部」与后台 monitor 的探活阶段都很快。
    各探活线程独立建连（get_conn 每次新建连接，线程安全），写库各自独立行互不冲突。

    无锁：可被后台 monitor 线程与手动「刷新全部」(/api/refresh) 并发调用，
    节点一旦探活失败，状态立即写库（不等 FAIL_THRESHOLD 阈值），因此「某设备
    离线」能在首个探活周期即被看到；同时把结果存入 _LAST_STATUS 供 _act_once
    复用，避免切换/自愈阶段二次连库。
    """
    clusters = list_clusters()
    if not clusters:
        return  # 尚未创建任何集群，空闲等待
    # 收集所有探活任务（跨集群），统一并发执行
    tasks = []  # (cid, cluster_name, host, port, creds, cur_key)
    for cl in clusters:
        cid = cl["id"]
        cluster_name = cl.get("name") or f"集群{cid}"
        try:
            creds = load_creds(cid)
        except Exception as e:
            print(f"[monitor] 集群[{cluster_name}] 读取凭证失败: {e}")
            continue
        if not creds["ha_user"]:
            continue  # 该集群尚未配置受管账号，跳过
        cur_h, cur_p = get_current_master(cid)
        cur_key = (cur_h, int(cur_p)) if cur_h else None
        hosts = query(
            "SELECT host, port, cluster_id FROM hosts WHERE cluster_id=%s", (cid,))
        for h in hosts:
            # 兼容历史数据：hosts.port 可能为 NULL/非法，跳过避免 int() 崩溃
            try:
                port = int(h["port"])
            except (TypeError, ValueError):
                print(f"[monitor] 跳过无效主机 {h['host']}（port 缺失/非法：{h['port']!r}）")
                continue
            tasks.append((cid, cluster_name, h["host"], port, creds, cur_key))

    if not tasks:
        return

    def _probe_one(task):
        cid, cluster_name, host, port, creds, cur_key = task
        ru, rp = node_root(host, cid)
        try:
            st = get_status(host, port, creds["ha_user"], creds["ha_pass"], ru, rp, timeout=PROBE_TIMEOUT)
        except Exception as e:
            st = dict(alive=False, error=str(e))
        return (cid, cluster_name, host, port, cur_key, st)

    # 并发探活：线程池并行连各主机，总耗时取最慢单个（而非累加）
    cache_by_cid = {t[0]: {} for t in tasks}
    with _cf.ThreadPoolExecutor(max_workers=min(16, len(tasks))) as ex:
        for cid, cluster_name, host, port, cur_key, st in ex.map(_probe_one, tasks):
            key = (host, port, cid)
            is_master = (host, port) == cur_key
            _update_host({"host": host, "port": port, "cluster_id": cid}, st, is_master)
            # 连续失败计数（用于阈值触发自动切换）
            f = run_once._fails.setdefault(cid, {})
            if not st["alive"]:
                f[key] = f.get(key, 0) + 1
            else:
                f[key] = 0
            # 复制是否正常（主库无复制通道，视为主库自身即「正常」）
            repl_ok = is_master or (st.get("slave_io") == "Yes" and st.get("slave_sql") == "Yes")
            _alert_on_transition(key, "up" if st["alive"] else "down", is_master, repl_ok,
                                 st.get("seconds_behind"), cluster_name, cid)
            cache_by_cid[cid][key] = (st, is_master)
    # 仅覆盖已探活集群的缓存（未配置账号的集群保留上一次结果，避免误自愈）
    for cid, cache in cache_by_cid.items():
        _LAST_STATUS[cid] = cache


def _act_once():
    """切换/自愈/拓扑对齐：复用 _probe_all 的探活缓存，不做二次连库。

    必须串行（互斥锁保护），避免并发执行 make_slave_of / START SLAVE / RESET
    等复制变更操作引发复制抖动。
    """
    clusters = list_clusters()
    if not clusters:
        return
    for cl in clusters:
        cid = cl["id"]
        cluster_name = cl.get("name") or f"集群{cid}"
        try:
            creds = load_creds(cid)
        except Exception as e:
            print(f"[monitor] 集群[{cluster_name}] 读取凭证失败: {e}")
            continue
        if not creds["ha_user"]:
            continue
        cur_h, cur_p = get_current_master(cid)
        cache = _LAST_STATUS.get(cid, {})
        for key, (st, is_master) in list(cache.items()):
            if key[2] != cid or not st["alive"]:
                continue
            h_host, h_port = key[0], key[1]
            ru, rp = node_root(h_host, cid)
            # 复制 SQL 线程中断（多为幂等冲突导致的 1007/1062），尝试自愈跳过冲突事务
            if not is_master and st["slave_sql"] != "Yes":
                ok, msg = heal_replication(h_host, h_port, creds["ha_user"], creds["ha_pass"], ru, rp)
                if ok:
                    print(f"[monitor] 自愈 {h_host}:{h_port} 成功: {msg}")
                else:
                    print(f"[monitor] 自愈 {h_host}:{h_port} 失败: {msg}")
            # 复制 IO 线程中断（13114/1236：源端主库丢失 GTID，从库已执行过）
            # 需在【源端主库】为缺失 GTID 注入空事务，从库 IO 方可恢复
            if not is_master and st["slave_io"] != "Yes":
                sru, srp = node_root(h_host, cid)
                mru, mrp = (node_root(cur_h, cid) if cur_h else ("root", ""))
                ok_g, msg_g = heal_io_gtid_gap(
                    h_host, h_port, creds["ha_user"], creds["ha_pass"],
                    sru, srp, cur_h, int(cur_p) if cur_p else None, mru, mrp)
                if ok_g:
                    print(f"[monitor] IO 自愈 {h_host}:{h_port}: {msg_g}")
                elif "more GTIDs" in msg_g or "13114" in msg_g or "1236" in msg_g:
                    print(f"[monitor] IO 自愈 {h_host}:{h_port} 失败: {msg_g}")
        # 当前主库连续失败达到阈值 -> 自动切换
        fails = run_once._fails.setdefault(cid, {})
        for key, (st, is_master) in list(cache.items()):
            if key[2] != cid:
                continue
            if is_master and fails.get(key, 0) >= FAIL_THRESHOLD:
                print(f"[monitor] 集群[{cluster_name}] 主库 {cur_h}:{cur_p} "
                      f"连续 {fails[key]} 次探活失败，触发自动切换")
                if do_failover(cid, "auto: master unreachable"):
                    run_once._fails[cid] = {}  # 重置，避免重复触发
                break
        # 维护拓扑：切后对齐 + 旧主恢复重入
        try:
            ensure_topology(cid)
        except Exception as e:
            print(f"[monitor] ensure_topology 集群[{cluster_name}] 错误(可忽略): {e}")


_MONITOR_THREAD = None


def run_forever():
    print("[monitor] 监控线程启动")
    while True:
        try:
            run_once()
        except Exception as e:
            # 管理库不可用等：降级，仅记录，不影响业务(keepalived 有安全回退)
            print(f"[monitor] 周期错误(已降级继续): {e}")
        try:
            _maybe_collect()
        except Exception as e:
            print(f"[monitor] 周期采集异常(已降级继续): {e}")
        time.sleep(CHECK_INTERVAL)


# 周期计数：用于错峰触发采集任务（避免每周期都重连库写指标）。
_COLLECT_CYCLE = {"metrics": 0, "slow": 0, "drift": 0}
# 采集周期倍数（相对于 CHECK_INTERVAL）：指标采样较频繁，慢日志/漂移较稀疏。
_METRICS_EVERY = 3      # 约 CHECK_INTERVAL*3（默认 9s）采样一次指标
_SLOW_EVERY = 20        # 约默认 60s 采集一次慢日志
_DRIFT_EVERY = 20       # 约默认 60s 巡检一次配置漂移


def _maybe_collect():
    """在后台 monitor 周期内错峰触发指标/慢日志/配置漂移采集（仅写入，无副作用）。"""
    _COLLECT_CYCLE["metrics"] += 1
    _COLLECT_CYCLE["slow"] += 1
    _COLLECT_CYCLE["drift"] += 1
    if _COLLECT_CYCLE["metrics"] >= _METRICS_EVERY:
        _COLLECT_CYCLE["metrics"] = 0
        try:
            metrics.collect_metrics()
        except Exception as e:
            print(f"[monitor] 指标采集异常: {e}", flush=True)
            import traceback; traceback.print_exc()
    if _COLLECT_CYCLE["slow"] >= _SLOW_EVERY:
        _COLLECT_CYCLE["slow"] = 0
        try:
            slow_log.collect_slow_log()
        except Exception as e:
            print(f"[monitor] 慢日志采集异常: {e}", flush=True)
            import traceback; traceback.print_exc()
    if _COLLECT_CYCLE["drift"] >= _DRIFT_EVERY:
        _COLLECT_CYCLE["drift"] = 0
        try:
            for cl in list_clusters():
                config_drift.check_cluster(cl["id"])
        except Exception as e:
            print(f"[monitor] 配置漂移巡检异常: {e}", flush=True)
            import traceback; traceback.print_exc()


def start_in_background():
    """随 Web 进程内常驻的后台监控线程：同一进程只启动一次（daemon，随 Web 退出）。

    目的：彻底避免「节点起来了但 monitor 没单独起」导致的全离线/无复制/VIP 不漂。
    由 uvicorn 加载 app.api 时自动拉起（见 app/api.py），无需再 `python -m app.monitor &`。
    """
    global _MONITOR_THREAD
    if _MONITOR_THREAD is not None and _MONITOR_THREAD.is_alive():
        return _MONITOR_THREAD
    t = threading.Thread(target=run_forever, name="monitor", daemon=True)
    t.daemon = True
    t.start()
    _MONITOR_THREAD = t
    print("[monitor] 已在 Web 进程内启动后台监控线程")
    return t


if __name__ == "__main__":
    run_forever()
