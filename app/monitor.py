"""后台监控循环：探活、更新拓扑、触发自动切换、维护拓扑对齐、状态告警（多集群）。"""
import concurrent.futures as _cf
import threading
import time
from .config import (CHECK_INTERVAL, FAIL_THRESHOLD, PROBE_TIMEOUT, SLOWLOG_ENABLED,
                     HEARTBEAT_INTERVAL, METRICS_INTERVAL, DRIFT_INTERVAL)
from .db import query, execute, get_setting
from .ha_client import get_status, heal_replication, heal_io_gtid_gap
from .state import get_current_master
from .failover import (do_failover, do_failover_to, ensure_topology, load_creds,
                      node_root, _repoint_slave, promote_to_master)
from .alerts import send_alert
from .clusters import list_clusters
from . import node_gen
from . import metrics, slow_log, config_drift

# 复制延迟告警阈值（秒）。超过即发邮件告警，避免重复刷屏只在「跨阈值的那一刻」触发。
REPL_LAG_ALERT_SEC = 30
# 各节点上一次观测状态（用于状态跃迁告警，避免每个周期重复刷屏）
_PREV = {}
# 各集群每个节点的连续探活失败计数（用于阈值触发自动切换）。
# 该属性挂在 run_once 函数对象上，必须在 def run_once 之后初始化（见下方），
# 否则 AttributeError 会被 run_forever 的 try 吞掉，导致 monitor 每个周期都在同一行
# 崩溃、永远不探活/不建复制/不漂 VIP（即「全离线」根因）。


# 每个节点心跳(last_seen)上一次落库的 monotonic 时刻，用于 HEARTBEAT_INTERVAL 限频
_HEARTBEAT_AT = {}


def _same(a, b):
    """归一化后比对：None 与空串视为等价，其余统一按字符串比较。"""
    return ("" if a is None else str(a)) == ("" if b is None else str(b))


def _update_host(h, st, is_master=False):
    """把节点探活结果写回 hosts；在线但复制线程中断时，记录真实复制错误便于排错。

    【空闲零写入】稳态（探活结果与库中已有记录完全一致）时**一次写都不做**。
    旧实现每周期对每个节点无条件 UPDATE，且含 `last_seen=NOW()` —— 该字段每次必变，
    导致稳态下每个节点每周期都产生一次真实行更新 + commit；管理库若开了
    sync_binlog=1，就直接表现为持续的磁盘写入。
    last_seen 改为按 HEARTBEAT_INTERVAL 限频（设为 0 则完全不写）。
    """
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

    status = "up" if st["alive"] else "down"
    role = st["role"]
    delay = st["seconds_behind"]
    s_io = st["slave_io"]
    s_sql = st["slave_sql"]
    # 节点半同步状态：主库看 master_status，从库看 slave_status
    semi = st.get("semi_master_status") if is_master else st.get("semi_slave_status")
    semi_clients = st.get("semi_master_clients")
    semi_no_times = st.get("semi_no_times")
    semi_no_tx = st.get("semi_no_tx")
    semi_yes_tx = st.get("semi_yes_tx")

    # 与库中当前值逐字段比对，全部一致则视为「无变化」
    changed = not (
        _same(status, h.get("status")) and _same(role, h.get("role"))
        and _same(delay, h.get("repl_delay")) and _same(s_io, h.get("slave_io"))
        and _same(s_sql, h.get("slave_sql")) and _same(err, h.get("err"))
        and _same(semi, h.get("semi_sync")) and _same(semi_clients, h.get("semi_clients"))
        and _same(semi_no_times, h.get("semi_no_times")) and _same(semi_no_tx, h.get("semi_no_tx"))
        and _same(semi_yes_tx, h.get("semi_yes_tx"))
    )

    hb_due = False
    if HEARTBEAT_INTERVAL > 0:
        _k = (h["cluster_id"], h["host"], int(h["port"]))
        if time.monotonic() - _HEARTBEAT_AT.get(_k, 0.0) >= HEARTBEAT_INTERVAL:
            hb_due = True
            _HEARTBEAT_AT[_k] = time.monotonic()

    if not changed and not hb_due:
        return  # 稳态：不产生任何写库

    if hb_due:
        execute(
            """UPDATE hosts SET status=%s, role=%s, repl_delay=%s, slave_io=%s, slave_sql=%s,
               err=%s, semi_sync=%s, semi_clients=%s, semi_no_times=%s, semi_no_tx=%s,
               semi_yes_tx=%s, last_seen=NOW() WHERE cluster_id=%s AND host=%s AND port=%s""",
            (status, role, delay, s_io, s_sql, err,
             semi, semi_clients, semi_no_times, semi_no_tx, semi_yes_tx,
             h["cluster_id"], h["host"], int(h["port"])),
        )
    else:
        execute(
            """UPDATE hosts SET status=%s, role=%s, repl_delay=%s, slave_io=%s, slave_sql=%s,
               err=%s, semi_sync=%s, semi_clients=%s, semi_no_times=%s, semi_no_tx=%s,
               semi_yes_tx=%s WHERE cluster_id=%s AND host=%s AND port=%s""",
            (status, role, delay, s_io, s_sql, err,
             semi, semi_clients, semi_no_times, semi_no_tx, semi_yes_tx,
             h["cluster_id"], h["host"], int(h["port"])),
        )


def _alert_on_transition(key, status, is_master, repl_ok, delay, cluster_name, cluster_id, semi=None):
    """根据与上一次观测的差异发送告警，仅在状态跃迁时触发（避免刷屏）。"""
    prev = _PREV.get(key)
    _PREV[key] = dict(status=status, is_master=is_master, repl_ok=repl_ok, delay=delay, semi=semi)
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
        # 4) 半同步退化（RPO 风险）：主库 master_status / 从库 slave_status 由 ON 变非 ON
        if semi is not None and prev.get("semi") is not None and prev["semi"] == "ON" and semi != "ON":
            if is_master:
                send_alert("半同步退化(异步)",
                           f"{tag}主库 {host} 已退化为异步复制（半同步 master_status=OFF）。此期间提交的事务不等待从库 ACK，"
                           f"存在故障切换后数据丢失(RPO)风险。请检查从库连通性与 Slave_IO 线程、rpl_semi_sync_master_timeout。",
                           cluster_id=cluster_id)
            else:
                send_alert("半同步降级",
                           f"{tag}从库 {host} 未处于半同步态（slave_status=OFF）。若该从库被提升为主库，可能缺失主库已提交事务。",
                           cluster_id=cluster_id)
        # 5) 半同步恢复：由非 ON 回到 ON
        if semi is not None and prev.get("semi") is not None and prev["semi"] != "ON" and semi == "ON":
            send_alert("半同步恢复",
                       f"{tag}节点 {host} 半同步已恢复（status=ON），数据丢失(RPO)风险解除。",
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

# 凭证漂移自愈：每隔该秒数，用各节点 root 把 ha_monitor/replication/app 账号密码
# 重新校正为集群当前设定值，避免重装/改密后从库报 Access denied（密码不匹配）。
# 复用 sync_node_creds（含 ensure_ha_creds 权威 ALTER + 重生成部署文件），限频避免每周期触发。
CRED_SYNC_INTERVAL = 300


def run_once():
    # 连续探活失败计数挂在函数对象上（每个集群独立）。延迟到此处初始化，
    # 因为模块加载时函数尚未定义，顶层执行 run_once._fails = {} 会 NameError。
    if not hasattr(run_once, "_fails"):
        run_once._fails = {}
    # 凭证漂移自愈的限频时间戳挂在函数对象上（每集群独立）
    if not hasattr(run_once, "_last_cred_sync"):
        run_once._last_cred_sync = {}
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
        # ②.5 凭证漂移自愈（限频）：用各节点 root 把 ha_monitor/replication/app
        #      密码校正为集群当前值，防止重装/改密后从库报 Access denied。
        #      放在锁内，避免与复制变更并发；限频 CRED_SYNC_INTERVAL 减少 ALTER 频率。
        _cred_self_heal()
    finally:
        _RUN_ONCE_LOCK.release()


# run_once._fails 的模块级初始化（必须在 def run_once 之后，避免导入期 NameError）。
# 保证即使手动「刷新全部」(api_refresh) 在后台 monitor 首个周期之前就直接调用
# _probe_all()，也不会因 run_once._fails 未初始化而抛 AttributeError。
run_once._fails = {}


def _cred_self_heal():
    """限频的凭证漂移自愈：用各节点 root 将 ha_monitor/replication/app 账号密码
    校正为集群当前设定值，防止重装/改密后从库报 Access denied（密码不匹配）。

    放在 run_once 的互斥锁内调用，避免与复制变更并发。按 CRED_SYNC_INTERVAL
    限频（每个集群独立计时），避免每个探活周期都执行 ALTER。
    复用 node_gen.sync_node_creds（含 ensure_ha_creds 权威 ALTER + 重生成部署文件）。
    """
    try:
        clusters = list_clusters()
    except Exception as e:
        print(f"[monitor][cred-heal] 读取集群失败(可忽略): {e}")
        return
    # 上次巡检结果（每集群独立）：只在「结果发生变化」时才打印，
    # 否则每个 CRED_SYNC_INTERVAL 都会刷一条「凭证已校正到 N 个节点」的噪音。
    if not hasattr(run_once, "_last_cred_state"):
        run_once._last_cred_state = {}

    now = time.time()
    for cl in clusters:
        cid = cl["id"]
        last = run_once._last_cred_sync.get(cid, 0.0)
        if now - last < CRED_SYNC_INTERVAL:
            continue
        run_once._last_cred_sync[cid] = now
        prev = run_once._last_cred_state.get(cid)
        try:
            # sync_node_creds 返回已同步节点数(int)
            synced = node_gen.sync_node_creds(cid)
        except Exception as e:
            # 自愈失败不应影响主流程（下次周期再试）；失败状态同样去重打印
            if prev != "err":
                print(f"[monitor][cred-heal] 集群 {cid} 校正失败(可忽略): {e}")
            run_once._last_cred_state[cid] = "err"
            continue
        if prev is None:
            print(f"[monitor][cred-heal] 集群 {cid} 凭证巡检完成：{synced} 个节点已对齐")
        elif prev == "err":
            print(f"[monitor][cred-heal] 集群 {cid} 凭证校正已恢复：{synced} 个节点已对齐")
        elif prev != synced:
            print(f"[monitor][cred-heal] 集群 {cid} 已对齐节点数变化：{prev} → {synced}")
        run_once._last_cred_state[cid] = synced


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
        # 一并把「待比对」的当前值查出来：_update_host 需要它们做变更检测（空闲时不写库）
        hosts = query(
            "SELECT host, port, cluster_id, status, role, repl_delay, slave_io, slave_sql, err, "
            "semi_sync, semi_clients, semi_no_times, semi_no_tx, semi_yes_tx "
            "FROM hosts WHERE cluster_id=%s", (cid,))
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
            # is_master 判定不能只依赖管理库记录的 current_master：切换完成后，
            # 新主写入管理库与 monitor 读取之间有时间窗，且若节点其实仍挂着复制通道
            # （slave 形态，如被提升为主时未清干净残留通道的边界情况），不应被当作主库。
            # 故 is_master = 管理库记它为主 且 该节点实际不是 slave 形态。
            # 这样：节点真实挂着复制通道(SQL/IO 可能停)时，无论管理库如何记，都走从库
            # 自愈分支（heal_replication / heal_io_gtid_gap），避免「切换后 SQL 线程停却
            # 不自愈、复制永久断流」的 bug。
            is_master = ((host, port) == cur_key) and (st.get("mode") != "slave")
            # 节点半同步状态：主库看 master_status，从库看 slave_status
            semi = st.get("semi_master_status") if is_master else st.get("semi_slave_status")
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
                                 st.get("seconds_behind"), cluster_name, cid, semi)
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
            # 当前主库但若仍挂着复制通道（SHOW REPLICA STATUS 有输出）：这是提升为主时
            # RESET SLAVE ALL 未清干净、或被 ensure_topology 误 repoint 的残留。它会让主库
            # 仍尝试拉旧源 binlog 而报 1236（"master 却 IO 错误"的矛盾现象）。
            # 必须强制 promote_to_master 清通道，使其成为纯主库。
            if is_master and st.get("slave_io") is not None:
                ok_p, msg_p = promote_to_master(
                    h_host, h_port, creds["ha_user"], creds["ha_pass"], ru, rp)
                if ok_p:
                    print(f"[monitor] 主库 {h_host}:{h_port} 清残留复制通道成功")
                else:
                    print(f"[monitor] 主库 {h_host}:{h_port} 清残留通道失败: {msg_p}")
            # 复制 SQL 线程中断（多为幂等冲突导致的 1007/1062），尝试自愈跳过冲突事务。
            # 仅当 SQL 线程明确为 "No"（非 None/空，即确有复制通道且停了）才触发，
            # 避免无复制通道节点（slave_sql=None）被误判为「需自愈」刷日志。
            if not is_master and st.get("slave_sql") == "No" and get_setting("AUTO_REPAIR", "1") == "1":
                ok, msg = heal_replication(h_host, h_port, creds["ha_user"], creds["ha_pass"], ru, rp)
                if ok:
                    print(f"[monitor] 自愈 {h_host}:{h_port} 成功: {msg}")
                    try:
                        if not query("SELECT 1 FROM alerts WHERE cluster_id=%s AND host=%s AND port=%s AND status='healed' AND created_at > NOW()-INTERVAL 10 MINUTE LIMIT 1", (cid, h_host, int(h_port))):
                            execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) VALUES(%s,%s,%s,'selfheal','healed',%s,NOW())", (cid, h_host, int(h_port), msg))
                    except Exception:
                        pass
                else:
                    print(f"[monitor] 自愈 {h_host}:{h_port} 失败: {msg}")
            # 复制 IO 线程中断，分两类处理：
            #  (A) 匿名事务 1236（"Cannot replicate anonymous transaction when AUTO_POSITION=1"）：
            #      源端 binlog 含匿名事务（节点曾被提升为主、旧 binlog 写于 gtid_mode<ON 期）。
            #      注入空事务无效，必须【重新挂载】并带 ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL
            #      （从库侧把匿名事务当本地 GTID 接收），故走 _repoint_slave（内部 make_slave_of 已带该选项）。
            #  (B) 缺 GTID 1236/13114（源端主库丢失从库已执行的 GTID）：在源端主库注入空事务，
            #      走 heal_io_gtid_gap。
            if not is_master and st.get("slave_io") == "No" and get_setting("AUTO_REPAIR", "1") == "1":
                io_err = st.get("slave_io_err") or ""
                if "anonymous transaction" in io_err and cur_h:
                    # (A) 匿名事务 1236：强制重挂载（带 ASSIGN_GTIDS），不治本则永久循环
                    ok_r, msg_r = _repoint_slave(
                        cid, {"host": h_host, "port": h_port, "status": "up"},
                        cur_h, cur_p, creds)
                    if ok_r:
                        print(f"[monitor] 匿名事务1236 自愈(重挂载) {h_host}:{h_port}: {msg_r}")
                        try:
                            if not query("SELECT 1 FROM alerts WHERE cluster_id=%s AND host=%s AND port=%s AND status='healed' AND created_at > NOW()-INTERVAL 10 MINUTE LIMIT 1", (cid, h_host, int(h_port))):
                                execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) VALUES(%s,%s,%s,'selfheal','healed',%s,NOW())", (cid, h_host, int(h_port), msg_r))
                        except Exception:
                            pass
                    else:
                        print(f"[monitor] 匿名事务1236 重挂载失败 {h_host}:{h_port}: {msg_r}")
                else:
                    # (B) 缺 GTID 类：源端注入空事务
                    sru, srp = node_root(h_host, cid)
                    mru, mrp = (node_root(cur_h, cid) if cur_h else ("root", ""))
                    ok_g, msg_g = heal_io_gtid_gap(
                        h_host, h_port, creds["ha_user"], creds["ha_pass"],
                        sru, srp, cur_h, int(cur_p) if cur_p else None, mru, mrp)
                    if ok_g:
                        print(f"[monitor] IO 自愈 {h_host}:{h_port}: {msg_g}")
                        try:
                            if not query("SELECT 1 FROM alerts WHERE cluster_id=%s AND host=%s AND port=%s AND status='healed' AND created_at > NOW()-INTERVAL 10 MINUTE LIMIT 1", (cid, h_host, int(h_port))):
                                execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) VALUES(%s,%s,%s,'selfheal','healed',%s,NOW())", (cid, h_host, int(h_port), msg_g))
                        except Exception:
                            pass
                    elif "more GTIDs" in msg_g or "13114" in msg_g or "1236" in msg_g:
                        print(f"[monitor] IO 自愈 {h_host}:{h_port} 失败: {msg_g}")
        # 当前主库连续失败达到阈值 -> 自动切换
        fails = run_once._fails.setdefault(cid, {})
        for key, (st, is_master) in list(cache.items()):
            if key[2] != cid:
                continue
            if is_master and fails.get(key, 0) >= FAIL_THRESHOLD and get_setting("AUTO_FAILOVER", "1") == "1":
                print(f"[monitor] 集群[{cluster_name}] 主库 {cur_h}:{cur_p} "
                      f"连续 {fails[key]} 次探活失败，触发自动切换")
                if do_failover(cid, "auto: master unreachable"):
                    run_once._fails[cid] = {}  # 重置，避免重复触发
                break

        # 高延迟 / 主库压力持续超阈值 -> 自动切换（默认关闭，需显式开启）
        try:
            _auto_switch_on_lag_or_pressure(cid, cluster_name, cache)
        except Exception as e:
            print(f"[monitor] 延迟/压力自动切换判定异常(可忽略): {e}")

        # 维护拓扑：切后对齐 + 旧主恢复重入
        try:
            ensure_topology(cid)
        except Exception as e:
            print(f"[monitor] ensure_topology 集群[{cluster_name}] 错误(可忽略): {e}")


# ==========================================================================
# 自动切换（高延迟 / 主库压力）
# --------------------------------------------------------------------------
# 节点在远程主机上，管理器只挂了本机 docker.sock，拿不到远程 CPU/内存/负载，
# 故「资源紧张」改用【MySQL 侧压力代理指标】：Threads_running、Threads_connected、
# Innodb_row_lock_waits 增量、Slow_queries 增量。
# ==========================================================================
# 压力类指标的上一轮原始累计值（用于做差得到「本周期增量」）
_PRESSURE_RAW = {}
_PRESSURE_AT = {}
# 「连续达标」计数 与 上次自动切换时间（每集群独立）
_SWITCH_STREAK = {}
_LAST_AUTO_SWITCH = {}


def _pressure_samples(cache, cid, now):
    """把累计型计数器（行锁等待 / 慢查询）换算成本周期增量。

    每个节点每周期只采样一次并一次性返回：重复采样会把增量算成 0。
    """
    out = {}

    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def _delta(cur, old):
        if cur is None or old is None:
            return None
        d = cur - old
        # 计数器回绕 / 实例重启：负值视为无效
        return d if d >= 0 else None

    for key, val in list(cache.items()):
        if key[2] != cid:
            continue
        st = val[0]
        rw = _int(st.get("row_lock_waits"))
        sq = _int(st.get("slow_queries"))
        prev = _PRESSURE_RAW.get(key) or {}
        out[key] = {
            "threads_running": _int(st.get("threads_running")),
            "threads_connected": _int(st.get("connections")),
            "row_lock_delta": _delta(rw, prev.get("row_lock_waits")),
            "slow_delta": _delta(sq, prev.get("slow_queries")),
        }
        _PRESSURE_RAW[key] = {"row_lock_waits": rw, "slow_queries": sq}
        _PRESSURE_AT[key] = now
    return out


def _auto_switch_on_lag_or_pressure(cid, cluster_name, cache):
    """高延迟 / 主库压力「持续」超阈值时，自动把最优从库提升为主库。

    需全部满足才动作（默认关闭，需显式开启）：
      1. 总开关 AUTO_FAILOVER=1，且 LAG_FAILOVER_ENABLED 或 PRESSURE_FAILOVER_ENABLED 开启；
      2. 主库在线（失联由既有「连续探活失败」逻辑处理）；
      3. 延迟：≥2 个从库延迟 ≥ LAG_FAILOVER_SEC；或压力：主库 Threads_running /
         行锁等待周期增量 超阈值；
      4. 连续 LAG_FAILOVER_CYCLES 个周期成立（避免大事务/批量任务造成瞬时抖动误切）；
      5. 距上次自动切换超过 FAILOVER_COOLDOWN_SEC（防反复切换）。

    目标节点：复制正常(IO/SQL 双 Yes)、延迟最小、且自身不在压力中的从库。
    """
    if get_setting("AUTO_FAILOVER", "1") != "1":
        return
    lag_on = get_setting("LAG_FAILOVER_ENABLED", "0") == "1"
    prs_on = get_setting("PRESSURE_FAILOVER_ENABLED", "0") == "1"
    if not (lag_on or prs_on):
        _SWITCH_STREAK[cid] = 0
        return

    def _int_setting(k, d):
        try:
            return int(get_setting(k, d))
        except (TypeError, ValueError):
            return int(d)

    lag_sec = _int_setting("LAG_FAILOVER_SEC", 60)
    cycles = max(1, _int_setting("LAG_FAILOVER_CYCLES", 3))
    prs_threads = _int_setting("PRESSURE_THREADS_RUNNING", 50)
    prs_locks = _int_setting("PRESSURE_ROW_LOCK_WAITS", 50)
    cooldown = _int_setting("FAILOVER_COOLDOWN_SEC", 120)

    now = time.time()
    if now - _LAST_AUTO_SWITCH.get(cid, 0.0) < cooldown:
        return

    master_key = None
    slaves = []
    for key, val in list(cache.items()):
        if key[2] != cid:
            continue
        st, is_master = val[0], val[1]
        if is_master:
            master_key = key
        elif st.get("alive"):
            slaves.append((key, st))
    if master_key is None:
        return
    if not cache[master_key][0].get("alive"):
        _SWITCH_STREAK[cid] = 0
        return

    samples = _pressure_samples(cache, cid, now)

    # ---- 判据 A：多数从库复制延迟过高 ----
    lag_hit, lag_detail = False, ""
    if lag_on and slaves:
        lagging = [k for k, s in slaves
                   if s.get("seconds_behind") is not None
                   and s["seconds_behind"] >= lag_sec]
        # 至少 2 个从库（或过半，取较大者）同时高延迟才算，避免单节点抖动
        need = max(2, (len(slaves) + 1) // 2)
        if len(lagging) >= need:
            lag_hit = True
            lag_detail = f"{len(lagging)}/{len(slaves)} 个从库延迟 ≥ {lag_sec}s"

    # ---- 判据 B：主库自身压力过高 ----
    prs_hit, prs_detail = False, ""
    if prs_on:
        p = samples.get(master_key) or {}
        tr, rld = p.get("threads_running"), p.get("row_lock_delta")
        if tr is not None and tr >= prs_threads:
            prs_hit = True
            prs_detail = f"Threads_running={tr} ≥ {prs_threads}"
        elif rld is not None and rld >= prs_locks:
            prs_hit = True
            prs_detail = f"行锁等待 {rld} 次/周期 ≥ {prs_locks}"

    if not (lag_hit or prs_hit):
        _SWITCH_STREAK[cid] = 0
        return

    _SWITCH_STREAK[cid] = _SWITCH_STREAK.get(cid, 0) + 1
    if _SWITCH_STREAK[cid] < cycles:
        return

    # 选目标：自身不承压 + 复制正常 + 延迟最小
    cands = []
    for key, st in slaves:
        if st.get("slave_io") != "Yes" or st.get("slave_sql") != "Yes":
            continue
        p = samples.get(key) or {}
        over = ((p.get("threads_running") or 0) >= prs_threads
                or (p.get("row_lock_delta") or 0) >= prs_locks)
        d = st.get("seconds_behind")
        cands.append(((1 if over else 0), (d if d is not None else 10 ** 9), key))
    if not cands:
        return
    cands.sort(key=lambda x: (x[0], x[1]))
    tkey = cands[0][2]

    reason = "auto: " + "；".join([x for x in (lag_detail, prs_detail) if x])
    print(f"[monitor] 集群[{cluster_name}] {reason}，连续 {_SWITCH_STREAK[cid]} 个周期达标，"
          f"自动切换主库 → {tkey[0]}:{tkey[1]}")
    try:
        ok = do_failover_to(cid, tkey[0], tkey[1], reason)
    except Exception as e:
        print(f"[monitor] 自动切换异常: {e}")
        return
    _SWITCH_STREAK[cid] = 0
    if ok:
        _LAST_AUTO_SWITCH[cid] = now
        try:
            send_alert("自动切换主库",
                       f"集群[{cluster_name}] {reason}，已切换主库至 {tkey[0]}:{tkey[1]}。",
                       cluster_id=cid)
        except Exception:
            pass


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
        # 轮询间隔动态读取 setting（页面可配置），默认 2s
        time.sleep(int(get_setting("CHECK_INTERVAL", "2") or 2))


# 周期计数：慢日志仍按周期倍数（默认关闭，影响可忽略）
_COLLECT_CYCLE = {"metrics": 0, "slow": 0, "drift": 0}
# 采集时间戳（monotonic，秒）：指标/漂移改为**按秒限频**，不随 CHECK_INTERVAL 变化而漂移
_COLLECT_AT = {"metrics": 0.0, "slow": 0.0, "drift": 0.0}
_SLOW_EVERY = 20        # 慢日志仍按周期倍数（约默认 60s 一次）


def _collect_due(name, seconds=0, every_cycles=0):
    """是否到了该采集的时候。

    every_cycles > 0 时按「周期倍数」判定；否则按 wall-clock 秒限频。
    指标/配置漂移之所以改用秒限频：按周期倍数的话，把 CHECK_INTERVAL 调小
    （如 2s→1s）会让采集频率同步放大，管理库 INSERT 量随之线性增长。
    """
    _COLLECT_CYCLE[name] += 1
    if every_cycles > 0:
        if _COLLECT_CYCLE[name] < every_cycles:
            return False
        _COLLECT_CYCLE[name] = 0
        return True
    if seconds <= 0:
        return True
    now = time.monotonic()
    if now - _COLLECT_AT.get(name, 0.0) < seconds:
        return False
    _COLLECT_AT[name] = now
    return True


def _maybe_collect():
    """在后台 monitor 周期内错峰触发指标/慢日志/配置漂移采集（仅写入，无副作用）。

    采集任务本身各自带「与上次一致的快照不写库」的变更检测（见 metrics.py /
    config_drift.py），因此空闲时即使到点也不会产生 INSERT。
    """
    if _collect_due("metrics", METRICS_INTERVAL):
        try:
            metrics.collect_metrics()
        except Exception as e:
            print(f"[monitor] 指标采集异常: {e}", flush=True)
            import traceback; traceback.print_exc()
    if _collect_due("slow", 0, _SLOW_EVERY):
        _COLLECT_CYCLE["slow"] = 0
        if slow_log.slowlog_enabled():
            try:
                slow_log.collect_slow_log()
            except Exception as e:
                print(f"[monitor] 慢日志采集异常: {e}", flush=True)
                import traceback; traceback.print_exc()
        else:
            print("[monitor] 慢日志采集已暂停(SLOWLOG_ENABLED=0)", flush=True)
    if _collect_due("drift", DRIFT_INTERVAL):
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
