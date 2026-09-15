"""自动/手动故障切换核心逻辑（多集群：所有操作均按 cluster_id 隔离）。"""
from .db import query, execute, get_setting
from .config import get_manager_public_ip, get_manager_docker_subnet_wildcard, FAILOVER_COOLDOWN_SEC
from .state import get_current_master, set_current_master
from .ha_client import (set_read_only, promote_to_master, make_slave_of,
                        set_local_master_flag, is_stale_master, get_status,
                        heal_replication, heal_io_gtid_gap, get_gtid_executed,
                        parse_gtid_set, fix_node_mysql_params, ensure_ha_creds,
                        ensure_connect_creds,
                        heal_anonymous_gtid_io, is_read_only)
import time
import threading
from .alerts import send_alert
from .clusters import get_cluster_creds

# 账号自愈限频：稳定态下 ensure_ha_creds 已幂等（无变化零写操作），
# 此处再按 CRED_SELFHEAL_INTERVAL 限制每集群调用频率，避免每监控周期（~2s）都
# 对主库建连比对账号，进一步降低管理器开销与主库 SELECT 压力。
CRED_SELFHEAL_INTERVAL = 300
_SELFHEAL_CRED_TS = {}
# 节点参数自愈（gtid_mode / server_id）限频：fix_node_mysql_params 会连节点并做
# SET GLOBAL/PERSIST 校验。环境性失败（如 ha_monitor 缺 SUPER/SYSTEM_VARIABLES_ADMIN）
# 时原本每周期重跑一次并刷同一条警告日志（每 2s 一行），且对节点是持续的无谓负载。
# 参数本就不需要每周期校验，改为每 PARAM_SELFHEAL_INTERVAL 秒最多一次。
PARAM_SELFHEAL_INTERVAL = 300
_SELFHEAL_PARAM_TS = {}

# 节点重指向失败的告警按 (host, port) 做冷却（默认 5 分钟最多提示一次），
# 避免 gtid_mode=OFF 等根因未解时，monitor 每个周期都刷同样的失败日志。
_repoint_fail_ts = {}
_REPOINT_WARN_COOLDOWN = 300

# 拓扑修复动作限频：ensure_topology 每个监控周期（~2s）都会跑一遍，
# 若某从库因环境问题始终「看起来没指向当前主」，会被无限次重挂复制
# （STOP/RESET/CHANGE MASTER/START REPLICA），是空闲时节点写盘与日志刷屏的主因。
# 故同一节点两次「拓扑修复」之间至少间隔 TOPO_REPAIR_INTERVAL 秒。
# 真正的故障切换走 do_failover，不受此限频影响（切换后会主动清除限频）。
TOPO_REPAIR_INTERVAL = 60
_TOPO_AT = {}
# 推送 current_master 到各节点本地 ha_local.state 的兜底间隔（秒）：
# 值未变化时不推送；但节点重启可能丢失本地状态，故最长 PUSH_MAX_INTERVAL 强制推一次。
PUSH_MAX_INTERVAL = 300
_LAST_PUSH = {}

# 故障切换全局互斥锁：自动（monitor 在 _RUN_ONCE_LOCK 保护区内调用 do_failover）与手动（API）
# 切换统一串行，避免并发切换造成 split-brain / 双主。
# ⚠ 必须用「独立」锁而非 monitor 的 _RUN_ONCE_LOCK——monitor 调用 do_failover 时已持有
# _RUN_ONCE_LOCK，若此处复用会在同一线程重入死锁。
_FAILOVER_LOCK = threading.Lock()
_LAST_FAILOVER_TS = {}


def _failover_guarded(func):
    """串行化所有故障切换（自动/手动统一互斥），避免并发切换造成 split-brain / 双主。

    必须在独立锁上串行，而非复用 monitor 的 _RUN_ONCE_LOCK（否则同一线程重入死锁）。
    仅当切换成功时才记录时间戳，供手动 API 路径做冷却判断（避免自动路径在冷却期刷告警）。
    """
    def wrapper(cluster_id, *args, **kwargs):
        with _FAILOVER_LOCK:
            result = func(cluster_id, *args, **kwargs)
            if result:
                _LAST_FAILOVER_TS[cluster_id] = time.time()
            return result
    return wrapper


def _warn_repoint(host, port, msg):
    now = time.time()
    key = (host, port)
    if now - _repoint_fail_ts.get(key, 0) > _REPOINT_WARN_COOLDOWN:
        print(f"[failover] {msg}")
        _repoint_fail_ts[key] = now


def load_creds(cluster_id=None):
    """读取指定集群凭证；未指定时返回首个集群（缺省集群）的凭证。

    兼容旧数据：若集群无密码，回退到全局 settings（迁移前的旧凭证）。
    """
    if cluster_id:
        c = get_cluster_creds(cluster_id)
        if c and c["ha_pass"]:
            return c
    from .clusters import get_default_cluster
    d = get_default_cluster()
    if d:
        c = get_cluster_creds(d["id"])
        if c and c["ha_pass"]:
            return c
    # 旧版兜底（迁移前）
    from .db import get_setting
    return dict(
        ha_user=get_setting("HA_USER") or "ha_monitor",
        ha_pass=get_setting("HA_PASS") or "",
        repl_user=get_setting("REPL_USER") or "replication",
        repl_pass=get_setting("REPL_PASS") or "",
        app_user=get_setting("APP_USER") or "app",
        app_pass=get_setting("APP_PASS") or "",
    )


def node_root(ip, cluster_id=None):
    """取节点 MySQL 的 root 凭证（容器创建时固定，作为 ha_monitor 漂移时的回退凭据）。"""
    try:
        if cluster_id:
            row = query(
                "SELECT n.root_user, n.root_pass FROM nodes n "
                "JOIN hosts h ON h.host = n.ip AND h.cluster_id = n.cluster_id "
                "WHERE n.ip=%s AND n.cluster_id=%s", (ip, cluster_id))
        else:
            row = query("SELECT root_user, root_pass FROM nodes WHERE ip=%s", (ip,))
        if row:
            return (row[0]["root_user"] or "root", row[0]["root_pass"] or "")
    except Exception:
        pass
    return ("root", "")


def list_hosts(cluster_id=None):
    if cluster_id:
        return query(
            "SELECT host, port, role, status, is_current_master, initial_role, repl_delay "
            "FROM hosts WHERE cluster_id=%s ORDER BY id", (cluster_id,))
    return query(
        "SELECT host, port, role, status, is_current_master, initial_role, repl_delay "
        "FROM hosts ORDER BY id"
    )


def push_current_master_to_nodes(cluster_id, master_str):
    """把当前主库标识写入集群内每个节点的本地 MySQL（ha_local.state）。

    供 keepalived 检查脚本本地读取判定 VIP 归属；完全本地，无需节点回连管理库。
    单节点失败不影响整体（下个监控周期会重试）。

    【空闲零写入】旧实现每个监控周期（~2s）都无条件写一遍集群内**所有节点**的
    ha_local.state —— 稳态下就是每 2 秒对每个节点做一次真实行更新，是「没有业务
    数据交互却在持续写盘」最直接的来源。改为：值未变化则不写；仅当主库变更、
    或距上次推送超过 PUSH_MAX_INTERVAL（兜底，防止节点重启丢失本地状态）才写。
    """
    prev = _LAST_PUSH.get(cluster_id)
    now = time.monotonic()
    if prev and prev[0] == master_str and (now - prev[1]) < PUSH_MAX_INTERVAL:
        return
    _LAST_PUSH[cluster_id] = (master_str, now)
    creds = load_creds(cluster_id)
    try:
        rows = query("SELECT host, port FROM hosts WHERE cluster_id=%s", (cluster_id,))
    except Exception:
        return
    for r in rows:
        try:
            ru, rp = node_root(r["host"], cluster_id)
            set_local_master_flag(r["host"], int(r["port"]), creds["ha_user"],
                                  creds["ha_pass"], master_str, ru, rp)
        except Exception:
            pass


def _repoint_slave(cluster_id, h, target_host, target_port, creds):
    """把节点 h 重指向新主，并立即自愈复制冲突（SQL 幂等冲突 + IO 缺口）。

    切换/拓扑对齐后，从库可能立即撞上两类复制错误：
    - SQL 线程 1007/1062 等幂等冲突（把滞后的从库提升为主、再降回从库时，本地已
      存在同名对象而 GTID 集合未记录该事务）；
    - IO 线程 1236（源端主库缺失 GTID，从库已执行过）。
    START SLAVE 后错误往往延迟上报，故重试几次直至复制线程双 Yes 或已自愈。

    返回 (ok, msg)：调用方必须按【元组】解包（monitor.py 两处即按 `ok_r, msg_r` 解包，
    早期返回裸 bool 会抛 `TypeError: cannot unpack non-iterable bool object`）。
    ok 为**真实**复制状态：重试耗尽仍未双 Yes 时返回 False，绝不返回假成功。
    """
    host, port = h["host"], int(h["port"])
    ru, rp = node_root(host, cluster_id)
    ok, err = make_slave_of(host, port, creds["ha_user"], creds["ha_pass"],
                            target_host, int(target_port), creds["repl_user"],
                            creds["repl_pass"], ru, rp)
    if not ok:
        _warn_repoint(host, port, f"重指向 {host}:{port} 失败: {err}")
        return False, f"重指向 {host}:{port} 失败: {err}"
    last_err = ""
    for _ in range(8):
        try:
            st = get_status(host, port, creds["ha_user"], creds["ha_pass"], ru, rp)
        except Exception:
            st = {}
        if st.get("slave_io") == "Yes" and st.get("slave_sql") == "Yes" and not st.get("error"):
            return True, f"{host}:{port} 已重指向 {target_host}:{target_port}（复制双 Yes）"
        last_err = (st.get("error") or st.get("slave_io_err") or st.get("slave_sql_err") or "")
        try:
            heal_replication(host, port, creds["ha_user"], creds["ha_pass"], ru, rp)
        except Exception as e:
            print(f"[failover] 复制自愈异常 {host}:{port}: {e}")
        try:
            mru, mrp = node_root(target_host, cluster_id)
            heal_io_gtid_gap(host, port, creds["ha_user"], creds["ha_pass"], ru, rp,
                             target_host, int(target_port), mru, mrp)
        except Exception as e:
            print(f"[failover] IO 自愈异常 {host}:{port}: {e}")
        # IO 仍失败且为「Access denied」：主库 replication 账号密码漂移，到主库重置密码并重启从库 IO
        try:
            st = get_status(host, port, creds["ha_user"], creds["ha_pass"], ru, rp)
            if st.get("slave_io") != "Yes" and "Access denied for user" in (st.get("slave_io_err") or ""):
                from .ha_client import reset_replication_creds_on_master, restart_slave_io
                mru, mrp = node_root(target_host, cluster_id)
                rok, rmsg = reset_replication_creds_on_master(
                    target_host, int(target_port), creds["ha_user"], creds["ha_pass"],
                    mru, mrp, creds["repl_user"], creds["repl_pass"], slave_ip=host)
                print(f"[failover] 主库重置复制密码 {host}:{port}: {rmsg}")
                if rok:
                    sok, smsg = restart_slave_io(host, port, creds["ha_user"], creds["ha_pass"], ru, rp)
                    print(f"[failover] {smsg}")
        except Exception as e:
            print(f"[failover] 复制密码重置异常 {host}:{port}: {e}")
        time.sleep(2)
    # 重试耗尽仍未达双 Yes：必须返回失败。
    # 原实现这里无条件 return True，导致 ensure_topology 打印「已重指向新主」的假成功、
    # 不产生任何告警，节点实际一直没加入集群、复制持续中断（典型静默故障，极难排查）。
    _msg = (f"{host}:{port} 重指向 {target_host}:{target_port} 后复制仍未就绪"
            f"（8 次自愈未达双 Yes）")
    if last_err:
        _msg += f"，最后错误：{last_err}"
    print(f"[failover] {_msg}")
    return False, _msg


def _gtid_is_superset(a, b):
    """True 表示 b 中的每个 GTID 也都存在于 a（即 b ⊆ a）。"""
    if not b:
        return True
    if not a:
        return False
    da, db = parse_gtid_set(a), parse_gtid_set(b)
    for uuid, nums in db.items():
        if uuid not in da:
            return False
        if not nums.issubset(da[uuid]):
            return False
    return True


def pick_best_master(cluster_id, hosts, creds):
    """从存活节点中选出最适合作为主库的节点，避免把 GTID 进度落后的节点选为主库
    （落后主库会导致其余从库比它更「超前」而报 1236：Replica has more GTIDs than
    the source has）。

    优先选「其 gtid_executed 是其余所有存活节点的超集」的节点（提升它不会让任何从库
    超前）；否则选事务总数最多的节点（最先进）。
    """
    alive = [h for h in hosts if h.get("status") == "up"]
    if not alive:
        return None, None
    info = {}
    for h in alive:
        ru, rp = node_root(h["host"], cluster_id)
        g = get_gtid_executed(h["host"], int(h["port"]), creds["ha_user"],
                              creds["ha_pass"], ru, rp)
        info[h["host"]] = g or ""
    # 1) 超集节点优先
    for h in alive:
        s = info[h["host"]]
        if s and all(_gtid_is_superset(s, info[o["host"]])
                     for o in alive if o["host"] != h["host"]):
            return h["host"], int(h["port"])
    # 2) 退而求其次：事务总数最多（最先进）
    def _count(s):
        d = parse_gtid_set(s)
        return sum(len(v) for v in d.values())
    best = max(alive, key=lambda h: _count(info[h["host"]]))
    return best["host"], int(best["port"])


def reset_topo_throttle(cluster_id):
    """真实故障切换后主动清除该集群的拓扑修复限频与本地主库推送缓存。

    限频是为了避免稳态下每 2s 重挂一次复制；一旦真的发生切换，必须放行
    ensure_topology 立即重挂载其余节点，并立刻把新主推到各节点的 ha_local.state，
    否则 VIP 漂移会被推迟到限频窗口结束。
    """
    for k in [k for k in _TOPO_AT
              if k[0] in ("repoint", "promote") and k[1] == cluster_id]:
        _TOPO_AT.pop(k, None)
    _LAST_PUSH.pop(cluster_id, None)


# 交叉验证用的「连不上主库」判定（与 monitor._repl_err_is_connectivity 同义；
# 此处本地实现，避免 failover ↔ monitor 循环导入）
_CONN_ERR_PATS = (
    "2003", "2002", "2013", "2059", "1129", "1130",
    "can't connect", "cannot connect", "lost connection",
    "reconnecting to master", "connecting to master",
    "connection refused", "no route to host", "network is unreachable", "timeout",
)


def _io_err_is_connectivity(err):
    low = (err or "").lower()
    return any(p in low for p in _CONN_ERR_PATS)


def confirm_master_down(cluster_id, master_host, master_port, creds, node_root_fn=None):
    """交叉验证：让集群内其它节点作证「主库确实连不上」，用于防误切与防脑裂。

    为什么需要：探活是**管理器单方面**的视角。管理器↔主库网络分区、或主库
    max_connections 打满，都会表现为「管理器连不上」，但主库其实好好的——
    此时若直接提升从库，就会出现两个可写节点（脑裂双写）。

    判定口径：
      - 有其它节点仍与主库保持复制连接（IO 线程 Yes）→ 主库从别的视角仍存活 → **不确认**
      - 至少一个其它节点明确报「连不上主库」→ 确认（独立证人）
      - 一个证人都联系不上 → 不确认（宁可不切，也不赌）
      - 集群没有其它节点 → 无从作证，放行（并在返回信息里说明）

    返回 (confirmed: bool, detail: str)
    """
    try:
        rows = query("SELECT host, port FROM hosts WHERE cluster_id=%s", (cluster_id,))
    except Exception as e:
        return False, f"读取集群节点失败，无法交叉验证：{e}"
    others = []
    for r in rows:
        try:
            h, p = r["host"], int(r["port"])
        except (TypeError, ValueError):
            continue
        if h == master_host and p == int(master_port):
            continue
        others.append((h, p))
    if not others:
        return True, "集群内无其它节点可交叉验证，放行"

    alive_seen, down_witness, unknown = [], [], []
    for h, p in others:
        ru, rp = (node_root_fn(h, cluster_id) if node_root_fn else ("root", ""))
        try:
            st = get_status(h, p, creds["ha_user"], creds["ha_pass"], ru, rp)
        except Exception as e:
            unknown.append(f"{h}:{p}(探活异常 {type(e).__name__})")
            continue
        if not st.get("alive"):
            unknown.append(f"{h}:{p}(节点离线)")
            continue
        io = st.get("slave_io")
        if io == "Yes":
            alive_seen.append(f"{h}:{p}")
        elif io in ("No", "Connecting"):
            if _io_err_is_connectivity(st.get("slave_io_err")):
                down_witness.append(f"{h}:{p}")
            else:
                unknown.append(f"{h}:{p}(IO 停但错误非连接类，无法作证)")
        else:
            unknown.append(f"{h}:{p}(无复制通道，无法作证)")

    if alive_seen:
        return False, ("其它节点仍与 %s:%s 保持复制连接（%s），说明主库从其它视角看仍然存活；"
                       "判定为管理器侧网络/权限问题，已中止切换以规避脑裂"
                       % (master_host, master_port, "、".join(alive_seen)))
    if down_witness:
        return True, "已由 %s 作证主库不可达" % "、".join(down_witness)
    detail = ("无法确认主库状态：没有任何节点能作证（%s），已中止切换" % "；".join(unknown)) \
        if unknown else "无可用证人，已中止切换"
    return False, detail


@_failover_guarded
def do_failover(cluster_id, reason="auto"):
    """主库判定宕机后执行切换：fencing -> 提升延迟最小从库 -> 更新 current_master -> 其余节点重指向。"""
    cur_h, cur_p = get_current_master(cluster_id)
    creds = load_creds(cluster_id)
    if not cur_h or not creds["ha_user"]:
        send_alert("切换中止", "未配置受管主机或 ha_monitor 账号，无法切换。", cluster_id=cluster_id)
        return False

    hosts = list_hosts(cluster_id)
    # 候选：存活且非当前主，按复制延迟升序
    candidates = [
        h for h in hosts
        if (h["host"], int(h["port"])) != (cur_h, int(cur_p)) and h["status"] == "up"
    ]
    if not candidates:
        send_alert("切换失败", f"主库 {cur_h}:{cur_p} 宕机，但无可用从库可提升。", cluster_id=cluster_id)
        return False
    # 选『最先进』的从库提升：优先 GTID 进度最高者，避免切换后其余从库因比新主更超前而报 1236
    best_h, best_p = pick_best_master(cluster_id, candidates, creds)
    if best_h:
        new = next((h for h in candidates
                    if (h["host"], int(h["port"])) == (best_h, int(best_p))),
                   candidates[0])
    else:
        candidates.sort(key=lambda h: (h["repl_delay"] is None, h["repl_delay"] or 999999))
        new = candidates[0]
    new_h, new_p = new["host"], int(new["port"])

    # 1) fencing：尽量把旧主置只读，使其释放 VIP 并拒绝写入
    ru, rp = node_root(cur_h, cluster_id)
    fenced = True
    try:
        set_read_only(cur_h, cur_p, creds["ha_user"], creds["ha_pass"], True, ru, rp)
    except Exception as e:
        fenced = False
        print(f"[failover] 无法连接旧主做 fencing(可能已宕机): {e}")

    # 1.5) 防误切 / 防脑裂：fencing 失败只说明「管理器连不上旧主」，不等于旧主已宕机
    #      （网络分区、max_connections 打满、账号权限失效都会如此）。此时必须由集群内
    #      其它节点作证主库确实不可达，否则**宁可不切**——两个可写节点（脑裂双写）
    #      的代价远大于「该切没切」。
    if not fenced and get_setting("REQUIRE_WITNESS_FOR_FAILOVER", "1") == "1":
        ok_c, detail = confirm_master_down(cluster_id, cur_h, cur_p, creds, node_root)
        if not ok_c:
            send_alert("切换中止(疑似脑裂风险)",
                       f"无法连接旧主 {cur_h}:{cur_p} 做 fencing，且交叉验证未通过：{detail}。"
                       f"为避免出现两个可写节点（脑裂双写），本次切换已中止。请检查管理器到旧主的"
                       f"网络、旧主 max_connections 与 ha_monitor 账号权限；确需强制切换时，"
                       f"可临时将 REQUIRE_WITNESS_FOR_FAILOVER 设为 0。",
                       cluster_id=cluster_id)
            print(f"[failover] 切换中止：{detail}")
            return False
        print(f"[failover] 交叉验证通过：{detail}")

    # 2) 提升新主
    ru, rp = node_root(new_h, cluster_id)
    promote_to_master(new_h, new_p, creds["ha_user"], creds["ha_pass"], ru, rp)

    # 3) 通知 keepalived：更新 current_master，并写入各节点本地 ha_local.state
    #    先解除限频+清推送缓存，确保新主立刻到达各节点（否则 VIP 漂移会被推迟）
    reset_topo_throttle(cluster_id)
    set_current_master(cluster_id, new_h, new_p)
    push_current_master_to_nodes(cluster_id, f"{new_h}:{new_p}")

    # 4) 其余存活节点重指向新主（并重试自愈复制冲突）
    for h in hosts:
        if (h["host"], int(h["port"])) == (new_h, new_p):
            continue
        if h["status"] != "up":
            continue  # 宕机节点恢复后由监控自动处理
        _repoint_slave(cluster_id, h, new_h, new_p, creds)

    execute("UPDATE hosts SET is_current_master=0 WHERE cluster_id=%s", (cluster_id,))
    execute("UPDATE hosts SET is_current_master=1 WHERE cluster_id=%s AND host=%s AND port=%s",
            (cluster_id, new_h, new_p))
    execute("INSERT INTO switch_history (from_host, from_port, to_host, to_port, reason, cluster_id) "
            "VALUES (%s,%s,%s,%s,%s,%s)", (cur_h, cur_p, new_h, new_p, reason, cluster_id))

    send_alert("已切换主库",
               f"原因: {reason}\n原主: {cur_h}:{cur_p}\n新主: {new_h}:{new_p}",
               cluster_id=cluster_id)
    return True


@_failover_guarded
def do_failover_to(cluster_id, target_host, target_port, reason="manual: 指定主库"):
    """手动将指定节点提升为主库（保留手动指定 + 自动选延迟最小两种切换方式）。"""
    target_port = int(target_port)
    cur_h, cur_p = get_current_master(cluster_id)
    reset_topo_throttle(cluster_id)   # 手动切换同样放行后续立即修复
    creds = load_creds(cluster_id)
    if not creds["ha_user"]:
        send_alert("切换中止", "未配置 ha_monitor 账号，无法切换。")
        return False
    if (cur_h, int(cur_p or 0)) == (target_host, target_port):
        return True  # 已是主库
    # 1) fencing 旧主（置只读），若已宕机则忽略
    fenced = True
    if cur_h:
        ru, rp = node_root(cur_h, cluster_id)
        try:
            set_read_only(cur_h, int(cur_p), creds["ha_user"], creds["ha_pass"], True, ru, rp)
        except Exception as e:
            fenced = False
            print(f"[failover] 无法连接旧主做 fencing(可能已宕机): {e}")
    # 1.5) 与自动切换同口径：fencing 失败时必须由其它节点作证旧主确实不可达，
    #      否则中止（手动切换同样会造成脑裂双写）。
    if cur_h and not fenced and get_setting("REQUIRE_WITNESS_FOR_FAILOVER", "1") == "1":
        ok_c, detail = confirm_master_down(cluster_id, cur_h, int(cur_p or 0), creds, node_root)
        if not ok_c:
            send_alert("手动切换中止(疑似脑裂风险)",
                       f"无法连接旧主 {cur_h}:{cur_p} 做 fencing，且交叉验证未通过：{detail}。"
                       f"为避免脑裂双写，本次手动切换已中止。请检查管理器到旧主的网络或"
                       f"旧主 max_connections；确需强制执行可将 REQUIRE_WITNESS_FOR_FAILOVER 设为 0。",
                       cluster_id=cluster_id)
            print(f"[failover] 手动切换中止：{detail}")
            return False
        print(f"[failover] 交叉验证通过：{detail}")
    # 2) 提升目标为新主（清 GTID 重置、置可写）
    ru, rp = node_root(target_host, cluster_id)
    ok_m, err_m = promote_to_master(target_host, target_port, creds["ha_user"], creds["ha_pass"], ru, rp)
    if not ok_m:
        send_alert("切换失败", f"无法将 {target_host}:{target_port} 提升为主库：{err_m}",
                   cluster_id=cluster_id)
        return False
    set_current_master(cluster_id, target_host, target_port)
    push_current_master_to_nodes(cluster_id, f"{target_host}:{target_port}")
    # 3) 其余存活节点重指向新主（并重试自愈复制冲突）
    for h in list_hosts(cluster_id):
        if (h["host"], int(h["port"])) == (target_host, target_port):
            continue
        if h["status"] != "up":
            continue
        ru, rp = node_root(h["host"], cluster_id)
        st = get_status(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"], ru, rp)
        was_stale = is_stale_master(st)
        _repoint_slave(cluster_id, h, target_host, target_port, creds)
        if was_stale:
            send_alert("节点重新加入集群",
                       f"节点 {h['host']}:{h['port']} 此前以独立主库运行，"
                       f"现已重新加入为 {target_host}:{target_port} 的从库。")
    execute("UPDATE hosts SET is_current_master=0 WHERE cluster_id=%s", (cluster_id,))
    execute("UPDATE hosts SET is_current_master=1 WHERE cluster_id=%s AND host=%s AND port=%s",
            (cluster_id, target_host, target_port))
    execute("INSERT INTO switch_history (from_host, from_port, to_host, to_port, reason, cluster_id) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (cur_h, cur_p, target_host, target_port, reason, cluster_id))
    send_alert("已手动指定主库", f"原因: {reason}\n新主: {target_host}:{target_port}",
               cluster_id=cluster_id)
    return True


def _selfheal_cluster(cluster_id):
    """对集群内每台存活节点自愈复制关键参数（gtid_mode/enforce_gtid_consistency/server_id）。

    通过 SET PERSIST 写入 mysqld-auto.cnf（重启仍生效），彻底摆脱「裸机 my.cnf 片段被面板/
    重启覆盖导致反复 gtid_mode=OFF、server_id 退回默认值 1」的问题。仅在参数未达标时执行，
    已达标则为空操作，可安全每周期调用。
    """
    creds = load_creds(cluster_id)
    if not creds["ha_user"]:
        return
    hosts = [h for h in list_hosts(cluster_id) if h["status"] == "up"]
    if not hosts:
        return
    # 计算各节点唯一 server_id（处理登记冲突/缺值），并回写 nodes 表保持一致
    want_sid = {}
    used = set()
    for h in hosts:
        row = query("SELECT server_id FROM nodes WHERE cluster_id=%s AND ip=%s AND port=%s",
                    (cluster_id, h["host"], int(h["port"])))
        sid = int(row[0]["server_id"]) if row and row[0].get("server_id") else 0
        if sid and sid not in used:
            used.add(sid)
            want_sid[(h["host"], int(h["port"]))] = sid
        else:
            cand = 2
            while cand in used:
                cand += 1
            used.add(cand)
            want_sid[(h["host"], int(h["port"]))] = cand
            try:
                execute("UPDATE nodes SET server_id=%s WHERE cluster_id=%s AND ip=%s AND port=%s",
                        (cand, cluster_id, h["host"], int(h["port"])))
            except Exception:
                pass
    did_cred = False
    for h in hosts:
        ip, port = h["host"], int(h["port"])
        ru, rp = node_root(ip, cluster_id)
        # 只读副本：replication/app 账号与全局参数(gtid_mode/server_id)来自主库经复制同步，
        # 管理器不应在副本上跑全量 ensure_ha_creds（会与其复制来的同名 DCL 争用元数据锁/漂权限）。
        # 但 ha_monitor/root 是管理器【直连本节点】所需的登录凭证，必须逐节点校正——
        # 否则副本重装后 init SQL 写入的 ha_monitor/root 与库存不一致时，管理器连不上副本 ->
        # 副本永远 "down" -> 不被重指向、不自动加入集群（这正是「重装节点难自动加入」的根因之一）。
        # 故副本仅跑 ensure_connect_creds（只校正 ha_monitor/root，且全程 sql_log_bin=0 不回灌主库）。
        if is_read_only(ip, port, creds["ha_user"], creds["ha_pass"], ru, rp):
            _ts = _SELFHEAL_CRED_TS.get(cluster_id)
            if _ts is None or (time.time() - _ts) >= CRED_SELFHEAL_INTERVAL:
                cok, cmsg = ensure_connect_creds(
                    ip, port, creds["ha_user"], creds["ha_pass"], ru, rp,
                    manager_ip=get_manager_public_ip(),
                    manager_subnet=get_manager_docker_subnet_wildcard())
                did_cred = True
                if not cok:
                    print(f"[selfheal] {ip}:{port}(副本) 连接凭证校正失败: {cmsg}")
            continue
        # 参数自愈限频（见 PARAM_SELFHEAL_INTERVAL 注释）
        _pk = (cluster_id, ip, port)
        if time.monotonic() - _SELFHEAL_PARAM_TS.get(_pk, 0.0) >= PARAM_SELFHEAL_INTERVAL:
            _SELFHEAL_PARAM_TS[_pk] = time.monotonic()
            ok, rep = fix_node_mysql_params(ip, port, ru, rp,
                                            target_server_id=want_sid.get((ip, port)))
            if ok and rep.get("changes"):
                print(f"[selfheal] {ip}:{port} 已自愈: {', '.join(rep['changes'])}")
            elif not ok:
                print(f"[selfheal] {ip}:{port} 自愈失败: {rep.get('error')}")
            for w in (rep.get("warns") or []):
                print(f"[selfheal] {ip}:{port} 警告: {w}")
        # 账号自愈限频：每 CRED_SELFHEAL_INTERVAL 秒最多一次（稳定态 ensure_ha_creds 已幂等
        # 无写操作，限频仅为降低每监控周期对主库建连比对的额外开销）。
        # 优先用 ha_monitor（已知正确）连接，并把 root@'<管理器IP>' 密码校正为库存值，
        # 打破「root 密码漂移 -> 连不上 -> 自愈不跑」死循环。
        # 注意：时间戳在整轮循环结束后统一更新，否则集群里只有第一个节点会被校正。
        _ts = _SELFHEAL_CRED_TS.get(cluster_id)
        if _ts is None or (time.time() - _ts) >= CRED_SELFHEAL_INTERVAL:
            cok, cmsg = ensure_ha_creds(ip, port, ru, rp,
                                        creds["ha_user"], creds["ha_pass"],
                                        creds["repl_user"], creds["repl_pass"],
                                        creds.get("app_user", ""), creds.get("app_pass", ""),
                                        manager_ip=get_manager_public_ip(),
                                        manager_subnet=get_manager_docker_subnet_wildcard())
            did_cred = True
            if not cok:
                print(f"[selfheal] {ip}:{port} 账号自愈失败: {cmsg}")
    if did_cred:
        _SELFHEAL_CRED_TS[cluster_id] = time.time()


def ensure_topology(cluster_id):
    """保证每台存活的'非当前主'库都是当前主的从库（用于切后对齐 + 旧主恢复重入）。"""
    cur_h, cur_p = get_current_master(cluster_id)
    creds = load_creds(cluster_id)
    if not creds["ha_user"]:
        return
    # 先自愈：确保各节点 gtid_mode=ON / server_id 唯一，否则后续 make_slave_of 会因
    # gtid_mode=OFF 反复失败（裸机 my.cnf 片段被覆盖时尤为常见）。SET PERSIST 持久化，
    # 已达标则为空操作。
    try:
        _selfheal_cluster(cluster_id)
    except Exception as e:
        print(f"[monitor] _selfheal_cluster 集群错误(可忽略): {e}")
    # 首次无主：优先选生成时指定为 master 的节点，否则选第一个存活库作为主，
    # 写入 current_master（确保一定有可写主，VIP 才能绑定）
    if not cur_h:
        up = [h for h in list_hosts(cluster_id) if h["status"] == "up"]
        if not up:
            return
        # 选 GTID 进度最先进的存活节点为主（避免落后主 → 其余从库报 1236）
        bh, bp = pick_best_master(cluster_id, up, creds)
        if bh:
            cur_h, cur_p = bh, bp
        else:
            preferred = [h for h in up if (h.get("initial_role") or "slave") == "master"]
            cur_h, cur_p = (preferred[0]["host"], int(preferred[0]["port"])) if preferred \
                else (up[0]["host"], int(up[0]["port"]))
        set_current_master(cluster_id, cur_h, cur_p)
    # 确保被指定为主库的 MySQL 实际可写（read_only=OFF），否则 keepalived VIP 检查不通过
    ru, rp = node_root(cur_h, cluster_id)
    # 用 promote_to_master：不仅置可写，还清空残留复制通道，
    # 避免主库被 monitor 误判为 slave / 形成环形复制。
    # 注意：此处失败绝不能再「可忽略」——主库若仍是 read_only=ON，
    # keepalived 检查脚本会判定本机只读而放弃 VIP，导致 VIP 永不绑定。
    ok, perr = promote_to_master(cur_h, int(cur_p), creds["ha_user"], creds["ha_pass"], ru, rp)
    if not ok:
        errmsg = (f"提升主库 {cur_h}:{cur_p} 为可写失败（VIP 将无法绑定到该节点，请检查账号权限/连接）：{perr}")
        print(f"[topology][严重] {errmsg}")
        # 只 print 到日志文件，生产侧是看不见的：主库若仍是 read_only=ON，
        # 业务经 VIP 写入会直接报 1290（The MariaDB server is running with the
        # --read-only=ON option）。故必须同时告警到管理端 UI（同一主库限频 5 分钟防刷屏）。
        _pk = ("promote_fail", cluster_id, cur_h, int(cur_p))
        if time.monotonic() - _TOPO_AT.get(_pk, 0.0) >= 300:
            _TOPO_AT[_pk] = time.monotonic()
            try:
                send_alert("主库置可写失败", errmsg, cluster_id=cluster_id)
            except Exception:
                pass
    else:
        # 稳态下这行原本每周期必打一次（每 2s 一行），刷屏且掩盖真正的问题。
        # 改为同一主库最多 5 分钟提示一次。
        _pk = ("promote", cluster_id, cur_h, int(cur_p))
        if time.monotonic() - _TOPO_AT.get(_pk, 0.0) >= 300:
            _TOPO_AT[_pk] = time.monotonic()
            print(f"[topology] 主库 {cur_h}:{cur_p} 已置为可写(read_only=OFF)")
    # 把当前主写入各节点本地 ha_local.state（keepalived 检查脚本据此持有/释放 VIP）
    push_current_master_to_nodes(cluster_id, f"{cur_h}:{cur_p}")
    for h in list_hosts(cluster_id):
        if (h["host"], int(h["port"])) == (cur_h, int(cur_p)):
            continue
        if h["status"] != "up":
            continue
        ru, rp = node_root(h["host"], cluster_id)
        st = get_status(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"], ru, rp)
        # 已是当前主的从库且运行正常 -> 跳过
        if (st["master_host"] == cur_h and int(st["master_port"] or 0) == int(cur_p)
                and st["slave_io"] == "Yes" and st["slave_sql"] == "Yes"):
            continue
        # 离线的旧主库重新上线时，仍以「独立主库」形态运行（read_only 可能为 0、
        # 残留自身 GTID）。GTID 自动定位会跳过它已执行过的事务，重入为从库时
        # 不会重放旧事务，因此无需 RESET MASTER（RESET MASTER 反而会因清空 GTID
        # 集合导致同名事务被新主重放而报「已存在」错误）。
        # 拓扑修复限频：同一节点两次重挂复制之间至少间隔 TOPO_REPAIR_INTERVAL。
        # 环境性故障（如复制账号漂移、1200 未配置复制通道）未解决时，原本会每 2s
        # 重挂一次复制，既刷屏又在节点上持续产生复制启停写盘；限频后仍会自愈，
        # 只是不再高频重试。真实切换走 do_failover，会主动清掉限频。
        _rk = ("repoint", cluster_id, h["host"], int(h["port"]))
        if time.monotonic() - _TOPO_AT.get(_rk, 0.0) < TOPO_REPAIR_INTERVAL:
            continue
        _TOPO_AT[_rk] = time.monotonic()
        was_stale_master = is_stale_master(st)
        ok, _rmsg = _repoint_slave(cluster_id, h, cur_h, cur_p, creds)
        if ok:
            msg = f"{h['host']}:{h['port']} 已重指向新主 {cur_h}:{cur_p}"
            if was_stale_master:
                msg = (f"节点 {h['host']}:{h['port']} 此前以独立主库运行（离线后重新上线），"
                       f"现已重新加入为 {cur_h}:{cur_p} 的从库。")
                send_alert("节点重新加入集群", msg)
            print(f"[topology] {msg}")
        else:
            _warn_repoint(h['host'], h['port'], f"{h['host']}:{h['port']} 重指向失败")
        # 自愈：从库 IO 报「匿名事务」1236（主库 binlog 含 gtid_mode=OFF 时期写入的匿名事务，
        # AUTO_POSITION=1 无法复制）。清理主库 binlog 后从库续传。每次重指向后都查一次。
        try:
            st2 = get_status(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"],
                             ru, rp)
            io_err = (st2.get("slave_io_err") or "")
            if "anonymous transaction" in io_err:
                # 注意：主库与从库 root 凭证通常不同，必须用主库的 root 连主库做
                # FLUSH/PURGE；这里取主库 root（cur_h），不可复用从库 ru/rp，否则
                # 主库连接失败、匿名事务自愈失效。
                mru, mrp = node_root(cur_h, cluster_id)
                ok2, msg2 = heal_anonymous_gtid_io(
                    h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"],
                    ru, rp, cur_h, int(cur_p), mru, mrp)
                print(f"[topology] 匿名事务自愈 {h['host']}:{h['port']}: {msg2}")
        except Exception as e:
            print(f"[topology] 匿名事务自愈异常(可忽略): {e}")

    # 收尾校准：以 current_master 表为权威口径，统一 hosts.is_current_master，
    # 避免 keepalived/监控提升或旧主重入后，is_current_master 与 current_master 表长期不一致
    # （典型表现：仪表盘页头主库与节点列表中的当前主对不上；双主/脑裂时也以 current_master 表为准）。
    try:
        cm = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cluster_id,))
        mh = mp = None
        if cm:
            mh, mp = cm[0]["host"], int(cm[0]["port"])
        else:
            rows = query("SELECT host, port, role FROM hosts WHERE cluster_id=%s", (cluster_id,))
            live_masters = [r for r in rows if r["role"] == "master"]
            if len(live_masters) == 1:
                mh, mp = live_masters[0]["host"], int(live_masters[0]["port"])
                set_current_master(cluster_id, mh, mp)
        if mh is not None:
            # 【空闲零写入】仅当 hosts.is_current_master 与 current_master 表不一致时才写。
            # 旧实现每周期无条件执行「全部置 0 + 主库置 1」，稳态下主库那一行会经历
            # 1→0→1 的两次真实行更新，是空闲时管理库持续写盘的来源之一。
            cur = query("SELECT host, port, is_current_master FROM hosts WHERE cluster_id=%s",
                        (cluster_id,))
            wrong = [r for r in cur
                     if (int(r["is_current_master"] or 0) == 1)
                     != (r["host"] == mh and int(r["port"]) == mp)]
            if wrong:
                execute("UPDATE hosts SET is_current_master=0 WHERE cluster_id=%s", (cluster_id,))
                execute("UPDATE hosts SET is_current_master=1 WHERE cluster_id=%s AND host=%s AND port=%s",
                        (cluster_id, mh, mp))
    except Exception as e:
        print(f"[topology] 校准 is_current_master 失败(可忽略): {e}")
