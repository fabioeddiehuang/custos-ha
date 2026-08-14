"""自动/手动故障切换核心逻辑（多集群：所有操作均按 cluster_id 隔离）。"""
from .db import query, execute
from .state import get_current_master, set_current_master
from .ha_client import (set_read_only, promote_to_master, make_slave_of,
                        set_local_master_flag, is_stale_master, get_status,
                        heal_replication, heal_io_gtid_gap, get_gtid_executed,
                        parse_gtid_set)
import time
from .alerts import send_alert
from .clusters import get_cluster_creds


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
    """
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
    """
    host, port = h["host"], int(h["port"])
    ru, rp = node_root(host, cluster_id)
    ok, err = make_slave_of(host, port, creds["ha_user"], creds["ha_pass"],
                            target_host, int(target_port), creds["repl_user"],
                            creds["repl_pass"], ru, rp)
    if not ok:
        print(f"[failover] 重指向 {host}:{port} 失败: {err}")
        return False
    for _ in range(8):
        try:
            st = get_status(host, port, creds["ha_user"], creds["ha_pass"], ru, rp)
        except Exception:
            st = {}
        if st.get("slave_io") == "Yes" and st.get("slave_sql") == "Yes" and not st.get("error"):
            break
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
        time.sleep(2)
    return True


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
    try:
        set_read_only(cur_h, cur_p, creds["ha_user"], creds["ha_pass"], True, ru, rp)
    except Exception as e:
        print(f"[failover] 无法连接旧主做 fencing(可能已宕机): {e}")

    # 2) 提升新主
    ru, rp = node_root(new_h, cluster_id)
    promote_to_master(new_h, new_p, creds["ha_user"], creds["ha_pass"], ru, rp)

    # 3) 通知 keepalived：更新 current_master，并写入各节点本地 ha_local.state
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


def do_failover_to(cluster_id, target_host, target_port, reason="manual: 指定主库"):
    """手动将指定节点提升为主库（保留手动指定 + 自动选延迟最小两种切换方式）。"""
    target_port = int(target_port)
    cur_h, cur_p = get_current_master(cluster_id)
    creds = load_creds(cluster_id)
    if not creds["ha_user"]:
        send_alert("切换中止", "未配置 ha_monitor 账号，无法切换。")
        return False
    if (cur_h, int(cur_p or 0)) == (target_host, target_port):
        return True  # 已是主库
    # 1) fencing 旧主（置只读），若已宕机则忽略
    if cur_h:
        ru, rp = node_root(cur_h, cluster_id)
        try:
            set_read_only(cur_h, int(cur_p), creds["ha_user"], creds["ha_pass"], True, ru, rp)
        except Exception as e:
            print(f"[failover] 无法连接旧主做 fencing(可能已宕机): {e}")
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


def ensure_topology(cluster_id):
    """保证每台存活的'非当前主'库都是当前主的从库（用于切后对齐 + 旧主恢复重入）。"""
    cur_h, cur_p = get_current_master(cluster_id)
    creds = load_creds(cluster_id)
    if not creds["ha_user"]:
        return
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
    try:
        # 用 promote_to_master：不仅置可写，还清空残留复制通道，
        # 避免主库被 monitor 误判为 slave / 形成环形复制
        promote_to_master(cur_h, int(cur_p), creds["ha_user"], creds["ha_pass"], ru, rp)
    except Exception as e:
        print(f"[topology] 设置主库可写出错(可忽略): {e}")
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
        was_stale_master = is_stale_master(st)
        ok = _repoint_slave(cluster_id, h, cur_h, cur_p, creds)
        if ok:
            msg = f"{h['host']}:{h['port']} 已重指向新主 {cur_h}:{cur_p}"
            if was_stale_master:
                msg = (f"节点 {h['host']}:{h['port']} 此前以独立主库运行（离线后重新上线），"
                       f"现已重新加入为 {cur_h}:{cur_p} 的从库。")
                send_alert("节点重新加入集群", msg)
            print(f"[topology] {msg}")
        else:
            print(f"[topology] {h['host']}:{h['port']} 重指向失败")

    # 收尾校准：以 current_master 表为权威口径，统一 hosts.is_current_master，
    # 避免 keepalived/监控提升或旧主重入后，is_current_master 与 current_master 表长期不一致
    # （典型表现：仪表盘页头主库与节点列表中的当前主对不上；双主/脑裂时也以 current_master 表为准）。
    try:
        cm = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cluster_id,))
        if cm:
            mh, mp = cm[0]["host"], int(cm[0]["port"])
            execute("UPDATE hosts SET is_current_master=0 WHERE cluster_id=%s", (cluster_id,))
            execute("UPDATE hosts SET is_current_master=1 WHERE cluster_id=%s AND host=%s AND port=%s",
                    (cluster_id, mh, mp))
        else:
            rows = query("SELECT host, port, role FROM hosts WHERE cluster_id=%s", (cluster_id,))
            live_masters = [r for r in rows if r["role"] == "master"]
            if len(live_masters) == 1:
                mh, mp = live_masters[0]["host"], int(live_masters[0]["port"])
                set_current_master(cluster_id, mh, mp)
                execute("UPDATE hosts SET is_current_master=0 WHERE cluster_id=%s", (cluster_id,))
                execute("UPDATE hosts SET is_current_master=1 WHERE cluster_id=%s AND host=%s AND port=%s",
                        (cluster_id, mh, mp))
    except Exception as e:
        print(f"[topology] 校准 is_current_master 失败(可忽略): {e}")
