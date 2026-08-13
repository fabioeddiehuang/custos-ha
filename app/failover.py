"""自动/手动故障切换核心逻辑。"""
from .db import query, execute, get_setting
from .state import get_current_master, set_current_master
from .ha_client import set_read_only, promote_to_master, make_slave_of, set_local_master_flag
from .alerts import send_alert


def load_creds():
    return dict(
        ha_user=get_setting("HA_USER"),
        ha_pass=get_setting("HA_PASS"),
        repl_user=get_setting("REPL_USER"),
        repl_pass=get_setting("REPL_PASS"),
    )


def node_root(ip):
    """取节点 MySQL 的 root 凭证（容器创建时固定，作为 ha_monitor 漂移时的回退凭据）。"""
    try:
        row = query("SELECT root_user, root_pass FROM nodes WHERE ip=%s", (ip,))
        if row:
            return (row[0]["root_user"] or "root", row[0]["root_pass"] or "")
    except Exception:
        pass
    return ("root", "")


def list_hosts():
    return query(
        "SELECT host, port, role, status, is_current_master, initial_role, repl_delay "
        "FROM hosts ORDER BY id"
    )


def push_current_master_to_nodes(master_str):
    """把当前主库标识写入每个节点的本地 MySQL（ha_local.state）。

    供 keepalived 检查脚本本地读取判定 VIP 归属；完全本地，无需节点回连管理库。
    单节点失败不影响整体（下个监控周期会重试）。
    """
    creds = load_creds()
    try:
        rows = query("SELECT host, port FROM hosts")
    except Exception:
        return
    for r in rows:
        try:
            ru, rp = node_root(r["host"])
            set_local_master_flag(r["host"], int(r["port"]), creds["ha_user"],
                                  creds["ha_pass"], master_str, ru, rp)
        except Exception:
            pass


def do_failover(reason="auto"):
    """主库判定宕机后执行切换：fencing -> 提升延迟最小从库 -> 更新 current_master -> 其余节点重指向。"""
    cur_h, cur_p = get_current_master()
    creds = load_creds()
    if not cur_h or not creds["ha_user"]:
        send_alert("切换中止", "未配置受管主机或 ha_monitor 账号，无法切换。")
        return False

    hosts = list_hosts()
    # 候选：存活且非当前主，按复制延迟升序
    candidates = [
        h for h in hosts
        if (h["host"], int(h["port"])) != (cur_h, int(cur_p)) and h["status"] == "up"
    ]
    if not candidates:
        send_alert("切换失败", f"主库 {cur_h}:{cur_p} 宕机，但无可用从库可提升。")
        return False
    candidates.sort(key=lambda h: (h["repl_delay"] is None, h["repl_delay"] or 999999))
    new = candidates[0]
    new_h, new_p = new["host"], int(new["port"])

    # 1) fencing：尽量把旧主置只读，使其释放 VIP 并拒绝写入
    ru, rp = node_root(cur_h)
    try:
        set_read_only(cur_h, cur_p, creds["ha_user"], creds["ha_pass"], True, ru, rp)
    except Exception as e:
        print(f"[failover] 无法连接旧主做 fencing(可能已宕机): {e}")

    # 2) 提升新主
    ru, rp = node_root(new_h)
    promote_to_master(new_h, new_p, creds["ha_user"], creds["ha_pass"], ru, rp)

    # 3) 通知 keepalived：更新 current_master，并写入各节点本地 ha_local.state
    set_current_master(new_h, new_p)
    push_current_master_to_nodes(f"{new_h}:{new_p}")

    # 4) 其余存活节点重指向新主
    for h in hosts:
        if (h["host"], int(h["port"])) == (new_h, new_p):
            continue
        if h["status"] != "up":
            continue  # 宕机节点恢复后由监控自动处理
        try:
            ru, rp = node_root(h["host"])
            make_slave_of(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"],
                          new_h, new_p, creds["repl_user"], creds["repl_pass"], ru, rp)
        except Exception as e:
            print(f"[failover] 重指向 {h['host']}:{h['port']} 失败: {e}")

    execute("UPDATE hosts SET is_current_master=0")
    execute("UPDATE hosts SET is_current_master=1 WHERE host=%s AND port=%s", (new_h, new_p))
    execute("INSERT INTO switch_history (from_host, from_port, to_host, to_port, reason) "
            "VALUES (%s,%s,%s,%s,%s)", (cur_h, cur_p, new_h, new_p, reason))

    send_alert("已切换主库",
               f"原因: {reason}\n原主: {cur_h}:{cur_p}\n新主: {new_h}:{new_p}")
    return True


def do_failover_to(target_host, target_port, reason="manual: 指定主库"):
    """手动将指定节点提升为主库（保留手动指定 + 自动选延迟最小两种切换方式）。"""
    target_port = int(target_port)
    cur_h, cur_p = get_current_master()
    creds = load_creds()
    if not creds["ha_user"]:
        send_alert("切换中止", "未配置 ha_monitor 账号，无法切换。")
        return False
    if (cur_h, int(cur_p or 0)) == (target_host, target_port):
        return True  # 已是主库
    # 1) fencing 旧主（置只读），若已宕机则忽略
    if cur_h:
        ru, rp = node_root(cur_h)
        try:
            set_read_only(cur_h, int(cur_p), creds["ha_user"], creds["ha_pass"], True, ru, rp)
        except Exception as e:
            print(f"[failover] 无法连接旧主做 fencing(可能已宕机): {e}")
    # 2) 提升目标为新主（清 GTID 重置、置可写）
    ru, rp = node_root(target_host)
    promote_to_master(target_host, target_port, creds["ha_user"], creds["ha_pass"], ru, rp)
    set_current_master(target_host, target_port)
    push_current_master_to_nodes(f"{target_host}:{target_port}")
    # 3) 其余存活节点重指向新主
    for h in list_hosts():
        if (h["host"], int(h["port"])) == (target_host, target_port):
            continue
        if h["status"] != "up":
            continue
        try:
            ru, rp = node_root(h["host"])
            make_slave_of(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"],
                          target_host, target_port, creds["repl_user"], creds["repl_pass"], ru, rp)
        except Exception as e:
            print(f"[failover] 重指向 {h['host']}:{h['port']} 失败: {e}")
    execute("UPDATE hosts SET is_current_master=0")
    execute("UPDATE hosts SET is_current_master=1 WHERE host=%s AND port=%s",
            (target_host, target_port))
    execute("INSERT INTO switch_history (from_host, from_port, to_host, to_port, reason) "
            "VALUES (%s,%s,%s,%s,%s)",
            (cur_h, cur_p, target_host, target_port, reason))
    send_alert("已手动指定主库", f"原因: {reason}\n新主: {target_host}:{target_port}")
    return True


def ensure_topology():
    """保证每台存活的'非当前主'库都是当前主的从库（用于切后对齐 + 旧主恢复重入）。"""
    cur_h, cur_p = get_current_master()
    creds = load_creds()
    if not creds["ha_user"]:
        return
    # 首次无主：优先选生成时指定为 master 的节点，否则选第一个存活库作为主，
    # 写入 current_master（确保一定有可写主，VIP 才能绑定）
    if not cur_h:
        up = [h for h in list_hosts() if h["status"] == "up"]
        if not up:
            return
        preferred = [h for h in up if (h.get("initial_role") or "slave") == "master"]
        pick = preferred[0] if preferred else up[0]
        cur_h, cur_p = pick["host"], int(pick["port"])
        set_current_master(cur_h, cur_p)
    # 确保被指定为主库的 MySQL 实际可写（read_only=OFF），否则 keepalived VIP 检查不通过
    ru, rp = node_root(cur_h)
    try:
        # 用 promote_to_master：不仅置可写，还清空残留复制通道，
        # 避免主库被 monitor 误判为 slave / 形成环形复制
        promote_to_master(cur_h, int(cur_p), creds["ha_user"], creds["ha_pass"], ru, rp)
    except Exception as e:
        print(f"[topology] 设置主库可写出错(可忽略): {e}")
    # 把当前主写入各节点本地 ha_local.state（keepalived 检查脚本据此持有/释放 VIP）
    push_current_master_to_nodes(f"{cur_h}:{cur_p}")
    from ha_client import get_status
    for h in list_hosts():
        if (h["host"], int(h["port"])) == (cur_h, int(cur_p)):
            continue
        if h["status"] != "up":
            continue
        ru, rp = node_root(h["host"])
        st = get_status(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"], ru, rp)
        # 已是当前主的从库且运行正常 -> 跳过
        if (st["master_host"] == cur_h and int(st["master_port"] or 0) == int(cur_p)
                and st["slave_io"] == "Yes" and st["slave_sql"] == "Yes"):
            continue
        try:
            make_slave_of(h["host"], int(h["port"]), creds["ha_user"], creds["ha_pass"],
                          cur_h, cur_p, creds["repl_user"], creds["repl_pass"], ru, rp)
            print(f"[topology] 已将 {h['host']}:{h['port']} 重指向新主 {cur_h}:{cur_p}")
        except Exception as e:
            print(f"[topology] {h['host']}:{h['port']} 重指向失败: {e}")
