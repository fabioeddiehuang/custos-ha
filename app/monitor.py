"""后台监控循环：探活、更新拓扑、触发自动切换、维护拓扑对齐、状态告警（多集群）。"""
import time
from .config import CHECK_INTERVAL, FAIL_THRESHOLD
from .db import query, execute
from .ha_client import get_status
from .state import get_current_master
from .failover import do_failover, ensure_topology, load_creds, node_root
from .alerts import send_alert
from .clusters import list_clusters

# 复制延迟告警阈值（秒）。超过即发邮件告警，避免重复刷屏只在「跨阈值的那一刻」触发。
REPL_LAG_ALERT_SEC = 30
# 各节点上一次观测状态（用于状态跃迁告警，避免每个周期重复刷屏）
_PREV = {}


def _update_host(h, st):
    execute(
        """UPDATE hosts SET status=%s, role=%s, repl_delay=%s, slave_io=%s, slave_sql=%s,
           err=%s, last_seen=NOW() WHERE cluster_id=%s AND host=%s AND port=%s""",
        (("up" if st["alive"] else "down"), st["role"],
         st["seconds_behind"], st["slave_io"], st["slave_sql"],
         None if st["alive"] else (st.get("error") or "探活失败"),
         h["cluster_id"], h["host"], int(h["port"])),
    )


def _alert_on_transition(key, status, is_master, repl_ok, delay, cluster_name):
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
                send_alert("节点离线", f"{tag}节点 {host} 已离线，监控连续探活失败。若为主库将触发自动切换。")
            else:
                send_alert("节点恢复在线", f"{tag}节点 {host} 已恢复在线，若为旧主将自动重新加入集群为从库。")
        # 2) 复制中断（仅非主库且节点在线时；节点离线属于「节点离线」告警，不重复报）
        if status == "up" and not is_master and prev["repl_ok"] and not repl_ok:
            send_alert("复制中断", f"{tag}节点 {host} 复制异常（Slave_IO 或 Slave_SQL 非 Yes），请检查复制链路。")
        # 3) 复制延迟过高（仅在「跨过阈值」那一刻告警，且仅在线节点）
        if status == "up" and not is_master and delay is not None:
            crossed = delay >= REPL_LAG_ALERT_SEC and (
                prev["delay"] is None or prev["delay"] < REPL_LAG_ALERT_SEC)
            if crossed:
                send_alert("复制延迟过高", f"{tag}节点 {host} 复制延迟 {delay}s，已超过阈值 {REPL_LAG_ALERT_SEC}s。")
    except Exception as e:
        print(f"[monitor] 发送告警失败(可忽略): {e}")


def run_once():
    clusters = list_clusters()
    if not clusters:
        return  # 尚未创建任何集群，空闲等待
    for cl in clusters:
        cid = cl["id"]
        cluster_name = cl.get("name") or f"集群{cid}"
        creds = load_creds(cid)
        if not creds["ha_user"]:
            continue  # 该集群尚未配置受管账号，跳过
        # 每个集群独立的连续失败计数（避免不同集群互相干扰）
        fails = run_once._fails.setdefault(cid, {})
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
            ru, rp = node_root(h["host"], cid)
            st = get_status(h["host"], port, creds["ha_user"], creds["ha_pass"], ru, rp, timeout=5)
            _update_host(h, st)
            key = (h["host"], port, cid)
            is_master = (key[0], key[1]) == cur_key
            if not st["alive"]:
                fails[key] = fails.get(key, 0) + 1
            else:
                fails[key] = 0
            # 复制是否正常（主库无复制通道，视为主库自身即「正常」）
            repl_ok = is_master or (st["slave_io"] == "Yes" and st["slave_sql"] == "Yes")
            _alert_on_transition(key, "up" if st["alive"] else "down", is_master, repl_ok,
                                 st["seconds_behind"], cluster_name)

            # 当前主库连续失败达到阈值 -> 自动切换
            if is_master and fails[key] >= FAIL_THRESHOLD:
                print(f"[monitor] 集群[{cluster_name}] 主库 {cur_h}:{cur_p} "
                      f"连续 {fails[key]} 次探活失败，触发自动切换")
                if do_failover(cid, "auto: master unreachable"):
                    run_once._fails[cid] = {}  # 重置，避免重复触发

        # 维护拓扑：切后对齐 + 旧主恢复重入
        try:
            ensure_topology(cid)
        except Exception as e:
            print(f"[monitor] ensure_topology 集群[{cluster_name}] 错误(可忽略): {e}")


def run_forever():
    print("[monitor] 监控线程启动")
    while True:
        try:
            run_once()
        except Exception as e:
            # 管理库不可用等：降级，仅记录，不影响业务(keepalived 有安全回退)
            print(f"[monitor] 周期错误(已降级继续): {e}")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_forever()
