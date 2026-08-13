"""后台监控循环：探活、更新拓扑、触发自动切换、维护拓扑对齐。"""
import time
from .config import CHECK_INTERVAL, FAIL_THRESHOLD
from .db import query, execute, get_conn
from .ha_client import get_status
from .state import get_current_master
from .failover import do_failover, ensure_topology, load_creds, node_root


def _update_host(h, st):
    execute(
        """UPDATE hosts SET status=%s, role=%s, repl_delay=%s, slave_io=%s, slave_sql=%s,
           err=%s, last_seen=NOW() WHERE host=%s AND port=%s""",
        (("up" if st["alive"] else "down"), st["role"],
         st["seconds_behind"], st["slave_io"], st["slave_sql"],
         None if st["alive"] else (st.get("error") or "探活失败"),
         h["host"], int(h["port"])),
    )


def run_once():
    creds = load_creds()
    if not creds["ha_user"]:
        return  # 尚未配置受管账号，空闲等待
    hosts = query("SELECT host, port FROM hosts")
    cur_h, cur_p = get_current_master()
    fails = run_once._fails if hasattr(run_once, "_fails") else {}
    run_once._fails = fails

    for h in hosts:
        # 兼容历史数据：hosts.port 可能为 NULL/非法，跳过避免 int() 崩溃
        try:
            port = int(h["port"])
        except (TypeError, ValueError):
            print(f"[monitor] 跳过无效主机 {h['host']}（port 缺失/非法：{h['port']!r}）")
            continue
        ru, rp = node_root(h["host"])
        st = get_status(h["host"], port, creds["ha_user"], creds["ha_pass"], ru, rp, timeout=5)
        _update_host(h, st)
        key = (h["host"], port)
        if not st["alive"]:
            fails[key] = fails.get(key, 0) + 1
        else:
            fails[key] = 0

        # 当前主库连续失败达到阈值 -> 自动切换
        if key == (cur_h, int(cur_p)) and fails[key] >= FAIL_THRESHOLD:
            print(f"[monitor] 主库 {cur_h}:{cur_p} 连续 {fails[key]} 次探活失败，触发自动切换")
            if do_failover("auto: master unreachable"):
                fails[key] = 0
                run_once._fails = {}  # 重置，避免重复触发
                return

    # 维护拓扑：切后对齐 + 旧主恢复重入
    try:
        ensure_topology()
    except Exception as e:
        print(f"[monitor] ensure_topology 错误(可忽略): {e}")


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
