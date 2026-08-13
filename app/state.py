"""当前主库状态（管理库 current_master 表）。keepalived 的 vrrp_script 会读这张表决定 VIP 归属。"""
from .db import query, execute


def get_current_master():
    rows = query("SELECT host, port FROM current_master WHERE id=1")
    if rows:
        return rows[0]["host"], int(rows[0]["port"])
    return None, None


def set_current_master(host, port):
    execute(
        "INSERT INTO current_master (id, host, port) VALUES (1, %s, %s) "
        "ON DUPLICATE KEY UPDATE host=%s, port=%s",
        (host, port, host, port),
    )
