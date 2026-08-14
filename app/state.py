"""当前主库状态（管理库 current_master 表）。keepalived 的 vrrp_script 会读这张表决定 VIP 归属。

多集群后，current_master 以 cluster_id 为主键——每个集群各自维护一台当前主库。
"""


from .db import query, execute


def get_current_master(cluster_id):
    rows = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cluster_id,))
    if rows:
        return rows[0]["host"], int(rows[0]["port"])
    return None, None


def set_current_master(cluster_id, host, port):
    execute(
        "INSERT INTO current_master (cluster_id, host, port) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE host=%s, port=%s",
        (cluster_id, host, port, host, port),
    )
