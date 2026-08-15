"""集群（多集群）数据访问与凭证管理。

每个集群是一组 MySQL 主从节点，拥有自己独立的管理账号(ha_monitor)/复制账号
(replication)/业务账号(app) 及对应密码，以及独立的 VIP / 网卡 / VRID。切换与
监控均按集群隔离进行。
"""
from .db import query, execute, get_setting
from .config import rand_secret, VIP, VIP_PREFIX, INTERFACE, VRID, VIP_PORT


def list_clusters():
    return query("SELECT * FROM clusters ORDER BY id")


def get_cluster(cluster_id):
    rows = query("SELECT * FROM clusters WHERE id=%s", (cluster_id,))
    return rows[0] if rows else None


def create_cluster(name, ha_user=None, ha_pass=None, repl_user=None, repl_pass=None,
                   app_user=None, app_pass=None, vip=None, vip_prefix=None,
                   interface=None, vrid=None, vip_port=None):
    name = (name or "").strip() or "未命名集群"
    ha_user = ha_user or "ha_monitor"
    ha_pass = ha_pass or rand_secret(16)
    repl_user = repl_user or "replication"
    repl_pass = repl_pass or rand_secret(16)
    app_user = app_user or "app"
    app_pass = app_pass or rand_secret(16)
    vip_prefix = vip_prefix or VIP_PREFIX
    interface = interface or INTERFACE
    vrid = vrid or VRID
    if vip_port is None:
        vip_port = VIP_PORT
    execute(
        """INSERT INTO clusters
        (name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
         vip, vip_prefix, interface, vrid, vip_port)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
         vip, vip_prefix, interface, vrid, int(vip_port)),
    )
    rows = query("SELECT LAST_INSERT_ID() AS id")
    return rows[0]["id"] if rows else None


def update_cluster(cluster_id, **fields):
    allowed = {"name", "ha_user", "ha_pass", "repl_user", "repl_pass", "app_user",
               "app_pass", "vip", "vip_prefix", "interface", "vrid", "vip_port"}
    sets, args = [], []
    for k, v in fields.items():
        if k in allowed and v is not None:
            sets.append(f"{k}=%s")
            args.append(v)
    if not sets:
        return
    args.append(cluster_id)
    execute(f"UPDATE clusters SET {', '.join(sets)} WHERE id=%s", args)


def delete_cluster(cluster_id):
    """删除集群及其下节点部署单元、主机与当前主记录。"""
    from .node_gen import delete_node_dir
    nodes = query("SELECT ip FROM nodes WHERE cluster_id=%s", (cluster_id,))
    for n in nodes:
        try:
            delete_node_dir(n["ip"])
        except Exception:
            pass
    execute("DELETE FROM current_master WHERE cluster_id=%s", (cluster_id,))
    execute("DELETE FROM hosts WHERE cluster_id=%s", (cluster_id,))
    execute("DELETE FROM nodes WHERE cluster_id=%s", (cluster_id,))
    execute("DELETE FROM clusters WHERE id=%s", (cluster_id,))


def get_cluster_creds(cluster_id):
    c = get_cluster(cluster_id)
    if not c:
        return None
    return dict(
        ha_user=c.get("ha_user") or "ha_monitor",
        ha_pass=c.get("ha_pass") or "",
        repl_user=c.get("repl_user") or "replication",
        repl_pass=c.get("repl_pass") or "",
        app_user=c.get("app_user") or "app",
        app_pass=c.get("app_pass") or "",
        vip=c.get("vip"),
        vip_prefix=c.get("vip_prefix") or VIP_PREFIX,
        interface=c.get("interface") or INTERFACE,
        vrid=c.get("vrid") or VRID,
        vip_port=c.get("vip_port") or VIP_PORT,
    )


def list_hosts(cluster_id):
    return query(
        "SELECT * FROM hosts WHERE cluster_id=%s "
        "ORDER BY is_current_master DESC, host",
        (cluster_id,),
    )


def list_nodes(cluster_id):
    return query(
        "SELECT n.ip, n.port, n.root_user, n.server_id, n.compose_path, n.created_at, "
        "h.initial_role, h.role, h.status "
        "FROM nodes n LEFT JOIN hosts h ON h.host = n.ip AND h.cluster_id=%s "
        "WHERE n.cluster_id=%s ORDER BY n.ip",
        (cluster_id, cluster_id),
    )


def get_default_cluster():
    """返回首个集群（id 最小），新建对象缺省归入此集群。"""
    rows = list_clusters()
    return rows[0] if rows else None
