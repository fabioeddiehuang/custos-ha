"""mysql_compat.py — v2.2 跨风味复制 SQL 兼容层。

v2.0 Docker(MySQL 8.0.x) 集群在复制 SQL 上保持「字节级不变」；
v2.1 新增对 8.0.22+/8.4 的 REPLICATION SOURCE/REPLICA 新语法；
v2.2 新增 MariaDB 12.x 裸机独立路径（脱离宝塔），与 MySQL 双路径完全独立。

双路径根本不兼容：
- MySQL 8.0：gtid_mode=ON + UUID GTID 域，CHANGE REPLICATION SOURCE ...
  SOURCE_AUTO_POSITION=1 [+ ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL]
- MariaDB 12.x：gtid_domain_id + 基于 server_id 的序列 GTID（形如 0-1-100），
  CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos（**不是** AUTO_POSITION=1）

判定规则：
- 存储的 mysql_version 为 NULL（老集群）或不传 → 一律按旧语法 (CHANGE MASTER / SHOW SLAVE)。
- 实际节点连接：用 detect_flavour() 按 SELECT VERSION() 判定，最可靠。
  >= 8.0.22（含 8.1/8.2/8.3/8.4）走 mysql8 新语法；MariaDB 走 mariadb 分支；
  其余 < 8.0.22 走 old 语法。

本模块只「生成 SQL 字符串」，执行交给调用方用 text() 包一层，避免重复依赖。
"""
import re

# 新复制术语 (CHANGE REPLICATION SOURCE / SHOW REPLICA) 在 8.0.22 引入
_NEW_SYNTAX_MIN = (8, 0, 22)

# MySQL 8.0.34 起 mysql_native_password 被标记废弃；8.4 起默认不再启用
_CSHA2_MIN = (8, 0, 34)

# 风味常量
F_MYSQL8 = "mysql8"
F_MARIADB = "mariadb"
F_OLD = "old"


def parse_version(version_str):
    """'8.0.36-cluster' -> (8,0,36)；'12.3.3-MariaDB' -> (12,3,3)；无法解析返回 (0,0,0)。"""
    if not version_str:
        return (0, 0, 0)
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)", version_str)
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def is_mariadb(version_str):
    return "mariadb" in (version_str or "").lower()


# 节点部署形态常量（nodes.node_type / clusters.node_type）
NT_DOCKER = "docker"                  # 容器化 MySQL 8.0
NT_MARIADB = "mariadb"                # 裸机 MariaDB（Install 脚本全量安装）
NT_MARIADB_DOCKER = "mariadb-docker"  # 容器化 MariaDB（官方镜像 + Install 脚本）

# 引擎族：同为 MariaDB 的两种形态可以同集群混部（复制/切换完全兼容），
# 与 MySQL 之间仍然禁止混部（GTID 机制根本不兼容）。
_MARIADB_NODE_TYPES = (NT_MARIADB, NT_MARIADB_DOCKER)


def is_mariadb_node(node_type):
    """node_type 是否属于 MariaDB 引擎族（裸机 MariaDB / 容器化 MariaDB）。"""
    return (node_type or "").strip().lower() in _MARIADB_NODE_TYPES


def node_family(node_type):
    """把部署形态归一到引擎族：mariadb / mysql / 原值。用于同集群「同构」校验。"""
    nt = (node_type or "").strip().lower()
    if nt in _MARIADB_NODE_TYPES:
        return "mariadb"
    if nt in (NT_DOCKER, "mysql", "mysql-docker"):
        return "mysql"
    return nt


def use_new_syntax(version_str=None):
    """按存储的 mysql_version 判定（None/空=旧语法，兼容 v2.0 老集群）。

    MariaDB 必须走 «old» 语法（术语层面），复制挂载逻辑另由 flavour 分支处理。
    """
    if not version_str:
        return False
    if is_mariadb(version_str):
        return False
    return parse_version(version_str) >= _NEW_SYNTAX_MIN


def get_syntax(flavour):
    """把 flavour 映射回术语语法（new/old），供 SHOW/START/STOP/RESET 复用。

    MariaDB 在术语层面使用 old（MASTER/SLAVE/SHOW SLAVE STATUS），但挂载逻辑不同。
    """
    if flavour == F_MYSQL8:
        return "new"
    return "old"


def detect_syntax(cur):
    """给定节点游标，按 SELECT VERSION() 判定术语（new/old）。保留旧接口。"""
    try:
        cur.execute("SELECT VERSION()")
        row = cur.fetchone()
        v = (row[0] if row else "") or ""
        if is_mariadb(v):
            return "old"
        return "new" if parse_version(v) >= _NEW_SYNTAX_MIN else "old"
    except Exception:
        return "old"


def detect_flavour(cur):
    """给定节点游标，按 SELECT VERSION() 判定风味（mysql8/mariadb/old）。

    这是 v2.2 新增的核心分派函数：MariaDB 与 MySQL 走各自独立的复制路径。
    """
    try:
        cur.execute("SELECT VERSION()")
        row = cur.fetchone()
        v = (row[0] if row else "") or ""
        return flavour_from_version(v)
    except Exception:
        return F_OLD


def flavour_from_version(version_str):
    if is_mariadb(version_str):
        return F_MARIADB
    if parse_version(version_str) >= _NEW_SYNTAX_MIN:
        return F_MYSQL8
    return F_OLD


def default_auth_plugin(version_str=None, node_type=None, prefer="auto"):
    """返回节点账号应使用的默认认证插件名。

    背景：MySQL 8.0.34 起 ``mysql_native_password`` 被标记废弃，任何以该插件认证
    的连接都会在 error log 写一条
    ``[Warning] [MY-013360] Plugin mysql_native_password reported: '...is deprecated...'``；
    8.4 起默认不再加载该插件。故对新版本改用 ``caching_sha2_password``。

    规则（prefer 可强制覆盖，取值 auto / native / caching_sha2）：
      - MariaDB         -> mysql_native_password（MariaDB 自带，无废弃告警）
      - MySQL >= 8.0.34 -> caching_sha2_password
      - MySQL < 8.0.34 / 5.7 / 版本未知 -> mysql_native_password
        （8.0 之前根本没有 caching_sha2；版本未知时沿用既有行为，避免误伤老集群）
    """
    p = (prefer or "auto").strip().lower()
    if p in ("caching_sha2", "caching_sha2_password", "sha2"):
        return "caching_sha2_password"
    if p in ("native", "mysql_native_password"):
        return "mysql_native_password"
    if not version_str:
        # 版本未知：MariaDB 节点（裸机/容器）可凭 node_type 判定；其余沿用既有行为
        if is_mariadb_node(node_type):
            return "mysql_native_password"
        # docker 节点固定 MySQL 8.0，但小版本未知（可能 < 8.0.34），不敢冒进改插件
        return "mysql_native_password"
    if is_mariadb(version_str):
        return "mysql_native_password"
    if parse_version(version_str) >= _CSHA2_MIN:
        return "caching_sha2_password"
    return "mysql_native_password"


def identify_clause(plugin, password, mariadb=False):
    """生成 CREATE/ALTER USER 的认证子句（明文密码）。

    MariaDB 10.6 不支持 ``IDENTIFIED VIA <plugin> BY '明文'``，必须用
    ``IDENTIFIED VIA <plugin> USING PASSWORD('明文')``。
    """
    if mariadb:
        return "IDENTIFIED VIA %s USING PASSWORD('%s')" % (plugin, password)
    return "IDENTIFIED WITH %s BY '%s'" % (plugin, password)


def flavour_from_node_type(node_type):
    """把库里的 node_type 映射到 flavour（无连库时可用，作为 fallback）。

    node_type: docker(MySQL8 容器) / mariadb(MariaDB 裸机) / mariadb-docker(MariaDB 容器)
    注：docker 节点固定为 MySQL 8.0；两种 MariaDB 形态都是 MariaDB（独立 GTID 机制，与 MySQL 不兼容）。
    真实 flavour 仍建议连库 detect_flavour 确认。
    """
    nt = (node_type or "").lower()
    if nt in _MARIADB_NODE_TYPES:
        return F_MARIADB
    if nt == NT_DOCKER:
        return F_MYSQL8
    return F_OLD


def _ch(channel):
    return f" FOR CHANNEL '{channel}'" if channel else ""


def change_replication_sql(flavour, host, port, user, password, channel=None,
                           public_key=False, assign_anon_gtids=False):
    """生成挂载复制通道的 SQL（按 flavour 独立分派）。

    MySQL 8.0 (flavour=mysql8):
        CHANGE REPLICATION SOURCE TO SOURCE_HOST=..., SOURCE_AUTO_POSITION=1
        [+ SOURCE_PUBLIC_KEY=1] [+ ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL]
    MariaDB (flavour=mariadb):
        CHANGE MASTER TO MASTER_HOST=..., MASTER_USE_GTID=slave_pos, MASTER_SSL=0
        —— 不使用 AUTO_POSITION（MariaDB 无此概念，用基于 gtid_domain_id 的 slave_pos）。
        —— MASTER_SSL=0 是必须的：见下方函数内注释（WolfSSL/OpenSSL 互操作问题）。
    Old (flavour=old, MySQL 5.7 等):
        CHANGE MASTER TO MASTER_HOST=..., MASTER_AUTO_POSITION=1
        [+ MASTER_PUBLIC_KEY=1]

    public_key=True 时追加 *_PUBLIC_KEY=1 —— MySQL 8.0 caching_sha2_password 非 TLS 复制的
    官方修复（让从库索取服务端公钥）。MariaDB / 旧版也识别 MASTER_PUBLIC_KEY=1。

    assign_anon_gtids=True 仅 MySQL 8.0 追加 ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL：
    根治反复切换主从时的 1236（源端旧 binlog 含匿名事务）。MariaDB GTID 机制不同，无需此选项。
    """
    if flavour == F_MYSQL8:
        pk = "SOURCE_PUBLIC_KEY=1, " if public_key else ""
        anon = "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL, " if assign_anon_gtids else ""
        return (f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='{host}', SOURCE_PORT={port}, "
                f"SOURCE_USER='{user}', SOURCE_PASSWORD='{password}', "
                f"{pk}{anon}SOURCE_AUTO_POSITION=1{_ch(channel)}")
    if flavour == F_MARIADB:
        # MariaDB：用 MASTER_USE_GTID=slave_pos 走 GTID 复制（自动定位，无需 binlog 坐标）。
        # 注意：MariaDB（尤其 12.x）的 CHANGE MASTER TO 不支持 MASTER_PUBLIC_KEY 参数，
        # 且 MariaDB 使用 mysql_native_password（非 MySQL8 的 caching_sha2_password），
        # 非 TLS 复制也不需要索取服务端公钥，故 public_key 参数对 MariaDB 一律忽略。
        #
        # MASTER_SSL=0 必须显式写：MariaDB 官方 tarball/发行包（裸机安装路径）用的是
        # **WolfSSL**，而 Docker Hub 官方镜像（mariadb:12.3.3-ubi / 12.3.2）用的是 **OpenSSL**。
        # WolfSSL 客户端 → OpenSSL 服务端的 TLS 握手会失败，IO 线程一直 Connecting 并报
        #     error connecting to master 'replication@x:3306' ... message: SSL connection error: ok
        # 反过来（容器节点做从库、裸机节点做主库）却完全正常，所以典型现象是
        # 「容器节点一旦被切成主库，所有裸机节点立刻连不上」。
        # 内网复制无需 TLS，直接关掉绕开这个非对称互操作问题。
        return (f"CHANGE MASTER TO MASTER_HOST='{host}', MASTER_PORT={port}, "
                f"MASTER_USER='{user}', MASTER_PASSWORD='{password}', "
                f"MASTER_USE_GTID=slave_pos, MASTER_SSL=0{_ch(channel)}")
    # old (MySQL 5.7 等 legacy)
    pk = "MASTER_PUBLIC_KEY=1, " if public_key else ""
    return (f"CHANGE MASTER TO MASTER_HOST='{host}', MASTER_PORT={port}, "
            f"MASTER_USER='{user}', MASTER_PASSWORD='{password}', "
            f"{pk}MASTER_AUTO_POSITION=1{_ch(channel)}")


def start_replication_sql(flavour, channel=None):
    syn = get_syntax(flavour)
    return ("START REPLICA" if syn == "new" else "START SLAVE") + _ch(channel)


def stop_replication_sql(flavour, channel=None):
    syn = get_syntax(flavour)
    return ("STOP REPLICA" if syn == "new" else "STOP SLAVE") + _ch(channel)


def reset_replica_sql(flavour, all_=True, channel=None):
    syn = get_syntax(flavour)
    s = "RESET REPLICA" if syn == "new" else "RESET SLAVE"
    if all_:
        s += " ALL"
    return s + _ch(channel)


def show_replica_status_sql(flavour, channel=None):
    syn = get_syntax(flavour)
    return ("SHOW REPLICA STATUS" if syn == "new" else "SHOW SLAVE STATUS") + _ch(channel)


def show_replicas_sql(flavour):
    syn = get_syntax(flavour)
    return "SHOW REPLICAS" if syn == "new" else "SHOW SLAVE HOSTS"


def change_replication_filter_sql(ignore_dbs, channel=None, flavour=None):
    """动态复制过滤 REPLICATE_IGNORE_DB。**仅 MySQL 支持**，MariaDB 返回 None。

    【重要更正】此前注释称「MariaDB 12.x 为无空格写法」是**错的**：MariaDB **根本
    没有** `CHANGE REPLICATION FILTER` 语句（动态复制过滤是 MySQL 独有；MariaDB 的
    `replicate_ignore_db` 只能写在配置文件里、不可运行期设置）。结果 MariaDB 节点上
    每次 `make_slave_of` 都恒报 1064：
        You have an error in your SQL syntax ... near 'REPLICATION FILTER REPLICATE_IGNORE_DB=(ha_local)'
    虽被 except 兜住不影响主流程，但日志刷屏且过滤器其实从未生效。

    MariaDB 侧无需该兜底：`ha_local` 的写入已用 `SET SESSION sql_log_bin=0`
    （见 `set_local_master_flag`），本就不会进 binlog、不会被复制。
    """
    if flavour == F_MARIADB:
        return None
    dbs = ",".join(f"({d})" for d in (ignore_dbs or []))
    return f"CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = {dbs}{_ch(channel)}"


def parse_gtid_set(gtid_text, flavour=None):
    """把 GTID 文本解析成「已执行事务编号」集合，用于缺 GTID 空事务补偿 / 选主比较。

    MySQL 8.0 (mysql8)：形如 "uuid1:1-5,uuid1:7,uuid2:3-9"
        -> 返回 {uuid: {n, ...}} 域聚合字典。
    MariaDB (mariadb)：形如 "0-1-100,0-1-150,1-2-30"
        -> 返回 {(domain, server): {seq, ...}} 字典。
    old：无 GTID，返回空集合。

    flavour 为 None 时**自动判别**：MariaDB 的每项均为三段纯数字「d-s-seq」（如 0-1-100），
    而 MySQL UUID 含字母与冒号。自动判别避免调用方处处传 flavour。
    """
    result = {}
    if not gtid_text:
        return result
    if flavour is None:
        # 自动判别：取第一个非空前导 token，判断是否为 MariaDB 序列格式
        flavour = F_MARIADB if _looks_like_mariadb_gtid(gtid_text) else F_MYSQL8
    if flavour == F_MARIADB:
        for token in gtid_text.split(","):
            token = token.strip()
            if not token:
                continue
            parts = token.split("-")
            try:
                nums = [int(p) for p in parts]
            except ValueError:
                continue
            if len(nums) == 3:
                d, s, seq = nums
                result.setdefault((d, s), set()).add(seq)
            elif len(nums) == 2:
                s, seq = nums
                result.setdefault((0, s), set()).add(seq)
        return result
    # MySQL 8.0 / old
    for token in gtid_text.split(","):
        token = token.strip()
        if ":" not in token:
            continue
        try:
            uuid, ranges = token.split(":", 1)
            seqs = result.setdefault(uuid, set())
            for r in ranges.split(":"):
                if "-" in r:
                    a, b = r.split("-")
                    seqs.update(range(int(a), int(b) + 1))
                else:
                    seqs.add(int(r))
        except ValueError:
            continue
    return result


def _looks_like_mariadb_gtid(gtid_text):
    """MariaDB GTID 形如 '0-1-100'（三段纯数字，域-服务-序列）；MySQL 形如 'uuid:1-5'。

    只要第一个非空前导 token 不含字母且由三段数字组成，即判为 MariaDB。
    """
    for token in gtid_text.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split("-")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            return True
        # MySQL uuid 含字母，MariaDB 纯数字 -> 非 MariaDB
        return False
    return False
