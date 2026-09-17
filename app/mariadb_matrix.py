"""mariadb_matrix.py — MariaDB 版本支持矩阵与能力分派（v2.3.0 新增）。

背景（为什么需要这个模块）
--------------------------
v2.2 对 MariaDB 只有「是 / 不是 MariaDB」一层判定，没有版本概念，导致三个问题：

  - ha_monitor 的授权串在 ``node_gen`` 与 ``ha_client`` 里各写一份且**内容不一致**
    （node_gen 有 ``REPLICATION MASTER ADMIN``，ha_client 没有）；
  - ``ensure_ha_creds`` 的补授循环对 4 个特权名**无条件** GRANT，靠 ``except`` 吞错，
    在不支持的版本上刷 1064/1227 且完全静默，排障时看不到；
  - 没有任何「这个版本受不受支持」的判断，运维不知道节点跑的是不是该跑的版本。

设计原则
--------
1. **主线优先**：项目主线为 MariaDB **12.3.3**（裸机 tarball + 官方容器镜像），
   矩阵围绕主线组织，其余版本按「支持 / 兼容」降级。
2. **能力探测优先于版本号硬编码**：特权名是否存在，用 ``SHOW PRIVILEGES`` 问服务器，
   而不是按版本号猜 —— MariaDB 各发行版构建（Debian / UBI / tarball）对
   「某版本是否具备某特权」并不一致，猜错就是 GRANT 1064/1227。
   版本号**只用于支持性提示**，不参与授权分派。
3. **MySQL 零影响**：本模块只服务 MariaDB，MySQL 8.0 分支一行不动。
"""
import re

# ---------------------------------------------------------------- 支持矩阵

MARIADB_MIN = (10, 6)              # 最低支持：低于此系列视为不支持
MARIADB_MAINLINE = (12, 3)         # 主线系列
MARIADB_MAINLINE_IMAGE = "mariadb:12.3.3"

LEVEL_MAINLINE = "mainline"        # 主线（推荐）
LEVEL_SUPPORTED = "supported"      # 支持
LEVEL_LEGACY = "legacy"            # 兼容运行，不再主动回归验证
LEVEL_UNSUPPORTED = "unsupported"  # 不支持
LEVEL_UNKNOWN = "unknown"          # 未探测到版本

LEVEL_LABEL = {
    LEVEL_MAINLINE: "主线",
    LEVEL_SUPPORTED: "支持",
    LEVEL_LEGACY: "兼容",
    LEVEL_UNSUPPORTED: "不支持",
    LEVEL_UNKNOWN: "未知",
}

# (major, minor) -> (level, 说明)
_SERIES = {
    (10, 6):  (LEVEL_LEGACY, "10.6 LTS：可运行，但不再主动回归验证"),
    (10, 11): (LEVEL_LEGACY, "10.11 LTS：可运行，但不再主动回归验证"),
    (11, 4):  (LEVEL_SUPPORTED, "11.4 LTS：支持，建议升级至 12.3 主线"),
    (11, 8):  (LEVEL_SUPPORTED, "11.8 LTS：支持，建议升级至 12.3 主线"),
    (12, 0):  (LEVEL_SUPPORTED, "12.0：支持，建议升级至 12.3 主线"),
    (12, 1):  (LEVEL_SUPPORTED, "12.1：支持，建议升级至 12.3 主线"),
    (12, 2):  (LEVEL_SUPPORTED, "12.2：支持，建议升级至 12.3 主线"),
    (12, 3):  (LEVEL_MAINLINE, "12.3：主线推荐版本（容器镜像 %s）" % MARIADB_MAINLINE_IMAGE),
}

_LEVEL_ADVICE = {
    LEVEL_MAINLINE: "版本符合主线，无需处理。",
    LEVEL_SUPPORTED: "版本受支持；建议规划升级至 12.3 主线以获得持续验证。",
    LEVEL_LEGACY: "版本已进入兼容区（仍可纳管，但不再回归验证），建议升级至 12.3 主线。",
    LEVEL_UNSUPPORTED: "版本低于最低支持线 %s，特权名/半同步变量可能与管理器预期不一致，请升级后再纳管。"
                       % ".".join(str(x) for x in MARIADB_MIN),
    LEVEL_UNKNOWN: "未能探测到版本（节点不可达或权限不足），按主线能力集处理。",
}


def parse(version_str):
    """'12.3.3-MariaDB' -> (12, 3, 3)；'10.6.21-MariaDB-log' -> (10, 6, 21)；无法解析 -> (0, 0, 0)。"""
    if not version_str:
        return (0, 0, 0)
    m = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?", str(version_str).strip())
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3) or 0))


def series_of(version_str):
    """返回 (major, minor)；未探测到返回 (0, 0)。"""
    return parse(version_str)[:2]


def support_level(version_str):
    """返回该版本在支持矩阵中的级别（mainline/supported/legacy/unsupported/unknown）。

    未列出的更高系列（如未来的 12.4/13.x）按 supported 处理 —— 向前兼容，
    不因为矩阵没更新就把新版本误判成不支持。
    """
    s = series_of(version_str)
    if s == (0, 0):
        return LEVEL_UNKNOWN
    if s in _SERIES:
        return _SERIES[s][0]
    if s < MARIADB_MIN:
        return LEVEL_UNSUPPORTED
    return LEVEL_SUPPORTED


def support_check(version_str):
    """版本准入检查，返回可直接用于预检 findings / 前端展示的字典。

    返回：{level, label, supported, message, advice, version, series}
      - supported=False 表示「不建议纳管」（低于最低支持线），预检应报 ERR
      - legacy 仅提示，不阻断
    """
    lv = support_level(version_str)
    note = _SERIES.get(series_of(version_str), (None, ""))[1]
    ver = (version_str or "").strip()
    return {
        "level": lv,
        "label": LEVEL_LABEL.get(lv, lv),
        "supported": lv != LEVEL_UNSUPPORTED,
        "message": ("%s（%s）" % (ver or "未知版本", note or LEVEL_LABEL.get(lv, lv))).strip(),
        "advice": _LEVEL_ADVICE.get(lv, ""),
        "version": ver,
        "series": "%d.%d" % series_of(version_str),
    }


def matrix_rows():
    """返回支持矩阵表格数据（供文档生成 / 前端展示 / API 输出）。"""
    rows = []
    for s in sorted(_SERIES):
        lv, note = _SERIES[s]
        rows.append({
            "series": "%d.%d" % s,
            "level": lv,
            "label": LEVEL_LABEL.get(lv, lv),
            "note": note,
        })
    rows.append({
        "series": "< %d.%d" % MARIADB_MIN,
        "level": LEVEL_UNSUPPORTED,
        "label": LEVEL_LABEL[LEVEL_UNSUPPORTED],
        "note": "低于最低支持线，不建议纳管",
    })
    return rows


# ---------------------------------------------------------------- 特权集

# ha_monitor 需要的全局特权全集。
# 这些特权在 MariaDB 10.5+ 引入（SUPER 拆分），而项目最低支持线是 10.6，
# 故「静态渲染 init SQL / fix SQL」时使用全集是安全的；
# 运行期（有连接）则一律经 server_privileges() 过滤，杜绝 1064/1227。
HA_PRIVS = (
    "CREATE USER", "CREATE", "INSERT", "UPDATE", "DELETE",
    "REPLICATION SLAVE", "REPLICATION SLAVE ADMIN", "REPLICATION MASTER ADMIN",
    "SLAVE MONITOR", "READ_ONLY ADMIN", "BINLOG ADMIN",
    "REPLICATION CLIENT", "PROCESS", "SELECT", "RELOAD", "SUPER",
    "EXECUTE",
)

REPL_PRIVS = ("REPLICATION SLAVE", "REPLICATION SLAVE ADMIN")

APP_PRIVS = (
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
    "INDEX", "REFERENCES", "EXECUTE", "SHOW VIEW", "CREATE TEMPORARY TABLES",
)

# 「有则补、无则跳过」的特权：老版本可能没有，缺失不影响主流程。
# 说明：SLAVE MONITOR 用于读复制状态（管理器探活必需）；其余为 SUPER 拆分出来的管理特权。
# 刻意**不含** SET USER —— ha_monitor 在 MariaDB 上凭 CREATE USER 已可 ALTER/DROP USER，
# 不额外放大权限。
OPTIONAL_PRIVS = (
    "CREATE USER", "READ_ONLY ADMIN", "BINLOG ADMIN",
    "REPLICATION MASTER ADMIN", "SLAVE MONITOR",
)


def _norm(p):
    return " ".join(str(p).split()).upper()


def server_privileges(cur):
    """用 SHOW PRIVILEGES 取服务器真实支持的特权名集合。

    返回 None 表示探测失败（调用方应按「全集 + 容错」处理，即沿用 v2.2 的 except 行为）；
    返回空集合同样按 None 处理，避免把「探测异常」误判成「服务器什么特权都没有」。
    """
    try:
        cur.execute("SHOW PRIVILEGES")
        rows = cur.fetchall() or []
    except Exception:
        return None
    out = set()
    for r in rows:
        try:
            name = r[0]
        except Exception:
            continue
        if name:
            out.add(_norm(name))
    return out or None


def filter_privs(privs, available):
    """按服务器实际支持的特权名过滤；available 为 None（探测失败）时原样返回。"""
    if not available:
        return [p for p in privs if p]
    return [p for p in privs if _norm(p) in available]


def _join(privs, available=None):
    return ", ".join(filter_privs(privs, available))


def ha_privs(available=None):
    """ha_monitor 的全局特权串（按服务器能力过滤后）。"""
    return _join(HA_PRIVS, available)


def repl_privs(available=None):
    """复制账号的全局特权串。"""
    return _join(REPL_PRIVS, available)


def app_privs(available=None):
    """业务账号的全局特权串。"""
    return _join(APP_PRIVS, available)


def optional_privs(available=None):
    """补授用的可选特权列表（有则补、无则跳过）。"""
    return filter_privs(OPTIONAL_PRIVS, available)


# ---------------------------------------------------------------- 半同步

# MariaDB 全系列沿用 master/slave 命名（11.5+ 才新增 source/replica 别名，
# 且旧名仍有效），故 **不做版本分支** —— 统一用旧名对 10.6 ~ 12.3 全兼容。
SEMI_MASTER = "rpl_semi_sync_master"
SEMI_SLAVE = "rpl_semi_sync_slave"


def semisync_prefixes(version_str=None):
    """返回 (主库侧变量前缀, 从库侧变量前缀)，MariaDB 恒为 master/slave。"""
    return SEMI_MASTER, SEMI_SLAVE


def semisync_plugin_load():
    """my.cnf 的 plugin_load_add 值（MariaDB 半同步自 10.3.3 起内置）。"""
    return "rpl_semi_sync_master;rpl_semi_sync_slave"


def describe(version_str=None):
    """给前端/API 用的一句话版本描述，如『12.3.3-MariaDB · 主线』。"""
    c = support_check(version_str)
    return "%s · %s" % (c["version"] or "未知版本", c["label"])
