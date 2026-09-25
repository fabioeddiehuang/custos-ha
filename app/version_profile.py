"""version_profile.py — 接管/纳管存量集群的**版本能力档案**（v2.4.1 新增）。

为什么需要这个模块
------------------
接管（adopt）面对的是「别人已经跑了很久的库」，版本杂、配置乱。v2.4.0 之前对
版本的处理是散落在各处的 ``if version >= ...``，导致：

  - 权限名在 ``node_gen`` 与 ``ha_client`` 各写一份且内容不一致；
  - 提主要什么权限、关 binlog 要什么权限，靠 ``except`` 吞 1064/1227 静默失败；
  - 无法回答「这个版本的老库我到底能不能接管」。

本模块把「某版本能做什么、要什么权限、用什么方言/工具」集中成**一张表**，作为
单一真源。所有接管决策（探测项、权限分派、dump 工具、复制方言）都查这张表，
不再散落判断。

设计原则（沿用 mariadb_matrix 并扩展到 MySQL）
----------------------------------------------
1. **能力探测优先于版本号硬编码**：本表给出的权限是「候选清单」，实际授予前必须用
   ``SHOW PRIVILEGES`` 过滤（见 ``mariadb_matrix.filter_privs``）——同一版本号的不同
   发行版构建（Debian / UBI / tarball / 官方镜像）权限集合并不一致，猜错就是
   GRANT 1064/1227。版本号只用于**支持性提示与默认选择**。
2. **按引擎族隔离**：MariaDB 与 MySQL 的 GTID 机制根本不兼容，档案按 family 分派，
   互不污染。
3. **MariaDB 主线优先**：主线仍是 12.3；接管只是让「存量老版本」也能被纳管，
   不改变主线方向。
4. **宁可标 legacy 也不要静默支持**：不支持/未验证的版本必须显式说清楚，避免
   运维以为接管成功、实际复制根本建不起来。

支持的版本（外界存量前 3 + 本产品主线）
--------------------------------------
  MySQL   : 5.7（EOL，仅兼容）、8.0（主力）、8.4（新 LTS）
  MariaDB : 10.6、10.11、11.4（三个 LTS 存量最大）、12.3（本产品主线）
"""
from .mysql_compat import parse_version, is_mariadb
from . import mariadb_matrix as _mm

# ---------------------------------------------------------------- 常量

# 支持级别（语义与 mariadb_matrix 保持一致，便于 UI 复用标签）
LEVEL_MAINLINE = _mm.LEVEL_MAINLINE        # 主线（推荐）
LEVEL_SUPPORTED = _mm.LEVEL_SUPPORTED      # 支持
LEVEL_LEGACY = _mm.LEVEL_LEGACY            # 兼容运行，不再主动回归验证
LEVEL_UNSUPPORTED = _mm.LEVEL_UNSUPPORTED  # 不支持
LEVEL_UNKNOWN = _mm.LEVEL_UNKNOWN          # 未探测到版本 / 未登记

LEVEL_LABEL = _mm.LEVEL_LABEL

FAMILY_MARIADB = "mariadb"
FAMILY_MYSQL = "mysql"

# 复制模式（接管探测的结论之一）
REPL_GTID = "gtid"          # GTID 自动定位
REPL_FILE_POS = "file_pos"  # 传统 binlog 文件名 + 位点
REPL_NONE = "none"          # 未配置任何复制

REPL_MODE_LABEL = {
    REPL_GTID: "GTID 自动定位",
    REPL_FILE_POS: "传统位点(file/pos)",
    REPL_NONE: "未配置复制",
}

# MariaDB 各特权引入的门槛版本（仅用于「候选清单排序」，最终仍由 SHOW PRIVILEGES 裁决）
_MARIA_READONLY_ADMIN_MIN = (10, 5)
_MARIA_BINLOG_ADMIN_MIN = (10, 5, 2)

# MySQL 8.0 起提主改 read_only 需要 SYSTEM_VARIABLES_ADMIN（5.7 只有 SUPER）
_MYSQL_SYSVAR_ADMIN_MIN = (8, 0)

# 接管时可能需要「开启」、但默认关闭的系统变量（MariaDB 存量老库常见）。
# 这些变量大多需改 my.cnf + 重启（停机窗口），故在接管向导里作为**显式 opt-in 选项**
# 提供；probe 仅报告当前是否缺失，由用户在确认停机窗口后启用。
# 仅 MariaDB 当前落地；MySQL 后续接入（log_bin / gtid_mode+enforce_gtid_consistency 等）。
_MARIA_ADOPT_SWITCHES = [
    {"name": "log_bin", "target": "ON", "restart": True, "scenario": "extend",
     "note": "单机扩从库(场景2)必须先开 binlog；已有主从(场景1)主库通常已开。"},
    {"name": "gtid_domain_id", "target": ">0", "restart": True, "scenario": "extend",
     "note": "场景2 选 GTID 复制时设置（MASTER_USE_GTID=slave_pos 依赖）；不设则回退 file/pos。"},
    {"name": "gtid_strict_mode", "target": "ON", "restart": True, "scenario": "extend",
     "note": "场景2 选 GTID 时建议开启，防止乱序 GTID 被应用。"},
]


# ---------------------------------------------------------------- 档案表
#
# 字段说明：
#   family         引擎族（决定能否同集群混部）
#   level          支持级别
#   gtid_var       该风味下「已执行 GTID 集合」的系统变量名（探测用）
#   gtid_dialect   GTID 方言标识（供 change_replication_sql 等分派）
#   promote_privs  提主（SET GLOBAL read_only=0）所需权限的**候选清单**（按优先级）
#   binlog_off     关闭会话 binlog（SET SESSION sql_log_bin=0）所需权限候选
#   repl_privs     复制账号所需权限候选
#   dump_tool      全量导出应使用的工具（导出在节点本机执行，见设计：绕开客户端协议差异）
#   notes          给运维看的一句话提示

_PROFILES = {
    # ---------------- MySQL ----------------
    (5, 7): dict(
        family=FAMILY_MYSQL, level=LEVEL_LEGACY,
        gtid_var="gtid_executed", gtid_dialect="mysql_gtid",
        promote_privs=["SUPER"],
        binlog_off_privs=["SUPER"],
        repl_privs=["REPLICATION SLAVE"],
        dump_tool="mysqldump",
        notes="MySQL 5.7 已 EOL（无安全更新），且无 SYSTEM_VARIABLES_ADMIN，提主只能靠 SUPER；"
              "建议接管后规划升级到 8.0/8.4。",
    ),
    (8, 0): dict(
        family=FAMILY_MYSQL, level=LEVEL_SUPPORTED,
        gtid_var="gtid_executed", gtid_dialect="mysql_gtid",
        promote_privs=["SYSTEM_VARIABLES_ADMIN", "SUPER"],
        binlog_off_privs=["SYSTEM_VARIABLES_ADMIN", "SUPER"],
        repl_privs=["REPLICATION SLAVE"],
        dump_tool="mysqldump",
        notes="MySQL 8.0 主力版本。默认认证插件 caching_sha2_password，非 TLS 复制需索取服务端公钥。",
    ),
    (8, 4): dict(
        family=FAMILY_MYSQL, level=LEVEL_SUPPORTED,
        gtid_var="gtid_executed", gtid_dialect="mysql_gtid",
        promote_privs=["SYSTEM_VARIABLES_ADMIN", "SUPER"],
        binlog_off_privs=["SYSTEM_VARIABLES_ADMIN", "SUPER"],
        repl_privs=["REPLICATION SLAVE"],
        dump_tool="mysqldump",
        notes="MySQL 8.4 LTS。mysql_native_password 默认不再启用，复制账号需注意认证插件兼容。",
    ),

    # ---------------- MariaDB ----------------
    (10, 6): dict(
        family=FAMILY_MARIADB, level=LEVEL_LEGACY,
        gtid_var="gtid_slave_pos", gtid_dialect="mariadb_gtid",
        promote_privs=["READ_ONLY ADMIN", "SUPER"],
        binlog_off_privs=["BINLOG ADMIN", "SUPER"],
        repl_privs=["REPLICATION SLAVE ADMIN", "REPLICATION SLAVE"],
        dump_tool="mariadb-dump",
        notes="MariaDB 10.6 LTS：可纳管，但不再主动回归验证。10.5+ 已具备 READ_ONLY/BINLOG ADMIN。",
    ),
    (10, 11): dict(
        family=FAMILY_MARIADB, level=LEVEL_SUPPORTED,
        gtid_var="gtid_slave_pos", gtid_dialect="mariadb_gtid",
        promote_privs=["READ_ONLY ADMIN", "SUPER"],
        binlog_off_privs=["BINLOG ADMIN", "SUPER"],
        repl_privs=["REPLICATION SLAVE ADMIN", "REPLICATION SLAVE"],
        dump_tool="mariadb-dump",
        notes="MariaDB 10.11 LTS：本轮回接管的优先目标版本（与 11.4 权限模型一致）。",
    ),
    (11, 4): dict(
        family=FAMILY_MARIADB, level=LEVEL_SUPPORTED,
        gtid_var="gtid_slave_pos", gtid_dialect="mariadb_gtid",
        promote_privs=["READ_ONLY ADMIN", "SUPER"],
        binlog_off_privs=["BINLOG ADMIN", "SUPER"],
        repl_privs=["REPLICATION MASTER ADMIN", "REPLICATION SLAVE ADMIN", "REPLICATION SLAVE"],
        dump_tool="mariadb-dump",
        notes="MariaDB 11.4 LTS：支持。11.x 起新增 REPLICATION MASTER ADMIN。",
    ),
    (12, 3): dict(
        family=FAMILY_MARIADB, level=LEVEL_MAINLINE,
        gtid_var="gtid_slave_pos", gtid_dialect="mariadb_gtid",
        promote_privs=["READ_ONLY ADMIN", "SUPER"],
        binlog_off_privs=["BINLOG ADMIN", "SUPER"],
        repl_privs=["REPLICATION MASTER ADMIN", "REPLICATION SLAVE ADMIN", "REPLICATION SLAVE"],
        dump_tool="mariadb-dump",
        notes="MariaDB 12.3：本产品主线版本（Custos 自建集群即此版本）。",
    ),
}


# ---------------------------------------------------------------- 查询 API

def series_key(version_str):
    """'8.0.36' -> (8,0)；'12.3.3-MariaDB' -> (12,3)；无法解析返回 None。"""
    v = parse_version(version_str)
    if v == (0, 0, 0):
        return None
    return (v[0], v[1])


def profile(version_str):
    """返回该版本的能力档案 dict；未登记版本返回一张「未知」档案（不抛异常）。

    未登记 ≠ 不能纳管：纳管本身是「只读探测 + 登记 + 装边车」，不依赖版本。
    版本只决定**能力边界与提示**，所以未知版本给 UNKNOWN 并附提示，由上层决定
    是继续（纳管）还是拒绝（建复制 / 切主）。
    """
    k = series_key(version_str)
    if k is not None and k in _PROFILES:
        p = dict(_PROFILES[k])
        p["series"] = "%d.%d" % k
        p["version"] = version_str or ""
        return p

    fam = FAMILY_MARIADB if is_mariadb(version_str) else FAMILY_MYSQL
    return dict(
        family=fam,
        level=LEVEL_UNKNOWN,
        series=("%d.%d" % k) if k else "",
        version=version_str or "",
        gtid_var=("gtid_slave_pos" if fam == FAMILY_MARIADB else "gtid_executed"),
        gtid_dialect=("mariadb_gtid" if fam == FAMILY_MARIADB else "mysql_gtid"),
        promote_privs=["SUPER"],
        binlog_off_privs=["SUPER"],
        repl_privs=["REPLICATION SLAVE"],
        dump_tool=("mariadb-dump" if fam == FAMILY_MARIADB else "mysqldump"),
        notes="该版本未登记在支持矩阵中：可纳管（只读探测/装边车），但「建复制、切主」等"
              "能力未经回归验证，请先人工确认。",
    )


def family(version_str):
    return profile(version_str)["family"]


def support_level(version_str):
    return profile(version_str)["level"]


def is_supported(version_str):
    """是否属于「支持」及以上（mainline / supported / legacy 都算可接管）。"""
    return profile(version_str)["level"] in (LEVEL_MAINLINE, LEVEL_SUPPORTED, LEVEL_LEGACY)


def support_note(version_str):
    p = profile(version_str)
    return "%s（%s）%s" % (
        p["series"] or "未知版本",
        LEVEL_LABEL.get(p["level"], p["level"]),
        ("：" + p["notes"]) if p.get("notes") else "",
    )


def dump_tool(version_str):
    """全量导出应使用的工具名。

    注意：接管场景一律**在节点本机**执行导出（由 custos-node-agent 调用节点自带的
    mysqldump / mariadb-dump），而不是从管理器连过去导出 —— 这样既绕开
    MariaDB 客户端连 MySQL 8.0（caching_sha2）的协议/认证不兼容，也不需要管理器
    为每个版本准备一套客户端。
    """
    return profile(version_str)["dump_tool"]


def priv_candidates(kind, version_str):
    """返回某类动作所需权限的**候选清单**（按优先级排序，需再用 SHOW PRIVILEGES 过滤）。

    kind: promote（提主改 read_only） / binlog_off（关会话 binlog） / repl（复制账号）
    """
    p = profile(version_str)
    return list(p.get(kind + "_privs") or p.get(kind) or [])


def gtid_var(version_str):
    """该版本「已执行 GTID 集合」的变量名（探测复制进度用）。"""
    return profile(version_str)["gtid_var"]


def gtid_dialect(version_str):
    return profile(version_str)["gtid_dialect"]


def needs_public_key(version_str):
    """MySQL 8.0+ 非 TLS 复制是否需要索取服务端公钥（caching_sha2_password）。

    MariaDB 不适用（用 mysql_native_password，且 CHANGE MASTER 不支持该参数）。
    """
    if is_mariadb(version_str):
        return False
    return parse_version(version_str) >= _MYSQL_SYSVAR_ADMIN_MIN


def adopt_switch_requirements(version_str):
    """返回接管时可能需要显式开启的开关清单（当前 MariaDB 落地）。

    清单里的变量默认常关（尤其 log_bin 在独立部署的 MariaDB 上默认 OFF），
    enabling 通常需改 my.cnf + 重启，故作为 opt-in 选项提供给用户，不在探测期自动改。
    """
    if is_mariadb(version_str):
        return [dict(s) for s in _MARIA_ADOPT_SWITCHES]
    return []


def supported_series():
    """返回登记在册的版本清单（供 UI 下拉 / 文档生成），按 family + 版本排序。"""
    out = []
    for k in sorted(_PROFILES):
        p = _PROFILES[k]
        out.append({
            "series": "%d.%d" % k,
            "family": p["family"],
            "level": p["level"],
            "level_label": LEVEL_LABEL.get(p["level"], p["level"]),
            "dump_tool": p["dump_tool"],
            "notes": p.get("notes", ""),
        })
    return out


def describe(version_str):
    """一行人类可读摘要（日志 / 页面提示用）。"""
    p = profile(version_str)
    return "%s %s · %s · 导出工具 %s" % (
        ("MariaDB" if p["family"] == FAMILY_MARIADB else "MySQL"),
        p["series"] or "?",
        LEVEL_LABEL.get(p["level"], p["level"]),
        p["dump_tool"],
    )
