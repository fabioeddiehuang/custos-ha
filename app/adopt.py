"""adopt.py — 存量集群「纳管/接管」（v2.4.1）。

设计立场：**接管 = 纳管，不是重建**
-----------------------------------
老库已经在跑业务，接管的第一原则是「别把它搞坏」。因此本模块严格分三层，
且**探测层（本文件）只做只读操作，一个字节都不写**：

  阶段 0  probe   —— 只读探测，幂等，可反复跑；产出「能力/健康报告 + 结论」
  阶段 1  adopt   —— 不动数据：建账号、登记进 nodes/hosts、装 agent + keepalived + VIP
  阶段 2  extend  —— 仅单机场景：开 binlog（需重启）→ 节点本机导出 → 导入 → 建复制

探测层为什么必须独立且只读：
  - 探测可以（也应该）在用户还没决定是否接管时就跑，跑坏了 = 事故；
  - 探测结论要能驱动 UI 显示「能不能接、差哪一步」，所以必须是结构化数据而非日志；
  - 幂等 + 只读 = 可以大胆重试，不用担心副作用。

场景
----
  场景 1（已有主从）：多节点、彼此之间已建复制 → 只加 VIP + agent 做容灾，**不动复制**
  场景 2（单机）    ：仅 1 台、无复制 → 以它为**主库**，另加一台同版本节点做从库

硬约束（探测阶段必须校验，不满足直接判定为 blocker）
--------------------------------------------------
  - 同集群必须**同版本**（用户明确要求非混布）
  - server_id 必须非 0 且互不相同（撞 id 会直接搞坏复制）
  - 节点必须在**同一二层网络**：VRRP 跨网段不工作，VIP 漂移方案直接不成立
"""
import ipaddress
import logging

from .ha_client import connect
from .mysql_compat import detect_flavour, show_replica_status_sql, show_replicas_sql
from . import version_profile as _vp

logger = logging.getLogger(__name__)

# 场景
SCENARIO_EXISTING_REPL = "existing_repl"  # 场景1：已存在主从复制
SCENARIO_SINGLE = "single"                # 场景2：单机，需扩从库
SCENARIO_UNKNOWN = "unknown"

SCENARIO_LABEL = {
    SCENARIO_EXISTING_REPL: "已有主从复制（加 VIP + agent 做容灾）",
    SCENARIO_SINGLE: "单机（以其为主库，另加一台同版本从库）",
    SCENARIO_UNKNOWN: "拓扑未识别，需人工确认",
}

# 结论
VERDICT_READY = "ready"              # 可直接纳管
VERDICT_NEEDS_CHANGE = "needs_change"  # 需先在节点上整改（附具体步骤）
VERDICT_UNSUPPORTED = "unsupported"  # 不支持接管

# 问题级别
LV_BLOCKER = "blocker"
LV_WARN = "warn"

# binlog 非 ROW 时的风险提示（非阻断：老库大量存在 STATEMENT，纳管本身可行）
_ROW_FORMAT = "ROW"

# 「快速修复」用到的配置默认值（MariaDB 专用）
_BINLOG_NAME = "mysql-bin"


def suggest_server_id(host, used=None):
    """给节点建议一个 server_id：优先取 IP 末段；为 0 或已被占用就顺延。

    server_id 撞号会直接搞坏复制，所以建议值必须在本次探测到的节点里去重。
    """
    used = set(int(x) for x in (used or []) if str(x).lstrip("-").isdigit())
    try:
        base = int(str(host).strip().split(".")[-1])
    except Exception:
        base = 0
    if base <= 0:
        base = 101
    sid = base
    for _ in range(10000):
        if sid != 0 and sid not in used:
            return sid
        sid += 1
        if sid > 4294967295:
            sid = 1
    return base


def build_fix_command(server_id=None, log_bin=False, binlog_format=False, container="",
                      gtid_strict=False, bind_addr=False, read_only_rel=False,
                      log_bin_name=None):
    """生成「改配置 + 重启」的修复命令（MariaDB，绝不覆盖旧库现有配置）。

    生产铁律（防范最高级事故）：
      1. 只**新增**一个独立配置文件（99-custos-adopt.cnf），绝不改动旧库任何现有 my.cnf；
      2. 该文件里同一项已存在就**原地替换**（含被 # 注释掉的），不存在才**追加到文件末尾**；
      3. 动手前先 `cp -a` 备份，随时可回滚。

    log_bin 是只读变量，SQL 改不了 —— 只能改配置 + 重启，命令里如实说明，不假装能在线开。

    container 非空 = 数据库跑在容器里（Docker / 1Panel 面板装的都算）：宿主机的 my.cnf
    和 systemctl 都管不到容器，必须进容器写配置 + docker restart；纯 SQL 只够改 server_id。
    """
    if container:
        cn = str(container).strip()
        sql = []
        if server_id:
            sql.append("SET GLOBAL server_id=%d;" % int(server_id))
        sql.append("SELECT @@log_bin, @@server_id, @@binlog_format;")
        # 客户端命令名不统一：MariaDB 11.x 官方镜像只有 `mariadb`（没有 `mysql` 这个名字），
        # 老镜像/部分面板镜像相反。这里两个都试，避免用户踩「executable file not found」。
        _sql_text = " ".join(sql)
        sql_line = (
            "docker exec -it %s sh -c 'if command -v mariadb >/dev/null 2>&1; then "
            "exec mariadb -uroot -p -e \"%s\"; else exec mysql -uroot -p -e \"%s\"; fi'\n"
            % (cn, _sql_text, _sql_text)
        )
        if log_bin or binlog_format or gtid_strict or bind_addr or read_only_rel:
            cnf_lines = ["[mysqld]"]
            if server_id:
                cnf_lines.append("server_id=%d" % int(server_id))
            if log_bin:
                cnf_lines.append("log_bin=%s" % (log_bin_name or _BINLOG_NAME))
            if binlog_format:
                cnf_lines.append("binlog_format=ROW")
            if gtid_strict:
                cnf_lines.append("gtid_strict_mode=ON")
            if bind_addr:
                cnf_lines.append("bind_address=0.0.0.0")
            if read_only_rel:
                cnf_lines.append("read_only=0")
                cnf_lines.append("super_read_only=0")
            cnf = "\\n".join(cnf_lines) + "\\n"
            return (
                "# ---- 数据库跑在容器里：在【宿主机】上执行（1Panel / Docker 都一样）----\n"
                "# 开 binlog 用 SQL 做不到，只能写配置：往容器里只新增这一个文件，不动它原有配置\n"
                + ("docker exec %s sh -c 'printf \"%s\" > /etc/mysql/conf.d/99-custos-adopt.cnf'\n"
                   % (cn, cnf))
                + "docker restart %s\n" % cn
                + "\n# 重启后进容器用 SQL 确认结果\n"
                + sql_line
                + "# 注意：写进容器内的配置，容器重建会丢；想持久请改面板映射到宿主机的那个配置目录\n"
            )
        return (
            "# ---- 数据库跑在容器里：在【宿主机】上执行（1Panel / Docker 都一样）----\n"
            "# 只差 server_id：进容器执行一条 SQL 就行，不用重启\n"
            + sql_line
            + "# 若提示 server_id 是只读变量，就写进容器配置再 docker restart %s\n" % cn
        )

    head = (
        "# ---- 在【数据库所在主机】上以 root 执行（MariaDB）----\n"
        "# 优先只新增一个独立配置文件（不动旧库现有 my.cnf）；\n"
        "# 但若该目录【并未】被主 my.cnf 的 !includedir 引用（实测 Anolis/RHEL 精简装会出现：\n"
        "#   文件写进去了却完全不生效，用户重启后重新探测仍是阻断项，且无从判断原因），\n"
        "#   则回退为写进主 my.cnf 的 [mysqld] 段。两种情况都先 cp -a 备份。\n"
        "MYCNF=/etc/my.cnf\n"
        "for c in /etc/my.cnf /etc/mysql/my.cnf; do [ -f \"$c\" ] && MYCNF=\"$c\" && break; done\n"
        "# 两个都不存在（精简装）时回落到 /etc/my.cnf 并由后续语句创建\n"
        "D=/etc/mysql/conf.d; [ -d \"$D\" ] || D=/etc/my.cnf.d; mkdir -p \"$D\"\n"
        "F=\"$D/99-custos-adopt.cnf\"\n"
        "if ! grep -qE \"^[[:space:]]*!includedir[[:space:]]+${D}\" \"$MYCNF\" 2>/dev/null; then\n"
        "  echo \"[提示] ${D} 未被 ${MYCNF} 的 !includedir 引用，改为写入主配置 ${MYCNF}\"\n"
        "  F=\"$MYCNF\"\n"
        "fi\n"
        "cp -a \"$F\" \"$F.bak.$(date +%F-%H%M%S)\" 2>/dev/null || true\n"
        "grep -q '^\\[mysqld\\]' \"$F\" 2>/dev/null || printf '[mysqld]\\n' >> \"$F\"\n"
        "set_kv(){ k=$1; v=$2; if grep -qE \"^[[:space:]]*#?[[:space:]]*${k}[[:space:]]*=\" \"$F\"; then "
        "sed -i \"s|^[[:space:]]*#\\?[[:space:]]*${k}[[:space:]]*=.*|${k}=${v}|\" \"$F\"; "
        "else sed -i \"/^\\[mysqld\\]/a ${k}=${v}\" \"$F\"; fi; }\n"
    )
    lines = []
    if server_id:
        lines.append("set_kv server_id %d" % int(server_id))
    if log_bin:
        lines.append("set_kv log_bin %s" % (log_bin_name or _BINLOG_NAME))
    if binlog_format:
        lines.append("set_kv binlog_format ROW")
    if gtid_strict:
        lines.append("set_kv gtid_strict_mode ON")
    if bind_addr:
        lines.append("set_kv bind_address 0.0.0.0")
    if read_only_rel:
        lines.append("set_kv read_only 0")
        lines.append("set_kv super_read_only 0")
    tail = (
        "\n# 让配置生效（服务名按实际，常见 mariadb / mysqld）\n"
        "systemctl restart mariadb\n"
        "# 重启后回本页面点「重新探测」确认。若 log_bin 仍为 OFF，说明该目录\n"
        "# 未被 my.cnf 的 !includedir 引用，把上面几行写进主 my.cnf 的 [mysqld] 段即可\n"
    )
    return head + ("\n".join(lines) + "\n" if lines else "") + tail


def fix_command_for(host, port, user, password, server_id=None, container="", timeout=5):
    """连上去看这台还差什么，按形态（裸机 / 容器）生成修复命令。

    **只读**：只查变量，不改任何东西。形态必须由用户在页面上选 —— 管理器只连得上
    数据库端口，猜不出它是裸机进程还是容器（1Panel 面板装的尤其看不出来）。
    """
    out = {"ok": False, "command": "", "msg": ""}
    conn = None
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
        if conn is None:
            out["msg"] = "连不上这台，没法判断还差什么"
            return out
        cur = conn.cursor()
        sid = _scalar(cur, "SELECT @@server_id")
        log_bin = _scalar_str(cur, "SELECT @@log_bin")
        fmt = _scalar_str(cur, "SELECT @@binlog_format")
        try:
            want = int(server_id) if server_id else 0
        except Exception:
            want = 0
        need_sid = sid in (None, 0) or (want > 0 and want != sid)
        # ⚠ 实测坑：存在「@@log_bin=1 但配置里根本没固化」的情况（如 1Panel 容器由启动参数开启）。
        # 若只按"当前是否缺失"判断，就不会写 log_bin，用户一重启 binlog 反而消失 ——
        # 等于接管操作把旧库的复制能力弄丢了。故接管场景**一律显式固化 binlog**。
        need_bin = True
        # 已开启时沿用原 basename，避免改变既有 binlog 文件名影响已有复制
        lbn = str(_scalar(cur, "SELECT @@log_bin_basename") or "").strip()
        bin_name = lbn or _BINLOG_NAME
        need_fmt = bool(fmt) and fmt.upper() != _ROW_FORMAT
        gsm = _scalar_str(cur, "SELECT @@gtid_strict_mode")
        need_gtid = gsm.upper() in ("OFF", "0")
        ba = str(_scalar(cur, "SELECT @@bind_address") or "").strip()
        need_bind = bool(ba) and ba not in ("0.0.0.0", "*", "::", "0.0.0.0/0")
        ro = _scalar_str(cur, "SELECT @@read_only")
        sro = _scalar_str(cur, "SELECT @@super_read_only")
        need_ro = ro.upper() in ("1", "ON") or sro.upper() in ("1", "ON")
        others = need_sid or need_fmt or need_gtid or need_bind or need_ro
        bin_on = log_bin.upper() in ("ON", "1")
        want_sid = want if want > 0 else (sid if sid else suggest_server_id(host))
        out["command"] = build_fix_command(
            server_id=(want_sid if need_sid else None),
            log_bin=need_bin, binlog_format=need_fmt,
            gtid_strict=need_gtid, bind_addr=need_bind, read_only_rel=need_ro,
            container=container, log_bin_name=bin_name)
        out["ok"] = True
        if not others and bin_on:
            out["msg"] = ("其余项均正常；仍建议把 binlog 显式写入配置"
                          "（当前虽已开启，但未固化，重启可能丢失）")
        elif not others:
            out["msg"] = "已生成开启并固化 binlog 的命令"
        else:
            out["msg"] = "已按「%s」形态生成" % ("容器" if container else "裸机")
        out["needs"] = {"server_id": need_sid, "log_bin": need_bin, "binlog_format": need_fmt,
                        "gtid_strict_mode": need_gtid, "bind_address": need_bind,
                        "read_only": need_ro}
        return out
    except Exception as e:
        out["msg"] = "生成失败：%s" % e
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _can_tune(node):
    """探测账号够不够格在线改全局参数（root 级 + 非只读副本）。

    只读副本上改 server_id 不安全（很可能正在跑复制），一律不放行，只给命令。
    """
    ro = str(node.get("read_only") or "")
    sro = str(node.get("super_read_only") or "")
    if ro.upper() in ("1", "ON") or sro.upper() in ("1", "ON"):
        return False
    g = " ".join(node.get("grants") or []).upper()
    if not g:
        return False
    return ("ALL PRIVILEGES" in g) or ("SUPER" in g)


def apply_switch_fix(host, port, user, password, server_id, timeout=5):
    """接管向导「快速修复」：在线设 server_id，其余只能给命令 + 人工重启。

    log_bin 是只读变量，SQL 改不动 —— 如实标记为「需重启」并给出命令，
    绝不假装改成功（否则用户下一步就是带病接管）。

    返回 {ok, applied:[], need_restart:[], command, error}
    """
    out = {"ok": False, "applied": [], "need_restart": [], "command": "", "error": ""}
    try:
        sid = int(server_id)
    except Exception:
        out["error"] = "server_id 必须是数字"
        return out
    if sid <= 0:
        out["error"] = "server_id 必须大于 0（0 无法建复制）"
        return out

    conn = None
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
        if conn is None:
            out["error"] = "连接失败"
            return out
        cur = conn.cursor()

        # 只读副本不在线改（可能正在跑复制），只给命令
        ro = _scalar_str(cur, "SELECT @@read_only")
        if ro.upper() in ("1", "ON"):
            out["need_restart"].append(
                "server_id=%d（该节点是只读副本，在线改不安全，请改配置并重启）" % sid)
        else:
            try:
                cur.execute("SET GLOBAL server_id = %d" % sid)
                out["applied"].append(
                    "server_id=%d 已在线生效（重启会回退，仍需写进配置）" % sid)
            except Exception as e:
                out["need_restart"].append(
                    "server_id=%d（该版本不支持在线改，需改配置并重启：%s）"
                    % (sid, str(e)[:80]))

        log_bin = _scalar_str(cur, "SELECT @@log_bin")
        fmt = _scalar_str(cur, "SELECT @@binlog_format")
        need_bin = log_bin.upper() in ("OFF", "0", "NONE")
        need_fmt = bool(fmt) and fmt.upper() != _ROW_FORMAT
        if need_bin:
            out["need_restart"].append(
                "log_bin=%s（只读变量，SQL 改不了，必须改配置并重启）" % _BINLOG_NAME)
        if need_fmt:
            out["need_restart"].append("binlog_format=ROW（需改配置并重启）")

        out["command"] = build_fix_command(
            server_id=sid, log_bin=need_bin, binlog_format=need_fmt)
        out["ok"] = True
        return out
    except Exception as e:
        out["error"] = "执行失败：%s" % e
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def release_read_only(host, port, user, password, timeout=5):
    """在线解除只读（read_only / super_read_only）。

    ⚠ 调用前提：调用方必须先确认该实例**不是别人的从库**（role != slave）。
    在从库上解除只读并写入会造成主从数据不一致，属高危操作，故把关责任在调用方
    （API 层会先校验 role，still_slave 阻断项未清零时拒绝执行）。

    在线生效但重启会回退，故持久化仍需把 read_only=0 写进 99-custos-adopt.cnf
    （由 build_fix_command 的 read_only_rel 分支一并生成）。
    """
    out = {"ok": False, "applied": [], "msg": ""}
    conn = None
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
        if conn is None:
            out["msg"] = "连不上，无法解除只读"
            return out
        cur = conn.cursor()
        for var in ("read_only", "super_read_only"):
            try:
                cur.execute("SET GLOBAL %s = 0" % var)
                out["applied"].append(
                    "%s=0 已在线生效（重启会回退，仍需写进配置持久化）" % var)
            except Exception as e:
                out["applied"].append(
                    "%s 未能在线解除：%s（需写配置并重启）" % (var, str(e)[:80]))
        out["ok"] = True
        out["msg"] = "已尝试在线解除只读"
        return out
    except Exception as e:
        out["msg"] = "解除只读失败：%s" % e
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _issue(level, code, msg, fix=""):
    return {"level": level, "code": code, "msg": msg, "fix": fix}


def _scalar(cur, sql):
    """执行单值查询，失败返回 None（不同版本变量名不同，缺失属正常）。"""
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _scalar_str(cur, sql):
    """取单值转字符串：**只把 None 转成空串，绝不吞掉 0**。

    踩过的实锤坑：`str(_scalar(...) or "")` 会把整型的 0 也吞成空串 ——
    `@@log_bin` 返回 0（表示没开）时会被误判成「没值/不用管」，于是提示"没问题不用改"，
    用户照着重启后才发现根本没生效。判空必须显式判 None。
    """
    v = _scalar(cur, sql)
    return "" if v is None else str(v)


def _dict_cursor(conn):
    """SHOW SLAVE/REPLICA STATUS 字段多且各版本命名不一，必须按列名取值。

    ha_client.connect 默认是元组游标（见文档：游标铁律），按列名取值必须显式
    换成 DictCursor，否则 show_replica_status_sql 返回的行无法可靠解析。
    """
    try:
        from pymysql.cursors import DictCursor
        return conn.cursor(DictCursor)
    except Exception:
        return conn.cursor()


def _pick(row, *names):
    """在行里按多个候选列名取值（MySQL 8.0.22+ 与旧版字段名不同）。"""
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return None


def probe_node(host, port, user, password, timeout=5):
    """只读探测单个节点，返回结构化报告（**绝不写入任何数据**）。

    返回 dict（永远不抛异常，失败时 ok=False 且带 error）：
      ok / error / host / port
      version / family / series / level / supported / profile_note
      server_id / read_only / super_read_only / log_bin / binlog_format
      gtid_enabled / repl_mode
      role                : master / slave / standalone
      replica             : 复制状态（本节点作为从库时）/ None
      replicas            : 有多少从库连着本节点（判断它是不是主库）
      gtid_executed
      data_size_bytes
      grants              : 当前连接用户的权限（字符串列表）
      issues              : [{level, code, msg, fix}]
    """
    out = {
        "ok": False, "error": "", "host": host, "port": int(port),
        "version": "", "family": "", "series": "", "level": _vp.LEVEL_UNKNOWN,
        "supported": False, "profile_note": "",
        "server_id": None, "read_only": None, "super_read_only": None,
        "log_bin": None, "binlog_format": "",
        "bind_address": "", "gtid_strict_mode": "", "deploy_form": "bare",
        "log_bin_basename": "",
        "gtid_enabled": False, "repl_mode": _vp.REPL_NONE, "gtid_domain_id": None,
        "role": "standalone", "replica": None, "replicas": 0,
        "gtid_executed": "", "data_size_bytes": 0,
        "grants": [], "issues": [],
        "can_tune": False, "fix_cmd": "", "suggested_server_id": None,
    }
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
    except Exception as e:
        out["error"] = "连接失败：%s" % e
        out["issues"].append(_issue(
            LV_BLOCKER, "connect_failed", out["error"],
            "检查地址/端口/账号密码，以及节点是否放行管理器来源 IP"))
        return out
    if conn is None:
        out["error"] = "连接失败（connect 返回 None）"
        out["issues"].append(_issue(LV_BLOCKER, "connect_failed", out["error"], ""))
        return out

    try:
        cur = conn.cursor()
        out["ok"] = True

        # ---- 版本与能力档案 ----
        ver = _scalar(cur, "SELECT VERSION()") or ""
        out["version"] = str(ver)
        p = _vp.profile(ver)
        out["family"] = p["family"]
        out["series"] = p["series"]
        out["level"] = p["level"]
        out["supported"] = _vp.is_supported(ver)
        out["profile_note"] = p.get("notes", "")
        # 接管可能需要显式开启的开关（log_bin 默认 OFF 等），向导作为 opt-in 选项提供
        out["switches"] = _vp.adopt_switch_requirements(ver)

        flavour = detect_flavour(cur)

        # ---- 关键变量（逐项取，缺失属正常）----
        out["server_id"] = _scalar(cur, "SELECT @@server_id")
        out["read_only"] = _scalar(cur, "SELECT @@read_only")
        out["super_read_only"] = _scalar(cur, "SELECT @@super_read_only")
        out["log_bin"] = _scalar(cur, "SELECT @@log_bin")
        out["binlog_format"] = str(_scalar(cur, "SELECT @@binlog_format") or "")
        # bind_address：决定 VIP 漂移过来后能不能连上（只监听 127.0.0.1 会直接拒连）
        out["bind_address"] = str(_scalar(cur, "SELECT @@bind_address") or "")
        # binlog 文件名基线：修复命令沿用原 basename，避免改变既有 binlog 文件名
        out["log_bin_basename"] = str(_scalar(cur, "SELECT @@log_bin_basename") or "")
        # gtid_strict_mode：Custos 复制走 GTID(slave_pos)，MariaDB 默认为 OFF 必须显式开
        if p["family"] == _vp.FAMILY_MARIADB:
            out["gtid_strict_mode"] = _scalar_str(cur, "SELECT @@gtid_strict_mode")
        # 部署形态（裸机/容器）管理器从数据库端口猜不出来，只能给建议值交由用户确认：
        # 非 3306 端口多为面板/容器化部署（如 1Panel 的 3307），3306 多为裸机。
        out["deploy_form"] = "container" if int(port) != 3306 else "bare"

        # ---- GTID 与复制模式 ----
        if p["family"] == _vp.FAMILY_MARIADB:
            out["gtid_executed"] = str(_scalar(cur, "SELECT @@gtid_slave_pos") or "")
            out["gtid_domain_id"] = _scalar(cur, "SELECT @@gtid_domain_id")
            # MariaDB 用 SHOW SLAVE STATUS 的 Using_Gtid 判定（No / Slave_pos / Current_pos）
        else:
            out["gtid_executed"] = str(_scalar(cur, "SELECT @@gtid_executed") or "")
            gm = str(_scalar(cur, "SELECT @@gtid_mode") or "")
            out["gtid_enabled"] = (gm.upper() == "ON")

        # ---- 复制状态（本节点作为从库）----
        try:
            dcur = _dict_cursor(conn)
            dcur.execute(show_replica_status_sql(flavour))
            row = dcur.fetchone()
            if row:
                row = dict(row) if not isinstance(row, dict) else row
                io = str(_pick(row, "Replica_IO_Running", "Slave_IO_Running") or "")
                sqlthr = str(_pick(row, "Replica_SQL_Running", "Slave_SQL_Running") or "")
                using_gtid = str(_pick(row, "Using_Gtid") or "")
                auto_pos = _pick(row, "Auto_Position")
                out["replica"] = {
                    "source_host": _pick(row, "Source_Host", "Master_Host"),
                    "source_port": _pick(row, "Source_Port", "Master_Port"),
                    "io_running": io,
                    "sql_running": sqlthr,
                    "seconds_behind": _pick(row, "Seconds_Behind_Source", "Seconds_Behind_Master"),
                    "last_io_error": str(_pick(row, "Last_IO_Error") or ""),
                    "last_sql_error": str(_pick(row, "Last_SQL_Error") or ""),
                    "using_gtid": using_gtid,
                    "auto_position": auto_pos,
                }

                if p["family"] == _vp.FAMILY_MARIADB:
                    out["repl_mode"] = (_vp.REPL_GTID if using_gtid.upper() in ("SLAVE_POS", "CURRENT_POS")
                                        else _vp.REPL_FILE_POS)
                else:
                    out["repl_mode"] = (_vp.REPL_GTID if str(auto_pos) == "1" else _vp.REPL_FILE_POS)
                out["role"] = "slave"
        except Exception:
            out["replica"] = None

        # ---- 有多少从库连着本节点（判断它是不是主库）----
        try:
            dcur = _dict_cursor(conn)
            dcur.execute(show_replicas_sql(flavour))
            rows = dcur.fetchall() or []
            out["replicas"] = len(rows)
            if out["replicas"] > 0:
                out["role"] = "master"
        except Exception:
            out["replicas"] = 0

        # ---- 数据量（评估导出耗时）----
        try:
            cur.execute(
                "SELECT IFNULL(SUM(data_length + index_length), 0) FROM information_schema.tables "
                "WHERE table_schema NOT IN "
                "('information_schema','mysql','performance_schema','sys','test')")
            r = cur.fetchone()
            out["data_size_bytes"] = int(r[0]) if r and r[0] is not None else 0
        except Exception:
            out["data_size_bytes"] = 0

        # ---- 当前连接用户权限 ----
        try:
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            out["grants"] = [str((g[0] if isinstance(g, (tuple, list)) else g))
                             for g in (cur.fetchall() or [])]
        except Exception:
            out["grants"] = []

        # ---- 阻断项 / 提示项 ----
        if not out["supported"]:
            out["issues"].append(_issue(
                LV_WARN, "version_unknown",
                "版本 %s 未登记在支持矩阵：可纳管（装 agent + VIP），但「建复制/切主」未经验证"
                % (out["version"] or "?")[:40],
                "确认该版本的复制/权限行为后再启用切主；或先升级到已登记版本"))

        # 说明只写「要改什么」，具体命令统一放在节点级 fix_cmd（避免同一条命令重复展示）
        sid = out["server_id"]
        if sid in (None, 0):
            out["issues"].append(_issue(
                LV_BLOCKER, "server_id_missing",
                "server_id 为 0 或未设置，无法建立复制",
                "配置里补 server_id（非 0、集群内唯一）→ 见下方修复命令"))

        if str(out["log_bin"]).upper() in ("OFF", "0", "NONE"):
            out["issues"].append(_issue(
                LV_BLOCKER, "binlog_off",
                "未开启 binlog，无法做复制（单机扩从库场景必须先开）",
                "配置里开 log_bin（只读变量，须重启）→ 见下方修复命令"))

        if out["binlog_format"] and out["binlog_format"].upper() != _ROW_FORMAT:
            out["issues"].append(_issue(
                LV_WARN, "binlog_format_not_row",
                "binlog_format=%s，非 ROW 格式在部分语句下复制不安全" % out["binlog_format"],
                "建议改为 ROW（须重启）→ 见下方修复命令"))

        # ---- read_only / super_read_only：在这上面建 Custos 账号会失败，且不能当主库写入 ----
        _ro = str(out.get("read_only") or "")
        _sro = str(out.get("super_read_only") or "")
        if _ro.upper() in ("1", "ON") or _sro.upper() in ("1", "ON"):
            out["issues"].append(_issue(
                LV_BLOCKER, "read_only_on",
                "该实例处于只读状态（read_only=%s / super_read_only=%s）：建 Custos 账号会失败，"
                "且无法作为主库接受写入" % (_ro or "?", _sro or "?"),
                "解除只读（standalone 可自动解除；若它仍是从库须先停复制）→ 见下方修复命令"))

        # ---- 仍是别人的从库：纳管为独立主库会造成数据分叉 ----
        if out.get("role") == "slave":
            _src = (out.get("replica") or {}).get("source_host") or "未知主库"
            out["issues"].append(_issue(
                LV_BLOCKER, "still_slave",
                "该实例仍在从 %s 复制数据：纳管为独立集群主库会造成数据分叉" % _src,
                "先断开与原主库的关系：STOP REPLICA; RESET REPLICA ALL;（不可逆，需确认）"
                "→ 见下方修复命令"))

        # ---- bind_address 受限：VIP 漂移后连不上 ----
        _ba = (out.get("bind_address") or "").strip()
        if _ba and _ba not in ("0.0.0.0", "*", "::", "0.0.0.0/0"):
            out["issues"].append(_issue(
                LV_BLOCKER, "bind_address_restricted",
                "bind_address=%s：MariaDB 只监听该地址，VIP 漂移后将无法连接" % _ba,
                "配置里设 bind-address=0.0.0.0（须重启）→ 见下方修复命令"))

        # ---- gtid_strict_mode：Custos 复制用 GTID(slave_pos)，MariaDB 默认 OFF ----
        _gsm = str(out.get("gtid_strict_mode") or "")
        if p["family"] == _vp.FAMILY_MARIADB and _gsm.upper() in ("OFF", "0"):
            out["issues"].append(_issue(
                LV_BLOCKER, "gtid_strict_off",
                "gtid_strict_mode=%s：Custos 复制使用 GTID（slave_pos），必须先开启" % _gsm,
                "配置里开 gtid_strict_mode=ON（须重启）→ 见下方修复命令"))

        # 探测账号够不够格「在线改全局参数」（root 级且非只读副本）→ 决定 UI 是否给快速修复开关
        out["can_tune"] = _can_tune(out)

        r = out["replica"]
        if r and (str(r.get("io_running")).upper() != "YES" or str(r.get("sql_running")).upper() != "YES"):
            out["issues"].append(_issue(
                LV_BLOCKER, "repl_broken",
                "现有复制未正常运行（IO=%s SQL=%s）" % (r.get("io_running"), r.get("sql_running")),
                "先修复现有复制再接管；错误：%s"
                % (r.get("last_io_error") or r.get("last_sql_error") or "无")[:200]))

        return out
    except Exception as e:
        out["ok"] = False
        out["error"] = "探测异常：%s" % e
        out["issues"].append(_issue(LV_BLOCKER, "probe_error", out["error"], ""))
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def same_l2(ips, prefix=24):
    """判断一组 IP 是否落在同一网段（VRRP/VIP 漂移要求同二层）。

    为什么只比网段：老库接管时节点上还没有 custos-node-agent，无法做真正的
    二层探测（ARP/VRRP 报文）。网段一致是必要条件，能拦掉绝大多数「跨机房/跨 VLAN」
    这类 VIP 根本漂不过去的情况；真正的 L2 连通性在 agent 装好后由 keepalived 实测。

    返回 (bool, 说明)
    """
    nets = []
    bad = []
    for s in (ips or []):
        try:
            nets.append(ipaddress.ip_network("%s/%d" % (s.strip(), int(prefix)), strict=False))
        except Exception:
            bad.append(s)
    if bad:
        return False, "IP 无法解析：%s" % "、".join(bad)
    if not nets:
        return False, "未给出任何节点 IP"
    first = nets[0]
    for n in nets[1:]:
        if n != first:
            return False, "节点不在同一网段：%s 与 %s（VIP 依赖 VRRP，跨网段不可用）" % (first, n)
    return True, "同一网段 %s" % first


def probe_cluster(nodes, l2_prefix=24):
    """探测一组节点并给出**集群级**结论（只读，可反复跑）。

    nodes: [{"host":..., "port":..., "user":..., "password":...}, ...]

    返回：
      nodes    : 各节点探测结果
      scenario : existing_repl / single / unknown
      verdict  : ready / needs_change / unsupported
      issues   : 集群级问题（版本一致性、server_id 唯一、同网段、复制健康）
      l2_ok / l2_note
      master   : 识别出的主库 host（可能为空）
    """
    res = {
        "nodes": [], "scenario": SCENARIO_UNKNOWN, "verdict": VERDICT_UNSUPPORTED,
        "issues": [], "l2_ok": False, "l2_note": "", "master": "",
        "repl_mode": _vp.REPL_NONE,
    }
    nodes = list(nodes or [])
    if not nodes:
        res["issues"].append(_issue(LV_BLOCKER, "no_nodes", "未给出任何节点", ""))
        return res

    probed = [probe_node(n.get("host"), n.get("port"), n.get("user"), n.get("password"))
              for n in nodes]
    res["nodes"] = probed

    # ---- 同网段（VIP 前提）----
    ok, note = same_l2([p.get("host") for p in probed if p.get("host")], l2_prefix)
    res["l2_ok"], res["l2_note"] = ok, note
    if not ok:
        res["issues"].append(_issue(LV_BLOCKER, "not_same_l2", note,
                                    "VIP 漂移依赖 VRRP，必须同二层网络；"
                                    "把节点迁到同一 VLAN 或改用其他容灾方案"))

    alive = [p for p in probed if p.get("ok")]
    if not alive:
        res["issues"].append(_issue(
            LV_BLOCKER, "all_unreachable", "所有节点都无法连接", "检查地址/端口/账号密码"))
        return res

    # ---- 同版本（用户要求非混布、只做同版本集群）----
    series = {p.get("series") for p in alive if p.get("series")}
    families = {p.get("family") for p in alive if p.get("family")}
    if len(families) > 1:
        res["issues"].append(_issue(
            LV_BLOCKER, "family_mismatch",
            "混布了不同引擎族：%s（MySQL 与 MariaDB 的 GTID 机制根本不兼容）" % "、".join(sorted(families)),
            "一个集群只能是一种引擎族"))
    if len(series) > 1:
        res["issues"].append(_issue(
            LV_BLOCKER, "version_mismatch",
            "集群内版本不一致：%s（要求同版本）" % "、".join(sorted(series)),
            "统一到同一版本后再接管"))

    # ---- server_id 唯一性 ----
    seen = {}
    for p in alive:
        sid = p.get("server_id")
        if sid in (None, 0):
            continue
        if sid in seen:
            res["issues"].append(_issue(
                LV_BLOCKER, "server_id_dup",
                "server_id %s 在 %s 与 %s 上重复（撞 id 会直接搞坏复制）"
                % (sid, seen[sid], p.get("host")),
                "为其中一个节点改 server_id 后重启数据库"))
        else:
            seen[sid] = p.get("host")

    # ---- 为需要整改的节点生成「可直接复制」的修复命令 ----
    # server_id 建议值在本次集群内去重（撞号会直接搞坏复制）；命令只写独立的新配置文件，
    # 绝不改动旧库现有 my.cnf（生产铁律）。
    used = set()
    for p in alive:
        try:
            v = int(p.get("server_id") or 0)
        except Exception:
            v = 0
        if v > 0:
            used.add(v)
    for p in alive:
        need_sid = p.get("server_id") in (None, 0)
        sug = suggest_server_id(p.get("host"), used) if need_sid else p.get("server_id")
        if need_sid:
            used.add(sug)
        p["suggested_server_id"] = sug
        # 一律显式固化 binlog（理由见 fix_command_for：@@log_bin=1 也可能没写进配置）
        need_bin = True
        need_fmt = bool(p.get("binlog_format")) and str(p.get("binlog_format")).upper() != _ROW_FORMAT
        need_gtid = str(p.get("gtid_strict_mode") or "").upper() in ("OFF", "0")
        _ba = (p.get("bind_address") or "").strip()
        need_bind = bool(_ba) and _ba not in ("0.0.0.0", "*", "::", "0.0.0.0/0")
        need_ro = (str(p.get("read_only") or "").upper() in ("1", "ON")
                   or str(p.get("super_read_only") or "").upper() in ("1", "ON"))
        if need_sid or need_bin or need_fmt or need_gtid or need_bind or need_ro:
            p["fix_cmd"] = build_fix_command(
                server_id=(sug if need_sid else None),
                log_bin=need_bin, binlog_format=need_fmt,
                gtid_strict=need_gtid, bind_addr=need_bind, read_only_rel=need_ro,
                container=(p.get("container") or ""),
                log_bin_name=(p.get("log_bin_basename") or None))

    # ---- 场景识别 ----
    slaves = [p for p in alive if p.get("role") == "slave"]
    masters = [p for p in alive if p.get("role") == "master"]
    if slaves:
        res["scenario"] = SCENARIO_EXISTING_REPL
        # 主库 = 被从库指向的那个节点
        target = None
        for p in alive:
            r = p.get("replica") or {}
            target = r.get("source_host") or target
        if not target and masters:
            target = masters[0].get("host")
        res["master"] = target or ""
        res["repl_mode"] = (slaves[0].get("repl_mode") or _vp.REPL_NONE)
    elif len(alive) == 1:
        res["scenario"] = SCENARIO_SINGLE
        res["master"] = alive[0].get("host") or ""
    else:
        res["scenario"] = SCENARIO_UNKNOWN

    # ---- 汇总结论 ----
    blockers = [i for i in res["issues"] if i["level"] == LV_BLOCKER]
    for p in probed:
        blockers.extend([i for i in (p.get("issues") or []) if i["level"] == LV_BLOCKER])

    if not alive:
        res["verdict"] = VERDICT_UNSUPPORTED
    elif blockers:
        res["verdict"] = VERDICT_NEEDS_CHANGE
    elif not any(p.get("supported") for p in alive):
        res["verdict"] = VERDICT_UNSUPPORTED
    else:
        res["verdict"] = VERDICT_READY

    res["scenario_label"] = SCENARIO_LABEL.get(res["scenario"], res["scenario"])
    return res


# --------------------------------------------------------------------------
# 阶段 1：adopt（建账号 + 登记 + 装 sidecar）
#
# 只读探测见 probe_node / probe_cluster（幂等、可反复跑、绝不改动老库）。
# 以下为「动手」部分——仅建立 Custos 接管账号，不动任何复制拓扑。
# 登记 nodes/hosts 与装 sidecar 由 API 层在账号建立成功后调用（阶段 1 后续）。
# --------------------------------------------------------------------------

def adopt_node(node, root_user, root_pass, creds, manager_ip="", manager_subnet=None):
    """在旧节点上建立 Custos 接管账号（ha_monitor / replication / app / root@'<管理器IP>'）。

    复用 ha_client.credentials.ensure_ha_creds 的完整自愈（账号不存在则 CREATE + GRANT、
    密码漂移则 ALTER、root 回退），MariaDB 与 MySQL 风味自动分派。host 集合由
    config.ACCOUNT_HOST_MODE 决定（segment 模式 = 仅网段%，落实「去 %」策略，最多开放内网段%）。

    :param node: probe_node 结果 dict（需含 host / port / flavour / reachable）
    :param root_user / root_pass: 旧节点 root（compose 固定 MYSQL_ROOT_PASSWORD）
    :param creds: Custos 接管账号凭据
        {"ha_user","ha_pass","repl_user","repl_pass","app_user","app_pass"}
        取自管理器统一凭据（AUTH 设置），全 Custos 集群一致
    :return: (ok: bool, msg: str)
    """
    # ⚠ 修正：probe_node 的输出里是 "ok"，没有 "reachable" 字段。
    # 原写法 `node.get("reachable")` 恒为假，会让每个节点都直接返回失败——
    # 接管流程在第一步就卡死，且错误信息「节点不可达」极具误导性（其实是字段对不上）。
    if not (node.get("ok") or node.get("reachable")):
        return False, "节点探测未通过，无法建立账号（请先确认探测结果）"
    from app.ha_client.credentials import ensure_ha_creds
    ok, msg = ensure_ha_creds(
        host=node["host"], port=node["port"],
        root_user=root_user, root_pass=root_pass,
        ha_user=creds.get("ha_user", ""), ha_pass=creds.get("ha_pass", ""),
        repl_user=creds.get("repl_user", ""), repl_pass=creds.get("repl_pass", ""),
        app_user=creds.get("app_user", ""), app_pass=creds.get("app_pass", ""),
        manager_ip=manager_ip, manager_subnet=manager_subnet,
    )
    return ok, msg


def test_root_credential(host, port, user, password, timeout=5):
    """验证「某台节点填的数据库 root 口令」连不连得上、够不够权限。

    只读：只连一下、读版本和权限，**不建任何账号、不改任何数据**。
    接管要靠这套口令在每台库上建 Custos 账号，所以除了连通，还要确认它有
    SUPER / ALL PRIVILEGES，且该节点不是只读的（只读库建不了账号）。

    返回 {ok, host, port, version, root_like, read_only, msg}
    """
    out = {"ok": False, "host": host, "port": int(port), "version": "",
           "root_like": False, "read_only": False, "msg": ""}
    conn = None
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
        if conn is None:
            out["msg"] = "连不上（地址/端口不对，或账号密码不对，或节点拒绝连接）"
            return out
        cur = conn.cursor()
        out["version"] = str(_scalar(cur, "SELECT VERSION()") or "")
        ro = str(_scalar(cur, "SELECT @@read_only") or "")
        out["read_only"] = ro.upper() in ("1", "ON")
        grants = []
        try:
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            grants = [str(g[0] if isinstance(g, (tuple, list)) else g)
                      for g in (cur.fetchall() or [])]
        except Exception:
            grants = []
        up = " ".join(grants).upper()
        out["root_like"] = bool(up) and (("ALL PRIVILEGES" in up) or ("SUPER" in up))

        out["ok"] = True
        if out["read_only"]:
            out["msg"] = "能连上，但该节点是只读的（多半是从库），在这上面建账号会失败"
        elif not out["root_like"]:
            out["msg"] = "能连上，但权限不够（缺 SUPER / ALL PRIVILEGES），建不了 Custos 账号"
        else:
            out["msg"] = "连通正常，权限足够" + ("（%s）" % out["version"] if out["version"] else "")
        return out
    except Exception as e:
        out["msg"] = "连不上：%s" % e
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def node_root_for(per_node_roots, node, root_user, root_pass):
    """取该节点的数据库 root 凭据：**优先用这台单独填的**，没填才回落到统一那份。

    生产环境每台库的 root 口令往往不一样，接管必须支持逐台指定，否则口令不同的
    节点会建账号失败。per_node_roots 形如
    {"192.0.2.50:3306": {"user": "root", "password": "..."}}（键也允许只写 host）。
    """
    host = str(node.get("host") or "").strip()
    try:
        port = int(node.get("port") or 3306)
    except Exception:
        port = 3306
    if per_node_roots:
        for key in ("%s:%d" % (host, port), host):
            v = per_node_roots.get(key)
            if not isinstance(v, dict):
                continue
            u = str(v.get("user") or "").strip()
            if u:
                return u, v.get("password") or ""
    return root_user, root_pass


def adopt_cluster(probe, root_user, root_pass, creds, manager_ip="", manager_subnet=None,
                  per_node_roots=None):
    """对 probe_cluster 结果逐个节点建立接管账号。

    :param probe: probe_cluster 返回（含 nodes 列表）
    :param per_node_roots: 每台节点各自的 root 凭据（生产环境口令常不同）；
        没给的节点回落到 root_user / root_pass。
    :return: [{"host","ok","msg"}, ...]
    """
    results = []
    for node in probe.get("nodes", []):
        u, p = node_root_for(per_node_roots, node, root_user, root_pass)
        ok, msg = adopt_node(node, u, p, creds, manager_ip, manager_subnet)
        results.append({"host": node.get("host"), "ok": ok, "msg": msg})
    return results


# --------------------------------------------------------------------------
# 阶段 1 后半：登记 nodes/hosts + 生成 sidecar 单元 + 给出引导命令
#
# 接管**不是重建**：老库正在跑业务，所以
#   1. 只登记，不动数据库本体；
#   2. sidecar（keepalived + agent）走 curl 引导命令在节点上装，
#      且**必须 sidecar_only**——不能跑 Install-ha-mariadb-Node.sh，那等于重装数据库。
# --------------------------------------------------------------------------

def _node_type_for(probed):
    """按探测结果决定登记形态。

    接管的库都是**已经在跑**的实例，按裸机 MariaDB 登记（mariadb）：
    它的部署单元只含 keepalived + agent 两个 sidecar，不会碰数据库本体。
    MySQL 族登记为容器形态（docker），同理只装 sidecar。
    """
    family = (probed.get("family") or "").lower()
    return "docker" if family == "mysql" else "mariadb"


def register_and_bootstrap(probe, cluster_id, root_user="root", root_pass="",
                           actor="", ttl_hours=24, per_node_roots=None):
    """登记被接管节点并生成「一行引导命令」。

    :param per_node_roots: 每台节点各自的 root 凭据，登记时按节点取用（同 adopt_cluster）。
    :return: [{"ip","ok","command","msg"}, ...]
    """
    from . import node_gen, bootstrap
    results = []
    master = (probe.get("master") or "").strip()
    for p in probe.get("nodes", []):
        ip = (p.get("host") or "").strip()
        if not ip or not p.get("ok"):
            results.append({"ip": ip, "ok": False, "command": "",
                            "msg": "节点探测失败，跳过登记"})
            continue
        role = "master" if (master and ip == master) else "slave"
        ntype = _node_type_for(p)
        u, pw = node_root_for(per_node_roots, p, root_user, root_pass)
        try:
            node_gen.generate_node(
                ip, int(p.get("port") or 3306), u, pw,
                role, int(cluster_id),
                node_type=ntype,
                mysql_version=(p.get("version") or "")[:48] or None,
                server_id=p.get("server_id") or None,
            )
        except Exception as e:
            results.append({"ip": ip, "ok": False, "command": "",
                            "msg": "登记失败：%s" % e})
            continue
        try:
            info = bootstrap.issue_for_node(
                ip, node_type=ntype, cluster_id=int(cluster_id), actor=actor,
                ttl_hours=ttl_hours, sidecar_only=True)
            results.append({"ip": ip, "ok": True, "command": info["command"],
                            "msg": "已登记，到节点执行引导命令装 sidecar（keepalived + agent）"})
        except Exception as e:
            results.append({"ip": ip, "ok": False, "command": "",
                            "msg": "生成引导命令失败：%s" % e})
    return results
