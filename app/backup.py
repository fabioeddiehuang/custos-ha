"""集群逻辑备份 / 恢复（纯 Python，基于 PyMySQL）。

为什么不用 mysqldump
--------------------
管理器镜像是 python:3.11-slim，容器内没有 mysql 客户端；而且 MariaDB 的
mysqldump 去连 MySQL 8.0 会撞上 caching_sha2_password 等鉴权/语法差异。
这里直接复用 ha_client.connect（与全站一致的鉴权、超时、字符集策略），
对 MySQL 8.0 与 MariaDB 一视同仁，且无需给镜像增加任何系统依赖。

产物
----
  /app/backups/<集群名称>/<库名>_<YYYYmmdd-HHMMSS>.sql.gz          指定库备份
  /app/backups/<集群名称>/All-Databases_<YYYYmmdd-HHMMSS>.sql.gz   全库备份

  同名 .meta.json 为备份元信息；所有时间戳一律为中国时间（Asia/Shanghai, UTC+8）。

备份范围
--------
  scope = "all"    全库（排除 information_schema / mysql / performance_schema / sys）
  scope = "custom" 指定库（databases 列表）

注意：恢复脚本默认【不关闭 binlog】，因此在主库执行恢复会自然复制到从库，
      与 mysqldump 在复制拓扑下的常规行为一致。若只想在单机恢复，请手动在
      会话里先执行 SET SESSION sql_log_bin=0。
"""

from __future__ import annotations

import gzip
import json
import os
import time
import re
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pymysql
from pymysql.cursors import SSCursor

# 备份根目录：容器内 /app/backups（docker-compose 挂到项目根 ./backups 持久化）
BACKUP_ROOT = Path(os.environ.get("BACKUPS_DIR") or "/app/backups")

# 系统库不参与备份
SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys", "ndbinfo"}

# 单条 INSERT 的近似体积上限（字节）。超过就切一条新 INSERT，
# 避免单条语句超过 max_allowed_packet 导致恢复失败。
_INSERT_MAX_BYTES = 900 * 1024


# --------------------------------------------------------------------------
# 基础设施
# --------------------------------------------------------------------------
# 全站统一使用中国时间（Asia/Shanghai = UTC+8）：不依赖容器 TZ，直接做偏移。
CN_TZ_OFFSET_HOURS = 8


def now_cn() -> datetime:
    """当前中国时间（naive datetime，已含 +08:00 偏移）。"""
    return datetime.utcnow() + timedelta(hours=CN_TZ_OFFSET_HOURS)


def from_ts_cn(ts: float) -> datetime:
    """把文件 mtime 等 POSIX 时间戳换算成中国时间。"""
    return datetime.utcfromtimestamp(ts) + timedelta(hours=CN_TZ_OFFSET_HOURS)


def _safe_name(s) -> str:
    """集群名可能含路径分隔符等字符，落盘前做安全化。"""
    t = re.sub(r'[\\/:*?"<>|\s]+', "_", str(s or "")).strip("_")
    return t or "cluster"


def _cluster_dir_name(cid) -> str:
    """备份目录按【集群名称】命名；查不到时退回集群 ID。"""
    try:
        from .db import query
        rows = query("SELECT name FROM clusters WHERE id=%s", (cid,))
        if rows and rows[0].get("name"):
            return _safe_name(rows[0]["name"])
    except Exception:
        pass
    return str(cid)


def _dir(cid) -> Path:
    d = BACKUP_ROOT / _cluster_dir_name(cid)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _conn(cid, host, port, timeout: int = 10):
    """用节点 root 建连（备份/恢复都是高权限操作，与终端一致走 root）。"""
    from .ha_client import connect
    from .failover import node_root

    user, pwd = node_root(host, cid)
    if not pwd:
        raise RuntimeError(
            f"节点 {host} 未保存 root 密码，无法执行备份/恢复。"
            f"请到「节点管理」确认该节点已登记 root 凭证。"
        )
    return connect(host, int(port), user or "root", pwd, timeout=timeout)


def _ident(name: str) -> str:
    """反引号包裹标识符，内部的反引号转义为两个。"""
    return "`" + str(name).replace("`", "``") + "`"


def _lit(v) -> str:
    """把 Python 值转成 SQL 字面量。"""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int,)):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        # 二进制一律走 16 进制字面量，彻底规避转义/字符集问题
        return "0x" + bytes(v).hex()
    if isinstance(v, datetime):
        return "'" + v.strftime("%Y-%m-%d %H:%M:%S") + "'"
    s = str(v)
    s = (s.replace("\\", "\\\\")
           .replace("'", "''")
           .replace("\n", "\\n")
           .replace("\r", "\\r")
           .replace("\x00", "\\0"))
    return "'" + s + "'"


# --------------------------------------------------------------------------
# 库列表
# --------------------------------------------------------------------------
def list_databases(cid, host, port):
    """列出该节点上可备份的业务库（排除系统库）。"""
    conn = _conn(cid, host, port)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            dbs = [r[0] for r in cur.fetchall()]
        return {"ok": True, "databases": sorted(d for d in dbs if d.lower() not in SYSTEM_DBS)}
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 备份
# --------------------------------------------------------------------------
def _scalar(cur, sql, args=None):
    cur.execute(sql, args or ())
    row = cur.fetchone()
    return row[0] if row else None


def _dump_database(conn, cur, w, db, with_data: bool,
                   host=None, port=None, cid=None) -> dict:
    """导出单个库（表结构 + 数据 + 视图），返回统计。

    数据流式读取使用「独立连接」上的服务端游标（SSCursor），与跑元数据的
    `cur`（同一连接）彻底解耦——这正是之前「全库备份失败」的根因：
    同一连接上 SSCursor 与常规游标交替使用会触发 MySQL
    "Commands out of sync"。单表导出失败不再中断整库备份。
    """
    stat = {"tables": 0, "views": 0, "rows": 0}

    w(f"CREATE DATABASE IF NOT EXISTS {_ident(db)} "
      f"/*!40100 DEFAULT CHARACTER SET utf8mb4 */;\n")
    w(f"USE {_ident(db)};\n\n")

    # 单独开一个连接专供数据流式导出，避免与上面的元数据游标争用同一连接
    sconn = None
    if with_data:
        try:
            sconn = _conn(cid, host, port) if (cid is not None) else conn
        except Exception as e:
            w(f"-- 数据导出连接建立失败（仅跳过数据，结构仍会导出）：{e}\n")

    # ---- 基表 ----
    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (db,))
    tables = [r[0] for r in cur.fetchall()]

    for t in tables:
        try:
            cur.execute(f"SHOW CREATE TABLE {_ident(db)}.{_ident(t)}")
            row = cur.fetchone()
            if not row:
                continue
            ddl = row[1]
            w(f"--\n-- 表结构 {db}.{t}\n--\n")
            w(f"DROP TABLE IF EXISTS {_ident(t)};\n")
            w(ddl + ";\n\n")
            stat["tables"] += 1

            if not with_data or sconn is None:
                continue

            # 用独立连接上的服务端游标流式读取：缓冲游标会在 execute 时把整张表
            # 拉进内存，大表会直接把管理器打爆；SSCursor 必须读完才能关闭结果集。
            with sconn.cursor(SSCursor) as scur:
                scur.execute(f"SELECT * FROM {_ident(db)}.{_ident(t)}")
                cols = [d[0] for d in (scur.description or [])]
                if not cols:
                    continue
                prefix = (f"INSERT INTO {_ident(t)} ("
                          + ",".join(_ident(c) for c in cols) + ") VALUES\n")
                buf, bufsize = [], 0
                while True:
                    rows = scur.fetchmany(500)
                    if not rows:
                        break
                    for r in rows:
                        piece = "(" + ",".join(_lit(v) for v in r) + ")"
                        buf.append(piece)
                        bufsize += len(piece) + 2
                        stat["rows"] += 1
                    if bufsize >= _INSERT_MAX_BYTES:
                        w("--\n-- 数据 " + f"{db}.{t}\n--\n")
                        w(prefix + ",\n".join(buf) + ";\n\n")
                        buf, bufsize = [], 0
                if buf:
                    w("--\n-- 数据 " + f"{db}.{t}\n--\n")
                    w(prefix + ",\n".join(buf) + ";\n\n")
        except Exception as e:
            w(f"-- 表 {db}.{t} 导出失败（已跳过该表）：{e}\n")
            stat["tables"] = max(0, stat["tables"] - 1)
            continue

    # ---- 视图（放在表之后，视图可能依赖表）----
    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='VIEW' ORDER BY TABLE_NAME", (db,))
    views = [r[0] for r in cur.fetchall()]
    for v in views:
        try:
            cur.execute(f"SHOW CREATE VIEW {_ident(db)}.{_ident(v)}")
            row = cur.fetchone()
            if not row:
                continue
            w(f"--\n-- 视图 {db}.{v}\n--\n")
            w(f"DROP VIEW IF EXISTS {_ident(v)};\n")
            w(row[1] + ";\n\n")
            stat["views"] += 1
        except Exception as e:
            w(f"-- 视图 {db}.{v} 导出失败: {e}\n")

    # ---- 触发器 / 存储过程 / 函数（失败不影响主流程）----
    try:
        cur.execute("SHOW TRIGGERS FROM " + _ident(db))
        trigs = cur.fetchall()
        for tg in trigs:
            name = tg[0]
            try:
                cur.execute(f"SHOW CREATE TRIGGER {_ident(db)}.{_ident(name)}")
                row = cur.fetchone()
                if row:
                    w(f"--\n-- 触发器 {db}.{name}\n--\n")
                    w(f"DROP TRIGGER IF EXISTS {_ident(name)};\n")
                    w("DELIMITER ;;\n" + row[2] + ";;\nDELIMITER ;\n\n")
            except Exception as e:
                w(f"-- 触发器 {db}.{name} 导出失败: {e}\n")
    except Exception:
        pass

    try:
        cur.execute(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES "
            "WHERE ROUTINE_SCHEMA=%s ORDER BY ROUTINE_NAME", (db,))
        routines = cur.fetchall()
    except Exception:
        routines = []
    for name, rtype in routines:
        try:
            kw = "PROCEDURE" if str(rtype).upper() == "PROCEDURE" else "FUNCTION"
            cur.execute(f"SHOW CREATE {kw} {_ident(db)}.{_ident(name)}")
            row = cur.fetchone()
            if row:
                w(f"--\n-- {kw} {db}.{name}\n--\n")
                w(f"DROP {kw} IF EXISTS {_ident(name)};\n")
                w("DELIMITER ;;\n" + row[2] + ";;\nDELIMITER ;\n\n")
        except Exception as e:
            w(f"-- {rtype} {db}.{name} 导出失败: {e}\n")

    if sconn is not None and sconn is not conn:
        try:
            sconn.close()
        except Exception:
            pass

    return stat


def run_backup(cid, host, port, databases=None, with_data: bool = True,
               scope: str = "all"):
    """执行备份。databases 为空(None/[]) 且 scope='all' 时备份全部业务库。"""
    conn = _conn(cid, host, port, timeout=15)
    ts = now_cn().strftime("%Y%m%d-%H%M%S")
    sql_path = None

    try:
        with conn.cursor() as cur:
            if scope == "all" or not databases:
                cur.execute("SHOW DATABASES")
                targets = [r[0] for r in cur.fetchall()
                           if r[0].lower() not in SYSTEM_DBS]
                scope = "all"
            else:
                targets = [d for d in databases if d.lower() not in SYSTEM_DBS]
                scope = "custom"
            if not targets:
                return {"ok": False, "error": "没有可备份的业务库"}

        # 命名规则：./backups/<集群名称>/<库名|All-Databases>_<时间戳>.sql.gz
        # 注意：库名可能含 "."，不能用 Path.with_suffix（会从最后一个点截断）。
        label = ("All-Databases" if scope == "all"
                 else "+".join(str(t) for t in targets))
        bdir = _dir(cid)
        sql_path = bdir / f"{label}_{ts}.sql.gz"
        meta_path = bdir / f"{label}_{ts}.meta.json"

        total = {"tables": 0, "views": 0, "rows": 0}
        with conn.cursor() as cur:
            with gzip.open(sql_path, "wt", encoding="utf-8", newline="\n") as f:
                def w(s):
                    f.write(s)

                w("-- Custos 逻辑备份\n")
                w(f"-- 集群     : {_cluster_dir_name(cid)} (id={cid})\n")
                w(f"-- 源节点    : {host}:{port}\n")
                w(f"-- 生成时间  : {now_cn().strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n")
                w(f"-- 备份范围  : {'全库' if scope == 'all' else '指定库'}\n")
                w(f"-- 包含数据  : {'是' if with_data else '否（仅结构）'}\n")
                w(f"-- 数据库    : {', '.join(targets)}\n")
                w("--\n")
                w("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
                w("/*!40101 SET NAMES utf8mb4 */;\n")
                w("SET FOREIGN_KEY_CHECKS=0;\n")
                w("SET UNIQUE_CHECKS=0;\n")
                w("SET TIME_ZONE='+08:00';\n")
                w("SET AUTOCOMMIT=0;\n\n")

                for db in targets:
                    st = _dump_database(conn, cur, w, db, with_data,
                                        host=host, port=port, cid=cid)
                    total["tables"] += st["tables"]
                    total["views"] += st["views"]
                    total["rows"] += st["rows"]

                w("SET FOREIGN_KEY_CHECKS=1;\n")
                w("SET UNIQUE_CHECKS=1;\n")
                w("COMMIT;\n")
                w("-- 备份结束\n")

        size = sql_path.stat().st_size
        meta = {
            "ts": ts,
            "time": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "Asia/Shanghai (UTC+8)",
            "cluster_id": int(cid),
            "cluster_name": _cluster_dir_name(cid),
            "label": label,
            "host": host, "port": int(port),
            "scope": scope,
            "databases": targets,
            "with_data": bool(with_data),
            "tables": total["tables"], "views": total["views"],
            "rows": total["rows"], "size": size,
            "file": sql_path.name,
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                             encoding="utf-8")
        return {"ok": True, "file": sql_path.name, "meta": meta}
    except Exception as e:
        # 备份失败要清理半截文件，避免留下不可用的残档
        try:
            if sql_path is not None and sql_path.exists():
                sql_path.unlink()
        except Exception:
            pass
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 备份列表 / 读取 / 删除
# --------------------------------------------------------------------------
def list_backups(cid):
    d = _dir(cid)
    out = []
    for p in sorted(d.glob("*.sql.gz"), reverse=True):
        meta_p = p.with_suffix("").with_suffix(".meta.json")
        meta = None
        if meta_p.exists():
            try:
                meta = json.loads(meta_p.read_text(encoding="utf-8"))
            except Exception:
                meta = None
        st = p.stat()
        out.append({
            "file": p.name,
            "size": st.st_size,
            "mtime": from_ts_cn(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "meta": meta,
        })
    return out


def purge_old_backups(cid, days):
    """按保留天数清理过期备份（连带删除配套 .meta.json）。

    days<=0 表示不清理。以文件 mtime 判定（与备份时间戳同为本地时间）。
    返回 {"deleted": [文件名...], "kept": 保留份数}。
    """
    try:
        days = int(days or 0)
    except (TypeError, ValueError):
        days = 0
    if days <= 0:
        return {"deleted": [], "kept": 0}
    cutoff = time.time() - days * 86400
    deleted, kept = [], 0
    for p in _dir(cid).glob("*.sql.gz"):
        try:
            if p.stat().st_mtime < cutoff:
                meta_p = p.with_suffix("").with_suffix(".meta.json")
                if meta_p.exists():
                    meta_p.unlink()
                p.unlink()
                deleted.append(p.name)
            else:
                kept += 1
        except Exception:
            kept += 1
    return {"deleted": deleted, "kept": kept}


def get_backup_path(cid, filename: str) -> Path:
    """只允许本集群目录下的文件名，杜绝 ../ 穿越。"""
    name = os.path.basename(filename)
    p = _dir(cid) / name
    if not p.exists():
        raise FileNotFoundError(f"备份文件不存在：{name}")
    return p


def delete_backup(cid, filename: str):
    p = get_backup_path(cid, filename)
    p.unlink()
    meta_p = p.with_suffix("").with_suffix(".meta.json")
    if meta_p.exists():
        meta_p.unlink()
    return {"ok": True, "msg": f"已删除 {p.name}"}


# --------------------------------------------------------------------------
# 语句切分（恢复用）
# --------------------------------------------------------------------------
def split_statements(text: str):
    """按 ; 切分 SQL，正确跳过字符串/标识符/注释，并支持 DELIMITER。"""
    stmts, buf = [], []
    i, n = 0, len(text)
    quote = None
    delimiter = ";"
    while i < n:
        ch = text[i]

        # 行注释
        if quote is None and text.startswith("--", i) and (i + 2 < n) and text[i + 2] in " \t\r\n":
            j = text.find("\n", i)
            i = n if j == -1 else j + 1
            buf.append("\n")
            continue
        if quote is None and text.startswith("#", i):
            j = text.find("\n", i)
            i = n if j == -1 else j + 1
            buf.append("\n")
            continue
        # 块注释
        if quote is None and text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j == -1 else j + 2
            continue

        if quote:
            buf.append(ch)
            # 反斜杠转义（MySQL 默认开启）
            if ch == "\\" and i + 1 < n:
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue

        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
            i += 1
            continue

        # DELIMITER 指令（整行）
        if text.startswith("DELIMITER", i) and (i == 0 or text[i - 1] == "\n"):
            j = text.find("\n", i)
            line = text[i:(n if j == -1 else j)].strip()
            parts = line.split(None, 1)
            if len(parts) == 2:
                delimiter = parts[1].strip()
            i = n if j == -1 else j + 1
            continue

        if text.startswith(delimiter, i):
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
            i += len(delimiter)
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


# --------------------------------------------------------------------------
# 恢复
# --------------------------------------------------------------------------
def restore_backup(cid, host, port, filename, databases=None, no_binlog=False):
    """把备份恢复到指定节点。

    databases 为空则恢复备份内的全部库；否则只恢复其中的指定库
    （按备份内的 CREATE DATABASE / USE 段落过滤）。

    no_binlog=True 时在本会话执行 SET sql_log_bin=0：
      重建从库必须关掉，否则恢复产生的海量 DML 会写入从库 binlog、生成新的
      GTID 集合，与主库历史 GTID 冲突，重链时被判「分叉」进而触发 RESET MASTER。
      该变量是 session 级（不能 SET GLOBAL），故只能在本函数建连后设置。
    """
    path = get_backup_path(cid, filename)
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        return {"ok": False, "error": f"读取备份失败：{e}"}

    stmts = split_statements(text)

    # 只恢复指定库：按 USE 语句切段
    if databases:
        want = set(databases)
        filtered, keep = [], False
        for s in stmts:
            head = s.lstrip().upper()
            # 建库语句出现在 USE 之前，必须一并保留，
            # 否则「只恢复指定库」且该库在目标节点尚不存在时会直接报错。
            m = re.match(r"^CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([^`\s]+)`?",
                         s.strip(), re.I)
            if m and m.group(1) in want:
                filtered.append(s)
                continue
            if head.startswith("USE "):
                db = s.strip().split(None, 1)[1].strip().strip("`").rstrip(";").strip("`")
                keep = db in want
                if keep:
                    filtered.append(s)
                continue
            if keep:
                filtered.append(s)
        # 头部 SET 语句始终保留
        head_stmts = [s for s in stmts if s.lstrip().upper().startswith("SET ")
                      or s.lstrip().upper().startswith("/*!")]
        stmts = head_stmts + [s for s in filtered
                              if not (s.lstrip().upper().startswith("SET ")
                                      or s.lstrip().upper().startswith("/*!"))]

    conn = _conn(cid, host, port, timeout=20)
    ok_n, err_n, errors = 0, 0, []
    try:
        with conn.cursor() as cur:
            if no_binlog:
                try:
                    cur.execute("SET sql_log_bin = 0")
                except Exception as e:
                    return {"ok": False,
                            "error": f"无法关闭会话 binlog（需 SUPER 权限）：{e}"}
            for s in stmts:
                try:
                    cur.execute(s)
                    ok_n += 1
                except Exception as e:
                    err_n += 1
                    if len(errors) < 10:
                        errors.append(f"{str(e)[:200]} —— 语句：{s[:120]}")
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()

    return {
        "ok": err_n == 0,
        "total": len(stmts), "ok_statements": ok_n, "failed": err_n,
        "errors": errors,
        "msg": f"恢复完成：成功 {ok_n} 条" + (f"，失败 {err_n} 条" if err_n else ""),
    }


def read_backup_head(cid, filename, limit: int = 4000) -> str:
    """预览备份头部（前若干字符）。"""
    path = get_backup_path(cid, filename)
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return f.read(limit)
