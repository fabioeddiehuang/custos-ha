"""集群逻辑备份 / 恢复（纯 Python，基于 PyMySQL）。

为什么不用 mysqldump
--------------------
管理器镜像是 python:3.11-slim，容器内没有 mysql 客户端；而且 MariaDB 的
mysqldump 去连 MySQL 8.0 会撞上 caching_sha2_password 等鉴权/语法差异。
这里直接复用 ha_client.connect（与全站一致的鉴权、超时、字符集策略），
对 MySQL 8.0 与 MariaDB 一视同仁，且无需给镜像增加任何系统依赖。

产物
----
  /app/backups/<cluster_id>/<YYYYmmdd-HHMMSS>.sql.gz   备份内容
  /app/backups/<cluster_id>/<YYYYmmdd-HHMMSS>.meta.json 备份元信息

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
import re
from datetime import datetime
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
def _dir(cid) -> Path:
    d = BACKUP_ROOT / str(cid)
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


def _dump_database(conn, cur, w, db, with_data: bool) -> dict:
    """导出单个库（表结构 + 数据 + 视图），返回统计。"""
    stat = {"tables": 0, "views": 0, "rows": 0}

    w(f"CREATE DATABASE IF NOT EXISTS {_ident(db)} "
      f"/*!40100 DEFAULT CHARACTER SET utf8mb4 */;\n")
    w(f"USE {_ident(db)};\n\n")

    # ---- 基表 ----
    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (db,))
    tables = [r[0] for r in cur.fetchall()]

    for t in tables:
        cur.execute(f"SHOW CREATE TABLE {_ident(db)}.{_ident(t)}")
        row = cur.fetchone()
        if not row:
            continue
        ddl = row[1]
        w(f"--\n-- 表结构 {db}.{t}\n--\n")
        w(f"DROP TABLE IF EXISTS {_ident(t)};\n")
        w(ddl + ";\n\n")
        stat["tables"] += 1

        if not with_data:
            continue

        # 用服务端游标流式读取：默认的缓冲游标会在 execute 时把整张表拉进内存，
        # 大表会直接把管理器打爆。SSCursor 必须读完才能在该连接上执行下一条语句，
        # 下面的 while 循环正是读到底。
        with conn.cursor(SSCursor) as scur:
            scur.execute(f"SELECT * FROM {_ident(db)}.{_ident(t)}")
            cols = [d[0] for d in (scur.description or [])]
            if not cols:
                continue
            prefix = (f"INSERT INTO {_ident(t)} ("
                      + ",".join(_ident(c) for c in cols) + ") VALUES\n")
            buf, bufsize, dumped = [], 0, 0
            while True:
                rows = scur.fetchmany(500)
                if not rows:
                    break
                for r in rows:
                    piece = "(" + ",".join(_lit(v) for v in r) + ")"
                    buf.append(piece)
                    bufsize += len(piece) + 2
                    stat["rows"] += 1
                    dumped += 1
                if bufsize >= _INSERT_MAX_BYTES:
                    w("--\n-- 数据 " + f"{db}.{t}\n--\n")
                    w(prefix + ",\n".join(buf) + ";\n\n")
                    buf, bufsize = [], 0
            if buf:
                w("--\n-- 数据 " + f"{db}.{t}\n--\n")
                w(prefix + ",\n".join(buf) + ";\n\n")

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

    return stat


def run_backup(cid, host, port, databases=None, with_data: bool = True,
               scope: str = "all"):
    """执行备份。databases 为空(None/[]) 且 scope='all' 时备份全部业务库。"""
    conn = _conn(cid, host, port, timeout=15)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    base = _dir(cid) / ts
    sql_path = base.with_suffix(".sql.gz")
    meta_path = base.with_suffix(".meta.json")

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

            total = {"tables": 0, "views": 0, "rows": 0}
            with gzip.open(sql_path, "wt", encoding="utf-8", newline="\n") as f:
                def w(s):
                    f.write(s)

                w("-- Custos 逻辑备份\n")
                w(f"-- 集群 ID   : {cid}\n")
                w(f"-- 源节点    : {host}:{port}\n")
                w(f"-- 生成时间  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                w(f"-- 备份范围  : {'全库' if scope == 'all' else '指定库'}\n")
                w(f"-- 包含数据  : {'是' if with_data else '否（仅结构）'}\n")
                w(f"-- 数据库    : {', '.join(targets)}\n")
                w("--\n")
                w("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
                w("/*!40101 SET NAMES utf8mb4 */;\n")
                w("SET FOREIGN_KEY_CHECKS=0;\n")
                w("SET UNIQUE_CHECKS=0;\n")
                w("SET TIME_ZONE='+00:00';\n")
                w("SET AUTOCOMMIT=0;\n\n")

                for db in targets:
                    st = _dump_database(conn, cur, w, db, with_data)
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
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cluster_id": int(cid),
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
            if sql_path.exists():
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
            "mtime": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "meta": meta,
        })
    return out


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
def restore_backup(cid, host, port, filename, databases=None):
    """把备份恢复到指定节点。

    databases 为空则恢复备份内的全部库；否则只恢复其中的指定库
    （按备份内的 CREATE DATABASE / USE 段落过滤）。
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
