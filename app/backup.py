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

  v2.3.2 起：带数据的备份在【一致性快照事务】内完成（REPEATABLE READ +
  START TRANSACTION WITH CONSISTENT SNAPSHOT），跨表数据属同一时点；meta 记录
  复制位点（MariaDB: gtid_binlog_pos/gtid_slave_pos；MySQL: gtid_executed +
  binlog file/pos），为将来的时间点恢复（PITR）打地基。老备份无这些字段，
  仍可正常列出、预览、恢复。

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
import threading
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pymysql
from pymysql.cursors import SSCursor

# 备份根目录：容器内 /app/backups（docker-compose 挂到项目根 ./backups 持久化）
BACKUP_ROOT = Path(os.environ.get("BACKUPS_DIR") or "/app/backups")

# 系统库不参与备份（test 为 MariaDB/MySQL 安装默认带出的空库，属安装产物而非业务库，一并排除）
# ha_local：Custos 的【节点本地状态库】（keepalived 检查脚本据其 current_master 判定 VIP 归属）。
#   它由管理端逐节点直连写入、绝不参与复制，**不是业务数据**。若被备份进去，恢复时会把
#   备份时刻【过期的 current_master】一并写回节点 —— 该节点可能因此自认为主库，与真正的
#   当前主库形成双主 / VIP 乱漂（脑裂）。故必须与系统库同等对待、排除在备份之外。
SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys", "ndbinfo", "test",
              "ha_local"}

# 单条 INSERT 的近似体积上限（字节）。超过就切一条新 INSERT，
# 避免单条语句超过 max_allowed_packet 导致恢复失败。
_INSERT_MAX_BYTES = 900 * 1024

# 备份/恢复操作日志（落盘，便于审计与追溯成功/失败条数）
BACKUP_LOG = BACKUP_ROOT / "backup_restore.log"


def _log_event(msg: str):
    """追加一条带中国时间戳的备份/恢复操作日志。失败静默不影响主流程。"""
    try:
        BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        ts = now_cn().strftime("%Y-%m-%d %H:%M:%S")
        with open(BACKUP_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def read_backup_log(cid=None, limit: int = 200):
    """读取备份/恢复日志（倒序，最新在前）。cid 给定时仅返回该集群行。"""
    try:
        if not BACKUP_LOG.exists():
            return []
        lines = BACKUP_LOG.read_text(encoding="utf-8").splitlines()
        if cid is not None:
            # 用边界匹配而非子串包含：否则 id=1 会误命中 id=11/12…（集群号可能重叠）
            tok = f"id={int(cid)}" if str(cid).isdigit() else f"集群={cid}"
            pat = re.compile(r'(^|\s)' + re.escape(tok) + r'(\s|$)')
            lines = [l for l in lines if pat.search(l)]
        out = [l for l in lines if l.strip()]
        out.reverse()
        return out[:limit]
    except Exception:
        return []


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
                   host=None, port=None, cid=None, data_conn=None) -> dict:
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

    # 单独开一个连接专供数据流式导出，避免与上面的元数据游标争用同一连接。
    # v2.3.2：若调用方传入 data_conn（一致性快照事务连接），则**复用它**——
    # 每个库各自开连接会让各库落在不同时点的快照上，跨库数据依旧不一致。
    # 关键：传入的 data_conn 由调用方（run_backup）统一提交/关闭，**绝不可在此关闭**，
    # 否则第一个库导出完就关掉共享快照连接，后续库的 SSCursor 建在已关闭连接上抛异常，
    # 被逐个表的 except 吞掉 -> 只写结构不写数据（"只备份了空表"）。
    sconn = None
    _own_sconn = False
    if with_data:
        if data_conn is not None:
            sconn = data_conn
        else:
            if cid is not None:
                try:
                    sconn = _conn(cid, host, port)
                    _own_sconn = True
                except Exception as e:
                    w(f"-- 数据导出连接建立失败（仅跳过数据，结构仍会导出）：{e}\n")
            else:
                sconn = conn

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

    # 仅关闭【本函数自行创建】的数据连接；传入的 data_conn（共享一致性快照）由调用方统一处理。
    if _own_sconn and sconn is not None and sconn is not conn:
        try:
            sconn.close()
        except Exception:
            pass

    return stat


def _capture_position(cur, flavour=None):
    """在一致性快照事务内采集复制位点（v2.3.2）。

    位点必须在快照建立**之后**读取，才能与导出的数据严格对应：先于快照采集会得到
    「位点比数据旧」的备份，将来按位点做 PITR 会重复应用事务；晚于快照则会丢事务。

    MariaDB 与 MySQL 的位点语义完全不同，必须分派：
      MariaDB: gtid_binlog_pos（本实例已写入 binlog 的 GTID）
               gtid_slave_pos （已从主库回放的 GTID —— 从库备份时是关键位点）
      MySQL  : gtid_executed + binlog file/pos
    """
    out = {}
    if flavour is None:
        try:
            from .mysql_compat import detect_flavour
            flavour = detect_flavour(cur)
        except Exception:
            flavour = "unknown"
    out["flavour"] = flavour
    try:
        if flavour == "mariadb":
            for var in ("gtid_binlog_pos", "gtid_slave_pos", "gtid_current_pos"):
                try:
                    cur.execute("SELECT @@" + var)
                    r = cur.fetchone()
                    out[var] = (r[0] if r and r[0] is not None else "")
                except Exception:
                    out[var] = ""
        else:
            try:
                cur.execute("SELECT @@gtid_executed")
                r = cur.fetchone()
                out["gtid_executed"] = (r[0] if r and r[0] is not None else "")
            except Exception:
                out["gtid_executed"] = ""
            # MySQL 8.4 起 SHOW MASTER STATUS 更名 SHOW BINARY LOG STATUS；
            # 项目锁定 8.0.46（旧名仍有效），失败再回退新名，两头都兜住。
            for stmt in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
                try:
                    cur.execute(stmt)
                    r = cur.fetchone()
                    if r:
                        out["binlog_file"] = r[0]
                        try:
                            out["binlog_pos"] = int(r[1])
                        except (TypeError, ValueError):
                            pass
                    break
                except Exception:
                    continue
    except Exception as e:
        out["error"] = str(e)
    return out


def _write_backup_header(w, cid, host, port, scope, targets, with_data,
                         position, snap_ok, snap_err):
    """备份文件头注释（v2.3.4 抽出，单文件/分库目录两种形态共用）。"""
    w("-- Custos 逻辑备份\n")
    w(f"-- 集群     : {_cluster_dir_name(cid)} (id={cid})\n")
    w(f"-- 源节点    : {host}:{port}\n")
    w(f"-- 生成时间  : {now_cn().strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n")
    w(f"-- 备份范围  : {'全库' if scope == 'all' else '指定库'}\n")
    w(f"-- 包含数据  : {'是' if with_data else '否（仅结构）'}\n")
    w(f"-- 数据库    : {', '.join(targets)}\n")
    if position:
        w(f"-- 复制位点  : {json.dumps(position, ensure_ascii=False)}\n")
    w(f"-- 一致性快照: {'是（REPEATABLE READ + WITH CONSISTENT SNAPSHOT）' if snap_ok else ('否（%s）' % (snap_err or '仅结构备份'))}\n")
    w("--\n")
    w("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
    w("/*!40101 SET NAMES utf8mb4 */;\n")
    w("SET FOREIGN_KEY_CHECKS=0;\n")
    w("SET UNIQUE_CHECKS=0;\n")
    w("SET TIME_ZONE='+08:00';\n")
    w("SET AUTOCOMMIT=0;\n\n")


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

        # ---- v2.3.2：一致性快照事务 ----
        # 所有库的数据读取必须在【同一个】REPEATABLE READ 快照事务内完成：
        # 备份期间若有写入，先导出的表与后导出的表会属于不同时点，恢复出来
        # 跨表不一致（典型：订单表有这笔、库存表没扣）。位点也必须在快照建立
        # 之后采集，才能与导出的数据严格对应（为将来的 PITR 打地基）。
        snap_conn = None
        snap_ok, snap_err, position = False, "", {}
        if with_data:
            try:
                snap_conn = _conn(cid, host, port, timeout=15)
                with snap_conn.cursor() as _sc:
                    _sc.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    _sc.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                    snap_ok = True
                with snap_conn.cursor() as _sc:
                    position = _capture_position(_sc)
            except Exception as _e:
                snap_ok, snap_err = False, str(_e)
                try:
                    if snap_conn:
                        snap_conn.close()
                except Exception:
                    pass
                snap_conn = None

        total = {"tables": 0, "views": 0, "rows": 0}
        # ★ 全库备份必须是【一个完整文件】（与 backup.py 顶部声明的产物格式一致）：
        #     /app/backups/<集群名称>/All-Databases_<YYYYmmdd-HHMMSS>.sql.gz
        # v2.3.4 曾把「业务库 >= 2 个的全备」改成【分库集目录】（每库一个 .sql.gz +
        # index.json），偏离了该要求：产物不再是单一文件，备份页「下载」按钮直接 500
        # （下载端点对目录 read_bytes 会抛 IsADirectoryError，见 /api/backup/download）。
        # 现统一走单文件模式：全备 / 单库 / 指定库都产出单个 .sql.gz，便于下载、归档与恢复。
        # 注：历史上已生成的分库集目录仍可被列出 / 预览 / 恢复 / 下载（list_backups、
        # restore_backup、api_backup_download 均已兼容目录形态），不受本次变更影响。

        # 单文件模式（全库 / 单库 / 指定库并集）
        meta_path = bdir / f"{label}_{ts}.meta.json"
        with conn.cursor() as cur:
            with gzip.open(sql_path, "wt", encoding="utf-8", newline="\n") as f:
                def w(s):
                    f.write(s)
                _write_backup_header(w, cid, host, port, scope, targets,
                                     with_data, position, snap_ok, snap_err)
                for db in targets:
                    st = _dump_database(conn, cur, w, db, with_data,
                                        host=host, port=port, cid=cid,
                                        data_conn=snap_conn)
                    total["tables"] += st["tables"]
                    total["views"] += st["views"]
                    total["rows"] += st["rows"]
                w("SET FOREIGN_KEY_CHECKS=1;\n")
                w("SET UNIQUE_CHECKS=1;\n")
                w("COMMIT;\n")
                w("-- 备份结束\n")
            if snap_conn is not None:
                try:
                    with snap_conn.cursor() as _c:
                        _c.execute("COMMIT")
                except Exception:
                    pass
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
            "kind": "file",
            "databases": targets,
            "with_data": bool(with_data),
            "tables": total["tables"], "views": total["views"],
            "rows": total["rows"], "size": size,
            "file": sql_path.name,
            "position": position,
            "consistent": bool(snap_ok and with_data),
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                             encoding="utf-8")
        return {"ok": True, "file": sql_path.name, "meta": meta}
    except Exception as e:
        # 备份失败要清理半截文件/目录，避免留下不可用的残档
        try:
            sd = locals().get("set_dir")
            if sd is not None and getattr(sd, "is_dir", lambda: False)():
                import shutil
                shutil.rmtree(sd, ignore_errors=True)
            elif sql_path is not None and sql_path.exists():
                sql_path.unlink()
        except Exception:
            pass
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if snap_conn is not None:
                snap_conn.close()
        except Exception:
            pass
        conn.close()


# --------------------------------------------------------------------------
# 备份列表 / 读取 / 删除
# --------------------------------------------------------------------------
def list_backups(cid):
    """列出备份。v2.3.4 起支持两种形态：
      - 单文件备份：<label>_<ts>.sql.gz + .meta.json（kind="file"）
      - 分库集备份：<label>_<ts>/ 目录，每库一个 .sql.gz + index.json（kind="set"）
    两类按生成时间倒序合并；老备份（无 kind 字段）按 "file" 处理。
    """
    d = _dir(cid)
    out = []
    # 单文件备份
    for p in sorted(d.glob("*.sql.gz"), reverse=True):
        meta_p = p.with_suffix("").with_suffix(".meta.json")
        meta = None
        if meta_p.exists():
            try:
                meta = json.loads(meta_p.read_text(encoding="utf-8"))
            except Exception:
                meta = None
        # 备份目录按集群【名】划分：删掉旧集群又用同名重建时，旧备份会残留在同一目录。
        if isinstance(meta, dict) and meta.get("cluster_id"):
            try:
                if int(meta.get("cluster_id")) != int(cid):
                    continue
            except (TypeError, ValueError):
                pass
        st = p.stat()
        m = dict(meta) if isinstance(meta, dict) else {}
        m["kind"] = m.get("kind", "file")
        out.append({
            "file": p.name,
            "size": st.st_size,
            "mtime": from_ts_cn(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "meta": m,
        })
    # 分库集备份
    for idx in sorted(d.glob("*/index.json"), reverse=True):
        try:
            meta = json.loads(idx.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(meta, dict):
            continue
        if meta.get("cluster_id") and int(meta.get("cluster_id")) != int(cid):
            continue
        set_dir = idx.parent
        st = set_dir.stat()
        m = dict(meta)
        m["kind"] = m.get("kind", "set")
        out.append({
            "file": set_dir.name,
            "size": m.get("size", st.st_size),
            "mtime": from_ts_cn(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "meta": m,
        })
    out.sort(key=lambda x: (x.get("meta") or {}).get("time", ""), reverse=True)
    return out


def purge_old_backups(cid, days):
    """按保留天数清理过期备份（连带删除配套 .meta.json / 分库集目录）。

    days<=0 表示不清理。以文件 mtime 判定（与备份时间戳同为本地时间）。
    返回 {"deleted": [文件名...], "kept": 保留份数}。

    ⚠ 修复（此前只扫单文件）：全库备份在「业务库 >= 2 个」时是**分库集目录**
    （<label>_<ts>/，内部每库一个 .sql.gz + index.json），而旧实现只在父目录 glob
    `*.sql.gz` —— 父目录根本匹配不到这些嵌套文件，于是**默认的全库备份永远不会被
    按天数清理**，只增不减直至撑爆备份盘（盘满后新备份必然失败＝「不能备份」）。
    这里把分库集目录一并纳入判定。
    """
    try:
        days = int(days or 0)
    except (TypeError, ValueError):
        days = 0
    if days <= 0:
        return {"deleted": [], "kept": 0}
    cutoff = time.time() - days * 86400
    deleted, kept = [], 0
    d = _dir(cid)
    for p in d.glob("*.sql.gz"):
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
    # 分库集备份（目录形态）
    import shutil as _sh
    for idx in d.glob("*/index.json"):
        set_dir = idx.parent
        try:
            if set_dir.stat().st_mtime < cutoff:
                _sh.rmtree(set_dir, ignore_errors=True)
                deleted.append(set_dir.name)
            else:
                kept += 1
        except Exception:
            kept += 1
    if deleted:
        _remove_history_entries(cid, deleted)
    return {"deleted": deleted, "kept": kept}


def _remove_history_entries(cid, names) -> int:
    """删除备份产物后，同步移除 backup_history.json 中的对应条目。

    为什么必须做：备份记录（「备份记录」页签）与磁盘文件是两份独立数据。删除/清理
    备份时若只删文件、不动记录，页面会继续显示一份【磁盘上已不存在】的备份
    （实测：记录里仍有 All-Databases_20260929-013753、目录里却没有）——运维据此
    误以为存在可用备份，真正需要恢复时才发现文件根本不存在。删文件必须连带清记录。
    """
    try:
        want = {str(n) for n in (names or []) if n}
        if not want:
            return 0
        p = _dir(cid) / _BACKUP_HISTORY
        if not p.exists():
            return 0
        arr = json.loads(p.read_text(encoding="utf-8")) or []
        if not isinstance(arr, list):
            return 0
        kept = [r for r in arr
                if not (isinstance(r, dict) and str(r.get("file") or "") in want)]
        removed = len(arr) - len(kept)
        if removed:
            p.write_text(json.dumps(kept, ensure_ascii=False, indent=2),
                         encoding="utf-8")
        return removed
    except Exception:
        return 0


def get_backup_path(cid, filename: str) -> Path:
    """只允许本集群目录下的文件名，杜绝 ../ 穿越。"""
    name = os.path.basename(filename)
    p = _dir(cid) / name
    if not p.exists():
        raise FileNotFoundError(f"备份文件不存在：{name}")
    return p


def delete_backup(cid, filename: str):
    p = get_backup_path(cid, filename)
    # v2.3.4：分库集备份是目录（kind="set"），不能用 unlink()（会报
    # [Errno 21] Is a directory）；单文件备份才是普通文件。
    if p.is_dir():
        import shutil
        shutil.rmtree(p, ignore_errors=True)
    else:
        p.unlink()
    # 元数据可能以 <name>.meta.json（单文件）或 <name>/index.json（目录）形式存在
    for cand in (p.with_suffix("").with_suffix(".meta.json"), p / "index.json",
                 p / ".meta.json"):
        if cand.exists():
            try:
                cand.unlink()
            except Exception:
                pass
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
def restore_backup(cid, host, port, filename, databases=None, no_binlog=False,
                   progress=None):
    """把备份恢复到指定节点。

    databases 为空则恢复备份内的全部库；否则只恢复其中的指定库
    （按备份内的 CREATE DATABASE / USE 段落过滤）。

    no_binlog=True 时在本会话执行 SET sql_log_bin=0：
      重建从库必须关掉，否则恢复产生的海量 DML 会写入从库 binlog、生成新的
      GTID 集合，与主库历史 GTID 冲突，重链时被判「不一致」进而触发 RESET MASTER。
      该变量是 session 级（不能 SET GLOBAL），故只能在本函数建连后设置。

    progress(done, total, phase)：进度回调（可为 None），供后台任务展示进度。
    """
    if progress:
        def _pg(done, total, phase=""):
            try:
                progress(done, total, phase or "执行 SQL 语句…")
            except Exception:
                pass
    else:
        _pg = None

    path = get_backup_path(cid, filename)
    if path.is_dir():
        # v2.3.4：分库集备份目录 -> 按 index.json 逐个库文件恢复
        return _restore_backup_set(cid, host, port, path, databases, no_binlog, _pg)
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
    if _pg:
        _pg(0, len(stmts), f"已解析 {len(stmts)} 条 SQL，开始执行…")
    try:
        ok_n, err_n, errors = _exec_stmts(conn, stmts, no_binlog, _pg)
    finally:
        conn.close()

    return {
        "ok": err_n == 0,
        "total": len(stmts), "ok_statements": ok_n, "failed": err_n,
        "errors": errors,
        "msg": f"恢复完成：成功 {ok_n} 条" + (f"，失败 {err_n} 条" if err_n else ""),
    }


# 每执行多少条语句提交一次事务。
# 为什么需要：原实现用 dump 头部的 SET AUTOCOMMIT=0 把【整个恢复】放进一个超长事务，
# 只在最后 commit。MySQL 8.0 对单个超长事务的处理明显差于 MariaDB——undo/history list
# 持续膨胀、redo checkpoint 被反复推迟，插入海量数据时慢得多（同样的代码在 MariaDB
# 上快得多，用户实测 MySQL 恢复远慢于数据量更大的 MariaDB）。按批提交把事务规模
# 控制在有限大小即可显著提速，对 MariaDB 同样无害。
# 恢复本来就不是原子操作（单条语句出错也继续执行、最后仍会 commit），因此分批提交
# 不改变既有语义。
_RESTORE_COMMIT_EVERY = 1000


def _exec_stmts(conn, stmts, no_binlog, progress=None):
    """在建连后的 conn 上执行切好的语句（v2.3.4 抽出，单文件/目录恢复共用）。

    返回 (ok_n, err_n, errors)。no_binlog 时关闭本会话 binlog，避免恢复产生的
    DML 写入从库 binlog 而与主库历史 GTID 冲突。

    progress(done, total, phase)：每提交一批回调一次，用于恢复进度展示（可为 None）。
    """
    ok_n, err_n, errors = 0, 0, []
    total = len(stmts)
    with conn.cursor() as cur:
        if no_binlog:
            try:
                cur.execute("SET sql_log_bin = 0")
            except Exception as e:
                return ok_n, err_n, [f"无法关闭会话 binlog（需 SUPER 权限）：{e}"]
        for i, s in enumerate(stmts, 1):
            try:
                cur.execute(s)
                ok_n += 1
            except Exception as e:
                err_n += 1
                if len(errors) < 10:
                    errors.append(f"{str(e)[:200]} —— 语句：{s[:120]}")
            if i % _RESTORE_COMMIT_EVERY == 0:
                try:
                    conn.commit()
                except Exception:
                    pass
                if progress:
                    try:
                        progress(ok_n + err_n, total, "")
                    except Exception:
                        pass
        try:
            conn.commit()
        except Exception:
            pass
    if progress:
        try:
            progress(ok_n + err_n, total, "")
        except Exception:
            pass
    return ok_n, err_n, errors


def _restore_backup_set(cid, host, port, set_dir, databases, no_binlog,
                        progress=None):
    """恢复分库集备份目录（v2.3.4）。

    databases 非空前只恢复其中的库（目录内每个 .sql.gz 已是独立库，无需再按 USE 切段）。
    进度按「库」粒度上报（分库集无法在执行前预知总语句数）。
    """
    idx = set_dir / "index.json"
    try:
        meta = json.loads(idx.read_text(encoding="utf-8"))
    except Exception as e:
        return {"ok": False, "error": f"读取备份索引失败：{e}"}
    all_dbs = meta.get("databases") or []
    if databases:
        want = set(databases)
        files = [set_dir / f"{d}.sql.gz" for d in all_dbs if d in want]
    else:
        files = [set_dir / f"{d}.sql.gz" for d in all_dbs]
    if not files:
        return {"ok": False, "error": "该备份集内没有可恢复的库"}
    conn = _conn(cid, host, port, timeout=20)
    ok_n, err_n, errors = 0, 0, []
    n_files = len(files)
    for fi, fp in enumerate(files, 1):
        if progress:
            try:
                progress(fi - 1, n_files, f"恢复库 {fp.stem}（{fi}/{n_files}）…")
            except Exception:
                pass
        try:
            with gzip.open(fp, "rt", encoding="utf-8") as f:
                text = f.read()
        except Exception as e:
            err_n += 1
            errors.append(f"{fp.name} 读取失败: {e}")
            continue
        _ok, _en, _ers = _exec_stmts(conn, split_statements(text), no_binlog, None)
        ok_n += _ok
        err_n += _en
        errors.extend(_ers)
    try:
        conn.close()
    except Exception:
        pass
    if progress:
        try:
            progress(n_files, n_files, "全部库已执行完毕")
        except Exception:
            pass
    return {
        "ok": err_n == 0,
        "total": ok_n + err_n, "ok_statements": ok_n, "failed": err_n,
        "errors": errors[:50],
        "restored": [f.stem for f in files],
        "msg": f"恢复完成：成功 {ok_n} 条" + (f"，失败 {err_n} 条" if err_n else ""),
    }


# --------------------------------------------------------------------------
# 恢复任务（后台执行 + 进度轮询）
# --------------------------------------------------------------------------
# 为什么要有：恢复大库可能持续数分钟到数十分钟。原实现把整个恢复同步阻塞在一次
# HTTP 请求里，前端只有一个「恢复中…」按钮——用户既看不到进展，也容易误判为卡死。
# 改为后台线程执行、前端轮询进度。进度只在进程内存里，管理器重启即丢失（可接受：
# 恢复本身也会随进程终止而中断）。
_RESTORE_TASKS = {}
_RESTORE_TASKS_LOCK = threading.Lock()
_RESTORE_TASKS_KEEP = 50


def _task_update(tid, **kw):
    with _RESTORE_TASKS_LOCK:
        rec = _RESTORE_TASKS.get(tid)
        if rec:
            rec.update(kw)


def start_restore_task(cid, host, port, filename, databases=None,
                       no_binlog=False, operator=""):
    """启动一个后台恢复任务，立即返回 task_id（用于轮询进度）。"""
    tid = "rst-" + uuid.uuid4().hex[:12]
    rec = {
        "id": tid, "cluster_id": int(cid),
        "cluster_name": _cluster_dir_name(cid),
        "target": f"{host}:{port}", "file": filename,
        "databases": list(databases or []),
        "operator": operator or "",
        "started": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
        "started_ts": time.time(),
        "finished": False, "ok": None,
        "phase": "准备中…", "done": 0, "total": 0, "pct": 0,
        "elapsed": 0.0, "result": None, "msg": "",
    }
    with _RESTORE_TASKS_LOCK:
        if len(_RESTORE_TASKS) >= _RESTORE_TASKS_KEEP:
            old = sorted(_RESTORE_TASKS.values(), key=lambda r: r["started_ts"])
            for r in old[:max(1, len(old) - _RESTORE_TASKS_KEEP + 1)]:
                if r.get("finished"):
                    _RESTORE_TASKS.pop(r["id"], None)
        _RESTORE_TASKS[tid] = rec
    threading.Thread(
        target=_run_restore_task,
        args=(tid, int(cid), host, int(port), filename, list(databases or []),
              bool(no_binlog), operator or ""),
        daemon=True,
    ).start()
    return tid


def get_restore_task(tid):
    """查询单个恢复任务进度（返回副本，取不到返回 None）。"""
    with _RESTORE_TASKS_LOCK:
        rec = _RESTORE_TASKS.get(tid)
        return dict(rec) if rec else None


def list_restore_tasks(limit: int = 20):
    """列出最近的恢复任务（倒序）。"""
    with _RESTORE_TASKS_LOCK:
        arr = sorted(_RESTORE_TASKS.values(),
                     key=lambda r: r["started_ts"], reverse=True)
        return [dict(r) for r in arr[:limit]]


def _run_restore_task(tid, cid, host, port, filename, dbs, no_binlog, operator):
    """后台线程主体：执行恢复 -> 落恢复记录 -> 写操作日志 -> 写审计。"""
    t0 = time.time()

    def _prog(done, total, phase=""):
        with _RESTORE_TASKS_LOCK:
            rec = _RESTORE_TASKS.get(tid)
            if not rec:
                return
            if total:
                rec["done"] = int(done)
                rec["total"] = int(total)
                rec["pct"] = max(0, min(99, int(done * 100 / max(1, total))))
            if phase:
                rec["phase"] = phase
            rec["elapsed"] = round(time.time() - t0, 1)

    _task_update(tid, phase="读取备份文件…")
    try:
        r = restore_backup(cid, host, port, filename, databases=dbs or None,
                           no_binlog=no_binlog, progress=_prog)
    except Exception as e:
        r = {"ok": False, "msg": f"恢复异常：{e}", "errors": [str(e)],
             "total": 0, "ok_statements": 0, "failed": 0}
    elapsed = round(time.time() - t0, 1)
    ok = bool(r.get("ok"))
    scope = "custom" if dbs else "all"

    try:
        record_restore(cid, {
            "time": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
            "cluster_id": int(cid),
            "cluster_name": _cluster_dir_name(cid),
            "target": f"{host}:{port}",
            "file": filename,
            "scope": scope,
            "databases": dbs,
            "ok": ok,
            "total": r.get("total", 0),
            "ok_statements": r.get("ok_statements", 0),
            "failed": r.get("failed", 0),
            "operator": operator,
            "msg": r.get("msg", ""),
            "elapsed": elapsed,
        })
    except Exception:
        pass

    # 操作日志（备份页「操作日志」页签按 id=<cid> 过滤）
    try:
        _log_event(
            f"id={int(cid)} 集群={_cluster_dir_name(cid)} 恢复 目标={host}:{port} "
            f"文件={filename} 范围="
            f"{'指定库:' + ','.join(dbs) if dbs else '全部'} "
            f"成功={r.get('ok_statements', 0)} 失败={r.get('failed', 0)} "
            f"耗时={elapsed}s 结果={'成功' if ok else '失败'}"
        )
    except Exception:
        pass

    # 审计（失败静默，不影响恢复结果）
    try:
        from . import audit as _audit
        _audit.add_audit(
            operator or "system", "restore_backup", f"{host}:{port}",
            f"file={filename} dbs={','.join(dbs) or 'ALL'} ok={ok} "
            f"failed={r.get('failed', 0)} elapsed={elapsed}s",
            cluster_id=int(cid))
    except Exception:
        pass

    _task_update(tid, finished=True, ok=ok, pct=100, phase=(
        "恢复完成" if ok else "恢复结束（存在失败语句）"),
        elapsed=elapsed, result=r, msg=r.get("msg", ""))


def read_backup_head(cid, filename, limit: int = 4000) -> str:
    """预览备份头部（前若干字符）。

    v2.3.4：分库集备份是目录（kind="set"），gzip.open 会报
    [Errno 21] Is a directory；此时改为展示 index.json 概览 + 前几个库文件头。
    """
    path = get_backup_path(cid, filename)
    if path.is_dir():
        parts = []
        idx = path / "index.json"
        if idx.exists():
            try:
                meta = json.loads(idx.read_text(encoding="utf-8"))
                dbs = meta.get("databases") or meta.get("tables") or []
                parts.append("/* 分库集备份：共 %s 个库 */" % len(dbs))
                parts.append(json.dumps(meta, ensure_ascii=False, indent=2)[:800])
            except Exception as e:
                parts.append("(index.json 读取失败: %s)" % e)
        for gz in sorted(path.glob("*.sql.gz"))[:3]:
            try:
                with gzip.open(gz, "rt", encoding="utf-8") as f:
                    parts.append("\n--- %s ---\n%s" % (gz.name, f.read(600)))
            except Exception:
                pass
        return ("\n".join(parts) or "（空备份目录）")[:limit]
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return f.read(limit)


# --------------------------------------------------------------------------
# 恢复记录（落盘为 JSON 列表，供备份页「恢复记录」展示与审计）
# --------------------------------------------------------------------------
_RESTORE_HISTORY = "restore_history.json"
_RESTORE_HISTORY_LIMIT = 200


def record_restore(cid, record: dict) -> None:
    """追加一条恢复记录到 <集群目录>/restore_history.json。

    倒序保存（最新在前），最多保留 `_RESTORE_HISTORY_LIMIT` 条；任何异常都
    静默吞掉，绝不影响恢复主流程。
    """
    try:
        d = _dir(cid)
        p = d / _RESTORE_HISTORY
        arr = []
        if p.exists():
            try:
                arr = json.loads(p.read_text(encoding="utf-8")) or []
            except Exception:
                arr = []
        if not isinstance(arr, list):
            arr = []
        arr.insert(0, record)
        if len(arr) > _RESTORE_HISTORY_LIMIT:
            arr = arr[:_RESTORE_HISTORY_LIMIT]
        p.write_text(json.dumps(arr, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def list_restores(cid, limit: int = 100) -> list:
    """读取恢复记录（倒序，最新在前）。"""
    try:
        p = _dir(cid) / _RESTORE_HISTORY
        if not p.exists():
            return []
        arr = json.loads(p.read_text(encoding="utf-8")) or []
        if not isinstance(arr, list):
            return []
        return arr[:limit]
    except Exception:
        return []


# --------------------------------------------------------------------------
# 备份记录（落盘为 JSON 列表，供备份页「备份记录」展示与审计）
# --------------------------------------------------------------------------
_BACKUP_HISTORY = "backup_history.json"
_HISTORY_LIMIT = 200


def record_backup(cid, record: dict) -> None:
    """追加一条备份记录到 <集群目录>/backup_history.json。

    倒序保存（最新在前），最多保留 `_HISTORY_LIMIT` 条；任何异常都静默吞掉，
    绝不影响备份主流程。
    """
    try:
        d = _dir(cid)
        p = d / _BACKUP_HISTORY
        arr = []
        if p.exists():
            try:
                arr = json.loads(p.read_text(encoding="utf-8")) or []
            except Exception:
                arr = []
        if not isinstance(arr, list):
            arr = []
        arr.insert(0, record)
        if len(arr) > _HISTORY_LIMIT:
            arr = arr[:_HISTORY_LIMIT]
        p.write_text(json.dumps(arr, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def list_backup_records(cid, limit: int = 100) -> list:
    """读取备份记录（倒序，最新在前）。

    ⚠ 记录与磁盘并非原子同步（历史存量条目 / 早期版本删除未联动清理 / 外部手工删文件），
    故每条都回查一次磁盘并标出 `file_missing`：页面据此把「记录里有、磁盘上已没有」的
    条目明确标为【文件已不存在】，避免运维误以为存在可用备份——这正是「显示有最新备份
    但 backup 目录下看不到」的表现。
    """
    try:
        p = _dir(cid) / _BACKUP_HISTORY
        if not p.exists():
            return []
        arr = json.loads(p.read_text(encoding="utf-8")) or []
        if not isinstance(arr, list):
            return []
        d = _dir(cid)
        out = []
        for r in arr[:limit]:
            if not isinstance(r, dict):
                out.append(r)
                continue
            fn = str(r.get("file") or "")
            rr = dict(r)
            rr["file_missing"] = bool(fn) and not (d / fn).exists()
            out.append(rr)
        return out
    except Exception:
        return []
