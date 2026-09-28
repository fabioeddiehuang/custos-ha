"""backup_policy.py — 定时备份策略（集群级，v2.5.0 F9）。

为什么要把策略下沉到集群
------------------------
原有定时备份是**全局**的（settings 表一把开关管所有集群），但不同集群的数据量、
重要程度、备份窗口都不一样。改为：

    **集群级策略优先，未配置时回退全局**（不破坏存量行为）。

为什么它现在是关键基础设施
--------------------------
v2.5.0 的「节点纯净重建（L3）」默认走「最近备份作基线 + binlog 增量回放」，
**不为重建现做备份**。于是有没有一份「不太旧」的备份，直接决定重建快慢：
  - 有备份：只回放增量，通常远快于全量导出
  - 没备份 / 备份过旧：只能全量 dump，2GB 库要几分钟，且压主库

保留策略：天数与份数**双条件**（满足任一即清理），并且**永不删除最新一份**——
防止策略填错（如填 0）把唯一基线清掉。
"""
import json
import logging
import os
import time

from .db import get_setting, upsert_setting

log = logging.getLogger(__name__)

DEFAULTS = {
    "enabled": False,          # 默认关闭（保持既有行为；由用户在知情前提下开启）
    "interval_hours": 24,
    "retention_days": 7,
    "retention_count": 7,      # v2.5.0 新增：份数维度
    "target": "slave",         # 优先从库备份，避免备份 IO 拖累主库
    "with_data": True,
    "wait_catch_up": True,     # v2.5.0：备份前先等从库追平，保证位点干净
    "disk_alert_gb": 0,        # 0=不告警
}


def _key(cid, name):
    return "backup_policy_%s" % int(cid)


def get_policy(cluster_id, global_fallback=True):
    """取集群策略；未配置字段回退全局 settings，再回退内置默认。"""
    pol = dict(DEFAULTS)
    if global_fallback:
        pol["enabled"] = get_setting("BACKUP_SCHEDULE_ENABLED", "0") == "1"
        try:
            pol["interval_hours"] = int(get_setting("BACKUP_INTERVAL_HOURS", "24") or 24)
        except (TypeError, ValueError):
            pass
        try:
            pol["retention_days"] = int(get_setting("BACKUP_RETENTION_DAYS", "7") or 7)
        except (TypeError, ValueError):
            pass
        try:
            pol["retention_count"] = int(get_setting("BACKUP_RETENTION_COUNT", "7") or 7)
        except (TypeError, ValueError):
            pass
        pol["target"] = (get_setting("BACKUP_TARGET", "slave") or "slave").lower()
        pol["with_data"] = get_setting("BACKUP_WITH_DATA", "1") != "0"
    raw = get_setting(_key(cluster_id, ""), "")
    if raw:
        try:
            pol.update(json.loads(raw))
        except Exception:
            log.warning("集群 %s 备份策略 JSON 解析失败，按默认处理", cluster_id)
    return pol


def set_policy(cluster_id, **kw):
    """更新集群策略（只覆盖传入字段）。"""
    pol = get_policy(cluster_id, global_fallback=False)
    for k, v in kw.items():
        if k in DEFAULTS and v is not None:
            pol[k] = v
    upsert_setting(_key(cluster_id, ""), json.dumps(pol, ensure_ascii=False))
    return pol


def last_run(cid):
    try:
        return float(get_setting("backup_last_run_%s" % int(cid), "0") or 0)
    except (TypeError, ValueError):
        return 0.0


def mark_run(cid):
    upsert_setting("backup_last_run_%s" % int(cid), str(int(time.time())))


def due(cid, interval_hours):
    """是否到点。返回 (bool, 逾期秒数)。

    逾期信息用于「补做」提示：管理器停机错过窗口后，恢复时能看到晚了多久。
    """
    try:
        interval_hours = float(interval_hours)
    except (TypeError, ValueError):
        interval_hours = 24.0
    if interval_hours <= 0:
        return False, 0
    last = last_run(cid)
    if last <= 0:
        return True, 0
    elapsed = time.time() - last
    return elapsed >= interval_hours * 3600, max(0, int(elapsed - interval_hours * 3600))


def next_run_at(cid, interval_hours):
    """下次执行时间（页面展示，让运维知道什么时候会跑）。"""
    last = last_run(cid)
    if last <= 0:
        return time.time() + 60
    return last + float(interval_hours) * 3600


def purge(cluster_id, retention_days, retention_count):
    """按「天数 + 份数」双条件清理，**永不删除最新一份**。"""
    from . import backup
    out = {"by_days": [], "by_count": [], "kept": 0}
    try:
        r = backup.purge_old_backups(cluster_id, int(retention_days or 0))
        out["by_days"] = r.get("deleted") or []
    except Exception as e:
        log.warning("按天数清理备份失败（集群 %s）：%s", cluster_id, e)
    try:
        cnt = int(retention_count or 0)
        if cnt > 0:
            files = _list_backup_files(cluster_id)
            # 按时间倒序，保留最新 cnt 份（最新一份永不在 extra 里）
            extra = files[cnt:]
            removed = []
            for f in extra:
                try:
                    if os.path.isdir(f):
                        # 分库集备份（目录）：os.remove 会报 IsADirectoryError
                        import shutil as _sh
                        _sh.rmtree(f, ignore_errors=True)
                    else:
                        os.remove(f)
                        # 连带删除配套元信息，避免留下孤儿 .meta.json
                        mp = (f[:-len(".sql.gz")] + ".meta.json"
                              if f.endswith(".sql.gz") else None)
                        if mp and os.path.exists(mp):
                            try:
                                os.remove(mp)
                            except OSError:
                                pass
                    removed.append(os.path.basename(f))
                    out["by_count"].append(os.path.basename(f))
                except Exception as e:
                    log.warning("删除过期备份失败 %s：%s", f, e)
            # 同步清理备份记录：否则「备份记录」页签会继续列出这些已删除的备份
            if removed:
                try:
                    from . import backup as _bk
                    _bk._remove_history_entries(cluster_id, removed)
                except Exception as e:
                    log.warning("清理备份记录失败（集群 %s）：%s", cluster_id, e)
            out["kept"] = min(len(files), cnt)
    except Exception as e:
        log.warning("按份数清理备份失败（集群 %s）：%s", cluster_id, e)
    return out


def _list_backup_files(cluster_id):
    """列出该集群备份产物（按修改时间倒序）。

    ⚠ 必须同时覆盖两种形态：
      - 单文件备份：<name>.sql.gz
      - 分库集备份：<name>/ 目录（全库备份在「业务库 >= 2 个」时走这种，
        内部每库一个 .sql.gz + index.json）
    早期版本只认第一种，导致**分库集永远不参与按份数保留**，只增不减直至撑爆备份盘。
    """
    from . import backup
    try:
        d = backup._dir(cluster_id)
    except Exception:
        return []
    if not os.path.isdir(d):
        return []
    files = []
    for name in os.listdir(d):
        p = os.path.join(d, name)
        if os.path.isfile(p) and name.endswith((".sql", ".sql.gz", ".gz")):
            files.append(p)
        elif os.path.isdir(p) and os.path.isfile(os.path.join(p, "index.json")):
            files.append(p)          # 分库集备份（目录形态）
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return files


def usage_bytes(cluster_id):
    """备份占用字节数（磁盘预警用）。分库集（目录）递归求和，避免预警严重低报。"""
    total = 0
    for f in _list_backup_files(cluster_id):
        try:
            if os.path.isdir(f):
                for root, _dirs, names in os.walk(f):
                    for n in names:
                        try:
                            total += os.path.getsize(os.path.join(root, n))
                        except OSError:
                            pass
            else:
                total += os.path.getsize(f)
        except OSError:
            pass
    return total


def wait_catch_up(cluster_id, timeout_sec=60):
    """备份前等从库追平（保证备份位点干净、数据完整）。

    失败不阻断备份——只是位点可能略旧，但备份本身仍然可用。
    """
    try:
        from . import syncgate
        deadline = time.time() + int(timeout_sec)
        while time.time() < deadline:
            r = syncgate.readiness(cluster_id)
            if r.get("ready"):
                return True
            time.sleep(2)
    except Exception as e:
        log.debug("备份前追平等待异常：%s", e)
    return False
