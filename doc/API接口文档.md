# Custos HA · API 接口文档

> 适用版本：**v2.4.0**（与 `.env` 的 `VERSION` 保持一致）
> 基址：管理端地址（默认 `http://<manager>:28888`）
> 本文档由 `app/api.py` 实际路由导出（共 **109** 个端点），与代码强一致。
> 所有示例中的管理/节点 IP 均使用文档专用段 `192.0.2.x`（RFC 5737），不对应任何真实内网地址。

## 通用约定

| 项 | 说明 |
|------|------|
| 鉴权 | 除 `POST /login` 外全部需登录态：`Cookie: session=<id>`（由 `POST /login` 写入）。未登录访问页面路由 → 重定向登录页；访问 API → 403 / `{"ok":false}` |
| 参数绑定 | `Form` = 表单字段（`application/x-www-form-urlencoded`）；`=默认值` = 查询参数（Query，可省略）；`必填` = 无默认值、必须传；JSON 体 = 迁移类接口以 `application/json` 提交（见各接口说明） |
| 返回约定 | 成功 `{"ok":true, ...}`；**业务失败返回 HTTP 200 + `{"ok":false,"error":"..."}`**（非 4xx/5xx） |
| 序列化 | 日期/时间/Decimal/bytes 统一由 `_json_ok` 兜底转 JSON（时间 → ISO 字符串） |
| 时区 | 全站 Asia/Shanghai（`now_cn`） |
| 并发 | 请以**单 worker** 部署；多 worker 会导致 `state` 缓存与决策器状态不一致 |

---

## 1. 认证与会话

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/login` | — | 登录页 |
| POST | `/login` | `username`(Form必填), `password`(Form必填) | 校验账密，成功写 session |
| GET | `/logout` | — | 清除 session 并跳转登录页 |
| GET | `/change_password` | — | 修改密码页（auth） |
| POST | `/change_password` | `current_password`(Form), `new_password`(Form) | 修改当前登录用户密码（auth） |

---

## 2. 页面路由（返回 HTML）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/` | — | 仪表盘：按集群分组展示拓扑、状态与当前主库 |
| GET | `/clusters` | `msg`, `error` | 集群管理总览（状态卡片列表） |
| GET | `/clusters/{cid}` | `cid`(路径), `msg`, `error` | 单集群详情页 |
| GET | `/clusters/{cid}/backup` | `cid`(路径) | 集群备份/恢复页 |
| GET | `/nodes` | `msg`, `error`, `cluster_id`=0 | 节点管理页（可按集群筛选） |
| GET | `/alerts` | `msg`, `error` | 告警中心（SMTP 状态/规则/收件人/当前告警） |
| GET | `/settings` | `msg`, `error` | 系统设置：SMTP 与多邮箱告警、凭证加密、用户与角色 |
| GET | `/audit` | `limit`=200 | 操作审计页 |
| GET | `/nodes/{ip}/creds_sql` | `ip`(路径) | 下载该节点 `fix-node-creds.sql`（auth） |
| GET | `/migration` | `msg`, `error` | 数据迁移向导页（既有 MariaDB 集群 → Custos 托管集群） |
| GET | `/migration/history` | — | 历史迁移记录独立页面 |

> 集群的 HA/复制/业务账号与密码在「集群管理」页维护，不在 `/settings` 中。

---

## 3. 概览 / 状态 / 指标

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/status` | — | 全量状态载荷（集群/节点/VIP/告警等），前端首屏数据源 |
| POST | `/api/refresh` | — | 立即并发探活并刷新；返回与 `/api/status` **同构**载荷，便于前端直接替换。只做探活后立即回读，拓扑收敛交给后台 monitor |
| GET | `/api/vip_status` | — | 各集群 VIP 在线检测（TCP 连通 `VIP:vip_port`） |
| GET | `/api/activity_feed` | `limit`=12 | 实时活动流：切换历史 + 操作审计聚合，按时间倒序 |
| GET | `/api/metric_history` | `cluster_id`=0, `host`='', `port`=0, `hours`=24 | 指标历史快照（趋势图数据源） |
| GET | `/api/node_resources` | `cluster_id`=0, `host`='', `port`=0 | 节点资源指标（连接数/缓冲池/数据量/QPS）。**说明**：远程节点无 SSH/agent，OS 级 CPU/内存/磁盘不可得，以 MySQL 可观测指标近似 |
| GET | `/api/selfheal/stats` | — | 自愈动作统计（进程内计数，管理器重启清零）：回答「自愈动过什么、成功率如何」 |
| GET | `/api/os_metrics` | `cluster_id`=0 | 拉取指定集群各节点的 OS 指标（来自节点 agent /stats）；agent 不可达时 `ok=false` |
| GET | `/api/collect/state` | — | 查询后台手动采集任务状态（供前端轮询，给出真实成败而非盲目「已提交」） |

---

## 4. 集群管理

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/clusters` | `name`, `vip`, `vip_prefix`, `interface`, `vrid`, `vip_port`, `node_type`, `mysql_version`, `ha_user`, `ha_pass`, `repl_user`, `repl_pass`, `app_user`, `app_pass`（均 Form） | 新建集群 |
| POST | `/clusters/{cid}` | 同上 + `ha_regen`, `repl_regen`, `app_regen`（均 Form） | 修改集群；`*_regen` 控制是否重新生成对应账号密码 |
| POST | `/clusters/{cid}/delete` | `cid`(路径) | 删除集群 |
| POST | `/clusters/{cid}/sync` | `cid`(路径) | 把集群最新密码同步到所有在线节点（返回已同步节点数） |

**`node_type` 取值**（三选一）：

| 值 | 含义 | 典型端口 |
|------|------|------|
| `docker` | 容器化 MySQL 8.0 | 3307 |
| `mariadb-docker` | 容器化 MariaDB 12.3.x（**纯 compose** 部署） | 3306 |
| `mariadb` | 裸机 MariaDB 12.x | 3306 |

> `mysql_version` 决定复制语法分支（MySQL 8.0 ↔ MariaDB）；为 NULL 沿用旧语法。

---

## 5. 节点管理

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/nodes/add` | `ip`, `port`, `root_user`, `root_pass`, `initial_role`, `cluster_id`, `node_type`, `mysql_version`（均 Form） | 新增节点（做同构校验、拦截把管理库地址当业务节点） |
| POST | `/nodes/delete` | `ip`(Form) | 删除节点 |
| POST | `/nodes/{ip}/regen` | `ip`(路径) | 用已保存参数重新生成配置文件（覆盖），返回各文件 md5 便于核对 |
| GET | `/api/node_creds_sql` | `cluster_id`=0, `host`='', `port`=0, `download`=0 | 查看/下载节点 `fix-node-creds.sql`（`download=1` 触发文件下载） |
| POST | `/api/node_detail` | `cluster_id`, `host`, `port`（Form） | 节点详情：运行时变量（server_id/datadir/gtid/read_only）+ 账号清单 |
| POST | `/api/node_error_log` | `cluster_id`, `host`, `port`, `limit`（Form） | 读取节点错误日志（优先 mysql.error_log 表，文件 LOAD_FILE 兜底；MySQL/MariaDB 裸机/docker 通用） |
| POST | `/api/fix_node` | `cluster_id`, `host`, `port`（Form） | 一键自愈节点复制参数（`SET PERSIST` 写 `mysqld-auto.cnf`，重启仍生效） |
| POST | `/api/precheck` | `cluster_id`, `host`, `port`（Form） | 节点/集群预检（仅告警）；不传 host/port 则整集群 |
| POST | `/api/collect` | `target`, `cluster_id`（Form） | 手动触发采集，`target` ∈ `metrics`/`slow`/`drift`/`all`。**后台线程非阻塞**，请求立即返回 accepted |
| GET | `/api/nodes/{ip}/pending` | `ip`(路径) | 查询节点当前「配置待生效」动作清单（无待办返回空数组） |
| POST | `/nodes/{ip}/ack_deploy` | `ip`(路径), `force`(Query,默认 false) | 运维在节点执行完动作后点「已执行」；仅清无法远程核验的 manual 类与已核验落实项，未生效项保留（可用 `?force=1` 人工兜底） |

---

## 6. 故障切换与复制自愈

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/failover` | `cluster_id`(Form) | 自动切换：`pick_best_master` 选最优从库并提升为主 |
| POST | `/api/failover/to` | `host`, `port`, `cluster_id`（Form） | 手动指定提升某节点为主 |
| POST | `/api/heal` | `host`, `port`, `cluster_id`（Form） | 复制自愈：SQL 线程冲突跳过 / IO 线程 GTID 缺口补齐 / 通道错位重指向。**IO 与 SQL 双线程独立自愈**，不再互相门控 |

> 切换/自愈会写 `switch_history`，并按集群隔离。

---

## 7. 备份与恢复

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/backup/databases` | `cluster_id`=0, `host`='', `port`=3306 | 列出该节点上可备份的业务库 |
| POST | `/api/backup/run` | `cluster_id`, `host`, `port`, `scope`, `databases`, `with_data`（Form） | 执行备份。`scope=all` 全库；`scope=custom` 时 `databases` 为逗号分隔库名 |
| GET | `/api/backup/list` | `cluster_id`=0 | 该集群历史备份列表（备份文件清单） |
| GET | `/api/backup/history` | `cluster_id`=0 | 该集群的备份记录（倒序，最新在前） |
| POST | `/api/backup/restore` | `cluster_id`, `host`, `port`, `file`, `databases`（Form） | 恢复备份到指定节点（默认恢复全部库，可指定部分）；改为**后台任务**，立即返回 `task_id` |
| GET | `/api/backup/restore_progress` | `task_id`='', `cluster_id`=0 | 查询恢复任务进度；`task_id` 为空时返回该集群最近一次恢复任务 |
| GET | `/api/backup/log` | `cluster_id`=0, `limit`=200 | 读取备份/恢复操作日志（倒序） |
| GET | `/api/restore/history` | `cluster_id`=0 | 读取该集群的恢复记录（倒序，最新在前） |
| POST | `/api/backup/delete` | `cluster_id`, `file`（Form） | 删除备份文件 |
| GET | `/api/backup/preview` | `cluster_id`=0, `file`='' | 备份内容预览 |
| GET | `/api/backup/download` | `cluster_id`=0, `file`='' | 下载备份文件（支持单文件 `.sql.gz` 与分库集目录自动打包 `.tar.gz`） |

> 备份产物落盘 `./backups/<集群>/<库>_<YYYYmmdd-HHMMSS>.sql.gz`；由 `app/backup.py` 纯 Python 实现（镜像内无 `mysqldump`）。

---

## 8. 告警中心

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/active_alerts` | — | 聚合当前未恢复告警：探活失败(`hosts.err`)、配置漂移(`config_drift`)、复制延迟超阈值 |
| GET | `/api/alerts` | `level`='', `status`='', `cluster_id`=0, `hours`=0, `q`='', `page`=1, `page_size`=20 | 告警中心分页查询（按级别/状态/集群/时间/关键字筛选） |
| POST | `/api/alert_ack` | JSON：`id` / `ids` / `all` | 确认（消音）告警：单条、批量或全部 |
| POST | `/api/alert_rules` | `cooldown`, `merge`, `lag_threshold`, `receivers`, `grace`（Form） | 保存告警规则（冷却/合并/延迟阈值/额外收件人/自愈观察期） |

> 可自愈告警自动标记 `healed`，未恢复标记 `unrecovered`。

---

## 9. 切换历史 / 审计 / 慢日志 / 配置漂移 / 一致性 / 节点重建

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/switch_history` | `cluster_id`=0, `hours`=0, `page`=1, `page_size`=20 | 切换历史（分页） |
| GET | `/api/audit` | `limit`=200, `cluster_id`=0 | 操作审计日志 |
| GET | `/api/slow_log` | `cluster_id`=0, `host`='', `port`=0, `limit`=200, `min_ms`=0 | 慢日志明细（按耗时倒序） |
| GET | `/api/config_drift` | `cluster_id`=0 | 最新一次配置一致性巡检结果 |
| POST | `/api/config_drift/remediate` | `cluster_id`=0（Form） | 一键修复最近一次巡检的 DRIFT 项（在线 `SET PERSIST`；其余标 `manual`） |
| GET | `/api/consistency/last` | `cluster_id`=0 | 最近一次主从一致性校验结论（集群页徽章用；从未校验过返回 `has=false`） |
| GET | `/api/consistency/check` | `cluster_id`=0, `mode`='rows' | 主从数据一致性校验（GET 形式，便于脚本调用）。`mode=rows` 比行数 / `checksum` 比 CHECKSUM TABLE |
| POST | `/api/consistency/check` | `cluster_id`(Form), `mode`(Form,'rows'), `wait_sec`(Form,-1) | 一致性校验（POST）；`wait_sec` 为候选差异复核前等待从库追平秒数上限（-1=模块默认） |
| POST | `/api/consistency/row_diff` | `cluster_id`, `host`, `port`, `table`（Form，`db.tbl`） | 展开某张差异表的行级差异（主键 + 两侧值） |
| POST | `/api/consistency/export` | `cluster_id`, `host`, `port`, `table`（Form） | 生成差异处置 SQL 并落 quarantine（卷内回收表，可一键回灌主库） |
| POST | `/api/consistency/remaster` | `recycle_id`(Form) | 把回收表中的从库独有行回灌到当前主库（救回分裂窗口内的真实写入） |
| POST | `/api/node/rebuild` | `cluster_id`, `host`, `port`（Form） | 从库在线重建（异步）：主库 dump → 从库 restore（关 binlog）→ GTID 重链；严禁对当前主库执行 |
| GET | `/api/node/rebuild_status` | `task`='' | 查询重建任务进度（status/pct/msg） |

> 切换/自愈会写 `switch_history`，并按集群隔离。一致性校验结论持久化（`consistency.save_result`），使「有未处置差异」与「页面显示正常」不可能同时成立。

---

## 10. 终端 / 日志 / 设置 / 凭证加密 / 用户角色 / 维护窗口

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/term` | `cluster_id`, `host`, `port`, `sql`（Form） | 在线 SQL 终端。优先用节点 root（可读写，支持全部 SQL）；未配置 root 密码则回退 `ha_monitor` 只读 |
| POST | `/api/term_complete` | `cluster_id`, `host`, `port`（Form） | Tab 补全候选：关键字 + 库 + 表 + 列 |
| GET | `/api/srv_log` | `svc`='ha_manager', `since`=86400, `offset`=0 | 读取管理端容器日志（经宿主 `docker.sock`） |
| GET | `/api/srv_units` | — | 动态列举宿主上可读取日志的容器（供下拉选择） |
| POST | `/api/setting_set` | `key`, `value`（Form） | 保存开关/配置项（**白名单校验**，见下） |
| POST | `/settings` | `smtp_host`, `smtp_port`, `smtp_user`, `smtp_pass`, `check_interval`, `manager_ip`（Form） | 保存 SMTP 与轮询间隔、管理器内网 IP |
| POST | `/settings/test_email` | — | 发送测试邮件（发往全部已配置邮箱） |
| POST | `/settings/email/add` | `email`(Form) | 新增收件邮箱 |
| POST | `/settings/email/delete` | `id`(Form) | 删除收件邮箱 |
| POST | `/settings/email/set_default` | `id`, `is_default`（Form） | 设为默认（接收全部集群） |
| POST | `/settings/email/toggle` | `id`, `enabled`（Form） | 启停该邮箱接收告警 |
| POST | `/settings/email/map` | `id`, `cluster_ids`（Form） | 绑定邮箱 ↔ 集群（`cluster_ids` 为逗号分隔） |
| GET | `/api/cred_status` | — | 凭证加密状态 + 凭证体检（系统设置「凭证加密」卡片用） |
| POST | `/api/cred_encryption` | `action`(Form: `enable`/`disable`/`reset_key`) | 凭证加密开关 / 重置密钥；每个操作返回体检结果，明确列出损坏项 |
| POST | `/api/cred_recover` | — | 从节点生成包取回明文口令并回填（依赖文件，不连节点 DB） |
| POST | `/api/cred_reset_repl` | — | 批量重置复制口令：生成新 `repl_pass` 落库，并在每个节点重置复制账号密码、对从库以新口令重挂 |
| GET | `/api/users` | — | 用户列表（含角色） |
| POST | `/api/users/add` | `username`, `password`, `role`(Form,'admin') | 新增用户；`role=viewer` 为只读账号 |
| POST | `/api/users/role` | `username`, `role`（Form） | 修改用户角色；禁止把最后一个管理员降级为只读 |
| POST | `/api/users/delete` | `username`(Form) | 删除用户；禁止删除自己、禁止删光最后一个管理员 |
| GET | `/api/maintenance` | `cluster_id`=0 | 列出生效中的维护窗口（`cluster_id=0` 表示全部集群） |
| POST | `/api/maintenance/begin` | `cluster_id`, `scope`(Form,'cluster'), `target`, `reason`, `minutes`(Form,0) | 开启维护窗口：抑制自动切换与状态类告警。`minutes>0` 到期自动结束；0 表示不限时需手动结束 |
| POST | `/api/maintenance/end` | `cluster_id`, `scope`, `target`（Form） | 结束维护窗口；`scope` 为空时结束该集群全部窗口 |

### `/api/setting_set` 白名单

不在白名单的 key 返回 `{"ok":false,"error":"不允许修改该项"}`。

```
AUTO_FAILOVER, AUTO_REPAIR,
ALERT_COOLDOWN_SEC, ALERT_MERGE, ALERT_LAG_THRESHOLD, ALERT_RECEIVERS, ALERT_GRACE_SEC,
SLOWLOG_ENABLED,
LAG_FAILOVER_ENABLED, LAG_FAILOVER_SEC, LAG_FAILOVER_CYCLES,
PRESSURE_FAILOVER_ENABLED, PRESSURE_THREADS_RUNNING, PRESSURE_ROW_LOCK_WAITS,
FAILOVER_COOLDOWN_SEC,
SKIP_DATA_CONFLICT, REQUIRE_WITNESS_FOR_FAILOVER,
AUTO_PURGE_BINLOG, BINLOG_SIZE_ALERT_MB, BINLOG_KEEP_HOURS, SEMI_SYNC,
AUTO_REBUILD_ON_PURGE
```

> 高延迟 / 压力自动切换（v2.2.9+）**默认关闭**，需显式开启。

---

## 11. 数据迁移向导（既有 MariaDB 集群 → Custos 托管集群）

> 专注 MariaDB；步骤驱动：前端逐步调用，天然支持重试（重复调用）/ 暂停（不调下一步）。除页面（`GET /migration`、`GET /migration/history` 见第 2 节）外，接口均以 `application/json` 提交（支持 `curl -H 'Content-Type: application/json' -d '{...}'`）。

| 方法 | 路径 | 参数（JSON 体） | 说明 |
|------|------|------|------|
| POST | `/api/migration/probe` | `src_host`, `src_port`, `src_user`, `src_pass` | 探测源 MariaDB（连通性/版本/权限/规模） |
| POST | `/api/migration/recommend` | `size_mb`, `flavor`, `channel_pref` | 按数据量推荐迁移通道（逻辑导入） |
| POST | `/api/migration/probe_target` | `tgt_host`, `tgt_port`, `tgt_user`, `tgt_root_pass` | 探测目标 MariaDB：版本 + 账号分层权限提示 |
| POST | `/api/migration/target_hint` | `ip` | 按目标 IP 自动给出端口/形态（取 `nodes` 表登记，未登记回退 `NODE_PORT`） |
| POST | `/api/migration/create` | `name`, `node_type`, `databases`, `src_*`, `tgt_*`, `src_flavor`, `tgt_flavor`, `size_mb` 等 | 创建迁移任务（落 `migration_tasks`，返回 `id`）；`node_type` 必须显式选择 |
| POST | `/api/migration/{task_id}/step` | `step`, `deep`(可选) | 异步启动一步（立即返回，前端轮询 `/status`） |
| POST | `/api/migration/{task_id}/cancel` | `task_id`(路径) | 中止执行中的迁移任务（安全退出并释放并发门禁） |
| POST | `/api/migration/{task_id}/precheck` | `task_id`(路径) | 迁移前兼容性预检（dry-run，不搬数据） |
| POST | `/api/migration/{task_id}/heal` | `task_id`(路径) | 迁移目标复制自愈（目标尚未登记进集群，直接用任务记录的 root 口令连接） |
| POST | `/api/migration/privileges` | 源库连接信息 | 源库账号权限体检：列出各步骤依赖权限是否具备 |
| POST | `/api/migration/cleanup-stale` | — | 清理卡死/陈旧的执行中任务（无后台线程在跑的 running 记录） |
| POST | `/api/migration/unlock` | `host`, `port`, `clear_tasks`(默认 true) | 强制解除某节点迁移锁（及可选清理指向它的 running 任务） |
| GET | `/api/migration/{task_id}/status` | `task_id`(路径) | 查询迁移任务进度/阶段（`progress`/`stage`/各步状态） |
| GET | `/api/migration/list` | — | 迁移任务列表 |
| DELETE | `/api/migration/{task_id}` | `task_id`(路径) | 删除迁移任务（执行中任务仅移除记录，不停止后台步骤） |

---

## 12. 调用示例

```bash
# 登录（保存 cookie）
curl -c ck.txt -X POST http://127.0.0.1:28888/login \
  -d 'username=admin&password=admin'

# 拉取全量状态
curl -b ck.txt http://127.0.0.1:28888/api/status

# 立即刷新（探活）
curl -b ck.txt -X POST http://127.0.0.1:28888/api/refresh

# 手动切换（指定提升目标）
curl -b ck.txt -X POST http://127.0.0.1:28888/api/failover/to \
  -d 'cluster_id=1&host=192.0.2.213&port=3306'

# 复制自愈
curl -b ck.txt -X POST http://127.0.0.1:28888/api/heal \
  -d 'cluster_id=1&host=192.0.2.213&port=3306'

# 开启维护窗口（抑制自动切换，2 小时后自动结束）
curl -b ck.txt -X POST http://127.0.0.1:28888/api/maintenance/begin \
  -d 'cluster_id=1&scope=cluster&minutes=120&reason=例行维护'

# 下载节点凭证修复 SQL
curl -b ck.txt -o node213.sql \
  'http://127.0.0.1:28888/api/node_creds_sql?cluster_id=1&host=192.0.2.213&port=3306&download=1'

# 在线终端执行 SQL
curl -b ck.txt -X POST http://127.0.0.1:28888/api/term \
  -d 'cluster_id=1&host=192.0.2.213&port=3306&sql=SHOW REPLICA STATUS'

# 开启延迟自动切换
curl -b ck.txt -X POST http://127.0.0.1:28888/api/setting_set \
  -d 'key=LAG_FAILOVER_ENABLED&value=1'

# 创建迁移任务（JSON 体）
curl -b ck.txt -X POST http://127.0.0.1:28888/api/migration/create \
  -H 'Content-Type: application/json' \
  -d '{"name":"迁移A","node_type":"mariadb-docker","databases":["shop"],"src_host":"192.0.2.51","src_port":3306,"src_user":"root","src_pass":"srcpw","tgt_host":"192.0.2.203","tgt_port":3307,"tgt_root_pass":"tgtpw","src_flavor":"mariadb","tgt_flavor":"mariadb","size_mb":5120}'
```

---

## 13. 错误处理与排错

| 场景 | 表现 | 处理 |
|------|------|------|
| 业务校验失败 | HTTP 200 + `{"ok":false,"error":"..."}` | 读 `error` 字段 |
| 未登录 | 页面 → 302 登录页；API → 403 | 重新 `POST /login` |
| 非白名单配置项 | `{"ok":false,"error":"不允许修改该项"}` | 仅可用白名单 key |
| 节点不可达 | `hosts.status=down`，`err` 带真实原因（账号错误 / 网络不可达） | 「修正节点」更新凭证或排查网络 |
| 迁移/重建失败 | 任务 `ok=false` + `error`/`msg` | 查 `/api/migration/{task_id}/status`、`/api/node/rebuild_status` 或 `/api/backup/log` |
| datetime 序列化 | 由 `_json_ok` 统一转 ISO 字符串 | 避免裸 500 |

> 前端 `dashboard.html` 通过 `data-f` 增量打补丁（`patchField`），仅更新变化单元格，稳态下不整表重绘。

---

## 14. 与旧版（v2.2.20）文档的差异（重要）

旧版文档标注「共 63 个端点」且缺漏大量 v2.3.x / v2.4.0 引入的能力。本版据 `app/api.py` 实际路由重写为 **109** 个端点。**旧文档列出的端点全部仍存在，无需要删除的过时端点**（旧文档仅为不完整列表）。

**v2.4.0 相对旧文档新增的主要能力（端点）：**
- 数据迁移向导：`/migration`、`/migration/history` 及 `/api/migration/*`（共 17 个，含 1 个 DELETE）
- 维护窗口：`/api/maintenance`、`/api/maintenance/begin`、`/api/maintenance/end`
- 用户与角色：`/api/users`、`/api/users/add`、`/api/users/role`、`/api/users/delete`
- 凭证加密：`/api/cred_status`、`/api/cred_encryption`、`/api/cred_recover`、`/api/cred_reset_repl`
- 主从一致性校验：`/api/consistency/last`、`/api/consistency/check`（GET/POST）、`/api/consistency/row_diff`、`/api/consistency/export`、`/api/consistency/remaster`
- 从库在线重建：`/api/node/rebuild`、`/api/node/rebuild_status`
- 监控/运维增强：`/api/selfheal/stats`、`/api/os_metrics`、`/api/collect/state`、`/api/config_drift/remediate`、`/api/nodes/{ip}/pending`、`/nodes/{ip}/ack_deploy`
- 备份/恢复增强：`/api/backup/history`、`/api/backup/restore_progress`、`/api/backup/log`、`/api/restore/history`
- `/api/setting_set` 白名单由 14 项扩充为 24 项（新增 `SKIP_DATA_CONFLICT`、`REQUIRE_WITNESS_FOR_FAILOVER`、`AUTO_PURGE_BINLOG`、`BINLOG_SIZE_ALERT_MB`、`BINLOG_KEEP_HOURS`、`SEMI_SYNC`、`ALERT_GRACE_SEC`、`AUTO_REBUILD_ON_PURGE`）

> 注意：v2.4.0 稳定支线**不包含** v2.5.x 才有的集群接管（adopt）、curl 引导 bootstrap、agent 指令通道、滚动升级（upgrade）、强制同步（syncgate）、无保护窗口、集群级定时备份等特性——这些端点在本仓库 `app/api.py` 中均不存在。
