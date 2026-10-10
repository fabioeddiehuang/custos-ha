# Custos HA · API 接口文档

> 适用版本：**v2.5.1**（与 `.env` 的 `VERSION` 一致）
> 基址：管理端地址（默认 `http://<manager>:28888`）
> 本文档由 `app/api.py` 实际路由导出（共 **140** 个端点），与代码强一致。

## 通用约定

| 项 | 说明 |
|------|------|
| 鉴权 | 除 `POST /login` 及节点侧无会话端点（`/api/node/bootstrap/{token}`、`/api/node/bundle/{token}`、`/api/node/image/{name}`、`/api/nodes/{ip}/ready`、`/api/agent/heartbeat`、`/api/agent/cmd_result`）外全部需登录态：`Cookie: session=<id>`（由 `POST /login` 写入）。未登录访问页面路由 → 302 重定向登录页；访问 API → 403 / `{"ok":false}`。节点侧端点凭**每节点独立 token** 或 **IP 绑定 token** 认证 |
| 参数绑定 | `Form` = 表单字段（`application/x-www-form-urlencoded`）；`=默认值` = 查询参数（Query，可省略）；`必填` = 无默认值、必须传。部分指令类接口（迁移、agent）收 JSON 请求体（`await request.json()`） |
| 返回约定 | 成功 `{"ok":true, ...}`；**业务失败返回 HTTP 200 + `{"ok":false,"error":"..."}`**（非 4xx/5xx） |
| 序列化 | 日期/时间/Decimal/bytes 统一由 `_json_ok` 兜底转 JSON（时间 → ISO 字符串） |
| 时区 | 全站 Asia/Shanghai（`now_cn`） |
| 并发 | 请以**单 worker** 部署；多 worker 会导致 `state` 缓存与决策器状态不一致 |
| 角色 | 管理员（admin）/ 只读（viewer）。**viewer 的任何写操作都会被服务端拒绝（403）** |

---

## 1. 认证与会话

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/login` | — | 登录页 |
| POST | `/login` | `username`(Form必填), `password`(Form必填) | 校验账密，成功写 session |
| GET | `/logout` | — | 清除 session 并跳转登录页 |
| GET | `/change_password` | — | 修改密码页（auth） |
| POST | `/change_password` | `current_password`(Form), `new_password`(Form) | 修改当前登录用户密码（auth），成功后清除 `ha_must_change` 强制改密标记 |

---

## 2. 页面路由（返回 HTML）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/` | — | 仪表盘：按集群分组展示拓扑、状态与当前主库；含「无保护窗口」红色横幅 |
| GET | `/clusters` | `msg`, `error` | 集群管理总览（状态卡片列表） |
| GET | `/clusters/{cid}` | `cid`(路径), `msg`, `error` | 单集群详情页（含「升级」Tab） |
| GET | `/clusters/{cid}/upgrade` | `cid`(路径), `msg`, `error` | 集群滚动升级向导页（v2.5.0 F8） |
| GET | `/clusters/{cid}/backup` | `cid`(路径), `msg`, `error` | 集群备份/恢复页（含备份文件/记录/恢复记录/操作日志 Tab） |
| GET | `/adopt` | `msg`, `error` | 集群接管（纳管已有库）向导页（v2.5.0 F10） |
| GET | `/nodes` | `msg`, `error`, `cluster_id`=0 | 节点管理页（可按集群筛选，含「配置待生效」徽标） |
| GET | `/alerts` | `msg`, `error` | 告警中心（SMTP 状态/规则/收件人/当前告警） |
| GET | `/settings` | `msg`, `error` | 系统设置：SMTP、轮询间隔、管理器 IP、全局开关、用户与角色、容器日志 |
| GET | `/audit` | `limit`=200 | 操作审计页（导航暂无入口，直接访问） |
| GET | `/migration` | `msg`, `error` | 数据迁移向导页 |
| GET | `/migration/history` | — | 历史迁移记录独立页面 |
| GET | `/nodes/{ip}/creds_sql` | `ip`(路径) | 下载该节点 `fix-node-creds.sql`（auth，含真实凭证，仅限节点本机 root 执行） |

> 集群的 HA/复制/业务账号与密码在「集群管理」页维护，不在 `/settings` 中。

---

## 3. 概览 / 状态 / 指标

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/status` | — | 全量状态载荷（集群/节点/VIP/告警等），前端首屏数据源 |
| GET | `/api/selfheal/stats` | — | 自愈动作统计（进程内计数，管理器重启清零） |
| POST | `/api/refresh` | — | 立即并发探活并刷新；返回与 `/api/status` **同构**载荷，便于前端直接替换。只做探活后立即回读，拓扑收敛交给后台 monitor |
| GET | `/api/vip_status` | — | 各集群 VIP 在线检测（TCP 连通 `VIP:vip_port`） |
| GET | `/api/activity_feed` | `limit`=12 | 实时活动流：切换历史 + 操作审计聚合，按时间倒序 |
| GET | `/api/metric_history` | `cluster_id`=0, `host`='', `port`=0, `hours`=24 | 指标历史快照（趋势图数据源） |
| GET | `/api/os_metrics` | `cluster_id`=0 | 节点 OS 指标（来自节点 agent `/stats`；agent 不可达时 `ok=false`） |
| GET | `/api/node_resources` | `cluster_id`=0, `host`='', `port`=0 | 节点资源指标（连接数/缓冲池/数据量/QPS）；远程节点无 agent 时以 MySQL 可观测指标近似 |

---

## 4. 集群管理

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/clusters` | `name`, `vip`, `vip_prefix`, `interface`, `vrid`, `vip_port`, `node_type`, `mysql_version`, `ha_user`, `ha_pass`, `repl_user`, `repl_pass`, `app_user`, `app_pass`（均 Form） | 新建集群（成功后跳转详情页） |
| POST | `/clusters/{cid}` | 同上 + `ha_regen`, `repl_regen`, `app_regen`（均 Form） | 修改集群；`*_regen` 控制是否重新生成对应账号密码；保存即「保存并同步」把密码下发到在线节点，并重渲染各节点部署文件（keepalived.conf + compose） |
| POST | `/clusters/{cid}/delete` | `cid`(路径) | 删除集群（清掉节点部署单元引用，不动被管节点数据） |
| POST | `/clusters/{cid}/sync` | `cid`(路径) | 把集群最新密码同步到所有在线节点（返回已同步节点数） |

**`node_type` 取值**（三选一）：

| 值 | 含义 | 典型端口 |
|------|------|------|
| `docker` | 容器化 MySQL 8.0 | 3307 |
| `mariadb-docker` | 容器化 MariaDB 12.3.x（**纯 compose** 部署） | 3306 |
| `mariadb` | 裸机 MariaDB 12.x | 3306 |

> `mysql_version` 决定复制语法分支（MySQL 8.0 ↔ MariaDB）；为 NULL 沿用旧语法。新增集群默认 `node_type=docker`；MySQL 与 MariaDB 禁止混部。

---

## 5. 节点管理

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/nodes/add` | `ip`, `port`='3307', `root_user`='root', `root_pass`, `initial_role`='master', `cluster_id`, `node_type`='docker', `mysql_version`, `server_id`='', `install_dir`='', `interface`=''（均 Form） | 新增节点（同构校验，拦截把管理库地址当业务节点，多网卡节点可逐节点设网卡） |
| POST | `/nodes/delete` | `ip`(Form) | 删除节点（并清理项目目录对应部署单元） |
| POST | `/nodes/{ip}/regen` | `ip`(路径) | 用已保存参数重新生成配置文件（覆盖），返回各文件 md5 与「待生效动作」清单 |
| POST | `/nodes/{ip}/ack_deploy` | `ip`(路径), `force`=False | 运维在节点执行完动作后「已执行」：远程核验落实的动作自动清除，未落实的保留（force 可人工绕过核验） |
| GET | `/api/nodes/{ip}/pending` | `ip`(路径) | 查询节点「配置待生效」动作清单（recreate/restart/exec/manual） |
| GET | `/api/node_creds_sql` | `cluster_id`=0, `host`='', `port`=0, `download`=0 | 查看/下载节点 `fix-node-creds.sql`（`download=1` 触发文件下载） |
| POST | `/api/node_detail` | `cluster_id`, `host`, `port`（Form） | 节点详情：运行时变量（server_id/datadir/gtid/read_only）+ 账号清单 + 版本支持矩阵 |
| POST | `/api/node_error_log` | `cluster_id`, `host`, `port`, `limit`=500, `segment`=''（Form） | 读取节点错误日志（三级：mysql.error_log 表 → LOAD_FILE → 节点 agent）；`segment=startup` 返回最近一次启动段 |
| POST | `/api/fix_node` | `cluster_id`, `host`, `port`=0（Form） | 一键自愈单节点复制参数（`SET PERSIST` 写 `mysqld-auto.cnf`，重启仍生效） |
| POST | `/api/precheck` | `cluster_id`, `host`='', `port`=0（Form） | 节点/集群预检（仅告警）；不传 host/port 则整集群 |
| POST | `/api/collect` | `target`='all', `cluster_id`=0（Form） | 手动触发采集，`target` ∈ `metrics`/`slow`/`drift`/`all`。**后台线程非阻塞**，立即返回 accepted |
| GET | `/api/collect/state` | — | 查询后台采集任务状态（供前端轮询真实成败） |

---

## 6. 故障切换与复制自愈

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/failover` | `cluster_id`(Form) | 自动切换：`pick_best_master` 选最优从库并提升为主（含切换冷却） |
| POST | `/api/failover/to` | `host`, `port`, `cluster_id`=0（Form） | 手动指定提升某节点为主（未传 cluster_id 按 host 反查） |
| POST | `/api/heal` | `cluster_id`, `host`, `port`（Form） | 复制自愈：SQL 线程冲突跳过 / IO 线程 GTID 缺口补齐 / 通道错位重指向。**IO 与 SQL 双线程独立自愈**；连点有 15s 冷却；主库 binlog 已清理（1236）直接给出重建指引 |

> 切换/自愈会写 `switch_history`，并按集群隔离；手动 heal 命中 `selfheal` 告警去重。

---

## 7. 一致性校验（v2.5.0 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/consistency/check` | `cluster_id`(Form), `mode`='rows', `wait_sec`=-1 | 触发主从一致性校验（行级/校验和）；`wait_sec>=0` 同步等待 |
| GET | `/api/consistency/check` | `cluster_id`=0, `mode`='rows' | GET 形式一致性校验（便于脚本直接调用），结论持久化 |
| GET | `/api/consistency/last` | `cluster_id`=0 | 最近一次一致性校验结论（从未校验过返回 `has=false`） |
| POST | `/api/consistency/row_diff` | `cluster_id`, `host`, `port`, `table`（Form） | 行级比对差异 |
| POST | `/api/consistency/export` | `cluster_id`, `host`, `port`, `table`（Form） | 导出差异/修复脚本 |
| POST | `/api/consistency/remaster` | `recycle_id`(Form) | 把回收表中从库独有行回灌到当前主库（救回分裂窗口内的真实写入） |

---

## 8. 节点在线重建（v2.5.0 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/node/rebuild` | `cluster_id`, `host`, `port`（Form） | 从库在线重建（异步）：主库 dump → 从库 restore（关 binlog）→ GTID 重链 |
| GET | `/api/node/rebuild_status` | `task`='' | 查询重建任务进度（`status`/`pct`/`msg`） |

---

## 9. 备份与恢复（含定时备份策略）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/backup/databases` | `cluster_id`=0, `host`='', `port`=3306 | 列出该节点上可备份的业务库 |
| POST | `/api/backup/run` | `cluster_id`, `host`, `port`=3306, `scope`='all', `databases`='', `with_data`=True（Form） | 执行备份。`scope=all` 全库；`scope=custom` 时 `databases` 为逗号分隔库名 |
| GET | `/api/backup/list` | `cluster_id`=0 | 该集群历史备份文件列表 |
| GET | `/api/backup/history` | `cluster_id`=0 | 该集群备份记录（倒序） |
| POST | `/api/backup/restore` | `cluster_id`, `host`, `port`=3306, `file`, `databases`=''（Form） | 恢复备份（**后台任务**，立即返回 `task_id`，前端轮询 `/api/backup/restore_progress`） |
| GET | `/api/backup/restore_progress` | `task_id`='', `cluster_id`=0 | 恢复任务进度；`task_id` 空时返回该集群最近一次恢复任务 |
| GET | `/api/backup/log` | `cluster_id`=0, `limit`=200 | 备份/恢复操作日志（倒序） |
| GET | `/api/restore/history` | `cluster_id`=0 | 该集群恢复记录（倒序） |
| POST | `/api/backup/delete` | `cluster_id`, `file`（Form） | 删除备份文件（不可恢复） |
| GET | `/api/backup/preview` | `cluster_id`=0, `file`='' | 备份内容预览 |
| GET | `/api/backup/download` | `cluster_id`=0, `file`='' | 下载备份文件（单文件或分库集目录均流式下发） |
| GET | `/api/backup/policy` | `cluster_id`=0 | 读取集群定时备份策略 + 下次执行时间 + 当前占用 |
| POST | `/api/backup/policy` | `cluster_id`, `enabled`=False, `interval_hours`=24, `retention_days`=7, `retention_count`=7, `target`='slave', `with_data`=True, `wait_catch_up`=True, `disk_alert_gb`=0（Form） | 设置**集群级定时备份**：保留份数 + 保留天数双条件（任一满足即保留，**绝不删最新一份**）；错峰执行、磁盘预警、备份前追平等待 |

> 备份产物落盘 `./backups/<集群>/<库>_<YYYYmmdd-HHMMSS>.sql.gz`；由 `app/backup.py` 纯 Python 实现（镜像内无 `mysqldump`）。定时备份策略由 `app/backup_policy.py` 执行（v2.5.0 F9）。

---

## 10. 凭证与密码管理（v2.5.0 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/passwords/change` | `cluster_id`, `ha_pass`='', `repl_pass`='', `app_pass`='', `root_pass`='', `sync_files`=1（Form） | 免 SSH 改密：主库 `ALTER USER` → 更新凭证记录 → 复制下发 → 同步节点凭据文件 → 重启 keepalived；任一环节失败即回滚。留空=不改该项 |
| GET | `/api/passwords/accounts` | `cluster_id`=0 | 列出本集群可修改的账号（`ha_monitor`/`replication`/`app`/`root`）供 UI 勾选 |
| GET | `/api/cred_status` | — | 凭证加密状态 + 凭证体检（系统设置「凭证加密」卡片） |
| POST | `/api/cred_encryption` | `action`(Form) | 凭证加密开关/重置密钥：`enable`/`disable`/`reset_key`（密钥丢失后关闭加密无法恢复已损坏密文） |
| POST | `/api/cred_recover` | — | 从节点生成包取回明文口令并回填（重装/重建管理器导致密文解不开时的「一键恢复」） |
| POST | `/api/cred_reset_repl` | — | 批量重置复制口令：生成新 `repl_pass` 落库并在各节点重置复制账号、对从库重挂 |

---

## 11. 告警中心

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/active_alerts` | — | 聚合当前未恢复告警：探活失败(`hosts.err`)、配置漂移(`config_drift`)、复制延迟超阈值；叠加自愈状态层 |
| GET | `/api/alerts` | `level`='', `status`='', `cluster_id`=0, `hours`=0, `q`='', `page`=1, `page_size`=20 | 告警中心分页查询（按级别/状态/集群/时间/关键字筛选） |
| POST | `/api/alert_ack` | `id` / `ids` / `all`（JSON body） | 确认（消音）告警：单条、批量或全部 |
| POST | `/api/alert_rules` | `cooldown`, `merge`, `lag_threshold`, `receivers`, `grace`（Form） | 保存告警规则（冷却/合并/延迟阈值/额外收件人/自愈观察期 `ALERT_GRACE_SEC`） |

> 可自愈告警自动标记 `healed`，未恢复标记 `unrecovered`。

---

## 12. 切换历史 / 审计 / 慢日志 / 配置漂移

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/switch_history` | `cluster_id`=0, `hours`=0, `page`=1, `page_size`=20 | 切换历史（分页） |
| GET | `/api/audit` | `limit`=200, `cluster_id`=0 | 操作审计日志 |
| GET | `/api/slow_log` | `cluster_id`=0, `host`='', `port`=0, `limit`=200, `min_ms`=0 | 慢日志明细（按耗时倒序） |
| GET | `/api/config_drift` | `cluster_id`=0 | 最新一次配置一致性巡检结果 |
| POST | `/api/config_drift/remediate` | `cluster_id`=0（Form） | 一键修复最近一次巡检的 DRIFT 项（在线 `SET PERSIST`，仅可在线修复项；其余标 manual） |

---

## 13. 用户与角色（v2.3.0 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/users` | — | 用户列表（含角色） |
| POST | `/api/users/add` | `username`, `password`, `role`='admin'（Form） | 新增用户；`role=viewer` 为只读账号 |
| POST | `/api/users/role` | `username`, `role`（Form） | 修改用户角色；禁止把最后一个管理员降级为只读 |
| POST | `/api/users/delete` | `username`(Form) | 删除用户；禁止删除自己、禁止删光最后一个管理员 |

---

## 14. 维护窗口（v2.3.3 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/maintenance` | `cluster_id`=0 | 列出生效中的维护窗口（0=全部集群） |
| POST | `/api/maintenance/begin` | `cluster_id`, `scope`='cluster', `target`='', `reason`='', `minutes`=0（Form） | 开启维护：`scope=cluster` 抑制整集群自动切换与状态告警；`scope=node` 只覆盖单节点。`minutes>0` 到期自动结束，0=不限时 |
| POST | `/api/maintenance/end` | `cluster_id`, `scope`='', `target`=''（Form） | 结束维护窗口；`scope` 为空时结束该集群全部窗口 |

---

## 15. 数据迁移向导（v2.4.0 重做 / v2.5.x 完善）

> 专注 MariaDB：纯逻辑导出导入（非物理通道）；步骤驱动、支持重试/暂停。**只搬数据，不搬账号权限**；MySQL 8.0 → MariaDB 明确拒绝。

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/migration/probe` | JSON body：`src_host`, `src_port`, `src_user`, `src_pass` | 探测源库（版本/可连接/权限），只读不写 |
| POST | `/api/migration/recommend` | JSON body：`size_mb`, `flavor`, `channel_pref` | 按数据量推荐迁移通道 |
| POST | `/api/migration/probe_target` | JSON body：`tgt_host`, `tgt_port`, `tgt_user`, `tgt_root_pass` | 探测目标库版本 + 账号分层权限 |
| POST | `/api/migration/target_hint` | JSON body：`ip` | 按目标 IP 自动给出端口/形态（优先查 nodes 表） |
| POST | `/api/migration/create` | JSON body：`node_type`, `databases`, `src_*`, `tgt_*`, `src_flavor`, `src_major`, `tgt_flavor`, `size_mb` | 创建迁移任务（draft）；`node_type` ∈ `mariadb`/`mariadb-docker`/`docker` |
| POST | `/api/migration/{task_id}/step` | JSON body：`step`, `deep`（异步） | 启动一步（后台线程），前端轮询 `/status` |
| POST | `/api/migration/{task_id}/cancel` | — | 中止执行中的任务（置标记，由步骤循环安全退出） |
| POST | `/api/migration/{task_id}/precheck` | — | 迁移前兼容性预检（dry-run） |
| POST | `/api/migration/{task_id}/heal` | — | 迁移目标复制自愈（桥接阶段从库线程异常时） |
| POST | `/api/migration/privileges` | JSON body：源库账号凭据 | 源库账号权限体检（各步骤依赖的权限是否具备） |
| GET | `/api/migration/{task_id}/status` | `task_id`(路径) | 任务完整状态（含 steps/progress/stage/log） |
| GET | `/api/migration/list` | — | 任务列表 |
| DELETE | `/api/migration/{task_id}` | `task_id`(路径) | 删除任务（执行中仅移除记录，不停止后台） |
| POST | `/api/migration/cleanup-stale` | — | 清理卡死/陈旧的执行中任务 |
| POST | `/api/migration/unlock` | JSON body：`host`, `port`, `clear_tasks`=True | 强制解除某节点的迁移锁（+ 清理指向它的 running 任务） |

---

## 16. 集群接管 adopt（v2.5.0 F10 新增）

> 接管 = **纳管**，不是重建：只读探测 → 建账号 → 登记 → 用引导命令装 sidecar。全程不碰数据、不重装数据库。

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/adopt/probe` | `nodes`(JSON 数组, Form), `l2_prefix`=24 | 只读探测一组存量节点（幂等，绝不写入老库） |
| POST | `/api/adopt/fix_switch` | `host`, `port`=3306, `db_user`='root', `db_pass`='', `server_id`=0 | 快速修复：在线设 `server_id`（log_bin 只能给命令人工重启） |
| POST | `/api/adopt/test_root` | `host`, `port`=3306, `db_user`='root', `db_pass`='' | 验证节点 root 口令（只连不建账号） |
| POST | `/api/adopt/fix_cmd` | `host`, `port`, `db_user`, `db_pass`, `server_id`=0, `container`='' | 按形态（裸机/容器）重新生成修复命令（只读） |
| POST | `/api/adopt/check_vip` | `vip`, `cluster_id`=0 | VIP 占用检测（登记比对 + 管理器 ping 警告） |
| POST | `/api/adopt/check_vrid` | `vrid`, `vip`='', `prefix`=24, `cluster_id`=0 | VRID 同网段冲突检测，冲突时推荐空闲值 |
| POST | `/api/adopt/release_read_only` | `host`, `port`=3306, `db_user`='root', `db_pass`='' | 在线解除 `read_only`/`super_read_only`（仍是别人从库时拒绝） |
| POST | `/api/adopt/run` | `nodes`(JSON), `cluster_id`=0, `name`, `vip`, `vip_prefix`='24', `vip_port`=3306, `root_user`='root', `root_pass`, `node_roots`(JSON), `deploy_form`, `container`, `vrid`, `interface`（Form） | 执行接管：建 Custos 账号 → 登记 nodes/hosts → 生成 sidecar 引导命令；`cluster_id=0` 按 name/vip 新建集群 |

---

## 17. 节点 curl 一行引导安装（v2.5.0 F1/F3 新增）

> 节点 `curl … | bash` 拉取引导脚本，含一次性 token（绑定 IP + 有效期 + 用后作废）。

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/node/bootstrap` | `ip`, `ttl_hours`=24, `install_dir`=''（Form） | 生成节点引导命令（含一次性 token；`install_dir` 须绝对路径） |
| GET | `/api/node/bootstrap/{token}` | `token`(路径) | 引导安装脚本本体（节点 `curl` 拉取）；token 无效/过期/已用一律 403 |
| GET | `/api/node/bundle/{token}` | `token`(路径) | 节点部署单元包（tar.gz，含集群凭据，凭 token 下载） |
| GET | `/api/node/image/{name}` | `name`(路径), `token`='' | 节点镜像 tar（引导脚本下载后 `docker load`；防路径穿越，须落在 `IMAGES_DIR`） |
| POST | `/api/nodes/{ip}/ready` | `ip`(路径), `token`, `status`='online', `error`=''（Form） | 节点部署回执（引导脚本最后一步调用）：成功即作废 token，置 `deploy_state=online` |

---

## 18. agent 指令通道（v2.5.0 F5 新增）

> 心跳方向 agent → manager，**不要求登录**，凭节点独立 token 认证。指令只允许白名单动词（agent 侧二次校验），通用 exec 默认关闭。

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/agent/heartbeat` | `ip`, `token`, `agent_version`='', `facts`=''（Form） | agent 心跳：上报存活并取回待执行指令；记录 `agent_last_seen`（无保护窗口提示用） |
| POST | `/api/agent/cmd_result` | `ip`, `token`, `id`=0, `ok`='0', `message`=''（Form） | agent 回报指令执行结果 |
| POST | `/api/agent/commands` | `ip`, `verb`, `payload`='{}', `ttl_sec`=3600, `dedupe_key`=''（Form） | 下发一条指令给节点（只入队，由心跳拉取执行）；`verb` 须在白名单（如 `update_files`/`restart_service`/`set_credential`/`run_builtin_script`/`stop_service`/`start_service`/`wipe_data`/`exec` 等） |
| GET | `/api/agent/commands` | `ip`='', `limit`=30 | 查询指令下发与执行情况（排障） |

---

## 19. 滚动升级（v2.5.0 F8 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/upgrade/start` | `cluster_id`, `actions`(JSON: `{ip:动作}`), `new_master`='', `do_switch`=1（Form） | 启动滚动升级。动作 ∈ `restart`(L1 重启)/`rebuild`(L2 重建复制)/`pure`(L3 纯净重建)；**L3 严禁对当前主库执行** |
| GET | `/api/upgrade/status` | `task`='' | 查询升级任务进度 |
| POST | `/api/upgrade/cancel` | `task`(Form) | 取消升级任务 |

---

## 20. 强制同步 syncgate（v2.5.0 F7 新增）

> 长任务绝不阻塞请求：立即返回 `task_id`，前端 1.5s 轮询。期间进入**锁写窗口**（主库置 `read_only`，并抑制 monitor 自动把主库设回可写）；超时/取消/宽限期到都**自动恢复主库写入**。

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/sync/force` | `cluster_id`, `timeout_sec`=120, `grace_sec`=300（Form） | 强制同步：停写 → 等从库追平 → 报告「可以安全切主」 |
| GET | `/api/sync/status` | `task`='' | 查询强制同步进度（`status`/`stage`/`progress`/`ready`/`locked`） |
| GET | `/api/sync/readiness` | `cluster_id`=0 | 只读判定：现在能不能安全切主（不动任何状态） |
| POST | `/api/sync/cancel` | `task`(Form) | 取消强制同步：立即恢复主库写入 |

---

## 21. 无保护窗口提示（v2.5.0 F11 新增）

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/system/protection` | — | 返回各节点最近心跳时间与管理器离线时长评估；管理器离线过久时 `manager_was_offline=true`，前端弹红色横幅（自动切换/自愈/告警/备份全部失效） |

---

## 22. 终端 / 日志 / 设置

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| POST | `/api/term` | `cluster_id`, `host`, `port`, `sql`（Form） | 在线 SQL 终端。优先用节点 root（可读写，支持全部 SQL）；未配置 root 密码则回退 `ha_monitor` 只读 |
| POST | `/api/term_complete` | `cluster_id`, `host`, `port`（Form） | Tab 补全候选：关键字 + 库 + 表 + 列 |
| GET | `/api/srv_log` | `svc`='ha_manager', `since`=86400, `offset`=0 | 读取管理端容器日志（经宿主 `docker.sock`）；其它容器按名读取兄弟容器日志 |
| GET | `/api/srv_units` | — | 动态列举宿主上可读取日志的容器（供下拉选择） |
| POST | `/api/setting_set` | `key`, `value`（Form） | 保存开关/配置项（**白名单校验**，见下） |
| POST | `/settings` | `smtp_host`, `smtp_port`, `smtp_user`, `smtp_pass`, `check_interval`, `manager_ip`（Form） | 保存 SMTP 与轮询间隔、管理器内网 IP |
| POST | `/settings/test_email` | — | 发送测试邮件（发往全部已配置邮箱） |
| POST | `/settings/email/add` | `email`(Form) | 新增收件邮箱 |
| POST | `/settings/email/delete` | `id`(Form) | 删除收件邮箱 |
| POST | `/settings/email/set_default` | `id`, `is_default`（Form） | 设为默认（接收全部集群） |
| POST | `/settings/email/toggle` | `id`, `enabled`（Form） | 启停该邮箱接收告警 |
| POST | `/settings/email/map` | `id`, `cluster_ids`（Form） | 绑定邮箱 ↔ 集群（`cluster_ids` 为逗号分隔） |

### `/api/setting_set` 白名单

不在白名单的 key 返回 `{"ok":false,"error":"不允许修改该项"}`。

```
AUTO_FAILOVER, AUTO_REPAIR,
ALERT_COOLDOWN_SEC, ALERT_MERGE, ALERT_LAG_THRESHOLD, ALERT_RECEIVERS,
SLOWLOG_ENABLED,
LAG_FAILOVER_ENABLED, LAG_FAILOVER_SEC, LAG_FAILOVER_CYCLES,
PRESSURE_FAILOVER_ENABLED, PRESSURE_THREADS_RUNNING, PRESSURE_ROW_LOCK_WAITS,
FAILOVER_COOLDOWN_SEC,
SKIP_DATA_CONFLICT, REQUIRE_WITNESS_FOR_FAILOVER,
AUTO_PURGE_BINLOG, BINLOG_SIZE_ALERT_MB, BINLOG_KEEP_HOURS, SEMI_SYNC,
PROVISION_WAIT_SEC, ALERT_GRACE_SEC, AUTO_REBUILD_ON_PURGE
```

> 高延迟 / 压力自动切换（v2.2.9+）、半同步 `SEMI_SYNC`、binlog 守卫（`AUTO_PURGE_BINLOG` 等）**默认关闭**，需显式开启。

---

## 23. 调用示例

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

# 下载节点凭证修复 SQL
curl -b ck.txt -o node213.sql \
  'http://127.0.0.1:28888/api/node_creds_sql?cluster_id=1&host=192.0.2.213&port=3306&download=1'

# 在线终端执行 SQL
curl -b ck.txt -X POST http://127.0.0.1:28888/api/term \
  -d 'cluster_id=1&host=192.0.2.213&port=3306&sql=SHOW REPLICA STATUS'

# 开启延迟自动切换
curl -b ck.txt -X POST http://127.0.0.1:28888/api/setting_set \
  -d 'key=LAG_FAILOVER_ENABLED&value=1'

# 节点引导命令（生成一次性 token）
curl -b ck.txt -X POST http://127.0.0.1:28888/api/node/bootstrap \
  -d 'ip=192.0.2.213&ttl_hours=24'

# 集群级定时备份策略
curl -b ck.txt -X POST http://127.0.0.1:28888/api/backup/policy \
  -d 'cluster_id=1&enabled=1&interval_hours=24&retention_days=7&retention_count=7&target=slave&disk_alert_gb=50'

# 强制同步（锁写窗口）
curl -b ck.txt -X POST http://127.0.0.1:28888/api/sync/force \
  -d 'cluster_id=1&timeout_sec=120&grace_sec=300'
```

---

## 24. 错误处理与排错

| 场景 | 表现 | 处理 |
|------|------|------|
| 业务校验失败 | HTTP 200 + `{"ok":false,"error":"..."}` | 读 `error` 字段 |
| 未登录 | 页面 → 302 登录页；API → 403 | 重新 `POST /login` |
| 只读账号写操作 | HTTP 403 | 改用 admin 账号（见「系统设置 → 用户与角色」） |
| 非白名单配置项 | `{"ok":false,"error":"不允许修改该项"}` | 仅可用白名单 key |
| 节点不可达 | `hosts.status=down`，`err` 带真实原因（账号错误 / 网络不可达） | 「修正节点」更新凭证或排查网络 |
| datetime 序列化 | 由 `_json_ok` 统一转 ISO 字符串 | 避免裸 500 |

> 前端 `dashboard.html` 通过 `data-f` 增量打补丁（`patchField`），仅更新变化单元格，稳态下不整表重绘。

---

## 25. 与 v2.2.20 版本文档的差异（重要）

v2.5.1 端点总数由 v2.2.20 的 **63** 增至 **140**。v2.2.20 文档列过、当前代码**已不存在**的端点（请勿再依赖）依旧不存在：

```
/api/summary, /api/topology, /api/cluster_health, /api/failover_status, /api/overview,
/api/cluster_meta, /api/cluster_nodes, /failover, /api/force,
GET /api/alert_rules, /api/alert_rule, /api/alert_emails, /api/alert_email,
/api/alert_email_map, /api/deploy_commands, /api/email/test,
GET /clusters/{cid}/delete, GET /nodes/{ip}/delete
```

**v2.5.0 / v2.5.1 相比 v2.2.20 新增的端点（约 77 个）**，按域归类：

- **状态/指标**：`/api/selfheal/stats`、`/api/os_metrics`、`/api/collect/state`
- **一致性校验**：`/api/consistency/check`(POST+GET)、`/api/consistency/last`、`/api/consistency/row_diff`、`/api/consistency/export`、`/api/consistency/remaster`
- **节点重建**：`/api/node/rebuild`、`/api/node/rebuild_status`
- **节点管理增强**：`/api/node_error_log`(新增 `segment` 启动段)、`/api/nodes/{ip}/pending`、`/nodes/{ip}/ack_deploy`、`/api/config_drift/remediate`
- **备份增强**：`/api/backup/history`、`/api/backup/log`、`/api/restore/history`、`/api/backup/restore_progress`、定时策略 `/api/backup/policy`(GET+POST)
- **引导安装 bootstrap**：`/api/node/bootstrap`(POST)、`/api/node/bootstrap/{token}`、`/api/node/bundle/{token}`、`/api/node/image/{name}`、`/api/nodes/{ip}/ready`
- **agent 指令通道**：`/api/agent/heartbeat`、`/api/agent/cmd_result`、`/api/agent/commands`(POST+GET)
- **强制同步 syncgate**：`/api/sync/force`、`/api/sync/status`、`/api/sync/readiness`、`/api/sync/cancel`
- **免 SSH 改密/凭证**：`/api/passwords/change`、`/api/passwords/accounts`、`/api/cred_status`、`/api/cred_encryption`、`/api/cred_recover`、`/api/cred_reset_repl`
- **用户与角色**：`/api/users`、`/api/users/add`、`/api/users/role`、`/api/users/delete`
- **维护窗口**：`/api/maintenance`、`/api/maintenance/begin`、`/api/maintenance/end`
- **数据迁移向导**：`/migration`(页)、`/migration/history`(页) + 13 个 `/api/migration/*`
- **集群接管 adopt**：`/adopt`(页) + 8 个 `/api/adopt/*`
- **滚动升级**：`/clusters/{cid}/upgrade`(页) + 3 个 `/api/upgrade/*`
- **无保护窗口提示**：`/api/system/protection`
