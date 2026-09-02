# Custos HA · API 接口文档

> 适用版本：**v2.2.15**（与 `app/VERSION` 保持一致）
> 基址：管理端地址（默认 `http://<manager>:28888`）
> 本文档由 `app/api.py` 实际路由导出（共 **63** 个端点），与代码强一致。

## 通用约定

| 项 | 说明 |
|------|------|
| 鉴权 | 除 `POST /login` 外全部需登录态：`Cookie: session=<id>`（由 `POST /login` 写入）。未登录访问页面路由 → 重定向登录页；访问 API → 403 / `{"ok":false}` |
| 参数绑定 | `Form` = 表单字段（`application/x-www-form-urlencoded`）；`=默认值` = 查询参数（Query，可省略）；`必填` = 无默认值、必须传 |
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
| GET | `/settings` | `msg`, `error` | 系统设置：SMTP 与多邮箱告警 |
| GET | `/audit` | `limit`=200 | 操作审计页 |
| GET | `/nodes/{ip}/creds_sql` | `ip`(路径) | 下载该节点 `fix-node-creds.sql`（auth） |

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
| POST | `/api/node_error_log` | `cluster_id`, `host`, `port`, `limit`（Form） | 读取节点 MySQL 错误日志（`LOAD_FILE` 尽力而为） |
| POST | `/api/fix_node` | `cluster_id`, `host`, `port`（Form） | 一键自愈节点复制参数（`SET PERSIST` 写 `mysqld-auto.cnf`，重启仍生效） |
| POST | `/api/precheck` | `cluster_id`, `host`, `port`（Form） | 节点/集群预检（仅告警）；不传 host/port 则整集群 |
| POST | `/api/collect` | `target`, `cluster_id`（Form） | 手动触发采集，`target` ∈ `metrics`/`slow`/`drift`/`all`。**后台线程非阻塞**，请求立即返回 accepted |

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
| GET | `/api/backup/list` | `cluster_id`=0 | 该集群历史备份列表 |
| POST | `/api/backup/restore` | `cluster_id`, `host`, `port`, `file`, `databases`（Form） | 恢复备份到指定节点（默认恢复全部库，可指定部分） |
| POST | `/api/backup/delete` | `cluster_id`, `file`（Form） | 删除备份文件 |
| GET | `/api/backup/preview` | `cluster_id`=0, `file`='' | 备份内容预览 |
| GET | `/api/backup/download` | `cluster_id`=0, `file`='' | 下载备份文件 |

> 备份产物落盘 `./backups/<集群>/<库>_<YYYYmmdd-HHMMSS>.sql.gz`；由 `app/backup.py` 纯 Python 实现（镜像内无 `mysqldump`）。

---

## 8. 告警中心

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/active_alerts` | — | 聚合当前未恢复告警：探活失败(`hosts.err`)、配置漂移(`config_drift`)、复制延迟超阈值 |
| GET | `/api/alerts` | `level`='', `status`='', `cluster_id`=0, `hours`=0, `q`='', `page`=1, `page_size`=20 | 告警中心分页查询（按级别/状态/集群/时间/关键字筛选） |
| POST | `/api/alert_ack` | `id` / `ids` / `all`（见实现） | 确认（消音）告警：单条、批量或全部 |
| POST | `/api/alert_rules` | `cooldown`, `merge`, `lag_threshold`, `receivers`（Form） | 保存告警规则（冷却/合并/延迟阈值/额外收件人） |

> 可自愈告警自动标记 `healed`，未恢复标记 `unrecovered`。

---

## 9. 切换历史 / 审计 / 慢日志 / 配置漂移

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/switch_history` | `cluster_id`=0, `hours`=0, `page`=1, `page_size`=20 | 切换历史（分页） |
| GET | `/api/audit` | `limit`=200, `cluster_id`=0 | 操作审计日志 |
| GET | `/api/slow_log` | `cluster_id`=0, `host`='', `port`=0, `limit`=200, `min_ms`=0 | 慢日志明细（按耗时倒序） |
| GET | `/api/config_drift` | `cluster_id`=0 | 最新一次配置一致性巡检结果 |

---

## 10. 终端 / 日志 / 设置

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

### `/api/setting_set` 白名单

不在白名单的 key 返回 `{"ok":false,"error":"不允许修改该项"}`。

```
AUTO_FAILOVER, AUTO_REPAIR,
ALERT_COOLDOWN_SEC, ALERT_MERGE, ALERT_LAG_THRESHOLD, ALERT_RECEIVERS,
SLOWLOG_ENABLED,
LAG_FAILOVER_ENABLED, LAG_FAILOVER_SEC, LAG_FAILOVER_CYCLES,
PRESSURE_FAILOVER_ENABLED, PRESSURE_THREADS_RUNNING, PRESSURE_ROW_LOCK_WAITS,
FAILOVER_COOLDOWN_SEC
```

> 高延迟 / 压力自动切换（v2.2.9+）**默认关闭**，需显式开启。

---

## 11. 调用示例

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
  -d 'cluster_id=4&host=192.168.1.213&port=3306'

# 复制自愈
curl -b ck.txt -X POST http://127.0.0.1:28888/api/heal \
  -d 'cluster_id=4&host=192.168.1.213&port=3306'

# 下载节点凭证修复 SQL
curl -b ck.txt -o node213.sql \
  'http://127.0.0.1:28888/api/node_creds_sql?cluster_id=4&host=192.168.1.213&port=3306&download=1'

# 在线终端执行 SQL
curl -b ck.txt -X POST http://127.0.0.1:28888/api/term \
  -d 'cluster_id=4&host=192.168.1.213&port=3306&sql=SHOW REPLICA STATUS'

# 开启延迟自动切换
curl -b ck.txt -X POST http://127.0.0.1:28888/api/setting_set \
  -d 'key=LAG_FAILOVER_ENABLED&value=1'
```

---

## 12. 错误处理与排错

| 场景 | 表现 | 处理 |
|------|------|------|
| 业务校验失败 | HTTP 200 + `{"ok":false,"error":"..."}` | 读 `error` 字段 |
| 未登录 | 页面 → 302 登录页；API → 403 | 重新 `POST /login` |
| 非白名单配置项 | `{"ok":false,"error":"不允许修改该项"}` | 仅可用白名单 key |
| 节点不可达 | `hosts.status=down`，`err` 带真实原因（账号错误 / 网络不可达） | 「修正节点」更新凭证或排查网络 |
| datetime 序列化 | 由 `_json_ok` 统一转 ISO 字符串 | 避免裸 500 |

> 前端 `dashboard.html` 通过 `data-f` 增量打补丁（`patchField`），仅更新变化单元格，稳态下不整表重绘。

---

## 13. 与 v2.2.11 版文档的差异（重要）

旧版文档列出的以下端点**在当前代码中已不存在**，请勿再依赖：

```
/api/summary, /api/topology, /api/cluster_health, /api/failover_status, /api/overview,
/api/cluster_meta, /api/cluster_nodes, /failover, /api/force,
GET /api/alert_rules, /api/alert_rule, /api/alert_emails, /api/alert_email,
/api/alert_email_map, /api/deploy_commands, /api/email/test,
GET /clusters/{cid}/delete, GET /nodes/{ip}/delete
```

**方法 / 路径变更**（旧文档标注错误，以本版为准）：

| 端点 | 旧文档 | 实际 |
|------|------|------|
| `/api/term` | GET | **POST** |
| `/api/node_detail` | GET | **POST** |
| `/api/node_error_log` | GET | **POST** |
| `/api/precheck` | GET | **POST** |
| `/api/collect` | GET | **POST** |
| 删除节点 | POST `/nodes/<ip>/delete` | **POST `/nodes/delete`**（`ip` 走表单） |
| 邮箱管理 | `/api/alert_email*` | **`/settings/email/{add,delete,set_default,toggle,map}`** |
| 测试邮件 | `/api/email/test` | **`/settings/test_email`** |
| 告警规则 | GET `/api/alert_rules` + POST `/api/alert_rule` | **POST `/api/alert_rules`** |

**新增端点**（v2.2.11 之后引入）：`/api/status`、`/api/refresh`、`/api/vip_status`、`/api/node_detail`、`/api/backup/*`、`/api/term_complete`、`/audit`、`/settings/email/*`。
