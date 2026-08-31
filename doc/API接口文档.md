# Custos HA · API 接口文档

> 适用版本：**v2.2.11**
> 所有接口基址为管理端地址（默认 `http://<manager>:28888`）。
> 鉴权：除登录/静态资源外，均需在请求头带 `Cookie: session=<id>`（由 `/login` 登录后获得）。文档中标注 `auth` 的接口均需登录。
> 统一返回：成功 `{"ok": true, ...}`，失败 `{"ok": false, "error": "..."}`（部分接口用 `JSONResponse`）。

---

## 1. 认证

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/login` | — | 返回登录页 |
| POST | `/login` | `username`, `password`（表单） | 校验账密，成功写 session |
| GET | `/logout` | — | 清除 session 并跳转登录页 |
| GET | `/change_password` | — | 返回修改密码页（auth） |
| POST | `/change_password` | `old_password`, `new_password`（JSON） | 修改当前用户密码（auth） |

---

## 2. 概览 / 仪表盘 / 指标

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/` | — | 仪表盘（多集群健康卡片 + KPI + 最近告警 + 切换历史） |
| GET | `/api/summary` | — | 返回各集群健康概览（集群列表、节点状态、VIP、当前主、KPI 计数）（auth） |
| GET | `/api/topology` | — | 各集群节点拓扑与角色/复制状态（auth） |
| GET | `/api/activity_feed` | — | 仪表盘「最近活动」流（auth） |
| GET | `/api/metric_history` | `cluster_id`, `host`, `port`, `range`(h) | 指标趋势（延迟/连接/线程）（auth） |
| GET | `/api/cluster_health` | `cluster_id` | 单集群健康状况（auth） |
| GET | `/api/failover_status` | `cluster_id` | 当前是否处于手动/自动切换冷却或过程中（auth） |
| GET | `/api/overview` | — | 管理端自身信息（版本、管理库状态等）（auth） |

---

## 3. 集群管理

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/clusters` | — | 集群管理页（列出全部集群） |
| POST | `/clusters` | `name`, `ha_user`, `ha_pass`, `repl_user`, `repl_pass`, `app_user`, `app_pass`, `vip`, `vip_prefix`, `interface`, `vrid`, `vip_port`, `node_type`, `mysql_version` | 创建集群（auth） |
| PUT | `/clusters/<cid>` | 同上（部分字段） | 修改集群配置（auth） |
| GET | `/clusters/<cid>/delete` | — | 删除集群确认页 |
| POST | `/clusters/<cid>/delete` | — | 删除集群（auth） |
| POST | `/clusters/<cid>/sync` | — | 拉取集群最新状态/拓扑（auth） |
| GET | `/api/cluster_meta` | `cluster_id` | 集群元信息（auth） |
| GET | `/api/cluster_nodes` | `cluster_id` | 集群节点列表（auth） |
| GET | `/api/vip_status` | `cluster_id` | VIP 是否在线 / 当前归属（auth） |

---

## 4. 节点管理

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/nodes` | — | 节点管理页（全部节点配置/状态） |
| POST | `/nodes/add` | `cluster_id`, `ip`, `port`, `root_user`, `root_pass`, `server_id`, `node_type`, `mysql_version` | 新增节点（做同构校验、拦截管理库地址）（auth） |
| GET | `/nodes/<ip>/delete` | — | 删除确认页 |
| POST | `/nodes/<ip>/delete` | — | 删除节点（auth） |
| GET | `/nodes/<ip>/regen` | — | 重新生成节点自包含目录（auth） |
| GET | `/nodes/<ip>/creds_sql` | — | **查看/下载节点凭证 SQL**（根因：路由装饰器曾误挂辅助函数导致 422，已修复） |
| GET | `/api/node_creds_sql` | `cluster_id`, `host`, `port`, `download`(0/1) | 同上，JSON 或文件下载（auth） |
| GET | `/api/node_detail` | `cluster_id`, `host`, `port` | 节点详情（变量/状态/复制）（auth） |
| GET | `/api/node_error_log` | `cluster_id`, `host`, `port`, `lines` | 节点错误日志（auth） |
| GET | `/api/node_resources` | `cluster_id`, `host`, `port` | 节点 OS 资源（CPU/内存/磁盘）（auth） |
| POST | `/api/fix_node` | `cluster_id`, `host`, `port`, `root_user`, `root_pass` | 修正节点 root 凭证/参数（auth） |
| POST | `/api/precheck` | `cluster_id`, `host`, `port` | 节点连通性/账号预检（auth） |
| POST | `/api/collect` | `cluster_id`, `host`, `port` | 手动触发一次指标采集（auth） |
| GET | `/api/deploy_commands` | `cluster_id`, `host`, `port` | 返回节点部署/启动命令（auth） |

---

## 5. 故障切换 / 复制修复

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/failover` | `cluster_id` | 故障切换页 |
| POST | `/api/failover` | `cluster_id`, `auto`(0/1) | 触发自动切换流程（auth） |
| POST | `/api/failover/to` | `host`, `port`, `cluster_id` | 手动指定提升某节点为主（auth） |
| POST | `/api/heal` | `cluster_id`, `host`, `port`, `io`(0/1), `sql`(0/1) | 修复复制中断（重建复制通道/GTID 自愈）（auth） |
| POST | `/api/force` | `cluster_id`, `host`, `port` | VIP/角色强制校正（auth） |

---

## 6. 备份与恢复

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/clusters/<cid>/backup` | — | 备份恢复页 |
| GET | `/api/backup/databases` | `cluster_id`, `host`, `port` | 列出可备份库（auth） |
| POST | `/api/backup/run` | `cluster_id`, `host`, `port`, `databases`(逗号) | 触发备份（auth） |
| GET | `/api/backup/list` | `cluster_id` | 备份文件列表（auth） |
| POST | `/api/backup/restore` | `cluster_id`, `host`, `port`, `file`, `databases` | 恢复备份（auth） |
| POST | `/api/backup/delete` | `cluster_id`, `file` | 删除备份（auth） |
| GET | `/api/backup/preview` | `cluster_id`, `file` | 备份内容预览（auth） |
| GET | `/api/backup/download` | `cluster_id`, `file` | 下载备份文件（auth） |

---

## 7. 告警中心

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/alerts` | — | 告警中心页 |
| GET | `/api/active_alerts` | `cluster_id` | 当前活跃告警（auth） |
| GET | `/api/alert_rules` | — | 告警规则/阈值配置（auth） |
| POST | `/api/alert_rule` | 规则字段 | 新增/修改告警规则（auth） |
| GET | `/api/alerts` | `cluster_id`, `status`, `page`, `size` | 分页查询告警/自愈记录（auth） |
| POST | `/api/alert_ack` | `id` | 确认/消音告警（auth） |
| GET | `/api/alert_emails` | — | 告警邮箱列表（auth） |
| POST | `/api/alert_email` | `email`, `is_default`, `enabled` | 新增/修改邮箱（auth） |
| POST | `/api/alert_email_map` | `email_id`, `cluster_id` | 绑定邮箱↔集群（auth） |

---

## 8. 切换历史 / 审计 / 慢日志 / 配置漂移

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/switch_history` | `cluster_id`, `limit` | 切换历史（auth） |
| GET | `/api/audit` | `cluster_id`, `page`, `size`, `action` | 操作审计日志（auth） |
| GET | `/api/slow_log` | `cluster_id`, `host`, `port`, `limit` | 慢日志（auth） |
| GET | `/api/config_drift` | `cluster_id` | 配置一致性巡检结果（auth） |

---

## 9. 终端 / 日志 / 设置

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/api/term` | `cluster_id`, `host`, `port`, `user`, `sql` | 在线 SQL 终端（带凭证回退）（auth） |
| POST | `/api/term_complete` | `cluster_id`, `host`, `port`(表单) | Tab 补全候选（关键字/库/表/列）（auth） |
| GET | `/api/srv_log` | `unit` | 读取管理器自身或某容器日志（auth） |
| GET | `/api/srv_units` | — | 列出宿主上可读取日志的容器（供下拉动态选择）（auth） |
| GET | `/settings` | — | 系统设置页 |
| POST | `/api/setting_set` | `key`, `value` | 修改某项配置开关（auth） |
| POST | `/api/email/test` | `email` | 发送测试告警邮件（auth） |

---

## 10. 调用示例

**登录**
```bash
curl -c ck.txt -X POST http://127.0.0.1:28888/login \
  -d 'username=admin&password=admin'
```

**查看某节点凭证 SQL（下载）**
```bash
curl -b ck.txt -o node213.sql \
  'http://127.0.0.1:28888/api/node_creds_sql?cluster_id=4&host=192.168.1.213&port=3306&download=1'
```

**手动触发复制修复**
```bash
curl -b ck.txt -X POST http://127.0.0.1:28888/api/heal \
  -H 'Content-Type: application/json' \
  -d '{"cluster_id":4,"host":"192.168.1.213","port":3306,"io":1,"sql":1}'
```

**在线终端执行 SQL**
```bash
curl -b ck.txt 'http://127.0.0.1:28888/api/term?cluster_id=4&host=192.168.1.213&port=3306&user=root&sql=SHOW%20REPLICAS'
```

---

## 11. 错误码约定

- 业务校验失败：HTTP 200 + `{"ok":false,"error":"..."}`
- 未登录/无权限：重定向登录页或 403
- 参数缺失：对应接口返回 `error` 字段说明（如「端口无效」）
- 节点不可达：`hosts.status=down`，`err` 字段带回真实失败原因（账号错误/网络不可达等）

> 注：前端 `dashboard.html` 通过 `data-f` 增量打补丁（`patchField`），仅更新变化单元格，稳态下不整表重绘。
