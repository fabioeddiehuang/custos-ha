# Custos · 发布说明与路线图（RELEASE NOTES）

> 当前版本：**v2.2.2**（镜像标签 `custos:v2.2.2`）
> 设计目标（v3 高保真稿）：`design/mockup_v3.html`
> 配套文档：`README.md`

---

## 〇、v2.2.0 项目更名（Custos）

- **产品正式更名为 Custos**（拉丁语「守护者」），tagline：**Never let your database down**。
- 镜像 / 容器 / Dockerfile 同步更名：
  - 镜像标签：`mysql-ha-manager:v2.1.x` → `custos:v2.2.0`
  - 容器名（compose `container_name`）：固定为 `custos`（不再随仓库目录名派生为 `mysql-ha-manager-ha-manager-1`）
  - compose 服务名：`ha-manager` → `custos`
  - 管理器日志文件：`ha-manager.log` / `ha-manager-console.log` → `custos.log` / `custos-console.log`
  - 节点部署目录默认前缀：`mysql-ha-manager/generated_nodes` → `custos-ha/generated_nodes`
- 代码 / 文档 / 页面品牌（标题、导航、登录页、告警 / 节点 / 设置等）统一为 Custos。

---

## 〇之二、v2.2.1 仓库目录改名（custos-ha）

- **仓库根目录 `mysql-ha-manager/` → `custos-ha/`**，产品名、镜像名、容器名、目录名四者统一。
- `NODES_HOST_BASE` 默认值 `custos/generated_nodes` → `custos-ha/generated_nodes`
  （Web「节点管理」展示的宿主机部署路径随之更新；可用环境变量 `NODES_HOST_BASE` 覆盖）。
- Web 展示剥离目录前缀时新增 `custos-ha/` 兼容项，并修正匹配顺序（`custos-ha/` 必须
  先于 `custos/` 匹配，否则会被截断成 `-ha/generated_nodes/<ip>`）。
- `.env` 增加 `COMPOSE_PROJECT_NAME=custos`：compose 项目名不再随目录名变化，
  后续再改名 / 迁移目录不会导致容器「孤儿」或重名冲突。
- **修复**：`init_schema()` 每次启动都对所有节点无条件 `UPDATE nodes SET compose_path=…`，
  造成无谓写库（节点多时拖慢启动）。改为仅更新「当前值与目标值不一致」的行。
- 新增 `LICENSE`（MIT，版权 eddys.cn）。

---

## 〇之三、v2.2.2 全站深色主题（对齐登录页）+ 首页全屏 + 空闲零写入

### UI / 品牌

1. **Logo 重设计**：新增内联 SVG「盾牌 + 数据库盘面」（盾=守护者 Custos，盘=数据库），
   青色渐变 `#22d3ee → #0ABAB5`，深墨 `#04121a` 描线。以 `<symbol id="cs-logo">` + `<use>` 复用，
   登录页与内页（侧栏品牌位、首页标题）同一枚，不再用 `◈` 字形。
2. **首页标题重排**：`Custos — Never let your database down`
   - `Custos` 30px/800，青→蒂凡尼渐变文字（`-webkit-background-clip:text`），字距 -.02em
   - 破折号用紫酱红 `#7C2D52` 提亮（深色底上纯 #7C2D52 太暗，已 `filter:brightness(1.9)`）
   - tagline 14.5px/500、字距 .06em、斜体、muted 色
3. **首页全屏**：`.content` 新增 `.full-bleed`（仅 `/` 生效，由 base.html 按 path 添加）：
   去掉 `max-width:1480px` 与容器内边距；≥1700px 时集群卡片区两列。
4. **内页主题色全面对齐登录页**：`v3.css` / `styles.css` `:root` 改为登录页同一套深色变量
   （底 `#070b14→#0e1830` 渐变、主色蒂凡尼青 `#0ABAB5`、高亮 `#22d3ee`、点缀紫酱红 `#7C2D52`）。
   - 主按钮 = 登录页 `.submit` 同款青色渐变 + 深墨文字 `#04121a`
   - 侧栏 / 顶栏改为半透明深色玻璃；导航选中项用同款渐变
   - 集群 6 套色板的 `--cDeep/--cText` 改为深底可读的亮色档
   - 各页面 `<style>` 与内联样式里的浅色硬编码值统一替换（含 SVG `fill`/`stroke`——
     **注意：SVG 呈现属性不支持 `var()`，必须用字面色值**）
   - 终端 / 日志块底色 `#0f172a` → `#060b16`（比卡片更深，保持层次）

5. **修复登录页「用户名 / 密码」被挤成竖排换行**：
   根因是 `v3.css` 的 `.field{display:flex;justify-content:space-between}`（那是设置页「标签左/控件右」的行式布局）
   在登录页生效了——登录页 `.field{margin-bottom:15px}` 没覆盖 `display`。于是 label 被挤成极窄一列，
   「用户名」「密码」逐字竖向换行。修复：`.auth-main .field{display:block;padding:0;border-bottom:0}`。

### 空闲零写入（管理库 / 节点 MySQL）

排查出 4 处「稳态下仍持续写」的点，全部改为**变更前检测**：

| 位置 | 原行为 | 修复 |
|---|---|---|
| `monitor._update_host` | 每周期对每个节点无条件 `UPDATE hosts ... last_seen=NOW()` | 与库中当前值逐字段比对，全同则不写；`last_seen` 按 `HEARTBEAT_INTERVAL`(默认 30s) 限频，设 0 = 完全不写 |
| `ha_client.promote_to_master` | 每周期对已是主库的节点仍执行 `STOP REPLICA` / `RESET REPLICA ALL` / **`FLUSH BINARY LOGS`** | 先只读快检 `@@GLOBAL.read_only` + 是否有复制通道，已是「可写且无残留通道」则直接返回，零写操作。**这是节点侧空闲写盘的主因**——`FLUSH BINARY LOGS` 每 ~2s 新建并 fsync 一个 binlog 文件（节点 `sync_binlog=1`） |
| `failover.ensure_topology` 收尾 | 每周期无条件 `UPDATE hosts SET is_current_master=0` + 再置 1（主库行 1→0→1 两次真实行更新） | 先比对，仅不一致时才写 |
| `metrics.collect_metrics` / `config_drift.check_cluster` | 每 9s / 60s 无条件 INSERT 快照（连 `status='OK'` 也插） | 与上次快照签名比对，完全一致则跳过；采集改为按秒限频（`METRICS_INTERVAL` 60s / `DRIFT_INTERVAL` 300s，不再随 `CHECK_INTERVAL` 调小而放大）；保留期清理限频 1h 一次 |

- `db.py` 新增迁移：为 `metric_history` / `config_drift` 补 `idx_ts(ts)` 单列索引
  （清理按 `ts` 删除，原索引最左前缀都不是 `ts`，导致每次全表扫描 → buffer pool 脏页 → checkpoint 写盘）。
- 新增环境变量：`HEARTBEAT_INTERVAL`(30) / `METRICS_INTERVAL`(60) / `DRIFT_INTERVAL`(300)。

### 顺带修掉的 3 个高频刷屏 / 写盘 Bug

排查时发现「日志每 2 秒刷一屏」，顺着挖出 3 个真 Bug（都会造成节点侧持续写盘）：

1. **`push_current_master_to_nodes` 每周期写所有节点**
   `ensure_topology` 每 ~2s 无条件向集群内**每个节点**写一次 `ha_local.state`
   （keepalived 检查脚本据本机该表判断 VIP 归属）。稳态下就是每 2 秒对每个节点
   做一次真实行更新。改为：值未变化不写，最长 `PUSH_MAX_INTERVAL`(300s) 兜底推一次
   （防节点重启丢失本地状态）。
2. **拓扑修复无限重试**：某从库因环境原因始终「看起来没指向当前主」时，
   每周期都会 `STOP REPLICA → RESET REPLICA ALL → CHANGE MASTER → START REPLICA`
   重挂一次复制。新增 `TOPO_REPAIR_INTERVAL`(60s) 限频；真实切换走 `do_failover`
   时会调用 `reset_topo_throttle()` 主动解限，不影响切换速度。
3. **参数自愈每周期重跑 + 刷同一条警告**：`_selfheal_cluster` 每周期对每个节点调
   `fix_node_mysql_params`，失败时每 2s 打一行 `server_id 设置失败: 1227`。
   新增 `PARAM_SELFHEAL_INTERVAL`(300s) 限频。
   顺带修正：账号自愈的时间戳原本在循环内更新，导致每轮只有**第一个**节点被校正
   （日志表现为「凭证已校正到 1 个节点」），改为整轮结束后统一更新。

### VIP 状态写入链路的 2 个真 Bug（`ha_local.state` 永远为空）

- **`set_local_master_flag` 被 CREATE 权限拖死**：函数里 `CREATE DATABASE IF NOT EXISTS /
  CREATE TABLE IF NOT EXISTS` 与业务写入在同一个 try 中，而 MySQL 对 `IF NOT EXISTS`
  也会先校验权限。在「表已存在但 ha_monitor 无 CREATE」的节点上必然抛 1044，
  被 `except: return False` 吞掉 → 状态写入静默失败。改为两句建表单独容错。
- **ha_monitor 根本没被授予 `ha_local` 的写权限**：MySQL 8.0 分支的 `ha_grant`
  只有 `REPLICATION SLAVE/CLIENT, REPLICATION_SLAVE_ADMIN, SYSTEM_VARIABLES_ADMIN,
  PROCESS, SELECT, RELOAD, SUPER, DELETE`，**没有 INSERT**（MariaDB 分支却有 CREATE/INSERT/UPDATE/DELETE）。
  于是 `REPLACE INTO ha_local.state` 报 1142。
  新增 `_ensure_local_state_grant()`：按最小权限只授 `SELECT,INSERT,UPDATE,DELETE,CREATE ON ha_local.*`
  （不放大 `*.*`），在 `ensure_ha_creds` 中执行；若当时走的是 ha_monitor（无 GRANT OPTION）
  则再用 root 补授一次。

> **验证**：连续 40s 观察，管理器日志**零条非 Web 轮询输出**（此前每 2s 数十行）。

> **遗留（需到节点本机处理，非代码问题）**：三个测试节点的 `root@'192.168.1.66'`
> 权限被限制（缺 RELOAD / 对 `mysql.user` 无 SELECT），`.213` 的 ha_monitor 还有 1045 密码漂移，
> 因此上述授权无法由管理器远程自愈，需在各节点本机以本地 root 执行
> `generated_nodes/<ip>/fix-node-creds.sql`。此外 `current_master` 记录的是 `192.168.1.203`，
> 而节点实际可写主库是 `192.168.1.213`（两者不一致），授权修好后建议重新指定主库或触发一次切换。

---

## 一、v2.1.0 已实现并部署功能

### 仪表盘（Dashboard）
- 集群健康卡片：VIP 状态、复制延迟 / IO / SQL 线程、主库标记、延迟超阈 / 漂移告警徽标。
- 趋势图：连接数 / QPS / 复制延迟（24h）。
- 切换记录时间线 + 历史 Tab。
- **主库状态 gauge（新增）**：连接数% / InnoDB 缓冲池% / 数据量 / QPS（基于 MySQL 可观测指标）。
- **实时活动 feed（新增）**：聚合故障切换（`switch_history`）+ 操作审计（`audit_log`），15s 刷新。

### 集群管理（Clusters）
- 集群卡片：拓扑、节点列表、手动切换、一键修复、自动开关。
- 节点详情抽屉：版本 / 参数 / 账号凭证、**查看 / 下载自愈 SQL（新增）**。
- 24h 日志 Tab、终端抽屉。

### 节点管理（Nodes）
- 节点单元生成、下载安装包、远程 root 管理、修复 SQL 下载。

### 告警（Alerts）
- SMTP、告警规则、收件人集群绑定。
- **当前告警「已自愈 / 未恢复」状态标记（新增）**：自愈成功后持久化标记，列表区分「已自愈」(绿) 与「未恢复」(红)。

### 系统设置（Settings）
- SMTP、服务端容器日志。
- **管理器配置（新增）**：监控轮询间隔（秒，动态生效）、管理器内网 IP。

### 运维工具
- 健康预检（Precheck）、配置漂移巡检、慢日志 Top、操作审计日志、节点终端。

---

## 二、v3 路线图（对照 `design/mockup_v3.html`）

| 模块 | 状态 | 说明 |
|------|------|------|
| 仪表盘总览 | ✅ 已落地 | 集群卡片 / 趋势 / 切换时间线 |
| 主库状态 gauge | ✅ 已落地 | MySQL 可观测指标近似（见已知限制） |
| 实时活动 feed | ✅ 已落地 | 切换 + 审计聚合 |
| 集群拓扑管理 | ✅ 已落地 | 切换 / 修复 / 终端 / 日志 |
| 节点详情 / 自愈 SQL | ✅ 已落地 | 查看 + 下载 |
| 告警 + 自愈状态 | ✅ 已落地 | 已自愈 / 未恢复标记 |
| 系统设置（轮询 / 内网 IP） | ✅ 已落地 | 动态生效 |
| 账号策略改造（去 %） | ⏳ 规划中 | 节点段% + 172.28% + 127.0.0.1，待统一安全整改（见 F 清单） |
| 多活 / 异地容灾 | 📋 规划中 | 超出单机 HA 范畴 |
| 节点 OS 级指标（CPU / 内存 / 磁盘） | ⚠️ 受限 | 远程节点无 SSH / agent，暂以 MySQL 指标近似 |

---

## 三、已知限制与说明

1. **主库状态 gauge 的指标来源**：远程节点无 SSH / agent，无法获取 OS 级 CPU / 内存 / 磁盘使用率。故 dashboard 的「主库状态」采用 MySQL 可观测指标近似——连接数%（`Threads_connected` / `max_connections`）、InnoDB 缓冲池使用%（`Innodb_buffer_pool_bytes_data` / `size`）、数据量（`information_schema` 字节数）、QPS（`Queries` / `Uptime`）。磁盘项为数据库数据量绝对值，非操作系统磁盘占用率。
2. **告警「已自愈」语义**：自愈（手动 / 监控自动）成功后写入 `alerts` 状态表，对该节点（`cluster_id+host+port`）的实时聚合告警叠加「已自愈」标记；自愈失败则保持「未恢复」。监控自动自愈的去重窗口为 10 分钟。
3. **账号策略**：当前账号 host 仍含 `%`（历史实现）。去 `%` 改造（节点网段% + docker 网关 172.28% + 127.0.0.1）为结构性重构，风险评估见项目 F 清单，暂缓至统一安全整改阶段。
4. **部署**：仅用 `docker compose`；改 `./app` 后须 `docker compose up -d --build custos` 重建。

---

## 四、版本历史

- **v2.1.0**：补齐 v3 高保真稿剩余模块——主库状态 gauge、实时活动 feed、系统设置（轮询间隔 / 内网 IP）、告警已自愈 / 未恢复标记、集群节点自愈 SQL 查看 / 下载。新增 `alerts`、`audit_log` 持久化与 `/api/node_resources`、`/api/activity_feed`、`/api/node_creds_sql` 等端点。
- **v2.0.x**：基础 HA 管理能力（监控 / 自愈 / 切换 / VIP / 告警 / 预检 / 慢日志 / 配置漂移 / 审计）。
