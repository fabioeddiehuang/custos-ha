# Custos · 发布说明与路线图（RELEASE NOTES）

> 当前版本：**v2.4.0**（镜像标签 `custos-server:v2.4.0`）
> 设计目标（v3 高保真稿）：`design/mockup_v3.html`
> 配套文档：`README.md`、`doc/用户操作手册.md`、`doc/API接口文档.md`、`doc/数据库字段说明.md`、`doc/业务流程图.xmind`

---

## 版本功能索引（按版本号看「增加了什么」）

| 版本 | 类型 | 本期新增的主要功能 |
|------|------|-------------------|
| v2.1.0 | 功能 | 仪表盘主库 gauge / 实时活动 feed / 系统设置（轮询·内网IP）/ 告警「已自愈·未恢复」标记 / 节点自愈 SQL 查看下载 |
| v2.2.0 | 更名 | 产品更名 Custos；仓库目录 `custos-ha`；镜像/容器 `custos-server` |
| v2.2.11 | 功能 | 节点形态扩展到三种（新增 Docker MariaDB 12.3.x）；安装脚本缺陷修复；控制台布局修复 |
| v2.2.13 | 改造 | 容器化 MariaDB 改为纯 compose 部署 |
| v2.2.15 | 清理 | 容器化 MariaDB 不再含安装脚本（纯 compose） |
| v2.2.16 | 修复 | 裸机包只适配裸机（移除容器回退脚本） |
| v2.2.17 | 修复 | 错误日志面板兼容全部部署形态（表优先 + 文件兜底） |
| v2.2.18 | 功能 | GTID 分叉感知 + 半同步复制基础（运行期 SET GLOBAL） |
| v2.2.19 | 改进 | GTID 分叉感知改进（续传优先，避免误 RESET MASTER） |
| v2.2.20 | 功能 | 半同步改配置文件持久化（重启不丢）；部署形态展示统一；版本号移入 `.env`；`install.sh` 一键安装；`custosctl` 命令行；复制卡死 / 1236 purge 自愈 |
| v2.3.0 | 地基 | MariaDB 版本支持矩阵 + 版本感知；变更生效提示（重启/重建/执行SQL 四级）；半同步命名统一 |
| v2.3.1 | 安全 | 凭证明文加密（`ENC1:` + Fernet，透明解密，启动自迁移） |
| v2.3.2 | 改进 | 备份一致性快照（CONSISTENT SNAPSHOT 内读） |
| v2.3.3 | 功能 | 维护模式（抑制切换/告警，放行关键字） |
| v2.3.4 | 功能 | 备份分库 + 备份操作日志 |
| v2.3.5 | 收官 | 2.3.x 收官：版本矩阵 / 凭证加密 / 备份一致性+位点 / 维护模式 / 备份分库 / 日志采集确认 |
| v2.3.6 | 体验 | 备份恢复性能/进度/日志闭环；MariaDB 错误日志三级读取（agent 兜底）；日志筛选与集群卡片 UI |
| v2.3.7 | 体验 | 待生效自动核验（`verify` 字段，落实后徽标自动消失） |
| v2.3.8 | 防呆 | 待生效徽标真实性（ack 核验门禁）；凭证解密防覆盖；半同步 SHOW VARIABLES 复核；`/api/heal` 冷却去重；只读 403 提示 |
| v2.3.9 | 重构 | 节点部署目录结构归整（配置/脚本/agent 分目录）；`custosctl node prepare` 自检；`.env` 去密（SECRET_KEY→`data/secret_key`，WEB_PASS/MGMT_MYSQL_PASS→compose，`.env` 纳入 git） |
| v2.3.10 | 修复 | 凭证加密改为**默认关闭**（`CRED_ENCRYPTION`，零密钥依赖）；密钥持久化 + 解密钥匙串回退；修 ENC1 密文当复制口令导致复制集体瘫痪（1470）；`_repoint_slave` 返回值契约与假成功；`custosctl node install` 支持容器 MySQL/MariaDB |
| v2.4.0 | 功能 | 数据迁移向导（MariaDB↔MariaDB + MySQL 5.7→MariaDB）；能力矩阵门禁；引擎方言适配；迁移前兼容性预检（dry-run） |

---

## v2.3.x 详细变更

### v2.3.0 · MariaDB 版本准绳与变更生效闭环
- **MariaDB 版本支持矩阵**（`app/mariadb_matrix.py`）：主线 12.3.3、支持 11.4/11.8/12.x、兼容 10.6/10.11、<10.6 不支持；预检新增「版本准入」项。
- **版本自动登记**：连库 `SELECT VERSION()` 回写 `nodes.mysql_version`（原常年空/过期）。
- **变更生效提示（核心）**：regen 时 diff 新旧文件，产出动作清单分四级 `recreate`/`restart`/`exec`/`manual`，每项带可直接复制的命令与「为什么必须这么做」。
- 节点列表「**配置待生效**」徽标 + 接口 `GET /api/nodes/{ip}/pending`、`POST /nodes/{ip}/ack_deploy`。
- 授权串收敛为单一来源 + `SHOW PRIVILEGES` 能力探测（不再吞 1064/1227）；半同步插件名统一由 `mariadb_matrix` 提供（MariaDB 全系列 `master/slave`，勿照 MySQL 改）。

### v2.3.1 · 凭证明文加密
- 新增 `app/crypto.py`（Fernet，`ENC1:` 前缀）：落库即密文、读取透明解密；存量明文在 `init_schema()` 末尾自动迁移。
- 仅 3 处写入显式 `encrypt()`（create/update_cluster、generate_node）；密钥优先级 `CRED_KEY` > `data/cred.key` > `SECRET_KEY` 派生。

### v2.3.2 · 备份一致性
- 备份读取统一在同一条 `START TRANSACTION WITH CONSISTENT SNAPSHOT` 内；`_dump_database` 复用 `data_conn`，不再按库自建连接。

### v2.3.3 · 维护模式
- 抑制 4 处（monitor 主库失联切换、延迟/压力切换、`_alert_on_transition`、`alerts._deliver`）；放行关键字：切换、数据冲突、备份失败、重建。

### v2.3.4 · 备份分库 + 备份操作日志
- 备份按库分别记录；`run_backup` / 恢复任务埋点写备份操作日志（`_log_event`/`read_backup_log` + `/api/backup/log`）。

### v2.3.5 · 2.3.x 收官
- 汇总：版本矩阵 / 凭证加密 / 备份一致性+位点 / 维护模式 / 备份分库 / 日志采集确认。版本号推进到 **v2.3.5**。

### v2.3.6 · 备份恢复体验闭环 + MariaDB 日志可读
- 恢复改为**后台任务** + 进度轮询（进度条/阶段/已用时长，刷新页面自动接管）；MySQL 恢复每 1000 条提交一次（修超长事务极慢）；备份操作日志端点 + 边界匹配 `id=N`。
- 错误日志**三级读取**：`mysql.error_log` 表 → `LOAD_FILE` → **节点 agent `/logs`**（兜底必留，绕开 0660 权限/`secure_file_priv`）；时间戳按来源区分时区（MySQL=UTC→+08:00；MariaDB=本地）。
- 日志筛选/集群卡片 UI 修复；审计页字段错配（`actor/action/target/detail`）修复。

### v2.3.7 · 待生效自动核验
- `deploy_actions` 每动作带 `verify` 字段（`db_uptime`/`proc:<svc>`/`db_alive`/`manual`）；`verify_node_pending` 自动核验落实后移除徽标；旧 JSON 无 `verify` 按 `manual` 保留。
- 详见 [`doc/releases/v2.3.7.md`](./doc/releases/v2.3.7.md)。

### v2.3.8 · 待生效徽标真实性 + 运维防呆
- `ack_node_pending` 改**核验门禁**（未落实保留并回 `reason`，`force` 人工兜底）；`decrypt()` 失败**原样返回密文**（防凭证被空口令覆盖）；`list_nodes` 补 `pending_actions`/`pending_since` 列；`ensure_semisync_runtime` 改 `SHOW VARIABLES` 复核真 ON（修半同步假成功）；`/api/heal` 加 15s 冷却 + 按(集群,主机,端口)去重；只读账号 403 提示；首次部署 `verify` 改 `db_alive`。
- 详见 [`doc/releases/v2.3.8.md`](./doc/releases/v2.3.8.md)。

### v2.3.9 · 节点部署目录结构归整 + 无口令 `.env`

- **节点包目录归整**：节点包配置/SQL 分 `mysql8.0/conf`、`mariadb12.3.3/conf`；脚本分 `<db>/scripts/`；agent 独立 `agent/` 子目录；日志统一软链到 `/data/custos-ha/logs/`；`custosctl node prepare` 自检 + 孤立单元清理；`deploy_actions` 执行命令改归整路径。
- **`.env` 去密、可入库同步（安全增强）**：`SECRET_KEY`、`WEB_PASS`、`MGMT_MYSQL_PASS` 均不再写入 `.env`：
  - `SECRET_KEY` → 首次启动持久化到 `data/secret_key`（权限 600，随 `data/` 备份、容器重建不丢；值不变则已有 `ENC1:` 凭据与登录态不受影响）；
  - `WEB_PASS` / `MGMT_MYSQL_PASS` → 由 `docker-compose.yml` 的 `environment` 驱动（`WEB_PASS` 留空则首次启动随机生成并打印到日志）；
  - `.env` 现仅含 `VERSION` + `COMPOSE_PROJECT_NAME` 等非密钥项，已纳入 git 跟踪（仓库不再忽略）。
- **升级注意（重要）**：从 v2.3.8 及更早升级时，`install.sh` 重跑会自动把旧 `.env` 的 `SECRET_KEY` 迁移到 `data/secret_key`（仅当 `data/secret_key` 不存在），**请勿在旧 `SECRET_KEY` 未迁移前手动删掉它**；否则由它派生的凭据密钥会变化、旧 `ENC1:` 凭据需靠管理库 `secrets` 表自愈（见 v2.3.1）。建议升级步骤：拉取新代码 → `sh install.sh`（自动迁移）→ 重启管理器。
- 详见 [`doc/releases/v2.3.9.md`](./doc/releases/v2.3.9.md)。

### v2.3.10 · 凭证加密默认关闭 + 复制自愈保底

- **凭证静态加密改为默认关闭（可按需开启）**：新增 `CRED_ENCRYPTION`（默认 `off`）。关闭时凭证明文落库、**完全不依赖任何密钥**，SECRET_KEY 变动 / 重装 / 重建镜像都不再导致存量凭证失效；有合规或高安全需求时置 `on` 启用。
  - 理由：凭证密钥（`data/cred.key`）与密文（管理库）同机同备份，加密实际只防「仅泄露 dump」一种场景，却引入「密钥一变全集群复制瘫痪且静默失败」的高危失效模式。
- **凭证密钥三层保底**（启用加密时）：① 主密钥持久化到 `data/cred.key`（0600，不再随 SECRET_KEY 变化，存量升级以当前派生密钥为种子、主密钥不变）；② 解密走**钥匙串**回退（`CRED_KEY` → `cred.key` → `SECRET_KEY` 派生，任一命中即成功）；③ SECRET_KEY 不可用时退化为随机密钥（原实现直接 `raise` 会导致应用起不来）。
- **反向迁移**：加密关闭时启动自动把仍可解开的 `ENC1:` 还原为明文；解不开的一律跳过、绝不改写。
- **修复（重大）**：复制口令为 `ENC1` 密文导致复制集体瘫痪 —— `decrypt()` 解密失败会原样返回密文，密文被当口令拼进 `CHANGE REPLICATION SOURCE`，报 `1470 - String 'ENC1:...' is too long for MASTER_PASSWORD (max 96)`。现于 `make_slave_of` 入口拦截未解密的 `ENC1:` 口令并给出可操作结论，**绝不把密文写进复制配置**。
- **修复**：`_repoint_slave` 返回值契约改为 `(ok, msg)`（原返回裸 bool，monitor 两处按元组解包会抛 `TypeError`，且该调用无 try 保护会中断整个监控循环、使 `ensure_topology` 永不执行）；重试耗尽仍未双 Yes 时返回**真实失败**（原无条件 `return True` 的假成功会掩盖故障、不告警）。
- **修复**：`custosctl node install` 不支持容器版 MySQL / MariaDB —— 节点包 compose 名为 `docker-compose.yaml` 而检测读 `docker-compose.yml`，导致容器节点被误判为裸机而找不到安装脚本。新增 `_cf()` 统一解析 compose 文件名。
- **卸载保底：密钥保险库（`/var/lib/custos`）**：`scripts/uninstall.sh` 无论是否 `-f`，都会先把 `data/cred.key` / `data/secret_key` 镜像到**项目目录之外**的 `/var/lib/custos/`（该目录卸载不删除）；`install.sh` 重装时若 `data/` 下缺失会自动取回，应用启动时 `crypto.bootstrap()` 也会从保险库回补，并把它纳入解密钥匙串。效果：**即便整个项目目录被删除后重装，存量凭证仍能解开**；只有删除 `/var/lib/custos` 才会真正失效。
- **启动自检（主动告警）**：`init_db` 启动时执行 `startup_credential_check()`，若存在解不开的凭证，立即在启动日志打印明细并写入告警中心（`alert_type=cred_key`，按集群聚合、24h 内同内容不重复）。这类故障原本**完全静默** —— 要等复制把密文当口令报 1470 才暴露，现象是「源库不可达」，与密钥毫无关联，排查成本极高。
- **凭证加密 Web 开关 + 一键回填 + 批量重置复制口令**：设置页「凭证加密」可在线切换开关、重置密钥，并新增「从节点包回填明文」「批量重置复制口令」两个一键按钮。
- 详见 [`doc/releases/v2.3.10.md`](./doc/releases/v2.3.10.md)。

---

## v2.4.0 详细变更

### v2.4.0 · 数据迁移向导（灰度实测）

- **新增「数据迁移向导」**：逻辑通道（dump + load）在真实旧数据上跑通，覆盖两类场景：
  - **MariaDB → MariaDB**（同引擎）：可桥接（bridge）做增量追平，管道 `continuous`。
  - **MySQL 5.7 → MariaDB 12.3.3**（跨产品线）：自动落进「宽松桶」，管道 `stopcopy`（停写拷贝，无增量追平）。
- **能力矩阵 `capabilities()` 为门禁唯一真源**（非品牌分支）：同引擎才允许物理通道与桥接；跨引擎走 stopcopy；**MySQL 8.0+ → MariaDB 明确拒绝**（`refused`）。MySQL 5.7 因无 `caching_sha2` 与 `utf8mb4_0900_ai_ci` 两个必炸项，天然与 MariaDB 兼容。
- **引擎方言适配层**：`tool_dialect` 用 `<binary> --help` 探测真实方言（Debian 的 `mysql-client` 是 `mariadb-client` 过渡包，文件名不可信）；`dump_gtid_args` / `ssl_args` 按方言分流。
- **迁移前兼容性预检（dry-run）**：只读 `information_schema` + 结构试运行（仅结构落目标 scratch schema 真实导入再删），在 TB 级拷贝前几秒拦下 `WITH PARSER ngram` 全文索引等必炸项。
- **后台异步逐步执行**：seed / bridge / verify / cutover / cleanup，进度 / 阶段文字 / 实时日志落库，前端 1.5s 轮询。
- **修复 7 个实测必炸点**：mariadb-dump 无 `--set-gtid-purged`、MariaDB 无 `@@log_replica_updates`、管理器镜像缺数据库客户端（Dockerfile 已补 `mariadb-client`）、GTID 位点写入 dump 冲突、方言按文件名误判、自签证书 TLS 校验失败、subprocess 二次 flush 崩溃。
- **已知边界**：`bridge / verify / cutover / cleanup` 四步尚未端到端实测；物理通道未实测（镜像内尚缺 `mariabackup`，且跨引擎本就被门禁拒绝）；账号/权限迁移未实现（当前排除 `mysql` 系统库）；MySQL 8.0 → MariaDB 按决策暂不实现。
- 详见 [`doc/releases/v2.4.0.md`](./doc/releases/v2.4.0.md)。

#### v2.4.0 稳定版定位

- **v2.4.0 为稳定版**：自本版起，**v2.4.0 分支只修 bug，不再新增功能**；新功能（集群接管 / 节点引导安装 / agent 指令通道）统一进入 **v2.5.0**。
- 镜像标签：`custos-server:v2.4.0`；版本号唯一来源 `.env` 的 `VERSION=`。
- **使用说明入口**（按使用场景选）：
  - 安装与部署：[`README.md`](./README.md)「一、快速开始」「1.4 添加被管节点（三种部署形态）」
  - Web 各页面与按钮：[`README.md`](./README.md)「二、Web 控制台各页面与按钮说明」
  - 日常运维 / 切换 / 备份 / 告警：[`doc/手册/用户操作手册.md`](./doc/手册/用户操作手册.md)
  - **把现有数据库搬进 Custos（v2.4.0 主线功能）**：[`doc/手册/迁移到本集群操作手册.md`](./doc/手册/迁移到本集群操作手册.md)（分步图解，小白版）
  - 接口对接：[`doc/开发/API接口文档.md`](./doc/开发/API接口文档.md)；字段含义：[`doc/开发/数据库字段说明.md`](./doc/开发/数据库字段说明.md)
- **升级方式**：`./scripts/deploy.sh up -d --build`（版本号经环境变量注入容器，**必须重建镜像才生效**）；节点侧无需升级。

---

## 历史版本（v2.2.x 及更早）

## v2.2.20 半同步复制配置文件持久化 + 部署形态展示统一

- **半同步复制改为「配置文件持久化」方式**（v2.2.18/19 为运行期 `SET GLOBAL` 启用）：
  - 节点生成时，按数据库类型注入对应半同步片段到 `my.cnf` / `ha-overrides.cnf`：
    - MySQL 8.0：写入 `plugin_load_add = semisync_source.so;semisync_replica.so`（≥8.0.26 用 `source/replica` 命名，更早用 `master/slave`；MySQL 不会自动补 `.so`，必须写全）。**注意**：`rpl_semi_sync_*_enabled` 现**不写进静态配置**（会导致 `mysqld --initialize` 中止、容器崩溃循环），改由管理器运行期开启，详见下方「v2.2.20 修订」。
    - MariaDB：写入 `plugin_load_add='semisync_master;semisync_slave'`、`rpl_semi_sync_master_enabled=1`/`rpl_semi_sync_slave_enabled=1`。
  - 随实例启动即生效、**重启不丢**，根治「升版/重启后 `SET GLOBAL` 清零导致半同步静默退化为异步」的反复问题。
- **移除运行期半同步启用逻辑**：`ha_client.enable_semisync` / `node_gen` 的运行期注入、`monitor` 的半同步自愈定时任务与 `SEMI_FIX_*` 标记全部删除；半同步状态完全由配置文件决定。（**注**：因 `rpl_semi_sync_*_enabled` 写进静态配置会导致 `mysqld --initialize` 中止、容器崩溃循环，该条款已调整 —— 改为「配置文件只注入 `plugin_load_add` + 管理器 `ensure_ha_creds` 运行期 `SET GLOBAL` 开启」，详见下方「v2.2.20 修订」。）
- **部署形态展示名统一**：`api.display_node_type` 将 `docker` 形态对外显示为「MySQL」（不再显示「Docker」），`mariadb` / `mariadb-docker` 显示为「MariaDB」；节点管理页与集群页徽标 `Docker→MySQL`。
- **版本号与镜像改名**：
  - 删除 `app/VERSION` 文件，版本号改由 `.env` 的 `VERSION` 字段管理。
  - 服务端容器/镜像名统一为 **`custos-server`**（原 `custos`），Docker 节点容器固定为 `custos-mysql`（MySQL）/ `custos-mariadb`（MariaDB）。
  - 镜像标签：`custos-server:v2.2.20`、`custos-node:v2.2.20`。

### v2.2.20 修订：半同步落地方案修正 / 复制账号认证 / OS 指标修复

> 本节记录 v2.2.20 发布后的就地修正（版本号未变，仍为 `v2.2.20`）。

- **半同步：修正原「配置文件持久化」方案（根治「节点起不来」）**
  - **问题**：把 `rpl_semi_sync_*_enabled=1` 写进静态 `my.cnf` / `ha-overrides.cnf` 后，`mysqld --initialize`（首次初始化）期间半同步插件尚未加载 → 报 `unknown variable` 并**中止初始化**，配合 `restart: always` 造成容器**崩溃循环**，表现为「节点起不来、也连不上」。
  - **现方案**：
    - `node_gen._semisync_block_mysql()` **只注入 `plugin_load_add`**（MySQL 8.0.26+ 必须写带 `.so` 的 soname：`semisync_source.so;semisync_replica.so`），保证插件随启动加载。
    - `rpl_semi_sync_*_enabled` **不再写入静态配置**，改由管理器 `ensure_ha_creds` 在自愈周期运行期 `SET GLOBAL` 开启（受 `SEMI_SYNC` 开关门控）。
  - **运维判读须知**：
    - 须区分 `enabled`（变量＝开关）与 `status`（状态＝运行时）。
    - 副本开启半同步后**需重启复制 IO 线程**（`STOP REPLICA; START REPLICA;`）才会以半同步方式重连。
    - 主库在副本未开启期间等待 ACK 超时会**降级为异步**（`Rpl_semi_sync_source_no_tx` 增长、`source_status=OFF`），属 MySQL 正常降级；**集群空闲无写入时不会自动恢复**，下一次写事务即自动恢复（`yes_tx` 增长、status 转 ON）。
- **MySQL 8.0 复制账号改为 `mysql_native_password`**：`ensure_ha_creds` 的 MySQL 分支新增 `idn_repl`，仅 `replication` 账号强制 `mysql_native_password`（`ha_monitor` / `app` / `root` 仍按 `auth_plugin`）。修复非 SSL 复制 IO 线程因 `caching_sha2_password` 握手失败导致的复制中断，并避免旧管理器每自愈周期把 `replication` 改回 `caching_sha2` 造成反复断链。**生效需重建管理器**。
- **节点 OS 指标面板修复**（原恒显示「加载中…」或「加载失败」）：
  - `/api/status` 返回的 `clusters` 仅有 `id` / `name`，前端却取 `cl.cluster_id` → `undefined` → `getElementById("osmetrics-undefined")` 为 `null` → 静默 return，占位文案永不刷新。改为 `cl.cluster_id || cl.id`。
  - `/api/os_metrics` 响应补 `ok: True`（`_json_ok` 不会自动补 `ok`），否则前端 `if (!d.ok)` 会显示「OS 指标加载失败」。
- **新增节点 agent 容器部署模板** `scripts/node-agent.compose.yml`：`custos-node-agent` 独立 compose（host 网络、端口 `28889`、`pid: host`、`/:/host:ro` 配 `--root /host` 读宿主机真实指标），便于在**已部署但缺少 agent** 的节点上单独拉起 OS 指标采集；也可把该 service 并入现有节点 compose。
- **复制卡死自愈（断电 / 异常恢复后延迟锁死）**：
  - **问题**：测试环境从库断电 1 天恢复后，`Seconds_Behind_Master` 锁死在固定高位（如 `54559s`）且恒定不变，主库无新写入也追不平。
  - **根因**：`Seconds_Behind_Master` = SQL 线程「当前正在执行的 event 在主库的提交时间」距 now 的秒数。断电复活时 SQL 线程卡在断点处半途事务（relay 损坏 / 半途 DDL）不前进 → `Executed_Gtid_Set` 停住 → 延迟恒等于「卡住事务提交时间距 now」；是**卡死而非真落后**。原自愈仅触发于 `slave_sql=="No"`，漏掉「线程在跑但卡住」；且 `get_status` 不返回复制位点，无法区分「卡死」与「正在追平」。
  - **修复**：
    - `ha_client.get_status` 新增返回 `exec_gtid`（`Executed_Gtid_Set` / MariaDB `Gtid_Slave_Pos`），使 monitor 能感知位点是否前进。
    - `monitor` 新增卡死检测：`seconds_behind > 600` 且 `exec_gtid` 连续 3 个周期不变 → 判定卡死，自动调用 `_repoint_slave`（内部 `make_slave_of`：`RESET SLAVE` 丢弃坏 relay + 重 `CHANGE MASTER` + `START SLAVE`，**不 RESET MASTER**）重挂当前主做 GTID 续传；带 5 分钟冷却退避。
  - **生效**：重建管理器（`./scripts/deploy.sh up -d --build custos-server`）。现场急救：从库 `STOP SLAVE; RESET SLAVE; CHANGE MASTER TO MASTER_AUTO_POSITION=1`（MySQL）/ `MASTER_USE_GTID=slave_pos`（MariaDB）; `START SLAVE;`（前提主库对应 binlog 未 purge，否则需 rebuild 从库）。
- **新增「数据交互状态」展示（节点在集群中是否处于数据交互、与谁读写）**：
  - **展示内容**：每个节点一行「数据交互」徽标 + 上下游箭头——主库显示 `主库·可写 → N 个从库`；从库显示 `复制正常/中断/卡死 ← 上游主库:port`；离线/脱管也有对应标识。状态枚举 `repl_state`：`primary`/`repl_ok`/`repl_broken`/`repl_stuck`/`orphan`/`offline`。
  - **后端采集**：`ha_client.get_status` 主库分支新增 `SHOW REPLICAS`/`SHOW SLAVE HOSTS` 取下游从库列表；`monitor._classify_repl_state` 据角色/复制线程/`stuck` 判定状态；卡死预标记用独立 key `stuck_pre` 在 `_update_host` 前设 `st["stuck"]`（原卡死自愈在写库之后，故需前置标记）。
  - **落库与接口**：`hosts` 表新增 `repl_state`/`upstream`/`downstream` 三列（`db.py` 迁移）；`_update_host` 写入（纳入空闲零写入的 changed 比对）；`api` 的 `_build_status_payload`/`get_cluster_detail` 的 `host_rows` 均 `SELECT` 三列并 `_parse_downstream` 把 JSON 还原为数组。
  - **前端**：仪表盘与集群详情的节点表格新增「数据交互」列（徽标 + 上下游），仪表盘刷新 JS 由 `renderReplState` 维护。版本号仍为 `v2.2.20`，重建管理器生效。
- **仪表盘：集群健康卡片内新增配置漂移「一键修复」**：
  - 原「一键修复」只在页面下方的「观测中心 → 配置一致性」卡片里，需先切集群下拉再点；现每个集群健康卡片头部（「预检」旁）在检测到漂移时自动出现同名同款按钮（`btn btn-sm btn-ghost`，样式与「预检」一致），无漂移时隐藏。
  - 实现：`dashboard.html` 卡片头新增 `driftfix-{cluster_id}` 按钮；`loadDrift()` 刷新徽标时同步按 `drift > 0` 显隐；`remediateDrift(cid, btn)` 通用化（不传参时沿用观测中心当前集群，兼容旧调用）。
- **告警邮件：新增「自愈观察期」（短时自愈只记录、不发信）**：
  - **问题**：节点抖动 / 复制瞬断常在 1~2 个探活周期内被自愈逻辑修好，却仍会发出「故障 + 已恢复」的无效邮件对，运维被刷屏。
  - **机制**：`alerts.send_alert` 新增 `grace` / `cancel_key`——状态类告警（节点离线、复制中断、复制延迟超阈、半同步退化）先挂起 `ALERT_GRACE_SEC` 秒；期间若故障自愈，monitor 用同一 `cancel_key` 调 `cancel_alert()` 撤销待发邮件，并 `record_alert()` 往告警中心记一条 **info 级**记录（`alert_type=selfheal`，status=`healed`），**不发邮件**。观察期结束仍未恢复才真正投递。
  - **默认 30 秒**（可在「告警 → 告警发送规则 → 自愈观察期」调整；填 0 = 关闭观察期、立即发送）。自动切换类邮件（`failover.py`）不受影响，仍立即发送。
  - 覆盖范围：节点离线/恢复、复制中断/恢复、延迟跨阈/回落、半同步退化/恢复。
- **主库 binlog 已清理（1236 purge）场景优化（体验 + 可操作性）**：
  - **问题**：节点被重装 / 数据清空后重新上线，IO 线程报 `Got fatal error 1236 ... 'Could not find GTID state requested by slave in any binlog files. Probably the slave state is too old and required binlog files have been purged.'`。该类故障**任何自愈都无效**（重挂复制只会重新请求同一批已被清理的 GTID；`heal_io_gtid_gap` 解决的是相反方向），但旧逻辑每周期反复尝试且界面无任何指引，表现为「点了修复也没用、看不到下一步」。
  - **修复**：
    - `ha_client.is_gtid_purged_error()` 判定该类错误；monitor 探活阶段置 `st["purged"]`，`repl_state` 新增 **`repl_purged`** → 界面显示 **「● 需重建」** 徽标（仪表盘 + 集群详情，含 tooltip 说明）。
    - 新增 `_purge_guard()`：命中即**停止无效自愈**（退避 10 分钟，不再每周期重挂）、首次发现时发一封**可操作**的告警（明确写「对该节点点击【重建】」）并落一条 `need_rebuild` 记录；复制恢复后自动解除标记。
    - 可选自动重建：设置 `AUTO_REBUILD_ON_PURGE=1`（默认 **0 关闭**，因重建会用主库数据覆盖该节点）后自动触发在线重建，带 30 分钟触发冷却。
    - 一键重建入口下沉到**仪表盘节点操作列**（原只在集群管理页），带二次确认 + 右下角进度条（轮询 `/api/node/rebuild_status`），完成后自动刷新状态。
    - `/api/heal`（「修复复制」按钮）命中该类错误时直接返回明确指引，不再空转。
    - 重建成功后 `rebuild.last_rebuilt_at()` 记录时间戳，monitor 立即清理该节点的自愈退避 / 卡死计数 / 需重建标记，避免「重建已完成但页面仍显示旧状态」。
    - **「重建成功但复制仍断、仍显示需重建」的根因与修复**：逻辑重建 = 主库 dump + 从库 restore（且 `SET sql_log_bin=0`），**不会写入从库 GTID 状态**；重建完的从库 `gtid_slave_pos` / `gtid_executed` 为空，以 GTID 自动定位（`MASTER_USE_GTID=slave_pos` / `MASTER_AUTO_POSITION=1`）连主库时等于从「时间起点」请求，而主库早期 binlog 多半已清理 → 恒报 1236。修复：`rebuild` 在重链复制前新增 `_align_gtid_after_restore()`，把从库位点对齐到**主库导出结束时**的位置（MariaDB `SET GLOBAL gtid_slave_pos`；MySQL 8 执行集为空时 `SET GLOBAL gtid_purged`）；仅当从库位点为空/非主库子集时才动作，已有且是子集则不动。导出窗口内提交的事务可能未被 dump 捕获，故完成消息里提示跑一次一致性校验。
    - **重建后仍 1236 的三级兜底（彻底根治）**：GTID 对齐后若 IO 仍报 1236，rebuild 依次尝试——① 重新取主库**当前** `gtid_binlog_pos` 再对齐（位点可能被 `make_slave_of` 的分叉处理冲掉，或导出期间主库已切换）；② 仍不通则改用**主库当前 binlog 文件+位点**（`SHOW BINLOG STATUS`/`SHOW MASTER STATUS`）做 `CHANGE MASTER TO ... MASTER_USE_GTID=no`（MySQL 用 `MASTER_AUTO_POSITION=0`）坐标续传，彻底绕开已被清理的 GTID 历史；③ 最终仍 `IO/SQL ≠ Yes` 则**如实返回失败**并把 `Last_IO_Error` 原文写进消息（旧实现一律返回成功，掩盖「数据恢复了但复制是断的」）。
- **新增命令行工具 `custosctl`（可安装到 `/usr/bin`，随卸载脚本一同卸载）**：
  - 来源：`custosctl`（仓库根，POSIX sh）为 `scripts/deploy.sh` 的 superset；`scripts/deploy.sh` 与 `Makefile` **保留**不变（Makefile 仅新增 `install`/`uninstall`，并把 `restart` 语义改为「down && up -d」与 custosctl 对齐）。
  - 安装/卸载：`sudo ./scripts/install-custosctl.sh [项目目录]`（或 `sudo make install` / 已部署后 `sudo custosctl install`）→ 写入 `/usr/bin/custosctl`；项目目录记录在 `/etc/custos/custosctl.env`（`CUSTOS_HOME`）。`custosctl uninstall` 会删除 `/usr/bin/custosctl` 与 `/etc/custos`。
  - **项目目录定位（不猜固定路径）**：各人安装习惯不同，故按可靠性降序多级发现——① 命令行 `-C/--dir <目录>` ② 环境变量 `CUSTOS_HOME` ③ `/etc/custos/custosctl.env` ④ **从运行中的容器反查**（`docker inspect` 读 compose 写入的 `com.docker.compose.project.working_dir` 标签，最贴合真实部署，实测在任意目录下都能自动命中）⑤ 常见路径探测 ⑥ 当前目录及父级。全部失败时给出 4 条修法指引。`custosctl status` 会打印「项目目录来源」便于排查。
  - 命令：`deploy|up`、`rebuild`（**重建管理器镜像**）、`start`(up -d)、`stop`(down)、`restart`(down && up -d)、`down`、`status`、`ps`、`tail [-n N] [--no-follow]`、`logs`、`shell`、`version`、`rebuild-node --host --port --cluster`（走 API 触发**从库**重建，与界面「重建」等价）、`install`、`uninstall [-f|--purge] [--yes]`、`help`；未识别子命令原样透传给 `docker compose`。
  - ⚠ 语义约定：`rebuild` = 重建**管理器镜像**；从库数据重建用 `rebuild-node`。二者不可混用。
  - 卸载策略：`uninstall` 默认停删容器 + 删除本项目镜像与匿名卷 + 删除 custosctl（**保留** `backups/ data/ generated_nodes/`）；加 `-f/--purge` 才**永久删除**这三个目录（含 `data/custos-ha/`），需输入 `DELETE` 二次确认（`--yes` 跳过）。**两种模式都绝不动被管节点**上的数据库与 keepalived。
- **新增 `install.sh`：【交互式一键安装】（解决「新用户首次部署不简便」）**：
  - 问题：仓库里 `env.example`、`docker-compose-example.yml` 都是脱敏模板，旧流程要 `cp env.example .env` + 手工改 `docker-compose.yml` 里的真实 IP / 管理库 / VIP / 网卡 / Web 口令，新人极易漏配或配错。
  - 方案：一条命令完成首次部署——`sudo sh install.sh`（交互）或 `sudo sh install.sh -y`（非交互，全默认 + 随机口令，适合脚本化）。
    1. 检查 docker / docker compose；2. 交互收集（管理库、管理器对外 IP、VIP、网卡、Web 账号端口，默认值从现有 compose / 自动探测取）；3. 生成 `.env`（**SECRET_KEY / WEB_PASS 自动生成强随机值**，不再用 admin/admin）；4. 把参数写回 `docker-compose.yml`（含 `set_yaml` 改值 + 取消注释 VIP，免手工编辑）；5. root 时自动 `install-custosctl.sh` 装到 `/usr/bin`；6. 构建启动 + 自检 + 打印访问地址与账号。
  - **幂等**：已有 `.env` / `docker-compose.yml` 时以现有值为默认；无 `docker-compose.yml` 时从 `docker-compose-example.yml` 复制再注入。重复执行安全。
  - `.gitignore` 已正确忽略 `.env` / `data/` / `generated_nodes/` / `backups/` / `docker-compose.yml`：真实 IP 与口令只留本地、不入库，故「脱敏」问题已被 `.gitignore` 解决，无需在仓库内改 compose。
  - 新增用户标准流程：`git clone` → `sudo sh install.sh` → 浏览器访问提示的地址登录。
- **MariaDB 日志刷 1064（CHANGE REPLICATION FILTER）**：`mysql_compat` 原注释误判为「MariaDB 只是等号无空格」，实际 **MariaDB 根本没有 `CHANGE REPLICATION FILTER` 语句**（动态复制过滤是 MySQL 独有，MariaDB 只能写配置文件），故每次 `make_slave_of` 都刷一条 `1064 ... near 'REPLICATION FILTER REPLICATE_IGNORE_DB=(ha_local)'`。修正：MariaDB 返回 `None` 直接跳过（`ha_local` 写入已由 `SET SESSION sql_log_bin=0` 保证不进 binlog，不依赖该过滤器）。

## v2.2.19 GTID 分叉感知（续传优先，解 v2.2.18 误判）

- **问题**：v2.2.18 的 GTID 分叉检测只看从库「已应用集」(`gtid_slave_pos`)，一旦检测到分叉就 `RESET MASTER`；对于「从库自有 binlog(`gtid_binlog_pos`) 早已领先、但已应用集落后」的节点会被误判强制重置，丢失已执行位点、破坏续传。
- **修复**：
  - 检测范围扩展为同时比较 `gtid_slave_pos` 与 `gtid_binlog_pos`：仅当**两者都不是主库 GTID 子集**时，才判定真正分叉并 `RESET MASTER` 解除冲突。
  - 否则仅重置从库复制链路（`RESET SLAVE` + 重 `CHANGE MASTER`），保留已应用位点继续从主库续传，避免无谓全量重建。
- **效果**：升版重启 / 历史切换后从库位点判断更精准，`out-of-order sequence` 类复制卡死自愈更安全。

## v2.2.18 GTID 分叉感知 + 半同步复制基础

- **GTID 分叉感知**：`ha_client.make_slave_of` 在重指向前新增 GTID 分叉检测与自愈，解决「升版重启 / 历史切换后从库执行集超前或分叉于主库」导致的复制卡死（`out-of-order sequence`）。检测到分叉时 `RESET MASTER` 解除冲突后重链。
- **半同步复制（初版，运行期启用）**：新增 `SEMI_SYNC` 环境变量；`node_gen` 生成节点时向 `my.cnf` 注入半同步插件加载与 `rpl_semi_sync_*_enabled=1` 参数，并在链接后在运行期 `SET GLOBAL` 启用，主库提交等待至少 1 个从库 ACK，压缩切换数据丢失窗口。**注意**：该初版依赖运行期 `SET GLOBAL`，实例重启后会清零退化为异步，故在 v2.2.20 改为配置文件持久化（见 v2.2.20）。

---

## v2.2.17 错误日志面板兼容全部部署形态（MySQL/MariaDB 裸机/docker）

- **问题**：「24 小时错误日志」面板此前仅用 `LOAD_FILE(@@log_error)` 读节点错误日志文件；容器化 MariaDB/MySQL 多将日志写 stdout 或受 `secure_file_priv` 限制，导致面板恒空，复制错误(如 1133)看不到。
- **修复**：
  - 管理器接口改为「`mysql.error_log` 表优先（纯 SELECT，容器/裸机通用，不受 secure_file_priv 限制）→ 文件 LOAD_FILE 兜底」，返回结构化 `rows{time,level,message}` + `source`。
  - MySQL 8.0 docker 节点 `ha-overrides.cnf` 启用 `log_error_services=...;log_sink_table`，错误日志落 `mysql.error_log` 表。
  - MariaDB docker 节点放开 `secure_file_priv=''`，保证文件日志 `LOAD_FILE` 可读（兜底）。
  - 前端按 `rows` 渲染时间/级别/消息，并标注来源（表/文件）。
- **生效**：重建管理器（`./scripts/deploy.sh`）；MySQL8 docker 节点需重新生成并部署以获取表错误日志配置。

## v2.2.16 裸机包只适配裸机（移除容器回退脚本）

- **问题**：裸机（`mariadb`）节点生成目录曾一并交付 `Install-mariadb-docker-Node.sh`，供 glibc 过低装不上 tarball 时改跑容器。该脚本把**裸机完整 `my.cnf`**（仅剔除 `datadir`/`socket`/`pid_file`）喂给官方镜像容器，残留裸机专属参数 —— 典型如**缺失 `log_error`**（官方镜像默认写 stdout、`@@log_error` 为空，导致管理器「错误日志」面板恒为空）与 `innodb_buffer_pool_size=1G`，致使容器 MariaDB 与裸机主从不能正确适配；且部署机制分裂为「`docker run --network host` + keepalived-only compose」，与 `mariadb-docker` 的纯 compose 形成两条并存路径，必然再次漂移。
- **另一后果**：节点库里 `node_type=mariadb` 而实际运行容器，导致 UI 徽标显示「MariaDB 裸机」、卸载脚本与数据目录指引与真实形态全部不符。
- **修复**：裸机节点目录**不再交付**该回退脚本，脚本文件从 `app/node_assets` 移除（如需恢复可从 git 历史取回）。需要容器化时**创建节点直接选 `mariadb-docker`** —— `node_family` 本就允许两种 MariaDB 形态同集群混部，且数据目录同为 `/data/custos-ha/mariadb12`，既有回退安装可**无损迁移**到纯 compose。

---

## 一、v2.2.15 容器化 MariaDB 节点清理安装脚本

- **生成节点不再含安装脚本**：`generate_node` 对 `mariadb-docker` 不再生成 `Install-mariadb-docker-Node.sh`（部署早已是纯 compose，入口即 `docker compose up -d`）；仅保留 `Uninstall-mariadb-docker-Node.sh`。
- **安装脚本保留为回退**：`Install-mariadb-docker-Node.sh` 文件不删除，仍作为「裸机 MariaDB 装不上 tarball（如 glibc 过低）」时的逃生通道，在裸机（mariadb）节点生成目录中一并交付。
- 同步修正 Web 生成提示与操作手册中仍指向安装脚本的过期文案。

---

## v2.2.13 容器化 MariaDB 部署方式改造（纯 compose）

> 部署方式重大变更：容器化 MariaDB 节点（`node_type=mariadb-docker`）从「安装脚本 + host 网络单独拉起容器」改为与容器化 MySQL 8.0 一致的**纯 compose** 部署；安装脚本仅保留给「裸机 + 容器混部」场景。

- **部署形态统一**：`generate_node` 对 `mariadb-docker` 直接生成含 `mariadb` + `keepalived` 双服务的 compose，部署入口即 `docker compose up -d`（原入口为 `sudo bash Install-mariadb-docker-Node.sh`）。
- **跨发行版镜像**：默认镜像改为 **Debian 构建**的 `mariadb:12.3.3`（备选 `mariadb:12.3.2`），规避 UBI 构建（`12.3.x-ubi`）的 `USER=mysql(uid 999)` 权限拒绝、无 `/etc/mysql/my.cnf` 配置路径不一等坑（实测是 Ubuntu 节点异常主因）；同时不再要求 x86-64-v3 指令集，兼容老 CPU（Anolis 7.9 等）。
- **数据目录统一**：容器化 MariaDB → `/data/custos-ha/mariadb12`，容器化 MySQL 8.0 → `/data/custos-ha/mysql8`。
- **VIP 端口在 compose 层解决**：compose 同时映射「节点端口」与「VIP 端口」到容器同一端口，业务连 `VIP:VIP_PORT` 直连容器，**不再依赖节点 iptables/firewalld 转发**，避开 firewalld 与 ufw 差异（Ubuntu / Anolis 行为一致）。
- **挂载 `:z` 兼顾 SELinux=Enforcing 的 Anolis/RHEL；未启用 SELinux 的 Ubuntu 忽略该标记**，同一份 compose 兼容两类系统。
- **低内存兼容**：容器化 MariaDB 的 `innodb_buffer_pool_size` 默认降至 512M（官方/裸机模板为 1G，小内存机易 OOM）。
- **卸载脚本同步改造**：`Uninstall-mariadb-docker-Node.sh` 现自动识别「纯 compose」与「安装脚本」两种来源，分别走 `docker compose down` 与 `docker rm -f` + keepalived 停止，并清理可能的 VIP 端口转发。

### 升级注意
- 重新生成节点会生成 `custos-node:v2.2.13` 的 keepalived 镜像 tag，首次 `docker compose up -d` 会在节点侧 `build` 新镜像。
- 存量容器化 MariaDB 节点重生成后，数据目录由 `/data/mariadb` 变为 `/data/custos-ha/mariadb12`，旧数据不会自动迁移（目录均在同一宿主机，需按需手动处理）。

---

## v2.2.0 项目更名（Custos）

- **产品正式更名为 Custos**（拉丁语「守护者」），tagline：**Never let your database down**。
- 镜像 / 容器 / Dockerfile 同步更名：
  - 镜像标签：`mysql-ha-manager:v2.1.x` → `custos:v2.2.0`
  - 容器名（compose `container_name`）：固定为 `custos`（不再随仓库目录名派生为 `mysql-ha-manager-ha-manager-1`）
  - compose 服务名：`ha-manager` → `custos`
  - 管理器日志文件：`ha-manager.log` / `ha-manager-console.log` → `custos.log` / `custos-console.log`
  - 节点部署目录默认前缀：`mysql-ha-manager/generated_nodes` → `custos-ha/generated_nodes`
- 代码 / 文档 / 页面品牌（标题、导航、登录页、告警 / 节点 / 设置等）统一为 Custos。

---

## v2.2.1 仓库目录改名（custos-ha）

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

## v2.2.2 全站深色主题（对齐登录页）+ 首页全屏 + 空闲零写入

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

> **遗留（需到节点本机处理，非代码问题）**：三个测试节点的 `root@'192.0.2.66'`
> 权限被限制（缺 RELOAD / 对 `mysql.user` 无 SELECT），`.213` 的 ha_monitor 还有 1045 密码漂移，
> 因此上述授权无法由管理器远程自愈，需在各节点本机以本地 root 执行
> `generated_nodes/<ip>/fix-node-creds.sql`。此外 `current_master` 记录的是 `192.0.2.203`，
> 而节点实际可写主库是 `192.0.2.213`（两者不一致），授权修好后建议重新指定主库或触发一次切换。

---

## v2.2.11 节点形态扩展 + 安装/控制台缺陷修复

### 节点部署形态扩展（三种）

1. **新增 Docker MariaDB 12.3.x 支持**：新增 `Install-mariadb-docker-Node.sh`（端口 3306），与管理器既有 Docker MySQL 8.0、裸机 MariaDB 12.x 并列，实现「裸机 MariaDB 12.3.3 ↔ Docker MariaDB 12.3.3」组 HA。
2. **CPU 指令集预检 + 镜像回退**：`mariadb:12.3.3-ubi` 按 x86-64-v3 编译，老旧 CPU（如 QEMU 虚拟 CPU，仅到 v2）会直接 `Fatal glibc error: CPU does not support x86-64-v3` 秒退。安装脚本起容器前用 `docker run --rm --entrypoint mariadbd <img> --version` 预检（glibc 的 v3 检查在进程加载阶段即触发，探测可靠），失败自动回退备用镜像 `mariadb:12.3.2`，两者皆失败才报错并提示改用裸机方案。
3. **小内存机保护**：物理内存 < 3000MB 时，脚本自动把 `innodb_buffer_pool_size` 收敛为 `内存×40%`（≤512M、≥128M），避免被 OOM 杀掉崩溃重启；亦可用 `MARIA_BUFFER_POOL=512M` 手动覆盖。
4. **keepalived 镜像统一命名**：随项目更名，keepalived 镜像由 `mysql-ha-keepalived` 统一改为 `custos-node`（与管理器 `custos` 对齐）。

### 安装脚本缺陷修复（Install-mariadb-docker-Node.sh）

- **误报「未找到客户端」根因修复**：原探测写成 `docker exec <ct> command -v mariadb`，而 `command -v` 是 shell 内建、**不是可执行文件**，`docker exec` 默认不经由 shell，于是去 exec 不存在的 `command` 程序 → 永远失败 → 误报。改为 `docker exec <ct> sh -c "command -v $b || which $b"`（经 shell 执行内建）。
- **OOM 崩溃重启不再被掩盖**：原逻辑在 `docker run -d` 后立刻找客户端，命中重启间隙就误报「未找到客户端」。新增 `docker inspect` 的 `RestartCount>3` 探测崩溃重启循环，直接打印 `docker logs` 真实错误（OOM/端口/权限/旧数据目录损坏）。

### Web 控制台缺陷修复

- **仪表盘集群卡片布局偏移**：原 `repeat(auto-fit,minmax(600px,1fr))` 在 3 集群时排成「2+1」，第 3 张卡独占半宽造成错位。改为**上下排列单列全宽**（`display:flex;flex-direction:column`），并移除旧媒体查询里的 `grid-template-columns` 覆盖；表格仍带 `.tbl-scroll` 横向滚动 + 操作列 `position:sticky` 吸右。
- **告警中心详情超出页面**：`.tbl td` 默认 `white-space:nowrap` 压制了详情列换行。详情改为 `<div class="alert-detail">` 包裹，`max-height:140px;overflow:auto` 框内滚动，并加深色底纹。
- **节点管理「下载/查看自愈 SQL」422**：`@app.get("/nodes/{ip}/creds_sql")` 装饰器误挂到内部辅助函数 `_render_node_fix_sql` 上，真正路由 `node_creds_sql` 无装饰器 → FastAPI 把辅助函数当路由、参数不匹配 → 422。装饰器移回 `node_creds_sql` 后正常（200 `application/sql`）。

### 文档

- 新增 `doc/`：`用户操作手册.md`、`API接口文档.md`、`数据库字段说明.md`、`业务流程图.xmind`（含裸机/容器与判断分支）；清理旧 `mysql编译版安装说明.md`。

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
4. **部署**：仅用 `docker compose`；改 `./app` 后须 `docker compose up -d --build custos-server` 重建（v2.2.20 起服务端容器/镜像名为 `custos-server`，原 `custos`）。

---

## 四、版本历史

- **v2.1.0**：补齐 v3 高保真稿剩余模块——主库状态 gauge、实时活动 feed、系统设置（轮询间隔 / 内网 IP）、告警已自愈 / 未恢复标记、集群节点自愈 SQL 查看 / 下载。新增 `alerts`、`audit_log` 持久化与 `/api/node_resources`、`/api/activity_feed`、`/api/node_creds_sql` 等端点。
- **v2.2.11**：节点形态扩展至三种（新增 Docker MariaDB 12.3.x，含 CPU 指令集预检回退 12.3.2 与小内存 buffer pool 自动收敛）；修复安装脚本误报「未找到客户端」(command -v 误用)、OOM 崩溃循环掩盖；修复仪表盘卡片布局偏移、告警详情溢出、节点自愈 SQL 下载 422；新增 doc/ 用户手册 / API 文档 / 数据库字段 / 业务流程图。
- **v2.0.x**：基础 HA 管理能力（监控 / 自愈 / 切换 / VIP / 告警 / 预检 / 慢日志 / 配置漂移 / 审计）。
- **v2.2.18**：GTID 分叉感知（make_slave_of 重指向前检测并自愈，解升版重启 / 历史切换后复制卡死）；半同步复制基础（SEMI_SYNC 运行期 SET GLOBAL 启用）。
- **v2.2.19**：GTID 分叉感知改进（同时比对 gtid_slave_pos 与 gtid_binlog_pos，仅真分叉才 RESET MASTER，续传优先）。
- **v2.2.20**：半同步复制改配置文件持久化（my.cnf 注入插件与 rpl_semi_sync_*_enabled=1，随启动生效、重启不丢），移除运行期 SET GLOBAL 启用逻辑；部署形态展示名统一（docker→MySQL / MariaDB）；版本号移入 .env，服务端镜像/容器改名 custos-server。
