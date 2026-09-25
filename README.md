# Custos

> 当前版本：**v2.4.0**（数据迁移向导：MariaDB↔MariaDB + MySQL 5.7→MariaDB）
> 版本定位与变更记录（含路线图）见 [`RELEASE_NOTES.md`](./RELEASE_NOTES.md)；**v2.3 起**每个版本的独立规划见 [`doc/releases/`](./doc/releases/)。
> 仓库目录名与产品名统一为 **`custos-ha` / Custos**（v2.2.0 起完成目录改名）；下文命令路径以仓库根目录 `custos-ha/` 为准。

基于 **Keepalived + MySQL/MariaDB 主从（GTID）** 的 MySQL 高可用方案，附带一个轻量管理器：
- **决策器（monitor）**：探活主从、按策略自动选主、控制 VIP 漂移与脑裂保护；
- **Web 控制台**：多集群管理、查看拓扑 / 切换历史 / 自动发现节点、一键生成节点部署文件；
- **节点一键部署**：在 Web 上填写节点信息后，自动生成可直接 `docker compose up -d` 的节点目录；
- **半同步复制（可选）**：服务端 `SEMI_SYNC=1` 时，节点 my.cnf 自动注入半同步插件与开启参数，主库提交等待从库 ACK，把故障切换的数据丢失窗口压缩到接近 0（随实例启动生效、重启不丢）。

---

## 一、快速开始（安装与部署流程）

### 1.1 前置条件

- 管理端机器：Linux + Docker + docker compose v2；
- 每台节点机器：Linux + Docker + docker compose v2，且**节点机器到管理库 3306 端口网络可达**；
- 一个**独立的外部管理 MySQL**（推荐），用户/密码/库名统一约定为 `ha`（也可自定义）；
- 一个与节点同网段、**当前空闲**的 VIP（如 `192.0.2.10`），本工具无法自动探测，必须填写；
- 节点 Docker 必须允许 `NET_ADMIN` / `NET_BROADCAST` / **`NET_RAW`**（keepalived 用原始套接字发 VRRP 通告，缺 `NET_RAW` 会报 `Operation not permitted` 并导致双主/split-brain）。节点生成目录已显式 `cap_add: - NET_RAW`。

### 1.2 一键安装（推荐新用户）

```bash
git clone <仓库地址> custos-ha && cd custos-ha
sudo sh install.sh               # 交互收集管理库/VIP/网卡/Web 账号，自动生成 .env（SECRET_KEY/WEB_PASS 随机）、写回 compose、装 custosctl
# 或 sudo sh install.sh -y        （非交互，全默认 + 随机口令）
# 或 sudo sh install.sh --with-db （零外部依赖：内置 mgmt-mysql 一键起管理端，演示/小规模推荐；管理库地址自动填 mgmt-mysql、端口固化 3306）
```

启动后访问 `http://<管理端IP>:28888`（账号密码为 install.sh 生成，见其输出）。

> **命令入口区分（重要，新手最容易混）**：本工具有「两套命令」，切勿混用——
> - **管理端命令**（在管理端机器、仓库根目录 `custos-ha/` 执行）：`install.sh`（安装）、`custosctl`（重建/重启/卸载管理器）、`./scripts/deploy.sh`（部署）。
> - **节点命令**（在每台节点机器、由 Web 控制台「一键生成节点部署文件」产出的节点目录内执行）：`Install-*.sh`（安装节点）、`Uninstall-*.sh`（卸载）、`docker compose up -d`（拉起节点容器）。
> 已有部署想重建/重启管理器：`custosctl rebuild`（= 重建管理器镜像）或 `custosctl restart`。
> 改 `./app` 源码后必须 `--build` 重建镜像才生效：`custosctl rebuild` 或 `./scripts/deploy.sh up -d --build`。

### 1.3 手动部署服务端（等价步骤）

```bash
cd custos-ha
cp env.example .env            # 改 MGMT_MYSQL_* / WEB_PASS / SECRET_KEY / VIP 等
# 把参数写回 docker-compose.yml（含取消注释 VIP）
./scripts/deploy.sh            # = docker compose up -d --build
```

服务端环境变量（写在 `.env`，由 compose `environment` 透传；改后重启管理器生效）：

| 变量 | 说明 | 默认 |
|------|------|------|
| `MGMT_MYSQL_HOST` | 管理库可达 IP（**务必填外部库真实 IP，不要写 auto / 127.0.0.1**；用 `install.sh --with-db` 时自动填 `mgmt-mysql` 服务名） | `192.0.2.88` |
| `MGMT_MYSQL_PORT` / `USER` / `PASS` / `DB` | 管理库连接 | `3306` / `ha` / `ha` / `ha` |
| `WEB_PORT` | Web 端口 | `28888` |
| `WEB_USER` / `WEB_PASS` | Web 登录（默认 `admin`/`admin`，生产务必修改） | `admin` / `admin` |
| `SECRET_KEY` | 会话密钥（生产务必改） | `change-me-in-production` |
| `CHECK_INTERVAL` / `FAIL_THRESHOLD` | 探活间隔(秒) / 连续失败阈值（失联≈`间隔×阈值` 触发切换） | `3` / `2` |
| `VIP` / `VIP_PREFIX` / `INTERFACE` / `VRID` / `VIP_PORT` | VIP / 掩码位 / 网卡 / 虚拟路由ID / VIP 对外端口 | 空 / `24` / `eth0` / `51` / `3307` |
| `SEMI_SYNC` | 半同步开关：置 `"1"` 启用（主库提交等待从库 ACK） | `"1"` |
| `MANAGER_PUBLIC_IP` | 管理器向节点下发的地址（keepalived 上报/校验用）。**建议显式配置**，不要依赖自动探测 | 自动探测 |

> 也支持文件注入敏感信息：`MGMT_MYSQL_PASS_FILE`、`WEB_PASS_FILE`（容器内路径）。

### 1.4 添加被管节点（三种部署形态）

无论哪种形态，流程都是：**Web「集群管理」建集群 →「节点管理」添加节点（选 `node_type`）→ 生成自包含节点目录 → 拷到目标机执行部署命令**。

> **v2.3.9 节点包布局**：生成的 `generated_nodes/<ip>/` 内，配置/SQL 在 `mysql8.0/conf/`（MySQL）或 `mariadb12.3.3/conf/`（MariaDB），脚本在 `<db>/scripts/`，agent 在 `agent/`，keepalived 在根 `keepalived/`，裸机日志软链 `logs -> /data/custos-ha/logs/mariadb`。**存量节点须 regen + 重装才用新布局。**

#### 形态一：docker（容器化 MySQL 8.0，推荐新环境）
1. 「节点管理」选 `node_type=docker`，填节点 IP、MySQL **容器端口**（默认 3307）、root 密码、初始角色；
2. 生成目录含 `docker-compose.yaml`（MySQL + keepalived，host 网络 + `NET_RAW`）、`mysql8.0/conf/`（init.sql、ha-overrides.cnf）；
3. 整体拷到目标机 `/home/docker/<ip>/`，执行：
   ```bash
   cd /home/docker/<ip> && docker compose up -d
   ```
4. 数据目录：`/data/custos-ha/mysql8`。

#### 形态二：mariadb-docker（容器化 MariaDB 12.3.x，纯 compose）
1. 选 `node_type=mariadb-docker`，填节点 IP、**端口**（默认 3306）、root 密码、初始角色；
2. 生成目录含 `docker-compose.yaml`（`mariadb` + `keepalived` 双服务）、`mariadb12.3.3/conf/`（docker-my.cnf、init_mariadb.sql、fix-node-creds.sql）、`keepalived/`、`Uninstall-mariadb-docker-Node.sh`；
3. 拷到目标机后 `cd /home/docker/<ip> && docker compose up -d`；镜像 `mariadb:12.3.3`（首选）/ `12.3.2`（兜底）；数据目录 `/data/custos-ha/mariadb12`；
4. 卸载：`sudo bash Uninstall-mariadb-docker-Node.sh`。

#### 形态三：mariadb（裸机 MariaDB，仅 keepalived 跑容器）
适用于生产机已有 MariaDB，复用宿主机实例、只把 VIP/选主交给 keepalived 容器。
1. 选 `node_type=mariadb`，填节点 IP、端口、root 用户/密码、初始角色；
2. 生成目录含 `Install-ha-mariadb-Node.sh`（核心）、`mariadb12.3.3/conf/`（init_mariadb.sql、fix-node-creds.sql、my.cnf 片段）、`keepalived/`、`agent/`；
3. 拷到目标机后**一条命令**完成安装：
   ```bash
   cd /home/docker/<ip> && sudo bash Install-ha-mariadb-Node.sh
   ```
   脚本自动：追加 `my.cnf` HA 片段（幂等）→ 重启 MySQL 使 GTID 生效 → 以真实 root 跑 init/fix SQL → `docker compose up -d` 起 keepalived。
4. 卸载：`sudo bash Uninstall-mariadb-Node.sh`（还原 `my.cnf`、停 keepalived，**保留** MySQL 数据与账号）。

> **裸机装不上 tarball（glibc 过低）时直接选 `mariadb-docker`**，可与裸机节点同集群混部（同为 MariaDB GTID）。MySQL 与 MariaDB **禁止混部**。

### 1.5 管理端看门狗（可选，自愈拉起）

管理器本身无状态，但它一挂自动切换能力就丢（VIP/keepalived 在节点上，业务不直接中断）。`scripts/watchdog_manager.sh` 从**外部**每分钟探活，连续失败达阈值自动拉起。

```cron
* * * * * cd /opt/custos-ha && ./scripts/watchdog_manager.sh --apply >> logs/watchdog.log 2>&1
```

告警通道默认邮件（`CUSTOS_ALERT_MAILTO`），可叠加 webhook（`CUSTOS_ALERT_WEBHOOK` + `CUSTOS_ALERT_WEBHOOK_TYPE`）。详见脚本内环境变量说明。

### 1.6 节点 OS 指标 agent（可选）

想要 dashboard「节点 OS 资源」看到真实 CPU/内存/磁盘，在每台节点上跑轻量 agent（统一端口 **28889**）。**随节点 compose 自动部署**（生成单元已内置 `custos-node-agent` 服务），无需单独部署；仅需节点防火墙放行**管理器 IP** 的 28889。跨网段/公网务必在管理器 `NODE_AGENT_TOKEN` 与 agent 端设一致令牌。

---

## 二、Web 控制台各页面与按钮说明

> 通用：右上角「⟳ 刷新」只做并发探活 + 后台收敛；仪表盘每 10s 自动轮询。只读账号（viewer）的**任何写操作都会被服务端拒绝**，点按钮会报 403。

### 2.1 仪表盘（Dashboard）
- 集群健康卡片：VIP 状态、复制延迟/IO/SQL 线程、主库标记、漂移/待生效徽标。
- 「配置漂移一键修复」：卡片头检测到配置漂移时出现，点一下即修（同「观测中心 → 配置一致性」）。
- 节点操作列「重建」：见 2.3 说明。

### 2.2 集群管理（Clusters）
- **＋ 新增集群**：创建集群，填名称、VIP/掩码/网卡/VRID/VIP 端口、HA/复制/业务账号（密码留空自动生成）。
- **进入管理**：跳到集群详情。
- **配置/密码**：修改集群参数与账号密码（见 2.3 的「修改配置/密码」弹窗）。
- **维护模式**：开启后抑制自动切换与告警（见 2.6 系统设置「维护模式」概念）；再点「结束维护」恢复。
- **删除**：删除集群（同时清掉其节点部署单元引用，不动被管节点数据）。

### 2.3 集群详情（Cluster Detail）
- **▷ 数据库终端**：打开 SQL 终端抽屉。
- **⋯ 更多操作 ▾**：
  - **修改配置/密码**：同 2.2 弹窗；保存即「保存并同步」把账号密码下发到该集群所有已生成节点（节点 root 自愈）。
  - **修复复制延迟**（高延迟）/ **修复复制中断**（IO/SQL 报错）：手动触发自愈（`/api/heal`）。连点有 15s 冷却。
  - **行数比对**（轻量）/ **校验和比对**（精确·较慢）：主从一致性校验（只读）。
  - **同步账号凭证**：把集群库存的账号密码重新下发到节点（连接数打满/凭证漂移时用）。
  - **重新下发配置**：重推节点配置文件（支持自动）。
- **⇄ 手动切换主从**：把指定从库提为主（会触发 VIP 漂移）。**生产慎用**，确认不会脑裂。
- **⚙ 自动化 ▾**：切换/修复相关的自动开关总览（实际开关在「系统设置」）。
- 节点行的 **详情**：查看版本/参数/凭证、**查看/下载自愈 SQL**。
- 从库行的 **重建**：复制无法自愈时（主库 binlog 已清理 1236 / 数据冲突 1062·1032）从主库重做本从库数据。**重建期间该从库不可用，耗时随数据量增长**；带进度条，完成后自动刷新状态。
- 日志 Tab：**错误日志 / 慢日志 / 启动日志**（错误日志三级读取：表 → 文件 → 节点 agent）。

### 2.4 节点管理（Nodes）
- **＋ 添加节点**：同 1.4 流程，选 `node_type` 填信息生成目录。
- **批量重新生成 / 批量下载修复SQL / 批量删除**：勾选多节点批量操作。
- 单节点：**重新生成**（regen，刷新部署文件并产出「待生效操作」）、**查看/下载自愈 SQL**（导出的 `fix-node-creds.sql` 含真实凭证，仅限节点本机以真实 root 执行）、**删除节点**。
- **配置待生效 / 待生效操作**（粉色徽标）：点开看「需要在节点执行的操作」及可直接复制的命令（分 recreate 重建容器 / restart 重启服务 / exec 节点执行一次 / manual 人工确认）。在节点执行完，点 **「已在节点执行，清除提示」**；若确认已生效但核验不满足，点 **「我确认已在节点生效，仍然清除」**（force）。
  - ⚠ 徽标不会自己消失；未落实的变更持续保留（v2.3.7 起已落实的会自动核验消失）。

### 2.5 数据迁移向导（Migration）
- **录入源**：填源库地址 / 端口 / 账号密码，选要搬的库。
- **预检（dry-run）**：只读探测 + 结构试运行，提前报出必炸项（如 ngram 全文索引）。**建议每次迁移前必跑**。
- **执行迁移**：后台异步执行，页面显示进度 / 阶段文字 / 实时日志（刷新页面自动接管）。
- **停桥切换（cutover）**：仅同引擎桥接场景有，需人工确认后把业务切到目标端。
- **收尾回收（cleanup）**：清理中间产物。
- **历史迁移**：查看以往迁移任务与结果。
- ⚠ 迁移**只搬数据，不搬账号权限**；**MySQL 8.0 → MariaDB 明确拒绝**。

### 2.6 备份与恢复（Backup）
- **⤓ 立即备份**：按设定周期/范围执行一次备份。
- **执行恢复**：用选中的备份文件恢复（弹窗可选恢复库范围）；**后台运行** 转后台任务并出进度条（进度条/阶段/已用时长，刷新页面自动接管）。
- 备份文件列表：**恢复 / 下载 / 预览 / 删除**（删除不可恢复）。
- Tab：**备份文件 / 备份记录 / 恢复记录 / 操作日志**（操作日志按 `id=集群` 边界匹配，可追溯每次备份/恢复成败）。

### 2.7 系统设置（Settings）⚠ 按钮使用注意事项

本页是**全局**自动运维开关与阈值（对所有集群生效），危险项默认关闭。改完即时写入并记审计。

- **保存设置**（管理器区块）：保存「管理器内网 IP」+「监控轮询间隔」。
  - ⚠ 若 `docker-compose.yml` 已设 `MANAGER_PUBLIC_IP` 环境变量，则「管理器内网 IP」**只读、页面填写不生效**（页面会红字提示），以环境变量为准。
- **保存高级设置**（高级切换/监控区块）：保存下列所有开关与阈值。
  - **自动切换（主库失联）**：总开关，默认开。**它同时是「延迟切换 / 压力切换」的总开关——关闭时那两项一律不生效**（静默）。
  - **⚠ 延迟切换**：多数从库复制延迟持续 ≥ 阈值秒、且连续达指定周期数时，自动把延迟最小的从库提为主。**开启前确认理解触发条件，避免误切。**
  - **⚠ 压力切换**：主库压力（Threads_running ≥ 阈值 或 行锁等待 ≥ 阈值）持续超阈且连续 3 周期时，自动把压力最低的从库提为主。
  - **自动修复**：高延迟从库自动摘流量并触发修复；账号/配置漂移在轮询中自动校正。
  - **☠ 允许跳过数据冲突**：复制报 1062/1032/1146 意味着**该节点数据已与主库不一致**。开启后自愈跳过这些事务让复制继续，但**分歧被永久保留**，切主时才暴露数据丢失。默认关闭 → 遇到时拒绝自愈并告警，由人工核对或重建从库。
  - **切换前交叉验证（防脑裂）**：管理器连不上旧主时，要求集群内**其它节点作证**主库确实不可达才允许切换；可规避管理器侧网络分区导致的误切与脑裂双写。
  - **binlog 占用告警阈值**：仅**告警**，与下方自动清理互不影响。
  - **⚠ binlog 自动清理**：开启后每个轮询周期检查，仅当**所有从库都已追平**才清理超过保留时长的 binlog。**保留时长**仅在本项开启且从库全追平时生效。
  - **切换冷却时间**：两次切换最小间隔，避免反复切换。
- **服务端容器日志**区块：**读取日志**（实时追加）、**导出TXT（24h）**、**全屏**、**清空**。
  - ⚠「清空」只清空**页面显示**，不删实际容器日志。
- **用户与角色**区块：**新增用户**（用户名+密码+角色 admin/viewer）、角色下拉切换、**删除**（带确认）。
  - ⚠ viewer（只读）账号任何写操作都会被服务端拒绝（统一拦截），排障先看日志是不是 403。

### 2.8 告警（Alerts）/ 审计（Audit）
- 告警：SMTP、告警规则、收件人按集群路由；实时告警区分「已自愈(绿)/未恢复(红)」。
- 审计：操作日志（导航暂无入口，直接访问 `/audit`；字段 时间/用户/操作/对象/详情）。

---

## 三、故障排查（从哪个页面点哪个按钮）

| 现象 | 先去哪个页面 | 点哪个按钮 / 做什么 |
|------|------|------|
| 节点初始化账号/权限失败 | 节点管理 | 该节点「查看/下载自愈 SQL」→ 在节点本机以真实 root 跑 `fix-node-creds.sql` |
| 复制中断 / IO·SQL 报错 | 集群详情 → ⋯ 更多操作 | **修复复制中断**（heal_sql）；或节点管理「查看/下载自愈 SQL」 |
| 复制高延迟 | 集群详情 → ⋯ 更多操作 | **修复复制延迟**（heal_io）；或开启系统设置「自动修复」 |
| 复制报 1236（主库 binlog 已清理）| 集群详情 → 从库行 | **重建**（从主库重做该从库数据）|
| 配置改了没生效 | 节点管理 | 点「配置待生效 / 待生效操作」看命令 → 节点执行 →「已在节点执行，清除提示」|
| 主库失联却不切换 | 系统设置 | 确认「自动切换（主库失联）」已开；若开「切换前交叉验证」，确认其它节点能作证 |
| 半同步没生效 | 系统设置 + 集群详情 | 确认 `SEMI_SYNC=1`；MySQL 节点重装后约 60s 内才运行期开启；用集群详情/ agent 看 `Rpl_semi_sync_*_status` |
| 错误/启动日志为空 | 集群详情 → 日志 Tab | MariaDB 需节点 agent 在线（重启 `custos-node-agent`）；确认节点防火墙放行管理器 28889 |
| VIP 未配置导致无法建节点 | 集群管理 → 配置/密码 | 填节点同网段空闲 VIP（或 `docker-compose.yml` 的 `VIP`）|
| 节点监控账号错/复制失败 | 集群管理 → 配置/密码 | **保存并同步**（重新下发账号密码）|
| 只读账号点按钮报 403 | 系统设置 → 用户与角色 | 改用 admin 账号登录 |
| 反复点修复却没效果 | 节点管理 / 系统设置 | 看「待生效操作」原因；确认不是「首次部署」未真正装完（v2.3.8 起首次部署装完 60s 自动消失）|

**常见坑**：
- **Q：keepalived 报 `Cant send advert ... Operation not permitted`** → 节点容器缺 `NET_RAW` 能力（见 1.1）。
- **Q：keepalived 日志 `interface eth0 doesn't exist`** → 网卡名不同；生成单元 `entrypoint.sh` 会按 `unicast_src_ip` 自动探测真实网卡，仍失败则手填 `keepalived.conf`。
- **Q：刷新卡顿 / 仪表盘不更新** → 源码改动后确认已 `--build` 重建管理器镜像；部署为单 worker（勿加 `--workers`）。
- **Q：MariaDB 错误日志面板恒空** → mysqld 生成的 error.log 是 0660，`LOAD_FILE` 恒返回 NULL；必须靠节点 agent `/logs` 兜底，确认 `custos-node-agent` 在线。

---

## 四、安全提示

- 节点部署资源统一来自 `app/node_assets/`，生成的 `generated_nodes/<ip>/` 内含明文密码（含 `ha_check.conf`、`init_mariadb.sql`、`fix-node-creds.sql`），仅限目标节点本机使用；生产请使用强密码，妥善保管节点目录，勿提交到版本库（`generated_nodes/` 已 gitignore）。
- 管理端 `SECRET_KEY`、`WEB_PASS` 等务必在生产环境修改（默认 `admin`/`admin`）。
- 凭证已在管理库以密文（`ENC1:`）存储，密钥由 `SECRET_KEY` 派生（可接 `CRED_KEY` / `data/cred.key`）；**备份项目 `data/` 目录即可完整恢复凭证**。旋转 `SECRET_KEY` 须重填存量密文。
- `data/` 已 gitignore，不会随仓库提交。

## 五、数据库版本支持矩阵

主线 **MariaDB 12.3.3**（裸机 tarball + 官方容器镜像），MySQL 侧**冻结**（只修 bug、不做新功能）。

| 系列 | 级别 | 说明 |
|------|------|------|
| MariaDB **12.3** | **主线** | 推荐。容器 `mariadb:12.3.3`；裸机 `mariadb-12.3.3-linux-systemd-x86_64.tar.gz` |
| MariaDB 12.0 / 12.1 / 12.2 | 支持 | 建议升级至 12.3 主线 |
| MariaDB 11.4 / 11.8 | 支持 | LTS，建议升级至 12.3 主线 |
| MariaDB 10.6 / 10.11 | 兼容 | 可纳管，但不再主动回归验证（预检 WARN） |
| MariaDB < 10.6 | **不支持** | 预检 ERR |
| MySQL 8.0.26 ~ 8.0.46 | 支持（冻结） | 仅修 bug；**禁止与 MariaDB 同集群混部** |

- 节点真实版本由管理器连库 `SELECT VERSION()` 探测并回写 `nodes.mysql_version`。
- 特权按 `SHOW PRIVILEGES` 能力探测，不按版本号硬编码；授权唯一来源 `app/mariadb_matrix.py`。
- 半同步变量名：MariaDB 全系列沿用 `rpl_semi_sync_master/slave`（11.5+ 的 `source/replica` 只是别名），**勿照 MySQL 改**，否则 10.6~11.4 启动即崩。
