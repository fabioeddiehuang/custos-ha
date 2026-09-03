# Custos

> 当前版本：**v2.2.17**
> 版本定位与变更记录（含路线图）见 [`RELEASE_NOTES.md`](./RELEASE_NOTES.md)。
> 仓库目录名与产品名统一为 **`custos-ha` / Custos**（v2.2.0 起完成目录改名）；下文命令路径以仓库根目录 `custos-ha/` 为准。

基于 **Keepalived + MySQL 主从（GTID）** 的 MySQL 高可用方案，附带一个轻量管理器：
- **决策器（monitor）**：探活主从、按策略自动选主、控制 VIP 漂移与脑裂保护；
- **Web 控制台**：多集群管理、查看拓扑 / 切换历史 / 自动发现节点、一键生成节点部署文件；
- **节点一键部署**：在 Web 上填写节点信息后，自动生成可直接 `docker compose up -d` 的节点目录。

---

## 功能清单

| 能力 | 说明 |
|------|------|
| 多集群隔离管理 | 每个集群独立 HA 监控账号 / 复制账号 / 业务账号、VIP、网卡、VRID；切换与监控按 `cluster_id` 隔离。 |
| 自动高可用与故障切换 | 探活主从、GTID 感知选主（`pick_best_master`）、VIP 漂移、脑裂保护；主库失联约 6s 触发切换（默认 `CHECK_INTERVAL=3` / `FAIL_THRESHOLD=2`）。 |
| 复制自愈 | 在线修复 SQL（1007/1050/1061/1062/1396/1032/1146）与 IO（13114/1236）复制错误（注入空事务跳过冲突）；支持手动 `/api/heal` 重指向 + 自愈。 |
| 节点一键部署 | **三种形态**：① Docker MySQL 8.0（`Install-mysql-docker-Node.sh` + compose）；② Docker MariaDB 12.3.x（生成即**纯 compose**，目录内 `docker compose up -d` 一键起 MariaDB + keepalived）；③ 裸机 MariaDB 12.x（`Install-ha-mariadb-Node.sh` 一条命令完成安装+账号初始化+keepalived 启动，**该包只适配裸机**）。**裸机装不上 tarball（如 glibc 过低）时直接选②容器版**，可与③同集群混部。账号/GTID 统一初始化，首次安装即可部署成功。 |
| Web 控制台 | 仪表盘（实时拓扑 / 统计 / 刷新）、集群管理（单色系卡片 + SVG 图标 + 按集群配置凭证与 VIP）、节点配置（生成 / 重新生成 / 删除）、系统配置（SMTP 多收件人告警 + 按集群路由）。 |
| 切换历史与告警 | 记录每次切换（from→to + 原因）；支持邮件告警与异常日志一键复制排错。 |
| 安全 | 账号密码可自动生成、明文不回显；「同步密码到节点」用 root 自愈全部节点；`SECRET_KEY` / `WEB_PASS` 生产须改。 |

---

## 1. 整体架构

```
                  ┌─────────────── 管理端（部署 Custos 的机器）────────────────┐
                  │  Custos（Web + 决策器 + 节点生成）                          │
                  │        │ 读取/写入拓扑、切换历史、账号                              │
                  │        ▼                                                            │
                  │  管理 MySQL（独立的外部库，存配置/拓扑/切换历史/账号）              │
                  └───────────────────────────────┬───────────────────────────────────┘
                                                  │ 管理库直连（防脑裂的仲裁来源）
        ┌─────────────────────────────────────────┼─────────────────────────────────────────┐
        ▼                                         │                                         ▼
┌─────────────── 节点 A ───────────────┐          │          ┌─────────────── 节点 B ───────────────┐
│  mysql（端口 3307，GTID 主从）        │          │          │  mysql（端口 3307，GTID 主从）        │
│  keepalived（vrrp，探测主/切 VIP）    │◄─ unicast ┼─ unicast►│  keepalived（vrrp，探测主/切 VIP）    │
│  VIP 192.0.2.10/24（漂移）          │          │          │  VIP 192.0.2.10/24（漂移）          │
└──────────────────────────────────────┘          │          └──────────────────────────────────────┘
                                                  │
                                 业务连接统一走 VIP:3307（VIP 端口 = 节点 MySQL 端口，默认 3307，可配）
```

- **业务流量**：应用只连 `VIP:3307`，VIP 由 keepalived 漂移到当前主节点（keepalived 使用 host 网络，VIP 即绑定在节点主机的该端口上）。
- **主从复制**：两节点互为 GTID 主从，`replication` 账号复制。
- **选主 / 防脑裂**：keepalived 脚本读本地 MySQL 只读状态 + 管理库 `current_master` 共同判断是否升主；管理器也会按策略自动切换。
- **数据目录**：一键生成的节点部署单元统一落在 `custos-ha/generated_nodes/<ip>/`（自包含，可直接拷到目标主机运行）；服务端可选演示库数据落在 `custos-ha/data/`。

---

## 2. 目录结构

```
custos-ha/
├── docker-compose.yml          # 服务端（管理器）编排
├── Dockerfile                  # 服务端镜像
├── LICENSE                     # 开源许可证（MIT，版权 eddys.cn）
├── app/                        # 管理器源码（Web + 决策器 + 节点生成）
│   ├── config.py               # 配置项（环境变量）
│   ├── db.py / init_db.py      # 管理库连接与表结构初始化（含多集群迁移）
│   ├── monitor.py              # 决策器（探活/选主/切换/自愈，后台常驻线程）
│   ├── failover.py             # 故障切换与拓扑收敛
│   ├── ha_client.py            # MySQL 节点探活客户端
│   ├── clusters.py             # 集群 CRUD / 凭证 / 节点查询
│   ├── node_gen.py             # 一键生成节点 compose（含模板渲染）
│   ├── api.py                  # Web 路由与 JSON 接口
│   ├── alerts.py               # 邮件告警
│   ├── state.py                # 运行时状态缓存
│   ├── VERSION                 # 版本号
│   ├── templates/              # Web 页面（base/login/dashboard/clusters/nodes/settings/change_password）
│   ├── static/                 # 样式与脚本
│   └── node_assets/            # 节点部署资源唯一来源（Web 生成节点目录时从这里渲染/复制）
│       ├── keepalived/         # keepalived 资源（keepalived.conf / ha_check.conf / check_mysql_master.sh / entrypoint.sh / notify 脚本）— keepalived 镜像已统一命名为 `custos-node`
│       ├── Install-mysql-docker-Node.sh       # 节点一键安装脚本（Docker MySQL 8.0）
│       ├── Install-ha-mariadb-Node.sh     # 节点一键安装脚本（裸机：自动应用 my.cnf 片段 + 初始化账号 + 起 keepalived）
│       ├── Uninstall-mariadb-Node.sh   # 节点一键卸载脚本
│       ├── Uninstall-mariadb-docker-Node.sh  # 容器化 MariaDB 卸载脚本（v2.2.13 起，自动识别纯 compose / 安装脚本两种来源）
│       ├── init_mariadb.sql  # 裸机节点初始化 SQL（建账号 / 授权 / 启 GTID / 本地状态库）
│       └── fix-node-creds.sql  # 节点账号/权限修复 SQL（节点本机真实 root 执行，校正漂移凭证）
├── app/mysql_compat.py        # 按 mysql_version 选择复制语法（MySQL8.0 CHANGE MASTER TO ↔ MariaDB CHANGE REPLICATION SOURCE TO）
├── generated_nodes/            # 一键生成的节点部署单元（已 gitignore，内容含敏感信息）
│   └── <ip>/                   # 自包含：Install-ha-mariadb-Node.sh / init_mariadb.sql / fix-node-creds.sql / docker-compose.yaml / keepalived/ / my.cnf.ha-fragment / ha_check.conf
└── data/                       # 运行时产物（已 gitignore）
    └── mysql-data/             # （可选）服务端自带演示管理库数据
```

> **部署资源唯一来源**：所有节点侧文件（keepalived 配置、安装/卸载脚本、初始化 SQL）统一存放在 `app/node_assets/`，由 `node_gen.py` 在 Web 生成节点时渲染并写入 `generated_nodes/<ip>/`。历史遗留的 `node-template/`、`host-keepalived/` 目录已废弃删除，请勿再引用。
> 生成的节点目录自包含，整体拷到目标主机 `/home/docker/<ip>/` 即可运行，**不会越级引用上级目录**。

---

## 3. 前置条件

- 管理端机器：Linux + Docker + docker compose v2；
- 每台节点机器：Linux + Docker + docker compose v2，且**节点机器到管理库 3306 端口网络可达**；
- 一个**独立的外部管理 MySQL**（推荐），用户/密码/库名统一约定为 `ha`（也可自定义）；
- 一个与节点同网段、**当前空闲**的 VIP（如 `192.0.2.10`），本工具无法自动探测，必须填写。
- 节点机器的 Docker 必须允许给容器附加 `NET_ADMIN` / `NET_BROADCAST` / **`NET_RAW`** 能力（`keepalived` 用原始套接字发 VRRP 通告，缺 `NET_RAW` 会报 `Cant send advert ... Operation not permitted` 并导致双主/split-brain）。节点 `docker-compose.yaml`（由 `app/node_assets` 渲染）已显式 `cap_add: - NET_RAW`。

---

## 4. 部署服务端（管理器）

```bash
cd custos-ha
./deploy.sh                     # 等价于 docker compose up -d --build（自动同步 app/VERSION 到 .env）
```

启动后访问：`http://<管理端IP>:28888`（默认账号 `admin` / `admin`，生产请改 `WEB_PASS`）。

> 源码改动必须 `--build` 重建镜像才生效（旧镜像跑旧代码会复现 500 / 不刷新等问题）。

配置通过环境变量（改 `docker-compose.yml` 的 `environment` 段，或新建 `.env`）：

| 变量 | 说明 | 默认 |
|------|------|------|
| `MGMT_MYSQL_HOST` | 管理库可达 IP（**务必填外部库真实 IP，不要写 auto / 127.0.0.1**） | `192.0.2.88` |
| `MGMT_MYSQL_PORT` / `USER` / `PASS` / `DB` | 管理库连接 | `3306` / `ha` / `ha` / `ha` |
| `WEB_PORT` | Web 端口 | `28888` |
| `WEB_USER` / `WEB_PASS` | Web 登录（默认 `admin`/`admin`，生产务必修改） | `admin` / `admin` |
| `SECRET_KEY` | 会话密钥（生产务必改） | `change-me-in-production` |
| `CHECK_INTERVAL` / `FAIL_THRESHOLD` | 探活间隔(秒) / 连续失败阈值 | `3` / `2` |
| `PROBE_TIMEOUT` | 单节点探活超时(秒) | `3` |
| `NODES_DIR` | 容器内节点生成目录 | `/app/generated_nodes` |
| `NODES_HOST_BASE` | 节点目录在宿主机的**显示路径**（Web 展示用） | `custos-ha/generated_nodes` |
| `VIP` / `VIP_PREFIX` / `INTERFACE` / `VRID` / `VIP_PORT` | VIP / 掩码位 / 网卡 / 虚拟路由ID / VIP 对外访问端口 | 空 / `24` / `eth0` / `51` / `3307` |
| `SMTP_*` / `ALERT_EMAIL` | 切换告警邮件（可选） | — |

> 也支持通过文件注入敏感信息：`MGMT_MYSQL_PASS_FILE`、`WEB_PASS_FILE`（容器内路径）。

### 可选：自带演示用管理库
取消 `docker-compose.yml` 中 `mgmt-mysql` 整段注释，并取消 `custos` 的 `depends_on` 注释；数据落在 `./data/mysql-data`。生产推荐使用你已有的外部管理库。

---

## 5. Web 控制台配置

登录后，主要配置在两个页面完成：

1. **集群管理（核心）**：在「集群管理」页为每个集群配置独立参数 —— 集群名称、VIP 地址 / 掩码位、网卡、VRID、**VIP 端口**，以及 HA 监控账号、复制账号、业务账号（密码留空则自动生成）。保存集群时会自动刷新该集群各节点部署文件（含 notify 脚本与 docker-compose 的 VIP 端口映射）。点击「同步密码到节点」可把账号密码下发到该集群所有已生成节点（节点 root 自愈）。
2. **系统配置**：在「系统配置」页配置 SMTP 邮件告警服务器，并维护提醒邮箱（可设默认接收全部集群，或按集群绑定路由）。

> 管理库（管理 MySQL）连接**不再通过 Web 表单设置**，而是通过服务端环境变量 `MGMT_MYSQL_*` 配置（见第 4 节）。

---

## 6. 添加被管节点（三种部署形态）

无论哪种形态，流程都是：Web「集群管理」建集群 →「节点管理」添加节点（选 `node_type`）→ 生成自包含节点目录 → 拷到目标机执行部署命令。

### 6.1 形态一：docker（容器化 MySQL 8.0，推荐新环境）

1. 「节点配置」选 `node_type=docker`，填：节点 IP、MySQL **容器端口**（默认 3307，避免与宿主机 3306 冲突）、root 密码、初始角色；
2. 生成目录 `generated_nodes/<ip>/`（含 `docker-compose.yaml`：MySQL + keepalived，host 网络 + `NET_RAW`；以及 `init.sql`、`keepalived/`、`MySQL-data` 数据卷）；
3. 整体拷到目标机 `/home/docker/<ip>/`，执行：

   ```bash
   cd /home/docker/<ip>
   docker compose up -d
   ```

4. 节点启动后由 `init.sql` 初始化账号并启 GTID；管理端自动发现并纳入拓扑。
5. 数据目录：`/data/custos-ha/mysql8`。

> 该目录自包含，路径均相对本目录，拷贝后无需改动即可运行。

### 6.2 形态二：mariadb-docker（容器化 MariaDB 12.3.x，纯 compose）

**v2.2.13 起改为纯 compose 部署**：生成目录**不再包含安装脚本**，入口即 `docker compose up -d`。

1. 选 `node_type=mariadb-docker`，填：节点 IP、**端口**（默认 3306）、root 密码、初始角色；
2. 生成目录含：`docker-compose.yaml`（`mariadb` + `keepalived` 双服务）、`docker-my.cnf`、`init_mariadb.sql`、`fix-node-creds.sql`、`keepalived/`、`Uninstall-mariadb-docker-Node.sh`；
3. 拷到目标机后执行：

   ```bash
   cd /home/docker/<ip>
   docker compose up -d
   ```

4. 镜像：Debian 构建 `mariadb:12.3.3`（首选），老 CPU / 拉取失败自动回退 `mariadb:12.3.2`；**已弃用 UBI 构建**。
5. 数据目录：`/data/custos-ha/mariadb12`；`innodb_buffer_pool_size` 默认 512M（低内存机友好）。
6. VIP 端口在 compose 层映射，业务连 `VIP:VIP_PORT` 直连容器，**不依赖节点 iptables/firewalld 转发**。
7. 卸载：`sudo bash Uninstall-mariadb-docker-Node.sh`。

> **裸机包只适配裸机**：裸机（`mariadb`）节点的生成目录**不再交付**容器回退脚本 `Install-mariadb-docker-Node.sh`（该脚本已从 `app/node_assets` 移除）。若目标机装不上 MariaDB tarball（如 CentOS 7 / glibc 2.17），**创建节点时直接选 `mariadb-docker`**（官方镜像，与宿主 glibc 无关），可与裸机节点**同集群混部**（同为 MariaDB GTID 复制路径）。

### 6.3 形态三：mariadb（裸机 MariaDB，仅 keepalived 跑容器）

适用于生产机已有 MariaDB，希望复用宿主机实例、只把 VIP/选主交给 keepalived 容器的场景。

1. 选 `node_type=mariadb`，填：节点 IP、端口（如 3306）、root 用户/密码、初始角色；
2. 生成目录 `generated_nodes/<ip>/`，内含：
   - `Install-ha-mariadb-Node.sh` —— 一键安装脚本（核心）；
   - `init_mariadb.sql` —— 初始化账号/授权/启 GTID/建本地状态库；
   - `fix-node-creds.sql` —— 账号/权限修复脚本（节点本机真实 root 执行）；
   - `my.cnf.ha-fragment` —— 需追加到宿主机 `my.cnf` 的 GTID/复制片段；
   - `docker-compose.yaml`（仅 keepalived 容器）、`keepalived/`、`ha_check.conf`；
3. 拷到目标机后**一条命令**完成安装：

   ```bash
   cd /home/docker/<ip>
   sudo bash Install-ha-mariadb-Node.sh
   ```

   脚本自动完成：读 `ha_check.conf` → 追加 `my.cnf` HA 片段（幂等，带 `HA-MYSQL-NODE-SETUP` 标记）→ 重启 MySQL 使 GTID 生效 → 以真实 root 跑 `init_mariadb.sql`（建 `ha_monitor`/`replication`/`app`，强制 `mysql_native_password`）→ 跑 `fix-node-creds.sql` → `docker compose up -d` 起 keepalived。
4. 卸载：`sudo bash Uninstall-mariadb-Node.sh`（还原 `my.cnf`、停 keepalived、保留 MySQL 数据与账号）。

### 6.4 账号与加密说明（重要）

- 所有服务账号（`ha_monitor` / `replication` / `app`）统一强制 `mysql_native_password`，以兼容 MySQL 8.0 默认 `caching_sha2_password` 在**非 TLS 复制**下报 `Authentication requires secure connection` 的问题。
- 管理端每个周期对在线节点执行自愈（`ensure_ha_creds`）：优先 `ha_monitor` 校正账号，若 `ha_monitor` 缺 `CREATE/ALTER USER` 权限则回退 `root` 重做——确保复制账号密码与各节点始终与集群库存一致。
- 节点「下载修复SQL」按钮导出的 `fix-node-creds.sql` 含真实凭证，仅限节点本机以真实 root 执行。

---

## 7. 数据目录约定

| 路径 | 用途 |
|------|------|
| `generated_nodes/<ip>/` | 一键生成的节点部署单元（自包含，整体拷到目标主机 `/home/docker/<ip>/` 运行）。Docker 模式含 `./MySQL-data` 数据卷；裸机模式仅含 keepalived 容器与配置，MySQL 数据仍在宿主机 |
| `data/mysql-data/` | （可选）服务端自带演示管理库数据 |

`generated_nodes/` 与 `data/` 均已 gitignore，避免提交明文密码与业务数据。

---

## 8. 高可用验证

1. 在两节点均启动后，确认 VIP 已落在某一节点：`ip addr show | grep <VIP>`；
2. 应用连接 `VIP:3307` 读写正常；
3. 模拟主节点故障（停掉主节点 MySQL 或 keepalived），观察 VIP 漂移到对端、对端升主；
4. 故障恢复后，原主以从库身份重新加入（GTID 自动补齐）；
5. Web「切换历史」「集群管理」可查看过程；如配置了邮件会收到告警。

---

## 9. 常见问题 / 排错

**Q1. 节点初始化账号/权限失败**
- Docker 模式：`init.sql` 以 `:ro` 挂载进容器，由容器内 `mysql` 用户读取；若宿主机文件权限过严（`0600`）会报 `Permission denied`。处理：确保 `init.sql` 权限 `0644`（一键生成已默认 `0644`）。
- 裸机模式：`init_mariadb.sql` / `fix-node-creds.sql` 由安装脚本以**真实 root** 通过 `mysql` 客户端执行，若出现 `Access denied`，请确认以节点真实 root 密码运行 `sudo bash Install-ha-mariadb-Node.sh` 或 `mysql -uroot -p < fix-node-creds.sql`。

**Q2. keepalived 报 `sed: cannot rename ... Device or resource busy`**
- 原因：`keepalived.conf` 以只读 `:ro` bind 挂载进容器，旧 `entrypoint.sh` 用 `sed -i` 就地改名会失败。
- 处理：新版 `entrypoint.sh` 改为先 `cp` 到可写目录 `/run` 再修改，并以 `-f /run/keepalived.conf` 运行，**不会改动宿主机配置文件**。升级镜像后重跑即可。

**Q3. keepalived 日志 `interface eth0 doesn't exist`**
- 原因：不同主机网卡名不同（如 `ens33`/`eth1`）。
- 处理：`entrypoint.sh` 会根据 `unicast_src_ip` 自动探测真实网卡并替换后启动（日志可见「自动改用 xx」）。仍失败则检查 `unicast_src_ip` 是否为本机 IP，或手动在 `keepalived.conf` 写对网卡名。

**Q4. 添加节点时报「VIP 未配置」**
- 在「集群管理」对应集群的 VIP 中填写节点同网段空闲 IP，或 `docker-compose.yml` 的 `VIP` 中填写。

**Q5. 节点监控显示账号错误 / 复制失败**
- 核对节点侧 `ha_check.conf` / `init.sql` 中的 `ha_monitor`、`replication` 密码，与「集群管理」中该集群设置的完全一致；可点「同步密码到节点」重新下发。

**Q6. 刷新按钮卡顿 / 仪表盘不更新**
- 刷新按钮只做并发探活 + 后台异步收敛，约单超时返回；仪表盘每 10s 轮询。若源码改动未生效，确认已 `docker compose up -d --build custos` 重建镜像，且部署为单 worker（勿加 `--workers`）。

---

## 10. 安全提示

- 节点部署资源统一来自 `app/node_assets/`，生成的 `generated_nodes/<ip>/` 内含明文密码（含 `ha_check.conf`、`init_mariadb.sql`、`fix-node-creds.sql`），仅限目标节点本机使用；生产请使用强密码，并妥善保管节点目录，勿提交到版本库（`generated_nodes/` 已 gitignore）。
- 管理端 `SECRET_KEY`、`WEB_PASS` 等务必在生产环境修改（默认 `admin`/`admin`）。
- `data/` 已 gitignore，不会随仓库提交。
