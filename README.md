# MySQL HA Manager

基于 **Keepalived + MySQL 主从（GTID）** 的 MySQL 高可用方案，附带一个轻量管理器：
- **决策器（monitor）**：探活主从、按策略自动选主、控制 VIP 漂移与脑裂保护；
- **Web 控制台**：配置管理库、查看拓扑 / 切换历史 / 自动发现节点、一键生成节点部署文件；
- **节点一键部署**：在 Web 上填写节点信息后，自动生成可直接 `docker compose up -d` 的节点目录。

> 项目中「项目根目录」指 `mysql-ha-manager/`（即本目录）。

---

## 1. 整体架构

```
                  ┌─────────────── 管理端（部署 mysql-ha-manager 的机器）───────────────┐
                  │  mysql-ha-manager（Web + 决策器 + 节点生成）                        │
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
│  VIP 192.168.1.10/24（漂移）          │          │          │  VIP 192.168.1.10/24（漂移）          │
└──────────────────────────────────────┘          │          └──────────────────────────────────────┘
                                                  │
                                      业务连接统一走 VIP:3306
```

- **业务流量**：应用只连 `VIP:3306`，VIP 由 keepalived 漂移到当前主节点。
- **主从复制**：两节点互为 GTID 主从，`repl` 账号复制。
- **选主 / 防脑裂**：keepalived 脚本读本地 MySQL 只读状态 + 管理库 `current_master` 共同判断是否升主；管理器也会按策略自动切换。
- **数据目录**：一键生成的节点部署单元统一落在 `mysql-ha-manager/generated_nodes/<ip>/`（自包含，可直接拷到目标主机运行）；服务端可选演示库数据落在 `mysql-ha-manager/data/`（见第 7 节）。

---

## 2. 目录结构

```
mysql-ha-manager/
├── docker-compose.yml          # 服务端（管理器）编排
├── Dockerfile                  # 服务端镜像
├── app/                        # 管理器源码（Web + 决策器 + 节点生成）
│   ├── config.py               # 配置项（环境变量）
│   ├── node_gen.py             # 一键生成节点 compose（含模板渲染）
│   ├── monitor.py              # 决策器
│   ├── db.py                   # 管理库初始化
│   ├── templates/              # Web 页面
│   └── node_assets/keepalived/ # 节点 keepalived 资源（生成时被复制到节点目录）
├── node-template/              # 手动部署模板（容器方式，开箱即用的示例配置已附）
│   ├── docker-compose.yml      # 节点编排（MySQL + keepalived）
│   ├── .env                    # 节点环境变量示例
│   └── keepalived/
│       ├── keepalived.conf     # ✅ 可直接使用（示例值）
│       ├── ha_check.conf       # ✅ 可直接使用（示例值）
│       ├── init.sql            # ✅ 可直接使用（示例值，首启初始化账号）
│       ├── check_mysql_master.sh
│       ├── entrypoint.sh
│       ├── Dockerfile
│       ├── keepalived.conf.template     # 模板（如需重新渲染可用）
│       ├── ha_check.conf.template       # 模板
│       └── init.sql.template            # 模板
├── host-keepalived/            # 宿主机直装方式模板（开箱即用的示例配置已附）
│   ├── keepalived.conf         # ✅ 可直接使用（示例值）
│   ├── ha_check.conf           # ✅ 可直接使用（示例值）
│   ├── check_mysql_master.sh
│   ├── keepalived.conf.template
│   └── ha_check.conf.template
├── generated_nodes/            # 一键生成的节点部署单元（已 gitignore，内容含敏感信息）
│   └── <ip>/                   # 自包含：docker-compose.yaml / init.sql / keepalived/ / MySQL-data
└── data/                       # 运行时产物（已 gitignore）
    └── mysql-data/             # （可选）服务端自带演示管理库数据
```

> 命名规范：`node-template/` 为容器方式手动部署模板，`host-keepalived/` 为宿主机直装模板，`generated_nodes/<ip>/` 为 Web 一键生成的部署单元（自包含、可直接拷到目标主机 `/home/docker/<ip>/` 运行）。
> 约定：凡是有 `.template` 的目录，都已附带一份**可直接使用**的示例配置文件，无需先渲染即可上手；模板仅用于按变量批量生成。

---

## 3. 前置条件

- 管理端机器：Linux + Docker + docker compose v2；
- 每台节点机器：Linux + Docker + docker compose v2，且**节点机器到管理库 3306 端口网络可达**；
- 一个**独立的外部管理 MySQL**（推荐），用户/密码/库名统一约定为 `ha`（也可自定义）；
- 一个与节点同网段、**当前空闲**的 VIP（如 `192.168.1.10`），本工具无法自动探测，必须填写。
- 节点机器的 Docker 必须允许给容器附加 `NET_ADMIN` / `NET_BROADCAST` / **`NET_RAW`** 能力（`keepalived` 用原始套接字发 VRRP 通告，缺 `NET_RAW` 会报 `Cant send advert ... Operation not permitted` 并导致双主/split-brain）。若宿主机做了安全加固（如 CIS 基线默认 drop `NET_RAW`），需在 `docker-compose.yml` 显式 `cap_add: - NET_RAW`。

---

## 4. 部署服务端（管理器）

```bash
cd mysql-ha-manager
docker compose up -d --build
```

启动后访问：`http://<管理端IP>:28888`（账号 `admin`，密码见日志或 `WEB_PASS`）。

配置通过环境变量（可直接改 `docker-compose.yml` 的 `environment` 段，或新建 `.env`）：

| 变量 | 说明 | 默认 |
|------|------|------|
| `MGMT_MYSQL_HOST` | 管理库可达 IP（**务必填外部库真实 IP，不要写 auto / 127.0.0.1**） | `127.0.0.1` |
| `MGMT_MYSQL_PORT` / `USER` / `PASS` / `DB` | 管理库连接 | `3306` / `ha` / `ha` / `ha` |
| `WEB_PORT` | Web 端口 | `28888` |
| `WEB_USER` / `WEB_PASS` | Web 登录（密码留空则首次随机生成并打印日志） | `admin` / 随机 |
| `SECRET_KEY` | 会话密钥（生产务必改） | `change-me-in-production` |
| `CHECK_INTERVAL` / `FAIL_THRESHOLD` | 探活间隔(秒) / 连续失败阈值 | `5` / `3` |
| `NODES_DIR` | 容器内节点生成目录 | `/app/generated_nodes` |
| `NODES_HOST_BASE` | 节点目录在宿主机的**显示路径**（Web 展示用） | `mysql-ha-manager/generated_nodes` |
| `VIP` / `VIP_PREFIX` / `INTERFACE` / `VRID` | VIP / 掩码位 / 网卡 / 虚拟路由ID | 空 / `24` / `eth0` / `51` |
| `SMTP_*` / `ALERT_EMAIL` | 切换告警邮件（可选） | — |

> 也支持通过文件注入敏感信息：`MGMT_MYSQL_PASS_FILE`、`AGENT_PASS_FILE`（容器内路径）。

### 可选：自带演示用管理库
取消 `docker-compose.yml` 中 `mgmt-mysql` 整段注释，并取消 `ha-manager` 的 `depends_on` 注释；数据落在 `./data/mysql-data`。生产推荐使用你已有的外部管理库。

---

## 5. 配置服务端（Web）

登录后建议按顺序填写：

1. **第一步 · 管理库连接**：填管理 MySQL 的 host/port/user/pass/db，点「测试连接」保存。
2. **第二步 · 节点 HA 配置（部署账号）**：设置节点上的 `ha_monitor`（监控）与 `repl`（复制）账号密码，**这组密码必须和节点侧 init.sql / ha_check.conf 完全一致**。
3. **第三步 · 网络配置**：填 VIP、掩码位、网卡名（默认 eth0，容器会自动探测真实网卡）、VRID（默认 51，同一 VRRP 组内必须唯一）。

---

## 6. 添加被管节点（两种方式）

### 6.1 方式一：Web 一键生成（推荐）

1. 在 Web「添加节点」填写：节点 IP、SSH 可选、MySQL 端口（默认 3307）、server_id、root 密码、只读模式；
2. 提交后，管理器在 `generated_nodes/<ip>/` 生成完整、自包含的节点目录（含渲染好的 `docker-compose.yaml`、`keepalived.conf`、`ha_check.conf`、`init.sql`、`check_mysql_master.sh`、`entrypoint.sh`；MySQL 数据落在目录内 `./MySQL-data`）；
3. 把该目录**整体**拷贝到目标节点机器（如 `/home/docker/<ip>/`），在节点机上执行：

   ```bash
   cd /home/docker/<ip>
   docker compose up -d
   ```

   > 该目录自包含，所有路径相对本目录（MySQL 数据 `./MySQL-data`、配置 `./keepalived/`、初始化 `./init.sql`），拷贝后无需任何改动即可运行，**不会越级引用上级目录**。

4. 节点启动后，管理端会自动发现并将其纳入拓扑，开始探活与选主。

> `generated_nodes/` 已挂载持久化（`docker-compose.yml` 中 `./generated_nodes:/app/generated_nodes`）。

### 6.2 方式二：手动部署（容器方式，使用 `node-template/`）

`node-template/` 已附带**可直接使用的示例配置**，按下列步骤即可：

```bash
cd node-template
# 1) 按需修改 .env（必改 ROOT_PASS；多节点改 NODE_NAME/SERVER_ID/NODE_PORT）
# 2) 按需修改 keepalived/keepalived.conf、keepalived/ha_check.conf、keepalived/init.sql
#    （其中 ha_monitor / repl 密码须与「服务端配置」第二步一致）
# 3) 启动
docker compose up -d
```

- 账号密码来源：`node-template/.env` 提供 `ROOT_PASS` 等；`node-template/keepalived/ha_check.conf`、`node-template/keepalived/init.sql` 提供 `ha_monitor`/`repl` 密码。
- 多节点：每台节点把 `keepalived.conf` 的 `unicast_src_ip` 与本机 IP 对应、`unicast_peer` 指向对端即可，其余相同。
- 如需从模板重新生成（换一批变量）：`envsubst < keepalived/keepalived.conf.template > keepalived/keepalived.conf` 等。

### 6.3 方式三：宿主机直装 Keepalived（不使用容器）

适用于节点机已自带 MySQL、只想用 keepalived 做 VIP 漂移的场景：

```bash
sudo cp host-keepalived/keepalived.conf      /etc/keepalived/keepalived.conf
sudo cp host-keepalived/ha_check.conf        /etc/keepalived/ha_check.conf
sudo cp host-keepalived/check_mysql_master.sh /etc/keepalived/check_mysql_master.sh
sudo chmod +x /etc/keepalived/check_mysql_master.sh
sudo systemctl restart keepalived
```

> 该方式仅负责 VIP 与选主脚本；MySQL 主从本身需你自行在宿主机 MySQL 上配置。

---

## 7. 数据目录约定

| 路径 | 用途 |
|------|------|
| `generated_nodes/<ip>/` | 一键生成的节点部署单元（自包含，整体拷到目标主机 `/home/docker/<ip>/` 运行）；内含 `./MySQL-data` 数据卷 |
| `data/mysql-data/` | （可选）服务端自带演示管理库数据 |

`generated_nodes/` 与 `data/` 均已 gitignore，避免提交明文密码与业务数据。

---

## 8. 高可用验证

1. 在两节点均启动后，确认 VIP 已落在某一节点：`ip addr show | grep <VIP>`；
2. 应用连接 `VIP:3306` 读写正常；
3. 模拟主节点故障（停掉主节点 MySQL 或 keepalived），观察 VIP 漂移到对端、对端升主；
4. 故障恢复后，原主以从库身份重新加入（GTID 自动补齐）；
5. Web「切换历史」「拓扑」可查看过程；如配置了邮件会收到告警。

---

## 9. 常见问题 / 排错

**Q1. 节点 MySQL 首启报 `Permission denied`（init.sql）**
- 原因：`init.sql` 以 `:ro` 挂载进容器，由容器内非 root 的 `mysql` 用户读取；若宿主机文件权限过严（如 `0600`）则无法读取。
- 处理：确保 `init.sql` 权限为 `0644`（一键生成已默认 `0644`；手动方式用仓库自带 `node-template/keepalived/init.sql` 即可，正常 checkout 即为 `0644`）。

**Q2. keepalived 报 `sed: cannot rename ... Device or resource busy`**
- 原因：`keepalived.conf` 以只读 `:ro` bind 挂载进容器，旧 `entrypoint.sh` 用 `sed -i` 就地改名会失败。
- 处理：新版 `entrypoint.sh` 改为先 `cp` 到可写目录 `/run` 再修改，并以 `-f /run/keepalived.conf` 运行，**不会改动宿主机配置文件**。升级镜像后重跑即可。

**Q3. keepalived 日志 `interface eth0 doesn't exist`**
- 原因：不同主机网卡名不同（如 `ens33`/`eth1`）。
- 处理：`entrypoint.sh` 会根据 `unicast_src_ip` 自动探测真实网卡并替换后启动（日志可见「自动改用 xx」）。仍失败则检查 `unicast_src_ip` 是否为本机 IP，或手动在 `keepalived.conf` 写对网卡名。

**Q4. 添加节点时报「VIP 未配置」**
- 在「服务端配置 · 第三步」或 `docker-compose.yml` 的 `VIP` 中填写节点同网段空闲 IP。

**Q5. 节点监控显示账号错误 / 复制失败**
- 核对节点侧 `ha_check.conf` / `init.sql` 中的 `ha_monitor`、`repl` 密码，与「服务端配置 · 第二步」完全一致。

---

## 10. 安全提示

- `node-template/keepalived/ha_check.conf`、`*/init.sql`、`node-template/.env` 内含明文密码，仅作示例；生产请替换为强密码，并妥善保管节点目录。
- 管理端 `SECRET_KEY`、`WEB_PASS` 等务必在生产环境修改。
- `data/` 已 gitignore，不会随仓库提交。
