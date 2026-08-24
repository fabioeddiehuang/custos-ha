# MariaDB 裸机节点安装说明（接入 HA-MySQL-Manager）

> 适用：裸机部署 **MariaDB 12.x**（管理器基于 MariaDB GTID `slave_pos` 实现高可用）。
> 目标：让 **HA-MySQL-Manager 管理器**能从内网连上你的裸机 MariaDB 节点，并顺利部署复制 + 高可用。
>
> 另一种节点形态是 **Docker MySQL 8.0 模式**（MySQL + keepalived 容器，见 README「节点一键部署」），二者互斥、二选一，按节点选择 `node_type`。

---

## 0. 管理器需要节点满足什么

管理器会在「节点配置」页用节点目录里 `keepalived/ha_check.conf` 的 **root 用户 / root 密码 / 端口 / IP** 去连接每台节点 MariaDB，因此节点必须：

| 要求 | 说明 | 不满足的后果 |
|---|---|---|
| 可从管理器内网连通 | MariaDB 必须监听 `0.0.0.0` 且防火墙放行 | 管理器报 `2003 Can't connect` |
| root 可远程登录 | 需有 `root@'%'`（或 `root@'<管理器IP>'`）账号 | 报 `1130 Host 'x' is not allowed to connect` |
| 开启 GTID | `gtid_mode=ON`、`log_bin=ON`、`log_slave_updates=ON` | 复制 / 自动切换无法建立 |
| server_id 唯一 | 同集群各节点不能重复 | 复制报错、配置巡检 DRIFT |
| HA 账号 | `ha_monitor` / `replication` / `app` 三个账号（由 `init_mariadb.sql` 创建） | 监控 / 复制 / 业务连不上 |

以上除「内网连通 / 防火墙」外，均由 `Install-ha-mariadb-Node.sh` 自动完成，无需手工干预。

---

## 1. 一键安装（推荐，全量安装 MariaDB）

裸机 MariaDB 是**独立全量安装**（不依赖宿主既有 MySQL，独立 datadir `/data/mariadb`、独立 systemd 单元 `mariadb-ha.service`），因此宿主上即使已有 MySQL 也不冲突。

这些脚本（`Install-ha-mariadb-Node.sh` 唯一全量安装脚本，自包含 / `Uninstall-mariadb-Node.sh` 卸载）**随节点目录一同生成**：在「节点配置 → 添加节点（类型选 MariaDB）」生成后，它们已位于 `generated_nodes/<节点IP>/` 内。

### 1.1 安装步骤

```bash
# 1) 把生成的整个目录拷到目标机某目录
scp -r generated_nodes/<节点IP>/ 目标机:/home/docker/<节点IP>/
ssh 目标机

cd /home/docker/<节点IP>/

# 2) 把 MariaDB 安装包放到同一目录（脚本优先用本地包，跳过下载）
#    也可留空，脚本按节点目录 my.cnf 里的版本自动从镜像下载
ls mariadb-*-linux-*.tar.gz

# 3) 全量安装（读取同目录 ha_check.conf 的真实凭证，无需交互输入）
sudo bash Install-ha-mariadb-Node.sh
```

脚本自动完成：下载/解压 MariaDB → `mariadb-install-db` 初始化 → 落盘服务端 `my.cnf` → systemd 启动 `mariadb-ha.service` → 跑 `init_mariadb.sql` + `fix-node-creds.sql` → 起 keepalived 客户端。

### 1.2 幂等更新（改密码 / 改配置后重跑即可）

`Install-ha-mariadb-Node.sh` 是**幂等**的：
- 已解压过 → 跳过解压；数据目录已初始化 → 跳过 `mariadb-install-db`；
- 重新生成节点目录并覆盖 `my.cnf` / `ha_check.conf` 后，**直接重跑**即可更新配置与 root 密码（脚本先连旧密码再改密，不会因「MariaDB 已启动」而误报失败）。

> 无需单独的 update 脚本：覆盖配置目录 → `sudo bash Install-ha-mariadb-Node.sh` 即为「快速更新密码/配置」。

---

## 2. 卸载

```bash
cd /home/docker/<节点IP>/

# 交互式卸载：默认【保留】MariaDB 程序目录 /opt/mariadb（便于快速重装），
# 其余新增文件（数据目录 / my.cnf / 软链 / 状态文件 / 账号 / 复制通道）全部清理并恢复原状
sudo bash Uninstall-mariadb-Node.sh

# 如需彻底删除程序目录，加 -f
sudo bash Uninstall-mariadb-Node.sh -f
```

卸载会：停止并禁用 `mariadb-ha.service`、清理复制通道与 HA 账号/远程 root、删除 `ha_local`、回退防火墙、恢复安装前 `/etc/my.cnf`、停止 keepalived 容器。卸载后请到管理器 Web 删除该节点。

---

## 3. 在管理器 Web 上添加节点并部署

1. **集群管理**：新建（或选择）集群，记下 **VIP** 与 **vip_port**。
2. **节点配置 → 添加节点**：
   - 节点 IP：本机 IP
   - MariaDB 端口：本机实际端口（默认 3306）
   - root 用户：`root`
   - root 密码：节点 MariaDB 的 root 密码（自动写入 `ha_check.conf`，安装脚本据此设密）
   - 节点类型：**MariaDB**
   - 初始角色：第一台选「主库」，其余选「从库」
   - MariaDB 版本：如 `12.3.3`（可留空，管理器会自动探测）
3. 生成后，管理器在 `generated_nodes/<节点IP>/` 产出：`my.cnf`、`init_mariadb.sql`、`fix-node-creds.sql`、`docker-compose.yaml`（仅 keepalived 客户端）、`keepalived/`、`Install-ha-mariadb-Node.sh`（唯一安装脚本，自包含）、`Uninstall-mariadb-Node.sh`。
4. 按第 1 节安装所有节点后，管理器监控线程（约 6–12s 一轮）自动：探测拓扑 → 提升主库 → 建立 GTID 复制 → 漂移 VIP。

---

## 4. 手工准备（不使用一键脚本 / Docker 模式参考）

> Docker MySQL 8.0 模式的节点无需本机安装 MariaDB；以下步骤仅用于理解参数或在特殊环境手工配置。

### 4.1 开启内网登录权限
```sql
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED VIA mysql_native_password BY '你的root密码';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

### 4.2 监听所有网卡
```ini
# /etc/my.cnf [mysqld]
bind-address = 0.0.0.0
```

### 4.3 写入 GTID / 复制参数
```ini
server-id = 203
gtid_domain_id = 203
gtid_strict_mode = ON
enforce_gtid_consistency = ON
log_bin = mysql-bin
log_slave_updates = ON
relay_log = relay-bin
relay_log_recovery = ON
sync_binlog = 1
innodb_flush_log_at_trx_commit = 1
read_only = ON
```

### 4.4 防火墙放行
```bash
firewall-cmd --permanent --add-port=3306/tcp; firewall-cmd --reload      # firewalld
iptables -I INPUT -p vrrp -j ACCEPT                                     # VRRP(112) 漂移所需
```

---

## 5. 常见问题排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `2003 Can't connect` | `bind-address=127.0.0.1` 或防火墙未放行 | 第 4.2 / 4.4 步 |
| `1130 Host not allowed` | 未建 `root@'%'` | 第 4.1 步 |
| 复制起不来 / 预检 `gtid_mode != ON` | my.cnf 未加 GTID 参数 | 第 4.3 步，确认写在配置文件并重启 |
| 配置巡检 `server_id` DRIFT | 两台 server_id 相同 | 改成各机唯一值后重启 |
| VIP 不上浮 | 本机不是当前主 / read_only 未清 / keepalived 未运行 | 确认管理器已提升主库、keepalived 容器 `docker compose up -d` |
| 重跑 Install 报「MariaDB 启动失败」 | 旧版脚本用免密 socket 探活，重装时已设密码导致误判 | 使用新版幂等脚本（先连旧密码再改密），直接重跑即可 |

---

## 6. 前置依赖小结

- 每台裸机 MariaDB 节点：目标机已装 `curl/tar/gzip/libaio`、已装 **docker**（供 keepalived 客户端容器，需 `NET_ADMIN`）。
- 网络：管理器 ↔ 节点 MariaDB 端口互通；节点间 MariaDB 端口互通；VIP 所在网段放行 **VRRP(112)**。
- 密码中尽量避免单引号，以免脚本 SQL 拼接异常。
