# MySQL 编译版 / 裸机 / 宝塔 安装说明（接入 HA-MySQL-Manager）

> 适用：MySQL 8.0.x、8.4（**不支持 MariaDB**，管理器基于 MySQL GTID 实现）。
> 目标：让 **HA-MySQL-Manager 管理器**能从内网连上你的裸机 MySQL 节点，并顺利部署复制 + 高可用。

---

## 0. 管理器需要节点满足什么

管理器会在「节点配置」页用你填写的 **root 用户 / root 密码 / 端口 / IP** 去连接每台节点 MySQL，因此节点必须：

| 要求 | 说明 | 不满足的后果 |
|---|---|---|
| 可从管理器内网连通 | MySQL 必须监听 `0.0.0.0` 且防火墙放行 | 管理器报 `2003 Can't connect`（宝塔默认只绑 `127.0.0.1`，最常见） |
| root 可远程登录 | 需有 `root@'%'`（或 `root@'<管理器IP>'`）账号 | 报 `1130 Host 'x' is not allowed to connect` |
| 开启 GTID | `gtid_mode=ON`、`log_bin=ON`、`log_slave_updates=ON` | 复制 / 自动切换无法建立 |
| server_id 唯一 | 同集群各节点不能重复 | 复制报错、配置巡检 DRIFT |
| HA 账号 | `ha_monitor` / `replication` / `app` 三个账号（由 `init_baremetal.sql` 创建） | 监控 / 复制 / 业务连不上 |

---

## 1. 一键准备（推荐）

这两个脚本（`Install-ha-mysql-Node.sh` 接入准备 / `uninstall-ha-mysql-Node.sh` 一键还原）**随节点目录一同生成**：在「节点配置 → 添加节点」生成后，它们已位于 `generated_nodes/<节点IP>/` 内。把整个目录拷到目标机 `/home/docker/<节点IP>/`，以 root 执行一次即可完成全部 MySQL 侧修改：

```bash
cd /home/docker/<节点IP>/

# 基础用法：把节点 MySQL root 密码作为参数
sudo bash Install-ha-mysql-Node.sh -p '你的MySQL的root密码'

# 只允许来自管理器的连接（更安全）—— 注意 -m 必须是【管理器宿主机 IP】，不是 VIP！
# 管理器是 docker 容器，连节点时源 IP 被 SNAT 成宿主机 IP（本例 192.168.1.66）。
# 在管理器宿主机上执行 `hostname -I` 查看其内网 IP，用它作为 -m 的值。
sudo bash Install-ha-mysql-Node.sh -p '密码' -m 192.168.1.66

# 不确定管理器 IP / 内网可信环境：省略 -m，脚本会创建 root@'%'（允许任意来源）
sudo bash Install-ha-mysql-Node.sh -p '密码'

# 连 init_baremetal.sql 也一起执行（同目录内）
sudo bash Install-ha-mysql-Node.sh -p '密码' -i init_baremetal.sql

# 自己用宝塔面板重启 MySQL，则加 --skip-restart
sudo bash Install-ha-mysql-Node.sh -p '密码' --skip-restart
```

脚本会自动完成：创建远程 `root@'%'`、改 `bind-address=0.0.0.0`、追加 GTID 片段到 `my.cnf` 的 `[mysqld]`、放行防火墙（MySQL 端口 + VRRP 112）、重启 MySQL、自检参数。

脚本会**备份原 my.cnf**（加 `.bak.时间戳`，并在状态文件记录），需要回滚时执行同目录的 `uninstall-ha-mysql-Node.sh` 即可还原原配置、删除远程账号、回退防火墙与 keepalived：

```bash
sudo bash uninstall-ha-mysql-Node.sh          # 按状态文件逐项还原
sudo bash uninstall-ha-mysql-Node.sh --force  # 状态文件缺失时强制还原（用备份 .bak 还原 my.cnf）
```

---

## 2. 手工准备（不使用脚本时）

### 2.1 开启内网登录权限（解决你遇到的「root 无法连接」）

宝塔 / 编译安装的 MySQL 8.0 默认 `root` 仅限本机。登录节点 MySQL 后执行：

```sql
-- 允许来自任意内网主机的 root 连接（生产可改为具体管理器 IP，如 'root'@'192.168.1.10'）
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED WITH mysql_native_password BY '你的root密码';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

> 提示：用 `mysql_native_password` 而非默认的 `caching_sha2_password`，可避免非 TLS 下的复制/连接兼容问题（管理器已做公钥兼容，但统一用 native 最稳）。

### 2.2 监听所有网卡

编辑 MySQL 配置（宝塔路径：`/www/server/mysql/my.cnf`；编译安装常见：`/etc/my.cnf`）的 `[mysqld]` 段：

```ini
bind-address = 0.0.0.0
```

> 宝塔重启 MySQL 会重写 `my.cnf`，所以**关键复制参数务必写进配置文件**（不要只用 `SET GLOBAL`），否则重启后失效。

### 2.3 写入 GTID / 复制参数

在 `[mysqld]` 段追加（**server_id 每台不同**，建议用 IP 末段，如 192.168.1.203 → 203）：

```ini
server-id = 203
gtid_mode = ON
enforce_gtid_consistency = ON
log_bin = mysql-bin
log_slave_updates = ON
relay_log = relay-bin
relay_log_recovery = ON
sync_binlog = 1
innodb_flush_log_at_trx_commit = 1
read_only = ON
slow_query_log = 1
long_query_time = 0.5
log_output = TABLE
log_queries_not_using_indexes = 0
```

> `read_only = ON` 是安全的：首台主库由管理器自动提升（`SET GLOBAL read_only=0`），节点重启后回到只读可避免脑裂，**无需手动取消**。

### 2.4 重启 MySQL 并放行防火墙

```bash
# 重启（按你的环境选其一）
systemctl restart mysqld          # 通用
/etc/init.d/mysqld restart        # 宝塔
/www/server/mysql/support-files/mysql.server restart

# 防火墙放行 MySQL 端口（下面以 3306 为例，按实际端口改）
firewall-cmd --permanent --add-port=3306/tcp; firewall-cmd --reload      # firewalld
ufw allow 3306/tcp                                                          # ufw

# keepalived 漂移 VIP 需要 VRRP 协议（协议号 112，不是端口）
firewall-cmd --permanent --add-protocol=vrrp; firewall-cmd --reload
iptables -I INPUT -p vrrp -j ACCEPT
```

### 2.5 自检

```bash
mysql -uroot -p'密码' -h 127.0.0.1 -P 3306 -e "SELECT @@gtid_mode,@@read_only,@@server_id;"
# 期望：gtid_mode=ON，server_id=你的取值
```

---

## 3. 在管理器 Web 上添加节点并部署

1. **集群管理**：新建（或选择）集群，记下 **VIP** 与 **vip_port**。裸机场景下建议 `vip_port` 与第 2 步的节点端口一致（如都是 3306），业务连接 `VIP:3306` 即可。
2. **节点配置 → 添加节点**：
   - 节点 IP：本机 IP
   - MySQL 端口：本机实际端口（宝塔常见 3306；页内默认显示 3307，按需改）
   - root 用户：`root`
   - root 密码：第 2.1 步设置的密码
   - 节点类型：**裸机**
   - 初始角色：第一台选「主库」，其余选「从库」
   - MySQL 版本：如 `8.0.36`（可留空，管理器会自动探测）
3. 生成后，管理器会在 `generated_nodes/<节点IP>/` 下产出：
   - `my.cnf.ha-fragment`（你已在第 2.3 步处理）
   - `init_baremetal.sql`（创建 `ha_monitor` / `replication` / `app` 三个账号 + `ha_local` 库）
   - `docker-compose.yaml` + `keepalived/`（仅 keepalived 客户端，MySQL 仍是宿主机实例）
4. 把该目录拷到节点 `/home/docker/<节点IP>/`，然后：
   ```bash
   # 创建 HA 账号（若没在第 1 步用 -i 执行过）
   mysql -uroot -p'密码' < /home/docker/<节点IP>/init_baremetal.sql

   # 启动 keepalived 客户端（节点需已装 docker）
   cd /home/docker/<节点IP>/ && docker compose up -d
   ```
5. 所有节点都准备好后，管理器监控线程（约 6–12s 一轮）会自动：探测拓扑 → 提升主库 → 建立 GTID 复制 → 漂移 VIP。也可在节点页点「重新生成」触发预检（连通性 / GTID / server_id 唯一性 / 版本一致性）。

---

## 4. 常见问题排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `2003 Can't connect` | `bind-address=127.0.0.1` 或防火墙未放行 | 第 2.2 / 2.4 步 |
| `1130 Host not allowed` | 未建 `root@'%'` | 第 2.1 步 |
| 复制起不来 / 预检 `gtid_mode != ON` | my.cnf 未加 GTID 片段或被宝塔重写 | 第 2.3 步，确认写在配置文件并重启 |
| 配置巡检 `server_id` DRIFT | 两台 server_id 相同 | 改成各机唯一值后重启 |
| VIP 不上浮 | 本机不是当前主 / read_only 未清 / keepalived 未运行 | 确认管理器已提升主库、keepalived 容器 `docker compose up -d` |
| 节点列表端口对不上 | 页内默认 3307 与宝塔 3306 不一致 | 添加节点时端口填 3306 |

---

## 5. 前置依赖小结

- 每台节点：MySQL 8.0（已按上文准备）、已安装 **docker**（供 keepalived 客户端容器，需 `NET_ADMIN`）。
- 网络：管理器 ↔ 节点 MySQL 端口互通；节点间 MySQL 端口互通；VIP 所在网段放行 **VRRP(112)**。
- 密码中尽量避免单引号，以免脚本 SQL 拼接异常。
