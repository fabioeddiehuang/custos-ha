# Custos 数据迁移功能 — 端到端实测报告

> 测试时间：2026-09-16 夜间 ~ 2026-09-17
> 执行方式：源码改动 + 真实节点（177 / 213）数据库实测，非模拟
> 结论：**核心迁移链路已可用且成功率可量化；预检(dry-run) 能在 TB 级拷贝前几秒拦下必炸项**

---

## 1. 测试目标

1. 验证「数据迁移向导」在真实旧数据上能否跑通（功能可用性）
2. 验证跨产品线场景 **MySQL 5.7 → MariaDB 12.3.3** 是否成功（用户最关心的路径）
3. 用真实环境坐实此前推断的若干「必炸」隐患，并确认修复有效

---

## 2. 测试环境

| 角色 | 地址 | 版本 | 说明 |
|---|---|---|---|
| 源库（旧数据） | 192.168.1.177:3310 | MariaDB 12.3.3 | 由 177 上 `/data/custos-ha/mariadb`（约 1GB 真实旧数据，含 certd/jms/sunpanel/sync-in）复制为 `mig_src_data` 后复活；`--skip-grant-tables` 便于免密连 |
| 目标库 | 192.168.1.213:3307 | MariaDB 12.3.3 | `mig-target` 容器（mariadb:12.3.3） |
| 源库（MySQL 5.7） | 192.168.1.177:3311 | MySQL 5.7.44 | `mig-src57` 容器，含 `shop`( customers/orders+JSON 列 ) 与 `risky`( ngram 全文索引 ) |
| 管理库 | 192.168.1.177:3307 | MariaDB | 177 的 `custos-mariadb` 上建 `ha2/ha2`，存 `migration_tasks` |
| 运行时 | 177 的 `migtest` 容器 | python:3.11-slim + mariadb-client | 装载应用源码，跑端到端脚本 |

> 注：本机 Docker 因外挂盘 `/media/fabio/SOFTWARE` 权限问题无法让 MySQL 写数据目录，故真实验证全部放到节点 177 / 213 进行。

---

## 3. 测试覆盖矩阵与结果

| 场景 | 通道 | 结果 | 数据一致性校验 |
|---|---|---|---|
| **MariaDB → MariaDB**（真实 1GB 旧数据） | 逻辑 | ✅ 成功（23s，5.5MB） | certd 47表/396行、sunpanel 8表/117行，**源=目标** |
| **MySQL 5.7 → MariaDB 12.3.3**（跨产品线） | 逻辑 | ✅ 成功 | shop 2表/6行一致；自动判定 `cross_engine=True / pipeline=stopcopy / bridge=False` |
| **预检拦截 ngram 全文索引** | dry-run | ✅ 提前拦下（不搬数据） | 报 `WITH PARSER ngram` 不兼容 + `ERROR 1128` |
| 后台异步进度链路 | — | ✅ 正常 | 进度 0%→45%、阶段文字、带时间戳日志均正确落库 |

---

## 4. 本轮修复的必炸 bug（全部由真实环境坐实）

| # | 问题 | 现象 | 修复 |
|---|---|---|---|
| 1 | dump 工具参数互斥 | `mariadb-dump --set-gtid-purged=ON` → `unknown variable`；`mysqldump --gtid` → `unknown option` | 按源端产品线分流工具与 GTID 参数（`dump_gtid_args`） |
| 2 | 探测变量名错误 | `SELECT @@log_replica_updates` 在 MariaDB 12.3.3 → `ERROR 1193` | 改 `@@log_slave_updates`，逐项探测不拖垮整体 |
| 3 | 管理器镜像无数据库客户端 | 纯 `python:3.11-slim`，无 `mysqldump/mariadb-dump`，逻辑/物理通道整体跑不起来 | Dockerfile 增加 `mariadb-client` |
| 4 | GTID 位点写进 dump | `SET GLOBAL gtid_slave_pos=''` 与目标 binlog 冲突 → `ERROR 1948` | 位点改为快照，留给 bridge 阶段 |
| 5 | 方言按文件名判断误判 | Debian 的 `mysql-client` 是指向 `mariadb-client` 的过渡包，`/usr/bin/mysqldump` 实为 MariaDB 版，传 `--set-gtid-purged` 直接失败 | 改用 `<binary> --help` 探测真实方言（`tool_dialect`） |
| 6 | 自签名证书 TLS 失败 | MySQL 5.7 自签证书 + 新客户端默认校验 → `TLS/SSL error 2026` | 对 MariaDB 方言加 `--ssl-verify-server-cert=0` |
| 7 | subprocess stdin 二次 flush | 手动 close(stdin) 后再 `communicate()` → `ValueError: flush of closed file` | 置 `stdin=None` 后 communicate |

---

## 5. 能力矩阵（门禁唯一真源）

```
capabilities(src_flavor, src_major, tgt_flavor):
  same_engine = (src_flavor == tgt_flavor)
  physical    = same_engine                 # 跨引擎物理迁移不可行(MySQL8 删 .frm)
  bridge      = same_engine and 两端皆 mariadb   # 跨引擎 GTID 格式不通
  pipeline    = "continuous" if (同引擎且目标 mariadb) else "stopcopy"
  refused     = 跨引擎 and src 为 MySQL 且 major>=8   # 按用户决策暂不实现 8.0→MariaDB
```

实测取值：

| 组合 | same_engine | physical | bridge | pipeline | refused |
|---|---|---|---|---|---|
| MariaDB → MariaDB | ✅ | ✅ | ✅ | continuous | ❌ |
| MySQL 5.7 → MariaDB | ❌ | ❌ | ❌ | stopcopy | ❌ |
| MySQL 8.0 → MariaDB | ❌ | ❌ | ❌ | stopcopy | ✅（暂不实现） |

> 设计上**不拆两套独立体系**，而是用「能力标志」驱动同一套骨架：MySQL 5.7 因天然躲开 `caching_sha2` 与 `utf8mb4_0900_ai_ci` 两个必炸项，自动与 MariaDB 落进同一「宽松桶」，无需硬编码例外。

---

## 6. 预检（结构试运行）价值演示

预检做两层检查（只读 `information_schema` + 仅结构 `--no-data` 改名落到目标 scratch schema 真实导入再删除）：

- 元数据层：排序规则 / 存储引擎 / JSON 列 / `WITH PARSER` 全文解析器
- 结构试运行：抓 `information_schema` 看不出的 DDL 失败

**实测效果**：对含 `risky`(ngram) 库的 MySQL 5.7 源，预检在真实迁移开始前报出 2 个阻断项，而非等到 TB 级拷贝一半才失败：

```
[blocker] fulltext_parser  使用了 MySQL 特有的全文解析器（WITH PARSER），MariaDB 无对应实现
[blocker] structural_trial 结构试运行导入失败
         ERROR 1128 (HY000) at line 34: Function 'ngra...
```

---

## 7. 向导使用（已支持）

1. 探测源库（自动识别 MariaDB / MySQL 及大版本，回显数据库列表与数据量）
2. 选库 + 选通道（逻辑 / 物理）；物理通道按 `node_type` 选数据目录，支持 `tgt_datadir`、`src_my_cnf`、`src_data_dir` 覆盖
3. **执行前可点「迁移前兼容性预检」** 先跑 dry-run
4. 逐步执行：种子 →（桥接）→ 校验 → 停桥切换 → 收尾；每步有 **百分比进度 / 阶段文字 / 实时日志**（后台线程落库，前端 1.5s 轮询）

---

## 8. 遗留事项（诚实清单）

- `bridge / verify / cutover / cleanup` 四步尚未跑端到端（仅 `seed` 与预检已实测）。
- 物理通道未实测（镜像内还缺 `mariabackup`；且物理跨引擎本就被门禁拒绝）。
- 账号/权限迁移未实现（当前排除 `mysql` 系统库，跨引擎时为必做项）。
- MySQL 8.0 → MariaDB 按用户决策暂不实现（`refused=True`）。
- 测试环境仍在节点上：177 有 `mig-source`(3310)/`mig-src57`(3311)/`migtest`；213 有 `mig-target`(3307)；1GB 旧数据副本在 `/data/custos-ha/mig_src_data`（原始数据未改动）。
