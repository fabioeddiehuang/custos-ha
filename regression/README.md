# Custos-HA 自动化测试

> 目标：在**不依赖真实 MySQL/MariaDB/keepalived** 的前提下，为最容易回归、最危险的「纯逻辑」建立回归护栏，
> 支撑「商用级稳定性」——改一行 MariaDB 逻辑，立刻知道有没有误伤 MySQL 路径、有没有把复制口令变成密文。

## 1. 怎么跑

```bash
# 1) 准备隔离 venv（推荐 Python 3.13+）
python3 -m venv .venv && . .venv/bin/activate

# 2) 装依赖
#    注意：若默认 PyPI 不可达（如本沙箱代理超时），改用腾讯云镜像：
pip install -r requirements-dev.txt
#   或： pip install -i https://mirrors.tencent.com/pypi/simple/ -r requirements-dev.txt

# 3) 运行
pytest -q            # 全部
pytest regression/test_mysql_compat.py -v   # 单文件
pytest -k "enc1 or mariadb" -v         # 按关键字
```

`pytest.ini` 已配置 `testpaths = regression`，无需额外参数。

## 2. 设计：为什么不需要真实数据库

`conftest.py` 在 import `app` 之前固化测试环境（`SECRET_KEY` 强随机、凭证加密默认关闭、
`DATA_DIR` 指向临时目录），并提供：

- **`FakeCursor`**：可脚本化的 PyMySQL 游标替身。按「SQL 子串 → 预设结果行」注册，
  记录所有执行过的 SQL，支持 `with` 上下文与 `fetchone/fetchall`。
- **`FakeConnection`**：`cursor()` 复用同一 `FakeCursor`。
- **`autouse` 夹具**：把 `app.db.get_setting` 桩成返回 `None`，**任何测试都不会触达真实管理库**，
  也让 `crypto.enabled()` 等稳定回退到 config 环境变量。

需要连库的函数（如 `make_slave_of`）通过 `monkeypatch` 把 `connect_robust` 替成假连接，
验证「入口守卫是否短路、是否真的发了连接」。

## 3. 覆盖清单（48 个用例）

| 文件 | 覆盖对象 | 守住的坑 |
|---|---|---|
| `test_mariadb_matrix.py` | 版本支持矩阵、版本准入、特权按服务器能力过滤、半同步命名 | 节点被错误拒绝/接受；硬编码特权名刷 `1064/1227` |
| `test_mysql_compat.py` | 跨风味复制 SQL 生成 + 风味分派（**MySQL↔MariaDB 双路径**） | 改 MariaDB 误伤 MySQL（TODO 1.3 核心）；MariaDB 误用 `SOURCE_` 术语；半同步 TLS 互操作 |
| `test_crypto.py` | 加解密闭环、ENC1 守卫、密钥不匹配「原样返回密文」 | 复制口令变 ENC1 密文报 `1470` 导致复制集体瘫痪；密钥不匹配时口令被覆盖为空 |
| `test_ha_client_pure.py` | `parse_gtid_set`、`is_gtid_purged_error`、`is_stale_master`、`make_slave_of` ENC1 入口守卫 | `1236` 清理错误被误自愈；残留主库误判；密文写进复制配置 |
| `test_monitor_pure.py` | `_classify_repl_state` 状态分类、`_repl_err_is_connectivity` | 「卡死」显示成「正常」；自愈空耗 CPU（源库不可达仍反复 START SLAVE） |

## 4. 还没覆盖（下一步）

- **TODO 1.2 环境测试矩阵**：真实「版本 × 形态 × 发行版」矩阵（10.11/11.4/12.3 × 裸机/容器 × Anolis/Ubuntu/CentOS），
  需容器编排，不在本单测范围内。
- **集成/端到端回归**：建节点 → 复制 → 切换 → 备份恢复 的完整链路（TODO 1.3 的「每次 MariaDB 改动跑一遍 MySQL 回归」）。
- **monitor 巡检/切换主流程**、**backup/rebuild 逻辑**：当前只覆盖了纯分类函数，重逻辑仍依赖真实库，需更多 `FakeConnection` 编排。

## 5. 怎么加新测试

1. 纯函数（只吃字符串/dict）：直接 `from app import xxx` 调，断言返回值。
2. 吃 `cur` 的函数：构造 `FakeCursor().add("SELECT VERSION()", [("12.3.3-MariaDB",)])` 传入。
3. 吃 `host,port,...` 并内部连库的函数：用 `monkeypatch.setattr(app.xxx, "connect_robust", lambda *a,**k: None)` 断连，
   验证入口守卫/短路行为。
4. 需要脚本化多步 SQL 的函数：用 `FakeConnection({子串: 行列表})`，按执行顺序在 `responses` 里注册。
