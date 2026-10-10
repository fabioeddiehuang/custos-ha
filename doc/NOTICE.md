# 致谢 / Acknowledgements

Custos（原 mysql-ha-manager）是一个基于 **Keepalived + MySQL/MariaDB 主从（GTID）** 的数据库高可用管理项目，以 MIT 许可证发布。

本项目能成立，离不开以下开源软件、平台与社区的坚实基础。在此对它们的作者与维护者致以诚挚感谢与尊重：

## 被纳管与依赖的数据库
- **MariaDB**（MariaDB Foundation）与 **MySQL**（Oracle Corporation）：本项目为其提供高可用管理能力；版本支持矩阵、引擎方言适配、半同步复制适配，全部建立在对两者行为与差异的深入理解之上。
- **MariaDB / MySQL 官方客户端工具**（`mariadb-dump` / `mysqldump` / `mariadb-client` 等）：数据迁移向导依赖其完成逻辑备份、恢复与方言探测。

## 高可用与运行时底座
- **Keepalived**：基于 VRRP 的 VIP 漂移与脑裂防护，是本项目自动切换与高可用能力的核心依赖。
- **Docker** 与 **Docker Compose**：管理器、节点数据库、keepalived、节点 agent 全部以容器形态部署，依赖其进程隔离与编排能力。

## 开发语言与核心依赖（Python）
- **Python**：本项目开发语言。
- **FastAPI**：Web API 框架。
- **Uvicorn**：ASGI 服务器。
- **Jinja2**：服务端模板引擎。
- **PyMySQL**：MySQL / MariaDB 客户端驱动。
- **itsdangerous**：签名与安全相关工具。
- **python-multipart**：HTTP 表单数据解析。
- **cryptography**：加密库（凭证静态加密使用其 Fernet 实现）。

## 基础镜像
- 节点与 keepalived 镜像基于 **Debian** / **Alpine Linux** 等发行版提供的最小系统构建，感谢其维护者。

---

版权声明见 [LICENSE](../LICENSE)。本项目 © 2026 fabiohuang，以 MIT 许可证开源。
