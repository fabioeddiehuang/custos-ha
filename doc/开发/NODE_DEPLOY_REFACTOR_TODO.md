# 节点部署改造 · 测试检查清单

> 生成日期：2026-09-13。目标：节点包目录归整 + 统一 `custosctl` + 日志软链接入。

## 0. 版本
- [ ] `.env` VERSION 已升到 `v2.3.8`（本 checkout 原为 v2.3.5，需确认部署 commit 真含 v2.3.8 修复）
- [ ] keepalived 镜像 tag 随 VERSION 变为 `custos-node:v2.3.8`

## 1. 节点目录结构（mysql / mariadb 同构）
- [ ] 包根仅留：`安装脚本` `docker-compose.yaml` `keepalived/` `<db>/`(`mysql8.0` 或 `mariadb12.3.3`) `agent/` `logs`(软链)
- [ ] `<db>/conf` 放 cnf；`<db>/scripts` 放脚本；今后相关文件归位到合理子目录
- [ ] `agent/` 目录存在，含 `node_agent.py` + `node-agent.service`
- [ ] `logs/` 为软链 → `/data/custos-ha/logs/<base>`（base=mysql8|mariadb12）

## 2. 日志接入（keepalived + agent 进 logs）
- [ ] `keepalived.conf` 加 `log_file /var/log/custos-ha/keepalived.log`
- [ ] docker `keepalived` service 加日志卷 `- /data/custos-ha/logs/<base>:/var/log/custos-ha:z`
- [ ] `node_agent.py` 支持 `--log`；docker `custos-node-agent` service 加 `--log` + 日志卷
- [ ] 裸机 `node-agent.service` `StandardOutput=append:/data/custos-ha/logs/<base>/node_agent.log`
- [ ] `custosctl node install` 后 `<ip>/logs` 可见 `error.log` / `keepalived.log` / `node_agent.log`

## 3. 统一命令 `custosctl`
- [ ] `custosctl` 加 `node` 子命令：`install/start/stop/restart/restart-keepalived/status/backup/restore/uninstall [-f]`
- [ ] 节点包根自带 `custosctl`；`custosctl node install` 自装到 `/usr/bin/custosctl`
- [ ] `backup/restore` 按 node_type 选 `mysqldump/mysql` 或 `mariadbdump/mariadb`
- [ ] 同机含管理端 + 节点 → 单一 `/usr/bin/custosctl`，覆盖幂等不冲突

## 4. 卸载
- [ ] docker：`uninstall` 停容器保留数据；`-f` 删 `/data/custos-ha/<base>`
- [ ] 裸机：`uninstall -f` 额外 `rm -rf /opt/mariadb`(或 `/opt/mysql`) + 数据目录

## 5. 回归（2.2.20 → 主线）
- [ ] 旧管理库直接喂新管理器：启动自动迁移 schema + 加密明文凭证，连旧节点
- [ ] `monitor` 自检：账号自愈 / 复制 / VIP
- [ ] 切换 / 复制自愈 / 配置巡检 / 备份恢复 / 告警 全通过
- [ ] 回退：停主线 → 恢复升级前管理库备份 → 重部署 2.2.20（凭证已加密不可逆，须靠备份）
