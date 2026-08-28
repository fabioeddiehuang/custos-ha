# MySQL HA 管理器 —— 构建 / 部署快捷方式
#
# 版本单一来源为 app/VERSION：make build 会经由 deploy.sh 把版本同步到 .env，
# 因此 docker-compose.yml 不需要写死版本号（升版时不必改 compose 文件）。

SHELL   := /bin/sh
VERSION := $(shell tr -d ' \t\r\n' < app/VERSION)

.PHONY: help version build up down restart logs ps shell

help:
	@echo "可用目标："
	@echo "  make version   显示当前版本（读 app/VERSION）"
	@echo "  make build     同步版本并重新构建、后台启动（最常用）"
	@echo "  make up        启动（不重建）"
	@echo "  make down      停止并移除容器"
	@echo "  make restart   重新构建并重启"
	@echo "  make logs      跟踪管理器日志"
	@echo "  make ps        查看容器状态"
	@echo "  make shell     进入管理器容器"

version:
	@echo $(VERSION)

build:
	@./deploy.sh up -d --build

up:
	@./deploy.sh up -d

down:
	@docker compose down

restart:
	@./deploy.sh up -d --build

logs:
	@docker compose logs -f --tail=200 ha-manager

ps:
	@docker compose ps

shell:
	@docker compose exec ha-manager sh
