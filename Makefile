# Custos —— 构建 / 部署快捷方式
#
# 版本单一来源为 .env 的 VERSION=：升版只改 .env 这一处，
# docker-compose.yml 不需要写死版本号（升版时不必改 compose 文件）。

SHELL   := /bin/sh
VERSION := $(shell grep -E '^VERSION=' .env 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')

.PHONY: help version build rebuild up down start stop restart logs ps shell install uninstall

help:
	@echo "可用目标："
	@echo "  make version   显示当前版本（读 .env）"
	@echo "  make build     重新构建、后台启动（最常用）"
	@echo "  make rebuild   重建管理器镜像并重启（等价 make build）"
	@echo "  make up        创建并启动（不重建）"
	@echo "  make start     同 make up"
	@echo "  make stop      停止并移除容器（= docker compose down）"
	@echo "  make down      停止并移除容器"
	@echo "  make restart   先 down 再 up -d（不重建镜像）"
	@echo "  make logs      跟踪管理器日志"
	@echo "  make ps        查看容器状态"
	@echo "  make shell     进入管理器容器"
	@echo "  make install   安装 custosctl 到 /usr/bin（需 sudo）"
	@echo "  make uninstall 卸载管理器并删除 custosctl（需 sudo）"
	@echo "  注：更推荐统一使用命令行工具 custosctl（custosctl help）"

version:
	@echo $(VERSION)

build:
	@./scripts/deploy.sh up -d --build

rebuild:
	@./scripts/deploy.sh up -d --build

up:
	@./scripts/deploy.sh up -d

start:
	@./scripts/deploy.sh up -d

stop:
	@docker compose down

down:
	@docker compose down

restart:
	@docker compose down && ./scripts/deploy.sh up -d

logs:
	@docker compose logs -f --tail=200 custos-server

ps:
	@docker compose ps

shell:
	@docker compose exec custos-server sh

install:
	@sudo ./scripts/install-custosctl.sh

uninstall:
	@sudo ./scripts/uninstall.sh
