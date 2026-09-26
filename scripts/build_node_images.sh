#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 构建并托管「节点镜像 tar」（v2.5.0 F2）
#
# 为什么需要它
#   节点的 keepalived 用镜像 custos-node:<版本>——它是**本地构建**的镜像，
#   Docker Hub 上没有；agent 用公共镜像 python:3-slim——**离线环境拉不到**。
#   两者缺一，curl 引导安装就会卡在 `docker compose up` 这一步。
#
#   本脚本在【服务端主机】上把这两个镜像打成 tar 放到项目根 ./images/，
#   服务端经 compose 挂载后通过 HTTP 下发给节点，引导脚本自动 `docker load`。
#
# 用法
#   ./scripts/build_node_images.sh              # 构建全部（custos-node + agent 基础镜像）
#   ./scripts/build_node_images.sh custos-node  # 只构建 keepalived 镜像
#
# 何时需要重跑
#   - custos-node：app/node_assets/keepalived/（Dockerfile / 检查脚本 / entrypoint）变了才需要
#   - agent 基础镜像：几乎不用重跑（agent 是脚本挂载方式运行，改 agent 只需下发文件 + 重启）
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="$(grep -E '^VERSION=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '\r' | xargs || true)"
if [ -z "${VERSION:-}" ]; then
  echo "[错误] 未能从 .env 读取 VERSION=，请先设置（升版只改这一处）" >&2
  exit 1
fi
# 镜像标签必须与 compose 里 keepalived 的 image **逐字符一致**，否则
# `docker load` 出来的名字对不上，compose 会退回本地 build（离线环境直接起不来）。
# compose 用的是 `custos-node:{PKG_VERSION}`，而 PKG_VERSION = APP_VERSION = .env 的 VERSION
# （形如 v2.5.0，**带 v 前缀**）—— 所以这里绝不能去掉 v。
# （历史 bug：曾写成 TAG="${VERSION#v}" 去掉 v，导致导出的 tar 是 custos-node:2.5.0，
#   节点 load 后找不到 custos-node:v2.5.0，keepalived 起不来。）
# 不再另打「不带 v」的兼容别名：新包一律带 v，保持单一口径，避免两种标签并存。
TAG="$VERSION"

IMAGES_DIR="./images"
mkdir -p "$IMAGES_DIR"

build_keepalived() {
  local ctx="app/node_assets/keepalived"
  if [ ! -f "$ctx/Dockerfile" ]; then
    echo "[错误] 未找到 $ctx/Dockerfile" >&2
    return 1
  fi
  echo "==> 构建 custos-node:${TAG}（上下文 $ctx）"
  docker build -t "custos-node:${TAG}" "$ctx"
  echo "==> 导出 $IMAGES_DIR/custos-node-${TAG}.tar"
  # 只导出【这一个】包（带 v）。不再另存不带版本的同名副本：
  # available_images() 会把目录下每个 tar 都列出来让节点逐个 docker load，
  # 存两份同内容的包等于让每个节点多下载约 70MB 并重复 load 一次。
  docker save "custos-node:${TAG}" -o "$IMAGES_DIR/custos-node-${TAG}.tar"
}

save_agent_image() {
  # agent 容器用 python:3-slim；离线环境节点拉不到，这里一并托管
  echo "==> 拉取并导出 python:3-slim（agent 基础镜像）"
  docker pull python:3-slim
  docker save python:3-slim -o "$IMAGES_DIR/python-3-slim.tar"
}

case "${1:-all}" in
  custos-node) build_keepalived ;;
  agent)       save_agent_image ;;
  all)         build_keepalived; save_agent_image ;;
  *) echo "未知参数：$1（可选 custos-node / agent / all）" >&2; exit 1 ;;
esac

echo
echo "[完成] 镜像包已生成在 $IMAGES_DIR/："
ls -lh "$IMAGES_DIR" | tail -n +2
echo
echo "提示：images/ 是【构建产物】—— .gitignore 已排除（不入库，每个约 70MB），"
echo "      .dockerignore 也已排除（不会打进服务端镜像）；"
echo "      它由 docker-compose.yml 挂载进容器，供节点 curl 引导时下载。"
echo "      install.sh 首次安装会自动执行本脚本，一般无需手工运行。"
