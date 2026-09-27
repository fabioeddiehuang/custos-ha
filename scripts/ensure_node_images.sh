#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 节点镜像包自检 / 自助生成（v2.5.0 F2 根治）
#
# 由 docker-entrypoint.sh 在管理器启动时调用（非阻断）。
#
# 根治目标：服务端 ./images 为空（忘了跑 build_node_images.sh）时，节点引导
#   会在「校正 keepalived 镜像标签」步骤失败。本脚本利用 compose 已挂载的
#   /var/run/docker.sock + 镜像内 docker 客户端，复用 build_node_images.sh
#   把缺失的包重新 build+save 到 /app/images（即宿主 ./images 挂载卷）。
#
# 前置：VERSION 环境变量（compose 注入）必须存在；docker CLI 与 docker.sock
#   可用。任一不满足则跳过并告警，退回「服务端预构建」的既有路径——不影响启动。
# ---------------------------------------------------------------------------
set -u

IMAGES_DIR="/app/images"
VER="${VERSION:-}"
[ -z "$VER" ] && { echo "[ensure] 未设置 VERSION 环境变量，跳过节点镜像自检（依赖服务端预构建 ./images）"; exit 0; }

mkdir -p "$IMAGES_DIR"

command -v docker >/dev/null 2>&1 || { echo "[ensure] 容器内无 docker CLI，跳过（依赖服务端预构建 ./images）"; exit 0; }
[ -S /var/run/docker.sock ] || { echo "[ensure] 未挂载 /var/run/docker.sock，跳过（依赖服务端预构建 ./images）"; exit 0; }
export DOCKER_HOST="unix:///var/run/docker.sock"

# 缺包判定：文件不存在/为空 → 需生成；存在但非 gzip（防「未压缩膨胀 3 倍」回归）→ 需重新生成
need() {  # $1 = tar 文件名
  local p="$IMAGES_DIR/$1"
  [ -s "$p" ] || return 0
  [ "$(head -c2 "$p" | od -An -tx1 | tr -d ' \n')" = "1f8b" ] && return 1 || return 0
}

cd "$(dirname "$0")/.."   # → /app

if need "custos-node-${VER}.tar"; then
  echo "[ensure] 缺失 custos-node-${VER}.tar，经 docker.sock 自助生成..."
  if bash scripts/build_node_images.sh custos-node; then
    echo "[ensure] ✓ custos-node-${VER}.tar 已生成"
  else
    echo "[ensure] ✗ custos-node 生成失败（可能离线无法拉取基础镜像）。节点引导若报缺镜像，请服务端执行 scripts/build_node_images.sh" >&2
  fi
else
  echo "[ensure] custos-node-${VER}.tar 已存在（跳过）"
fi

if need "python-3-slim.tar"; then
  echo "[ensure] 缺失 python-3-slim.tar，经 docker.sock 自助生成..."
  if bash scripts/build_node_images.sh agent; then
    echo "[ensure] ✓ python-3-slim.tar 已生成"
  else
    echo "[ensure] ✗ python-3-slim 生成失败（可能离线无法拉取）。节点可改为自行拉取该公共镜像，或服务端预构建" >&2
  fi
else
  echo "[ensure] python-3-slim.tar 已存在（跳过）"
fi

echo "[ensure] 节点镜像自检完成"
