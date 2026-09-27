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

VERSION="${VERSION:-$(grep -E '^VERSION=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '\r' | xargs || true)}"
if [ -z "${VERSION:-}" ]; then
  echo "[错误] 未能从环境变量 VERSION 或 .env 读取 VERSION=，请先设置（升版只改这一处）" >&2
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

# ---------------------------------------------------------------------------
# 导出镜像 tar（根治「tar 体积膨胀 3 倍」）：
#   部分 Docker 环境导出的镜像层是【未压缩】的（经典 docker save 格式，或
#   containerd 存储对本地构建镜像按原始层保存），同一镜像 tar 会从 ~66MB
#   膨胀到 ~228MB，每个节点白白多下约 3 倍流量。
#   ⚠ 管道 gzip 压缩内容，但文件名保持 .tar——available_images() 只认
#     *.tar、下载端点强制补 .tar 后缀，文件名不变则全链路零改动；docker
#     load 对 gzip 内容自动解压（与扩展名无关）。
#   导出后立即校验 gzip 魔数（1f 8b），防止未来任何 docker 版本/导出方式
#   变化悄悄退回未压缩导出。
save_compressed() {  # $1=镜像名  $2=输出 .tar 路径
  docker save "$1" | gzip > "$2"
  if [ "$(head -c2 "$2" | od -An -tx1 | tr -d ' \n')" != "1f8b" ]; then
    echo "[错误] $2 导出内容未压缩（体积会膨胀约 3 倍），已删除。请检查 docker save 行为后重试。" >&2
    rm -f "$2"
    return 1
  fi
}

prune_old() {
  # 删除旧版本 custos-node tar，仅保留当前 custos-node-${TAG}.tar：
  # 否则 available_images() 会把过期版本也一起下发给节点、导致重复 docker load
  # （既浪费流量，又可能让节点误 load 到非当前版本镜像）。python-3-slim.tar
  # 不带版本号、永远只有一个且被覆盖写，无需清理。
  local keep="custos-node-${TAG}.tar"
  local removed=0
  for f in "$IMAGES_DIR"/custos-node-*.tar; do
    [ -e "$f" ] || continue
    [ "$(basename "$f")" = "$keep" ] && continue
    echo "==> 清理旧版本镜像包：$(basename "$f")"
    rm -f "$f"
    removed=1
  done
  [ "$removed" = 0 ] || echo "==> 已用新版本 ${TAG} 覆盖旧 tar"
}

build_keepalived() {
  local ctx="app/node_assets/keepalived"
  if [ ! -f "$ctx/Dockerfile" ]; then
    echo "[错误] 未找到 $ctx/Dockerfile" >&2
    return 1
  fi
  # ⚠ 铁律：基础镜像国内源优先。依次尝试国内镜像源作为 FROM 来源拉取 debian 基础镜像，
  #   全部不可达才回退官方源。base 仅决定 FROM 来源，产物仍打标 custos-node:${TAG}
  #   （节点 compose 用的就是这个名字），与 base 来自哪个源无关。
  local built=""
  for base in \
    "registry.cn-hangzhou.aliyuncs.com/library/debian:bookworm-slim" \
    "hub-mirror.c.163.com/library/debian:bookworm-slim" \
    "docker.m.daocloud.io/library/debian:bookworm-slim" \
    "dockerproxy.com/library/debian:bookworm-slim" ; do
    echo "==> 尝试国内源基础镜像构建 custos-node:${TAG}（base=$base）"
    if docker build --build-arg "NODE_BASE_IMAGE=$base" -t "custos-node:${TAG}" "$ctx" >/dev/null 2>&1; then
      built=1
      echo "==> 已从国内源构建 custos-node:${TAG}"
      break
    fi
  done
  if [ -z "$built" ]; then
    echo "==> 国内源基础镜像均不可用，回退官方源（debian:bookworm-slim）构建 custos-node:${TAG}"
    docker build -t "custos-node:${TAG}" "$ctx"
  fi
  echo "==> 导出 $IMAGES_DIR/custos-node-${TAG}.tar"
  # 只导出【这一个】包（带 v）。不再另存不带版本的同名副本：
  # available_images() 会把目录下每个 tar 都列出来让节点逐个 docker load，
  # 存两份同内容的包等于让每个节点多下载约 70MB 并重复 load 一次。
  save_compressed "custos-node:${TAG}" "$IMAGES_DIR/custos-node-${TAG}.tar"
  # 导出完成后，删掉目录里残留的旧版本 tar（自动「旧→新」替换）
  prune_old
}

save_agent_image() {
  # agent 容器用 python:3-slim；离线/国内环境节点拉不到，这里一并托管。
  # ⚠ 铁律：国内源优先。先尝试国内镜像源拉取，全部不可达才回退官方源；
  #   拉到后 retag 为 python:3-slim（节点 compose 用的就是这个名字），再 save。
  local img="python:3-slim"
  # 本地已有则直接复用：既避免每次安装都重下一遍，也保证「只保留一个镜像」——
  # 否则下面摘掉国内源标签后，下次 pull 会因本地无该 repo 记录而重新下载一份。
  if docker image inspect "$img" >/dev/null 2>&1; then
    echo "==> 本地已有 $img，跳过拉取（如需更新基础镜像，请先 docker rmi $img 再重跑）"
    save_compressed "$img" "$IMAGES_DIR/python-3-slim.tar"
    return 0
  fi
  local pulled=""
  for src in \
    "registry.cn-hangzhou.aliyuncs.com/library/python:3-slim" \
    "hub-mirror.c.163.com/library/python:3-slim" \
    "docker.m.daocloud.io/library/python:3-slim" \
    "dockerproxy.com/library/python:3-slim" ; do
    echo "==> 尝试国内源拉取 agent 基础镜像：$src"
    if docker pull "$src" >/dev/null 2>&1; then
      docker tag "$src" "$img"
      pulled=1
      echo "==> 已从国内源拉取并打标 $img"
      # 只保留一个镜像：docker tag 【不会】删除源标签，会留下「同一 IMAGE ID 两条记录」
      #（如 docker.m.daocloud.io/library/python:3-slim 与 python:3-slim）——看似两个镜像、
      # 实为同一份数据。这里摘掉国内源标签，只留节点 compose 实际使用的名字。
      # ⚠ 顺序必须「先 tag 再 untag」：此时数据由 $img 持有，rmi 只删标签、不删数据。
      docker rmi "$src" >/dev/null 2>&1 || true
      break
    fi
  done
  if [ -z "$pulled" ]; then
    echo "==> 国内源均不可用，回退官方源拉取 $img"
    docker pull "$img"
  fi
  save_compressed "$img" "$IMAGES_DIR/python-3-slim.tar"
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
