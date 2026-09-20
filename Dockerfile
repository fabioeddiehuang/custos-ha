FROM python:3.11-slim

WORKDIR /app

# 先装依赖，且必须在【任何随版本变化的指令】之前：
# requirements.txt 未变时该层命中缓存，源码改动或升版都不会触发重装依赖
# （pip 层 83.6MB、apt 层 150MB，是全镜像最大的两层；这两层一旦需要重跑就是构建慢的主因，
#   故下面两层都用 BuildKit 缓存挂载，把下载缓存留在宿主机，重跑时零下载）。
COPY requirements.txt .

# pip：国内镜像源【优先】，装失败才回退官方源。
#   1) 按序探测国内候选源（腾讯 → 清华 → 阿里 → 华为），取第一个可达的；全部不可达则用官方；
#   2) ⚠ 不能"常挂" --extra-index-url 官方源：pip 会对【每个包】都同时查两个索引（含境外
#      pypi.org），逐包跨境请求会显著拖慢解析与下载 —— 兜底必须用"整体失败后重试"来实现，
#      而不是让两个索引并存。
#   3) 用 BuildKit 缓存挂载把 wheel 缓存放到宿主机：即使本层因命令变更而重跑，也【不再
#      重新下载】，只做本地安装（重建慢的主因）。缓存不在镜像里，不增加镜像体积。
RUN --mount=type=cache,id=custos-pip,target=/root/.cache/pip \
    set -e; \
    pick_url() { \
      for u in "$@"; do \
        if python3 -c 'import sys,urllib.request;urllib.request.urlopen(sys.argv[1],timeout=6)' "$u" >/dev/null 2>&1; then \
          echo "$u"; return 0; \
        fi; \
      done; \
      echo ""; \
    }; \
    PM=$(pick_url https://mirrors.tencent.com/pypi/simple/ \
                  https://pypi.tuna.tsinghua.edu.cn/simple/ \
                  https://mirrors.aliyun.com/pypi/simple/ \
                  https://repo.huaweicloud.com/pypi/simple/); \
    if [ -n "$PM" ] && pip install -i "$PM" --timeout 120 --retries 10 -r requirements.txt; then \
      echo "pip: 已用国内源 $PM 安装完成"; \
    else \
      echo "pip: 国内源不可用，回退官方源重试"; \
      pip install --timeout 120 --retries 10 -r requirements.txt; \
    fi

# 数据迁移必须的客户端与编排工具：
#   · mariadb-client —— 逻辑通道 A 的 mariadb-dump / mariadb（连远程源库导出/导入）。
#   · openssh-client + rsync + sshpass —— 物理通道 B 的编排：管理器经 SSH 到【源/目标节点】
#     执行 mariabackup（备份在源节点、copy-back 在目标节点，mariabackup 只能读本地 datadir，
#     无法备份远程服务器，故管理器【不】预装 mariabackup，节点自带即可）。
# ⚠ 注意：脱离了这些工具，/migration 页面能打开但一点即错，务必保留此层。
#
# apt：国内镜像源【优先】，官方源兜底 + BuildKit 缓存挂载（本层是全镜像最大的一层，约 150MB）：
#   · 体积构成已实测：mariadb-client 65MB（mariadb-dump 在此包）+ perl 约 49MB（mariadb-client
#     硬依赖 <perl:any>，无法剔除）+ openssh-client/rsync/sshpass 等。既然体积无法再砍，
#     就只能砍掉"重复下载"：用缓存挂载把 .deb 归档留在宿主机，重跑时零下载。
#   · ⚠ 只缓存 /var/cache/apt（deb 归档），【绝不要缓存 /var/lib/apt/lists】：
#     BuildKit 的缓存按【目标路径】共享，一旦缓存索引，不同基础镜像会互相复用对方的索引
#     （例如 python:3.11-slim 从 bookworm 升到 trixie，或本镜像与节点镜像之间串味），
#     apt 就会报 "Unable to locate package xxx"（实测踩过）。索引必须每次随当前基础镜像刷新。
#   · 每个镜像用独立 id=，避免与其它 Dockerfile 的缓存串味。
#   · 每个候选源的尝试都含「update + install」两步，任一步失败即换下一个源；全失败才还原
#     官方源重试（每次尝试前先还原官方源，否则 sed 叠加会重复试同一个源）。
#   · Acquire::*::Timeout 收紧单源等待，避免某个源不可达时构建长时间卡住。
RUN --mount=type=cache,id=custos-server-apt,target=/var/cache/apt,sharing=locked \
    set -e; \
    tar cf /tmp/apt-src-bak.tar /etc/apt/sources.list /etc/apt/sources.list.d 2>/dev/null || true; \
    PKGS="mariadb-client openssh-client rsync sshpass"; \
    apt_try() { \
      apt-get -o Acquire::Retries=1 -o Acquire::http::Timeout=15 -o Acquire::https::Timeout=15 update \
        && apt-get install -y --no-install-recommends $PKGS; \
    }; \
    APT_OK=0; \
    for M in mirrors.tencent.com/debian mirrors.tuna.tsinghua.edu.cn/debian \
             mirrors.aliyun.com/debian repo.huaweicloud.com/debian; do \
      H=$(echo "$M" | sed 's|/debian$||'); \
      tar xf /tmp/apt-src-bak.tar -C / 2>/dev/null || true; \
      sed -i "s|deb.debian.org|$H|g; s|security.debian.org|$H/debian-security|g" \
          /etc/apt/sources.list.d/debian.sources 2>/dev/null || \
      sed -i "s|deb.debian.org|$H|g; s|security.debian.org|$H/debian-security|g" \
          /etc/apt/sources.list 2>/dev/null || true; \
      echo "apt: 尝试 $H"; \
      if apt_try; then APT_OK=1; break; fi; \
    done; \
    if [ "$APT_OK" != "1" ]; then \
      echo "apt: 国内源均不可用，回退官方源（deb.debian.org）"; \
      tar xf /tmp/apt-src-bak.tar -C / 2>/dev/null || true; \
      apt-get -o Acquire::Retries=3 update \
        && apt-get install -y --no-install-recommends $PKGS; \
    fi; \
    rm -rf /var/lib/apt/lists/* /tmp/apt-src-bak.tar

# 依赖装完后再引入随版本变化的元数据：LABEL 内含 ${VERSION}，
# 若放在 pip 之前，每次升版都会使该层及其后所有层缓存失效 → 依赖被反复重装。
ARG VERSION
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.title="Custos"
LABEL org.opencontainers.image.description="MySQL/MariaDB 高可用管理：故障切换、复制自愈、配置巡检"

COPY . .

# 版本单一来源 = .env 的 VERSION=（由 docker-compose 经 build-arg 传入、再由 environment 注入容器）。
# 源码树不再维护 app/VERSION（v2.2.20 起废弃）；应用只读环境变量 VERSION（config.APP_VERSION）。
# 此处仅把实际版本写入镜像内元数据文件，供排障时 docker exec 查阅。
RUN ARG_VER=$(printf '%s' "${VERSION}" | tr -d '[:space:]'); \
    if [ -z "${ARG_VER}" ]; then \
      echo "ERROR: 未提供构建参数 VERSION —— 请改用 ./scripts/deploy.sh（或 make build），它从 .env 读取后构建。" >&2; \
      exit 1; \
    fi; \
    echo "VERSION=${ARG_VER}" > /app/.version.env

RUN chmod +x docker-entrypoint.sh

EXPOSE 28888

ENTRYPOINT ["./docker-entrypoint.sh"]
