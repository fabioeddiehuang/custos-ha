# 极简 keepalived 镜像：在宿主机网卡挂载 VIP（VRRP 单播）+ check_mysql_master.sh 探活。
# 基础镜像：国内源优先（铁律）。默认官方 debian:bookworm-slim，构建时由
# scripts/build_node_images.sh 经 --build-arg NODE_BASE_IMAGE=<国内镜像> 覆盖为国内源，
# 不可达时回退官方源；产物仍打标 custos-node:<版本>（与 base 来源无关）。
ARG NODE_BASE_IMAGE=debian:bookworm-slim
FROM ${NODE_BASE_IMAGE}

# 构建时可指定 keepalived 版本：docker compose build --build-arg KEEPALIVED_VERSION=2.2.7
ARG KEEPALIVED_VERSION=2.2.7
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Shanghai

# apt：国内镜像源【优先】，官方源兜底 + BuildKit 缓存挂载（与服务端 Dockerfile 同一套做法）
#   · 每个候选源的尝试都含「update + install」两步，任一步失败即换下一个源；全失败才还原
#     官方源重试（每次尝试前先还原官方源，否则 sed 叠加会重复试同一个源）。
#   · 兼容 deb822（debian.sources）与传统 sources.list 两种写法，并同时替换
#     deb.debian.org 与 security.debian.org。
#   · ⚠ 只缓存 /var/cache/apt（deb 归档），【绝不要缓存 /var/lib/apt/lists】：BuildKit 缓存按
#     【目标路径】共享，缓存索引会导致不同基础镜像互相复用对方索引（本镜像是 bookworm，
#     服务端镜像是 trixie，实测已发生过 "Unable to locate package keepalived"）。
#   · 用独立 id=custos-node-apt，避免与服务端镜像的缓存串味。
#   · Acquire::*::Timeout 收紧单源等待，避免某个源不可达时构建长时间卡住。
RUN --mount=type=cache,id=custos-node-apt,target=/var/cache/apt,sharing=locked \
    set -e; \
    tar cf /tmp/apt-src-bak.tar /etc/apt/sources.list /etc/apt/sources.list.d 2>/dev/null || true; \
    PKGS="keepalived=1:${KEEPALIVED_VERSION}* iproute2 procps default-mysql-client tzdata ca-certificates"; \
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

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 由 docker-compose 的 build 上下文传入 keepalived.conf / check 脚本后挂载；
# entrypoint 在启动前自动校正网卡，再前台运行并输出日志到控制台，便于 docker logs 排障。
CMD ["/entrypoint.sh"]
