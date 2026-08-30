# 极简 keepalived 镜像：在宿主机网卡挂载 VIP（VRRP 单播）+ check_mysql_master.sh 探活。
FROM debian:bookworm-slim

# 构建时可指定 keepalived 版本：docker compose build --build-arg KEEPALIVED_VERSION=2.2.7
ARG KEEPALIVED_VERSION=2.2.7
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Shanghai

# 国内镜像源（阿里云）加速 apt；同时覆盖 deb.debian.org 与 security.debian.org，
# 并兼容 deb822（bookworm 的 debian.sources）与传统 sources.list 两种写法。
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com/debian-security|g' \
        /etc/apt/sources.list.d/debian.sources 2>/dev/null || \
    sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com/debian-security|g' \
        /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update \
 && apt-get install -y --no-install-recommends \
        keepalived=1:${KEEPALIVED_VERSION}* \
        iproute2 \
        procps \
        default-mysql-client \
        tzdata \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 由 docker-compose 的 build 上下文传入 keepalived.conf / check 脚本后挂载；
# entrypoint 在启动前自动校正网卡，再前台运行并输出日志到控制台，便于 docker logs 排障。
CMD ["/entrypoint.sh"]
