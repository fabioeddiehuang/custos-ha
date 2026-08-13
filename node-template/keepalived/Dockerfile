# 极简 keepalived 镜像：在宿主机网卡上挂载 VIP（VRRP 单播），
# 通过 check_mysql_master.sh 读取本机与管理库状态决定是否持有 VIP。
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
# 使用国内镜像源（阿里云）加速 apt 安装；若不可用会自动回退 deb.debian.org
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources 2>/dev/null || \
    sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update \
 && apt-get install -y --no-install-recommends \
        keepalived \
        iproute2 \
        procps \
        default-mysql-client \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# entrypoint 在启动前自动校正网卡，再前台运行并输出日志到控制台，便于 docker logs 排障。
CMD ["/entrypoint.sh"]
