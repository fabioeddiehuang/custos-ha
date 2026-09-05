FROM python:3.11-slim

WORKDIR /app

# 先装依赖，且必须在【任何随版本变化的指令】之前：
# requirements.txt 未变时该层命中缓存，源码改动或升版都不会触发重装依赖
# （pip 层约 94MB，每次重跑是构建慢的主因）。
COPY requirements.txt .
# 使用国内镜像源加速安装，并加大超时与重试；如需其他源可替换 -i 地址
RUN pip install --no-cache-dir \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --timeout 120 --retries 10 \
    -r requirements.txt

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
      echo "ERROR: 未提供构建参数 VERSION —— 请改用 ./deploy.sh（或 make build），它从 .env 读取后构建。" >&2; \
      exit 1; \
    fi; \
    echo "VERSION=${ARG_VER}" > /app/.version.env

RUN chmod +x docker-entrypoint.sh

EXPOSE 28888

ENTRYPOINT ["./docker-entrypoint.sh"]
