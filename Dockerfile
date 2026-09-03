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

# 统一版本真相：构建时若传入 --build-arg VERSION（由 docker-compose 的 .env 提供），
# 以它为准并写回镜像内 app/VERSION；否则回退使用源码内的 app/VERSION。
# 这样 镜像标签 / LABEL / 容器内 app/VERSION 三处一致，Web 也能读到版本。
RUN SRC_VER=$(tr -d '[:space:]' < app/VERSION 2>/dev/null || echo ""); \
    ARG_VER=$(printf '%s' "${VERSION}" | tr -d '[:space:]'); \
    if [ -n "${SRC_VER}" ] && [ -n "${ARG_VER}" ] && [ "${SRC_VER}" != "${ARG_VER}" ]; then \
      echo "ERROR: 版本不一致 —— app/VERSION=${SRC_VER}，构建参数 VERSION=${ARG_VER}" >&2; \
      echo "       请改用 ./deploy.sh（或 make build），它会自动同步 .env 后再构建。" >&2; \
      exit 1; \
    fi; \
    VER="${SRC_VER:-${ARG_VER}}"; \
    printf '%s' "${VER}" > app/VERSION && \
    echo "VERSION=${VER}" > /app/.version.env

RUN chmod +x docker-entrypoint.sh

EXPOSE 28888

ENTRYPOINT ["./docker-entrypoint.sh"]
