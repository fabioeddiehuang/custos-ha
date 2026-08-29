FROM python:3.11-slim

ARG VERSION
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.title="Custos"
LABEL org.opencontainers.image.description="MySQL/MariaDB 高可用管理：故障切换、复制自愈、配置巡检"

WORKDIR /app

COPY requirements.txt .
# 使用国内镜像源加速安装，并加大超时与重试；如需其他源可替换 -i 地址
RUN pip install --no-cache-dir \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --timeout 120 --retries 10 \
    -r requirements.txt

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
