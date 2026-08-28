FROM python:3.11-slim

ARG VERSION
LABEL org.opencontainers.image.version="${VERSION}"

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
RUN if [ -z "${VERSION}" ] && [ -f app/VERSION ]; then \
      VERSION=$(cat app/VERSION | tr -d '[:space:]'); \
    fi; \
    printf '%s' "${VERSION}" > app/VERSION && \
    echo "VERSION=${VERSION}" > /app/.version.env

RUN chmod +x docker-entrypoint.sh

EXPOSE 28888

ENTRYPOINT ["./docker-entrypoint.sh"]
