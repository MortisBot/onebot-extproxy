# onebot-extproxy —— 最小可运行镜像
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 先装依赖层（仅 pyproject/README/src，避免把运行时配置打进镜像）
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

# 运行期挂载点：/config/consumers.json 与 /state/state.json（均只读挂载）
VOLUME ["/config", "/state"]
EXPOSE 8090

# 中继只应部署在内网/容器网络中；不要映射到公网。
CMD ["onebot-extproxy", "--config", "/config/consumers.json"]
