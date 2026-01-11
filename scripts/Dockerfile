FROM python:3.12-slim

# 设置工作目录
WORKDIR /workspace/scripts

# 设置环境变量
ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 使用阿里云镜像源加速（可选，如果网络慢可以启用）
# RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# 一次性安装所有系统依赖和Python依赖，减少层数
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client && \
    pip install --upgrade pip && \
    pip install \
    requests>=2.31.0 \
    mysql-connector-python>=8.2.0 \
    psycopg2-binary>=2.9.9 \
    apache-dolphinscheduler>=3.0.0 && \
    apt-get purge -y gcc && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# 设置默认命令
CMD ["tail", "-f", "/dev/null"]
