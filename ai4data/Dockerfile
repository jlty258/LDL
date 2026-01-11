# 基于 Jupyter 科学计算镜像
FROM jupyter/scipy-notebook:latest

# 设置工作目录
WORKDIR /home/jovyan

# 复制 requirements.txt
COPY requirements.txt /tmp/requirements.txt

# 安装 Python 依赖包
# 使用 --no-cache-dir 减少镜像大小，使用 --user 安装到用户目录
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt && \
    # 清理 pip 缓存
    pip cache purge || true

# 切换到工作目录
WORKDIR /home/jovyan/work

# 保持原始镜像的启动命令
# 使用 start-notebook.sh 作为默认命令
