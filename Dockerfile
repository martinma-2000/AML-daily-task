# 使用官方Python运行时作为基础镜像
FROM python:3.13-slim

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 创建非root用户
RUN adduser --disabled-password --gecos '' appuser

# 设置工作目录
WORKDIR /app

# 升级pip并安装typing-extensions以解决Python 3.13兼容性问题
RUN pip install --upgrade pip && \
    pip install --no-cache-dir typing-extensions==4.15.0

# 复制项目依赖文件
COPY requirements.txt .

# 安装项目依赖（使用更具体的版本）
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir SQLAlchemy==2.0.45 APScheduler==3.10.4 && \
    # 清理缓存以减小镜像大小
    pip cache purge

# 复制项目文件到工作目录
COPY . .

# 更改文件所有者
RUN chown -R appuser:appuser /app

# 切换到非root用户
USER appuser

# 暴露应用端口
EXPOSE 5000

# 运行应用
CMD ["python", "main.py"]