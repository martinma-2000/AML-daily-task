# Docker 部署指南

## 构建和运行

### 使用Docker命令行

```bash
# 构建镜像
docker build -t aml-daily-task .

# 运行容器（使用环境变量）
docker run -p 5000:5000 \
  -e DATABASE_URL="postgresql+psycopg2://root:postgre%40123@localhost:35432/task_container_db" \
  -e TASK_SAMPLE="*/30 * * * *" \
  -e TASK_DAILY="0 2 * * *" \
  aml-daily-task
```

### 使用Docker Compose

```bash
# 构建并运行
docker-compose up --build

# 后台运行
docker-compose up -d --build
```

## 环境变量配置

应用支持以下环境变量：

- `DATABASE_URL`: 数据库连接URL
- `TASK_SAMPLE`: 示例任务调度表达式
- `TASK_DAILY`: 每日任务调度表达式

配置信息也可以通过 `.env` 文件管理。

## 端口

- 应用端口: 5000

## 数据库连接

如果你使用外部数据库，需要确保数据库URL配置正确，并且数据库服务可访问。

## 日志

应用日志输出到控制台，使用 `docker logs` 命令查看。

## 健康检查

应用启动后会输出以下API端点信息：

- GET  /health - 健康检查
- POST /tasks/trigger/<task_id> - 通过ID手动触发任务
- POST /tasks/trigger_by_name/<task_name> - 通过名称手动触发任务
- GET  /tasks/list - 列出所有任务