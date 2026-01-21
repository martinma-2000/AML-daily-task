# AML Daily Task Container

## 项目概述

这是一个定时任务容器系统，支持多种类型的定时任务，包括数据同步、报表生成、批量API调用等。

## 新增功能：UNL文件下载

本系统新增了一个重要功能：在定时任务开始执行之前，自动调用POST接口下载`.unl.gz`文件。

### 配置方式

在 `.env` 文件中配置以下参数：

```bash
# UNL文件下载配置
UNL_DOWNLOAD_URL=http://your-api-endpoint.com/api/download  # 下载接口的URL
UNL_FILE_NAME_LIST=file1.unl.gz,file2.unl.gz              # 要下载的文件名列表，逗号分隔
UNL_FILE_SVR_ID=server123                                  # 文件服务器ID
UNL_RMT_PUB_PATH=/public/path                             # 远程发布路径
```

### 功能特点

- 在每次定时任务执行前自动下载UNL文件
- 通过环境变量配置，保证安全性
- 非阻塞模式，下载失败不会影响主任务执行
- 自动临时文件管理
- 完整的日志记录
- 支持处理特定名称的UNL文件（t3b_case_aml_llmp.unl.gz 或 T3B_CASE_AML_LLMP.unl.gz）
- 自动跳过所有CSV文件，仅处理目标UNL文件

## 系统架构

- **调度器**: 使用APScheduler进行任务调度
- **服务层**: 包含各种业务逻辑处理
- **API层**: 提供RESTful接口用于手动触发任务
- **配置管理**: 通过环境变量进行灵活配置

## 运行方式

```bash
python main.py
```

系统启动后会在5000端口提供API服务。
