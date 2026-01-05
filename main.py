from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.task_config import Base, TaskConfig
from models.dify_result import DifyCallResult  # 导入DifyCallResult以确保模型被注册到Base中
from services.task_service import TaskService
from scheduler.task_scheduler import TaskScheduler
from config.settings import Settings
from api.task_api import create_app
import logging
import atexit
import time
import threading
from flask import Flask

# 配置日志
logging.basicConfig(level=logging.INFO)

def init_database():
    """初始化数据库"""
    engine = create_engine(Settings.DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    return engine, db_session

def create_sample_data(db_session):
    """创建示例数据"""
    from datetime import datetime
    
    # 检查是否已有数据
    existing_task = db_session.query(TaskConfig).filter(
        TaskConfig.task_name == 'sample_task'
    ).first()
    
    if not existing_task:
        sample_task = TaskConfig(
            task_name='sample_task',
            task_schedule='*/30 * * * *',  # 每30分钟执行一次
            task_data={
                'type': 'data_sync',
                'source': 'api_endpoint',
                'target': 'database',
                'params': {'batch_size': 100}
            },
            enabled=True
        )
        db_session.add(sample_task)
        db_session.commit()
        logging.info("已创建示例任务数据")

def main():
    # 初始化数据库
    engine, db_session = init_database()
    
    # 创建服务实例
    task_service = TaskService(db_session)
    
    # 创建调度器
    scheduler = TaskScheduler(Settings.DATABASE_URL)
    scheduler.set_task_service(task_service, sessionmaker(bind=engine))
    
    # 创建示例数据
    create_sample_data(db_session)
    
    # 重新加载任务
    scheduler.reload_tasks()
    
    # 启动调度器
    scheduler.start()
    
    # 创建API应用
    api_app = create_app(scheduler, task_service)
    
    # 注册退出处理
    def shutdown():
        scheduler.stop()
        db_session.close()
        engine.dispose()
    
    atexit.register(shutdown)
    
    # 在单独的线程中启动API服务器
    def run_api():
        api_app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    
    api_thread = threading.Thread(target=run_api, daemon=True)
    api_thread.start()
    
    try:
        # 保持程序运行
        logging.info("定时任务容器已启动，API服务运行在 http://localhost:5000")
        logging.info("可用API接口:")
        logging.info("  GET  /health - 健康检查")
        logging.info("  POST /tasks/trigger/<task_id> - 通过ID手动触发任务")
        logging.info("  POST /tasks/trigger_by_name/<task_name> - 通过名称手动触发任务")
        logging.info("  GET  /tasks/list - 列出所有任务")
        logging.info("按 Ctrl+C 退出")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("接收到退出信号")
        shutdown()

if __name__ == "__main__":
    main()