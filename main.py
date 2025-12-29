from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.task_config import Base, TaskConfig
from services.task_service import TaskService
from scheduler.task_scheduler import TaskScheduler
from config.settings import Settings
import logging
import atexit
import time

# 配置日志
logging.basicConfig(level=logging.INFO)

def init_database():
    """初始化数据库"""
    engine = create_engine(Settings.DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return engine, Session()

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
        print("已创建示例任务数据")

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
    
    # 注册退出处理
    def shutdown():
        scheduler.stop()
        db_session.close()
    
    atexit.register(shutdown)
    
    try:
        # 保持程序运行
        print("定时任务容器已启动，按 Ctrl+C 退出")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("接收到退出信号")
        shutdown()

if __name__ == "__main__":
    main()