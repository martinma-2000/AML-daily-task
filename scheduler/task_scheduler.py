from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.task_service import TaskService
import logging

logger = logging.getLogger(__name__)

def execute_task_function(task_service_class, db_session_class, task_id):
    """可序列化的任务执行函数"""
    # 重新创建数据库会话
    from config.settings import Settings
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    engine = create_engine(Settings.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    
    try:
        # 重新获取任务配置
        task_service = task_service_class(db_session)
        task_config = task_service.get_task_by_id(task_id)
        
        if not task_config:
            logger.error(f"找不到ID为 {task_id} 的任务")
            return
        
        logger.info(f"执行任务: {task_config.task_name}")
        # 这里实现具体的任务逻辑
        # task_config.task_data 包含从数据库获取的具体数据
        logger.info(f"执行任务 {task_config.task_name}，数据: {task_config.task_data}")
        
        # 可以根据任务类型执行不同的逻辑
        _run_task_logic(task_config, task_id)
    except Exception as e:
        logger.error(f"执行任务失败: {str(e)}")
    finally:
        db_session.close()

def _run_task_logic(task_config, task_id=None):
    """具体的任务逻辑实现"""
    # 根据任务名称或类型执行不同的业务逻辑
    task_data = task_config.task_data or {}
    
    # 添加任务ID到task_data中，以便在处理函数中使用
    if task_id is not None:
        task_data['task_id'] = task_id
    
    # 示例：不同任务类型的不同处理逻辑
    task_type = task_data.get('type', 'default')
    
    if task_type == 'data_sync':
        _handle_data_sync(task_data)
    elif task_type == 'report_generation':
        _handle_report_generation(task_data)
    elif task_type == 'batch_api_call':
        # 导入批量API处理模块
        from services.batch_api_service import BatchApiService
        BatchApiService().handle_batch_api_call(task_data)
    else:
        # 默认处理逻辑
        _handle_default_task(task_data)

def _handle_data_sync(task_data):
    """处理数据同步任务"""
    logger.info(f"执行数据同步任务，配置: {task_data}")

def _handle_report_generation(task_data):
    """处理报表生成任务"""
    logger.info(f"执行报表生成任务，配置: {task_data}")

def _handle_default_task(task_data):
    """默认任务处理"""
    logger.info(f"执行默认任务，数据: {task_data}")

class TaskScheduler:
    def __init__(self, db_url: str):
        self.db_url = db_url
        # 使用内存存储而不是SQLAlchemy存储以避免序列化问题
        self.scheduler = BackgroundScheduler()
        self.task_service = None
        self.db_session_class = None
    
    def set_task_service(self, task_service: TaskService, db_session_class):
        """设置任务服务"""
        self.task_service = task_service
        self.db_session_class = db_session_class
    
    def add_task(self, task_config):
        """添加定时任务"""
        if not self.task_service:
            raise ValueError("TaskService未设置")
        
        # 解析定时配置
        cron_expression = task_config.task_schedule
        if not cron_expression:
            logger.warning(f"任务 {task_config.task_name} 没有定时配置，跳过")
            return
        
        # 添加到调度器
        try:
            trigger = CronTrigger.from_crontab(cron_expression)
            self.scheduler.add_job(
                func=execute_task_function,
                args=[TaskService, self.db_session_class, task_config.id],
                trigger=trigger,
                id=f"task_{task_config.id}",
                name=task_config.task_name,
                replace_existing=True
            )
            logger.info(f"已添加任务: {task_config.task_name} - {cron_expression}")
        except Exception as e:
            logger.error(f"添加任务 {task_config.task_name} 失败: {str(e)}")
    
    def start(self):
        """启动调度器"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("任务调度器已启动")
    
    def stop(self):
        """停止调度器"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("任务调度器已停止")
    
    def reload_tasks(self):
        """重新加载所有任务"""
        # 先清除所有现有任务
        self.scheduler.remove_all_jobs()
        
        # 从数据库获取所有启用的任务
        tasks = self.task_service.get_all_enabled_tasks()
        
        for task in tasks:
            self.add_task(task)
        
        logger.info(f"重新加载了 {len(tasks)} 个任务")