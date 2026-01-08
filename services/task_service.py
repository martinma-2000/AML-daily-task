from sqlalchemy.orm import Session
from models.task_config import TaskConfig
from config.settings import Settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class TaskService:
    def __init__(self, db_session=None):
        # 不再直接存储会话，而是提供会话工厂方法
        self._db_session = db_session
        # 为独立操作创建引擎（仅在需要时）
        self.engine = None
        if db_session is None:
            self.engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
    
    def get_session(self):
        """获取数据库会话，优先使用传入的会话，否则创建新的"""
        if self._db_session:
            return self._db_session
        elif self.engine:
            Session = sessionmaker(bind=self.engine)
            return Session()
        else:
            # 如果没有预设引擎，创建临时会话
            engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
            Session = sessionmaker(bind=engine)
            return Session()
    
    def get_all_enabled_tasks(self, db_session=None):
        """获取所有启用的任务配置"""
        session_to_use = db_session or self.get_session()
        # 如果我们创建了临时会话，需要在方法结束时关闭它
        temp_session = db_session is None and self._db_session is None
        try:
            result = session_to_use.query(TaskConfig).filter(
                TaskConfig.enabled == True
            ).all()
            return result
        finally:
            if temp_session:
                session_to_use.close()
    
    def get_task_by_id(self, task_id: int, db_session=None):
        """根据ID获取任务配置"""
        session_to_use = db_session or self.get_session()
        # 如果我们创建了临时会话，需要在方法结束时关闭它
        temp_session = db_session is None and self._db_session is None
        try:
            result = session_to_use.query(TaskConfig).filter(
                TaskConfig.id == task_id
            ).first()
            return result
        finally:
            if temp_session:
                session_to_use.close()
    
    def get_task_by_name(self, task_name: str, db_session=None):
        """根据名称获取任务配置"""
        session_to_use = db_session or self.get_session()
        # 如果我们创建了临时会话，需要在方法结束时关闭它
        temp_session = db_session is None and self._db_session is None
        try:
            result = session_to_use.query(TaskConfig).filter(
                TaskConfig.task_name == task_name
            ).first()
            return result
        finally:
            if temp_session:
                session_to_use.close()
    
    def update_task_data(self, task_id: int, new_data: dict, db_session=None):
        """更新任务数据"""
        session_to_use = db_session or self.get_session()
        # 如果我们创建了临时会话，需要在方法结束时关闭它
        temp_session = db_session is None and self._db_session is None
        try:
            task = self.get_task_by_id(task_id, session_to_use)
            if task:
                task.task_data = new_data
                session_to_use.commit()
                return task
            return None
        finally:
            if temp_session:
                session_to_use.close()