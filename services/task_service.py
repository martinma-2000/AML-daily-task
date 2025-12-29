from sqlalchemy.orm import Session
from models.task_config import TaskConfig

class TaskService:
    def __init__(self, db_session: Session):
        self.db_session = db_session
    
    def get_all_enabled_tasks(self):
        """获取所有启用的任务配置"""
        return self.db_session.query(TaskConfig).filter(
            TaskConfig.enabled == True
        ).all()
    
    def get_task_by_id(self, task_id: int):
        """根据ID获取任务配置"""
        return self.db_session.query(TaskConfig).filter(
            TaskConfig.id == task_id
        ).first()
    
    def get_task_by_name(self, task_name: str):
        """根据名称获取任务配置"""
        return self.db_session.query(TaskConfig).filter(
            TaskConfig.task_name == task_name
        ).first()
    
    def update_task_data(self, task_id: int, new_data: dict):
        """更新任务数据"""
        task = self.get_task_by_id(task_id)
        if task:
            task.task_data = new_data
            self.db_session.commit()
            return task
        return None