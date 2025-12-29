from sqlalchemy import create_engine, Column, Integer, String, Boolean, JSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class TaskConfig(Base):
    __tablename__ = 'task_configs'
    
    id = Column(Integer, primary_key=True)
    task_name = Column(String(255), nullable=False)
    task_schedule = Column(String(255))  # 从.env获取的定时配置
    task_data = Column(JSON)            # 从数据库获取的具体数据
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<TaskConfig(id={self.id}, name={self.task_name})>"