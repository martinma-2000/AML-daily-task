from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class TaskConfig(Base):
    __tablename__ = 'task_configs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_name = Column(String(255), nullable=False)
    task_schedule = Column(String(255), nullable=True)
    task_data = Column(JSON, nullable=True)
    enabled = Column(Boolean, default=True, nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=True)
    
    # 添加反向关系，与DifyCallResult关联
    dify_call_results = relationship('DifyCallResult', back_populates='task_config', lazy='select')