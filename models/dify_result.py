from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
# 从task_config导入Base，确保使用同一个Base
from models.task_config import Base

class DifyCallResult(Base):
    __tablename__ = 'dify_call_results'
    
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('task_configs.id'))  # 关联到任务配置表
    # 使用字符串引用以避免循环导入问题，并指定back_populates与TaskConfig中一致
    task_config = relationship('TaskConfig', back_populates='dify_call_results', lazy='select')
    upload_api_response = Column(JSON)  # 存储上传API的响应
    run_response = Column(JSON)  # 存储运行响应
    parsed_result = Column(JSON)  # 解析结果

    execution_time = Column(DateTime, default=datetime.utcnow)  # 执行时间
    status = Column(String(50), default='pending')  # 状态，默认为pending
    
    def __repr__(self):
        return f"<DifyCallResult(id={self.id}, task_id={self.task_id}, status={self.status})>"