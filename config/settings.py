import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///task_container.db')
    # 从环境变量中获取任务配置
    TASK_CONFIGS = {}
    
    # 动态获取所有任务配置
    for key, value in os.environ.items():
        if key.startswith('TASK_'):
            TASK_CONFIGS[key] = value