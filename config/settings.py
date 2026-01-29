import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///task_container.db')
    # 从环境变量中获取任务配置
    TASK_CONFIGS = {}
    
    # 并发任务相关配置
    TASK_CONCURRENCY = int(os.getenv('TASK_CONCURRENCY', '3'))  # 默认并发数为3
    
    # CSV处理相关配置
    CSV_PROCESSING_CHUNK_SIZE = int(os.getenv('CSV_PROCESSING_CHUNK_SIZE', '50000'))
    CSV_PROCESSING_TEMP_DIR = os.getenv('CSV_PROCESSING_TEMP_DIR', './temp_csv_processing')
    
    # 动态获取所有任务配置
    for key, value in os.environ.items():
        if key.startswith('TASK_'):
            TASK_CONFIGS[key] = value