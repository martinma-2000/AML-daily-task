from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.task_service import TaskService
import logging
import os
import requests
import csv
import json
from datetime import datetime

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
        print(f"执行任务 {task_config.task_name}，数据: {task_config.task_data}")
        
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
        _handle_batch_api_call(task_data)
    else:
        # 默认处理逻辑
        _handle_default_task(task_data)

def _handle_data_sync(task_data):
    """处理数据同步任务"""
    print(f"执行数据同步任务，配置: {task_data}")

def _handle_report_generation(task_data):
    """处理报表生成任务"""
    print(f"执行报表生成任务，配置: {task_data}")

def _handle_batch_api_call(task_data):
    """处理批量API调用任务"""
    print(f"执行批量API调用任务，配置: {task_data}")
    
    api_endpoint = task_data.get('api_endpoint')
    csv_file_path = task_data.get('csv_file_path')
    result_table = task_data.get('result_table')
    api_key = task_data.get('API-KEY')
    
    if not api_endpoint or not csv_file_path:
        print("错误：缺少api_endpoint或csv_file_path参数")
        return
    
    # 检查CSV目录是否存在
    if not os.path.exists(csv_file_path) or not os.path.isdir(csv_file_path):
        print(f"错误：CSV目录不存在: {csv_file_path}")
        return
    
    # 获取目录中的所有CSV文件
    csv_files = [f for f in os.listdir(csv_file_path) if f.lower().endswith('.csv')]
    
    if not csv_files:
        print(f"警告：在目录 {csv_file_path} 中未找到CSV文件")
        return
    
    # 遍历CSV目录中的所有CSV文件
    for csv_file in csv_files:
        file_path = os.path.join(csv_file_path, csv_file)
        print(f"处理CSV文件: {file_path}")
        
        try:
            # 尝试以不同的编码读取CSV文件
            csv_content = None
            for encoding in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        csv_content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if csv_content is None:
                print(f"错误：无法使用常见编码读取CSV文件 {file_path}")
                continue
            
            # 将内容转换为字符串流以供csv模块使用
            import io
            import csv
            
            # 将内容转换为字符串流
            csv_string_io = io.StringIO(csv_content)
            
            # 读取CSV内容，没有列名，直接处理数据行
            csv_reader_obj = csv.reader(csv_string_io)
            
            # 获取列数以确定列名
            csv_string_io.seek(0)  # 重置流位置
            first_row = next(csv_reader_obj, None)
            if first_row:
                fieldnames = [f"column_{i}" for i in range(len(first_row))]
                # 重置流位置并创建DictReader
                csv_string_io.seek(0)
                csv_reader = csv.DictReader(csv_string_io, fieldnames=fieldnames)
            else:
                # 如果没有数据行，创建空的reader
                csv_reader = []
            
            for row_idx, row in enumerate(csv_reader):
                # 构建API请求
                headers = {
                    'Authorization': f'Bearer {api_key}' if api_key else ''
                }
                
                # 获取第五列的数据作为交易流水号（如果存在）
                transaction_id = "N/A"
                row_values = list(row.values())
                if len(row_values) >= 5:
                    transaction_id = row_values[4]  # 第五列（索引为4）
                
                # 准备上传的CSV文件（单行数据）
                import io
                # 创建仅包含当前行数据的CSV内容
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=row.keys())
                # 写入表头和当前行
                writer.writeheader()
                writer.writerow(row)
                csv_row_content = output.getvalue()
                csv_row_io = io.BytesIO(csv_row_content.encode('utf-8'))
                
                files = {
                    'file': (f'row_{row_idx+1}_{os.path.basename(file_path)}', csv_row_io, 'text/csv')
                }
                
                # 将CSV文件的行数据作为表单数据发送
                data = {}
                for key, value in row.items():
                    data[key] = value
                
                try:
                    response = requests.post(
                        f"{api_endpoint}/files/upload",
                        headers=headers,
                        files=files,
                        data=data
                    )
                    print(f"API调用结果 (文件 {os.path.basename(file_path)}, 行 {row_idx+1}, 交易流水号: {transaction_id}): {response.status_code}")
                    
                    if result_table:
                        # 从调用上下文中获取任务ID和数据库会话
                        # 由于在执行时重新创建数据库会话，需要再次连接数据库
                        from config.settings import Settings
                        from sqlalchemy import create_engine
                        from sqlalchemy.orm import sessionmaker
                        # 导入模型，确保它们被注册到Base中
                        from models.dify_result import DifyCallResult
                        
                        engine = create_engine(Settings.DATABASE_URL)
                        Session = sessionmaker(bind=engine)
                        db_session = Session()
                        
                        try:
                            # 创建结果记录
                            result_record = DifyCallResult(
                                task_id=task_data.get('task_id', 0),  # 需要从外部传入或通过其他方式获取
                                upload_api_response={
                                    'status_code': response.status_code,
                                    'content': response.text,  # 存储响应内容
                                    'headers': dict(response.headers),
                                    'url': response.url
                                },
                                status='completed' if response.status_code == 200 or 201 else 'failed',
                                execution_time=datetime.utcnow()
                            )
                            
                            db_session.add(result_record)
                            db_session.commit()
                            print(f"API响应已保存到表 {result_table}，记录ID: {result_record.id}")
                        except Exception as db_error:
                            print(f"保存到数据库失败: {str(db_error)}")
                            db_session.rollback()
                        finally:
                            db_session.close()
                        
                except requests.exceptions.RequestException as e:
                    print(f"API调用失败 (文件 {os.path.basename(file_path)}, 行 {row_idx+1}, 交易流水号: {transaction_id}): {str(e)}")
                    
        except Exception as e:
            print(f"处理CSV文件 {file_path} 时出错: {str(e)}")

def _handle_default_task(task_data):
    """默认任务处理"""
    print(f"执行默认任务，数据: {task_data}")

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