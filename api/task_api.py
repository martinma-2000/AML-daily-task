from flask import Flask, jsonify, request
from scheduler.task_scheduler import execute_task_function
from services.task_service import TaskService
from config.settings import Settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
import os
from models.dify_result import DifyCallResult  # 导入DifyCallResult模型
import threading
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)

# 导入CSV处理服务
try:
    from services.csv_processing_service import CSVProcessingService, process_csv_for_dify
except ImportError:
    logger.warning("CSV处理服务未找到，CSV预处理API不可用")
    CSVProcessingService = None
    process_csv_for_dify = None

# 存储任务执行状态的简单字典（在生产环境中，建议使用Redis或数据库）
task_execution_status = {}

def execute_task_in_background(TaskServiceClass, Session, task_id, task_execution_id):
    """在后台线程中执行任务"""
    try:
        # 更新任务状态为正在执行
        task_execution_status[task_execution_id] = {
            'status': 'running',
            'start_time': datetime.utcnow(),
            'progress': 0,
            'message': '任务开始执行'
        }
        
        # 执行实际任务
        execute_task_function(TaskServiceClass, Session, task_id)
        
        # 更新任务状态为完成
        task_execution_status[task_execution_id]['status'] = 'completed'
        task_execution_status[task_execution_id]['progress'] = 100
        task_execution_status[task_execution_id]['message'] = '任务执行完成'
        task_execution_status[task_execution_id]['end_time'] = datetime.utcnow()
        
    except Exception as e:
        # 更新任务状态为失败
        task_execution_status[task_execution_id]['status'] = 'failed'
        task_execution_status[task_execution_id]['message'] = f'任务执行失败: {str(e)}'
        task_execution_status[task_execution_id]['end_time'] = datetime.utcnow()
        logger.error(f"后台任务执行失败: {str(e)}")

def create_app(task_scheduler, task_service):
    app = Flask(__name__)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """健康检查接口"""
        return jsonify({'status': 'healthy', 'message': 'Task API is running'})
    
    @app.route('/dify_result/<case_id>', methods=['GET'])
    def get_dify_result(case_id):
        """根据case_id获取解析结果"""
        try:
            # 创建独立的数据库会话
            engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            
            try:
                # 查询匹配的DifyCallResult记录 - 同时检查正常的case_id和带BOM的case_id
                result = db_session.query(DifyCallResult).filter(
                    (DifyCallResult.case_id == case_id) | 
                    (DifyCallResult.case_id == '\ufeff' + case_id) |
                    (DifyCallResult.case_id == case_id.replace('\ufeff', ''))
                ).order_by(DifyCallResult.execution_time.desc()).first()
                
                if not result:
                    return jsonify({'error': f'未找到case_id为 {case_id} 的记录'}), 404
                
                return jsonify({
                    'case_id': result.case_id,  # 返回数据库中实际存储的case_id
                    'parsed_result': result.parsed_result,
                    'execution_time': result.execution_time.isoformat() if result.execution_time else None,
                    'status': result.status
                })
            finally:
                db_session.close()
                engine.dispose()
        
        except Exception as e:
            logger.error(f"查询Dify结果失败: {str(e)}")
            return jsonify({'error': f'查询Dify结果失败: {str(e)}'}), 500
    
    @app.route('/tasks/trigger/<int:task_id>', methods=['POST'])
    def trigger_task(task_id):
        """手动触发指定ID的任务"""
        try:
            # 创建独立的数据库会话来获取任务信息
            engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            
            try:
                # 验证任务是否存在
                task = task_service.get_task_by_id(task_id, db_session)
                if not task:
                    return jsonify({'error': f'任务ID {task_id} 不存在'}), 404
                
                if not task.enabled:
                    return jsonify({'error': f'任务ID {task_id} 已被禁用'}), 400
                
                # 生成任务执行ID用于跟踪
                task_execution_id = str(uuid.uuid4())
                
                # 在后台线程中执行任务，立即返回响应
                thread = threading.Thread(
                    target=execute_task_in_background,
                    args=(TaskService, Session, task_id, task_execution_id)
                )
                thread.daemon = True
                thread.start()
                
                return jsonify({
                    'message': f'任务 {task.task_name} (ID: {task_id}) 已提交后台执行',
                    'task_id': task_id,
                    'task_name': task.task_name,
                    'execution_id': task_execution_id,
                    'status': 'submitted'
                })
            finally:
                db_session.close()
                engine.dispose()
        
        except Exception as e:
            logger.error(f"手动触发任务失败: {str(e)}")
            return jsonify({'error': f'执行任务失败: {str(e)}'}), 500
    
    @app.route('/tasks/trigger_by_name/<task_name>', methods=['POST'])
    def trigger_task_by_name(task_name):
        """通过任务名称手动触发任务"""
        try:
            # 创建独立的数据库会话来获取任务信息
            engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            
            try:
                # 验证任务是否存在
                task = task_service.get_task_by_name(task_name, db_session)
                if not task:
                    return jsonify({'error': f'任务名称 {task_name} 不存在'}), 404
                
                if not task.enabled:
                    return jsonify({'error': f'任务 {task_name} 已被禁用'}), 400
                
                # 生成任务执行ID用于跟踪
                task_execution_id = str(uuid.uuid4())
                
                # 在后台线程中执行任务，立即返回响应
                thread = threading.Thread(
                    target=execute_task_in_background,
                    args=(TaskService, Session, task.id, task_execution_id)
                )
                thread.daemon = True
                thread.start()
                
                return jsonify({
                    'message': f'任务 {task_name} (ID: {task.id}) 已提交后台执行',
                    'task_id': task.id,
                    'task_name': task_name,
                    'execution_id': task_execution_id,
                    'status': 'submitted'
                })
            finally:
                db_session.close()
                engine.dispose()
        
        except Exception as e:
            logger.error(f"手动触发任务失败: {str(e)}")
            return jsonify({'error': f'执行任务失败: {str(e)}'}), 500
    
    @app.route('/tasks/status/<execution_id>', methods=['GET'])
    def get_task_status(execution_id):
        """获取任务执行状态"""
        if execution_id in task_execution_status:
            status_info = task_execution_status[execution_id].copy()
            return jsonify(status_info)
        else:
            return jsonify({'error': f'执行ID {execution_id} 不存在'}), 404
    
    @app.route('/tasks/list', methods=['GET'])
    def list_tasks():
        """列出所有任务"""
        try:
            # 创建独立的数据库会话来获取任务列表
            engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            
            try:
                tasks = task_service.get_all_enabled_tasks(db_session)
                task_list = []
                for task in tasks:
                    task_list.append({
                        'id': task.id,
                        'name': task.task_name,
                        'schedule': task.task_schedule,
                        'enabled': task.enabled,
                        'created_at': task.created_at.isoformat() if task.created_at else None,
                        'updated_at': task.updated_at.isoformat() if task.updated_at else None
                    })
                
                return jsonify({
                    'tasks': task_list,
                    'count': len(task_list)
                })
            finally:
                db_session.close()
                engine.dispose()
        
        except Exception as e:
            logger.error(f"获取任务列表失败: {str(e)}")
            return jsonify({'error': f'获取任务列表失败: {str(e)}'}), 500
    
    @app.route('/csv/preprocess', methods=['POST'])
    def preprocess_csv():
        """CSV预处理接口，用于在获取原始CSV文件和上传CSV文件之间进行数据处理"""
        if not CSVProcessingService or not process_csv_for_dify:
            return jsonify({'error': 'CSV处理服务不可用'}), 500
        
        try:
            # 获取请求参数
            data = request.get_json()
            
            if not data:
                return jsonify({'error': '请提供JSON格式的请求数据'}), 400
            
            input_file_path = data.get('input_file_path')
            csv_content = data.get('csv_content')
            output_file_path = data.get('output_file_path')
            
            # 必须提供输入方式（文件路径或内容）
            if not input_file_path and not csv_content:
                return jsonify({'error': '必须提供input_file_path或csv_content参数'}), 400
            
            # 如果没有提供输出路径，创建临时文件
            if not output_file_path:
                import tempfile
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file_path = os.path.join(tempfile.gettempdir(), f"preprocessed_{timestamp}.csv")
            
            # 调用CSV处理服务
            result = process_csv_for_dify(
                csv_file_path=input_file_path,
                csv_content=csv_content,
                output_path=output_file_path
            )
            
            return jsonify(result)
        
        except Exception as e:
            logger.error(f"CSV预处理失败: {str(e)}")
            return jsonify({'error': f'CSV预处理失败: {str(e)}'}), 500
    
    return app