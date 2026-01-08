from flask import Flask, jsonify, request
from scheduler.task_scheduler import execute_task_function
from services.task_service import TaskService
from config.settings import Settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

def create_app(task_scheduler, task_service):
    app = Flask(__name__)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """健康检查接口"""
        return jsonify({'status': 'healthy', 'message': 'Task API is running'})
    
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
                
                # 执行任务 - execute_task_function内部会创建自己的会话
                execute_task_function(TaskService, Session, task_id)
                
                return jsonify({
                    'message': f'任务 {task.task_name} (ID: {task_id}) 已手动触发执行',
                    'task_id': task_id,
                    'task_name': task.task_name
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
                
                # 执行任务 - execute_task_function内部会创建自己的会话
                execute_task_function(TaskService, Session, task.id)
                
                return jsonify({
                    'message': f'任务 {task_name} (ID: {task.id}) 已手动触发执行',
                    'task_id': task.id,
                    'task_name': task_name
                })
            finally:
                db_session.close()
                engine.dispose()
        
        except Exception as e:
            logger.error(f"手动触发任务失败: {str(e)}")
            return jsonify({'error': f'执行任务失败: {str(e)}'}), 500
    
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
    
    return app