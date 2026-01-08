import os
import requests
import csv
import json
import io
from datetime import datetime
import logging
from config.settings import Settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.dify_result import DifyCallResult

logger = logging.getLogger(__name__)

class BatchApiService:
    """批量API调用服务类"""

    def handle_batch_api_call(self, task_data):
        """处理批量API调用任务"""
        logger.info(f"执行批量API调用任务，配置: {task_data}")
        
        api_endpoint = task_data.get('api_endpoint')
        csv_file_path = task_data.get('csv_file_path')
        result_table = task_data.get('result_table')
        api_key = task_data.get('API-KEY')
        workflow_run_endpoint = task_data.get('workflow_run_endpoint', f"{api_endpoint}/workflows/run")  # 工作流运行端点
        
        if not api_endpoint or not csv_file_path:
            logger.error("错误：缺少api_endpoint或csv_file_path参数")
            return
        
        # 检查CSV目录是否存在
        if not os.path.exists(csv_file_path) or not os.path.isdir(csv_file_path):
            logger.error(f"错误：CSV目录不存在: {csv_file_path}")
            return
        
        # 获取目录中的所有CSV文件
        csv_files = [f for f in os.listdir(csv_file_path) if f.lower().endswith('.csv')]
        
        if not csv_files:
            logger.warning(f"警告：在目录 {csv_file_path} 中未找到CSV文件")
            return
        
        # 遍历CSV目录中的所有CSV文件
        for csv_file in csv_files:
            file_path = os.path.join(csv_file_path, csv_file)
            logger.info(f"处理CSV文件: {file_path}")
            
            self._process_csv_file(file_path, api_endpoint, api_key, workflow_run_endpoint, result_table, task_data)

    def _process_csv_file(self, file_path, api_endpoint, api_key, workflow_run_endpoint, result_table, task_data):
        """处理单个CSV文件"""
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
                logger.error(f"错误：无法使用常见编码读取CSV文件 {file_path}")
                return
            
            # 将内容转换为字符串流以供csv模块使用
            csv_string_io = io.StringIO(csv_content)
            
            # 读取CSV内容，直接处理数据行
            csv_reader = csv.reader(csv_string_io)
            
            for row_idx, row in enumerate(csv_reader):
                # 将行数据转换为字典格式，使用索引作为键
                row_dict = {f"column_{i}": value for i, value in enumerate(row)}
                self._process_csv_row(row_dict, row_idx, file_path, api_endpoint, api_key, workflow_run_endpoint, result_table, task_data)
                
        except Exception as e:
            logger.error(f"处理CSV文件 {file_path} 时出错: {str(e)}")

    def _process_csv_row(self, row, row_idx, file_path, api_endpoint, api_key, workflow_run_endpoint, result_table, task_data):
        """处理CSV文件中的单行数据"""
        # 构建API请求
        headers = {
            'Authorization': f'Bearer {api_key}' if api_key else ''
        }
        
        # 获取第1列的数据作为案例ID（如果存在）
        case_id = row.get('column_0', 'N/A')  # 第1列（索引为0）
        
        # 准备上传的CSV文件（单行数据）- 不包含列名
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=row.keys())
        # 只写入当前行数据，不写入表头
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
            # 第一步：上传文件
            response = requests.post(
                f"{api_endpoint}/files/upload",
                headers=headers,
                files=files,
                data=data
            )
            logger.info(f"API调用结果 (文件 {os.path.basename(file_path)}, 行 {row_idx+1}, 交易流水号: {case_id}): {response.status_code}")
            
            # 如果上传成功，则调用工作流运行接口
            if response.status_code in [200, 201]:
                run_response_data = self._call_workflow_api(response, api_key, workflow_run_endpoint, file_path, row_idx, case_id)
            else:
                run_response_data = None
            
            if result_table:
                # 调用封装的函数保存结果到数据库，包括工作流响应
                self._save_api_result_to_db(task_data, response, result_table, run_response_data, case_id)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API调用失败 (文件 {os.path.basename(file_path)}, 行 {row_idx+1}, 交易流水号: {case_id}): {str(e)}")

    def _call_workflow_api(self, upload_response, api_key, workflow_run_endpoint, file_path, row_idx, case_id):
        """调用工作流API"""
        try:
            # 解析上传响应以获取文件ID
            upload_response_data = upload_response.json()
            file_id = upload_response_data.get('id')
            
            if not file_id:
                # 如果直接获取不到，尝试解析content字段中的JSON
                try:
                    content_str = upload_response.text
                    content_data = json.loads(content_str)
                    file_id = content_data.get('id')
                except json.JSONDecodeError:
                    logger.error(f"无法从响应中解析文件ID")
                    file_id = None
            
            if file_id:
                logger.info(f"获取到上传文件ID: {file_id}")
                
                # 构建工作流运行请求
                workflow_headers = {
                    'Authorization': f'Bearer {api_key}',
                    'Content-Type': 'application/json'
                }
                
                workflow_data = {
                    "inputs": {
                        "AML_message": {
                            "transfer_method": "local_file",
                            "upload_file_id": file_id,
                            "type": "document"
                        }
                    },
                    "response_mode": "blocking",
                    # TODO 用户信息是否需要配置
                    "user": "ma"
                }
                
                # 调用工作流运行接口
                workflow_response = requests.post(
                    workflow_run_endpoint,
                    headers=workflow_headers,
                    json=workflow_data
                )
                
                logger.info(f"工作流运行结果 (文件 {os.path.basename(file_path)}, 行 {row_idx+1}, 交易流水号: {case_id}): {workflow_response.status_code}")
                
                # 准备保存到数据库的结果
                run_response_data = {
                    'status_code': workflow_response.status_code,
                    'content': workflow_response.text,
                    'headers': dict(workflow_response.headers),
                    'url': workflow_response.url
                }
                
                return run_response_data
            else:
                logger.warning(f"未能获取文件ID，跳过工作流运行 (文件 {os.path.basename(file_path)}, 行 {row_idx+1})")
                return None
                
        except Exception as workflow_error:
            logger.error(f"工作流运行失败 (文件 {os.path.basename(file_path)}, 行 {row_idx+1}, 交易流水号: {case_id}): {str(workflow_error)}")
            return {
                'error': str(workflow_error),
                'status': 'failed'
            }

    def _save_api_result_to_db(self, task_data, response, result_table, run_response=None, case_id=None):
        """将 API 调用结果保存到数据库"""
        # 每次都创建新的会话以避免事务状态问题
        engine = create_engine(Settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        parsed_result = None

        if run_response:
            parsed_result = self._parse_workflow_result(run_response)

            # 避免误传未解码的 \uXXXX 形式
            if isinstance(parsed_result, (dict, list)):
                parsed_result = json.dumps(parsed_result, ensure_ascii=False)
            elif isinstance(parsed_result, str) and "\\u" in parsed_result:
                try:
                    parsed_result = json.loads(f'"{parsed_result}"')
                except json.JSONDecodeError:
                    pass

        try:
            # 创建结果记录
            result_record = DifyCallResult(
                task_id=task_data.get('task_id', 0),
                upload_api_response={
                    'status_code': response.status_code,
                    'content': response.text,
                    'headers': dict(response.headers),
                    'url': response.url
                },
                run_response=run_response,
                parsed_result=parsed_result,
                case_id=case_id,
                status='completed' if response.status_code in (200, 201) else 'failed',
                execution_time=datetime.utcnow()
            )

            db_session.add(result_record)
            db_session.commit()
            logger.info(f"API响应已保存到表 {result_table}，记录ID: {result_record.id}")
            return result_record

        except Exception as db_error:
            logger.error(f"保存到数据库失败: {str(db_error)}")
            db_session.rollback()
            return None

        finally:
            db_session.close()  # 会话关闭，连接归还到连接池
            engine.dispose()    # 释放引擎资源

    def _parse_workflow_result(self, workflow_response_data):
        """解析工作流结果，提取 outputs.RES 的值"""
        if not workflow_response_data:
            return None

        try:
            # 解析 content 字段中的 JSON 字符串
            content_str = workflow_response_data.get('content', '{}')
            content_data = json.loads(content_str)

            # 提取 outputs.RES
            outputs = content_data.get('data', {}).get('outputs', {})
            res_value = outputs.get('RES')

            # 递归处理 unicode 转义
            res_value = self._handle_unicode_in_dict(res_value)
            return res_value

        except Exception as e:
            # 解析失败时，返回 None 或原始数据
            print(f"解析 workflow 结果异常: {e}")
            return None

    def _handle_unicode_in_dict(self, data):
        """递归处理字典、列表或字符串中的 Unicode 转义序列"""
        if isinstance(data, str):
            # 若字符串包含 \uXXX 转义形式，解码一次
            if "\\u" in data:
                try:
                    # 用 JSON 处理 Unicode 转义更安全
                    return json.loads(f'"{data}"')
                except json.JSONDecodeError:
                    # 备用方案：直接用 unicode_escape 解码
                    return data.encode('utf-8').decode('unicode_escape')
            else:
                return data  # 已是正常中文，直接返回

        elif isinstance(data, dict):
            return {k: self._handle_unicode_in_dict(v) for k, v in data.items()}

        elif isinstance(data, list):
            return [self._handle_unicode_in_dict(v) for v in data]

        else:
            return data