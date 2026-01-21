import os
import requests
import logging
from typing import List, Dict, Any
from config.settings import Settings
import tempfile
import gzip
from urllib.parse import urlparse
import shutil

logger = logging.getLogger(__name__)

class DownloadUnlService:
    """UNL文件下载服务类"""

    def __init__(self):
        # 从环境变量获取配置
        self.download_url = os.getenv('UNL_DOWNLOAD_URL', '')
        self.file_name_list = os.getenv('UNL_FILE_NAME_LIST', '').split(',') if os.getenv('UNL_FILE_NAME_LIST') else []
        self.file_svr_id = os.getenv('UNL_FILE_SVR_ID', '')
        self.rmt_pub_path = os.getenv('UNL_RMT_PUB_PATH', '')
        
    def validate_config(self) -> bool:
        """验证配置是否完整"""
        if not self.download_url:
            logger.error("未配置UNL_DOWNLOAD_URL环境变量")
            return False
        if not self.file_name_list:
            logger.error("未配置UNL_FILE_NAME_LIST环境变量")
            return False
        if not self.file_svr_id:
            logger.error("未配置UNL_FILE_SVR_ID环境变量")
            return False
        if not self.rmt_pub_path:
            logger.error("未配置UNL_RMT_PUB_PATH环境变量")
            return False
        return True

    def download_unl_files(self) -> List[str]:
        """下载UNL文件并返回本地文件路径列表"""
        if not self.validate_config():
            logger.error("配置验证失败，无法下载UNL文件")
            return []

        # 准备POST请求体
        payload = {
            "fileNameList": self.file_name_list,
            "fileSvrId": self.file_svr_id,
            "rmtPubPath": self.rmt_pub_path
        }

        logger.info(f"开始下载UNL文件，请求URL: {self.download_url}")
        logger.info(f"请求参数: fileNameList={self.file_name_list}, fileSvrId={self.file_svr_id}, rmtPubPath={self.rmt_pub_path}")

        try:
            # 发送POST请求
            response = requests.post(
                url=self.download_url,
                json=payload,
                headers={
                    'Content-Type': 'application/json'
                },
                timeout=300  # 设置5分钟超时
            )

            if response.status_code != 200:
                logger.error(f"下载请求失败，状态码: {response.status_code}, 响应: {response.text}")
                return []

            # 检查响应内容类型
            content_type = response.headers.get('Content-Type', '')
            logger.info(f"响应内容类型: {content_type}")

            # 保存响应内容到临时文件
            downloaded_files = []
            
            # 如果响应是二进制内容（如压缩文件），直接保存
            if 'application/gzip' in content_type or 'application/x-gzip' in content_type or response.content[:2] == b'\x1f\x8b':
                # 保存为临时文件
                temp_dir = Settings.CSV_PROCESSING_TEMP_DIR
                os.makedirs(temp_dir, exist_ok=True)
                
                # 生成唯一的临时文件名
                filename = f"downloaded_{self.file_svr_id}_{len(self.file_name_list)}files_{os.getpid()}_{hash(str(self.file_name_list))}.unl.gz"
                filepath = os.path.join(temp_dir, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"UNL文件已保存到: {filepath}")
                downloaded_files.append(filepath)
            else:
                # 如果响应是JSON格式，可能是包含了文件下载链接或其他信息
                try:
                    json_response = response.json()
                    logger.info(f"接收到JSON响应: {json_response}")
                    
                    # 尝试从JSON响应中提取文件URL并下载
                    if 'fileUrl' in json_response or 'downloadUrl' in json_response:
                        file_urls = []
                        if 'fileUrl' in json_response:
                            file_urls = [json_response['fileUrl']] if isinstance(json_response['fileUrl'], str) else json_response['fileUrl']
                        elif 'downloadUrl' in json_response:
                            file_urls = [json_response['downloadUrl']] if isinstance(json_response['downloadUrl'], str) else json_response['downloadUrl']
                        
                        for idx, file_url in enumerate(file_urls):
                            downloaded_file = self._download_from_url(file_url, f"downloaded_file_{idx}.unl.gz")
                            if downloaded_file:
                                downloaded_files.append(downloaded_file)
                except Exception as e:
                    logger.error(f"解析JSON响应失败: {str(e)}")
                    return []

            return downloaded_files

        except requests.exceptions.Timeout:
            logger.error(f"下载UNL文件超时")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"下载UNL文件请求异常: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"下载UNL文件过程中发生未知错误: {str(e)}")
            return []

    def _download_from_url(self, file_url: str, filename: str) -> str:
        """从指定URL下载文件"""
        try:
            response = requests.get(file_url, timeout=300)
            if response.status_code == 200:
                temp_dir = Settings.CSV_PROCESSING_TEMP_DIR
                os.makedirs(temp_dir, exist_ok=True)
                
                filepath = os.path.join(temp_dir, filename)
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"文件已从 {file_url} 下载到 {filepath}")
                return filepath
            else:
                logger.error(f"从 {file_url} 下载文件失败，状态码: {response.status_code}")
                return ""
        except Exception as e:
            logger.error(f"从 {file_url} 下载文件时发生错误: {str(e)}")
            return ""

    def cleanup_temp_files(self, file_paths: List[str]):
        """清理临时下载的文件"""
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"已清理临时文件: {file_path}")
            except Exception as e:
                logger.error(f"清理临时文件 {file_path} 时出错: {str(e)}")