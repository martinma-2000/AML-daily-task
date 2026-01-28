import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional
import tempfile
from pathlib import Path
import numpy as np
import hashlib

from config.settings import Settings

logger = logging.getLogger(__name__)

class CSVProcessingService:
    """CSV数据预处理服务类，用于在获取原始CSV文件和上传CSV文件之间进行数据处理"""

    def __init__(self, chunk_size: int = None):
        """初始化CSV处理服务"""
        # 使用配置文件中的默认值，如果未提供chunk_size
        if chunk_size is None:
            chunk_size = Settings.CSV_PROCESSING_CHUNK_SIZE
        self.chunk_size = chunk_size  # 设置分块大小
        self.seen_trans_keys = set()  # 用于跟踪已见过的trans_key，实现跨块去重
        self.seen_case_trans_keys = set()  # 用于跟踪已见过的case_id+trans_key组合，实现跨块去重
        
        self.column_mapping = {
            '案例编号': 'case_id',
            '数据日期': 'data_date',
            '主客户编号': 'main_cust_id',
            '主客户名称': 'main_cust_name',
            '证件类型': 'id_type',
            '证件号': 'id_number',
            '主客户职业行业': 'main_cust_industry',
            '主客户性别': 'main_cust_gender',
            '主客户开户日期': 'main_cust_open_date',
            '主客户地址': 'main_cust_addr',
            '主客户联系电话': 'main_cust_phone_number',
            '可疑模型编号': 'model_id',
            '可疑模型名称': 'model_name',
            '可疑特征规则编号': 'suspect_rule_id',
            '可疑特征规则特征名称': 'suspect_rule_name',
            '模型平台最高分数': 'highest_score',
            '机器学习匹配规则前10特征序号': 'serial_num',
            '机器学习匹配规则前10特征说明': 'features',
            '机器学习匹配规则前10特征风险值': 'feature_value',
            '可疑案例下所有客户号': 'all_case_cust_ids',
            '可疑案例下所有客户名称': 'all_case_cust_names',
            '可疑案例下所有账号': 'all_case_acct_nos',
            '交易主键': 'trans_key',
            '交易日期': 'trans_date',
            '交易日期和时间': 'trans_datetime',
            '交易机构': 'trans_org',
            '客户类型': 'cust_type',
            '卡号折号': 'card_no',
            '卡片类型': 'card_type',
            'am1交易渠道': 'aml_channel',
            '源系统交易渠道': 'src_channel',
            'am1交易代码': 'aml_trans_code',
            '源系统交易代码': 'src_trans_code',
            '现转标志': 'cash_transfer_flag',
            '借贷标志': 'debit_credit_flag',
            '收付标志': 'income_pay_flag',
            '币种': 'currency',
            '原币种交易金额': 'trans_amt',
            '折人民币交易金额': 'cny_amt',
            '折美元交易金额': 'usd_amt',
            '交易余额': 'trans_balance',
            '交易发生国家': 'trans_country',
            '交易发生地区': 'trans_region',
            '资金用途和来源': 'fund_usage',
            '对方名称': 'counterparty_name',
            '对方账号': 'counterparty_acct_no',
            '对手PBC账户类型': 'pbc_acct_type',
            '对方是否我行客户': 'is_our_cust',
            '对方客户编号': 'counterparty_cust_id',
            '对方客户类型': 'counterparty_cust_type',
            '对方卡号折号': 'counterparty_card_no',
            '对方金融机构编号': 'fin_inst_id',
            '对方金融机构名称': 'fin_inst_name',
            '对方金融机构网点国家': 'fin_inst_country',
            '对方金融机构网点地区': 'fin_inst_region',
            # '交易去向国家': 'fund_dest_country',
            # '交易去向地区': 'fund_dest_region',
            '交易IPV6地址': 'ipv6_addr',
            'IP地址': 'ip_addr',
            '交易MAC地址': 'mac_addr',
            '摘要码': 'summary_code',
            '交易备注': 'trans_remark'
        }
        self.chunk_size = chunk_size  # 设置分块大小

    def _safe_convert_to_float(self, value, default=0.0):
        """安全转换值为浮点数"""
        if pd.isna(value) or value == '' or value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            try:
                # 尝试转换一些常见的非标准数值表示
                str_val = str(value).strip().lower()
                if str_val in ['null', 'n/a', 'nan', 'inf', '-inf', '<null>', '#n/a']:
                    return default
                return float(str_val)
            except (ValueError, TypeError):
                return default

    def _safe_convert_to_str(self, value, default=''):
        """安全转换值为字符串"""
        if pd.isna(value) or value is None:
            return default
        return str(value)

    def _safe_format_date(self, date_value, format_str='%Y-%m-%d', default=''):
        """安全格式化日期，避免NaT错误"""
        if pd.isna(date_value) or date_value is None:
            return default
        try:
            if hasattr(date_value, 'strftime'):
                return date_value.strftime(format_str)
            else:
                # 如果已经是字符串形式的日期
                dt = pd.to_datetime(date_value, errors='coerce')
                if pd.isna(dt):
                    return default
                return dt.strftime(format_str)
        except (AttributeError, ValueError):
            return default

    def _parse_flexible_datetime(self, datetime_series):
        """灵活解析多种时间格式"""
        def convert_single_datetime(val):
            if pd.isna(val) or val == '' or val is None:
                return pd.NaT
            try:
                # 如果已经是日期时间对象，直接返回
                if isinstance(val, (pd.Timestamp, datetime)):
                    return val
                # 转换为字符串并清理
                str_val = str(val).strip()
                if str_val.lower() in ['null', 'n/a', 'nan', '<null>', '#n/a', '']:
                    return pd.NaT
                # 尝试不同的时间格式
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y/%m/%d %H:%M:%S',
                    '%m/%d/%Y %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S',
                    '%Y-%m-%d',
                    '%Y/%m/%d',
                    '%m/%d/%Y',
                    '%d/%m/%Y',
                    '%Y-%m-%d %H:%M',
                    '%m/%d/%Y %H:%M',
                    '%Y-%m-%d %h:%i:%s'  # MySQL格式
                ]
                for fmt in formats:
                    try:
                        return pd.to_datetime(str_val, format=fmt, errors='raise')
                    except ValueError:
                        continue
                # 如果所有特定格式都失败，使用pandas的自动推断
                return pd.to_datetime(str_val, errors='coerce')
            except Exception:
                return pd.NaT

        return datetime_series.apply(convert_single_datetime)

    def _aggregate_features(self, group):
        """
        聚合并去重TOP10特征信息，以JSON格式存储完整的特征记录
        """
        # 创建特征记录列表，保持每个记录的完整性
        feature_records = []
        for idx, row in group.iterrows():
            serial_num = row.get('serial_num')
            features_val = row.get('features')
            feature_value = row.get('feature_value')
            highest_score = row.get('highest_score')
            
            if pd.notna(serial_num) or pd.notna(features_val) or pd.notna(feature_value) or pd.notna(highest_score):
                feature_record = {
                    'serial_num': serial_num,
                    'features': features_val,
                    'feature_value': feature_value,
                    'highest_score': highest_score
                }
                # 转换为字符串以便比较和去重
                record_str = str(feature_record)
                # 检查是否已存在相同的记录
                if record_str not in [str(r) for r in feature_records]:
                    feature_records.append(feature_record)
        
        return feature_records

    def _process_chunk(self, chunk_df):
        """处理单个数据块"""
        # 数据清洗：处理特殊值和类型转换
        # 清理数值字段
        chunk_df['trans_amt'] = chunk_df['trans_amt'].apply(lambda x: self._safe_convert_to_float(x, 0.0))
        if 'cny_amt' in chunk_df.columns:
            chunk_df['cny_amt'] = chunk_df['cny_amt'].apply(lambda x: self._safe_convert_to_float(x, 0.0))
        if 'usd_amt' in chunk_df.columns:
            chunk_df['usd_amt'] = chunk_df['usd_amt'].apply(lambda x: self._safe_convert_to_float(x, 0.0))

        # 灵活解析时间字段
        chunk_df['trans_datetime'] = self._parse_flexible_datetime(chunk_df['trans_datetime'])
        chunk_df['trans_date'] = chunk_df['trans_datetime'].apply(lambda x: x.date() if pd.notna(x) else pd.NaT)

        # 提取小时用于判断夜间交易（仅对有效时间进行提取）
        chunk_df['hour'] = chunk_df['trans_datetime'].apply(lambda x: x.hour if pd.notna(x) else np.nan)

        # 实现跨块去重
        if 'trans_key' in chunk_df.columns and 'case_id' in chunk_df.columns:
            # 首先移除trans_key为空值的行
            chunk_df = chunk_df.dropna(subset=['trans_key', 'case_id'])  # 同时检查case_id和trans_key
            
            if len(chunk_df) > 0:
                # 创建case_id+trans_key的组合键
                chunk_df['_case_trans_key'] = chunk_df['case_id'].astype(str) + '_' + chunk_df['trans_key'].astype(str)
                
                # 使用向量化操作过滤掉之前已见过的case_id+trans_key组合
                mask = ~chunk_df['_case_trans_key'].isin(self.seen_case_trans_keys)
                chunk_df = chunk_df[mask]
                
                # 批量更新已见的case_id+trans_key组合集合
                if len(chunk_df) > 0:
                    new_case_trans_keys = set(chunk_df['_case_trans_key'])
                    self.seen_case_trans_keys.update(new_case_trans_keys)
                
                # 删除临时列
                chunk_df = chunk_df.drop('_case_trans_key', axis=1)

        return chunk_df

    def _aggregate_case_data(self, grouped_data):
        """聚合案例数据"""
        results = []
        processed_cases = set()
        
        for case_id, group in grouped_data:
            try:
                # 确保数值字段都是数字类型
                g = group.copy()
                # 由于在_process_chunk中已经进行了类型转换，这里需要更加谨慎地处理
                # 检查trans_amt列的数据类型并相应处理
                if not pd.api.types.is_numeric_dtype(g['trans_amt']):
                    g['trans_amt'] = pd.to_numeric(g['trans_amt'], errors='coerce').fillna(0.0)
                
                if 'income_pay_flag' in g.columns and not pd.api.types.is_string_dtype(g['income_pay_flag']):
                    g['income_pay_flag'] = g['income_pay_flag'].astype(str).fillna('')

                # 夜间交易（23点-6点）- 只对有效小时数计算
                valid_hours = g['hour'].dropna()
                night_count = ((valid_hours >= 23) | (valid_hours <= 6)).sum()

                # 风险关键词
                keywords = set()

                # 确保均值计算不会出错
                valid_trans_amt = g['trans_amt'][pd.notna(g['trans_amt']) & (g['trans_amt'] != '')]
                avg_trans_amt = valid_trans_amt.mean() if len(valid_trans_amt) > 0 else 0.0

                if avg_trans_amt <= 10:
                    keywords.add('小额')
                if len(g) >= 50:
                    keywords.add('高频')
                if len(valid_hours) > 0 and (night_count / len(valid_hours)) > 0.8:
                    keywords.add('夜间')
                
                # 添加整数交易金额统计分析
                if len(valid_trans_amt) > 0:
                    # 统计整数交易金额
                    integer_amounts = valid_trans_amt[valid_trans_amt.apply(lambda x: x.is_integer() if pd.notna(x) else False)]
                    integer_count = len(integer_amounts)
                    integer_ratio = integer_count / len(valid_trans_amt) if len(valid_trans_amt) > 0 else 0
                    
                    # 特定整数金额检测（如整百、整千等）
                    round_amounts = valid_trans_amt[
                        (valid_trans_amt % 100 == 0) | 
                        (valid_trans_amt % 1000 == 0) | 
                        (valid_trans_amt % 10000 == 0)
                    ]
                    round_amount_count = len(round_amounts)
                    round_amount_ratio = round_amount_count / len(valid_trans_amt) if len(valid_trans_amt) > 0 else 0
                    
                    # 如果整数金额比例超过一定阈值，则标记为可疑
                    if integer_ratio > 0.7:  # 70%以上的交易金额为整数
                        keywords.add('整数金额高')
                    if round_amount_ratio > 0.5:  # 50%以上的交易金额为整百、整千等
                        keywords.add('整额交易')
                
                # 检查IP和MAC地址异常（增加健壮性检查）
                try:
                    if 'ip_addr' in g.columns:
                        unique_ips = g['ip_addr'].dropna().nunique()
                        if unique_ips > 1:
                            keywords.add('多IP')
                except Exception:
                    logger.warning("检查IP地址异常时出错")

                try:
                    if 'mac_addr' in g.columns:
                        unique_macs = g['mac_addr'].dropna().nunique()
                        if unique_macs > 1:
                            keywords.add('多设备')
                except Exception:
                    logger.warning("检查MAC地址异常时出错")

                # 计算对手方名称中的空值数量
                counterparty_count = len(g)
                if 'counterparty_name' in g.columns:
                    nan_counterparty_count = g['counterparty_name'].isna().sum()
                    if nan_counterparty_count > counterparty_count * 0.5:
                        keywords.add('匿名')
                
                # 检查资金用途
                if 'fund_usage' in g.columns:
                    fund_usage_series = g['fund_usage'].fillna('').astype(str)
                    if fund_usage_series.str.contains('充值|返现|游戏|彩票', na=False, case=False).any():
                        keywords.add('可疑用途')

                # 提取交易样本（前3笔 + 后3笔），排除低价值自动交易
                sample_trx = []

                # 定义低价值交易关键词
                low_value_keywords = ['扣费', '手续费', '服务费', '系统', '自动', '代扣', '短信费', '管理费', '工本费']

                # 过滤掉低价值交易
                valid_trx = g
                if 'fund_usage' in g.columns:
                    pattern = '|'.join(low_value_keywords)
                    fund_usage_clean = g['fund_usage'].fillna('').astype(str)
                    valid_trx = g[~fund_usage_clean.str.contains(pattern, na=True, case=False)]

                # 如果过滤后数据不足，回退使用原始数据
                if len(valid_trx) == 0:
                    valid_trx = g

                # 确保有有效的 trans_datetime 用于排序
                valid_trx_with_dt = valid_trx[pd.notna(valid_trx['trans_datetime'])]
                if len(valid_trx_with_dt) > 0:
                    # 取前3笔和后3笔
                    first_trx = valid_trx_with_dt.nsmallest(min(3, len(valid_trx_with_dt)), 'trans_datetime')
                    last_trx = valid_trx_with_dt.nlargest(min(3, len(valid_trx_with_dt)), 'trans_datetime')

                    # 合并并去重（按 trans_datetime）
                    combined = pd.concat([first_trx, last_trx]).drop_duplicates(subset=['trans_datetime'])

                    for _, trx in combined.iterrows():
                        # 确保安全获取各项数据
                        trans_date_val = trx.get('trans_date', pd.NaT)
                        trans_datetime_val = trx.get('trans_datetime', pd.NaT)
                        
                        sample_trx.append({
                            'TR_DT': self._safe_format_date(trans_date_val, '%Y-%m-%d', ''),
                            'TR_TM': self._safe_format_date(trans_datetime_val, '%H:%M', ''),
                            'TR_AMT': self._safe_convert_to_float(trx.get('trans_amt', 0.0)),
                            'CURR_CD': self._safe_convert_to_str(trx.get('currency', ''), 'CNY'),
                            'OPP_NAME': self._safe_convert_to_str(trx.get('counterparty_name', ''), ''),
                            'FUND_USE': self._safe_convert_to_str(trx.get('fund_usage', ''), ''),
                            'TR_CHNL': self._safe_convert_to_str(trx.get('aml_channel', ''), ''),
                            'TR_AREA': self._safe_convert_to_str(trx.get('trans_region', ''), ''),
                            'SRC_CHNL': self._safe_convert_to_str(trx.get('src_channel', ''), ''),
                            'TR_ORG': self._safe_convert_to_str(trx.get('trans_org', ''), ''),
                            'REMARK': self._safe_convert_to_str(trx.get('trans_remark', ''), '')
                        })

                # 交易对手地区统计（转换为字符串，跳过空值）
                top_areas = []
                if 'trans_region' in g.columns:
                    region_counts = g['trans_region'].dropna().value_counts().head(5)
                    top_areas = [self._safe_convert_to_str(x) for x in region_counts.index.tolist()]
                
                main_channels = []
                if 'aml_channel' in g.columns:
                    channel_counts = g['aml_channel'].dropna().value_counts().head(5)
                    main_channels = [self._safe_convert_to_str(x) for x in channel_counts.index.tolist()]

                # 处理收入支出标志，兼容字符串 '01', '02' 和整数 1,2
                debit_count = 0
                debit_amt = 0.0
                credit_count = 0
                credit_amt = 0.0
                
                if 'income_pay_flag' in g.columns:
                    # 确保转换为字符串并去除空格
                    flag = g['income_pay_flag'].apply(lambda x: self._safe_convert_to_str(x, '').strip())
                    # 支持多种表示方式
                    debit_mask = flag.isin(['1', '01', '借', 'debit', 'D'])
                    credit_mask = flag.isin(['2', '02', '贷', 'credit', 'C'])
                    
                    debit_count = debit_mask.sum()
                    debit_amt = float(g[debit_mask]['trans_amt'].sum()) if debit_mask.any() else 0.0
                    credit_count = credit_mask.sum()
                    credit_amt = float(g[credit_mask]['trans_amt'].sum()) if credit_mask.any() else 0.0

                # 获取有效的交易金额用于计算
                valid_trans_amt = g['trans_amt'][pd.notna(g['trans_amt'])]
                total_trans_amt = float(valid_trans_amt.sum()) if len(valid_trans_amt) > 0 else 0.0
                trans_count = len(g)
                avg_trans_amt = float(valid_trans_amt.mean()) if len(valid_trans_amt) > 0 else 0.0
                max_trans_amt = float(valid_trans_amt.max()) if len(valid_trans_amt) > 0 else 0.0

                # 获取有效的交易日期用于计算
                valid_trans_dates = g['trans_date'][pd.notna(g['trans_date'])]
                first_trans_date = valid_trans_dates.min() if len(valid_trans_dates) > 0 else pd.NaT
                last_trans_date = valid_trans_dates.max() if len(valid_trans_dates) > 0 else pd.NaT

                # 基础聚合结果
                result_dict = {
                    'main_cust_name': self._safe_convert_to_str(g['main_cust_name'].iloc[0] if len(g) > 0 and 'main_cust_name' in g.columns else '', ''),
                    'main_cust_id': self._safe_convert_to_str(g['main_cust_id'].iloc[0] if len(g) > 0 and 'main_cust_id' in g.columns else '', ''),
                    'main_cust_industry': self._safe_convert_to_str(g['main_cust_industry'].iloc[0] if len(g) > 0 and 'main_cust_industry' in g.columns else '', ''),
                    'main_cust_gender': self._safe_convert_to_str(g['main_cust_gender'].iloc[0] if len(g) > 0 and 'main_cust_gender' in g.columns else '', ''),
                    'main_cust_open_date': self._safe_convert_to_str(g['main_cust_open_date'].iloc[0] if len(g) > 0 and 'main_cust_open_date' in g.columns else '', ''),
                    'main_cust_addr': self._safe_convert_to_str(g['main_cust_addr'].iloc[0] if len(g) > 0 and 'main_cust_addr' in g.columns else '', ''),
                    'main_cust_phone_number': self._safe_convert_to_str(g['main_cust_phone_number'].iloc[0] if len(g) > 0 and 'main_cust_phone_number' in g.columns else '', ''),
                    'id_type': self._safe_convert_to_str(g['id_type'].iloc[0] if len(g) > 0 and 'id_type' in g.columns else '', ''),
                    'id_number': self._safe_convert_to_str(g['id_number'].iloc[0] if len(g) > 0 and 'id_number' in g.columns else '', ''),
                    'total_trans_amt': total_trans_amt,
                    'trans_count': trans_count,
                    'avg_trans_amt': avg_trans_amt,
                    'max_trans_amt': max_trans_amt,
                    'first_trans_date': first_trans_date if pd.notna(first_trans_date) else '',
                    'last_trans_date': last_trans_date if pd.notna(last_trans_date) else '',
                    'report_start_date': self._safe_format_date((first_trans_date - timedelta(days=7)) if pd.notna(first_trans_date) else pd.NaT, '%Y年%m月%d日', ''),
                    'report_end_date': self._safe_format_date(last_trans_date, '%Y年%m月%d日', ''),
                    'night_trans_count': night_count,
                    'risk_keywords': ','.join(sorted(keywords)),
                    # 排除已知非可疑对手（如平台、系统、手续费等）
                    'counterparty_sample': '',
                    'model_name': self._safe_convert_to_str(g['model_name'].iloc[0] if len(g) > 0 and 'model_name' in g.columns else '', ''),
                    'highest_score': self._safe_convert_to_float(g['highest_score'].iloc[0] if len(g) > 0 and 'highest_score' in g.columns else 0, 0),
                    'features': self._aggregate_features(g) if len(g) > 0 else [],
                    'is_network_gambling_suspected': '否',  # 默认值，后面再更新
                    'sample_trx_list': sample_trx,
                    'top_opposing_areas': ','.join(top_areas),
                    'main_tnx_channels': ','.join(main_channels),
                    'tr_org': self._safe_convert_to_str(g['trans_org'].iloc[0] if len(g) > 0 and 'trans_org' in g.columns else '', '未知机构'),
                    'debit_count': debit_count,
                    'debit_amt': debit_amt,
                    'credit_count': credit_count,
                    'credit_amt': credit_amt,
                    # 使用健壮的IP和MAC地址获取方法
                    'ipv6_addr': self._get_representative_ip(g, 'ipv6_addr'),
                    'ip_addr': self._get_representative_ip(g, 'ip_addr'),
                    'mac_addr': self._get_representative_mac(g, 'mac_addr'),
                }

                # 根据条件判断是否涉嫌网络赌博
                is_network_gambling = False
                try:
                    if ('fund_usage' in g.columns and 
                        len(g) >= 50 and
                        avg_trans_amt <= 10 and
                        len(valid_hours) > 0 and (night_count / len(valid_hours)) > 0.8 and
                        g['fund_usage'].fillna('').astype(str).str.contains('充值|返现', na=False, case=False).any()):
                        is_network_gambling = True
                except Exception:
                    logger.warning("检查网络赌博模式时出错")

                # 检测IP和MAC相关的风险模式
                is_ip_suspicious = False
                is_mac_suspicious = False
                
                try:
                    if 'ip_addr' in g.columns:
                        unique_ips = g['ip_addr'].dropna().nunique()
                        total_trans = len(g)
                        if unique_ips > 0 and total_trans > 0:
                            ip_concentration = unique_ips / total_trans
                            # 如果IP地址过于分散，可能表示跨区域操作
                            if ip_concentration > 0.5:  # 超过一半的交易来自不同IP
                                is_ip_suspicious = True
                                keywords.add('IP分散')
                except Exception:
                    logger.warning("检测IP地址风险时出错")
                
                try:
                    if 'mac_addr' in g.columns:
                        unique_macs = g['mac_addr'].dropna().nunique()
                        total_trans = len(g)
                        if unique_macs > 0 and total_trans > 0:
                            mac_concentration = unique_macs / total_trans
                            # 如果MAC地址过于分散，可能表示多设备操作
                            if mac_concentration > 0.3:  # 超过30%的交易来自不同MAC
                                is_mac_suspicious = True
                                keywords.add('设备分散')
                except Exception:
                    logger.warning("检测MAC地址风险时出错")

                # 综合判断风险等级
                if is_network_gambling or is_ip_suspicious or is_mac_suspicious:
                    result_dict['is_network_gambling_suspected'] = '是'
                else:
                    result_dict['is_network_gambling_suspected'] = '否'

                # 处理交易对手样本
                if 'counterparty_name' in g.columns:
                    counterparty_names = g['counterparty_name'].dropna().astype(str)
                    filtered_counterparties = []
                    non_suspicious_keywords = ['手续费', '服务费', '系统', '自动', '结算', '财付通', '微信', '支付宝', '银联', '代扣', '平台', '科技', '银行']
                    for name in counterparty_names:
                        if name and not any(kw in name for kw in non_suspicious_keywords):
                            filtered_counterparties.append(name)
                    # 去重并限制最多20个对手方
                    unique_counterparties = list(dict.fromkeys(filtered_counterparties))[:20]
                    result_dict['counterparty_sample'] = ';'.join(unique_counterparties)

                result_dict['case_id'] = self._safe_convert_to_str(case_id, '')
                results.append(result_dict)
                processed_cases.add(case_id)
                
            except Exception as e:
                logger.error(f"处理案例 {case_id} 时出错: {str(e)}")
                continue  # 跳过有问题的案例，继续处理其他案例
        
        return results, processed_cases

    def preprocess_csv(self, input_csv_path: str, output_csv_path: str) -> Dict[str, Any]:
        """
        预处理CSV文件：将原始交易级CSV按案例编号聚合为案例级CSV
        
        Args:
            input_csv_path: 输入CSV文件路径
            output_csv_path: 输出CSV文件路径
            
        Returns:
            包含处理结果的字典
        """
        try:
            logger.info(f"开始预处理CSV文件: {input_csv_path}")
            
            # 初始化汇总结果存储
            all_groups = {}
            total_processed_rows = 0
            total_chunks = 0

            # 重置已见case_id+trans_key组合集合，开始新的处理任务
            self.seen_case_trans_keys = set()

            # 使用分块读取处理大文件
            # 设置dtype为str以避免混合类型问题，然后在后续处理中进行适当转换
            chunk_iter = pd.read_csv(
                input_csv_path, 
                encoding='utf-8', 
                header=None, 
                names=list(self.column_mapping.keys()),
                chunksize=self.chunk_size,
                dtype=str,  # 使用字符串类型避免混合类型问题
                on_bad_lines='skip'  # 跳过格式错误的行
            )
            
            for chunk_idx, chunk_df in enumerate(chunk_iter):
                logger.info(f"正在处理第 {chunk_idx + 1} 个数据块，包含 {len(chunk_df)} 行数据")
                
                # 重命名列
                chunk_df.rename(columns=self.column_mapping, inplace=True)

                # 确保关键字存在
                required_columns = ['case_id', 'main_cust_name', 'trans_amt', 'trans_datetime']
                missing_columns = [col for col in required_columns if col not in chunk_df.columns]
                if missing_columns:
                    raise ValueError(f"缺少必要字段: {missing_columns}")

                # 处理当前块
                processed_chunk = self._process_chunk(chunk_df)
                
                # 按case_id分组合并数据，而不是立即处理
                for case_id, group in processed_chunk.groupby('case_id'):
                    if case_id in all_groups:
                        # 如果case_id已存在，将新的数据追加到现有的组中
                        all_groups[case_id] = pd.concat([all_groups[case_id], group], ignore_index=True)
                    else:
                        # 否则，创建新的组
                        all_groups[case_id] = group

                total_processed_rows += len(chunk_df)
                total_chunks += 1

                logger.info(f"第 {chunk_idx + 1} 个数据块处理完成")

            # 对所有合并后的案例组进行聚合处理
            all_results = {}
            for case_id, group in all_groups.items():
                # 创建临时DataFrame用于聚合
                temp_grouped = [(case_id, group)]
                chunk_results, _ = self._aggregate_case_data(temp_grouped)
                if chunk_results:
                    all_results[case_id] = chunk_results[0]

            # 将结果转换为DataFrame
            if not all_results:
                logger.warning("没有成功处理任何案例，可能输入数据存在问题")
                return {
                    "success": False,
                    "message": "没有成功处理任何案例，请检查输入数据格式",
                    "processed_count": 0,
                    "output_file": None
                }

            result = pd.DataFrame(list(all_results.values()))

            # 确保所有列都存在
            expected_columns = [
                'case_id', 'main_cust_name', 'main_cust_id', 'main_cust_industry',
                'main_cust_gender', 'main_cust_open_date','main_cust_addr','main_cust_phone_number', 'id_type', 'id_number',
                'total_trans_amt', 'trans_count', 'avg_trans_amt',
                'max_trans_amt', 'first_trans_date', 'last_trans_date',
                'report_start_date', 'report_end_date', 'night_trans_count',
                'risk_keywords', 'counterparty_sample', 'top_opposing_areas',
                'main_tnx_channels', 'sample_trx_list', 'debit_count',
                'debit_amt', 'credit_count', 'credit_amt',
                'model_name', 'is_network_gambling_suspected', 'tr_org','features','highest_score',
                'ipv6_addr','ip_addr','mac_addr', 'integer_trans_info'
            ]

            for col in expected_columns:
                if col not in result.columns:
                    result[col] = ''

            result = result[expected_columns]

            # 保存结果，不包含列名
            result.to_csv(output_csv_path, index=False, encoding='utf-8-sig', header=False)
            
            logger.info(f"预处理完成！共处理 {total_processed_rows} 行数据，{total_chunks} 个数据块，共生成 {len(result)} 个案例，已保存至 {output_csv_path}")
            
            return {
                "success": True,
                "message": f"预处理完成，共处理 {total_processed_rows} 行数据，{total_chunks} 个数据块，生成 {len(result)} 个案例",
                "processed_count": len(result),
                "output_file": output_csv_path
            }
            
        except Exception as e:
            logger.error(f"预处理CSV文件时出错: {str(e)}")
            return {
                "success": False,
                "message": f"预处理失败: {str(e)}",
                "processed_count": 0,
                "output_file": None
            }
        finally:
            # 处理完成后重置已见case_id+trans_key组合集合
            self.seen_case_trans_keys = set()

    def process_csv_content(self, csv_content: str, output_csv_path: str) -> Dict[str, Any]:
        """
        直接处理CSV内容字符串
        
        Args:
            csv_content: CSV内容字符串
            output_csv_path: 输出CSV文件路径
            
        Returns:
            包含处理结果的字典
        """
        try:
            # 创建临时文件来处理CSV内容
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as temp_file:
                temp_file.write(csv_content)
                temp_file_path = temp_file.name

            # 调用预处理方法
            result = self.preprocess_csv(temp_file_path, output_csv_path)
            
            # 删除临时文件
            os.unlink(temp_file_path)
            
            return result
        except Exception as e:
            logger.error(f"处理CSV内容时出错: {str(e)}")
            return {
                "success": False,
                "message": f"处理CSV内容失败: {str(e)}",
                "processed_count": 0,
                "output_file": None
            }

    def _get_representative_ip(self, group, column_name):
        """
        获取代表性的IP地址用于反洗钱分析
        在一个案例的所有交易中，返回最常见或第一个非空的IP地址
        """
        try:
            if group is None or column_name not in group.columns:
                return ''
            
            # 获取非空的IP地址
            ip_values = group[column_name].dropna()
            if len(ip_values) == 0 or ip_values.isna().all():
                return ''
            
            # 统计IP地址出现频率，返回最常见的IP地址
            ip_counts = ip_values.value_counts()
            if len(ip_counts) > 0 and not ip_counts.empty:
                # 返回出现次数最多的IP地址
                most_common_ip = ip_counts.index[0:10].tolist()
                return ','.join(map(str, most_common_ip)) if most_common_ip else ''
            else:
                # 如果无法统计，返回第一个非空IP
                valid_ips = ip_values.dropna()
                if len(valid_ips) > 0:
                    first_ip = valid_ips.iloc[0]
                    return str(first_ip) if pd.notna(first_ip) else ''
                else:
                    return ''
        except Exception as e:
            logger.warning(f"获取代表性IP地址时出错: {str(e)}, 列名: {column_name}")
            return ''

    def _get_representative_mac(self, group, column_name):
        """
        获取代表性的MAC地址用于反洗钱分析
        在一个案例的所有交易中，返回最常见或第一个非空的MAC地址
        """
        try:
            if group is None or column_name not in group.columns:
                return ''
            
            # 获取非空的MAC地址
            mac_values = group[column_name].dropna()
            if len(mac_values) == 0 or mac_values.isna().all():
                return ''
            
            # 统计MAC地址出现频率，返回最常见的MAC地址
            mac_counts = mac_values.value_counts()
            if len(mac_counts) > 0 and not mac_counts.empty:
                # 返回出现次数最多的MAC地址
                most_common_mac = mac_counts.index[0:10].tolist()
                return ','.join(map(str, most_common_mac)) if most_common_mac else ''
            else:
                # 如果无法统计，返回第一个非空MAC
                valid_macs = mac_values.dropna()
                if len(valid_macs) > 0:
                    first_mac = valid_macs.iloc[0]
                    return str(first_mac) if pd.notna(first_mac) else ''
                else:
                    return ''
        except Exception as e:
            logger.warning(f"获取代表性MAC地址时出错: {str(e)}, 列名: {column_name}")
            return ''
# 用于Dify等平台的函数接口
def process_csv_for_dify(csv_file_path: str = None, csv_content: str = None, output_path: str = None) -> Dict[str, Any]:
    """
    为Dify等平台设计的CSV处理函数，支持文件路径或内容输入

    Args:
        csv_file_path: CSV文件路径 (可选)
        csv_content: CSV内容字符串 (可选)
        output_path: 输出文件路径 (可选，如果不提供则生成临时文件)

    Returns:
        包含处理结果的字典
    """
    service = CSVProcessingService()

    # 如果没有提供输出路径，创建临时文件
    if not output_path:
        output_path = os.path.join(tempfile.gettempdir(), f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    if csv_file_path:
        return service.preprocess_csv(csv_file_path, output_path)
    elif csv_content:
        return service.process_csv_content(csv_content, output_path)
    else:
        return {
            "success": False,
            "message": "必须提供csv_file_path或csv_content参数",
            "processed_count": 0,
            "output_file": None
        }