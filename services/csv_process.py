import sys
import pandas as pd
from datetime import datetime, timedelta


def aggregate_case_data(input_csv, output_csv):
    """
    将原始交易级CSV按案例编号聚合为案例级CSV
    """
    # 中文列名映射为英文变量名（用于内部处理）
    column_mapping = {
        '案例编号': 'case_id',
        '数据日期': 'data_date',
        '主客户编号': 'main_cust_id',
        '主客户名称': 'main_cust_name',
        '证件类型': 'id_type',
        '证件号': 'id_number',
        '主客户职业行业': 'main_cust_industry',
        '主客户性别': 'main_cust_gender',
        '主客户开户日期': 'main_cust_open_date',
        '可疑模型编号': 'suspect_model_id',
        '可疑模型名称': 'suspect_model_name',
        '可疑特征规则编号': 'suspect_rule_id',
        '可疑特征规则特征名称': 'suspect_rule_name',
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
        '交易去向国家': 'fund_dest_country',
        '交易去向地区': 'fund_dest_region',
        '交易IPV6地址': 'ipv6_addr',
        'IP地址': 'ip_addr',
        '交易MAC地址': 'mac_addr',
        '摘要码': 'summary_code',
        '交易备注': 'trans_remark'
    }

    # 读取CSV：支持无列名的CSV输入，数据顺序需与原始列名顺序一致
    df = pd.read_csv(input_csv, encoding='utf-8', header=None, names=list(column_mapping.keys()))

    # 重命名列
    df.rename(columns=column_mapping, inplace=True)

    # 确保关键字存在
    required_columns = ['case_id', 'main_cust_name', 'trans_amt', 'trans_datetime']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"缺少必要字段: {col}")

    # 转换时间字段
    df['trans_datetime'] = pd.to_datetime(df['trans_datetime'], errors='coerce')
    df['trans_date'] = df['trans_datetime'].dt.date

    # 提取小时用于判断夜间交易
    df['hour'] = df['trans_datetime'].dt.hour

    # 聚合函数
    def aggregate_group(g):
        # 夜间交易（23点-6点）
        night_count = ((g['hour'] >= 23) | (g['hour'] <= 6)).sum()

        # 风险关键词
        keywords = set()

        if g['trans_amt'].mean() <= 10:
            keywords.add('小额')
        if len(g) >= 50:
            keywords.add('高频')
        if night_count / len(g) > 0.8:
            keywords.add('夜间')
        if g['counterparty_name'].isna().sum() > len(g) * 0.5:
            keywords.add('匿名')
        if g['fund_usage'].str.contains('充值|返现|游戏|彩票', na=False).any():
            keywords.add('可疑用途')

        # 提取交易样本（前3笔 + 后3笔），排除低价值自动交易
        sample_trx = []

        # 定义低价值交易关键词
        low_value_keywords = ['扣费', '手续费', '服务费', '系统', '自动', '代扣', '短信费', '管理费', '工本费']

        # 过滤掉低价值交易
        pattern = '|'.join(low_value_keywords)
        valid_trx = g[~g['fund_usage'].str.contains(pattern, na=True, case=False)]

        # 如果过滤后数据不足，回退使用原始数据
        if len(valid_trx) == 0:
            valid_trx = g

        # 取前3笔和后3笔
        first_trx = valid_trx.nsmallest(3, 'trans_datetime')
        last_trx = valid_trx.nlargest(3, 'trans_datetime')

        # 合并并去重（按 trans_datetime）
        combined = pd.concat([first_trx, last_trx]).drop_duplicates(subset=['trans_datetime'])

        for _, trx in combined.iterrows():
            sample_trx.append({
                'TR_DT': trx['trans_date'].strftime('%Y-%m-%d') if pd.notna(trx['trans_date']) else '',
                'TR_TM': trx['trans_datetime'].strftime('%H:%M') if pd.notna(trx['trans_datetime']) else '',
                'TR_AMT': float(trx['trans_amt']),
                'CURR_CD': trx['currency'] if pd.notna(trx['currency']) else 'CNY',
                'OPP_NAME': trx['counterparty_name'] if pd.notna(trx['counterparty_name']) else '',
                'FUND_USE': trx['fund_usage'] if pd.notna(trx['fund_usage']) else '',
                'TR_CHNL': str(trx['aml_channel']) if pd.notna(trx['aml_channel']) else '',
                'TR_AREA': str(trx['trans_region']) if pd.notna(trx['trans_region']) else '',
                'SRC_CHNL': str(trx['src_channel']) if pd.notna(trx['src_channel']) else '',
                'TR_ORG': str(trx['trans_org']) if pd.notna(trx['trans_org']) else '',
                'REMARK': str(trx['trans_remark']) if pd.notna(trx['trans_remark']) else ''
            })

        # 交易对手地区统计（转换为字符串）
        top_areas = [str(x) for x in g['trans_region'].value_counts().head(5).index.tolist()]
        main_channels = [str(x) for x in g['aml_channel'].value_counts().head(5).index.tolist()]

        # 优化：兼容字符串 '01', '02' 和整数 1,2
        flag = g['income_pay_flag'].astype(str).str.strip()
        debit_mask = flag == '1'  # 支持 '1', '01'
        credit_mask = flag == '2'  # 支持 '2', '02'
        debit_count = debit_mask.sum()
        debit_amt = float(g[debit_mask]['trans_amt'].sum())
        credit_count = credit_mask.sum()
        credit_amt = float(g[credit_mask]['trans_amt'].sum())

        # 基础聚合结果
        result_dict = {
            # 'case_id': g['case_id'].iloc[0],
            'main_cust_name': g['main_cust_name'].iloc[0],
            'main_cust_id': g['main_cust_id'].iloc[0] if 'main_cust_id' in g.columns else '',
            'main_cust_industry': g['main_cust_industry'].iloc[0] if 'main_cust_industry' in g.columns else '',
            'main_cust_gender': g['main_cust_gender'].iloc[0] if 'main_cust_gender' in g.columns else '',
            'main_cust_open_date': g['main_cust_open_date'].iloc[0] if 'main_cust_open_date' in g.columns else '',
            'id_type': g['id_type'].iloc[0] if 'id_type' in g.columns else '',
            'id_number': g['id_number'].iloc[0] if 'id_number' in g.columns else '',
            # 'suspect_model_id': g['suspect_model_id'].iloc[0] if 'suspect_model_id' in g.columns else '',
            # 'suspect_model_name': g['suspect_model_name'].iloc[0] if 'suspect_model_name' in g.columns else '',
            'total_trans_amt': float(g['trans_amt'].sum()),
            # 'total_cny_amt': float(g['cny_amt'].sum()) if 'cny_amt' in g.columns else 0.0,
            # 'total_usd_amt': float(g['usd_amt'].sum()) if 'usd_amt' in g.columns else 0.0,
            'trans_count': len(g),
            'avg_trans_amt': float(g['trans_amt'].mean()),
            'max_trans_amt': float(g['trans_amt'].max()),
            'first_trans_date': g['trans_date'].min(),
            'last_trans_date': g['trans_date'].max(),
            'report_start_date': (g['trans_date'].min() - timedelta(days=7)).strftime('%Y年%m月%d日'),
            'report_end_date': g['trans_date'].max().strftime('%Y年%m月%d日'),
            'night_trans_count': night_count,
            'risk_keywords': ','.join(sorted(keywords)),
            # 排除已知非可疑对手（如平台、系统、手续费等）
            'counterparty_sample': ';'.join(
                g['counterparty_name']
                .dropna().astype(str)
                .apply(lambda x: x if not any(
                    kw in x for kw in ['手续费', '服务费', '系统', '自动', '结算', '财付通', '微信', '支付宝','银联','代扣','平台','科技','银行']) else '')
                .tolist()
            ),
            'model_name': g['model_name'].iloc[0] if 'suspect_model_name' in g else '',
            'is_network_gambling_suspected':'是' if(
                len(g) >= 50 and
                g['trans_amt'].mean()<=10 and
                night_count /len(g) > 0.8 and
                (g['fund_usage'].str.contains('充值|返现',na=False).any() if 'fund_usage' in g else False)
            ) else '否',
            'sample_trx_list': sample_trx,
            'top_opposing_areas': ','.join(top_areas),
            'main_tnx_channels': ','.join(main_channels),
            'tr_org': g['trans_org'].iloc[0] if 'trans_org' in g else '未知机构',
            'debit_count': debit_count,
            'debit_amt': debit_amt,
            'credit_count': credit_count,
            'credit_amt': credit_amt
        }

        return result_dict

    # 按case_id分组并聚合
    result = df.groupby('case_id').apply(aggregate_group).reset_index()

    # 确保所有列都存在
    expected_columns = [
        'case_id', 'main_cust_name', 'main_cust_id', 'main_cust_industry',
        'main_cust_gender', 'main_cust_open_date', 'id_type', 'id_number',
        'total_trans_amt', 'trans_count', 'avg_trans_amt',
        'max_trans_amt', 'first_trans_date', 'last_trans_date',
        'report_start_date', 'report_end_date', 'night_trans_count',
        'risk_keywords', 'counterparty_sample', 'top_opposing_areas',
        'main_tnx_channels', 'sample_trx_list', 'debit_count',
        'debit_amt', 'credit_count', 'credit_amt',
        'model_name', 'is_network_gambling_suspected', 'tr_org'
    ]

    for col in expected_columns:
        if col not in result.columns:
            result[col] = ''

    result = result[expected_columns]

    # 保存结果
    result.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"聚合完成！共处理 {len(result)} 个案例，已保存至 {output_csv}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python aggregate_cases_for_dify.py <输入CSV> <输出CSV>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    aggregate_case_data(input_file, output_file)