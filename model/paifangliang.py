# 1. 某个时间段的总排放量大于设定数值
# 2. 某个时间段比另一个时间段的总排放量小于设定数值

import numpy as np
import pandas as pd
from utils.get_logger import Logger

log = Logger().logger

class PFL:

    def __init__(self, orig_df, params_dict):
        self.orig_df = orig_df
        self.params_dict = params_dict

    def get_one_abnormal_data(self):  # 单个时间段
        try:
            # 读取生产状态数据
            df = self.orig_df[self.orig_df['是否生产'] == 1]
            # 因为这个场景与污染物无关，所以污染物ID列变成空值
            df['污染物ID'] = None
            # 铲掉因为merge污染物ID而重复的列
            df_pfl = df.drop_duplicates(subset=['排口ID', '监测时间'], keep='first')
            # 单个时间段
            single = self.params_dict['ZDYPFL']
            # 开始时间，结束时间
            start_hour, end_hour = [int(t.split(':')[0]) for t in single['SJ'].split(',')]
            # 排放量
            pfl = float(single['PFL'])
            # 筛选出时间段内的数据
            single_df = df_pfl[(df_pfl['监测时间'].dt.hour >= start_hour) & (df['监测时间'].dt.hour <= end_hour)]
            # 计算时间段内的污染物排放总量
            single_group = single_df.groupby(['排口ID','污染物ID'])['排放量'].sum().to_frame('排放总量').reset_index()
            # 筛选出污染物排放总量大于设定数值的数据
            abnormal_single_group = single_group[single_group['排放总量'] >= pfl]
            # 合并原df
            abnormal_df = single_df.merge(abnormal_single_group, on=['排口ID','污染物ID'], how='right')
            # group
            abnormal_group = abnormal_df.groupby(['排口ID', '污染物ID']).agg(
                起始时间=('监测时间', 'min'),
                结束时间=('监测时间', 'max'),
                持续时长=('监测时间', 'count'),
                污染物浓度均值=('污染物浓度值', 'mean'),
                生产状态_流量中位数=('生产状态_流量中位数', 'first'),
                流量最小值=('流量', 'min'),
                流量最大值=('流量', 'max'),
                流量均值=('流量', 'mean'),
                排放总量=('排放量', 'sum')
            ).reset_index()

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '排放异常',
                '一级场景代码': 'PFYC',
                '二级场景名称': '小时排放异常',
                '二级场景代码': 'Hour_PFYC',
                '三级场景名称': '普通小时排放异常',
                '三级场景代码': 'One_Hour_PFYC'
            }

            for k, v in abnormal_dict.items():
                abnormal_group[k] = v

            return abnormal_group

        except Exception as e:
            log.error(f'【排放量】{repr(e)}')
            return pd.DataFrame()

    def get_two_abnormal_data(self):
        try:
            # 读取生产状态数据
            df = self.orig_df[self.orig_df['是否生产'] == 1]
            # 因为这个场景与污染物无关，所以污染物ID列变成空值
            df['污染物ID'] = None
            # 铲掉因为merge污染物ID而重复的列
            df_pfl = df.drop_duplicates(subset=['排口ID', '监测时间'], keep='first')
            # 读参数
            double = self.params_dict['ZDYPFLBD']  # 两个时间段比对
            start_hour0, end_hour0 = [int(t.split(':')[0]) for t in double['SJ'].split(',')]  # 开始时间，结束时间
            start_hour1, end_hour1 = [int(t.split(':')[0]) for t in double['BDSJ'].split(',')]  # 比对开始时间，比对结束时间
            pfl = float(double['PFL'])  # 排放量
            # 增加一列区分两个时间段
            condlist = [
                (df_pfl['监测时间'].dt.hour >= start_hour0) & (df_pfl['监测时间'].dt.hour <= end_hour0),
                (df_pfl['监测时间'].dt.hour >= start_hour1) & (df_pfl['监测时间'].dt.hour <= end_hour1)
                        ]
            choicelist = [1, 2]
            df_pfl['时段'] = np.select(condlist, choicelist)
            # 筛选出两个时段内的数据
            double_df = df_pfl[df_pfl['时段'].isin([1,2])]
            # groupby 时段 并 unstack 时段
            double_group = double_df.groupby(['排口ID','污染物ID','时段'])['排放量'].sum().to_frame('排放总量').unstack()
            # flatten colum names
            double_group.columns = double_group.columns.get_level_values(1)
            # 筛选出时段1比时段2排放量小于设定数值的数据
            abnormal_double_group = double_group[double_group[2] - double_group[1] >= pfl].reset_index()
            # 与原df合并
            abnormal_df = double_df.merge(abnormal_double_group, on=['排口ID','污染物ID'], how='right')

            abnormal_group = abnormal_df.groupby(['排口ID', '污染物ID', '时段']).agg(
                起始时间=('监测时间', 'min'),
                结束时间=('监测时间', 'max'),
                持续时长=('监测时间', 'count'),
                污染物浓度均值=('污染物浓度值', 'mean'),
                工作时段_流量中位数=('工作时段_流量中位数', 'first'),
                流量最小值=('流量', 'min'),
                流量最大值=('流量', 'max'),
                流量均值=('流量', 'mean'),
                排放总量=('排放量', 'sum')
            ).reset_index()

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '排放异常',
                '一级场景代码': 'PFYC',
                '二级场景名称': '小时排放异常',
                '二级场景代码': 'Hour_PFYC',
                '三级场景名称': '对比小时排放异常',
                '三级场景代码': 'Two_Hour_PFYC'
            }

            for k, v in abnormal_dict.items():
                abnormal_group[k] = v

            return abnormal_group


        except Exception as e:
            log.error(repr(e))
            return pd.DataFrame()
