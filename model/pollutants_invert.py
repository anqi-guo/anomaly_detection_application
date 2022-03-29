# 数据倒挂：氨氮w21003浓度大于总氮w21001浓度
import pandas as pd
import numpy as np
import datetime
from utils.get_logger import Logger

log = Logger().logger


class Invert:

    def __init__(self, orig_df):
        self.orig_df = orig_df

    def get_abnormal_data(self):
        try:
            # 生产状态
            df_work = self.orig_df[self.orig_df['是否生产'] == 1]

            # 删掉氨氮浓度超标的数据
            df_work = df_work[~((df_work['污染物ID'] == 'w21003') & (df_work['污染物浓度值'] > df_work['污染物浓度上限值']))]

            # pivot
            df_pivot = df_work.pivot(index=['排口ID', '监测时间'], columns='污染物ID', values='污染物浓度值')

            # 筛选出氨氮浓度大于总氮浓度的数据
            abnormal_df = df_pivot[(df_pivot['w21003'] >= df_pivot['w21001']) & (df_pivot['w21003'] != 0)].reset_index()

            # 改列名
            abnormal_df.rename(columns={'w21001':'COMPAREWRWNDZ'}, inplace=True)
            abnormal_df['COMPAREWRWBH'] = 'w21001'
            abnormal_df['COMPAREWRWMC'] = '总氮'
            abnormal_df['污染物ID'] = 'w21003'

            # 与原始表合并
            abnormal_df = df_work.merge(abnormal_df[['排口ID', '监测时间', '污染物ID', 'COMPAREWRWBH', 'COMPAREWRWMC', 'COMPAREWRWNDZ']],
                                        on=['排口ID', '监测时间','污染物ID'], how='right')

            # 筛选出污染物ID为w21003的数据
            #abnormal_df = abnormal_df[abnormal_df['污染物ID'].isin(['w21003', 'w21001'])]

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '数据倒挂',
                '一级场景代码': 'NDDG',
                '二级场景名称': '小时数据倒挂',
                '二级场景代码': 'Hour_NDDG',
                '三级场景名称': '普通小时数据倒挂',
                '三级场景代码': 'One_Hour_NDDG'
            }

            for k, v in abnormal_dict.items():
                abnormal_df[k] = v

            # del abnormal_group['abnormal_no']

            # 因为没有做group，但是要和其他场景的数据合并，所以增加以下列
            for i in ['起始时间', '结束时间']:
                abnormal_df[i] = abnormal_df['监测时间']

            abnormal_df['持续时长'] = 1

            abnormal_df.rename(columns={
                '污染物浓度值': '污染物浓度均值',
                '流量': '流量均值',
                '排放量': '排放总量'
            }, inplace=True)

            return abnormal_df[['一级场景名称', '一级场景代码', '二级场景名称', '二级场景代码', '三级场景名称', '三级场景代码',
                                '排口ID', '污染物ID', '起始时间', '结束时间', '持续时长', '污染物浓度均值',
                                '生产状态_流量中位数', '流量均值', '排放总量', 'COMPAREWRWBH', 'COMPAREWRWMC', 'COMPAREWRWNDZ']]


        except Exception as e:
            log.error(f'【倒挂】{repr(e)}')
            return pd.DataFrame()
