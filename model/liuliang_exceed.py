# 流量异常
import datetime
import numpy as np
import pandas as pd
from utils.get_logger import Logger

log = Logger().logger

class LL:

    def __init__(self, orig_df, exceed_amount):

        self.orig_df = orig_df
        self.exceed_amount = float(exceed_amount)

    def get_abnormal_data(self):
        try:
            # 停产状态数据
            df = self.orig_df[self.orig_df['是否生产'] == 0]

            # 因为流量异常场景与具体污染物无关，所以污染物ID列变成空值
            df['污染物ID'] = None

            # 因为是流量数据，所以把因污染物ID而复制导致重复的流量数据删掉
            df_ll = df.drop_duplicates(subset=['排口ID', '监测时间'], keep='first')

            # 筛选出流量异常数据
            abnormal_df = df_ll[df_ll['流量'] >= self.exceed_amount]

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '流量异常',
                '一级场景代码': 'LLYC',
                '二级场景名称': '小时流量异常',
                '二级场景代码': 'Hour_LLYC',
                '三级场景名称': '普通小时流量异常',
                '三级场景代码': 'One_Hour_LLYC'
            }

            for k, v in abnormal_dict.items():
                abnormal_df[k] = v

            # 因为没有做group，但是要和其他场景的数据合并，所以增加以下列
            for i in ['起始时间', '结束时间']:
                abnormal_df[i] = abnormal_df['监测时间']

            # 这个场景不用统计连续时长
            abnormal_df['持续时长'] = 1

            abnormal_df.rename(columns={
                '污染物浓度值': '污染物浓度均值',
                '流量': '流量均值',
                '排放量': '排放总量'
            }, inplace=True)

            return abnormal_df[['一级场景名称', '一级场景代码', '二级场景名称', '二级场景代码', '三级场景名称', '三级场景代码',
                                '排口ID', '污染物ID', '起始时间', '结束时间', '持续时长', '污染物浓度均值',
                                '生产状态_流量中位数', '流量均值','排放总量']]

        except Exception as e:
            log.error(f'【流量】{repr(e)}')
            return pd.DataFrame()
