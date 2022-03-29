# 浓度低值

import pandas as pd
import numpy as np
import datetime
from utils.get_logger import Logger

log = Logger().logger


class Low:

    def __init__(self, orig_df, pollutants_list):
        self.orig_df = orig_df
        self.pollutants_list = pollutants_list

    def get_abnormal_data(self):
        try:
            # 生产状态
            df_work = self.orig_df[self.orig_df['是否生产'] == 1]
            pollutants_df = pd.DataFrame(self.pollutants_list)  # 每一行就是一个污染物，列名为WRYZDM(污染物ID), NDBZZ(浓度标准值)，DZSC(低值时长)
            df = pollutants_df.merge(df_work, left_on='WRYZDM', right_on='污染物ID', how='left')

            # 筛选出低值（监测值<=浓度标准值）
            abnormal_df = df[df['污染物浓度值'] <= df['NDBZZ']]

            # 排序
            abnormal_df.sort_values(by=['排口ID', '污染物ID', '监测时间'], inplace=True)

            # 监测时间cumsum
            abnormal_df['abnormal_no'] = (
                        abnormal_df['监测时间'] != abnormal_df['监测时间'].shift() + datetime.timedelta(hours=1)).cumsum()

            # groupby
            abnormal_group = abnormal_df.groupby(['排口ID', '污染物ID', 'abnormal_no']).agg(
                起始时间=('监测时间', 'first'),
                结束时间=('监测时间', 'last'),
                持续时长=('监测时间', 'count'),
                污染物浓度均值=('污染物浓度值', 'mean'),
                生产状态_流量中位数=('生产状态_流量中位数', 'first'),
                流量最小值=('流量', 'min'),
                流量最大值=('流量', 'max'),
                流量均值=('流量', 'mean'),
                低值时长=('DZSC', 'first'),
                低值时长类型=('DZSCLX', 'first'),
                排放总量=('排放量', 'sum')
            ).reset_index()

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '浓度低值',
                '一级场景代码': 'NDDZ',
                '二级场景名称': '小时浓度低值',
                '二级场景代码': 'Hour_NDDZ'
            }

            for k, v in abnormal_dict.items():
                abnormal_group[k] = v

            # 根据持续时长来判断三级场景
            abnormal_group['三级场景名称'] = np.where(abnormal_group['持续时长'] >= 2, '连续小时浓度低值', '普通小时浓度低值')
            abnormal_group['三级场景代码'] = np.where(abnormal_group['持续时长'] >= 2, 'Keep_Hour_NDDZ', 'One_Hour_NDDZ')

            # 过滤掉时长低于低值时长的数据
            abnormal_group = abnormal_group[
                ((abnormal_group['持续时长'] >= abnormal_group['低值时长']) & (abnormal_group['低值时长类型'] == "LX")) | (
                            abnormal_group['低值时长类型'] == 'LJ')]

            del abnormal_group['abnormal_no']

            return abnormal_group

        except Exception as e:
            log.error(f'【低值】{repr(e)}')
            return pd.DataFrame()
