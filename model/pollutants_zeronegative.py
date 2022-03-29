import datetime
import pandas as pd
import numpy as np
from utils.get_logger import Logger

log = Logger().logger

class ZeroNegative:

    def __init__(self, orig_df, params_dict, dt_params_dict=None):
        self.orig_df = orig_df
        self.params_dict = params_dict

        if dt_params_dict is not None and dt_params_dict['SCLX'] == 'LX':
            self.filtered_duration = int(dt_params_dict['SC'])
        else:
            self.filtered_duration = None

    def get_abnormal_data(self):
        try:
            # 数据
            df = self.orig_df[self.orig_df['是否生产'] == 1]

            # 筛选出监测值是0或负值，且浓度标准值大于0的数据
            abnormal_df = df[(df['污染物浓度值'] <= 0) & (df['污染物浓度上限值'] > 0)]

            # 排序
            abnormal_df.sort_values(by=['排口ID', '污染物ID', '监测时间'], inplace=True)

            # 监测时间cumsum
            abnormal_df['abnormal_no'] = (abnormal_df['监测时间'] != abnormal_df['监测时间'].shift() + datetime.timedelta(hours=1)).cumsum()
            
            # 通过abnormal_no分组
            abnormal_group = abnormal_df.groupby(['排口ID', '污染物ID', 'abnormal_no']).agg(
                起始时间=('监测时间', 'first'),
                结束时间=('监测时间', 'last'),
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
                '一级场景名称': '浓度0负值',
                '一级场景代码': 'NDLFZ',
                '二级场景名称': '小时浓度0负值',
                '二级场景代码': 'Hour_NDLFZ'
            }

            for k, v in abnormal_dict.items():
                abnormal_group[k] = v
            
            # 根据持续时长来判断三级场景
            abnormal_group['三级场景名称'] = np.where(abnormal_group['持续时长'] >= int(self.params_dict['hours']), '连续小时0负值', '普通小时0负值')
            abnormal_group['三级场景代码'] = np.where(abnormal_group['持续时长'] >= int(self.params_dict['hours']), 'Keep_Hour_NDLFZ', 'One_Hour_NDLFZ')

            # 如果指定时长
            if self.filtered_duration is not None:
                abnormal_group = abnormal_group[abnormal_group['持续时长'] >= self.filtered_duration]

            del abnormal_group['abnormal_no']

            return abnormal_group


        except Exception as e:
            log.error(f'【零负值】{repr(e)}')
            return pd.DataFrame()
