import time
import datetime
import pandas as pd
from utils.get_logger import Logger

log = Logger().logger


class HourlyConstant:

    def __init__(self, orig_df, params_dict, dt_params_dict=None):
        self.orig_df = orig_df
        self.params_dict = params_dict
        if dt_params_dict is not None and dt_params_dict['HZSCLX'] == 'LX':
            self.filtered_duration = int(dt_params_dict['HZSC'])
        else:
            self.filtered_duration = None

    def get_abnormal_data(self):
        # 参数
        hours = int(self.params_dict['hours'])
        water_min = float(self.params_dict['water_min'])

        try:
            # 数据（工作状态）
            abnormal_df = self.orig_df[self.orig_df['是否生产'] == 1]

            # 删掉流量小于流量标准值或10的数据
            #abnormal_df = df[(df['流量'] >= df['生产状态_流量中位数']) | (df['流量'] >= water_min)]

            # 排序
            abnormal_df.sort_values(by=['排口ID', '污染物ID', '监测时间'], inplace=True)
            # 监测时间cumsum
            abnormal_df['abnormal_no1'] = (
                        abnormal_df['监测时间'] != abnormal_df['监测时间'].shift() + datetime.timedelta(hours=1)).cumsum()
            # 监测值cumsum
            abnormal_df['abnormal_no2'] = (abnormal_df['污染物浓度值'] != abnormal_df['污染物浓度值'].shift()).cumsum()

            abnormal_df['rolling_diff'] = abnormal_df['污染物浓度值'].rolling(window=5, min_periods=5, center=False).apply(lambda x: max(x) - min(x))
            abnormal_df['rolling_pctchg'] = abnormal_df['污染物浓度值'].rolling(window=5, min_periods=5, center=False).apply(lambda x: (max(x) - min(x)) / max(x))

            # 恒值时长计算
            abnormal_group = abnormal_df.groupby(['排口ID', '污染物ID', 'abnormal_no1', 'abnormal_no2']).agg(
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

            # 过滤掉持续时长小于设定时长（4小时）的数据
            abnormal_group = abnormal_group[(abnormal_group['持续时长'] >= hours)]

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '浓度恒值',
                '一级场景代码': 'NDHZ',
                '二级场景名称': '小时浓度恒值',
                '二级场景代码': 'Hour_NDHZ',
                '三级场景名称': '单因子小时浓度恒值',
                '三级场景代码': 'One_Hour_NDHZ'
            }

            for k, v in abnormal_dict.items():
                abnormal_group[k] = v

            # 如果指定时长
            if self.filtered_duration is not None:
                abnormal_group = abnormal_group[abnormal_group['持续时长'] >= self.filtered_duration]

            del abnormal_group['abnormal_no1']
            del abnormal_group['abnormal_no2']

            return abnormal_group

        except Exception as e:
            log.error(f'【恒值】{repr(e)}')
            return pd.DataFrame()
