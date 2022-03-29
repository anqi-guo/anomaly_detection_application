import pandas as pd
import numpy as np
import datetime
from utils.get_logger import Logger

log = Logger().logger

class PollutantsExceed:

    def __init__(self, df_dict, params_dict):
        self.df_dict = df_dict
        self.params_dict = params_dict
        # 超标类型字典
        self.exceed_dict = {
            'HOUR': {
                'exceed_type': '小时',
                'duration': 'hours',
                '连续小时浓度超标': 'Keep_Hour_NDCB',
                '普通小时浓度超标': 'One_Hour_NDCB',
                '二级场景名称': '小时浓度超标',
                '二级场景代码': 'Hour_NDCB'
            },
            'DAY': {
                'exceed_type': '日',
                'duration': 'days',
                '连续日浓度超标': 'Keep_Day_NDCB',
                '普通日浓度超标': 'One_Day_NDCB',
                '二级场景名称': '日浓度超标',
                '二级场景代码': 'Day_NDCB'
            }
        }

    def get_abnormal_data(self, df, exceed_type):
        try:
            # 筛选出超标数据
            abnormal_df = df[df['污染物浓度值'] > df['污染物浓度上限值']]

            # 超标倍数
            abnormal_df['超标倍数'] = abnormal_df['污染物浓度值'] / abnormal_df['污染物浓度上限值']

            # 排序
            abnormal_df.sort_values(by=['排口ID', '污染物ID', '监测时间'], inplace=True)

            # cumsum监测时间
            if exceed_type=='HOUR':
                abnormal_df['abnormal_no'] = (
                            abnormal_df['监测时间'] != abnormal_df['监测时间'].shift() + datetime.timedelta(hours=1)).cumsum()
            else:
                abnormal_df['abnormal_no'] = (
                        abnormal_df['监测时间'] != abnormal_df['监测时间'].shift() + datetime.timedelta(days=1)).cumsum()

            # 计算abnormal_no持续时长/天数
            abnormal_group = abnormal_df.groupby(['排口ID', '污染物ID', 'abnormal_no']).agg(
                起始时间=('监测时间', 'min'),
                结束时间=('监测时间', 'max'),
                持续时长=('监测时间', 'count'),
                污染物浓度均值=('污染物浓度值', 'mean'),
                生产状态_流量中位数=('生产状态_流量中位数', 'first'),
                流量最小值=('流量', 'min'),
                流量最大值=('流量', 'max'),
                流量均值=('流量', 'mean'),
                超标倍数=('超标倍数', 'mean'),
                排放总量=('排放量', 'sum')
            ).reset_index()

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '浓度超标',
                '一级场景代码': 'NDCB',
                '二级场景名称': self.exceed_dict[exceed_type]['二级场景名称'],
                '二级场景代码': self.exceed_dict[exceed_type]['二级场景代码']
            }

            for k, v in abnormal_dict.items():
                abnormal_group[k] = v

            # 根据持续时长来判断三级场景
            abnormal_group['三级场景名称'] = np.where(abnormal_group['持续时长'] >= 2,
                                                f'连续{self.exceed_dict[exceed_type]["exceed_type"]}浓度超标',
                                                f'普通{self.exceed_dict[exceed_type]["exceed_type"]}浓度超标')

            abnormal_group['三级场景代码'] = np.where(abnormal_group['持续时长'] >= 2,
                                                self.exceed_dict[exceed_type][f'连续{self.exceed_dict[exceed_type]["exceed_type"]}浓度超标'],
                                                self.exceed_dict[exceed_type][f'普通{self.exceed_dict[exceed_type]["exceed_type"]}浓度超标'])

            del abnormal_group['abnormal_no']

            return abnormal_group

        except Exception as e:
            log.error(f'【超标】{repr(e)}')
            return pd.DataFrame()

    def get_instant_data(self):  # 实时查询
        # 超标类型+时长+时长类型
        exceed_list = zip(self.params_dict['CBLX'].split(','), self.params_dict['CBSC'].split(','), self.params_dict['CBSCLX'].split(','))
        # 循环
        df_list = []
        for exceed in exceed_list:
            exceed_type = exceed[0]
            # 超标时长
            if exceed[2] == 'LX':
                exceed_duration = int(exceed[1])
            else:
                exceed_duration = 1

            df = self.df_dict[f'{self.exceed_dict[exceed_type]["exceed_type"]}数据']
            abnormal_df = self.get_abnormal_data(df, exceed_type)

            # 筛选出超过超标时长的数据
            abnormal_df = abnormal_df[abnormal_df['持续时长'] >= exceed_duration]

            df_list.append(abnormal_df)

        big_abnormal_df = pd.concat(df_list)

        return big_abnormal_df

    def get_daily_data(self):  # 定时查询
        time_dict = {'日数据':'DAY', '小时数据':'HOUR'}
        abnormal_df_list = []
        for k, df in self.df_dict.items():
            if k == '分钟数据':
                pass
            else:
                # 筛选生产状态数据
                df_work = df[df['是否生产'] == 1]
                # 筛选超标数据
                abnormal_df = self.get_abnormal_data(df_work, time_dict[k])
                # 筛选出超过超标时长的数据
                abnormal_df = abnormal_df[abnormal_df['持续时长'] >= int(self.params_dict[self.exceed_dict[time_dict[k]]['duration']])]
                abnormal_df_list.append(abnormal_df)

        big_abnormal_df = pd.concat(abnormal_df_list)

        return big_abnormal_df





