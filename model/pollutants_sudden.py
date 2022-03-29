import numpy as np
import pandas as pd
import warnings
from utils.get_logger import Logger

log = Logger().logger
warnings.filterwarnings('ignore')


# 排放浓度突变
class Sudden:

    def __init__(self, df_dict, params_dict, time_type=None):
        self.df_dict = df_dict
        self.params_dict = params_dict
        self.time_type = time_type
        self.sudden_dict = {'MINUTE': '分钟',
                            '分钟': 'MINUTE',
                            'HOUR': '小时',
                            '小时': 'HOUR',
                            'DAY': '日',
                            '日': 'DAY',
                            'TZ': '突增',
                            'TJ': '突降'}

    def get_abnormal_data(self, df, sudden_type, sudden_ratio, sudden_direction):
        time2sec_dict = {
            'DAY': 24 * 60 * 60,
            'HOUR': 60 * 60,
            'MINUTE': 10 * 60
        }
        try:
            # 删除零负值
            df = df[df['污染物浓度值'] > 0]

            # 排序
            df.sort_values(by=['排口ID', '污染物ID', '监测时间'], inplace=True)

            # 判断是否超标
            df['is_exceed'] = np.where(df['污染物浓度值'] > df['污染物浓度上限值'], 1, 0)

            # 对是否超标列做shift，这样每一行的这个值就代表上一个值是否超标
            df['previous_is_exceed'] = df['is_exceed'].shift()

            # 监测时间diff
            df['seconds_diff'] = df.groupby(['排口ID', '污染物ID'])['监测时间'].diff().dt.total_seconds()

            # 计算pct_chg
            df['较上一时间变化幅度'] = df.groupby(['排口ID', '污染物ID'])['污染物浓度值'].pct_change().round(6) * 100

            # 过滤掉超标的，等于inf的，和监测时间不连续的数据
            df = df[(df['previous_is_exceed'] == 0) & (df['较上一时间变化幅度'] != np.inf) & (
                    df['seconds_diff'] == time2sec_dict[sudden_type])]

            # 过滤掉幅度低的数据
            if sudden_direction == 'TZ':
                abnormal_df = df[df['较上一时间变化幅度'] >= float(sudden_ratio)]
            elif sudden_direction == 'TJ':
                abnormal_df = df[df['较上一时间变化幅度'] <= -1 * float(sudden_ratio)]

            # 增加以下几列
            abnormal_dict = {
                '一级场景名称': '浓度突变',
                '一级场景代码': 'NDTB',
                '二级场景名称': f'{self.sudden_dict[sudden_type]}浓度突变',
                '二级场景代码': f'{sudden_type.capitalize()}_NDTB',
                '三级场景名称': f'{self.sudden_dict[sudden_type]}浓度{self.sudden_dict[sudden_direction]}',
                '三级场景代码': f'{sudden_type.capitalize()}_NDTB_{sudden_direction}'
            }

            for k, v in abnormal_dict.items():
                abnormal_df[k] = v

            # 因为没有做group，但是要和其他场景的数据合并，所以增加以下列
            for i in ['起始时间', '结束时间']:
                abnormal_df[i] = abnormal_df['监测时间']

            abnormal_df['持续时长'] = 1
            
            abnormal_df.rename(columns={
                '污染物浓度值': '污染物浓度均值',
                '流量': '流量均值'
            }, inplace=True)

            columns = ['一级场景名称', '一级场景代码', '二级场景名称', '二级场景代码', '三级场景名称', '三级场景代码',
                                '排口ID', '污染物ID', '起始时间', '结束时间', '持续时长', '污染物浓度均值',
                                '生产状态_流量中位数', '流量均值', '较上一时间变化幅度']

            if self.time_type!='MINUTE':
                abnormal_df.rename(columns={'排放量':'排放总量'}, inplace=True)
                columns.append('排放总量')

            return abnormal_df[columns]

        except Exception as e:
            log.error(f'【突变】{repr(e)}')
            return pd.DataFrame()

    def get_instant_data(self):  # 实时查询
        sudden_list = zip(self.params_dict['TBLX'].split(','), self.params_dict['FDZ'].split(','))
                                           
        # 提取对应时间类型的数据
        df = self.df_dict.copy()
        
        # 计算异常数据
        df_list = []
        for sudden in sudden_list:
            sudden_direction = sudden[0]
            sudden_ratio = int(sudden[1])
            abnormal_df = self.get_abnormal_data(df, self.time_type, sudden_ratio, sudden_direction)
            df_list.append(abnormal_df)
        
        big_abnormal_df = pd.concat(df_list)

        return big_abnormal_df

    def get_daily_data(self):  # 定时查询
        sudden_ratio = float(self.params_dict['sudden_ratio'])  # 突变幅度（eg. 20%如果输入的是20则需要除以100）

        # 分钟、小时和日数据
        abnormal_df_list = []
        for k, df in self.df_dict.items():  # k->日数据，小时数据，分钟数据
            df_work = df[df['是否生产'] == 1]
            sudden_type = self.sudden_dict[k[:-2]]
            for sudden_direction in ['TZ', 'TJ']:
                abnormal_data = self.get_abnormal_data(df_work, sudden_type, sudden_ratio, sudden_direction)
                abnormal_df_list.append(abnormal_data)

        big_abnormal_df = pd.concat(abnormal_df_list)
        return big_abnormal_df
