from utils.get_connect import ConnectDB
import datetime
import pandas as pd
import numpy as np
import warnings
import uuid
import sqlalchemy
from sqlalchemy.sql import text as sa_text
import time
import json
from utils.file_util import get_file_path
from utils.get_logger import Logger

log = Logger().logger

warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None


class AlterTable:

    def __init__(self, db_info, monitor_type=None, data_dict=None):
        # self.data = data_dict
        self.engine = ConnectDB(db_info).connect_mysql()
        self.connection = self.engine.connect()

    def insert(self, table_name, data):
        XH = data['XH']
        PARAMS_JSON = data['PARAMS']
        PARAMS_STR = data['PARAMS']['CXTJBQ']
        self.connection.execute(sa_text(
            f'INSERT INTO {table_name} (XH, CXTJ, CXKSSJ, ZT, CXTJBQ) VALUES ("{XH}", "{PARAMS_JSON}", now(), 1, "{PARAMS_STR}")').execution_options(
            autocommit=True))


    def update(self, table_name, data):
        SCQYS = data["SCQYS"]
        XH = data["XH"]
        self.connection.execute(sa_text(
            f'UPDATE {table_name} SET SCQYS={SCQYS}, ZT=2, CXJSSJ=now() WHERE XH="{XH}"').execution_options(
            autocommit=True))

    def delete_1yb4(self, table_name, date=None):
        if date is not None:
            year_ago = datetime.datetime.strftime(datetime.datetime.combine(date,
                                                                            datetime.datetime.min.time()) - datetime.timedelta(
                days=366), '%Y-%m-%d %H:%M:%S')
        else:
            year_ago = datetime.datetime.strftime(datetime.datetime.combine(datetime.datetime.today(),
                                                                            datetime.datetime.min.time()) - datetime.timedelta(
                days=366), '%Y-%m-%d %H:%M:%S')
        
        self.connection.execute(
            sa_text(f'DELETE FROM {table_name} WHERE 监测时间 < "{year_ago}"').execution_options(autocommit=True))

    def truncate(self, table_name):
        self.connection.execute(sa_text(f'TRUNCATE TABLE {table_name}').execution_options(autocommit=True))


class ReadQuery:

    def __init__(self, db_info, query_file=None, start_date=None, end_date=None, monitor_type=None, time_type=None,
                 query=None):
        self.query_file = query_file
        self.start_date = start_date
        self.end_date = end_date
        self.monitor_type = monitor_type
        self.time_type = time_type
        self.db_info = db_info
        self.query = query

    def get_query(self):
        fd = open(self.query_file, 'r', encoding='utf-8')
        query = fd.read()
        fd.close()
        return query

    def read_query(self):
        if self.query is not None:
            return self.query
        else:
            if self.start_date is not None:
                start_time = datetime.datetime.combine(self.start_date, datetime.datetime.min.time())
                end_time = datetime.datetime.combine(self.end_date, datetime.datetime.max.time())
                if self.monitor_type is not None:
                    db_dict = {'废水': 'fs', '废气': 'fq', '日数据': 'rsj', '小时数据': 'ssj', '分钟数据': 'fsj'}
                    monitor_type = db_dict[self.monitor_type]
                    time_type = db_dict[self.time_type]
                    query = self.get_query().format(monitor_type, time_type, start_time, end_time)
                else:
                    query = self.get_query().format(start_time, end_time)
            else:
                query = self.get_query()
            return query

    def from_oracle(self):
        query = self.read_query()
        engine = ConnectDB(self.db_info).connect_oracle()    
        df = pd.read_sql_query(query, engine, index_col=None)
        return df

    def from_mysql(self):
        query = self.read_query()
        engine = ConnectDB(self.db_info).connect_mysql()
        df = pd.read_sql_query(query, engine, index_col=None)
        return df

    def from_influxdb(self):
        query = self.read_query()
        client = ConnectDB(self.db_info).connect_influxdb()
        result = client.query(query)
        df = result['m_FQ_minute']
        return df


class SaveData:

    def __init__(self, data, monitor_type=None, date=None, model_type=None, remote_db=None, local_db=None,
                 time_type=None):
        self.remote_db = remote_db
        self.local_db = local_db
        self.monitor_type = monitor_type
        self.data = data
        self.model_type = model_type
        self.time_type = time_type
        if date is not None:
            self.date_str = datetime.datetime.strftime(date, '%Y-%m-%d')
        self.db_dict = {'废气': 'fq', '废水': 'fs', '小时数据': 'ssj', '日数据': 'rsj', '分钟数据': 'fsj', '结果表': '', '扩展表': '_zkb'}

    def save_processed_data_to_db(self, date=None):
        engine = ConnectDB(self.local_db).connect_mysql()
        table_name = f't_zxyc_{self.db_dict[self.monitor_type]}_{self.db_dict[self.time_type]}_processed'

        # delete data older than 1 year
        AlterTable(db_info=self.local_db).delete_1yb4(table_name=table_name, date=date)

        self.data.to_sql(name=table_name,
                         con=engine,
                         schema=self.local_db['database'],
                         if_exists='append',
                         chunksize=1000,
                         index=False,
                         dtype={'监测时间': sqlalchemy.DateTime()})

    def save_daily_results_to_db(self):
        engine = ConnectDB(self.local_db).connect_mysql()

        for k, v in self.data.items():
            table_name = f't_zxyc_{self.db_dict[self.monitor_type]}_lsjzb{self.db_dict[k]}'

            # truncate first
            AlterTable(db_info=self.local_db).truncate(table_name)

            v.replace([np.inf, -np.inf], np.nan, inplace=True)
            v.to_sql(name=table_name,
                     con=engine,
                     schema=self.local_db['database'],
                     if_exists='append',
                     chunksize=1000,
                     index=False
                     )

    def save_instant_results_to_db(self):
        engine = ConnectDB(self.local_db).connect_mysql()

        for k, v in self.data.items():
            v.replace([np.inf, -np.inf], None, inplace=True)
            v.to_sql(name=f't_zxyc_{self.monitor_type.lower()}_dtcxjg{self.db_dict[k]}',
                     con=engine,
                     schema=self.local_db['database'],
                     if_exists='append',
                     chunksize=1000,
                     index=False
                     )


class CalcStandardStats:  # 计算流量中位数

    def __init__(self, date, monitor_type, query_file, local_db=None):
        self.local_db = local_db
        self.monitor_type = monitor_type
        self.date = date
        self.query_file = query_file

    def get_stats(self):
        # 读数据库
        data364 = ReadQuery(db_info=self.local_db, query_file=self.query_file,
                            start_date=self.date - datetime.timedelta(days=364),
                            end_date=self.date - datetime.timedelta(days=1),
                            monitor_type=self.monitor_type, time_type='小时数据').from_mysql()

        # 生产状态
        data_work = data364[data364['是否生产'] == 1]
        # 计算工作时间流量中位数
        ll_median = data_work.groupby('排口ID')['流量'].median().round(4).to_frame('生产状态_流量中位数').reset_index()

        return ll_median, data364


class GetNPT:  # 停运时间

    def __init__(self, remote_db=None, start_date=None, end_date=None):
        self.remote_db = remote_db
        self.start_date = start_date
        self.end_date = end_date

    def get_data(self):
        query_file = get_file_path('停运时间.sql')
        npt_data = ReadQuery(db_info=self.remote_db, query_file=query_file).from_oracle()

        # 增加一列，停运时间列表
        npt_data['停运时间'] = npt_data.apply(lambda row: pd.date_range(start=row['停运开始时间'], end=row['停运结束时间'], freq='H'),
                                          axis=1)
        npt_data_explode = npt_data.explode('停运时间').loc[:, ['排口ID', '停运时间']].rename(columns={'停运时间': '监测时间'})
        npt_data_explode['是否生产'] = 0

        # 把停运时间改成以日为单位
        npt_data_day = npt_data_explode.copy(deep=True)
        # 提取监测时间的日期部分
        npt_data_day['停运日期'] = npt_data_day['监测时间'].dt.date
        # 统计每个排口每个停运日期的小时数量
        npt_data_day = npt_data_day.groupby(['排口ID', '停运日期']).size()
        # 如果小时数量大于等于6个小时，则当天停产
        npt_data_day = npt_data_day[npt_data_day >= 6].reset_index().loc[:, ['排口ID', '停运日期']].rename(
            columns={'停运日期': '监测时间'})
        npt_data_day['是否生产'] = 0
        # 把监测时间改成日期格式，即加上时间00:00:00
        npt_data_day['监测时间'] = pd.to_datetime(npt_data_day['监测时间'])

        # 如果传入了日期，则删掉日期区间外的数据
        if self.start_date is not None:
            npt_data_explode = npt_data_explode[
                (npt_data_explode['监测时间'] >= (
                    datetime.datetime.combine(self.start_date, datetime.datetime.min.time()))) & (
                        npt_data_explode['监测时间'] <= datetime.datetime.combine(self.end_date,
                                                                              datetime.datetime.max.time()))]
            npt_data_day = npt_data_day[
                (npt_data_day['监测时间'] >= (datetime.datetime.combine(self.start_date, datetime.datetime.min.time()))) & (
                        npt_data_day['监测时间'] <= datetime.datetime.combine(self.end_date, datetime.datetime.max.time()))]

        return {'小时数据': npt_data_explode, '日数据': npt_data_day}


class GetHistoricalData:  # 每天从原始数据库读取前一天数据，再和公司数据库前一年数据整合

    def __init__(self, oracledb, mysqldb, influxdb, date, monitor_type, num_days=None):
        self.oracledb = oracledb
        self.mysqldb = mysqldb
        self.influxdb = influxdb
        self.date = date
        self.monitor_type = monitor_type
        self.num_days = num_days

    def get_data_1d(self):  # 前一天数据
        
        # 停运数据
        npt_dict = GetNPT(remote_db=self.oracledb, start_date=self.date, end_date=self.date).get_data()

        # 污染物浓度上限值
        ul_query_file = get_file_path('污染物浓度上限值.sql')
        upper_limit_data = ReadQuery(db_info=self.oracledb, query_file=ul_query_file).from_oracle()

        # 算流量中位数
        history_query_file = get_file_path('processed.sql')
        ll_median, hourly_data364 = CalcStandardStats(date=self.date,
                                                    monitor_type=self.monitor_type,
                                                    local_db=self.mysqldb,
                                                    query_file=history_query_file).get_stats()

        mt_dict = {
            '废水': ['日数据', '小时数据'],
            '废气': ['日数据', '小时数据', '分钟数据']
        }

        dt_dict = {}
        for time_type in mt_dict[self.monitor_type]:  # 日数据，小时数据, 分钟数据
            query_file = get_file_path(f'{self.monitor_type}_{time_type}.sql')

            if time_type == '分钟数据':
                df = ReadQuery(db_info=self.influxdb,
                               query_file=query_file,
                               start_date=self.date,
                               end_date=self.date).from_influxdb()
                df = PreprocessMinuteData(df, self.oracledb).get_processed_data()
            else:
                df = ReadQuery(db_info=self.oracledb,
                               query_file=query_file,
                               start_date=self.date,
                               end_date=self.date).from_oracle()

            # 加入是否生产
            if time_type == '分钟数据':
                df['监测时间H'] = df['监测时间'].apply(lambda x: x.replace(minute=0))
                npt_minute = npt_dict['小时数据']
                npt_minute.rename(columns={'监测时间': '监测时间H'}, inplace=True)
                df = df.merge(npt_dict['小时数据'], on=['排口ID', '监测时间H'], how='left')
                del df['监测时间H']
            else:
                df = df.merge(npt_dict[time_type], on=['排口ID', '监测时间'], how='left')

            df['是否生产'] = df['是否生产'].fillna(1)

            # 加入污染物浓度上限值
            df = df.merge(upper_limit_data[['排口ID', '污染物ID', '污染物浓度上限值']], on=['排口ID', '污染物ID'], how='left')

            if time_type == '分钟数据':
                df = df[df['污染物浓度上限值'].notnull()]

            # 加入流量中位数
            df = df.merge(ll_median, on=['排口ID'], how='left')

            dt_dict[time_type] = df

        return dt_dict, hourly_data364

    def get_data_365d(self, append_yesterday):  # 加入前364天数据

        query_file = get_file_path('processed.sql')

        if append_yesterday == True:
            dt_dict1d, hourly_data364 = self.get_data_1d()

            dt_dict365d = {}
            for time_type, df in dt_dict1d.items():
                # 再从公司数据库读前一年的数据
                if time_type == '小时数据':
                    data364 = hourly_data364
                elif time_type == '日数据':
                    data364 = ReadQuery(db_info=self.mysqldb,
                                    query_file=query_file,
                                    monitor_type=self.monitor_type,
                                    time_type=time_type,
                                    start_date=self.date - datetime.timedelta(days=364),
                                    end_date=self.date - datetime.timedelta(days=1)).from_mysql()
                else: # 因为历史查询场景不涉及分钟数据，所以不用提取
                    pass
                
                data365 = data364.append(df, ignore_index=True)
                
                # date type
                data365['监测时间'] = pd.to_datetime(data365['监测时间'])

                dt_dict365d[time_type] = data365

            return dt_dict1d, dt_dict365d

        else:
            dt_dict365d = {}
            for time_type in ['小时数据','日数据']:
                log.info(time_type)
                data365 = ReadQuery(db_info=self.mysqldb,
                                        query_file=query_file,
                                        monitor_type=self.monitor_type,
                                        time_type=time_type,
                                        start_date=self.date-datetime.timedelta(days=364),
                                        end_date=self.date).from_mysql()
                
                data365['监测时间'] = pd.to_datetime(data365['监测时间'])
                
                dt_dict365d[time_type] = data365

            return None, dt_dict365d


class PreprocessMinuteData:

    def __init__(self, df, db_info):
        self.df = df
        self.db_info = db_info

    def get_processed_data(self):
        # 提取Avg结尾的列
        avg_cols = [col for col in self.df.columns if col.endswith('_Avg')]
        # 提取_ZsAvg结尾的列
        zsavg_cols = [col.rsplit('_', 1)[0] for col in self.df.columns if col.endswith('ZsAvg')]
        # 如果有折算值，就用折算值，如果没有，就用平均值
        for pollutant in zsavg_cols:
            self.df[f'{pollutant}_Avg'] = np.where(self.df[f'{pollutant}_ZsAvg'].notnull(),
                                                   self.df[f'{pollutant}_ZsAvg'],
                                                   self.df[f'{pollutant}_Avg'])
        # 排口ID列
        cols = ['jcdxh']
        # 加上_Avg结尾的列
        cols.extend(avg_cols)
        # 只保留以上列
        df_avg = self.df[cols]
        # 列名去掉_Avg后缀
        df_avg.columns = df_avg.columns.str.rstrip("_Avg")
        # 改索引名字
        df_avg.index.name = 'datetime'
        # 要分组的列
        resample_cols = df_avg.columns.tolist()
        resample_cols.remove('jcdxh')
        # 按10分钟分组算平均值
        df_avg_10T = df_avg.groupby(['jcdxh'])[resample_cols].resample('10T').mean().reset_index()
        # 把jcdxh，datetime，流量，含氧量作为索引，并stack污染物ID列
        df_avg_10T_stack = df_avg_10T.set_index(['jcdxh', 'datetime', 'a00000', 'a99411']).stack().reset_index()
        # 列重命名
        df_avg_10T_stack.columns = ['排口ID', '监测时间', '流量', '含氧量', '污染物ID', '污染物浓度值']
        # 排序
        df_avg_10T_stack.sort_values(by=['排口ID', '污染物ID', '监测时间'], inplace=True)
        # 删掉时区
        df_avg_10T_stack['监测时间'] = pd.to_datetime(df_avg_10T_stack['监测时间']).dt.tz_localize(None)
        # 保留小数点后四位
        for col in ['流量', '含氧量', '污染物浓度值']:
            df_avg_10T_stack[col] = df_avg_10T_stack[col].round(4)

        # 过滤排口
        # 排口审核时间
        pk_query_file = get_file_path('排口审核时间.sql')
        pk_df = ReadQuery(db_info=self.db_info, query_file=pk_query_file).from_oracle()

        #log.info(pk_df.columns)

        final_df = df_avg_10T_stack.merge(pk_df, left_on='排口ID', right_on='xh', how='inner')
        final_df = final_df[final_df['监测时间'] > final_df['shsj'] + datetime.timedelta(days=7)]

        del final_df['xh']
        del final_df['shsj']
        
        return final_df


class FilterData:

    def __init__(self, orig_df, to_filter_data, time_freq='H'):
        self.orig_df = orig_df
        self.to_filter_data = to_filter_data
        self.time_freq = time_freq

    def get_filtered_data(self):
        if not self.to_filter_data.empty:
            self.to_filter_data['监测时间'] = self.to_filter_data.apply(
                lambda row: pd.date_range(start=row['起始时间'], end=row['结束时间'], freq=self.time_freq), axis=1)
            to_filter_data_explode = self.to_filter_data.explode('监测时间').loc[:, ['排口ID', '污染物ID', '监测时间']]
            to_filter_data_explode['is_filter'] = 1

            new_df = self.orig_df.merge(to_filter_data_explode, on=['排口ID', '污染物ID', '监测时间'], how='left')

            new_df = new_df[new_df['is_filter'].isnull()]
            del new_df['is_filter']

            return new_df

        else:
            return self.orig_df


class Postprocess:

    def __init__(self, raw_df_dict, model_df, db_info):
        self.raw_df_dict = raw_df_dict
        self.model_df = model_df
        self.db_info = db_info

    def calc_pollutants_cnt_by_company(self, df):
        pollutants_cnt = df.groupby('企业ID').agg(
            企业污染物种类=('污染物名称', pd.Series.unique),
            企业污染物总数=('污染物名称', pd.Series.nunique)
        ).reset_index()

        pollutants_cnt['企业污染物种类'] = pollutants_cnt['企业污染物种类'].astype(str)
        return pollutants_cnt

    def get_results(self):
        if self.model_df.empty:
            log.info('results_df空')
            return pd.DataFrame()
        else:
            try:
                # 静态数据
                static_query_file = get_file_path('静态数据.sql')
                static_data = ReadQuery(db_info=self.db_info, query_file=static_query_file).from_oracle()
                # 污染物名称
                pol_name_file = get_file_path('污染物名称.sql')
                pol_name_data = ReadQuery(db_info=self.db_info, query_file=pol_name_file).from_oracle()
                # 污染物浓度上限值
                upper_limit_file = get_file_path('污染物浓度上限值.sql')
                ul_data = ReadQuery(db_info=self.db_info, query_file=upper_limit_file).from_oracle()
                # 合并
                self.model_df['污染物ID'] = self.model_df['污染物ID'].astype(str)

                results_data = self.model_df.merge(static_data, how='inner', on=['排口ID']) \
                    .merge(pol_name_data, how='left', on=['污染物ID']) \
                    .merge(ul_data, how='left', on=['排口ID', '污染物ID'])

                # 各企业的污染物种类和数量
                pollutants_cnt = self.calc_pollutants_cnt_by_company(results_data)
                # 合并
                results_data = results_data.merge(pollutants_cnt, how='left', on='企业ID')

                
                
                # 增加异常事件ID
                results_data['异常事件ID'] = results_data.apply(lambda
                                                                row: f'[{row["一级场景名称"]}]{row["企业名称"]}-{row["排口名称"]}({str(row["起始时间"])[:10]}@{str(row["起始时间"])[11:]})',
                                                            axis=1)

                # 增加序号
                id_list = [uuid.uuid4() for _ in range(len(results_data))]
                results_data['序号'] = id_list

                # RENAME
                results_data.rename(columns={
                    '序号': 'XH',
                    '一级场景代码': 'YJCJDM',
                    '一级场景名称': 'YJCJMC',
                    '二级场景代码': 'EJCJDM',
                    '二级场景名称': 'EJCJMC',
                    '三级场景代码': 'SJCJDM',
                    '三级场景名称': 'SJCJMC',
                    '异常事件ID': 'SJID',
                    '城市名称': 'DQMC',
                    '区县名称': 'QXMC',
                    '企业ID': 'QYID',
                    '企业名称': 'QYMC',
                    '排口ID': 'PKBH',
                    '排口名称': 'PKMC',
                    '起始时间': 'XXKSSJ',
                    '结束时间': 'JSJZSJ',
                    '持续时长': 'CXSC',
                    '污染物ID': 'WRWBH',
                    '污染物名称': 'WRWMC',
                    '污染物浓度均值': 'WRWNDZ',
                    '污染物浓度上限值': 'CBXZ',
                    '企业污染物总数': 'WRWZS',
                    '企业污染物种类': 'WRWZL',
                    '流量均值': 'LLJZ',
                    '流量最大值': 'LLZDZ',
                    '流量最小值': 'LLZXZ',
                    '生产状态_流量中位数': 'LLBZZ',
                    '超标倍数': 'WRWCBBS',
                    '较上一时间变化幅度': 'WRWBHFD',
                    '排放总量': 'PFZL'
                }, inplace=True)

                # 列顺序
                col_order = [
                    'XH', 'YJCJDM', 'YJCJMC', 'EJCJDM', 'EJCJMC', 'SJCJDM', 'SJCJMC', 'SJID', 'DQMC', 'QXMC', 'QYID',
                    'QYMC', 'PKBH', 'PKMC', 'XXKSSJ', 'JSJZSJ', 'CXSC', 'WRWBH', 'WRWMC', 'WRWNDZ', 'CBXZ', 'WRWZS', 'WRWZL',
                    'LLJZ', 'LLZDZ', 'LLZXZ', 'LLBZZ', 'WRWCBBS', 'WRWBHFD','PFZL', 'COMPAREWRWBH', 'COMPAREWRWMC', 'COMPAREWRWNDZ'
                ]

                for col in col_order:
                    if col not in results_data.columns:
                        results_data[col] = None

                results_data = results_data[col_order]

                return results_data

            except Exception as e:
                log.error(f'【结果表】{repr(e)}')
                return pd.DataFrame()

    def get_details(self):
        try:
            results_df = self.get_results()
            
            if results_df.empty:
                return pd.DataFrame(), pd.DataFrame()
            else:
                results_copy = results_df.copy(deep=True)

                # 将results分成3个时间类型的表
                results_copy['time_type'] = np.where(results_copy['EJCJMC'].str.contains('日'), '日数据',
                                                 np.where(results_copy['EJCJMC'].str.contains('分'), '分钟数据', '小时数据'))

                time_dict = {'分钟数据': 'T', '小时数据': 'H', '日数据': 'D'}
                details_list = []
                for col in results_copy['time_type'].unique():
                    results_copy_t = results_copy[results_copy['time_type'] == col]

                    # 生成时间列表
                    results_copy_t['监测时间'] = results_copy_t.apply(
                    lambda row: pd.date_range(start=row['XXKSSJ'], end=row['JSJZSJ'], freq=time_dict[col]),
                    axis=1)
                    # explode监测时间
                    results_copy_explode = results_copy_t.explode('监测时间')

                    # 与df_dict对应时间类型的df合并
                    raw_df = self.raw_df_dict[col]
                    raw_df['监测时间'] = pd.to_datetime(raw_df['监测时间'])
                    details_df_t = results_copy_explode.merge(raw_df, left_on=['PKBH', 'WRWBH', '监测时间'],
                                                          right_on=['排口ID', '污染物ID', '监测时间'], how='left')

                    # 只保留以下列
                    details_df_t = details_df_t[['XH', '监测时间', '污染物浓度值', '流量']]
                    # 改列名
                    details_df_t.columns = ['YCSJXH', 'JCSJ', 'WRWNDZ', 'LL']

                    details_list.append(details_df_t)

                details_df = pd.concat(details_list)

                # 增加uuid序号
                id_list = [uuid.uuid4() for _ in range(len(details_df))]
                details_df.insert(loc=0, column='XH', value=id_list)

                return results_df, details_df

        except Exception as e:
            log.error(f'【展开表】{repr(e)}')
            return pd.DataFrame(), pd.DataFrame()


class GetInstantData:

    def __init__(self, params, conf_input):
        self.params = params
        self.conf_input = conf_input

    def get_data(self):
        # 监测类型
        monitor_type_dict = {'FS': '废水', 'FQ': '废气'}
        monitor_type = monitor_type_dict[self.params['JCLX']]
        print(monitor_type)

        # 日期
        start_date = datetime.datetime.strptime(self.params['TJSJ']['KSSJ'], '%Y-%m-%d')  # 开始日期
        start_time = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date = datetime.datetime.strptime(self.params['TJSJ']['JSSJ'], '%Y-%m-%d')  # 结束日期
        end_time = datetime.datetime.combine(end_date, datetime.datetime.max.time())

        # 日期类型
        time_type_list = ['小时数据']
        if ('DAY' in self.params['NDCB']['CBLX']) or (
                self.params['NDRJZTB']['TBLX'] != ''):
            time_type_list.append('日数据')
        if self.params['NDFZTB']['TBLX'] != '':
            time_type_list.append('分钟数据')

        add_query_list = []
        # 污染物ID
        if self.params['WRYZ'] != 'ALL':
            pollutants_list = self.params['WRYZ'].split(',')
            pollutants_tuple = tuple(pollutants_list)
            add_query_list.append(f'AND 污染物ID IN {pollutants_tuple}')

        if monitor_type == '废气':
            add_query_list.append(f'AND 含氧量 >= {self.params["QZTJ"]["HYL"]}')

        add_query = ' '.join(add_query_list)

        # time_dict
        time_dict = {'小时数据': 'ssj', '日数据': 'rsj', '分钟数据': 'fsj'}

        # 读数据
        df_dict = {}
        for time_type in time_type_list:
            query = f'SELECT * FROM t_zxyc_{self.params["JCLX"].lower()}_{time_dict[time_type]}_processed WHERE 监测时间 BETWEEN "{start_time}" AND "{end_time}"' + add_query
            df = ReadQuery(db_info=self.conf_input.get_dict('database_mysql'),
                           query=query).from_mysql()
            df['监测时间'] = pd.to_datetime(df['监测时间'])

            df_dict[time_type] = df

        return df_dict


class ConfFromDB:
    def __init__(self, db_info):
        conf_df = ReadQuery(db_info=db_info, query='select * from t_zxyc_conf').from_mysql()
        self.big_dict = {}
        for k1 in conf_df['c1'].unique():
            sub_df = conf_df[conf_df['c1']==k1]
            sub_df.set_index('c2', inplace=True)
            self.big_dict[k1] = sub_df.to_dict()['c3']

    def get_dict(self, key):
        return self.big_dict[key]

def get_conf_from_db(db_info):
    conf_df = ReadQuery(db_info=db_info, query='select * from t_zxyc_conf').from_mysql()
    big_dict = {}
    for k1 in conf_df['c1'].unique():
        sub_df = conf_df[conf_df['c1']==k1]
        sub_df.set_index('c2', inplace=True)
        big_dict[k1] = sub_df.to_dict()['c3']
    return big_dict
