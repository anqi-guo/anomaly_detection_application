from utils.data_util import GetHistoricalData, SaveData
import datetime
import pandas as pd
import time
from conf.config import Conf
from utils.get_logger import Logger

conf = Conf("conf.ini")
log = Logger().logger


def get_1d_n_save(date, monitor_type):
    # 读取数据
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, '%Y-%m-%d')

    dt_dict1d, _ = GetHistoricalData(oracledb=conf.get_dict('database_oracle'),
                                     mysqldb=conf.get_dict('database_mysql'),
                                     influxdb=conf.get_dict('database_influxdb'),
                                     date=date,
                                     monitor_type=monitor_type).get_data_365d(append_yesterday=True)

    # 保存数据
    for time_type, df in dt_dict1d.items():
        SaveData(monitor_type=monitor_type,
                 data=df,
                 time_type=time_type,
                 local_db=conf.get_dict('database_mysql')).save_processed_data_to_db(date=date)

    log.info(f'{date}: {monitor_type}预处理数据已录入')


def temp():
    dates = pd.date_range(end=datetime.datetime.strptime('2022-03-13','%Y-%m-%d'),
                          start=datetime.datetime.strptime('2022-03-11','%Y-%m-%d'),
                          freq='D')

    for monitor_type in [
        '废水'
    ]:
        for date in dates:
            get_1d_n_save(date, monitor_type)
