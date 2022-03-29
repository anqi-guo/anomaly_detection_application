from utils.data_util import GetHistoricalData, FilterData, Postprocess, SaveData, ConfFromDB
from model.pollutants_constant import HourlyConstant
from model.pollutants_zeronegative import ZeroNegative
from model.pollutants_exceed import PollutantsExceed
from model.pollutants_sudden import Sudden
from model.pollutants_invert import Invert
import datetime
import pandas as pd
import time
from conf.config import Conf
from utils.get_logger import Logger

conf_local = Conf("conf.ini")
log = Logger().logger

conf = ConfFromDB(db_info=conf_local.get_dict('database_mysql'))
#conf = get_conf_from_db(db_info=conf_local.get_dict('database_mysql'))
    

def ls_job(monitor_type, append_yesterday=False):
    """
    每天定时执行一次，从最近一年数据中找到异常数据
    :param monitor_type: 废气 or 废水
    :param append_yesterday: 是否把昨日预处理数据加入数据库中
    :return: 无
    """
    t0 = time.time()

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    num_days = int(conf.get_dict('time_info')['historical_num_days'])

    log.info(f'{yesterday}:【{monitor_type}】start')

    # 读取数据
    dt_dict1d, dt_dict365d = GetHistoricalData(oracledb=conf.get_dict('database_oracle'),
                                               mysqldb=conf.get_dict('database_mysql'),
                                               influxdb=conf.get_dict('database_influxdb'),
                                               date=yesterday,
                                               num_days=num_days,
                                               monitor_type=monitor_type).get_data_365d(append_yesterday)

    log.info('读取365天数据完毕')
    
    if append_yesterday:
        # 保存昨日数据
        for time_type, df in dt_dict1d.items():
            SaveData(monitor_type=monitor_type,
                     data=df,
                     time_type=time_type,
                     local_db=conf.get_dict('database_mysql')).save_processed_data_to_db()
        log.info('昨日数据保存完毕')
        
    # 模型计算
    new_df_dict = {}

    # 零值或负值模型
    new_df_dict['零负值'] = ZeroNegative(orig_df=dt_dict365d['小时数据'],
                                      params_dict=conf.get_dict('model_zeronegative')).get_abnormal_data()
    log.info(f'零负值：{len(new_df_dict["零负值"])}')

    # 删掉零负值
    for k, v in dt_dict365d.items():
        dt_dict365d[k] = v[v['污染物浓度值'] > 0]

    # 浓度超标模型
    new_df_dict['超标'] = PollutantsExceed(df_dict=dt_dict365d,
                                           params_dict=conf.get_dict('model_exceed')).get_daily_data()
    log.info(f'超标：{len(new_df_dict["超标"])}')
    
    # 先把符合上面场景的数据删掉
    dt_dict365d['小时数据'] = FilterData(orig_df=dt_dict365d['小时数据'],
                                     to_filter_data=new_df_dict['超标'][
                                         new_df_dict['超标']['二级场景名称'] == '小时浓度超标']).get_filtered_data()
    dt_dict365d['日数据'] = FilterData(orig_df=dt_dict365d['日数据'],
                                    to_filter_data=new_df_dict['超标'][
                                        new_df_dict['超标']['二级场景名称'] == '日浓度超标']).get_filtered_data()
    
    # 数据倒挂（废水）
    if monitor_type == '废水':
        new_df_dict['数据倒挂'] = Invert(orig_df=dt_dict365d['小时数据']).get_abnormal_data()
        log.info(f'数据倒挂：{len(new_df_dict["数据倒挂"])}')

        # 再把符合上面场景的数据删掉
        dt_dict365d['小时数据'] = FilterData(orig_df=dt_dict365d['小时数据'],
                                         to_filter_data=new_df_dict['数据倒挂']).get_filtered_data()

    # 恒值
    new_df_dict['恒值'] = HourlyConstant(orig_df=dt_dict365d['小时数据'],
                                       params_dict=conf.get_dict('model_constant')).get_abnormal_data()
    log.info(f'恒值：{len(new_df_dict["恒值"])}')
    
    # 再把符合上面场景的数据删掉
    dt_dict365d['小时数据'] = FilterData(orig_df=dt_dict365d['小时数据'],
                                     to_filter_data=new_df_dict['恒值']).get_filtered_data()

    # 突变
    new_df_dict['突变'] = Sudden(df_dict=dt_dict365d, params_dict=conf.get_dict('model_sudden')).get_daily_data()
    log.info(f'突变：{len(new_df_dict["突变"])}')

    # 合并
    try:
        model_df = pd.concat(new_df_dict.values())
    except Exception as e:
        log.error(e)
        model_df = pd.DataFrame()
        
    # 输出结果表和扩展表
    results_data, details_data = Postprocess(raw_df_dict=dt_dict365d, model_df=model_df,
                               db_info=conf.get_dict('database_oracle')).get_details()
    
    # 保存结果表和扩展表
    SaveData(monitor_type=monitor_type,
             data={'结果表': results_data, '扩展表': details_data},
             local_db=conf.get_dict('database_mysql')).save_daily_results_to_db()

    t1 = time.time()
    log.info(f'历史查询录入{monitor_type}数据{len(results_data)}个，耗时{round((t1 - t0)/60, 2)}分，昨日预处理数据录入数据库：{append_yesterday}')


'''
if __name__ == '__main__':

    for monitor_type in ['废水', '废气']:
        ls_job(monitor_type, append_yesterday=False)
'''

