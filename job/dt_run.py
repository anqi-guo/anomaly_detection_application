from utils.data_util import AlterTable, FilterData, Postprocess, SaveData, GetInstantData, ConfFromDB
from model.pollutants_exceed import PollutantsExceed
from model.pollutants_low import Low
from model.pollutants_zeronegative import ZeroNegative
from model.pollutants_constant import HourlyConstant
from model.liuliang_exceed import LL
from model.pollutants_sudden import Sudden
from model.pollutants_invert import Invert
from model.paifangliang import PFL
import uuid
import time
import datetime
import pandas as pd
import json
from conf.config import Conf
from utils.get_logger import Logger

conf_local = Conf("conf.ini")
log = Logger().logger
conf = ConfFromDB(db_info=conf_local.get_dict('database_mysql'))


def dt_job(params, conf):
    """
    动态查询
    :param params: 参数
    :param conf: 配置
    :return: 无
    """
    t0 = time.time()

    # 先传入一条查询数据到数据库
    intro_id = uuid.uuid4()

    AlterTable(db_info=conf.get_dict('database_mysql')).insert(table_name=f't_zxyc_{params["JCLX"].lower()}_dtcxtj',
                                                               data={'XH':intro_id, 'PARAMS':params})


    # 读取数据
    df_dict = GetInstantData(params=params, conf_input=conf).get_data()
    df_dict_orig = df_dict.copy()

    for k,v in df_dict.items():
        log.info(f'{k}:{len(v)}')

    # 计算模型
    # 建立一个空字典，用来存放异常数据
    new_df_dict = {}

    # 零负值
    if params['ND0FZ']['SC'] != '':
        new_df_dict['零负值'] = ZeroNegative(orig_df=df_dict['小时数据'],
                                          params_dict=conf.get_dict('model_zeronegative'),
                                          dt_params_dict=params['ND0FZ']).get_abnormal_data()

        log.info(f'查询ID：{intro_id}，零负值场景数据量：{len(new_df_dict["零负值"])}')

        # 删掉符合以上场景的数据
        df_dict['小时数据'] = FilterData(orig_df=df_dict['小时数据'],
                                     to_filter_data=new_df_dict['零负值']).get_filtered_data()

    # 删除所有零负值
    for k, v in df_dict.items():
        df_dict[k] = v[v['污染物浓度值'] > 0]

    # 流量异常
    if params['LLYC']['LLZ'] != '':
        new_df_dict['流量异常'] = LL(orig_df=df_dict['小时数据'], exceed_amount=params['LLYC']['LLZ']).get_abnormal_data()
        log.info(f'查询ID：{intro_id}，流量异常场景数据量：{len(new_df_dict["流量异常"])}')

    # 排放异常
    if params['PFLYC']['ZDYPFL']['PFL'] != '':
        new_df_dict['排放异常'] = PFL(orig_df=df_dict['小时数据'],
                                  params_dict=params['PFLYC']).get_one_abnormal_data()
        log.info(f'查询ID：{intro_id}，排放异常场景数据量：{len(new_df_dict["排放异常"])}')

    if params['PFLYC']['ZDYPFLBD']['PFL'] != '':
        new_df_dict['排放异常'] = PFL(orig_df=df_dict['小时数据'],
                                  params_dict=params['PFLYC']).get_two_abnormal_data()
        log.info(f'查询ID：{intro_id}，排放异常场景数据量：{len(new_df_dict["排放异常"])}')

    # 浓度超标
    if params['NDCB']['CBLX'] != '':
        exceed_dict = {'HOUR': '小时', 'DAY': '日'}
        exceed_params = params['NDCB']

        new_df_dict['超标'] = PollutantsExceed(
            df_dict=df_dict,
            params_dict=exceed_params).get_instant_data()
        log.info(f'查询ID：{intro_id}，超标场景数据量：{len(new_df_dict["超标"])}')

        # 删掉符合以上场景的数据
        for exceed_type in params['NDCB']['CBLX'].split(','):
            df_dict[f'{exceed_dict[exceed_type]}数据'] = FilterData(
                orig_df=df_dict[f'{exceed_dict[exceed_type]}数据'],
                to_filter_data=new_df_dict['超标']).get_filtered_data()

    # 数据倒挂
    if params["JCLX"] == "FS" and params['NDDG']['SFJS'] == "true":
        new_df_dict['数据倒挂'] = Invert(orig_df=df_dict['小时数据']).get_abnormal_data()
        # 删掉符合以上场景的数据
        df_dict['小时数据'] = FilterData(orig_df=df_dict['小时数据'],
                                     to_filter_data=new_df_dict['数据倒挂']).get_filtered_data()

        log.info(f'查询ID：{intro_id}，数据倒挂场景数据量：{len(new_df_dict["数据倒挂"])}')

    to_filter_data_list = []
    # 浓度恒值
    if params['NDHZ']['HZSC'] != '':
        new_df_dict['恒值'] = HourlyConstant(orig_df=df_dict['小时数据'],
                                           params_dict=conf.get_dict('model_constant'),
                                           dt_params_dict=params['NDHZ']).get_abnormal_data()
        log.info(f'查询ID：{intro_id}，恒值场景数据量：{len(new_df_dict["恒值"])}')
        to_filter_data_list.append(new_df_dict['恒值'])

    # 浓度低值
    if params['NDDZ']:
        new_df_dict['低值'] = Low(orig_df=df_dict['小时数据'],
                                pollutants_list=params['NDDZ']).get_abnormal_data()
        log.info(f'查询ID：{intro_id}，低值场景数据量：{len(new_df_dict["低值"])}')
        to_filter_data_list.append(new_df_dict['低值'])

    # 浓度突变
    if len(to_filter_data_list) > 0:
        to_filter_data = pd.concat(to_filter_data_list)
        df_dict['小时数据'] = FilterData(orig_df=df_dict['小时数据'],
                                     to_filter_data=to_filter_data).get_filtered_data()

    if params['NDFZTB']['TBLX'] != '':  # 浓度分钟突变
        new_df_dict['分钟浓度突变'] = Sudden(df_dict=df_dict['分钟数据'],
                                        params_dict=params['NDFZTB'],
                                        time_type='MINUTE').get_instant_data()
        log.info(f'查询ID：{intro_id}，分钟浓度突变场景数据量：{len(new_df_dict["分钟浓度突变"])}')

    if params['NDXSTB']['TBLX'] != '':  # 浓度小时突变
        new_df_dict['小时浓度突变'] = Sudden(df_dict=df_dict['小时数据'],
                                        params_dict=params['NDXSTB'],
                                        time_type='HOUR').get_instant_data()
        log.info(f'查询ID：{intro_id}，小时浓度突变场景数据量：{len(new_df_dict["小时浓度突变"])}')

    if params['NDRJZTB']['TBLX'] != '':  # 浓度日均值突变
        new_df_dict['日浓度突变'] = Sudden(df_dict=df_dict['日数据'],
                                       params_dict=params['NDRJZTB'],
                                       time_type='DAY').get_instant_data()
        log.info(f'查询ID：{intro_id}，日浓度突变场景数据量：{len(new_df_dict["日浓度突变"])}')
    
    # 输出结果
    try:
        model_df = pd.concat(new_df_dict.values())
    except Exception as e:
        log.error(e)
        model_df = pd.DataFrame()
        
    # 结果表, 扩展表
    results_data, details_data = Postprocess(df_dict_orig, model_df, conf.get_dict('database_oracle')).get_details()

    # 生成一个查询ID
    results_data.insert(0, 'CXID', intro_id)

    # 保存数据
    SaveData(data={'结果表': results_data, '扩展表': details_data},
             monitor_type=params['JCLX'],
             local_db=conf.get_dict('database_mysql')).save_instant_results_to_db()

    # 更新数据
    try:
        no_company = results_data['QYID'].nunique()
    except:
        no_company = 0

    AlterTable(db_info=conf.get_dict('database_mysql')).update(table_name=f't_zxyc_{params["JCLX"].lower()}_dtcxtj',
                                                               data={'XH': intro_id, 'SCQYS': no_company})

    t1 = time.time()
    log.info(f'查询ID：{intro_id}，动态查询录入数据{len(results_data)}个，耗时{round(t1 - t0, 2)}秒')

    # 检查查询时间区间，如果小于等于3天，则除了保存到数据库，还要返回结果
    start_date = datetime.datetime.strptime(params['TJSJ']['KSSJ'], '%Y-%m-%d')
    end_date = datetime.datetime.strptime(params['TJSJ']['JSSJ'], '%Y-%m-%d')
    if (end_date - start_date).days <= 3:
        return intro_id
    else:
        return None


def dt_run_job(file_name):
    intro_id = dt_job(params=file_name, conf=conf)

    if intro_id is not None:
        return intro_id



if __name__ == '__main__':
    p = {
    "params": {
        "HYLX": "",
        "JCLX": "FQ",
        "LLYC": {
            "LLZ": 777
        },
        "ND0FZ": {
            "SC": 5,
            "SCLX": "LJ"
        },
        "NDCB": {
            "CBLX": "HOUR",
            "CBSC": 111,
            "CBSCLX": "LJ"
        },
        "NDDG": {
            "SFJS": "false"
        },
        "NDDZ": [
            {
                "DZSC": 444,
                "DZSCLX": "LJ",
                "NDBZZ": 1,
                "WRYZDM": "w01018"
            }
        ],
        "NDHZ": {
            "HZSC": 50,
            "HZSCLX": "LJ"
        },
        "NDRJZTB": {
            "FDZ": "20",
            "TBLX": "TZ"
        },
        "NDXSTB": {
            "FDZ": "200",
            "TBLX": "TZ"
        },
        "NDFZTB": {
            "FDZ": "",
            "TBLX": ""
        },
        "PFLYC": {
            "ZDYPFL": {
                "PFL": 888,
                "SJ": "00:00,23:59"
            },
            "ZDYPFLBD": {
                "BDSJ": "",
                "PFL": "",
                "SJ": ""
            }
        },
        "TJSJ": {
            "JSSJ": "2021-08-03",
            "KSSJ": "2021-07-23",
            "TJLX": "DAY"
        },
        "WRYZ": "ALL",
        "XZQY": "ALL",
        "pageNum": 1,
        "pageSize": 10,
        "QZTJ":{"HYL":10}
    }
}

    dt_job(params=p['params'])
