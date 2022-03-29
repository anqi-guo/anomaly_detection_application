#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  Copyright (C) 2020 ShenZhen Powersmart Information Technology Co.,Ltd
#  All Rights Reserved.
#  本软件为深圳博沃智慧开发研制。未经本公司正式书面同意，其他任何个人、团体不得使用、
#  复制、修改或发布本软件.

# @Time : 2020/11/25 15:07
# @Author : wanpeng
# @Software: PyCharm
from sys import argv
from job import run_post
from job import ls_run
from job import temp_run


if __name__ == '__main__':
    is_run_job = False
    is_run_update = False
    if len(argv) > 1:
        is_dtrun = ('dt_runjob' == argv[1])
        is_lsrun = ('ls_runjob' == argv[1])
        is_temprun = ('temp_runjob' == argv[1])

    if is_dtrun:
        run_post.run_post()

    if is_lsrun:
        for monitor_type in ['废气', '废水']:
            ls_run.ls_job(monitor_type, append_yesterday=True)

        #ls_run.ls_job('废水', append_yesterday=False)

    if is_temprun:
        temp_run.temp()
        


