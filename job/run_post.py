#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  Copyright (C) 2021 ShenZhen Powersmart Information Technology Co.,Ltd
#  All Rights Reserved.
#  本软件为深圳博沃智慧开发研制。未经本公司正式书面同意，其他任何个人、团体不得使用、
#  复制、修改或发布本软件.

# @Time : 2022/1/19 9:38
# @Author : liangbiwen
# @Software: PyCharm
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#  Copyright (C) 2021 ShenZhen Powersmart Information Technology Co.,Ltd
#  All Rights Reserved.
#  本软件为深圳博沃智慧开发研制。未经本公司正式书面同意，其他任何个人、团体不得使用、
#  复制、修改或发布本软件.

# @Time : 2022/1/19 9:12
# @Author : liangbiwen
# @Software: PyCharm
import shutil

import werkzeug
from flask import Flask
import tarfile
from flask_restful import Resource,Api,reqparse
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
import os

from job.dt_run import dt_run_job
from utils.file_util import get_file_path
from utils.get_logger import log

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument("params", required=True, action='store', type=str, nullable=False, location='json')


class HelloWorld(Resource):
    """
    接口类
    """
    def post(self):
        args = parser.parse_args()
        params = args.get('params')
        params = eval(params)
        log.info(params)

        failure_result = {
            "code": "01",
            "msg": "失败"
        }
        success_result = {
            "code": "00",
            "msg": "成功"
        }
        try:
            intro_id = dt_run_job(params)
            if intro_id is not None:
                success_result['cxid'] = str(intro_id)
        except Exception as e:
            log.exception(e)
            return failure_result
        return success_result


api.add_resource(HelloWorld,'/')


def run_post():
    app.run('0.0.0.0','1000', debug=False, threaded=True)


if __name__ == '__main__':
    app.run('0.0.0.0', '5002', debug=True)
