# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import logging
import json
import time

from pykylin import connection,proxy
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

class AirflowDruidLoadException(AirflowException):
    pass


class KylinHook(BaseHook):
    '''
    Interact with Apache Kylin.
    '''

    def __init__(
            self,
            project,
            kylin_conn_id='kylin_conn_id',
            default_conn_name = 'kylin_default'):
        self.kylin_conn_id = kylin_conn_id
        self.default_conn_name = default_conn_name
        # self.header = {'content-type': 'application/json'}

    def get_conn(self):
        """
        Returns a kylin connection object for query
        """
        conn = self.get_connection(self.kylin_conn_id)
        return connection.connect(conn.login, conn.password,
            "http://{conn.host}:{conn.port}/kylin/api".format(**locals(),
                self.project),
            # conn.extra_dejson.get('endpoint', '')
            )

    def get_cube_details(self, cube_name, limit=50, offset=0):
        '''
        Returns kylin cube details
        '''
        conn = self.get_conn(self.kylin_conn_id)
        route = 'cubes'
        data = {'cubeName':cube_name,'limit':limit, 'offset':offset}
        return conn.proxy.get(route, data=data)

    def build_cube(self, cube_name, end_time, start_time=0, build_type="BUILD"):
        '''
        Build kylin cube
        '''
        conn = self.get_conn(self.kylin_conn_id)
        route = 'cubes/{cube_name}/rebuild'.format(**locals())
        data = {'startTime':start_time, 'endTime':end_time, 'buildType':build_type}
        return conn.proxy.put(route, data=data)

    def get_query_result(self, sql):
        '''
        get query result
        '''
        conn = self.get_conn(self.kylin_conn_id)
        conn.cursor.execute(sql)
        return conn.results

    def get_pandas_df(self, sql, schema='default'):
        pass