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

import datetime
import os
import unittest
import mock
import nose
import six

from airflow import DAG, configuration, operators
configuration.load_test_config()


DEFAULT_DATE = datetime.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


if 'AIRFLOW_RUNALL_TESTS' in os.environ:

    import airflow.hooks.kylin_hooks
    import airflow.operators.presto_to_mysql

    class KylinTest(unittest.TestCase):
        def setUp(self):
            configuration.load_test_config()
            self.nondefault_schema = "nondefault"

        def test_select_conn(self):
            from airflow.hooks.kylin_hooks import KylinHook
            sql = "select 1"
            hook = KylinHook()
            hook.get_records(sql)

        def test_multi_statements(self):
            from airflow.hooks.kylin_hooks import KylinHook
            sqls = [
                "CREATE TABLE IF NOT EXISTS test_multi_statements (i INT)",
                "DROP TABLE test_multi_statements",
            ]
            hook = KylinHook()
            hook.get_records(sqls)

        def test_get_metastore_databases(self):
            if six.PY2:
                from airflow.hooks.kylin_hooks import HiveMetastoreHook
                hook = HiveMetastoreHook()
                hook.get_databases()

        def test_to_csv(self):
            from airflow.hooks.kylin_hooks import KylinHook
            sql = "select 1"
            hook = KylinHook()
            hook.to_csv(hql=sql, csv_filepath="/tmp/test_to_csv")

        def connect_mock(self, host, port,
                         auth_mechanism, kerberos_service_name,
                         user, database):
            self.assertEqual(database, self.nondefault_schema)

        @mock.patch('KylinHook.connect', return_value="foo")
        def test_select_conn_with_schema(self, connect_mock):
            from airflow.hooks.kylin_hooks import KylinHook

            # Configure
            hook = KylinHook()

            # Run
            hook.get_conn(self.nondefault_schema)

            # Verify
            self.assertTrue(connect_mock.called)
            (args, kwargs) = connect_mock.call_args_list[0]
            self.assertEqual(self.nondefault_schema, kwargs['database'])

        def test_get_results_with_schema(self):
            from airflow.hooks.kylin_hooks import KylinHook
            from unittest.mock import MagicMock

            # Configure
            sql = "select 1"
            schema = "notdefault"
            hook = KylinHook()
            cursor_mock = MagicMock(
                __enter__=cursor_mock,
                __exit__=None,
                execute=None,
                fetchall=[],
            )
            get_conn_mock = MagicMock(
                __enter__=get_conn_mock,
                __exit__=None,
                cursor=cursor_mock,
            )
            hook.get_conn = get_conn_mock

            # Run
            hook.get_results(sql, schema)

            # Verify
            get_conn_mock.assert_called_with(self.nondefault_schema)

        @mock.patch('KylinHook.get_results', return_value={'data': []})
        def test_get_records_with_schema(self, get_results_mock):
            from airflow.hooks.kylin_hooks import KylinHook

            # Configure
            sql = "select 1"
            hook = KylinHook()

            # Run
            hook.get_records(sql, self.nondefault_schema)

            # Verify
            self.assertTrue(self.connect_mock.called)
            (args, kwargs) = self.connect_mock.call_args_list[0]
            self.assertEqual(sql, args[0])
            self.assertEqual(self.nondefault_schema, kwargs['schema'])

        @mock.patch('KylinHook.get_results', return_value={'data': []})
        def test_get_pandas_df_with_schema(self, get_results_mock):
            from airflow.hooks.kylin_hooks import KylinHook

            # Configure
            sql = "select 1"
            hook = KylinHook()

            # Run
            hook.get_pandas_df(sql, self.nondefault_schema)

            # Verify
            self.assertTrue(self.connect_mock.called)
            (args, kwargs) = self.connect_mock.call_args_list[0]
            self.assertEqual(sql, args[0])
            self.assertEqual(self.nondefault_schema, kwargs['schema'])
