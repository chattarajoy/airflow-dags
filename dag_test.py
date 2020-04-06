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
from builtins import range
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from airflow.contrib.operators.qubole_operator import QuboleOperator
import time
from pprint import pprint
from airflow.sensors.http_sensor import HttpSensor

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='dag_test', default_args=args,
    schedule_interval=None)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

python_task = PythonOperator(
    task_id='python_task',
    provide_context=True,
    python_callable=print_context,
    dag=dag)

qubole_task = QuboleOperator(
    task_id='qubole_task',
    command_type='hivecmd',
    query='show tables',
    cluster_label='default',
    fetch_logs=True, # If true, will fetch qubole command logs and concatenate them into corresponding airflow task logs
    tags='aiflow_example_run',  # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    pool='test',
    dag=dag)

http_sensor_task = HttpSensor(
    task_id='http_sensor_task',
    http_conn_id='http_default',
    endpoint='',
    request_params={},
    response_check=lambda response: True if "Google" in response.content else False,
    poke_interval=5,
    dag=dag)

qubole_task.set_upstream(python_task)
bash_task.set_upstream(python_task)
http_sensor_task.set_upstream(python_task)