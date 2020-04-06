"""
Airflow Demo leveraging QuboleOperator
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.qubole_operator import QuboleOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sdabade',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['sdabade@qubole.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('ecommerce-airflow-demo', default_args=default_args, schedule_interval='@daily')

dag.doc_md = __doc__

# Task = start
start = DummyOperator(
    task_id='start',
    dag=dag
)

# Task = cleanup (cleanup schema)
# start ---> cleanup (cleanup schema)
cleanup = QuboleOperator(
    task_id='hive_schema_cleanup',
    command_type='hivecmd',
    script_location="s3n://uwddefbucket/scripts/ecommerce_schema_cleanup.hql",
    cluster_label='hadoop2',
    tags='airflow_example_run',  # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag)
start.set_downstream(cleanup)

# Task = t1 (create schema)
# cleanup ---> t1 (create schemas)
t1 = QuboleOperator(
    task_id='hive_create_schema',
    command_type='hivecmd',
    script_location="s3n://uwddefbucket/scripts/ecommerce_create_schema.hql",
    cluster_label='hadoop2',
    tags='airflow_example_run',  # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag)
cleanup.set_downstream(t1)

# Task = join1
join1 = DummyOperator(
    task_id='join1',
    trigger_rule="all_success",
    dag=dag
)

# Task = t2 (dbimport categories)
# t1 ---> t2 (dbimport categories) ---> join1
t2 = QuboleOperator(
    task_id='db_import_categories',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.categories',
    db_table='categories',
    db_parallelism=2,
    dbtap_id="508",
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
    qubole_conn_id='qubole_default',
    dag=dag)
t2.set_upstream(t1)  
t2.set_downstream(join1)

# Task = t3 (dbimport customers)
# start ---> t3 (dbimport customers) ---> join1
t3 = QuboleOperator(
    task_id='db_import_customers',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.customers',
    db_table='customers',
    db_parallelism=2,
    dbtap_id="508",
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
    qubole_conn_id='qubole_default',
    dag=dag)
t3.set_upstream(t1)  
t3.set_downstream(join1)

# Task = t4 (dbimport departments)
# start ---> t4 (dbimport departments) ---> join1
t4 = QuboleOperator(
    task_id='db_import_departments',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.departments',
    db_table='departments',
    db_parallelism=2,
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',    
	dbtap_id="508",
    qubole_conn_id='qubole_default',
    dag=dag)
t4.set_upstream(t1)  
t4.set_downstream(join1)

# Task = t5 (dbimport order_items)
# start ---> t5 (dbimport order_items) ---> join1
t5 = QuboleOperator(
    task_id='db_import_order_items',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.order_items',
    db_table='order_items',
    db_parallelism=2,
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
    dbtap_id="508",
    qubole_conn_id='qubole_default',
    dag=dag)
t5.set_upstream(t1)  
t5.set_downstream(join1)

# Task = t6 (dbimport orders)
# start ---> t6 (dbimport orders) ---> join1
t6 = QuboleOperator(
    task_id='db_import_orders',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.orders',
    db_table='orders',
    db_parallelism=2,
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
    dbtap_id="508",
    qubole_conn_id='qubole_default',
    dag=dag)
t6.set_upstream(t1)  
t6.set_downstream(join1)

# Task = t7 (dbimport orders_temp)
# start ---> t7 (dbimport orders_temp) ---> join1
t7 = QuboleOperator(
    task_id='db_import_orders_temp',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.orders_temp',
    db_table='orders_temp',
    db_parallelism=2,
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
    dbtap_id="508",
    qubole_conn_id='qubole_default',
    dag=dag)
t7.set_upstream(t1)  
t7.set_downstream(join1)

# Task = t8 (dbimport products)
# start ---> t8 (dbimport products) ---> join1
t8 = QuboleOperator(
    task_id='db_import_products',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.products',
    db_table='products',
    db_parallelism=2,
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
    dbtap_id="508",
    qubole_conn_id='qubole_default',
    dag=dag)
t8.set_upstream(t1)  
t8.set_downstream(join1)

# Task = t9 (dbimport salesperson)
# start ---> t9 (dbimport salesperson) ---> join1
t9 = QuboleOperator(
    task_id='db_import_salesperson',
    command_type='dbimportcmd',
    mode=1,
    hive_table='ecommerce_db.salesperson',
    db_table='salesperson',
    db_parallelism=2,
	customer_cluster_label='hadoop2',
	use_customer_cluster='true',
	dbtap_id="508",
    qubole_conn_id='qubole_default',
    dag=dag)
t9.set_upstream(t1) 
t9.set_downstream(join1)

# Task = t10 (dbimport create hive external table)
# start ---> t10 (create hive external table) ---> join1
t10 = QuboleOperator(
    task_id='hive_create_ext_table',
    command_type='hivecmd',
    script_location="s3n://uwddefbucket/scripts/ecommerce_create_hive_external_table.hql",
    cluster_label='hadoop2',
    tags='airflow_example_run',  # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag)
t10.set_upstream(t1)  

# Task = join2
join2 = DummyOperator(
    task_id='join2',
    trigger_rule="all_success",
    dag=dag
)

# Task = t12 (hivecmd - most frequently bought products)
# join1 ---> t12 (hivecmd - most frequently bought products) ---> join2
t12 = QuboleOperator(
    task_id='hive_most_frequently_bought',
    command_type='hivecmd',
    script_location="s3n://uwddefbucket/scripts/ecommerce_top_10_products_by_quantity.hql",
    cluster_label='hadoop2',
    tags='airflow_example_run',  # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag)
t12.set_upstream(join1)
t12.set_downstream(join2)

# Task = t11 (hivecmd - most frequently viewed products)
# t10 ---> t11 (hivecmd - most frequently viewed products) ---> join2
t11 = QuboleOperator(
    task_id='hive_most_frequently_viewed',
    command_type='hivecmd',
    script_location="s3n://uwddefbucket/scripts/ecommerce_top_10_products_by_views.hql",
    cluster_label='hadoop2',
    tags='airflow_example_run',  # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',  # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag)
t11.set_upstream(t10)
t11.set_downstream(join2)

# Task = end
# join2 ---> end
end = DummyOperator(
    task_id='end',
    dag=dag
)

join2.set_downstream(end)
