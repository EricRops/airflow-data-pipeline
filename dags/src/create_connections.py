import datetime
import logging

# from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.models import Connection
from airflow import settings

# dag_config = Variable.get("variables_config", deserialize_json=True)
# var1 = dag_config["var1"]
# var2 = dag_config["var2"]
# var3 = dag_config["var3"]
#
# def redshift_conn(username, password, host=None):
#     new_conn = Connection(conn_id=f'{username}_connection',
#                                   login=username,
#                                   host=host if host else None)
#     new_conn.set_password(password)
#
#     session = settings.Session()
#     session.add(new_conn)
#     session.commit()

