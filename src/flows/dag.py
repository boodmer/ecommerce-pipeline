from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from dotenv import load_dotenv

with DAG(
    "ecommerce-pineline",
    default_args={
        'depends_on_past': False,
        'email': ['minh.tonn@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2024, 6, 1),
    schedule_interval='@daily',
    description="ingest and transform",
    catchup=False
) as dag:
    table_names = ['users', 'orders', 'products']
    path = Variable.get("PYSPARK_APP_HOME")
    env_path = os.path.join(path, '.env')
    load_dotenv(env_path)
    print('path', env_path)
    print('env', dict(os.environ))
    def get_ingestion(table_name: str):
        return SparkSubmitOperator(
        task_id=f'ingestion-{table_name}',
        conn_id='spark_conn',
        jars=os.path.join(path, 'jars/postgresql-42.7.3.jar'),
        application=os.path.join(path, 'src/jobs/ingest.py'),
        application_args=[f'--table_name={table_name}', '--execution_date={{ ds }}'],
        env_vars=dict(os.environ)
    )

    tranformation = SparkSubmitOperator(
        task_id='tranformation',
        conn_id='spark_conn',
        application=os.path.join(path, 'src/jobs/transform.py'),
        application_args=['--execution_date={{ ds }}'],
        env_vars=dict(os.environ)
    )

    [get_ingestion(table_name) for table_name in table_names] >> tranformation
