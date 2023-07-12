# Este es el DAG que orquesta el ETL de la tabla users

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta
from os import environ as env

# Variables de configuración de Redshift
REDSHIFT_SCHEMA = env['REDSHIFT_SCHEMA']

QUERY_CREATE_TABLE = '''
CREATE TABLE IF NOT EXISTS finance (
    "date_from" VARCHAR(10),
    "1. open" VARCHAR(10),
    "2. high" VARCHAR(10),
    "3. low" VARCHAR(10),
    "4. close" VARCHAR(10), 
    "5. volume" VARCHAR(10),
    symbol VARCHAR(10) distkey
) sortkey(date_from);
'''


defaul_args = {
    "owner": "Agustin Yanzón",
    "start_date": datetime(2023, 7, 11),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_finance",
    default_args=defaul_args,
    description="ETL de la tabla finance",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    spark_etl_finance = SparkSubmitOperator(
        task_id="spark_etl_finance",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Finance.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    create_table >> spark_etl_finance
