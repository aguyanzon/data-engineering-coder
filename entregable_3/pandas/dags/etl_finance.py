# Este es el DAG que orquesta el ETL de la tabla users
import sys
sys.path.append('/opt/airflow/scripts/')
import modules

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta

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

    task_1 = PythonOperator(
        task_id='extraer_data',
        python_callable=modules.union_data,
        dag=dag,
    )

    task_2 = PythonOperator(
        task_id='transformar_data',
        python_callable=modules.transform,
        op_args=[task_1.output],
        dag=dag,
    )

    task_3 = PythonOperator(
        task_id='cargar_data',
        python_callable=modules.load,
        op_args=[task_2.output],
        dag=dag,
    )

    task_1 >> task_2 >> task_3
