from airflow import DAG

from airflow.operators.bash import BashOperator
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

    etl = BashOperator(
        task_id="ETL_Pandas",
        bash_command="python /opt/airflow/scripts/modules.py",
    )

    etl
