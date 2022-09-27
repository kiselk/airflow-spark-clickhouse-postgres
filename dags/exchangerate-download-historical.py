from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import requests
import pandas as pd
from io import StringIO
from modules import exchangerate
###############################################
# Parameters
###############################################
base = 'BTC'
symbol = 'USD'
start_date = '2020-05-05'
clickhouse_host = 'host.docker.internal'
###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "Konstantin Kiselev",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["konstantin_kiselev@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="rates-download-historical",
    description="Downloads historical part from exchangerate.host into ClickHouse.",
    default_args=default_args,
    schedule_interval='@once'
)


def download_and_insert_historical(**kwargs):

    data = exchangerate.clickhousefunctions.getExchangeRate(base, symbol, path='timeseries',
                                                            start_date=start_date, end_date=datetime.today().strftime('%Y-%m-%d'))
    client = Client(host=clickhouse_host,
                    settings={'use_numpy': True})
    client.insert_dataframe(""" INSERT INTO raw_CSV_rates VALUES
    """, data)


download_latest_job = PythonOperator(
    task_id="download_latest_job", dag=dag, python_callable=download_and_insert_historical)


download_latest_job
