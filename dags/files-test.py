from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import requests
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Files - Test 1"

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
    dag_id="files-test-1",
    description="This DAG downloads files",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)
paths = []


spark_job = SparkSubmitOperator(
    task_id="spark_job",
    # Spark application path created in airflow and spark cluster
    application="/usr/local/spark/app/files-test.py",
    name="files-test",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": spark_master,
          "spark.driver.extraJavaOptions": "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'",
          "spark.executor.extraJavaOptions": "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'"
          },
    application_args=[],
    # files=','.join(['/usr/local/airflow/airflow/output/' + \
    #                x for x in os.listdir('/usr/local/airflow/airflow/output')]),
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# start >> download_job >> spark_job >> end
start >> spark_job >> end
