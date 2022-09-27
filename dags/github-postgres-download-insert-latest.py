from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from modules.github_model import GHModel
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-42.5.0.jar"

postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

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
    dag_id="github-postgres-download-insert-latest",
    description="This DAG downloads current GHArchive for the latest hour, uploads to PostgreSQL and performs incremental update with SCD2 of hNhM",
    default_args=default_args,
    schedule_interval=timedelta(1)
)


spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    # Spark application path created in airflow and spark cluster
    application="/usr/local/spark/app/github-load-postgres.py",
    name="github-load-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": spark_master,
          "spark.driver.extraJavaOptions": "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'",
          "spark.executor.extraJavaOptions": "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'"
          },
    application_args=[
        postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)


start = DummyOperator(task_id="start", dag=dag)
insert_end = DummyOperator(task_id="insert_end", dag=dag)


source = 'events'
gh_model = GHModel(dag, source, mode='insert')
insert = gh_model.get_insert_operators()

start >> spark_job_load_postgres >> insert >> insert_end
