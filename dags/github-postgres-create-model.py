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
    dag_id="github-postgres-create-model",
    description="This DAG recreates hNhM for GHArchive in PostgreSQL (drops old tables).",
    default_args=default_args,
    schedule_interval=timedelta(1)
)
start = DummyOperator(task_id="start", dag=dag)
delete_end = DummyOperator(task_id="delete_end", dag=dag)
create_end = DummyOperator(task_id="create_end", dag=dag)


source = 'events'

gh_model = GHModel(dag, source, mode='create')
create = gh_model.get_create_operators()
delete = gh_model.get_delete_operators()

start >> delete >> delete_end >> create >> create_end
