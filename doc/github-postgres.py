from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="github-postgres-historical-2",
    description="This DAG is a sample of integration between Spark and DB. It reads CSV files, load them into a Postgres DB and then read them from the same Postgres DB.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

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


def generate_add_new_sql(src_table_name: str, object_name: str):
    return f"""
            INSERT INTO {object_name}s ({object_name})
            SELECT distinct {object_name} FROM {src_table_name}
            where {object_name} not in (select distinct {object_name} from {object_name}s )
    """


def generate_add_new_sql_with_id(src_table_name: str, object_name: str):
    return f"""
            INSERT INTO {object_name}s ({object_name}_id,{object_name})
            SELECT distinct {object_name}_id,{object_name} FROM {src_table_name}
            where {object_name} not in (select distinct {object_name} from {object_name}s )
            
    """


def generate_create_sql(object_name: str):
    return f"""
       
        CREATE TABLE IF NOT EXISTS {object_name}s (
            {object_name}_id SERIAL PRIMARY KEY,
            {object_name} VARCHAR NOT NULL
        );
    
    """


def generate_create_sql_with_id(object_name: str):
    return f"""
       
        CREATE TABLE IF NOT EXISTS {object_name}s (
            {object_name}_id bigint PRIMARY KEY,
            {object_name} VARCHAR NOT NULL
        );
    
    """


def generate_create_sql_with_id(object_name: str):
    return f"""
       
        CREATE TABLE IF NOT EXISTS {object_name}s (
            {object_name}_id bigint PRIMARY KEY,
            {object_name} VARCHAR NOT NULL
        );
    
    """


def create_object_table_operator(object_name: str):
    return PostgresOperator(dag=dag, task_id=f"create_{object_name}_table",
                            postgres_conn_id="postgres_default",
                            sql=generate_create_sql(object_name))


def create_object_table_operator_with_id(object_name: str):
    return PostgresOperator(dag=dag, task_id=f"create_{object_name}_table",
                            postgres_conn_id="postgres_default",
                            sql=generate_create_sql_with_id(object_name))


def generate_insert_new_objects_operator(object_name: str):
    return PostgresOperator(dag=dag, task_id=f"populate_{object_name}_table",
                            postgres_conn_id="postgres_default",
                            sql=generate_add_new_sql('events', object_name))


def generate_insert_new_objects_operator_with_id(object_name: str):
    return PostgresOperator(dag=dag, task_id=f"populate_{object_name}_table",
                            postgres_conn_id="postgres_default",
                            sql=generate_add_new_sql_with_id('events', object_name))

# create hub_actor (id)
# create sat_actor_name (actor_id, name, from_time)
# detect dupes in new - > changes

# insert into hub (id) new
# insert into sat _new


create_types_table = create_object_table_operator('type')
create_actors_table = create_object_table_operator_with_id('actor')
create_repos_table = create_object_table_operator_with_id('repo')


populate_new_types = generate_insert_new_objects_operator('type')
populate_new_actors = generate_insert_new_objects_operator_with_id('actor')
populate_new_repos = generate_insert_new_objects_operator_with_id('repo')
# spark_job_read_postgres = SparkSubmitOperator(
#     task_id="spark_job_read_postgres",
#     # Spark application path created in airflow and spark cluster
#     application="/usr/local/spark/app/read-postgres.py",
#     name="read-postgres",
#     conn_id="spark_default",
#     verbose=1,
#     conf={"spark.master": spark_master},
#     application_args=[postgres_db, postgres_user, postgres_pwd],
#     jars=postgres_driver_jar,
#     driver_class_path=postgres_driver_jar,
#     dag=dag)

mid = DummyOperator(task_id="mid", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
start >> spark_job_load_postgres >> [
    create_types_table, create_actors_table, create_repos_table] >> mid >> [populate_new_types, populate_new_actors, populate_new_repos] >> end
# spark_job_load_postgres >>
