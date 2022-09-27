from airflow import DAG
from datetime import datetime, timedelta
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


###############################################
# Parameters
###############################################

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
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="rates-aggregate",
    description="Downloads latest part from exchangerate.host into ClickHouse.",
    default_args=default_args,
    schedule_interval=timedelta(hours=3)
)


aggregate_data_clickhouse_job = ClickHouseOperator(
    task_id='aggregate_data_clickhouse_job',
    database='default',
    sql=('''alter table agg_rates delete  where date in (select max(date) from agg_rates)
            ''', '''
            insert into agg_rates 
            select base, symbol, avg(rate) as avg_rate, min(rate) as min_rate, max(rate) as max_rate,date,NOW()  from 

            (select base, code as symbol, cast(REPLACE (rate,',','.') as float) as rate, cast(date as Date) as date from raw_CSV_rates
            where cast(date as Date) not in (select distinct date from agg_rates) ) as converted
            group by base, symbol, date;
            '''
         ),
    clickhouse_conn_id='clickhouse_test',
    dag=dag
)

aggregate_data_clickhouse_job
