
# Airflow Spark ClickHouse PostgreSQL

This is an updated fork of https://github.com/cordon-thiago/airflow-spark

* CHANGES by September 26th, 2022: 
  * PostgreSQL version updated: 9.6 -> 14 (Latest)
  * Airflow version updated: 1.10.7 -> 2.4.0 (Latest)
  * Spark version updated: 3.1.2 -> 3.3.0 (Latest)
  * ClickHouse added: 21.3.20.1 (Latest)
  * Python version updated across all platforms: 3.6 -> 3.8 (Latest 3.9 is not compatible with Spark 3.3.0)

  * Fixed issues:
    * Airflow: New configuration file (airflow.cfg) generated and applied
    * Airflow: FERNET_KEY and SECRET_KEY 
    * Airflow: Database init and default airflow:airflow user creation
    * Spark's server/works/jupyter now have the same Python 3.8 version for compatibility
    * Performance: Reduced Spark workers from 1 to 3 (uncomment in docker-compose for default usage)
  * requirements.txt compatibility: 
    * Raised all dependency versions 
    * Added:
        * apache-airflow-providers-apache-spark==3.0.0
        * clickhouse-driver==0.2.4
        * airflow-clickhouse-plugin==0.8.2
        * gender_guesser==0.4.0
        * requests==2.28.1
        * apache-airflow-providers-postgres==5.2.1

IMPORTANT: Airflow configuration steps needed:
  * Login to http://localhost:8282 as airflow/airflow and navigate to (Airflow->Admin->Connections):
  * Edit 'postgres_default' connection settings to match postgres://test:postgres@postgres:5432/test
  * Edit 'spark_default' connection settings to match spark://spark:7077

This project contains the following examples:

  * GitHub Archive (https://www.gharchive.org/) daily data analysis (Python/Spark/PostgreSQL) - downloads daily data archive and collects metrics over it
    
    * Attempted to make code-generated Airflow jobs for a 'highly Normalized hybrid Model' based on the historical data
      * './dags/modules/github_model.py' to automatically generate Airflow PythonOerators and SQL code
        ![](./doc/airflow_hNhM_structure.png "hNhM Structure")

      * Airflow 'github-postgres-create-model' job - creates table structure for hNhM in PostgreSQL
         ![](./doc/airflow_hNhM_create.png "hNhM Creation")
      * Airflow 'github-postgres-download-insert-latest' job - downloads latest (in GMT+3 TZ) archive and populates hNhM incrementally (SCD2)
         ![](./doc/airflow_hNhM_insert.png "hNhM Insertion")

    * Run 'github-daily-actions' Airflow job to collect the following metrics for a single 'tar.gz' file:
      * Outputs to './spark/resources/data' in a distributed CSV
      * List of Developers that own more than one repository
      * List of Developers who did more than one commit in a day, ordered by name and number of commits
      * List of Developers with less than one commit in a day
      * Total Developers grouped by gender
      * Total projects with more than 10 members

    * Metrics use the following:
      * Airflow job 'github-daily-actions' (dags\github-daily-actions.py) - runs Spark job
      * Spark job 'github-daily-test-spark-module' (spark\app\github-daily-test-spark-module.py)
      * Jupyter Notebook for testing purposes (notebooks\github_archive.ipynb)


* MT4/MT5 trade analytics (Python/SQL) - local database source is necessary
  * Edit mt\config.ini for your postgres path instead of test
  * Run mt\main.py (edit contents in case necessary)
  * Collect results in CSV format mt\results
  * Study mt\sql\*.sql scripts for detailed information

* ETL-pipelines for BTC/USD rates (http://exchangerates.host) historical and current data collection and aggregation (Python/Aiflow/ClickHouse)
  * Run init database Python script 'rate\rates.py'
  * Same can be done by scripts located in rates\sql\rates. Historical, latest and aggregated data will be downloaded into these tables in ClickHouse using Python later
    * Connect to ClichHouse with any SQL tool (e.g. DBeaver)
      * connection string: 'jdbc:clickhouse://localhost:8123'
      * user 'default' with no password
    * CREATE_TABLE_RAW_CSV_RATES.sql
    * CREATE_TABLE_AGG_RATES.sql
  * Open Airflow UI and run DAGs (located in 'dags' folder):
    * rates-download-historical (scheduled once)
    * rates-download-latest (scheduled every 3 hours)
    * rates-aggregate (scheduled every 3 hours)
  * Query ClickHouse tables with SQL tool for data populated

This project contains the following containers:

* postgres: Postgres database for Airflow metadata and a Test database to test whatever you want.
    * Image: postgres:14
    * Database Port: 5432
    * References: https://hub.docker.com/_/postgres

* airflow-webserver: Airflow webserver and Scheduler.
    * Image: docker-airflow-spark:2.4.0_3.3.0
    * Port: 8282

* spark: Spark Master.
    * Image: bitnami/spark:3.3.0
    * Port: 8181
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.
    * Image: bitnami/spark:3.3.0
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

* jupyter-spark: Jupyter notebook with pyspark for interactive development.
  * Image: jupyter/pyspark-notebook:spark-3.3.0
  * Port: 8888
  * References: 
    * https://hub.docker.com/layers/jupyter/pyspark-notebook/spark-3.1.2/images/sha256-37398efc9e51f868e0e1fde8e93df67bae0f9c77d3d3ce7fe3830faeb47afe4d?context=explore
    * https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook
    * https://hub.docker.com/r/jupyter/pyspark-notebook/tags/

* clickhouse-server: ClickHouse server to be used as a database instead of PostgreSQL in case necessary
  * Image: yandex/clickhouse-server:21.3.20.1
  * Ports: 8123,9000,9009
  * References: 
    * https://hub.docker.com/r/yandex/clickhouse-server/


## Architecture components

![](./doc/architecture.png "Architecture")

## Setup

### Clone project

    $ git clone https://github.com/konstantin_kiselev/airflow-spark

### Build airflow Docker

Inside the airflow-spark/docker/docker-airflow

    $ docker build --rm --force-rm -t docker-airflow-spark:2.4.0_3.3.0 .

Optionally, you can override the arguments in the build to choose specific Spark, Hadoop and Airflow versions. As an example, here is how to build an image containing Airflow version `2.4.0`, Spark version `3.3.0` and Hadoop version `3.2`.

    $ docker build --rm --force-rm \
    -t docker-airflow-spark:2.4.0_3.3.0 . \
    --build-arg AIRFLOW_VERSION=2.4.0 \
    --build-arg SPARK_VERSION=3.3.0 \
    --build-arg HADOOP_VERSION=3.2

Spark and hadoop versions follow the versions as defined at Spark download page: https://spark.apache.org/downloads.html

Airflow versions can be found here: https://pypi.org/project/apache-airflow/#history

If you change the name or the tag of the docker image when building, remember to update the name/tag in docker-compose file.
### Start containers

Navigate to airflow-spark/docker and:

    $ docker-compose up

If you want to run in background:

    $ docker-compose up -d

Note: when running the docker-compose for the first time, the images `postgres:14`, `bitnami/spark:3.3.0`, `jupyter/pyspark-notebook:spark-3.3.30` and `yandex/clickhouse-server:21.3.20.1` will be downloaded before the containers started.

### Check if you can access

Airflow: http://localhost:8282

Spark Master: http://localhost:8181

PostgreSql - Database Test:

* Server: localhost:5432
* Database: test
* User: test
* Password: postgres

Postgres - Database airflow:

* Server: localhost:5432
* Database: airflow
* User: airflow
* Password: airflow

Jupyter Notebook: http://127.0.0.1:8888
  * For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:
  
        $ docker logs -f docker_jupyter-spark_1

ClickHouse Server: http://localhost:8123
  * Database creation scripts are available in sql\mt


## How to run a DAG to test

1. Configure spark connection acessing airflow web UI http://localhost:8282 and going to Connections
   ![](./doc/airflow_connections_menu.png "Airflow Connections")

2. Edit the spark_default connection inserting `spark://spark` in Host field and Port `7077`
    ![](./doc/airflow_spark_connection.png "Airflow Spark connection")

3. Run the spark-test DAG
   
4. Check the DAG log for the task spark_job. You will see the result printed in the log
   ![](./doc/airflow_dag_log.png "Airflow log")

5. Check the spark application in the Spark Master web UI (http://localhost:8181)
   ![](./doc/spark_master_app.png "Spark Master UI")

## How to run the Spark Apps via spark-submit
After started your docker containers, run the command below in your terminal:
```
$ docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 <spark_app_path> [optional]<list_of_app_args>
```

Example running the hellop-world.py application:
```
$ docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 /usr/local/spark/app/hello-world.py /usr/local/spark/resources/data/airflow.cfg
```

## Increasing the number of Spark Workers

You can increase the number of Spark workers just adding new services based on `bitnami/spark:3.3.0` image to the `docker-compose.yml` file like following:

```
spark-worker-n:
        image: bitnami/spark:3.3.0
        user: root
        networks:
            - default_net
        environment:
            - SPARK_MODE=worker
            - SPARK_MASTER_URL=spark://spark:7077
            - SPARK_WORKER_MEMORY=1G
            - SPARK_WORKER_CORES=1
            - SPARK_RPC_AUTHENTICATION_ENABLED=no
            - SPARK_RPC_ENCRYPTION_ENABLED=no
            - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
            - SPARK_SSL_ENABLED=no
        volumes:
            - ../spark/app:/usr/local/spark/app # Spark scripts folder (Must be the same path in airflow and Spark Cluster)
            - ../spark/resources/data:/usr/local/spark/resources/data #Data folder (Must be the same path in airflow and Spark Cluster)

```

## Adding Airflow Extra packages

Rebuild Dockerfile (in this example, adding GCP extra):

    $ docker build --rm --build-arg AIRFLOW_DEPS="gcp" -t docker-airflow-spark:2.4.0_3.3.0 .

After successfully built, run docker-compose to start container:

    $ docker-compose up

More info at: https://github.com/puckel/docker-airflow#build

## Useful docker commands

    List Images:
    $ docker images <repository_name>

    List Containers:
    $ docker container ls

    Check container logs:
    $ docker logs -f <container_name>

    To build a Dockerfile after changing sth (run inside directoty containing Dockerfile):
    $ docker build --rm -t <tag_name> .

    Access container bash:
    $ docker exec -i -t <container_name> /bin/bash

## Useful docker-compose commands

    Start Containers:
    $ docker-compose -f <compose-file.yml> up -d

    Stop Containers:
    $ docker-compose -f <compose-file.yml> down --remove-orphans
    
# Extras
## Spark + Postgres sample

* The DAG [spark-postgres.py](dags/spark-postgres.py) loads [movies.csv](spark/resources/data/movies.csv) and [ratings.csv](spark/resources/data/ratings.csv) data into Postgres tables and query these tables to generate a list of top 10 movies with more rates.
  * This DAG runs the load-postgres.py and read-postgres.py applications. These applications are also available in the notebooks [load-postgres-notebook.ipynb](notebooks/load-postgres-notebook.ipynb) and [read-postgres-notebook.ipynb](notebooks/read-postgres-notebook.ipynb).
