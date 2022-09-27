docker compose down --remove-orphans
cd docker-airflow
docker build --rm --force-rm --no-cache -t docker-airflow-spark:2.4.0_3.3.0 .
# docker build  -t docker-airflow-spark:2.4.0_3.3.0 .
cd ..
docker-compose up -d
