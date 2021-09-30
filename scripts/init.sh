#! /bin/bash

# Setup DB Connection String
# AIRFLOW__CORE__SQL_ALCHEMY_CONN="mysql://${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}"
# export AIRFLOW__CORE__SQL_ALCHEMY_CONN

# AIRFLOW__WEBSERVER__SECRET_KEY="openssl rand -hex 30"
# export AIRFLOW__WEBSERVER__SECRET_KEY

# DBT_MYSQL_CONN="mysql://${DBT_MYSQL_USER}@${DBT_MYSQL_HOST}:${MYSQL_PORT}/${DBT_MYSQL_DB}"

cd /airflow/dags && python add_data.py

cd /dbt && dbt compile && dbt docs generate && dbt docs serve --port 8081
rm -f /airflow/airflow-webserver.pid

sleep 10
airflow scheduler & airflow webserver -p 8082