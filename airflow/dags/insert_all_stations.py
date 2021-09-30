# -*- coding: utf-8 -*-
"""
Created on Thurs Sep  23 01:54:51 2021

@author: Jabor047
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from add_data import createTables, preprocess_stations, insert_to_all_station_table

default_args = {"owner": "Jabor047", "start_date": datetime(2021, 9, 20)}
with DAG(
    dag_id="insert_all_station_data",
    default_args=default_args,
    schedule_interval="@once",
    tags=["mysql", "connector", "dwh", "station"],
) as dag:

    pre_process = PythonOperator(
        task_id="pre_process",
        python_callable=preprocess_stations,
        op_kwargs={
            "path": "/data/sample_data.csv"
        },
    )

    create_table = PythonOperator(
        task_id="create_tables",
        python_callable=createTables,
        op_kwargs={
            "dwhName": "Sensor_DW",
            "schema_name": "/schema/all_stations_schema.sql",
        },
    )

    # insert = MySqlOperator(
    #     task_id='insert_to_db',
    #     mysql_conn_id="mysql_db1",
    #     sql="LOAD DATA  INFILE '/home/Abuton/Desktop/ML_PATH/week0/dwh-techstack/data_store/station_summary.csv'
    #     INTO TABLE dimStationSummaryAirflow FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;")

    insert = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_to_all_station_table,
        op_kwargs={
            "dwhName": "Sensor_DW",
            "data_path": "/data/sample_data.csv",
            "table_name": "dimAllStations",
        },
    )
    # email = EmailOperator(
    #     task_id='send_email',
    #     # email_conn_id = 'sendgrid_default',
    #     to='kevin@10academy.org',
    #     subject='Weekly Data Warehouse Update',
    #     html_content=""" <h1>Congratulations! Your data is inserted correctly and ready for use.</h1> """,
    #     )

    pre_process >> create_table >> insert
