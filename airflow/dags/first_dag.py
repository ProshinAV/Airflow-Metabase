import psycopg2
import os
from psycopg2 import Error
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from os import listdir
from os.path import isfile, join
from random import randint
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook

def get_db_url(conn_id) -> str:
    connection = BaseHook.get_connection(conn_id)
    return f'postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}'

def _training_model():
    new_path = str(os.getcwd()) + '/upload-data/ecard.csv'
    print(new_path)
    print(get_db_url('Postgres'))
    cards = pd.read_csv(new_path)
    print(cards.info())


with DAG("first_dag", start_date=datetime(2022, 1 ,1),
    schedule_interval="@daily", catchup=False) as dag:

        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )
        load_data_csv = PostgresOperator(
            task_id="load_data_csv",
            postgres_conn_id ="Postgres",
            sql = "COPY credits(limit_bal, sex, education, marriage, age, pay_1, pay_2, pay_3, pay_4, pay_5, pay_6, bill_amt1, bill_amt2, bill_amt3, bill_amt4, bill_amt5, bill_amt6, pay_amt1, pay_amt2, pay_amt3, pay_amt4, pay_amt5, pay_amt6, def_pay) FROM '/opt/airflow/upload-data/UCI_Credit_Card.csv' DELIMITER ',' CSV HEADER"
        )

        task_load_iris_data = BashOperator(
            task_id='load_iris_data',
            bash_command=(
                'psql postgresql://airflow:airflow@postgres:5432/postgres -c "'
                '\COPY credits(id, limit_bal, sex, education, marriage, age, pay_1, pay_2, pay_3, pay_4, pay_5, pay_6, bill_amt1, bill_amt2, bill_amt3, bill_amt4, bill_amt5, bill_amt6, pay_amt1, pay_amt2, pay_amt3, pay_amt4, pay_amt5, pay_amt6, def_pay) '
                "FROM '/opt/airflow/upload-data/UCI_Credit_Card.csv' "
                "DELIMITER ',' "
                'CSV HEADER"'
            )
        )
        