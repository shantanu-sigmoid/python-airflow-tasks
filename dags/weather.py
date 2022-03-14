from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


from datetime import datetime
from utils import utils

# default arguments to pass while creating a dag
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 13),
    "retries": 0,
}

# Using context manager to create dag
with DAG("weather", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

    # Fetch weather data from api and add it to csv file in airflow container of docker as weather.csv
    t1 = PythonOperator(task_id = "fetch_weather_data_from_api_into_csv_file", python_callable = utils.convert_weather_data_from_api_for_multiple_cities_to_csv_file,
        op_kwargs = {"cities" : ["Lucknow,IN", "Bengaluru,IN", "Patna,IN", "Bhopal,IN"], "path": "/usr/local/airflow/store_files_airflow/weather_data.csv"})
    
    # Create table in postgres container with specified table name using sql query stored in sql/create_new_table.sql
    t2 = PostgresOperator(task_id="create_new_table_in_postgre", postgres_conn_id='postgres_conn', \
        sql="sql/create_new_table.sql", params = {"table_name": "weather_data"})

    # Read data from data location (demo.csv) and insert into table (weath) in postgres container in docker
    t3 = PostgresOperator(task_id="insert_weather_csv_data_into_postgre_table", postgres_conn_id='postgres_conn', \
    sql="sql/insert_data_into_table.sql", \
    params = {"data_location": "'/usr/local/airflow/store_files_postgres/weather_data.csv'", "table_name": "weather_data"})

    # Set dependencies in DAG
    t1 >> t2 >> t3
