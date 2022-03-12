"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import json
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests


# Args:
    # cities: ["", "", ""]  type: list
# Return:
    # responseArray: [{},{},{}] type: list
def convert_weather_data_from_api_for_multiple_cities_to_csv_file(cities, path):
    # URL from where the weather data needs to be fetched
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    # Creating headers to pass in request
    # read_config = configparser.ConfigParser()
    # read_config.read("keys.ini")
    headers = {
        'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
        'x-rapidapi-key': "10d4f3d1fdmshe88aae10d625ac7p1736fcjsn65d4dfd885c7"
    }
    queryArray = []
    for city in cities:
        queryArray.append({
            "q":city,
            "lat":"0",
            "lon":"0",
            "id":"2172797",
            "lang":"null",
            "units":"imperial",
            "mode":"json"
        })
    # Generating response array
    responseArray = []
    for query in queryArray:
        print("============")
        print(query)
        print("============")
        response = requests.get(url, headers=headers, params=query)
        responseArray.append(response.json())
    print("============")
    print(responseArray)
    print("=============")
    # Change this: Only for debug purpose
    # return responseArray
    
    write_to_csv(responseArray, path)
    # print(responseArray) 



import csv
# header = ['state', 'description', 'temp', 'feels_like_temp', 'min_temp', 'max_temp', 'humidity', 'clouds']
# data = ['Punjab', 'clear sky', 87.87, 91.83, 91.83, 18, 2]

def write_to_csv(data, filepath):
    with open(filepath, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        header = ['state', 'description', 'temp', 'feels_like_temp', 'min_temp', 'max_temp', 'humidity', 'clouds']
        writer.writerow(header)
        # write the data
        for row in json_to_list_pretty_formatter(data):
            writer.writerow(row)

def json_to_list_pretty_formatter(jsonArray):
    pretty_data = []
    for json_data in jsonArray:
        data = [json_data["name"],json_data["weather"][0]["description"],json_data["main"]["temp"], json_data["main"]["feels_like"], json_data["main"]["temp_min"], json_data["main"]["temp_max"],json_data["main"]["humidity"], json_data["clouds"]["all"]]
        pretty_data.append(data)
    return pretty_data





default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def create_table(ds, **kwargs):
    query = """CREATE TABLE IF NOT EXISTS Weather (
            id SERIAL PRIMARY KEY,
            state VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            temp NUMERIC(6, 4) NOT NULL,
            feels_like_temp NUMERIC(6, 4) NOT NULL,
            min_temp NUMERIC(6, 4) NOT NULL,
            max_temp NUMERIC(6, 4) NOT NULL,
            humidity NUMERIC(6, 4) NOT NULL,
            clouds NUMERIC(6, 4) NOT NULL
            );
            """
    source_cursor = PostgresHook(postgres_conn_id = "postgres_conn", schema = "airflow").get_conn().cursor()
    source_cursor.execute(query)
    # records = source_cursor.fetchall()
    # print("RECORDS +======>>>>>>")
    # print(records)
    source_cursor.close()


import pandas as pd
def read_data_from_csv_and_add_to_table(path):
    source_cursor = PostgresHook(postgres_conn_id = "postgres_conn", schema = "airflow").get_conn().cursor()
    df = pd.read_csv(path, header = 0)
    all_data_insert_query = """
            CREATE TABLE IF NOT EXISTS Weather (
            id SERIAL PRIMARY KEY,
            state VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            temp NUMERIC(6, 4) NOT NULL,
            feels_like_temp NUMERIC(6, 4) NOT NULL,
            min_temp NUMERIC(6, 4) NOT NULL,
            max_temp NUMERIC(6, 4) NOT NULL,
            humidity NUMERIC(6, 4) NOT NULL,
            clouds NUMERIC(6, 4) NOT NULL
            );
    """
    for row in range(len(df)):
        row_data = df.iloc[row]
        single_insert_query = f"INSERT INTO Weather VALUES ({row+1}, '{row_data['state']}', '{row_data['description']}', {row_data['temp']},{row_data['feels_like_temp']},{row_data['min_temp']},{row_data['max_temp']},{row_data['humidity']},{row_data['clouds']});"
        all_data_insert_query += single_insert_query
    source_cursor.execute(all_data_insert_query)
    print("Data INSERTED Successfully")
    source_cursor.execute("SELECT * FROM Weather;")
    records = source_cursor.fetchall()
    print("RECORDS +======>>>>>>")
    print(records)
    # source_conn.close()
    source_cursor.close()



with DAG("weather", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

# t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = DummyOperator(task_id="start_task")

    t2 = PythonOperator(task_id = "fetch_weather_data", python_callable = convert_weather_data_from_api_for_multiple_cities_to_csv_file,
        op_kwargs = {"cities" : ["Lucknow,IN", "Bengaluru,IN"], "path": "weather.csv"})
    
    t3 = PythonOperator(task_id = "create_table_postgres", python_callable = create_table, provide_context = True)

    t4 = PythonOperator(task_id = "read_data_from_csv_and_add_to_table", python_callable = read_data_from_csv_and_add_to_table, op_kwargs = {"path": "weather.csv"})

    t1 >> t2 >> t3 >>t4
