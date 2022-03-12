from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


from datetime import datetime, timedelta
import requests
import csv
import pandas as pd



# Args:
    # cities: ["", "", ""]  type: list
def convert_weather_data_from_api_for_multiple_cities_to_csv_file(cities, path):
    # URL from where the weather data needs to be fetched
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    # Creating headers to pass in request
    headers = {
        'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
        'x-rapidapi-key': "10d4f3d1fdmshe88aae10d625ac7p1736fcjsn65d4dfd885c7"
    }
    # Query Array for weather API to give back weather of multiple cities in queryArray
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
    # Generating response array came back from weather API
    responseArray = []
    for query in queryArray:
        response = requests.get(url, headers=headers, params=query)
        responseArray.append(response.json())
    print(responseArray) # Remove this print 
    # Format the data from json {"": [], "": {}, "": ""} to list of lists [[], [], []]
    data = json_to_list_pretty_formatter(responseArray)
    # Write the data into CSV
    write_to_csv(data, path)


# Args:
    # data : {"": [], "": {}, "": ""} type: json
    # filepath: ""        type: string
def write_to_csv(data, filepath):
    with open(filepath, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        header = ['state', 'description', 'temp', 'feels_like_temp', 'min_temp', 'max_temp', 'humidity', 'clouds']
        writer.writerow(header)
        # write the data after converting it to list of lists [[], [], []] from json {"": [], "": {}, "": ""} 
        for row in data:
            writer.writerow(row)


# Args:
    # jsonArray : {"": [], "": {}, "": ""} type: json
# Return:
    # pretty_data : [[], [], []]           type: list of lists
def json_to_list_pretty_formatter(jsonArray):
    pretty_data = []
    for json_data in jsonArray:
        data = [json_data["name"],json_data["weather"][0]["description"],json_data["main"]["temp"], json_data["main"]["feels_like"], json_data["main"]["temp_min"], json_data["main"]["temp_max"],json_data["main"]["humidity"], json_data["clouds"]["all"]]
        pretty_data.append(data)
    return pretty_data


# Creates table weather in default database in postgres_conn connection id
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
    # Create hook then take connection from it
    source_conn = PostgresHook(postgres_conn_id = "postgres_conn", schema = "airflow").get_conn()
    # Get cursor from connection 
    source_cursor = source_conn.cursor()
    # Execute query
    source_cursor.execute(query)
    # Close the cursor
    source_cursor.close()
    # Close the connection
    source_conn.close()


def read_data_from_csv_and_add_to_table(path):
    # Create hook then take connection from it
    source_conn = PostgresHook(postgres_conn_id = "postgres_conn", schema = "airflow").get_conn()
    # Get cursor from connection
    source_cursor = source_conn.cursor()
    # Read csv file as dataframe with first row as header
    df = pd.read_csv(path, header = 0)
    # Create table again to insert query [REMOVE THIS ASAP]
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
    # For each row in dataframe, generate subsequent query to be run on cursor
    for row in range(len(df)):
        row_data = df.iloc[row]
        single_insert_query = f"INSERT INTO Weather VALUES ({row+1}, '{row_data['state']}', '{row_data['description']}', {row_data['temp']},{row_data['feels_like_temp']},{row_data['min_temp']},{row_data['max_temp']},{row_data['humidity']},{row_data['clouds']});"
        all_data_insert_query += single_insert_query
    # Execute query on cursor
    source_cursor.execute(all_data_insert_query)
    print("Data INSERTED Successfully")
    # Query of selecting all data in cursor
    source_cursor.execute("SELECT * FROM Weather;")
    # Fetching records from cursor
    records = source_cursor.fetchall()
    print(records)
    # Close the cursor
    source_cursor.close()
    # Close the connection
    source_conn.close()
    


# default arguments to pass while creating a dag
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Using context manager to create dag
with DAG("weather", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

    # Dummy task 
    t1 = DummyOperator(task_id="start_task")

    # Fetch weather data from api and add it to csv file in airflow container of docker as weather.csv
    t2 = PythonOperator(task_id = "fetch_weather_data", python_callable = convert_weather_data_from_api_for_multiple_cities_to_csv_file,
        op_kwargs = {"cities" : ["Lucknow,IN"], "path": "weather.csv"})
    
    # Create a brand new table in postgres named as weather [?????????]
    t3 = PythonOperator(task_id = "create_table_postgres", python_callable = create_table, provide_context = True)

    # Read data from csv [weather.csv] and load the content to postgres table [?????????]
    t4 = PythonOperator(task_id = "read_data_from_csv_and_add_to_table", python_callable = read_data_from_csv_and_add_to_table, op_kwargs = {"path": "weather.csv"})

    # Set dependencies in DAG
    t1 >> t2 >> t3 >>t4
