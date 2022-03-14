def get_weather_api_method():
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    # 10 States
    state_list = ['bihar', 'jharkhand', 'goa', 'uttar pradesh', 'karnataka', 'punjab', 'haryana', 'gujarat', 'kerala','assam']
    df = pd.DataFrame(columns=["State", "Description", "Temperature", "Feels_Like_Temperature", "Min_Temperature", "Max_Temperature",
                 "Humidity", "Clouds"])
    for state in state_list:
        querystring = {"q": state}
        # API Host and Key
        headers = {
            'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
            'x-rapidapi-key': "ecf7830d16msh64cf3478497ecdcp15bd97jsn0db09825b716"
        }

        response = requests.get(url, headers=headers, params=querystring)
        info = response.json()
        # time.sleep(10)
        try:
            df = df.append({'State': info['name'], "Description": info['weather'][0]['description'],
                            'Temperature': info['main']['temp'], "Feels_Like_Temperature": info['main']['feels_like'],
                            "Min_Temperature": info['main']['temp_min'], "Max_Temperature": info['main']['temp_max'],
                            "Humidity": info['main']['humidity'], "Clouds": info['clouds']['all']}, ignore_index=True)
        except:
            print("API Request limit exceeds")

    path = "/usr/local/airflow/store_files_airflow"
    if not os.path.isfile(os.path.join(path, '/weather_data.csv')):
        df.to_csv(path + '/weather_data.csv', index=False)
    else:
        os.remove(os.path.join(path, '/weather_data.csv'))
        df.to_csv(os.path.join(path, '/weather_data.csv'), index=False)
    print(df.head())




from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from utils import get_weather_api_method

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
with DAG("Weather_Dag", default_args=default_args, schedule_interval='* 18 * * *',
         template_searchpath=['/usr/local/airflow/sql_files'], catchup=False) as dag:
    # Filling up the CSV with the 10 states weather data
    task1 = PythonOperator(task_id="check_file_exist_or_create_new_file", python_callable=get_weather_api_method)
    # Creating the table same as csv columns
    task2 = PostgresOperator(task_id="create_new_table", postgres_conn_id='postgres_conn', sql="create_new_table.sql")
    # Filling up the columns of the table while reading the data from the csv file
    task3 = PostgresOperator(task_id="insert_data_into_table", postgres_conn_id='postgres_conn',
                          sql="copy weather FROM '/store_files_postgresql/weather_data.csv' DELIMITER ',' CSV HEADER ;")
    task1 >> task2 >> task3