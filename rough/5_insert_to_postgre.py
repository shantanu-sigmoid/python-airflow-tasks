# Use Xcom to get the data
insert_weather_data = PostgresOperator(
    task_id="insert_weather_data",
    postgres_conn_id="postgres_default",
    sql="sql/insert_weather_data.sql",
    parameters={
        "state": "", 
        "description": "",
        "temp": "",
        "feels_like_temp": "",
        "min_temp": "",
        "max_temp": "",
        "humidity": "",
        "clouds": ""
    },
)