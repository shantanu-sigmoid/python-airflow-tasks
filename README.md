# Python AirFlow Combined Assignment Questions:

### Subscribe to https://rapidapi.com/community/api/open-weather-map/

Use the docker container to create airflow and postgres instances.

1. Create the first task to use endpoint /Current Weather Data for at least 10 states of India and fill up the csv file with details of 
    State, Description, Temperature, Feels Like Temperature, Min Temperature, Max Temperature, Humidity, Clouds.
2. Create a second task to create a postgres table “Weather” that would have columns same as the csv file.
3. Create a third task that should fill the columns of the table while reading the data from the csv file.
4. Schedule the DAG in AirFlow to run every day at 6:00 am and update the daily weather detail in csv as well as the table.


## ERRORS ENCOUNTERED
|   Error    | Resolve |  Links |
| ----------- | ----------- | ----- |
| `cryptography.fernet.InvalidToken`      | Remake postgres_conn connection from ADMIN in Airflow UI  |  [here](https://qiita.com/ctivan/items/068a26fc6ba25110a87a) |
|`'weather' relation doesn't exist`|Use volume instead of creating table in metadata (postgres) because as soon as tasks changes, it will be all gone (Alternative Solutions: Xcom, Airflow Plugins)|[here](https://stackoverflow.com/questions/50858770/airflow-retain-the-same-database-connection)|
|`COPY 'weather' FROM .. `<br>why weather in string format??|Instead of using `parameters = {"table_name": "weather"}` and then using via `COPY %(table_name)s FROM ..` use `params = {"table_name": "weather"}` and place `COPY {{table_name.weather}} FROM .. ` in sql||


