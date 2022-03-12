# Python AirFlow Combined Assignment Questions:

### Subscribe to https://rapidapi.com/community/api/open-weather-map/

Use the docker container to create airflow and postgres instances.

1. Create the first task to use endpoint /Current Weather Data for at least 10 states of India and fill up the csv file with details of 
    State, Description, Temperature, Feels Like Temperature, Min Temperature, Max Temperature, Humidity, Clouds.
2. Create a second task to create a postgres table “Weather” that would have columns same as the csv file.
3. Create a third task that should fill the columns of the table while reading the data from the csv file.
4. Schedule the DAG in AirFlow to run every day at 6:00 am and update the daily weather detail in csv as well as the table.
`0 6 * * * `


TODO: 
1. When i am creating weather database via PostgreHook using postgre_conn as id, exactly where it is creating the table because as soon as i move on to another task, it disappears. (like there is no weather database in the same postgre_conn via PostgreHook)