import requests
import configparser

# Args:
    # cities: ["", "", ""]  type: list
# Return:
    # responseArray: [{},{},{}] type: list
def fetch_weather_data_from_api_for_multiple_cities(cities):
    # URL from where the weather data needs to be fetched
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    # Creating headers to pass in request
    read_config = configparser.ConfigParser()
    read_config.read("keys.ini")
    headers = {
        'x-rapidapi-host': read_config.get("SECRET", "hostname"),
        'x-rapidapi-key': read_config.get("SECRET", "key")
    }
    # Generating query Array
    querystring = {
        "q":"",
        "lat":"0",
        "lon":"0",
        "id":"2172797",
        "lang":"null",
        "units":"imperial",
        "mode":"json"
    }
    queryArray = []
    for city in cities:
        querystring["q"] = city
        queryArray.append(querystring)
    # Generating response array
    responseArray = []
    for querystring in queryArray:
        response = requests.get(url, headers=headers, params=querystring)
        responseArray.append(response.json())
    return responseArray


