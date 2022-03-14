import csv
import requests

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
        try:
            response = requests.get(url, headers=headers, params=query)
        except requests.exceptions.Timeout:
            print("Timeout Exception Occured: Limit reached for fetching API data")
        except requests.exceptions.TooManyRedirects:
            # Telling user their URL was bad and try a different one
            print("Bad URL Exception: Check if entered URL is correct")
        responseArray.append(response.json())
    # Format the data from json {"": [], "": {}, "": ""} to list of lists [[], [], []]
    data = json_to_list_pretty_formatter(responseArray)
    # Write the data into CSV
    write_to_csv(data, path)


# Args:
    # data : {"": [], "": {}, "": ""} type: json
    # filepath: ""        type: string
def write_to_csv(data, filepath):
    # Using context manager to open file
    with open(filepath, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        header = ['city', 'description', 'temp', 'feels_like_temp', 'min_temp', 'max_temp', 'humidity', 'clouds']
        # write the data after converting it to list of lists [[], [], []] from json {"": [], "": {}, "": ""} 
        # for row in data:
        #     writer.writerow(row)
        try:
            writer.writerow(header)
            writer.writerows(data)
        except:
            print(f"Error occured while writing multiple rows to csv file {filepath}")


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