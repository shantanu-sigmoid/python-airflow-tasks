class WeatherData(object):
    """Class for a representation of the data"""

    def __init__(self, name, country, temp, humidity, pressure, visibility, wind_speed, timestamp=datetime.now()):
        self.name = name
        self.country = country
        self.temp = temp
        self.humidity = humidity
        self.pressure = pressure
        self.visibility = visibility
        self.wind_speed = wind_speed
        self.timestamp = timestamp