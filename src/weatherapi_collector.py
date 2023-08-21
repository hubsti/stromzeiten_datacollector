import os
from dataclasses import dataclass

import pandas as pd
import requests

from utils.weatherapi_tags import CURRENT_WEATHER_TAGS, WEATHER_TAGS

WEATHERAPI_URL: str = 'http://api.weatherapi.com/v1/'


@dataclass
class WeatherAPIData(object):
    """Object for data collection from the WeatherAPI.com Platform:

    Attributes
    ----------
    location : str
        Latitude/Longitude (decimal degree) or city name
    tz : str
        timezone identifier (example: Brussels/Europe)
    days_forecast : int
        Number of days of weather forecast. Value ranges from 1 to 10

    Methods
    -------
    format_weatherapi_data():
        format API response and save in dataframe
    """
    location: str
    tz: str
    days_forecast: int
    country_code: str
    def format_weatherapi_data(self, request):
        weather_data_raw: pd.DataFrame = pd.DataFrame(
            request.json()["forecast"]['forecastday'])
        days = []
        for day in weather_data_raw['hour']:
            weather_data_raw: pd.DataFrame = pd.DataFrame(
                day)
            weather_data_raw = weather_data_raw[WEATHER_TAGS].copy()
            weather_data_raw = weather_data_raw.set_index("time")
            weather_data_raw.index = pd.to_datetime(weather_data_raw.index)
            weather_data_raw.index = weather_data_raw.index.tz_localize(
                self.tz)
            days.append(weather_data_raw)
        merged_weahter_data = pd.concat(days)
        merged_weahter_data["country_code"] = self.country_code
        return merged_weahter_data


class CurrentWeather(WeatherAPIData):
    """Data about current weather for a specified location"""

    def fetch(self) -> pd.DataFrame:
        """Fetch current weather data

        Returns:
            current_weather_raw (pd.DataFrame): dataframe with raw current weater data (single timestamp)
        """
        req: requests.Response = requests.get(
            f'{WEATHERAPI_URL}current.json?key={os.environ["API_KEY_WEATHERAPI"]}&q={self.location}&aqi=yes')
        current_weather_raw: pd.DataFrame = pd.json_normalize(req.json())
        current_weather_raw = current_weather_raw[CURRENT_WEATHER_TAGS].copy()
        current_weather_raw["country_code"] = self.country_code
        return current_weather_raw


class WeatherForecast(WeatherAPIData):
    """Data about current weather for a specified location"""

    def fetch(self) -> pd.DataFrame:
        """Fetch weather data forecast

        Returns:
            weather_forecast_raw (pd.DataFrame): dataframe with a raw forecast weater data
        """
        req = requests.Response = requests.get(
            f'{WEATHERAPI_URL}forecast.json?key={os.environ["API_KEY_WEATHERAPI"]}&q={self.location}&days={self.days_forecast}&aqi=yes')
        weather_forecast_raw: pd.DataFrame = self.format_weatherapi_data(req)
        weather_forecast_raw = weather_forecast_raw.drop('time_epoch', axis=1)
        weather_forecast_raw = weather_forecast_raw.drop('wind_dir', axis=1)
        weather_forecast_raw["country_code"] = self.country_code
        return weather_forecast_raw


class HistoricalWeather(WeatherAPIData):
    api_date_from = "2023-06-15"
    def fetch(self, api_date_from) -> pd.DataFrame:
        """Fetch historical weather data

        Returns:
            weather_historical_raw (pd.DataFrame): dataframe with a raw historical weater data
        """
        req: requests.Response = requests.get(
            f'{WEATHERAPI_URL}history.json?key={os.environ["API_KEY_WEATHERAPI"]}&q={self.location}&dt={api_date_from}')
        weather_historical_raw: pd.DataFrame = self.format_weatherapi_data(req)
        weather_historical_raw["country_code"] = self.country_code
        return weather_historical_raw
