import argparse
import datetime
import logging
import os
import time
from itertools import count

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

from src.db_load import load_forecast_to_db, load_to_db
from src.entsoe_collector import Generation, Load, Prices
from src.forecast_calculator import Next3DaysForecast
from src.weatherapi_collector import WeatherForecast
from utils.logger import CustomFormatter

load_dotenv()

# Set up logging
logger = logging.getLogger("Data_Loader")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)
logger.propagate = False

YESTERDAY: datetime = datetime.datetime.today() - datetime.timedelta(days=1)
TODAY: datetime = datetime.datetime.today()
DAYS_FORECAST = 1


def main(country_code, country, city, timezone):
    start_date = pd.Timestamp(YESTERDAY, tz=timezone)
    end_date = pd.Timestamp(TODAY, tz=timezone)
    logger.info(
        f"fetching generation data from {start_date} to {end_date} for a country {country}")
    try:
        generation, emissions = Generation(
            start_date, end_date, country_code).fetch_process_and_calculate_emissions()
        logger.info("loading generation data to database")
        time_elapsed = load_to_db(generation, country)
        logger.info(
            f"loading generation data to db executed in {time_elapsed}!")
        logger.info("loading emissions data to database")
        time_elapsed = load_to_db(emissions, country)
        logger.info(
            f"loading emissions data to db executed in {time_elapsed}!")
    except Exception as e:
        logger.exception(f"error while fetching generation data: {e}")
        pass

    logger.info(
        f"fetching load data from {start_date} to {end_date} for a country {country}")
    try:
        load: pd.DataFrame = Load(start_date, end_date, country_code).fetch()
        logger.info("loading consumption data to database")
        time_elapsed = load_to_db(load, country)
        logger.info(
            f"loading consumption data to db executed in {time_elapsed}!")
    except Exception as e:
        logger.error(f"error while fetching load data: {e}")
        pass

    logger.info(
        f"fetching prices data from {start_date} to {end_date} for a country {country}")
    try:
        prices: pd.DataFrame = Prices(
            start_date, end_date, country_code).fetch()
        logger.info("loading prices data to database")
        time_elapsed = load_to_db(prices, country)
        logger.info(f"loading prices data to db executed in {time_elapsed}!")
    except Exception as e:
        logger.exception(f"error while fetchin prices data: {e}")
        pass

    logger.info(
        f"fetching weather forecast in {city} for today")
    try:
        forecast = WeatherForecast(city, timezone, DAYS_FORECAST).fetch()
        logger.info("loading weather data to database")
        time_elapsed = load_to_db(forecast, country)
        logger.info(f"loading weather data to db executed in {time_elapsed}!")
    except Exception as e:
        logger.exception(f"error while fetching weather forecast data: {e}")
        pass
    

    logger.info(
        f"fetching and calculating carbon emission forecast for {country}")
    try:
        forecast_data, historical_data  = Next3DaysForecast(country_code, country, city, timezone).train_and_predict()
        logger.info("loading forecast data to database")
        time_elapsed = load_forecast_to_db(forecast_data, country)
        logger.info(f"loading forecast data to db executed in {time_elapsed}!")
    except Exception as e:
        logger.exception(f"error while fetching weather forecast data: {e}")
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='StromzeitenDataLoader',
        description='Fetch electricity and weather data and load them into database ',
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true", help="Interactive mode"
    )
    parser.add_argument(
        "cc",
        help="ISO 3166 ALPHA-2 country code of the country for which data will be fetched",
    )
    parser.add_argument(
        "country",
        help="country for which data will be fetched",
    )
    parser.add_argument(
        "city",
        help="city for which weather data will be fetched"
    )
    parser.add_argument(
        "tz",
        help="timezone identifier (example: Brussels/Europe)"
    )
    args = parser.parse_args()

    country_code = args.cc
    country = args.country
    city = args.city
    timezone = args.tz

    main(country_code, country, city, timezone)
