import argparse
import datetime
import logging
import os
import time
from itertools import count

import pandas as pd
import pandas as pds
import psycopg
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.db_load import load_forecast_to_db, load_to_db
from src.entsoe_collector import Generation, Load, Prices
from src.forecast_calculator import Next3DaysForecast
from src.weatherapi_collector import WeatherForecast
from utils.logger import CustomFormatter
from utils.db_cleanup import remove_dupilcates

load_dotenv()

# Set up logging
alchemyEngine   = create_engine('postgresql+psycopg2://admin:admin@127.0.0.1/stromzeiten', pool_recycle=3600);

 

# Connect to PostgreSQL server

dbConnection    = alchemyEngine.connect();
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
        print("---------------Generation-----------------")
        print(generation)
        remove_dupilcates(generation, alchemyEngine, country_code, "generation_acc")
        remove_dupilcates(emissions, alchemyEngine, country_code, "emissions_acc")
        #generation.to_sql('generation_acc', alchemyEngine,if_exists='append')
        print("---------------Emission-----------------")
        print(emissions)
    except Exception as e:
        logger.exception(f"error while fetching generation data: {e}")
        pass

    logger.info(
        f"fetching load data from {start_date} to {end_date} for a country {country}")
    try:
        load: pd.DataFrame = Load(start_date, end_date, country_code).fetch()
        print("---------------load-----------------")
        print(load)
        remove_dupilcates(load, alchemyEngine, country_code, "load_acc")
        logger.info("loading consumption data to database")
    
    except Exception as e:
        logger.error(f"error while fetching load data: {e}") 
        pass

    logger.info(
        f"fetching prices data from {start_date} to {end_date} for a country {country}")
    try:
        prices: pd.DataFrame = Prices(
            start_date, end_date, country_code).fetch()
        print("---------------Prices-----------------")
        print(prices)
        remove_dupilcates(prices, alchemyEngine, country_code, "prices_acc")
        logger.info("loading prices data to database")
    except Exception as e:
        logger.exception(f"error while fetchin prices data: {e}")
        pass

    logger.info(
        f"fetching weather forecast in {city} for today")
    try:
        forecast = WeatherForecast(city, timezone, DAYS_FORECAST, country_code).fetch()
        print("---------------weather-----------------")
        print(forecast)
        logger.info("loading weather data to database")
        remove_dupilcates(forecast, alchemyEngine, country_code, "weather_acc")
    except Exception as e:
        logger.exception(f"error while fetching weather forecast data: {e}")
        pass
    

    logger.info(
        f"fetching and calculating carbon emission forecast for {country}")
    try:
        forecast_data, historical_data  = Next3DaysForecast(country_code, country, city, timezone).train_and_predict()
        logger.info("loading forecast data to database")
        remove_dupilcates(forecast_data, alchemyEngine, country_code, "forecast_acc")
        print("---------------Forecast-----------------")
        print(forecast_data)
    except Exception as e:
        logger.exception(f"error while fetching weather forecast data: {e}")
        pass
    dbConnection.close();


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
