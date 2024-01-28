# Description: This script is used to fetch data from ENTSOE, weatherapi and calculate forecast for all countries in EU
import datetime
import logging


import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


from src.entsoe_collector import Generation, Load, Prices
from src.forecast_calculator import Next3DaysForecast
from src.weatherapi_collector import WeatherForecast
from utils.logger import CustomFormatter
from src.db_cleanup import insert_dataframe, update_dataframe
from utils.cron import european_countries

load_dotenv()

# Set up logging
alchemyEngine = create_engine(
    "postgresql+psycopg2://postgres:<3Fuksik69@127.0.0.1", pool_recycle=3600
)


# Connect to PostgreSQL server

dbConnection = alchemyEngine.connect()
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
NEW_DB = False


def main():
    for country_eu in european_countries:
        country = country_eu[0]
        country_code = country_eu[1]
        city = country_eu[2]
        timezone = country_eu[3]
        start_date = pd.Timestamp(YESTERDAY, tz=timezone)
        end_date = pd.Timestamp(TODAY, tz=timezone)
        logger.info(
            f"fetching generation data from {start_date} to {end_date} for a country {country}"
        )
        try:
            generation, emissions = Generation(
                start_date, end_date, country_code
            ).fetch_process_and_calculate_emissions()
            print("---------------Generation-----------------")
            generation.columns = generation.columns.str.lower()
            generation["country_code"] = country_code
            # print(generation)
            if NEW_DB:
                generation.to_sql(
                    name="generation", con=alchemyEngine, if_exists="append"
                )
            else:
                update_dataframe(generation, alchemyEngine, country_code, "generation")
            print("---------------Emission-----------------")
            # print(emissions)
            emissions["country_code"] = country_code
            if NEW_DB:
                emissions.to_sql(
                    name="emissions", con=alchemyEngine, if_exists="append"
                )
            else:
                update_dataframe(emissions, alchemyEngine, country_code, "emissions")
        except Exception as e:
            logger.exception(f"error while fetching generation data: {e}")
            pass

        logger.info(
            f"fetching load data from {start_date} to {end_date} for a country {country}"
        )
        try:
            load: pd.DataFrame = Load(start_date, end_date, country_code).fetch()
            print("---------------load-----------------")
            # print(load)
            load["country_code"] = country_code
            if NEW_DB:
                load.to_sql(name="load", con=alchemyEngine, if_exists="append")
            else:
                update_dataframe(load, alchemyEngine, country_code, "load")
            logger.info("loading consumption data to database")

        except Exception as e:
            logger.error(f"error while fetching load data: {e}")
            pass

        logger.info(
            f"fetching prices data from {start_date} to {end_date} for a country {country}"
        )
        try:
            prices: pd.DataFrame = Prices(start_date, end_date, country_code).fetch()
            print("---------------Prices-----------------")
            # print(prices)
            prices["country_code"] = country_code
            if NEW_DB:
                prices.to_sql(name="prices", con=alchemyEngine, if_exists="append")
            else:
                update_dataframe(prices, alchemyEngine, country_code, "prices")
            logger.info("loading prices data to database")
        except Exception as e:
            logger.exception(f"error while fetchin prices data: {e}")
            pass

        logger.info(f"fetching weather forecast in {city} for today")
        try:
            forecast = WeatherForecast(city, timezone, DAYS_FORECAST).fetch()
            print("---------------weather-----------------")
            # print(forecast)
            forecast["country_code"] = country_code
            if NEW_DB:
                forecast.to_sql(name="forecast", con=alchemyEngine, if_exists="append")
            else:
                update_dataframe(
                    forecast, alchemyEngine, country_code, "forecast", "time"
                )
            logger.info("loading weather data to database")
        except Exception as e:
            logger.exception(f"error while fetching weather forecast data: {e}")
            pass

        logger.info(f"fetching and calculating carbon emission forecast for {country}")
        try:
            forecast_data, historical_data = Next3DaysForecast(
                country_code, country, city, timezone
            ).train_and_predict()
            logger.info("loading forecast data to database")
            print("---------------Forecast-----------------")
            # print(forecast_data)
            forecast_data["country_code"] = country_code
            if NEW_DB:
                forecast_data.to_sql(
                    name="forecast_data", con=alchemyEngine, if_exists="append"
                )
            else:
                update_dataframe(
                    forecast_data, alchemyEngine, country_code, "forecast_data", "time"
                )
        except Exception as e:
            logger.exception(f"error while fetching weather forecast data: {e}")
            pass
        dbConnection.close()


if __name__ == "__main__":
    main()
