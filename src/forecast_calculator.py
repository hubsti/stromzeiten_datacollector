from dataclasses import dataclass
import datetime

import xgboost as xgb
import pandas as pd

from src.entsoe_collector import Generation
from src.weatherapi_collector import HistoricalWeather, WeatherForecast

WEEK_AGO: datetime = datetime.date.today() - datetime.timedelta(days=7)
TODAY: datetime = datetime.date.today()
TOMORROW: datetime = datetime.date.today()+datetime.timedelta(days=1)
DAYS_FORECAST = 3


@dataclass
class Next3DaysForecast(object):
    """Object for fetching and calculating carbon emission forecast. 
    Forecast is estimated basing on the last 7 days of CEI data, and 3 next days of a weahter forecast
    Model is using XGBoost: an efficient implementation of gradient boosting for classification and regression problems.

    Args:
        country_code (str): ISO 3166 ALPHA-2 country code
        country (str): country for which data will be fetched
        city (str)

    Methods:
        fetch_forecast_data: fetch data from entso and weather api
        add_lags: add lagging values as new features (not used)
        create_features: create new features depending of the time of the day
        train_and_predict: set up the mode
        
    """

    country_code: str
    country: str
    city: str
    tz: str

    def fetch_forecast_data(self):
        """Fetches last 7 days of data of the weather and carbon emissions,
        and weather forecast for 3 days

        Returns:
            historical_data, weather_forecas (tuple): dataframes containig hitorical and 
            weather forecast data respectively
        """
        start_date = pd.Timestamp(WEEK_AGO, tz=self.tz)
        end_date = pd.Timestamp(TOMORROW, tz=self.tz)
        generation, emissions = Generation(
            start_date, end_date, self.country_code).fetch_process_and_calculate_emissions()
        weather_forecast = WeatherForecast(
            self.city, self.tz, DAYS_FORECAST).fetch()

        week_of_data = pd.date_range(
            WEEK_AGO, TOMORROW-datetime.timedelta(days=1), freq='d')
        historical_weather_list = []
        for day in week_of_data:
            historical = HistoricalWeather(
                self.city, self.tz, DAYS_FORECAST).fetch(day)
            historical_weather_list.append(historical)
        historical_weather = pd. concat(historical_weather_list)
        #change data to hourly granurality if needed (case of german data)
        emissions = emissions.asfreq('H')
        emissions = emissions.fillna(0.0)
        emissions.index = emissions.index.tz_convert(self.tz)
        historical_data = emissions.join(historical_weather)
        weather_forecast = weather_forecast[~weather_forecast.index.isin(
            historical_data.index)]
        historical_data["country_code"] = self.country_code
        weather_forecast["country_code"] = self.country_code
        return historical_data, weather_forecast

    def create_features(self, df):
        """
        Create time series features based on time series index.
        """
        df = df.copy()
        df['hour'] = df.index.hour
        df['dayofweek'] = df.index.dayofweek
        return df

    def add_lags(self, df):
        """
        Add lagging values of target
        """
        target_map = df['Carbon_Intensity_CEI'].to_dict()
        df['lag1'] = (df.index - pd.Timedelta('72 hour')).map(target_map)
        return df

    def train_and_predict(self):
        """
        use XGBoost to provide a regularizing gradient boosting framework
        """
        historical_data, weather_forecast = self.fetch_forecast_data()

        historical_data_featrues = self.create_features(historical_data)
        weather_forecast_featrues = self.create_features(weather_forecast)

        FEATURES = ['temp_c', 'wind_kph', 'wind_degree', 'pressure_mb', 'precip_mm',
                    'humidity', 'cloud', 'feelslike_c', 'windchill_c', 'vis_km', 'hour',
                    'dayofweek']
        TARGET = 'Carbon_Intensity_CEI'

        X_train = historical_data_featrues[FEATURES]
        y_train = historical_data_featrues[TARGET]

        X_test = weather_forecast_featrues[FEATURES]

        reg = xgb.XGBRegressor(base_score=0.5, booster='gbtree',
                               n_estimators=5000,
                               early_stopping_rounds=50,
                               objective='reg:squarederror',
                               max_depth=5,
                               learning_rate=0.02)
        reg.fit(X_train, y_train,
                eval_set=[(X_train, y_train)],
                verbose=0)
        weather_forecast_featrues['Cei_prediction'] = reg.predict(X_test)
        weather_forecast_featrues["country_code"] = self.country_code
        historical_data_featrues["country_code"] = self.country_code
        return weather_forecast_featrues, historical_data_featrues
