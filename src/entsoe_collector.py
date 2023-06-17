import os
from dataclasses import dataclass

import pandas as pd
from entsoe import EntsoePandasClient

from utils.entso_generation_tags import ALL_TAGS, TAGS_RENEW, TAGS_NON_RENEW
from utils.emission_factors import CO2_FACTORS


@dataclass
class EntsoeData(object):
    """Object for data collection from the ENTSO-E Transparency Platform:
    Central collection of electriocity generation, 
    transportation and consumption data for the pan-European market

    Attributes
    ----------
    api_start_date : pd.Timestamp
        start date of data
    api_end_date : pd.Timestamp
        end date of data
    country_code : str
        ISO 3166 ALPHA-2 country code

    Methods
    -------
    collector():
        set up the API client using API KEY
    """

    api_start_date: pd.Timestamp
    api_end_date: pd.Timestamp
    country_code: str

    def collector(self) -> EntsoePandasClient:
        client = EntsoePandasClient(api_key=os.environ["ENTSOE_API_KEY"])
        return client


class Generation(EntsoeData):
    """Data about energy production per production type in Megawatts"""

    def fetch(self) -> pd.DataFrame:
        """Fetch generation data

        Returns:
            generation_raw (pd.DataFrame): dataframe with raw generation data
        """
        client: EntsoePandasClient = self.collector()
        # check if the index contains "Actual Aggregated" and remove it
        generation_raw: pd.DataFrame = client.query_generation(
            country_code=self.country_code, start=self.api_start_date, end=self.api_end_date)
        if isinstance(generation_raw.columns, pd.MultiIndex):
            generation_raw.columns = generation_raw.columns.droplevel(level=1)
        #remove duplicated columns
        generation_raw = generation_raw.loc[:,~generation_raw.columns.duplicated()].copy()
        return generation_raw

    def process(self, generation_raw) -> pd.DataFrame:
        """Processes generation data; sums up renewable and non-renewable generation;
           cleans-up NaNs; renames columns

        Parameters:
            generation_raw (pd.DataFrame): dataframe containing raw data

        Returns:
            generation_processed (pd.DataFrame): dataframe with processed generation data
        """
        generation_processed: pd.DataFrame = pd.DataFrame(
            index=generation_raw.index,
            columns=ALL_TAGS.keys(),
        )  # create a new data frame based on raw data
        for k, v in ALL_TAGS.items():  # iterate over all production types
            # if the production type is not available, assume the production is 0
            if v in generation_raw.columns:
                generation_processed[k] = generation_raw[v]
            else:
                generation_processed[k] = 0.0
        generation_processed = generation_processed.fillna(0.0)
        generation_processed["Renewables"] = generation_processed[list(
            TAGS_RENEW)].sum(axis=1)
        generation_processed["NonRenewables"] = generation_processed[list(
            TAGS_NON_RENEW)].sum(axis=1)
        generation_processed["Total"] = generation_processed["NonRenewables"] + \
            generation_processed["Renewables"]
        return generation_processed

    def calculate_carbon_emissions(self, processed_generation: pd.DataFrame) -> pd.DataFrame:
        """Returns calculated carbon emissions per production type in gCO₂eq 
           and the total carbon intensity in gCO₂eq/kWh

        Parameters:
            generation_raw (pd.DataFrame): dataframe containing processed generation data

        Return:
            carbon_emissions (pd.DataFrame): carbon_emissions with calculated carbon emissions
        """
        carbon_emissions = pd.DataFrame()
        for k, v in CO2_FACTORS.items():
            carbon_emissions[k] = (processed_generation[k] * 1e3 * v) / 1e6
        carbon_emissions["Total"] = carbon_emissions.sum(axis="columns")
        carbon_emissions["Carbon_Intensity"] = (
            carbon_emissions["Total"] * 1e6 / (processed_generation["Total"] * 1e3))
        carbon_emissions = carbon_emissions.add_suffix('_CEI')
        return carbon_emissions

    def fetch_process_and_calculate_emissions(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Returns processed genration data and calcualted carbon eimissions"""
        generation_raw = self.fetch()
        generation_processed = self.process(generation_raw)
        carbon_emissions = self.calculate_carbon_emissions(
            generation_processed)
        return generation_processed, carbon_emissions


class Load(EntsoeData):
    """Data about power consumption in Megawatts"""

    def fetch(self) -> pd.DataFrame:
        """Fetch load data

        Returns:
            load_raw (pd.DataFrame): dataframe with raw load data
        """
        client: EntsoePandasClient = self.collector()
        load_raw: pd.DataFrame = client.query_load(
            country_code=self.country_code, start=self.api_start_date, end=self.api_end_date)
        return load_raw


class Prices(EntsoeData):
    """Data of actual electricity prices in EUR/MWh"""

    def fetch(self) -> pd.DataFrame:
        """Fetch load data

        Returns:
            load_raw (pd.DataFrame): dataframe with raw load data
        """
        client: EntsoePandasClient = self.collector()
        prices_raw: pd.DataFrame = client.query_day_ahead_prices(
            country_code=self.country_code, start=self.api_start_date, end=self.api_end_date)
        prices_raw = prices_raw.to_frame()
        prices_raw= prices_raw.rename( columns={0 :'Price'})
        return prices_raw
