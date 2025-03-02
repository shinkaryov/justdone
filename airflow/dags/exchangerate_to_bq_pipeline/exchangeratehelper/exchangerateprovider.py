import aiohttp
import asyncio
import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any


class ExchangeRateProvider(ABC):
    """
    Abstract base class for fetching exchange rates.

    Provides methods to fetch the latest and historical exchange rates.
    """

    @abstractmethod
    async def get_latest_rates(self, base_currency: str) -> Dict[str, float]:
        """
        Fetches the latest exchange rates for a given base currency.

        Args:
            base_currency (str): The base currency for exchange rates.

        Returns:
            Dict[str, float]: A dictionary containing the latest exchange rates.
        """
        pass

    @abstractmethod
    async def get_historical_rates(self, start_date: str, base_currency: str) -> Dict[str, Dict[str, float]]:
        """
        Fetches historical exchange rates from a given start date up to today.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            base_currency (str): The base currency for exchange rates.

        Returns:
            Dict[str, Dict[str, float]]: A dictionary mapping dates to exchange rates.
        """
        pass


class OpenExchangeRatesProvider(ExchangeRateProvider):
    """
    Implementation of ExchangeRateProvider that fetches exchange rates from OpenExchangeRates API.
    """
    BASE_URL = "https://openexchangerates.org/api"

    def __init__(self, api_key: str):
        """
        Initializes the provider with the API key.

        Args:
            api_key (str): The API key for OpenExchangeRates.
        """
        if not api_key:
            raise ValueError("API key is required")
        self.api_key = api_key

    async def _fetch_data(self, session: aiohttp.ClientSession, endpoint: str, params: Dict[str, str]) -> Dict[
        str, Any]:
        """
        Helper method to fetch data from the API.

        Args:
            session (aiohttp.ClientSession): The active session for HTTP requests.
            endpoint (str): The API endpoint to fetch data from.
            params (Dict[str, str]): Query parameters for the request.

        Returns:
            Dict[str, Any]: The JSON response from the API.

        Raises:
            Exception: If the API request fails or the response format is invalid.
        """
        params["app_id"] = self.api_key
        async with session.get(f"{self.BASE_URL}/{endpoint}.json", params=params) as response:
            if response.status != 200:
                raise Exception(f"API request failed: {response.status}, {await response.text()}")

            try:
                data = await response.json()
                if "rates" not in data:
                    raise ValueError("Invalid response format: missing 'rates'")
                return data
            except Exception as e:
                raise ValueError(f"Error parsing API response: {str(e)}")

    async def get_latest_rates(self, base_currency: str = "USD") -> Dict[str, float]:
        """
        Fetches the latest exchange rates for a given base currency.

        Args:
            base_currency (str): The base currency for exchange rates.

        Returns:
            Dict[str, float]: A dictionary containing the latest exchange rates.

        Raises:
            Exception: If the API request fails.
        """
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, "latest", {"base": base_currency})

        return {datetime.datetime.today().strftime("%Y-%m-%d"): {base_currency: data["rates"]}}

    async def get_historical_rates(self, start_date: str, base_currency: str = "USD") -> Dict[str, Dict[str, float]]:
        """
        Fetches historical exchange rates from a given start date up to today.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            base_currency (str): The base currency for exchange rates.

        Returns:
            Dict[str, Dict[str, float]]: A dictionary mapping dates to exchange rates.

        Raises:
            ValueError: If the date format is incorrect or the start date is in the future.
            Exception: If the API request fails.
        """
        try:
            start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format. Expected YYYY-MM-DD")

        end_date_obj = datetime.datetime.today()
        if start_date_obj > end_date_obj:
            raise ValueError("Historical date cannot be in the future")

        async with aiohttp.ClientSession() as session:
            tasks = []
            dates = [(start_date_obj + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                     for i in range((end_date_obj - start_date_obj).days + 1)]

            for date in dates:
                tasks.append(self._fetch_data(session, f"historical/{date}", {"base": base_currency}))

            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                processed_results = {}
                for date, result in zip(dates, results):
                    if isinstance(result, Exception):
                        print(f"Error fetching data for {date}: {result}")
                        continue
                    processed_results[date] = {base_currency: result["rates"]}
                return processed_results
            except Exception as e:
                raise Exception(f"Error fetching historical exchange rates: {str(e)}")