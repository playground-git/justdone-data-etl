import logging
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Optional, Union

import requests

logger = logging.getLogger(__name__)


class BaseExchangeRateClient(ABC):
    """Abstract base class for exchange rate API clients"""

    def __init__(self, api_key: str, base_currency: str = "USD"):
        """Initialize the exchange rate client"""
        self.api_key = api_key
        self.base_currency = base_currency
        self.session = requests.Session()
        logger.info(
            f"Initialized {self.__class__.__name__} with base currency {base_currency}"
        )

    @abstractmethod
    def get_latest_rates(self, symbols: Optional[list[str]] = None) -> dict[str, float]:
        """Get the latest exchange rates"""
        pass

    @abstractmethod
    def get_rates(
        self, date_str: Union[str, date], symbols: Optional[list[str]] = None
    ) -> dict[str, float]:
        """Get historical exchange rates for specific date"""
        pass

    def get_rates_for_period(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        symbols: Optional[list[str]] = None,
    ) -> dict[str, dict[str, float]]:
        """Get historical rates for date range"""
        # Convert date objects to strings if needed
        if isinstance(start_date, date):
            start_date = start_date.isoformat()
        if isinstance(end_date, date):
            end_date = end_date.isoformat()

        # Validate dates
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except ValueError:
            raise ValueError("Dates must be in YYYY-MM-DD format")

        if end < start:
            raise ValueError("End date must be after start date")

        result = {}
        current_date = start

        while current_date <= end:
            date_str = current_date.isoformat()
            try:
                rates = self.get_rates(date_str, symbols)
                result[date_str] = rates
                logger.info(f"Retrieved rates for {date_str}")
            except Exception as e:
                logger.error(f"Failed to retrieve rates for {date_str}: {str(e)}")

            current_date += timedelta(days=1)

        return result

    def _validate_response(self, response: requests.Response) -> dict:
        """Validate API response and return JSON data"""
        if response.status_code != 200:
            logger.error(
                f"API request failed with status code {response.status_code}: {response.text}"
            )
            response.raise_for_status()

        try:
            data = response.json()
        except ValueError:
            logger.error(f"Invalid JSON response: {response.text}")
            raise ValueError(
                f"Invalid JSON response from API: {response.text[:100]}..."
            )

        return data
