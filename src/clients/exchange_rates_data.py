import logging
from datetime import date
from typing import Optional, Union

import requests

from .base import BaseExchangeRateClient

logger = logging.getLogger(__name__)


class ExchangeRatesDataClient(BaseExchangeRateClient):
    """Client for ExchangeRates Data API from APILayer"""

    BASE_URL = "https://api.apilayer.com/exchangerates_data"

    def __init__(self, api_key: str, base_currency: str = "USD"):
        """Initialize ExchangeRates Data API client"""
        super().__init__(api_key, base_currency)
        self.headers = {"apikey": self.api_key, "Content-Type": "application/json"}
        logger.info("Initialized ExchangeRates Data API client")

    def get_latest_rates(self, symbols: Optional[list[str]] = None) -> dict[str, float]:
        """Get the latest exchange rates from ExchangeRates Data API"""
        url = f"{self.BASE_URL}/latest"

        params = {"base": self.base_currency}
        if symbols:
            params["symbols"] = ",".join(symbols)

        try:
            response = self.session.get(url, headers=self.headers, params=params)
            data = self._validate_response(response)

            # Validate response structure
            if not data.get("success"):
                error_info = data.get("error", {})
                error_code = error_info.get("code", "unknown")
                error_message = error_info.get("info", "Unknown error")
                raise ValueError(f"API error {error_code}: {error_message}")

            if "rates" not in data:
                raise ValueError("Missing 'rates' in API response")

            rates = data["rates"]
            logger.info(f"Retrieved latest rates for {len(rates)} currencies")
            return rates

        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    def get_rates(
        self, date_str: Union[str, date], symbols: Optional[list[str]] = None
    ) -> dict[str, float]:
        """Get historical exchange rates for specific date from ExchangeRates Data API"""
        if isinstance(date_str, date):
            date_str = date_str.isoformat()

        url = f"{self.BASE_URL}/{date_str}"

        params = {"base": self.base_currency}
        if symbols:
            params["symbols"] = ",".join(symbols)

        try:
            response = self.session.get(url, headers=self.headers, params=params)
            data = self._validate_response(response)

            if not data.get("success"):
                error_info = data.get("error", {})
                error_code = error_info.get("code", "unknown")
                error_message = error_info.get("info", "Unknown error")
                raise ValueError(f"API error {error_code}: {error_message}")

            if "rates" not in data:
                raise ValueError("Missing 'rates' in API response")

            rates = data["rates"]
            logger.info(
                f"Retrieved historical rates for {date_str} ({len(rates)} currencies)"
            )
            return rates

        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    def get_rates_for_period(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        symbols: Optional[list[str]] = None,
    ) -> dict[str, dict[str, float]]:
        """Get historical rates for date range using timeseries endpoint"""
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

        url = f"{self.BASE_URL}/timeseries"

        params = {
            "base": self.base_currency,
            "start_date": start_date,
            "end_date": end_date,
        }

        if symbols:
            params["symbols"] = ",".join(symbols)

        try:
            response = self.session.get(url, headers=self.headers, params=params)
            data = self._validate_response(response)

            if not data.get("success"):
                error_info = data.get("error", {})
                error_code = error_info.get("code", "unknown")
                error_message = error_info.get("info", "Unknown error")
                raise ValueError(f"API error {error_code}: {error_message}")

            if "rates" not in data:
                raise ValueError("Missing 'rates' in API response")

            rates = data["rates"]
            date_count = len(rates)
            currency_count = len(next(iter(rates.values()))) if rates else 0

            logger.info(
                f"Retrieved rates for {date_count} dates and {currency_count} currencies"
            )
            return rates

        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
