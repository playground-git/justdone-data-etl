from datetime import date as date_type
from datetime import datetime, timezone
from typing import Optional, Type

from pydantic import BaseModel, Field


class ExchangeRate(BaseModel):
    """Pydantic model representing exchange rate record"""

    date: date_type
    base_currency: str = Field(min_length=3, max_length=3)
    target_currency: str = Field(min_length=3, max_length=3)
    rate: float = Field(gt=0)
    source: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

    class Config:
        json_encoders = {
            date_type: lambda d: d.isoformat(),
            datetime: lambda dt: dt.isoformat(),
        }

    @classmethod
    def from_api_response(
        cls: Type["ExchangeRate"],
        date_value: date_type,
        base_currency: str,
        currency_data: dict[str, float],
        source: str,
    ) -> list["ExchangeRate"]:
        """Create exchange rate objects from API response"""
        rates = []
        for target_currency, rate in currency_data.items():
            rates.append(
                cls(
                    date=date_value,
                    base_currency=base_currency,
                    target_currency=target_currency,
                    rate=rate,
                    source=source,
                )
            )
        return rates
