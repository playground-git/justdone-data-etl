import logging
from datetime import date
from typing import Optional

from google.cloud import bigquery

from src.models.exchange_rate import ExchangeRate

logger = logging.getLogger(__name__)


class BigQueryStorage:
    """Storage class for writing exchange rate data to BigQuery"""

    def __init__(
        self,
        project_id: str,
        dataset_id: str = "exchange_rates",
        table_id: str = "rates",
        client: Optional[bigquery.Client] = None,
    ):
        """Initialize BigQuery storage"""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        # Use provided client or create a new one
        self.client = (
            client if client is not None else bigquery.Client(project=project_id)
        )

        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"

        logger.info(f"Initialized BigQuery storage with table: {self.table_ref}")

    def save_exchange_rates(
        self, rates: list[ExchangeRate], replace_existing: bool = True
    ) -> int:
        """Save exchange rates to BigQuery"""
        if not rates:
            logger.warning("No rates provided to save")
            return 0

        try:
            # Convert exchange rates to JSON dictionaries (pydantic helps serialize date and datetime)
            import json

            rows = [json.loads(rate.model_dump_json()) for rate in rates]

            # If replacing existing data, delete matching records first
            if replace_existing:
                dates = set(rate.date.isoformat() for rate in rates)
                currencies = set(f"'{rate.target_currency}'" for rate in rates)

                delete_query = f"""
                delete from `{self.table_ref}`
                where date in ({', '.join(f"date('{d}')" for d in dates)})
                and target_currency in ({', '.join(currencies)})
                """

                self.client.query(delete_query).result()
                logger.info(
                    f"Deleted existing records for {len(dates)} dates and {len(currencies)} currencies if needed"
                )

            # Insert new rows
            errors = self.client.insert_rows_json(self.table_ref, rows)

            if errors:
                logger.error(f"Errors inserting rows: {errors}")
                raise ValueError(f"Failed to insert rows: {errors}")

            logger.info(f"Successfully saved {len(rows)} exchange rates to BigQuery")
            return len(rows)

        except Exception as e:
            logger.error(f"Error saving exchange rates: {str(e)}")
            raise

    def get_rates(
        self,
        date_value: date,
        base_currency: str = "USD",
        target_currencies: Optional[list[str]] = None,
    ) -> list[ExchangeRate]:
        """Get exchange rates for a specific date"""
        try:
            date_str = date_value.isoformat()

            query = f"""
            select *
            from `{self.table_ref}`
            where date = date('{date_str}')
            and base_currency = '{base_currency}'
            """

            if target_currencies:
                target_list = ", ".join(
                    f"'{currency}'" for currency in target_currencies
                )
                query += f" and target_currency in ({target_list})"

            query_run = self.client.query(query)
            results = query_run.result()

            rates = []
            for row in results:
                rates.append(
                    ExchangeRate(
                        date=row.date,
                        base_currency=row.base_currency,
                        target_currency=row.target_currency,
                        rate=row.rate,
                        source=row.source,
                        created_at=row.created_at,
                    )
                )

            logger.info(
                f"Retrieved {len(rates)} exchange rates for {date_str} from BigQuery"
            )
            return rates

        except Exception as e:
            logger.error(f"Error retrieving rates for {date_value}: {str(e)}")
            raise

    def get_rates_for_period(
        self,
        start_date: date,
        end_date: date,
        base_currency: str = "USD",
        target_currencies: Optional[list[str]] = None,
    ) -> dict[str, list[ExchangeRate]]:
        """Get exchange rates for a date range"""
        try:
            start_date_str = start_date.isoformat()
            end_date_str = end_date.isoformat()

            query = f"""
            select *
            from `{self.table_ref}`
            where date between date('{start_date_str}') and date('{end_date_str}')
            and base_currency = '{base_currency}'
            """

            if target_currencies:
                target_list = ", ".join(
                    f"'{currency}'" for currency in target_currencies
                )
                query += f" and target_currency in ({target_list})"

            query += " order by date, target_currency"

            query_run = self.client.query(query)
            results = query_run.result()

            # Group by date
            rates_by_date = {}
            for row in results:
                date_str = row.date.isoformat()

                if date_str not in rates_by_date:
                    rates_by_date[date_str] = []

                rates_by_date[date_str].append(
                    ExchangeRate(
                        date=row.date,
                        base_currency=row.base_currency,
                        target_currency=row.target_currency,
                        rate=row.rate,
                        source=row.source,
                        created_at=row.created_at,
                    )
                )

            total_rates = sum(len(rates) for rates in rates_by_date.values())
            logger.info(
                f"Retrieved {total_rates} exchange rates for period {start_date_str} to {end_date_str} "
                f"({len(rates_by_date)} dates) from BigQuery"
            )

            return rates_by_date

        except Exception as e:
            logger.error(
                f"Error retrieving rates for period {start_date} to {end_date}: {str(e)}"
            )
            raise
