from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from src.clients.exchange_rates_data import ExchangeRatesDataClient
from src.config.logging import setup_logging
from src.models.exchange_rate import ExchangeRate
from src.storage.bigquery import BigQueryStorage

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "monthly_exchange_rates",
    default_args=default_args,
    description="Fetch monthly historical exchange rates and store in BigQuery",
    schedule_interval="0 1 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["exchange_rates", "monthly"],
)


def get_config():
    """Get configuration from Airflow variables"""
    return {
        "api_key": Variable.get("exchange_rates_api_key"),
        "base_currency": Variable.get("base_currency"),
        "target_currencies": Variable.get("target_currencies").split(","),
        "project_id": Variable.get("gcp_project_id"),
        "dataset_id": Variable.get("bq_dataset_id"),
        "table_id": Variable.get("bq_table_id"),
    }


def fetch_monthly_rates(**kwargs):
    """Fetch exchange rates for the previous month"""
    setup_logging()
    config = get_config()

    try:
        execution_date = kwargs.get("execution_date", datetime.now())

        # Calculate previous month date range
        current_month_first_day = execution_date.replace(day=1)
        previous_month_last_day = current_month_first_day - timedelta(days=1)
        previous_month_first_day = previous_month_last_day.replace(day=1)

        start_date = previous_month_first_day.date()
        end_date = previous_month_last_day.date()

        print(f"Fetching exchange rates for previous month: {start_date} to {end_date}")

        kwargs["ti"].xcom_push(
            key="date_range",
            value={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "month_name": previous_month_first_day.strftime("%B %Y"),
            },
        )

        client = ExchangeRatesDataClient(
            api_key=config["api_key"],
            base_currency=config["base_currency"],
        )

        rates_by_date = client.get_rates_for_period(
            start_date=start_date,
            end_date=end_date,
            symbols=config["target_currencies"],
        )

        all_exchange_rates_json = []
        for date_str, rates_data in rates_by_date.items():
            date_value = datetime.fromisoformat(date_str).date()
            exchange_rates = ExchangeRate.from_api_response(
                date_value=date_value,
                base_currency=config["base_currency"],
                currency_data=rates_data,
                source="ExchangeRatesData API",
            )
            all_exchange_rates_json.extend(
                [rate.model_dump_json() for rate in exchange_rates]
            )

        print(
            f"Successfully fetched {len(all_exchange_rates_json)} exchange rates for {previous_month_first_day.strftime('%B %Y')}"
        )
        return all_exchange_rates_json

    except Exception as e:
        import traceback

        error_message = (
            f"Error fetching monthly rates: {str(e)}\n{traceback.format_exc()}"
        )
        print(error_message)
        raise Exception(error_message)


def store_monthly_rates(**kwargs):
    """Store monthly exchange rates in BigQuery"""
    setup_logging()
    config = get_config()

    try:
        ti = kwargs["ti"]
        exchange_rates_json = ti.xcom_pull(task_ids="fetch_monthly_rates")
        date_range = ti.xcom_pull(task_ids="fetch_monthly_rates", key="date_range")

        if not exchange_rates_json:
            raise ValueError("No exchange rates data received from previous task")

        exchange_rates = [
            ExchangeRate.model_validate_json(rate_json)
            for rate_json in exchange_rates_json
        ]

        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = hook.get_client(project_id=config["project_id"])

        storage = BigQueryStorage(
            project_id=config["project_id"],
            dataset_id=config["dataset_id"],
            table_id=config["table_id"],
            client=client,
        )

        num_saved = storage.save_exchange_rates(exchange_rates)

        month_name = date_range.get("month_name", "previous month")
        return (
            f"Successfully saved {num_saved} exchange rates for {month_name} "
            f"({date_range['start_date']} to {date_range['end_date']}) to BigQuery"
        )

    except Exception as e:
        import traceback

        error_message = (
            f"Error storing monthly rates: {str(e)}\n{traceback.format_exc()}"
        )
        print(error_message)
        raise Exception(error_message)


fetch_rates_task = PythonOperator(
    task_id="fetch_monthly_rates",
    python_callable=fetch_monthly_rates,
    provide_context=True,
    dag=dag,
)

store_rates_task = PythonOperator(
    task_id="store_monthly_rates",
    python_callable=store_monthly_rates,
    provide_context=True,
    dag=dag,
)

fetch_rates_task >> store_rates_task
