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
    "daily_exchange_rates",
    default_args=default_args,
    description="Fetch daily exchange rates and store in BigQuery",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exchange_rates", "daily"],
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


def fetch_daily_rates(**kwargs):
    """Fetch exchange rates for the previous day"""
    setup_logging()
    config = get_config()

    try:
        execution_date = kwargs["execution_date"]
        yesterday = (execution_date - timedelta(days=1)).date()

        kwargs["ti"].xcom_push(key="date", value=yesterday.isoformat())

        client = ExchangeRatesDataClient(
            api_key=config["api_key"],
            base_currency=config["base_currency"],
        )

        rates_data = client.get_rates(
            date_str=yesterday,
            symbols=config["target_currencies"],
        )

        exchange_rates = ExchangeRate.from_api_response(
            date_value=yesterday,
            base_currency=config["base_currency"],
            currency_data=rates_data,
            source="ExchangeRatesData API",
        )

        return [rate.model_dump_json() for rate in exchange_rates]

    except Exception as e:
        import traceback

        error_message = (
            f"Error fetching daily rates: {str(e)}\n{traceback.format_exc()}"
        )
        print(error_message)
        raise Exception(error_message)


def store_daily_rates(**kwargs):
    """Store exchange rates in BigQuery"""
    setup_logging()
    config = get_config()

    try:
        ti = kwargs["ti"]
        exchange_rates_json = ti.xcom_pull(task_ids="fetch_daily_rates")
        date_str = ti.xcom_pull(task_ids="fetch_daily_rates", key="date")

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

        return (
            f"Successfully saved {num_saved} exchange rates for {date_str} to BigQuery"
        )

    except Exception as e:
        import traceback

        error_message = f"Error storing daily rates: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        raise Exception(error_message)


fetch_rates_task = PythonOperator(
    task_id="fetch_daily_rates",
    python_callable=fetch_daily_rates,
    provide_context=True,
    dag=dag,
)

store_rates_task = PythonOperator(
    task_id="store_daily_rates",
    python_callable=store_daily_rates,
    provide_context=True,
    dag=dag,
)

fetch_rates_task >> store_rates_task
