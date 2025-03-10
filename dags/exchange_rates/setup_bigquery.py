from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

from src.config.logging import setup_logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "setup_bigquery_dag",
    default_args=default_args,
    description="Set up BigQuery infrastructure for exchange rates data",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exchange_rates", "setup", "infrastructure"],
)


def get_config():
    """Get configuration from Airflow variables"""
    return {
        "project_id": Variable.get("gcp_project_id"),
        "dataset_id": Variable.get("bq_dataset_id"),
        "table_id": Variable.get("bq_table_id"),
        "location": Variable.get("gcp_location"),
    }


def create_dataset(**kwargs):
    """Create BigQuery dataset if it doesn't exist"""
    setup_logging()
    config = get_config()

    try:
        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = hook.get_client(project_id=config["project_id"])

        dataset = bigquery.Dataset(f"{config['project_id']}.{config['dataset_id']}")
        dataset.location = config["location"]

        dataset = client.create_dataset(dataset, exists_ok=True)

        return f"Dataset {dataset.dataset_id} created or already exists in {dataset.location}"

    except Exception as e:
        import traceback

        error_message = f"Error creating dataset: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        raise Exception(error_message)


def create_table(**kwargs):
    """Create BigQuery table with schema if it doesn't exist"""
    setup_logging()
    config = get_config()

    try:
        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = hook.get_client(project_id=config["project_id"])

        table_ref = (
            f"{config['project_id']}.{config['dataset_id']}.{config['table_id']}"
        )

        schema = [
            bigquery.SchemaField(
                "date", "DATE", description="Date of the exchange rate"
            ),
            bigquery.SchemaField(
                "base_currency", "STRING", description="Base currency code"
            ),
            bigquery.SchemaField(
                "target_currency", "STRING", description="Target currency code"
            ),
            bigquery.SchemaField("rate", "FLOAT", description="Exchange rate value"),
            bigquery.SchemaField(
                "source", "STRING", description="Source of the exchange rate data"
            ),
            bigquery.SchemaField(
                "created_at", "TIMESTAMP", description="When this record was created"
            ),
        ]

        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )

        table = client.create_table(table, exists_ok=True)

        return f"Table {table.project}.{table.dataset_id}.{table.table_id} created or already exists"

    except Exception as e:
        import traceback

        error_message = f"Error creating table: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        raise Exception(error_message)


create_dataset_task = PythonOperator(
    task_id="create_dataset",
    python_callable=create_dataset,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag,
)

create_dataset_task >> create_table_task
