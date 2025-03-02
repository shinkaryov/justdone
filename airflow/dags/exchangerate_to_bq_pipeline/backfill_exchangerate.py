from datetime import datetime, timedelta
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import sys
import os
import json

# Add necessary paths for module imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "exchangerate_to_bq_pipeline"))

from exchangeratehelper.exchangerateprovider import OpenExchangeRatesProvider

# Retrieve Airflow variables
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_DATASET = "justdone_analytics"
BQ_TABLE = "exchangerate"
BQ_CONN_ID = "justdonegcloud"
API_KEY = Variable.get("OPEN_EXCHANGE_API_KEY")
base_currency = Variable.get("BASE_CURRENCY", default_var="USD")
start_date = Variable.get("START_EXCHANGE_RATE_BACKFILL", default_var="2025-02-01")


def delete_old_rates(start_date: str, base_currency: str):
    """
    Deletes existing exchange rate records in BigQuery for the given base currency and date range.
    """
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
    client = hook.get_client()
    query = f"""
        DELETE FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE CurrencyFrom = '{base_currency}'
        AND EffectiveFrom >= '{start_date}'
    """
    client.query(query).result()


async def fetch_historical_rates(start_date: str, base_currency: str, ti):
    """
    Fetches historical exchange rates from OpenExchangeRates API starting from the given date.
    Stores the retrieved data in XCom for later processing.
    """
    provider = OpenExchangeRatesProvider(API_KEY)
    rates = await provider.get_historical_rates(start_date, base_currency)
    ti.xcom_push(key="rates", value=json.dumps(rates))


def insert_historical_rates(start_date: str, base_currency: str, **kwargs):
    """
    Inserts historical exchange rate data into BigQuery.
    Retrieves exchange rates from XCom and loads them into BigQuery after deleting outdated records.
    """
    ti = kwargs["ti"]
    rates_json = ti.xcom_pull(task_ids="fetch_historical_rates", key="rates")
    if not rates_json:
        raise ValueError("No exchange rate data found in XCom")

    rates = json.loads(rates_json)
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
    client = hook.get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # Delete old records before inserting new data
    delete_old_rates(start_date, base_currency)

    # Prepare data for insertion
    rows_to_insert = []
    for date, rate_data in rates.items():
        for currency, rate in rate_data[base_currency].items():
            rows_to_insert.append({
                "CurrencyFrom": base_currency,
                "CurrencyTo": currency,
                "RATE": round(rate, 6),
                "EffectiveFrom": date + ' 00:00:00',
                "EffectiveTo": date + ' 23:59:59'
            })

    # Insert data into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise Exception(f"BigQuery insert errors: {errors}")
    return f"Inserted {len(rows_to_insert)} rows into BigQuery"


# DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "backfill_exchangerate",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Fetches and backfills historical exchange rates into BigQuery."
) as dag:

    fetch_historical_rates_task = PythonOperator(
        task_id="fetch_historical_rates",
        python_callable=lambda **kwargs: asyncio.run(
            fetch_historical_rates(start_date, base_currency, kwargs["ti"])
        ),
        provide_context=True,
    )

    insert_historical_rates_task = PythonOperator(
        task_id="insert_historical_rates",
        python_callable=insert_historical_rates,
        provide_context=True,
        op_kwargs={"start_date": start_date, "base_currency": base_currency},
    )

    # Task dependencies
    fetch_historical_rates_task >> insert_historical_rates_task
