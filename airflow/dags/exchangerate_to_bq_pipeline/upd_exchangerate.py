from datetime import datetime, timedelta
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "exchangerate_to_bq_pipeline"))

from exchangeratehelper.exchangerateprovider import OpenExchangeRatesProvider  # Імпортуємо твій клас

# Константи
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_DATASET = "justdone_analytics"
BQ_TABLE = "exchangerate"
BQ_CONN_ID = "justdonegcloud"
API_KEY = Variable.get("OPEN_EXCHANGE_API_KEY")
base_currency = Variable.get("BASE_CURRENCY") if Variable.get("BASE_CURRENCY") else "USD"


def fetch_and_store_exchange_rates(**kwargs):
    """
    Отримує останні курси валют та зберігає їх у BigQuery.
    """
    provider = OpenExchangeRatesProvider(API_KEY)

    # Використовуємо asyncio для асинхронного виклику
    latest_rates = asyncio.run(provider.get_latest_rates(base_currency))

    # Перетворення у формат для BigQuery
    rows_to_insert = []
    fetch_date = datetime.utcnow()

    for currency_to, rate in latest_rates[fetch_date.strftime("%Y-%m-%d")][base_currency].items():
        row = {
            "CurrencyFrom": base_currency,
            "CurrencyTo": currency_to,
            "RATE": round(rate, 6),
            "EffectiveFrom": fetch_date.strftime("%Y-%m-%d") + ' 00:00:00',
            "EffectiveTo": None  # В цьому прикладі EffectiveTo залишається NULL
        }
        rows_to_insert.append(row)

    # Інсерт у BigQuery
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
    client = hook.get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors:
        raise Exception(f"Помилка вставки в BigQuery: {errors}")

    return f"Inserted {len(rows_to_insert)} rows into {BQ_TABLE}"


# Налаштування DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        "upd_exchangerate",
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False,
) as dag:
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_exchange_rates",
        python_callable=fetch_and_store_exchange_rates,
        provide_context=True,
    )