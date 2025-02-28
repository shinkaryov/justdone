"""
Airflow DAG to create a BigQuery table for storing exchange rates.

This DAG ensures that the `exchangerate` table exists in the `justdone_analytics` dataset.
If the table does not exist, it will be created. After creation, a simple query checks
if the table exists.

DAG Tasks:
1. **create_table**: Runs a BigQuery query to create the table if it doesn't exist.
2. **check_table**: Runs a query to verify that the table exists.

BigQuery Table Schema:
- **CurrencyFrom**: The base currency (STRING)
- **CurrencyTo**: The target currency (STRING)
- **Rate**: The exchange rate between the two currencies (NUMERIC)
- **EffectiveFrom**: Timestamp indicating when the rate becomes effective (TIMESTAMP)
- **EffectiveTo**: Timestamp indicating when the rate expires (TIMESTAMP)

This table is not partitioned.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# BigQuery dataset and table name
DATASET = "justdone_analytics"
TABLE = "exchangerate"

# SQL query to create the table if it doesn't exist
CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS `{DATASET}.{TABLE}` (
    CurrencyFrom STRING,
    CurrencyTo STRING,
    RATE NUMERIC,
    EffectiveFrom TIMESTAMP,
    EffectiveTo TIMESTAMP
);
"""

# Define the DAG
with DAG(
    "create_exchangerate_table",
    schedule=None,  # This DAG does not run on a schedule, it is triggered manually.
    start_date=days_ago(1),
    catchup=False,  # Ensures no backfill execution.
) as dag:

    # Task: Create the BigQuery table if it doesn't exist
    create_table = BigQueryInsertJobOperator(
        task_id="create_table",
        configuration={
            "query": {
                "query": CREATE_TABLE_QUERY,
                "useLegacySql": False,  # Uses Standard SQL
            }
        },
        gcp_conn_id="justdonegcloud",
    )

    # Task: Check if the table exists by running a simple SELECT query
    check_table = BigQueryInsertJobOperator(
        task_id="check_table_exists",
        configuration={
            "query": {
                "query": f"SELECT 1 FROM `{DATASET}.{TABLE}`",
                "useLegacySql": False,
            }
        },
        gcp_conn_id="justdonegcloud",
    )

    # Define task dependencies
    create_table >> check_table  # Ensure the table is created before checking its existence
