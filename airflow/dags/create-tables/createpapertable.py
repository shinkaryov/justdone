"""
Airflow DAG to create a BigQuery table for storing paper data.

This DAG ensures that the `paper` table exists in the `justdone_analytics` dataset.
If the table does not exist, it will be created. After creation, a simple query checks
if the table exists.

DAG Tasks:
1. **create_table**: Runs a BigQuery query to create the table if it doesn't exist.
2. **check_table**: Runs a query to verify that the table exists.

BigQuery Table Schema:
- **Id**: Unique paper ID (STRING, NOT NULL)
- **Title**: Title of the paper (STRING)
- **PublishedAt**: Timestamp when the paper was published (TIMESTAMP)
- **Authors**: List of authors (ARRAY<STRING>)
- **ShortDescriptionEng**: Short description in English (STRING)
- **ShortDescriptionUkr**: Short description in Ukrainian (STRING)
- **Links**: Struct containing HTML and PDF links (STRUCT<HTML STRING, PDF STRING>)
- **Categories**: List of paper categories (ARRAY<STRING>)
- **LinkToTextEng**: Link to the extracted text in English (GCS) (STRING)
- **LinkToTextUkr**: Link to the extracted text in Ukrainian (GCS) (STRING)
- **Source**: Source of the paper (STRING)
- **CreatedAt**: Timestamp when the record was created (TIMESTAMP, default to `CURRENT_TIMESTAMP()`)

Partitioning:
- The table is partitioned by `PublishedAt` at the **year level** using `DATETIME_TRUNC(PublishedAt, YEAR)`.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# BigQuery dataset and table name
DATASET = "justdone_analytics"
TABLE = "paper"

# SQL query to create the table if it doesn't exist
CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS `{DATASET}.{TABLE}` (
    Id STRING NOT NULL,
    Title STRING,
    PublishedAt TIMESTAMP,
    Authors ARRAY<STRING>,
    ShortDescriptionEng STRING,
    ShortDescriptionUkr STRING,
    Links STRUCT<HTML STRING, PDF STRING>,
    Categories ARRAY<STRING>,
    LinkToTextEng STRING,
    LinkToTextUkr STRING,
    Source STRING,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATETIME_TRUNC(PublishedAt, YEAR);
"""

# Define the DAG
with DAG(
    "create_paper_table",
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
