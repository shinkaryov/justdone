from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

DATASET = "justdone_analytics"
TABLE = "paper"

CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS `{DATASET}.{TABLE}` (
    Id STRING,
    Title STRING,
    Year INT64,
    Authors ARRAY<STRING>,
    ShortDescriptionEng STRING,
    ShortDescriptionUkr STRING,
    Links STRUCT<HTML STRING, PDF STRING>,
    Categories ARRAY<STRING>,
    LinkToTextEng STRING,
    LinkToTextUkr STRING,
    Source STRING,
    CreatedAt TIMESTAMP
);
"""

with DAG(
    "create_paper_table",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    create_table = BigQueryInsertJobOperator(
        task_id="create_table",
        configuration={
            "query": {
                "query": CREATE_TABLE_QUERY,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="justdonegcloud",
    )

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

    create_table >> check_table
