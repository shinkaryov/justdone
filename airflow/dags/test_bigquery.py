from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.dates import days_ago

with DAG("test_bigquery", schedule=None, start_date=days_ago(1), catchup=False) as dag:
    test_bq = BigQueryCheckOperator(
        task_id="test_bq_connection",
        sql="SELECT 1",
        use_legacy_sql=False,
        gcp_conn_id="bigquery_default",
    )
