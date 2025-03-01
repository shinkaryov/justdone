"""
DAG for Extracting, Processing, and Loading ArXiv Papers into BigQuery

This DAG performs the following steps:
1. Scrapes ArXiv for the latest machine learning research papers.
2. Extracts and processes PDF texts, uploads them to Google Cloud Storage (GCS).
3. Uses Gemini AI for translation of texts and summaries into Ukrainian.
4. Loads processed data into a BigQuery table (`justdone_analytics.paper`).

Technologies used:
- **Airflow** for orchestration.
- **ArXiv API** for paper metadata extraction.
- **Google Cloud Storage (GCS)** for storing extracted texts.
- **Gemini AI** (Vertex AI) for translation.
- **BigQuery** for structured data storage.

DAG Schedule:
- Runs **daily** to fetch new papers.
- Implements **error handling and retries**.
"""

from datetime import datetime, timedelta
import json
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "arxiv_to_bq_pipeline"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from scrapers.pdfscraper import AsyncPDFScraper
from gcp_helper_functions.gcsuploader import GCSUploader
from scrapers.scraper import ArxivScraper
from gcp_helper_functions.llmtranslator import GeminiTranslator

# Constants
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BUCKET_NAME = f"{GCP_PROJECT_ID}-data-bucket"
BQ_DATASET = "justdone_analytics"
BQ_TABLE = "paper"
BQ_CONN_ID = "justdonegcloud"


async def process_papers_and_upload(papers):
    """
    Processes papers by extracting text from PDFs, translating them, and uploading results to GCS.

    Args:
        papers (list[dict]): List of paper metadata.

    Returns:
        list[dict]: Processed paper metadata with updated GCS links.
    """
    pdf_scraper = AsyncPDFScraper()
    uploader = GCSUploader(BUCKET_NAME)
    translator = GeminiTranslator(GCP_PROJECT_ID)

    pdf_texts = await asyncio.gather(*[
        pdf_scraper.get_paper_text(paper["Links"]["PDF"])
        for paper in papers if "PDF" in paper["Links"]
    ])

    for paper, text in zip(papers, pdf_texts):
        if text is None:
            paper["LinkToTextEng"] = None
            paper["LinkToTextUkr"] = None
            continue

        paper_id = paper["Id"].split("/")[-1]
        eng_file = f"{paper_id}_ENG.txt"
        ukr_file = f"{paper_id}_UKR.txt"

        uploader.upload_text(eng_file, text)
        paper["LinkToTextEng"] = f"https://storage.googleapis.com/{BUCKET_NAME}/{eng_file}"

        translated_text = await translator.translate(text, "en", "uk")
        if translated_text:
            uploader.upload_text(ukr_file, translated_text)
            paper["LinkToTextUkr"] = f"https://storage.googleapis.com/{BUCKET_NAME}/{ukr_file}"
        else:
            paper["LinkToTextUkr"] = None

    translated_summaries = await asyncio.gather(*[
        translator.translate(paper["ShortDescriptionEng"], "en", "uk")
        for paper in papers
    ])

    for paper, summary_translation in zip(papers, translated_summaries):
        paper["ShortDescriptionUkr"] = summary_translation

    return papers


def process_papers(**kwargs):
    """
    Scrapes ArXiv for recent machine learning papers and processes them.

    - Calls `ArxivScraper` to fetch papers.
    - Runs `process_papers_and_upload` asynchronously.
    - Stores processed papers in XCom for downstream tasks.

    Args:
        **kwargs: Airflow context variables.
    """
    scraper = ArxivScraper("http://export.arxiv.org/api/query", "machine learning")
    papers = scraper.get_papers(3)

    processed_papers = asyncio.run(process_papers_and_upload(papers))

    kwargs["ti"].xcom_push(key="papers", value=json.dumps(processed_papers))


def load_to_bq(**kwargs):
    """
    Loads processed paper metadata from XCom into BigQuery.

    - Retrieves processed papers from XCom.
    - Converts necessary fields to match BigQuery schema.
    - Inserts data into `justdone_analytics.paper`.

    Args:
        **kwargs: Airflow context variables.

    Raises:
        Exception: If BigQuery insertion fails.
    """
    ti = kwargs["ti"]
    papers = json.loads(ti.xcom_pull(task_ids="process_papers", key="papers"))

    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
    client = hook.get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    rows_to_insert = []
    for paper in papers:
        authors = paper["Authors"]
        if isinstance(authors, str):
            authors = authors.strip("[]").replace("'", "").split(", ")

        categories = paper["Categories"]
        if isinstance(categories, str):
            categories = categories.strip("[]").replace("'", "").split(", ")

        links = paper.get("Links", {})
        links_struct = {
            "HTML": links.get("HTML", ""),
            "PDF": links.get("PDF", "")
        }

        row = {
            "Id": paper["Id"],
            "Title": paper["Title"].replace("\n", " "),
            "PublishedAt": paper["PublishedAt"],
            "Authors": authors,
            "ShortDescriptionEng": paper.get("ShortDescriptionEng", "").replace("\n", " "),
            "ShortDescriptionUkr": paper.get("ShortDescriptionUkr", "").replace("\n", " "),
            "Links": links_struct,
            "Categories": categories,
            "LinkToTextEng": paper.get("LinkToTextEng", ""),
            "LinkToTextUkr": paper.get("LinkToTextUkr", ""),
            "Source": paper.get("Source", "")
        }

        rows_to_insert.append(row)

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise Exception(f"BigQuery insert errors: {errors}")
    return f"Successfully inserted {len(rows_to_insert)} papers into BigQuery"


# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "arxiv_to_bq_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    process_papers_task = PythonOperator(
        task_id="process_papers",
        python_callable=process_papers,
        provide_context=True,
    )

    load_to_bq_task = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
        provide_context=True,
    )

    process_papers_task >> load_to_bq_task
