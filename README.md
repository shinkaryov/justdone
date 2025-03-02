# Data Pipeline for Research Paper Processing

## Overview

This project is implemented as a test task and consists of two main data pipelines:

1. **Research Paper Processing Pipeline**
   - Scrapes research papers from [arXiv](https://arxiv.org/).
   - Translates their content from English to Ukrainian using Google's Gemini API.
   - Stores the processed data in **Google Cloud Storage (GCS)** and **BigQuery (BQ)**.
   - Code is located in `airflow/dags/arxiv_to_bq_pipeline`.

2. **Exchange Rate Pipeline**
   - Retrieves exchange rates from an external API.
   - Stores the data in a **BigQuery table**.
   - Code is located in `airflow/dags/exchangerate_to_bq_pipeline`.

## Architectural Decisions

Considering the requirements of both pipelines, the following architectural choices were made:

- **Google Cloud Platform (GCP)** is used for infrastructure, leveraging **Google Cloud Storage (GCS), BigQuery (BQ), and Gemini API**.
- **Terraform** is used to provision and manage cloud resources.
- **GitHub Actions** is integrated for CI/CD, enabling:
  - **Terraform Plan**: Dry-run execution to preview changes.
  - **Terraform Apply**: Deployment on developer request.
- **Apache Airflow** is deployed **locally** to avoid unnecessary cloud deployment costs for the test task.

## Repository Structure

```
.
├── .github/workflows
│   ├── deploy.yml # Github Actions pipeline to provision cloud resources
├── airflow/
│   ├── dags/
│   │   ├── requirements.txt  # File with Python dependencies
│   ├── dags/
│   │   ├── arxiv_to_bq_pipeline/  # Research paper processing pipeline
│   │   │   ├── gcp_helper_functions # module to work with GCP resources
│   │   │   │   ├── __init__.py # File to initialize gcp_helper_functions as a Python module
│   │   │   │   ├── gcsuploader.py # Class to upload files to a GCS bucket
│   │   │   │   ├── llmtranslator.py # Class to translate text using LLM
│   │   │   ├── scrapers # module to scrape data
│   │   │   │   ├── __init__.py # File to initialize scrapers as a Python module
│   │   │   │   ├── pdfscraper.py # Class to transform PDF into text
│   │   │   │   ├── scraper.py # Class to scrap data from Arxiv API
│   │   │   ├── main.py # DAG which scrapes data from Arxiv API, transforms it and saves in GCS and BQ
│   │   ├── create tables/ #DAGs for table creation in BigQuery
│   │   │   ├── createexchangeratetable.py # DAG to create justdone_analytics.exchangeratetable (needed for Exchange Rate Pipeline)
│   │   │   ├── mcreatepapertable.py # DAG to create justdone_analytics.paper (needed for Research Paper Processing Pipeline)
│   │   ├── exchange_rate_pipeline/  # Exchange rate processing pipeline
│   │   │   ├── exchangeratehelper # module to work with Exchange Rate API
│   │   │   │   ├── __init__.py # File to initialize exchangeratehelper as a Python module
│   │   │   │   ├── exchangerateprovider.py # Class to work with Exchange Rate API
│   │   │   ├── backfill_exchangerate.py # DAG to backfill justdone_analytics.exchangeratetable table with historical values
│   │   │   ├── upd_exchangerate.py # DAG to insert into justdone_analytics.exchangeratetable table current values
│   ├── docker-compose.yml  # Local Airflow setup (webserver, scheduler postgres for metadata)
│   ├── .gitignore # Ignore extra Airflow files in commits
├── gcp_infra/  # Infrastructure as Code (GCS, BQ)
│   ├── main.tf # Creating of BQ dataset, GCS bucket
│   ├── output.tf # Output resources (nice to have as a best practice)
│   ├── provider.tf # Setup GCP provider
│   ├── variables.tf # Project Id and Region variables
├── .gitignore # Ignore files in commits
└── README.md  # Project documentation
```

## Naming conventions

1. **Folders** - snake_case
2. **Python files** - onelongwordfile.py (except backfill_smth.py, fill_smth.py, upd_smth.py, etc.)
3. **Python functions** - snake_case
4. **Python classes** - PascalCase
5. **Python variables** - snake_case
6. **Datasets** - snake_case
7. **Tables** - onelongwordtablename (singular)
8. **Columns** - PascalCase
9. **Airflow Variables** - UPPER_SNAKE_CASE
10. **Airflow DAGs** - snake_case
11. **Terraform resources and variables** - snake_case

## Deployment & Execution

1. **Fork and clone GitHub repository**
2. **Create GCP project**
   - Go to GCP console
   - Create new project in Cloud Resource Manager
   - Go to IAM & Admin → Service Accounts and create a Service Account
   - Give Editor permissions to Service Account
   - Load JSON key
   - Go to APIs & Services → Library and Enable BiqQuery API, IAM API, GCS API and Cloud Storage API
3. **Go to Github repo -> Settings -> Secrets and Variables -> Actions and create two secrets**
   - GCP_PROJECT_ID with a your GCP project id
   - SERVICE_ACCOUNT_KEY with contains of loaded JSON key
4. **Go to Github Actions and run Deploy Terraform to GCP pipeline using `apply` option**
5. Go to local repo
   - Create file airflow/.env file with following keys: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, AIRFLOW_FERNET_KEY, AIRFLOW_USER, AIRFLOW_PASSWORD, GCP_PROJECT_ID
   - Create folder airflow/config and put here a loaded JSON key with name `service-account.json`
   - Start Docker Daemon on a local machine
   - Navigate to airflow folder and run `docker compose up -d`
6. After successful build go to http://localhost:8080/ and login in Airflow UI
     - Go to Admin -> Connections and create following connection<img width="951" alt="image" src="https://github.com/user-attachments/assets/eec5ad22-aadd-4213-b117-f7f4de9f63df" />
     - Go to Admin -> Variables and create at least variables GCP_PROJECT_ID and OPEN_EXCHANGE_API_KEY (API key from https://openexchangerates.org)<img width="1259" alt="image" src="https://github.com/user-attachments/assets/639b02b7-2de5-41ee-8809-ac115d136c5b" />
7. **Run DAGs create_exchangerate_table and create_paer_table to create tables in BigQuery**
8. **Trigger DAGs from the Airflow UI or schedule them as needed.**<img width="1676" alt="image" src="https://github.com/user-attachments/assets/ffc6f03d-37ee-4563-8ecc-4a0af5204216" />

After successful runs of DAGs your BQ tables and GCS bucket will look like these:
<img width="1369" alt="image" src="https://github.com/user-attachments/assets/db0ec7ba-1aed-4d89-a16f-2bfa9dee6113" />
![telegram-cloud-photo-size-2-5344026828169408769-y](https://github.com/user-attachments/assets/b2ac9e1d-1d3c-429d-916d-48ab32b84ef3)
<img width="997" alt="image" src="https://github.com/user-attachments/assets/52d84b4f-0bda-47b0-bd0d-c06cdaf2b15c" />

## Project strenghts

- GitHub Actions integration: Automates infrastructure deployment.
- Advanced data modeling: Utilizes array and map types, SCD Type 2 for exchange rate table.
- Scalability & Modularity – The architecture can be easily extended to accommodate new data sources or processing steps.
- Cost-Efficient – Designed with GCP’s free tier in mind to minimize costs.
- Resilient Data Pipeline – Implements retries and error handling to ensure robustness.
- Efficient Data Processing – Uses asynchronous requests and batch processing for optimal performance.
- Cloud-Native Design – Fully leverages managed services (BigQuery, GCS) for reliability and ease of maintenance.
- Clear Logging & Monitoring – Airflow provides detailed execution logs and task monitoring.
- Reproducibility & Portability – The IaC and automated deployment approach make it easy to reproduce in different environments.

## Possible Improvements

- Deploying Airflow on **GCP (Cloud Composer)** for production readiness.
- Utilizing https://info.arxiv.org/help/bulk_data_s3.html fot **bulk data extraction** (noticed too late)
- Utilizing **Vector databases** for text saving
- Implementing more **data quality checks** before inserting data into BigQuery.
- Optimize **costs** by using efficient storage formats (e.g., Parquet).

---
This project demonstrates the ability to design and implement data pipelines with **scalability, automation, and cloud best practices** while balancing cost-effectiveness.

