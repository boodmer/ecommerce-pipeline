# Project: Ecommerce Pipeline
Developed and automated  data pipelines to ingest, transform, and generate daily revenue reports from PostgreSQL databases to a data lake HDFS and data warehouse HIVE using Pyspark and Apache Airflow.

# Run test

### Set up Postgres Database

set up postgres data and create users, user_details, orders, order_details, products, product_inventories table

### Set Env to env file

### Run Ingest

`python src/jobs/ingest.py --table_name=users --execution_date=2024-06-01`

### Run Transform

`python src/jobs/transform.py --execution_date=2024-06-01`

# DAGs Airflow

`src/flows/dag.py`
