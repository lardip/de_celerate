import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# ####################################################
# 1. SET YOUR VARIABLES
# ####################################################

GCP_PROJECT_ID = "fair-myth-477202-m8"
BRONZE_DATASET = "bronze"
SILVER_DATASET = "silver"

# --- [!! ADD THIS !!] ---
# Set this to the location of your BQ datasets (e.g., 'us-central1')
BQ_LOCATION = "us-central1"
# --- [!! ADD THIS !!] ---


# ####################################################
# 2. DEFINE SQL TRANSFORMATIONS
# ####################################################

# (SQL_CLEAN_CUSTOMERS, SQL_CLEAN_ORDERS, etc. are unchanged)

SQL_CLEAN_CUSTOMERS = f"""
CREATE OR REPLACE TABLE `{SILVER_DATASET}.customers` AS
SELECT
  CAST(customer_id AS INT64) AS customer_id,
  customer_name,
  gender,
  CAST(age AS INT64) AS age,
  home_address,
  CAST(zip_code AS INT64) AS zip_code,
  city,
  state,
  country
FROM
  `{BRONZE_DATASET}.customers`;
"""
SQL_CLEAN_ORDERS = f"""
CREATE OR REPLACE TABLE `{SILVER_DATASET}.orders` AS
SELECT
  CAST(order_id AS INT64) AS order_id,
  CAST(customer_id AS INT64) AS customer_id,
  CAST(payment AS INT64) AS payment,
  order_date,
  delivery_date
FROM
  `{BRONZE_DATASET}.orders`;
"""
SQL_CLEAN_PRODUCTS = f"""
CREATE OR REPLACE TABLE `{SILVER_DATASET}.products` AS
SELECT
  CAST(product_ID AS INT64) AS product_id,
  product_type,
  product_name,
  size,
  colour,
  CAST(price AS INT64) AS price,
  CAST(quantity AS INT64) AS inventory_quantity,
  description
FROM
  `{BRONZE_DATASET}.products`;
"""
SQL_CLEAN_SALES = f"""
CREATE OR REPLACE TABLE `{SILVER_DATASET}.sales` AS
SELECT
  CAST(sales_id AS INT64) AS sales_id,
  CAST(order_id AS INT64) AS order_id,
  CAST(product_id AS INT64) AS product_id,
  CAST(price_per_unit AS INT64) AS price_per_unit,
  CAST(quantity AS INT64) AS quantity,
  CAST(total_price AS INT64) AS total_price
FROM
  `{BRONZE_DATASET}.sales`;
"""

# ####################################################
# 3. DEFINE THE DAG
# ####################################################
with DAG(
    dag_id="transform_to_silver",
    start_date=days_ago(1),
    schedule_interval=None, # Triggered by bronze DAG
    catchup=False,
    tags=["project", "silver", "transform"],
) as dag:

    # ####################################################
    # 4. DEFINE TASKS
    # ####################################################

    clean_customers = BigQueryInsertJobOperator(
        task_id="clean_customers",
        configuration={
            "query": {
                "query": SQL_CLEAN_CUSTOMERS,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,  # <-- FIX IS HERE
    )

    clean_orders = BigQueryInsertJobOperator(
        task_id="clean_orders",
        configuration={
            "query": {
                "query": SQL_CLEAN_ORDERS,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,  # <-- FIX IS HERE
    )

    clean_products = BigQueryInsertJobOperator(
        task_id="clean_products",
        configuration={
            "query": {
                "query": SQL_CLEAN_PRODUCTS,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,  # <-- FIX IS HERE
    )

    clean_sales = BigQueryInsertJobOperator(
        task_id="clean_sales",
        configuration={
            "query": {
                "query": SQL_CLEAN_SALES,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,  # <-- FIX IS HERE
    )

    # --- [!! ADD THIS NEW TASK !!] ---
    # This task will trigger the gold DAG
    trigger_gold_dag = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="build_gold_analytics",  # The ID of the next DAG
        wait_for_completion=False,
    )
    # --- [!! ADD THIS NEW TASK !!] ---


    # ####################################################
    # 5. SET TASK DEPENDENCIES
    # ####################################################
    
    # Run all cleaning tasks in parallel
    [
        clean_customers,
        clean_orders,
        clean_products,
        clean_sales
    ] >> trigger_gold_dag # Then trigger the gold DAG