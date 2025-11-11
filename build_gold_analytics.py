import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# ####################################################
# 1. SET YOUR VARIABLES
# ####################################################

GCP_PROJECT_ID = "fair-myth-477202-m8"
SILVER_DATASET = "silver"
GOLD_DATASET = "gold"
DQ_LOG_DATASET = "data_quality_logs"

# The location of your BQ datasets (e.g., 'us-central1')
BQ_LOCATION = "us-central1"


# ####################################################
# 2. DEFINE SQL TRANSFORMATIONS
# ####################################################

# --- Build Dimension: dim_customer ---
# Create a clean, simple dimension table for customers.
SQL_BUILD_DIM_CUSTOMER = f"""
CREATE OR REPLACE TABLE `{GOLD_DATASET}.dim_customer` AS
SELECT
  customer_id,
  customer_name,
  gender,
  age,
  city,
  state,
  country
FROM
  `{SILVER_DATASET}.customers`;
"""

# --- Build Dimension: dim_product ---
# Create a clean, simple dimension table for products.
SQL_BUILD_DIM_PRODUCT = f"""
CREATE OR REPLACE TABLE `{GOLD_DATASET}.dim_product` AS
SELECT
  product_id,
  product_name,
  product_type,
  colour,
  size,
  price,
  inventory_quantity
FROM
  `{SILVER_DATASET}.products`;
"""

# --- Build Fact Table: fct_sales ---
# This is the main query. It joins all 4 silver tables
# into one wide, easy-to-query table for analytics.
SQL_BUILD_FCT_SALES = f"""
CREATE OR REPLACE TABLE `{GOLD_DATASET}.fct_sales` AS
SELECT
  s.sales_id,
  s.order_id,
  o.customer_id,
  p.product_id,
  -- Customer Details
  c.customer_name,
  c.city,
  c.state,
  -- Product Details
  p.product_name,
  p.product_type,
  -- Order & Date Details
  o.order_date,
  o.delivery_date,
  DATE_DIFF(o.delivery_date, o.order_date, DAY) AS shipping_time_days,
  -- Sales Metrics
  s.quantity,
  s.price_per_unit,
  s.total_price,
  o.payment
FROM
  `{SILVER_DATASET}.sales` AS s
LEFT JOIN
  `{SILVER_DATASET}.orders` AS o ON s.order_id = o.order_id
LEFT JOIN
  `{SILVER_DATASET}.products` AS p ON s.product_id = p.product_id
LEFT JOIN
  `{SILVER_DATASET}.customers` AS c ON o.customer_id = c.customer_id;
"""

# --- Data Quality Check for Gold ---
# Check if any key sales metrics are negative, which indicates a problem.
SQL_CHECK_GOLD_DATA = f"""
BEGIN
  DECLARE bad_rows INT64;

  SET bad_rows = (
    SELECT COUNT(*)
    FROM `{GOLD_DATASET}.fct_sales`
    WHERE total_price < 0 OR quantity < 0
  );

  INSERT INTO `{DQ_LOG_DATASET}.bronze_checks` -- We can reuse the same log table
  (log_timestamp, dag_id, table_checked, check_name, check_status, metric_value, execution_date)
  VALUES (
    CURRENT_TIMESTAMP(),
    '{{{{ dag.dag_id }}}}',
    'gold.fct_sales',
    'negative_sales_check',
    CASE WHEN bad_rows = 0 THEN 'SUCCESS' ELSE 'FAIL' END,
    bad_rows,
    CAST('{{{{ ds }}}}' AS DATE)
  );

  IF bad_rows > 0 THEN
    RAISE USING MESSAGE = 'Data quality check failed: Found negative sales or quantity in gold.fct_sales';
  END IF;
END;
"""

# ####################################################
# 3. DEFINE THE DAG
# ####################################################
with DAG(
    dag_id="build_gold_analytics",
    start_date=days_ago(1),
    schedule_interval=None, # Triggered by silver DAG
    catchup=False,
    tags=["project", "gold", "analytics"],
) as dag:

    # ####################################################
    # 4. DEFINE TASKS
    # ####################################################

    build_dim_customer = BigQueryInsertJobOperator(
        task_id="build_dim_customer",
        configuration={
            "query": {
                "query": SQL_BUILD_DIM_CUSTOMER,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,
    )

    build_dim_product = BigQueryInsertJobOperator(
        task_id="build_dim_product",
        configuration={
            "query": {
                "query": SQL_BUILD_DIM_PRODUCT,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,
    )

    build_fct_sales = BigQueryInsertJobOperator(
        task_id="build_fct_sales",
        configuration={
            "query": {
                "query": SQL_BUILD_FCT_SALES,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,
    )

    check_gold_data = BigQueryInsertJobOperator(
        task_id="check_gold_data",
        configuration={
            "query": {
                "query": SQL_CHECK_GOLD_DATA,
                "useLegacySql": False
            }
        },
        location=BQ_LOCATION,
    )


    # ####################################################
    # 5. SET TASK DEPENDENCIES
    # ####################################################
    
    # Build dimensions in parallel
    # Then build the fact table
    # Then run the quality check
    [
        build_dim_customer,
        build_dim_product
    ] >> build_fct_sales >> check_gold_data