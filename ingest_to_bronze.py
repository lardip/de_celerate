import datetime
from airflow import DAG
# --- [!! ADD THIS IMPORT !!] ---
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# --- [!! ADD THIS IMPORT !!] ---
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago

# ####################################################
# 1. SET YOUR VARIABLES
# ####################################################
GCP_PROJECT_ID = "fair-myth-477202-m8"
COMPOSER_GCS_BUCKET = "us-central1-composer-ecom-1d4c97ae-bucket"
SOURCE_DATA_PATH = "data/bronze_staging"
BRONZE_DATASET = "bronze"
DQ_LOG_DATASET = "data_quality_logs"
DQ_LOG_TABLE = f"{DQ_LOG_DATASET}.bronze_checks"

# ####################################################
# 2. DEFINE SQL FOR DATA QUALITY CHECKS
# ####################################################
dq_check_sql = f"""
BEGIN
  DECLARE rows_with_null_pk INT64;
  SET rows_with_null_pk = (
    SELECT COUNT(*)
    FROM `{BRONZE_DATASET}.{{{{ params.table_name }}}}`
    WHERE {{{{ params.pk_column }}}} IS NULL
  );
  INSERT INTO `{DQ_LOG_TABLE}`
  (log_timestamp, dag_id, table_checked, check_name, check_status, metric_value, execution_date)
  VALUES (
    CURRENT_TIMESTAMP(),
    '{{{{ dag.dag_id }}}}',
    '{{{{ params.table_name }}}}',
    'null_primary_key_check',
    CASE WHEN rows_with_null_pk = 0 THEN 'SUCCESS' ELSE 'FAIL' END,
    rows_with_null_pk,
    CAST('{{{{ ds }}}}' AS DATE)
  );
  IF rows_with_null_pk > 0 THEN
    RAISE USING MESSAGE = 'Data quality check failed: Found NULL primary keys in {{{{ params.table_name }}}}';
  END IF;
END;
"""

# ####################################################
# 3. DEFINE THE DAG
# ####################################################
with DAG(
    dag_id="ingest_to_bronze",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["project", "bronze", "ingestion"],
) as dag:

    # ####################################################
    # 4. DEFINE TASKS
    # ####################################################

    # --- Task Group: Customers ---
    wait_for_customers_csv = GCSObjectExistenceSensor(
        task_id="wait_for_customers_csv", bucket=COMPOSER_GCS_BUCKET, object=f"{SOURCE_DATA_PATH}/customers.csv"
    )
    load_customers_to_bronze = GCSToBigQueryOperator(
        task_id="load_customers_to_bronze",
        bucket=COMPOSER_GCS_BUCKET,
        source_objects=[f"{SOURCE_DATA_PATH}/customers.csv"],
        destination_project_dataset_table=f"{BRONZE_DATASET}.customers",
        source_format="CSV", skip_leading_rows=1, write_disposition="WRITE_TRUNCATE", autodetect=True,
    )
    check_bronze_customers = BigQueryInsertJobOperator(
        task_id="check_bronze_customers",
        configuration={"query": {"query": dq_check_sql, "useLegacySql": False}},
        params={"table_name": "customers", "pk_column": "customer_id"},
    )

    # --- Task Group: Orders ---
    wait_for_orders_csv = GCSObjectExistenceSensor(
        task_id="wait_for_orders_csv", bucket=COMPOSER_GCS_BUCKET, object=f"{SOURCE_DATA_PATH}/orders.csv"
    )
    load_orders_to_bronze = GCSToBigQueryOperator(
        task_id="load_orders_to_bronze",
        bucket=COMPOSER_GCS_BUCKET,
        source_objects=[f"{SOURCE_DATA_PATH}/orders.csv"],
        destination_project_dataset_table=f"{BRONZE_DATASET}.orders",
        source_format="CSV", skip_leading_rows=1, write_disposition="WRITE_TRUNCATE", autodetect=True,
    )
    check_bronze_orders = BigQueryInsertJobOperator(
        task_id="check_bronze_orders",
        configuration={"query": {"query": dq_check_sql, "useLegacySql": False}},
        params={"table_name": "orders", "pk_column": "order_id"},
    )

    # --- Task Group: Products ---
    wait_for_products_csv = GCSObjectExistenceSensor(
        task_id="wait_for_products_csv", bucket=COMPOSER_GCS_BUCKET, object=f"{SOURCE_DATA_PATH}/products.csv"
    )
    load_products_to_bronze = GCSToBigQueryOperator(
        task_id="load_products_to_bronze",
        bucket=COMPOSER_GCS_BUCKET,
        source_objects=[f"{SOURCE_DATA_PATH}/products.csv"],
        destination_project_dataset_table=f"{BRONZE_DATASET}.products",
        source_format="CSV", skip_leading_rows=1, write_disposition="WRITE_TRUNCATE", autodetect=True,
    )
    check_bronze_products = BigQueryInsertJobOperator(
        task_id="check_bronze_products",
        configuration={"query": {"query": dq_check_sql, "useLegacySql": False}},
        params={"table_name": "products", "pk_column": "product_ID"},
    )

    # --- Task Group: Sales ---
    wait_for_sales_csv = GCSObjectExistenceSensor(
        task_id="wait_for_sales_csv", bucket=COMPOSER_GCS_BUCKET, object=f"{SOURCE_DATA_PATH}/sales.csv"
    )
    load_sales_to_bronze = GCSToBigQueryOperator(
        task_id="load_sales_to_bronze",
        bucket=COMPOSER_GCS_BUCKET,
        source_objects=[f"{SOURCE_DATA_PATH}/sales.csv"],
        destination_project_dataset_table=f"{BRONZE_DATASET}.sales",
        source_format="CSV", skip_leading_rows=1, write_disposition="WRITE_TRUNCATE", autodetect=True,
    )
    check_bronze_sales = BigQueryInsertJobOperator(
        task_id="check_bronze_sales",
        configuration={"query": {"query": dq_check_sql, "useLegacySql": False}},
        params={"table_name": "sales", "pk_column": "sales_id"},
    )

    # --- [!! ADD THIS NEW TASK !!] ---
    # This task will trigger the silver DAG
    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="transform_to_silver",  # The ID of the DAG to run
        wait_for_completion=False,  # Don't wait, just trigger it
    )
    # --- [!! ADD THIS NEW TASK !!] ---

    # ####################################################
    # 5. SET TASK DEPENDENCIES
    # ####################################################
    
    # Run the customer flow
    wait_for_customers_csv >> load_customers_to_bronze >> check_bronze_customers
    # Run the orders flow
    wait_for_orders_csv >> load_orders_to_bronze >> check_bronze_orders
    # Run the products flow
    wait_for_products_csv >> load_products_to_bronze >> check_bronze_products
    # Run the sales flow
    wait_for_sales_csv >> load_sales_to_bronze >> check_bronze_sales

    # --- [!! CHANGE THE DEPENDENCIES !!] ---
    # After ALL 4 check tasks are successful, trigger the silver DAG
    [
        check_bronze_customers,
        check_bronze_orders,
        check_bronze_products,
        check_bronze_sales
    ] >> trigger_silver_dag
    # --- [!! CHANGE THE DEPENDENCIES !!] ---