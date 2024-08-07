from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage
import pandas as pd
import io
import csv
from datetime import datetime
from google.cloud import bigquery


# Define constants
bucket_name = 'ecom_raw_data_layer'
files_to_process = [
    {'input_blob_name': 'ecommerce/Customer.csv', 'processed_blob_name': 'ecommerce/processed_Customer.csv'},
    {'input_blob_name': 'ecommerce/Inventory.csv', 'processed_blob_name': 'ecommerce/processed_Inventory.csv'},
    {'input_blob_name': 'ecommerce/order.csv', 'processed_blob_name': 'ecommerce/processed_order.csv'},
    {'input_blob_name': 'ecommerce/order_item.csv', 'processed_blob_name': 'ecommerce/processed_order_item.csv'},
    {'input_blob_name': 'ecommerce/product.csv', 'processed_blob_name': 'ecommerce/processed_product.csv'},
]

project_id = 'testdataproc-382013'
stg_dataset_id = 'stg_ecom_data'
dwh_dataset_id = 'dwh_ecom_data'

# Define SQL queries for table creation
create_table_sql = {
    'customer_dim': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.customer_dim` (
            customer_id STRING,
            customer_name STRING,
            customer_email STRING,
            customer_phone STRING,
            start_date DATETIME,
            end_date DATETIME,
            current_flag BOOLEAN
        )
    """,
    'payment_method_dim': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.payment_method_dim` (
            payment_method_id STRING,
            payment_method STRING,
            start_date DATETIME,
            end_date DATETIME,
            current_flag BOOLEAN
        )
    """,
    'shipping_method_dim': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.shipping_method_dim` (
            shipping_method_id STRING,
            shipping_method STRING,
            start_date DATETIME,
            end_date DATETIME,
            current_flag BOOLEAN
        )
    """,
    'order_status_dim': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.order_status_dim` (
            order_status_id STRING,
            status STRING,
            start_date DATETIME,
            end_date DATETIME,
            current_flag BOOLEAN
        )
    """,
    'shipping_status_dim': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.shipping_status_dim` (
            shipping_status_id STRING,
            status STRING,
            start_date DATETIME,
            end_date DATETIME,
            current_flag BOOLEAN
        )
    """,
    'order_fact': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.order_fact` (
    order_fact_id STRING,
    order_id STRING,
    customer_id STRING,
    total_price FLOAT64,
    order_date DATETIME,
    delivery_date DATETIME,
    payment_method_id STRING,
    payment_date DATETIME,
    shipping_method_id STRING,
    shipping_status_id STRING,
    shipping_date DATETIME,
    shipping_address STRING,
    billing_address STRING,
    payment_status STRING,
    )
    PARTITION BY DATE(order_date);

        )
    """,
    'order_items_fact': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.order_items_fact` (
            order_id STRING,
            product_id STRING,
            order_quantity INT64,
            order_item_price FLOAT64
        )
    """,
    'inventory_fact': """
        CREATE TABLE `testdataproc-382013.dwh_ecom_data.inventory_fact` (
            inventory_id STRING,
            product_id STRING,
            inventory_quantity INT64,
            inventory_location_id STRING
        )
    """
}

update_insert_customer_dim = BigQueryExecuteQueryOperator(
        task_id='update_insert_customer_dim',
        sql="""
            -- Step 1: Update existing records
            UPDATE `testdataproc-382013.dwh_ecom_data.customer_dim` AS d
            SET
                d.customer_name = s.customer_name,
                d.customer_email = s.customer_email,
                d.customer_phone = s.customer_phone,
                d.end_date = CURRENT_DATE(),
                d.current_flag = FALSE
            FROM `testdataproc-382013.stg_ecom_data.stg_customer_data` AS s
            WHERE d.customer_id = s.customer_id
            AND d.current_flag = TRUE
            AND d.end_date IS NULL;

            -- Step 2: Insert new records
            INSERT INTO `testdataproc-382013.dwh_ecom_data.customer_dim` (
                customer_id,
                customer_name,
                customer_email,
                customer_phone,
                start_date,
                end_date,
                current_flag
            )
            SELECT
                s.customer_id,
                s.customer_name,
                s.customer_email,
                s.customer_phone,
                CURRENT_DATE() AS start_date,
                NULL AS end_date,
                TRUE AS current_flag
            FROM `testdataproc-382013.stg_ecom_data.stg_customer_data` AS s
            LEFT JOIN `testdataproc-382013.dwh_ecom_data.customer_dim` AS d
            ON s.customer_id = d.customer_id
            WHERE d.customer_id IS NULL OR (d.current_flag = FALSE);
        """,
        use_legacy_sql=False,
    )
    
update_insert_payment_method_dim = BigQueryExecuteQueryOperator(
        task_id='update_insert_payment_method_dim',
        sql="""      
        WITH new_payment_methods AS (
            SELECT DISTINCT o.order_payment_method
            FROM `testdataproc-382013.stg_ecom_data.stg_order_data` AS o
            LEFT JOIN `testdataproc-382013.dwh_ecom_data.payment_method_dim` AS d
            ON o.order_payment_method = d.payment_method
            WHERE d.payment_method IS NULL
        )
        
        INSERT INTO `testdataproc-382013.dwh_ecom_data.payment_method_dim` (
            payment_method_id,
            payment_method,
            start_date,
            end_date,
            current_flag
        )
        SELECT
            CONCAT(GENERATE_UUID(), "-", ns.order_payment_method) AS payment_method_id,
            ns.order_payment_method AS payment_method,
            CURRENT_DATE() AS start_date,
            NULL AS end_date,
            TRUE AS current_flag
        FROM new_payment_methods AS ns;

        """,
        use_legacy_sql=False,
)
update_insert_shipping_method_dim = BigQueryExecuteQueryOperator(
        task_id='update_insert_shipping_method_dim',
        sql="""

        WITH new_shipping_methods AS (
            SELECT DISTINCT s.order_shipping_method
            FROM `testdataproc-382013.stg_ecom_data.stg_order_data` AS s
            LEFT JOIN `testdataproc-382013.dwh_ecom_data.shipping_method_dim` AS d
            ON s.order_shipping_method = d.shipping_method
            WHERE d.shipping_method IS NULL
        )
        
        
        INSERT INTO `testdataproc-382013.dwh_ecom_data.shipping_method_dim` (
            shipping_method_id,
            shipping_method,
            start_date,
            end_date,
            current_flag
        )
        SELECT
            CONCAT(GENERATE_UUID(), '_', ns.order_shipping_method) AS shipping_method_id,
            ns.order_shipping_method AS shipping_method,
            CURRENT_DATE() AS start_date,
            NULL AS end_date,
            TRUE AS current_flag
        FROM new_shipping_methods AS ns;
        """,
        use_legacy_sql=False,
    )
update_insert_order_status_dim = BigQueryExecuteQueryOperator(
        task_id='update_insert_order_status_dim',
        sql="""

        WITH new_statuses AS (
            SELECT DISTINCT s.order_status
            FROM `testdataproc-382013.stg_ecom_data.stg_order_data` AS s
            LEFT JOIN `testdataproc-382013.dwh_ecom_data.order_status_dim` AS d
            ON s.order_status = d.status
            WHERE d.status IS NULL
        )
        
        
        INSERT INTO `testdataproc-382013.dwh_ecom_data.order_status_dim` (
            order_status_id,
            status,
            start_date,
            end_date,
            current_flag
        )
        SELECT
            CONCAT(GENERATE_UUID(), '_', ns.order_status) AS order_status_id,
            ns.order_status AS status,
            CURRENT_DATE() AS start_date,
            NULL AS end_date,
            TRUE AS current_flag
        FROM new_statuses AS ns;

        """,
        use_legacy_sql=False,
    )
update_insert_shipping_status_dim = BigQueryExecuteQueryOperator(
        task_id='update_insert_shipping_status_dim',
        sql="""
            WITH new_statuses AS (
                SELECT DISTINCT s.order_shipping_status
                FROM `testdataproc-382013.stg_ecom_data.stg_order_data` AS s
                LEFT JOIN `testdataproc-382013.dwh_ecom_data.shipping_status_dim` AS d
                ON s.order_shipping_status = d.status
                WHERE d.status IS NULL
            )
            
            INSERT INTO `testdataproc-382013.dwh_ecom_data.shipping_status_dim` (
                shipping_status_id,
                status,
                start_date,
                end_date,
                current_flag
            )
            SELECT
                CONCAT(GENERATE_UUID(), '_', ns.order_shipping_status) AS shipping_status_id,
                ns.order_shipping_status AS status,
                CURRENT_DATE() AS start_date,
                NULL AS end_date,
                TRUE AS current_flag
            FROM new_statuses AS ns;

        """,
        use_legacy_sql=False,
    )

update_insert_order_fact = """
    WITH extracted_data AS (
        SELECT
        s.order_id,
        c.customer_id,
        s.order_total_price AS total_price,
        CASE
            WHEN s.order_date IS NOT NULL AND s.order_date != 'nan'
            THEN SAFE.PARSE_DATETIME('%m/%d/%Y', s.order_date)
            ELSE NULL
        END AS order_date,
        CASE
            WHEN s.order_delivery_date IS NOT NULL AND s.order_delivery_date != 'nan'
            THEN SAFE.PARSE_DATETIME('%m/%d/%Y', s.order_delivery_date)
            ELSE NULL
        END AS delivery_date,
        p.payment_method_id,
        SAFE.PARSE_DATETIME('%m/%d/%Y', s.order_payment_date) AS payment_date,
        sm.shipping_method_id,
        ss.shipping_status_id,
        SAFE.PARSE_DATETIME('%m/%d/%Y', s.order_shipping_date) AS shipping_date
    FROM `testdataproc-382013.stg_ecom_data.stg_order_data` AS s
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.customer_dim` AS c
    ON s.order_customer_id = c.customer_id
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.payment_method_dim` AS p
    ON s.order_payment_method = p.payment_method
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.shipping_method_dim` AS sm
    ON s.order_shipping_method = sm.shipping_method
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.shipping_status_dim` AS ss
    ON s.order_shipping_status = ss.status
)


    UPDATE `testdataproc-382013.dwh_ecom_data.order_fact` AS fact
    SET
        fact.customer_id = ed.customer_id,
        fact.total_price = ed.total_price,
        fact.order_date = ed.order_date,
        fact.delivery_date = ed.delivery_date,
        fact.payment_method_id = ed.payment_method_id,
        fact.payment_date = ed.payment_date,
        fact.shipping_method_id = ed.shipping_method_id,
        fact.shipping_status_id = ed.shipping_status_id,
        fact.shipping_date = ed.shipping_date
        FROM extracted_data AS ed
        WHERE fact.order_id = ed.order_id;

    INSERT INTO `testdataproc-382013.dwh_ecom_data.order_fact` (
        order_fact_id,
        order_id,
        customer_id,
        total_price,
        order_date,
        delivery_date,
        payment_method_id,
        payment_date,
        shipping_method_id,
        shipping_status_id,
        shipping_date
    )
    SELECT
        ROW_NUMBER() OVER() AS order_fact_id,
        s.order_id,
        c.customer_id,
        s.order_total_price,
        CASE
        WHEN s.order_date != 'nan' AND s.order_date IS NOT NULL
        THEN SAFE.PARSE_DATETIME('%m/%d/%Y', s.order_date)
        ELSE NULL
    END AS order_date,
    CASE
      WHEN s.order_delivery_date != 'nan' AND s.order_delivery_date IS NOT NULL
        THEN SAFE.PARSE_DATETIME('%m/%d/%Y', s.order_delivery_date)
    ELSE NULL
  END AS order_date,
    p.payment_method_id,
    PARSE_DATETIME('%m/%d/%Y', s.order_payment_date) as order_payment_date,
    sm.shipping_method_id,
    ss.shipping_status_id,
    PARSE_DATETIME('%m/%d/%Y', s.order_shipping_date) as order_shipping_date,
    FROM `testdataproc-382013.stg_ecom_data.stg_order_data` AS s
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.customer_dim` AS c
    ON s.order_customer_id = c.customer_id
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.payment_method_dim` AS p
    ON s.order_payment_method = p.payment_method
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.order_status_dim` AS os
    ON s.order_status = os.status
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.shipping_method_dim` AS sm
    ON s.order_shipping_method = sm.shipping_method
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.shipping_status_dim` AS ss
    ON s.order_shipping_status = ss.status
;

"""

update_insert_order_items_fact_query = """
    WITH extracted_data AS (
        SELECT
            s.order_id,
            s.product_id,
            s.order_quantity,
            s.order_item_price
        FROM `testdataproc-382013.stg_ecom_data.stg_order_items_data` AS s
    )
    
    UPDATE `testdataproc-382013.dwh_ecom_data.order_items_fact` AS f
    SET
        f.order_id = ed. order_id,
        f.product_id = ed.product_id,
        f.order_quantity = ed.order_quantity,
        f.order_item_price = ed.order_item_price
    FROM extracted_data AS ed
    WHERE f.order_id = ed.order_id
    AND f.product_id = ed.product_id;

    INSERT INTO `testdataproc-382013.dwh_ecom_data.order_items_fact` (
        order_id,
        product_id,
        order_quantity,
        order_item_price
    )
    SELECT
    ed.order_id,
    ed.product_id,
    ed.order_quantity,
    ed.order_item_price
    FROM extracted_data AS ed
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.order_items_fact` AS f
    ON ed.order_id = f.order_id AND ed.product_id = f.product_id
    WHERE f.order_id IS NULL;
"""
update_insert_inventory_fact_query = """

    WITH extracted_data AS (
        SELECT
            s.inventory_id,
            s.product_id,
            s.inventory_quantity,
            s.inventory_location_id
        FROM `testdataproc-382013.stg_ecom_data.stg_inventory_data` AS s
    )

    UPDATE `testdataproc-382013.dwh_ecom_data.inventory_fact` AS f
    SET
        f.inventory_quantity = ed.inventory_quantity,
        f.inventory_location_id = ed.inventory_location_id
    FROM extracted_data AS ed
    WHERE f.inventory_id = ed.inventory_id
    AND f.product_id = ed.product_id;
    
    INSERT INTO `testdataproc-382013.dwh_ecom_data.inventory_fact` (
        inventory_id,
        product_id,
        inventory_quantity,
        inventory_location_id
    )
    SELECT
        ed.inventory_id,
        ed.product_id,
        ed.inventory_quantity,
        ed.inventory_location_id
    FROM extracted_data AS ed
    LEFT JOIN `testdataproc-382013.dwh_ecom_data.inventory_fact` AS f
    ON ed.inventory_id = f.inventory_id
    AND ed.product_id = f.product_id
    WHERE f.inventory_id IS NULL;
"""


def process_and_upload_csv():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    for file_info in files_to_process:
        input_blob_name = file_info['input_blob_name']
        processed_blob_name = file_info['processed_blob_name']
        
        input_blob = bucket.blob(input_blob_name)
        csv_data = input_blob.download_as_text()
        
        df = pd.read_csv(io.StringIO(csv_data), delimiter=',', quotechar='"', escapechar='\\', engine='python')
        
        for column in df.columns:
            df[column] = df[column].astype(str).str.replace(r'\n', ' ', regex=True)
        
        df['dw_created_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df = df.applymap(lambda x: None if pd.isna(x) else x)
        
        output_csv_data = df.to_csv(sep='|', index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
        
        output_blob = bucket.blob(processed_blob_name)
        output_blob.upload_from_string(output_csv_data, content_type='text/csv')

# Function to check and create table
def check_and_create_table(table_name):
    print("Table name:", table_name)
    client = bigquery.Client()
    dataset_id = 'dwh_ecom_data'
    project_id = 'testdataproc-382013'
    
    query = f"""
    SELECT COUNT(*)
    FROM testdataproc-382013.dwh_ecom_data.INFORMATION_SCHEMA.TABLES
    WHERE table_name = '{table_name}'
    """
    
    results = client.query(query).result()
    
    # Assume the table does not exist
    table_exists = False
    
    for row in results:
        # If count is greater than 0, the table exists
        if row[0] > 0:
            table_exists = True
            print(f"Table '{table_name}' already exists.")
            break  # Exit the loop since we've found the result
    
    if not table_exists:
        # Table does not exist, so create it
        create_sql = create_table_sql[table_name]
        client.query(create_sql).result()
        print(f"Table '{table_name}' created successfully.")



# Define the DAG
with DAG(
    'ecommerce_etl',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='DAG to process CSV from GCS and load into BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to process and upload the CSV
    process_csv_task = PythonOperator(
        task_id='process_and_upload_csv',
        python_callable=process_and_upload_csv
    )
    
    # Task to check and create tables
    check_create_customer_dim_table_task = PythonOperator(
        task_id='check_create_customer_dim_table',
        python_callable=check_and_create_table,
        op_args=['customer_dim']
    )

    check_create_payment_method_dim_table_task = PythonOperator(
        task_id='check_create_payment_method_dim_table',
        python_callable=check_and_create_table,
        op_args=['payment_method_dim']
    )

    check_create_shipping_method_dim_table_task = PythonOperator(
        task_id='check_create_shipping_method_dim_table',
        python_callable=check_and_create_table,
        op_args=['shipping_method_dim']
    )

    check_create_order_status_dim_table_task = PythonOperator(
        task_id='check_create_order_status_dim_table',
        python_callable=check_and_create_table,
        op_args=['order_status_dim']
    )

    check_create_shipping_status_dim_table_task = PythonOperator(
        task_id='check_create_shipping_status_dim_table',
        python_callable=check_and_create_table,
        op_args=['shipping_status_dim']
    )

    check_create_order_fact_table_task = PythonOperator(
        task_id='check_create_order_fact_table',
        python_callable=check_and_create_table,
        op_args=['order_fact']
    )

    check_create_order_items_fact_table_task = PythonOperator(
        task_id='check_create_order_items_fact_table',
        python_callable=check_and_create_table,
        op_args=['order_items_fact']
    )

    check_create_inventory_fact_table_task = PythonOperator(
        task_id='check_create_inventory_fact_table',
        python_callable=check_and_create_table,
        op_args=['inventory_fact']
    )
    
    update_insert_order_fact = BigQueryExecuteQueryOperator(
    task_id='update_insert_order_fact',
    sql=update_insert_order_fact,
    use_legacy_sql=False,
    dag=dag,
    )
    
    update_insert_order_items_fact = BigQueryExecuteQueryOperator(
    task_id='update_insert_order_items_fact',
    sql=update_insert_order_items_fact_query,
    use_legacy_sql=False,
    dag=dag,
    )
    
    # Define the BigQueryExecuteQueryOperator task
    update_insert_inventory_fact = BigQueryExecuteQueryOperator(
    task_id='update_insert_inventory_fact',
    sql=update_insert_inventory_fact_query,
    use_legacy_sql=False,
    dag=dag,
)
    
    # Task to load the processed file into BigQuery
    load_to_bq_task1 = GCSToBigQueryOperator(
        task_id='load_to_bigquery1',
        bucket=bucket_name,
        source_objects=['ecommerce/processed_order.csv'],
        destination_project_dataset_table=f'{project_id}.{stg_dataset_id}.stg_order_data',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=False,
        schema_object='schema/order.json',
        field_delimiter='|',
        write_disposition='WRITE_TRUNCATE'
    )
    
    load_to_bq_task2 = GCSToBigQueryOperator(
        task_id='load_to_bigquery2',
        bucket=bucket_name,
        source_objects=['ecommerce/processed_Customer.csv'],
        destination_project_dataset_table=f'{project_id}.{stg_dataset_id}.stg_customer_data',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=False,
        schema_object='schema/customer.json',
        field_delimiter='|',
        write_disposition='WRITE_TRUNCATE'
    )
    
    load_to_bq_task3 = GCSToBigQueryOperator(
        task_id='load_to_bigquery3',
        bucket=bucket_name,
        source_objects=['ecommerce/processed_order_item.csv'],
        destination_project_dataset_table=f'{project_id}.{stg_dataset_id}.stg_order_item_data',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=False,
        schema_object='schema/order_item.json',
        field_delimiter='|',
        write_disposition='WRITE_TRUNCATE'
    )
    
    load_to_bq_task4 = GCSToBigQueryOperator(
        task_id='load_to_bigquery4',
        bucket=bucket_name,
        source_objects=['ecommerce/processed_product.csv'],
        destination_project_dataset_table=f'{project_id}.{stg_dataset_id}.stg_product_data',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=False,
        schema_object='schema/product.json',
        field_delimiter='|',
        write_disposition='WRITE_TRUNCATE'
    )
    
    load_to_bq_task5 = GCSToBigQueryOperator(
        task_id='load_to_bigquery5',
        bucket=bucket_name,
        source_objects=['ecommerce/processed_Inventory.csv'],
        destination_project_dataset_table=f'{project_id}.{stg_dataset_id}.stg_inventory_data',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=False,
        schema_object='schema/inventory.json',
        field_delimiter='|',
        write_disposition='WRITE_TRUNCATE'
    )
    process_csv_task >> load_to_bq_task1 >> load_to_bq_task2 >> load_to_bq_task3 >> load_to_bq_task4 >> load_to_bq_task5
    load_to_bq_task5 >> check_create_customer_dim_table_task >> check_create_payment_method_dim_table_task >> check_create_shipping_method_dim_table_task >> check_create_order_status_dim_table_task >> check_create_shipping_status_dim_table_task
    check_create_shipping_status_dim_table_task >> check_create_order_fact_table_task >> check_create_order_items_fact_table_task >> check_create_inventory_fact_table_task
    check_create_inventory_fact_table_task >> update_insert_customer_dim >> update_insert_payment_method_dim >> update_insert_shipping_method_dim >> update_insert_order_status_dim  >> update_insert_shipping_status_dim >> update_insert_order_fact >> update_insert_order_items_fact