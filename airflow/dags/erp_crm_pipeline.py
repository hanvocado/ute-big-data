import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from minio import Minio

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1), 
    "end_date": datetime(2025, 8, 30),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "nguyenhoang692004@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Minio connection
minio_client = Minio(
    "minio:9000", access_key=AWS_ACCESS_KEY, secret_key=AWS_SECRET_KEY, secure=False
)

csv_urls = [
    'https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_crm/cust_info.csv',
    'https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_crm/prd_info.csv',
    'https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_crm/sales_details.csv',
    'https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_erp/CUST_AZ12.csv',
    'https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_erp/LOC_A101.csv',
    'https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/main/datasets/source_erp/PX_CAT_G1V2.csv',
]

path_to_local_base = "/opt/airflow/dags/files"
path_to_local_erp = os.path.join(path_to_local_base, "erp")
path_to_local_crm = os.path.join(path_to_local_base, "crm")

os.makedirs(path_to_local_erp, exist_ok=True)
os.makedirs(path_to_local_crm, exist_ok=True)

def download_file_from_url(url, local_path):
    print(f"Downloading {url} to {local_path}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise

def upload_file_to_minio(local_path, bucket_name, object_name):
    print(f"Uploading {local_path} to MinIO bucket {bucket_name} as {object_name}")
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        minio_client.fput_object(bucket_name, object_name, local_path)
        print(f"Successfully uploaded {local_path} to MinIO")
    except Exception as e:
        print(f"Error uploading {local_path} to MinIO: {e}")
        raise

def indicate_downloads_complete():
    print("All downloads are complete. Proceeding to uploads.")

with DAG(
    dag_id="erp_crm_pipeline",
    schedule_interval="@daily", 
    default_args=default_args,
    catchup=False,
    tags=["erp", "crm", "etl"],
) as erp_crm_dag:

    download_tasks = []
    upload_tasks = []

    for url in csv_urls:
        filename = os.path.basename(url)

        if "source_erp" in url:
            local_base = path_to_local_erp
            minio_dir = "erp"
        elif "source_crm" in url:
            local_base = path_to_local_crm
            minio_dir = "crm"
        else:
            print(f"Skipping unexpected URL format: {url}")
            continue 

        local_path = os.path.join(local_base, filename)
        object_name = f"bronze/{minio_dir}/{filename}"

        download_task = PythonOperator(
            task_id=f"download_{minio_dir}_{filename.replace('.', '_')}",
            python_callable=download_file_from_url,
            op_kwargs={
                "url": url,
                "local_path": local_path,
            },
        )

        upload_task = PythonOperator(
            task_id=f"upload_{minio_dir}_{filename.replace('.', '_')}_to_minio",
            python_callable=upload_file_to_minio,
            op_kwargs={
                "local_path": local_path,
                "bucket_name": "warehouse",
                "object_name": object_name,
            },
        )

        download_tasks.append(download_task)
        upload_tasks.append(upload_task)

    downloads_complete_task = PythonOperator(
        task_id="downloads_complete",
        python_callable=indicate_downloads_complete,
    )

    transform_bronze_to_silver = SparkSubmitOperator(
        task_id="transform_bronze_to_silver",
        application="/opt/airflow/dags/scripts/transform_bronze_to_silver.py", 
        name="transform_bronze_to_silver",
        conn_id="spark_conn",
        verbose=True,
        jars=(
            "/opt/airflow/dags/spark_jars/s3-2.31.36.jar,"
            "/opt/airflow/dags/spark_jars/iceberg-spark-runtime-3.2_2.12-1.4.3.jar,"
            "/opt/airflow/dags/spark_jars/iceberg-hive-runtime-1.4.3.jar"
        ),
        application_args=[
            "--year",
            "{{ logical_date.strftime('%Y') }}",
            "--month",
            "{{ logical_date.strftime('%m') }}",
        ],
    )

    load_silver_to_gold = SparkSubmitOperator(
        task_id="load_silver_to_gold",
        application="/opt/airflow/dags/scripts/load_silver_to_gold.py", 
        name="load_silver_to_gold",
        conn_id="spark_conn",
        verbose=True,
        jars=(
            "/opt/airflow/dags/spark_jars/s3-2.31.36.jar,"
            "/opt/airflow/dags/spark_jars/iceberg-spark-runtime-3.2_2.12-1.4.3.jar,"
            "/opt/airflow/dags/spark_jars/iceberg-hive-runtime-1.4.3.jar"
        ),
        application_args=[
            "--year",
            "{{ logical_date.strftime('%Y') }}",
            "--month",
            "{{ logical_date.strftime('%m') }}",
        ],
    )


    download_tasks >> downloads_complete_task

    downloads_complete_task >> upload_tasks

    upload_tasks >> transform_bronze_to_silver

    transform_bronze_to_silver >> load_silver_to_gold

