from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator 
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 11, 13),
}

dag = DAG(
    'product_data_gcs_to_bq',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc ',
    schedule_interval='*/10 * * * *',
    catchup=False
)

pyspark_config = Variable.get("pyspark_etl_variables", deserialize_json=True)
bucket_uri = pyspark_config['bucket_uri']
bq_project = pyspark_config['bq_project']
bq_dataset = pyspark_config['bq_dataset']
bq_staging_table = pyspark_config['tables']['bq_staging_table']
bq_final_table = pyspark_config['tables']['bq_final_table']

# Fetch configuration from Airflow variables
cluster_config = Variable.get("dataproc_cluster_details", deserialize_json=True)
CLUSTER_NAME = cluster_config['dataproc_cluster_name']
PROJECT_ID = cluster_config['PROJECT_ID']
REGION = cluster_config['REGION']


#create bigquery job to run the pyspark job on dataproc cluster

create_bigquery_tables = BigQueryInsertJobOperator(
    task_id="create_bigquery_tables",
    configuration={
        "query": {
            "query": f"""
                -- Create staging table
                CREATE TABLE IF NOT EXISTS
                `{bq_project}.{bq_dataset}.{bq_staging_table}` (
                    product_id STRING,
                    name STRING,
                    category STRING,
                    price FLOAT64,
                    supplier STRING,
                    status STRING,
                    effective_start_date DATE,
                    effective_end_date DATE,
                    is_current BOOL
                );

                -- Create SCD Type 2 target table
                CREATE TABLE IF NOT EXISTS
                `{bq_project}.{bq_dataset}.{bq_final_table}` (
                    product_id STRING,
                    name STRING,
                    category STRING,
                    price FLOAT64,
                    supplier STRING,
                    status STRING,
                    effective_start_date DATE,
                    effective_end_date DATE,
                    is_current BOOL
                );
            """,
            "useLegacySql": False
        }
    },
    gcp_conn_id="google_cloud_default",
    dag=dag
)

#fetch data as soon as file found in gcs bucket 
file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=bucket_uri,
        object="data/product_data/products_*.csv",# Full file path in GCS
        use_glob=True,  
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",
        trigger_rule='all_success',  # Ensure this task runs only if the previous tasks are successful
        dag=dag  # Blocking mode
    )

def get_logs():
    
    logger.info("PYSPARK_JOB configuration: %s", PYSPARK_JOB)

log_task=PythonOperator(
    task_id='log_task',
    python_callable=get_logs,
    dag=dag,
)


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket_uri}/dags/spark-scripts/main_gcs_bq.py",
            "python_file_uris": [f"gs://{bucket_uri}/dags/spark-scripts/pyspark_etl_process.py"],
             "args": [
                 f"--bucket_uri={bucket_uri}",
                 f"--bq_project={bq_project}",
                 f"--bq_dataset={bq_dataset}",
                 f"--bq_staging_table={bq_staging_table}",
                 f"--bq_final_table={bq_final_table}"
             ]
                   }
}


submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
    trigger_rule='all_success',  # Ensure this task runs only if the previous tasks are successful
)

# Set the task dependencies
create_bigquery_tables>>file_sensor>>log_task>>submit_pyspark_job