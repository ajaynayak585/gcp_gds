from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
from google.cloud import bigquery
from google.cloud import storage
import logging,sys
from datetime import datetime

# Initialize Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def spark_session_builder():
    """
    Initializes a SparkSession with the given application name.
    """
    try:
        spark = SparkSession.builder \
            .appName('custom_gcs_bq_spark') \
            .getOrCreate()
        logger.info("Spark session initialized.")
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}")
        sys.exit(1)

def read_daily_data(spark, bucket_uri: str) -> DataFrame:
    try:
        logger.info(f"Reading daily data from GCS bucket: {bucket_uri}")
        """
        Reads products_{yyyy_MM_dd}.csv from GCS.
        """
        path = f"gs://{bucket_uri}/data/product_data/products_*.csv"
        return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
    except Exception as e:
        logger.error(f"Error reading daily data from GCS: {e}")
        sys.exit(1)

def process_for_scd2(df: DataFrame) -> DataFrame:
    """
    Add SCD-2 bookkeeping columns:
      - effective_start_date
      - effective_end_date (NULL for new rows)
      - is_current flag
    """
    try:
        logger.info(f"Processing DataFrame for SCD-2")

        return (
          df
          .withColumn("effective_start_date", col('effective_date').cast("date"))
          .withColumn("effective_end_date", lit("3000-01-01").cast("date"))
          .withColumn("is_current", lit(True))
        )
    except Exception as e:
        logger.error(f"Error processing DataFrame for SCD-2: {e}")
        sys.exit(1)

def write_to_staging(df, project, dataset, staging_table):
    """
    Overwrite the staging table in BigQuery.
    """

    df.select("product_id", "name", "category", "price", "supplier", "status","effective_start_date", "effective_end_date", "is_current").write \
      .format("bigquery") \
      .option("table", f"{project}.{dataset}.{staging_table}") \
      .option("temporaryGcsBucket", "gds_temp_test") \
      .mode("overwrite") \
      .save()


def merge_scd2_bq(project: str,dataset: str,staging_table: str,target_table: str):
    """
    Expire old SCD2 rows and upsert new/changed ones directly in BigQuery.
    """
    try:
        key = "product_id"
        cols = ["name", "category", "price", "supplier", "status"]
        on = f"T.{key} = S.{key} AND T.is_current = TRUE"
        changes = " OR ".join(f"T.{c} <> S.{c}" for c in cols)

        target_fqn = f"`{project}.{dataset}.{target_table}`"
        staging_fqn = f"`{project}.{dataset}.{staging_table}`"
        insert_cols = [key] + cols + ["effective_start_date", "effective_end_date", "is_current"]
        # insert_values = ", ".join(f"S.{c}" for c in insert_cols)

        merge_sql = f"""
        MERGE {target_fqn} AS T
        USING {staging_fqn} AS S
          ON {on}
        WHEN MATCHED AND ({changes}) THEN
          UPDATE SET 
            T.is_current = FALSE,
            T.effective_end_date = S.effective_start_date
        WHEN NOT MATCHED BY TARGET THEN
          INSERT ({', '.join(insert_cols)})
          VALUES ({', '.join(f'S.{c}' for c in insert_cols)})
        """

        logger.info(f"Running SCD-2 merge for target table: {project}.{dataset}.{target_table}")
        client = bigquery.Client(project=project)
        client.query_and_wait(merge_sql)
        logger.info("SCD-2 merge completed successfully.")
    except Exception as e:
        logger.error(f"Error during SCD-2 merge: {e}")
        sys.exit(1)
