# writer.py
from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery

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

def merge_scd2_bq(
    spark: SparkSession,
    project: str,
    dataset: str,
    staging_table: str,
    target_table: str,
):
    """
    Expire old SCD2 rows and upsert new/changed ones directly in BigQuery.
    """
    key  = "product_id"
    cols = ["name","category","price","supplier","status"]
    on   = f"T.{key} = S.{key} AND T.is_current"
    changes = " OR ".join(f"T.{c} <> S.{c}" for c in cols)

    # merge_sql = f"""
    # MERGE `{project}.{dataset}.{target_table}` AS T
    # USING `{project}.{dataset}.{staging_table}` AS S
    #   ON {on}
    # WHEN MATCHED AND ({changes}) THEN
    #   UPDATE SET
    #     T.is_current = FALSE,
    #     T.effective_end_date = S.effective_start_date 
    # WHEN NOT MATCHED BY TARGET THEN
    #   INSERT ({key}, {', '.join(cols + ['effective_start_date','effective_end_date','is_current'])})
    #   VALUES ({', '.join('S.'+c for c in [key] + cols + ['effective_start_date','effective_end_date','is_current'] )})
    # """

    merge_sql = f"""
    MERGE `{project}.{dataset}.{target_table}` AS T
    USING `{project}.{dataset}.{staging_table}` AS S
      ON {on}
    WHEN MATCHED AND ({changes}) THEN
      UPDATE SET
        T.is_current = FALSE,
        T.effective_end_date = S.effective_start_date 
    """

    # kick off the job on BigQuery
    client = bigquery.Client(project=project)
    job    = client.query(merge_sql)
    job.result()  # wait for it to finish

    insert_query = f"""INSERT INTO `{project}.{dataset}.{target_table}` SELECT * FROM `{project}.{dataset}.{staging_table}`"""
    insert_staging_job = client.query(insert_query)
    insert_staging_job.result()  # wait for it to finish
    print(f"Merge completed: {job.job_id}")

# import subprocess

# def archive_processed_csv(bucket, date):
#     src = f"gs://{bucket}/products/input/products_{date}.csv"
#     dst = f"gs://{bucket}/products/archive/products_{date}.csv"
#     subprocess.check_call(["gsutil", "mv", src, dst])

def archive_processed_csv(bucket_name: str, proc_date: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    src = bucket.blob(f"input/products_{proc_date}.csv")
    dst_name = f"input/archive/products_{proc_date}.csv"
    bucket.copy_blob(src, bucket, dst_name)
    src.delete()