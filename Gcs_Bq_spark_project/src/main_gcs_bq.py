import pyspark_etl_process
import argparse

def main():

    parser = argparse.ArgumentParser(description="Run PySpark ETL process for SCD-2.")
    parser.add_argument("--bucket_uri", help="GCS bucket URI")
    parser.add_argument("--bq_project", help="BigQuery project name")
    parser.add_argument("--bq_dataset", help="BigQuery dataset name")
    parser.add_argument("--bq_staging_table", help="BigQuery staging table name")
    parser.add_argument("--bq_final_table", help="BigQuery final table name")

    args = parser.parse_args()

    spark = pyspark_etl_process.spark_session_builder()

    bucket_uri = args.bucket_uri 
    bq_project = args.bq_project
    bq_dataset = args.bq_dataset
    bq_staging_table = args.bq_staging_table
    bq_final_table = args.bq_final_table

    # Read daily data
    df = pyspark_etl_process.read_daily_data(spark, bucket_uri)

    # Process for SCD-2
    df_scd2 = pyspark_etl_process.process_for_scd2(df)

    pyspark_etl_process.write_to_staging(df_scd2, bq_project, bq_dataset, bq_staging_table)

    pyspark_etl_process.merge_scd2_bq(bq_project, bq_dataset, bq_staging_table, bq_final_table)

    # Show the processed DataFrame (for demonstration purposes)
    df_scd2.show()

    spark.stop()

if __name__ == "__main__":
    main()