# main.py
import argparse
from datetime import datetime

import spark_builder
import etl
import deequ_checks
import writer
import sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="yyyy_MM_dd to process", required=False)
    args = p.parse_args()

    proc_date = args.date or datetime.utcnow().strftime("%Y_%m_%d")

    spark = spark_builder.get_spark("Product Dimension SCD")
    # 1) Read
    df = etl.read_daily_data(spark, "gs://gds_test_ajay/", proc_date)
    # 2) Quality
    deequ_checks.run_quality_checks(spark, df)
    # 3) Sculpt SCD2
    df2 = etl.sculpt_scd2(df, proc_date)
    # 4) Stage
    writer.write_to_staging(df2, "project-afcbba29-4ec9-4335-86b", "test_db", "dim_products_staging")
    # 5) Merge
    writer.merge_scd2_bq(spark, "project-afcbba29-4ec9-4335-86b", "test_db", "dim_products_staging", "dim_products")
    # 6) Archive
    writer.archive_processed_csv("gds_test_ajay", proc_date)

    spark.stop()
    pass
    sys.exit(0)

if __name__ == "__main__":
    main()