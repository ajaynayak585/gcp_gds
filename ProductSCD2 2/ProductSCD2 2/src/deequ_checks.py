# deequ_checks.py
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

def run_quality_checks(spark, df):
    """
    Runs a handful of core DQ checks. Raises ValueError if any fail.
    """
    check = (
      Check(spark, CheckLevel.Error, "Data quality")
      .hasSize(lambda sz: sz > 0, "non_empty")
      .isComplete("product_id", "product_id_not_null")
      .isUnique("product_id", "product_id_unique")
    )

    product_check = (
      VerificationSuite(spark)
      .onData(df)
      .addCheck(check)
      .run()
    )

    dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, product_check)
    dq_check_df.show()
    
    if product_check.status != "Success":
        raise ValueError("Data Quality Checks Failed for Products Data")