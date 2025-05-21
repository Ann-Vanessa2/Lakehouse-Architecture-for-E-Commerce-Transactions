import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import *
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET_NAME = "lakehouse-e-commerce"
DATA_FOLDER = "raw-data/"
ARCHIVED_DATA_FOLDER = "archive/"
DELTA_LAKE_FOLDER = "lakehouse-dwh/"

# Defining S3 folder paths
products_path = f"s3a://{S3_BUCKET_NAME}/{DATA_FOLDER}/products.csv"
products_lakehouse_path = f"s3a://{S3_BUCKET_NAME}/{DELTA_LAKE_FOLDER}/products"

# Functions
def validate_schema(df, required_columns):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise Exception(f"Missing columns: {missing}")
    return df

def deduplicate(df, primary_keys):
    return df.dropDuplicates(primary_keys)

def validate_nulls(df, columns):
    for col in columns:
        for null_count in df.select(F.count(F.when(F.col(col).isNull(), col))).collect()[0]:
            if null_count > 0:
                raise Exception(f"Nulls found in column {col}")
    return df

def write_delta(df, output_path, partition_by=None):
    
    if partition_by:
        df.write.format("delta").mode("overwrite").partitionBy(partition_by).save(output_path)
    else:
        df.write.format("delta").mode("overwrite").save(output_path)

def merge_upsert(spark, delta_path, df, primary_keys):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, delta_path)
    cond = ' AND '.join([f"target.{pk} = source.{pk}" for pk in primary_keys])

    delta_table.alias("target").merge(
        source=df.alias("source"),
        condition=cond
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Read from S3
    p_df = spark.read.csv(products_path, header=True)
    
    # Validate schema
    required_columns = ["product_id", "department_id", "department", "product_name"]
    p_df = validate_schema(p_df, required_columns)
    
    # Check for null primary keys
    p_df = validate_nulls(p_df, ["product_id"])
    
    # Deduplicate
    p_df = deduplicate(p_df, ["product_id"])
    
    # Write to Delta
    write_delta(p_df, products_lakehouse_path, partition_by="department_id")
    
    # Perform Merge Upsert into Delta Lake
    merge_upsert(spark, products_lakehouse_path, p_df, primary_keys=["product_id"])
    
    logger.info("Products ETL job finished successfully.")

    job.commit()

if __name__ == "__main__":
    main()


# References
# https://spark.apache.org/docs/latest/sql-data-sources-delta.html
# https://github.com/delta-io/delta-rs
# https://github.com/delta-io/delta-rs/issues/123
# https://github.com/delta-io/delta-rs/issues/124

# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html
# https://docs.delta.io/latest/delta-intro.html
# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
