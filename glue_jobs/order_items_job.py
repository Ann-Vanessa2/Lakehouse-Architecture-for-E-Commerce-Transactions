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
DATA_FOLDER = "raw-data"
ARCHIVED_DATA_FOLDER = "archive"
DELTA_LAKE_FOLDER = "lakehouse-dwh"

# Defining S3 folder paths
order_items_path = f"s3a://{S3_BUCKET_NAME}/{DATA_FOLDER}/order_items.csv"
order_items_lakehouse_path = f"s3a://{S3_BUCKET_NAME}/{DELTA_LAKE_FOLDER}/order_items"

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

    # --- Order_Item ETL Job ---
    # Read from S3
    df = spark.read.csv(order_items_path, header=True)
    
    # Validate schema
    required_columns = ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"]
    df = validate_schema(df, required_columns)
    
    # Check for null primary keys
    df = validate_nulls(df, ["id"])
    
    # Deduplicate
    df = deduplicate(df, ["id"])
    
    # Write to Delta
    write_delta(df, order_items_lakehouse_path, partition_by="date")
    
    # Perform Merge Upsert into Delta Lake
    merge_upsert(spark, order_items_lakehouse_path, df, primary_keys=["id"])
    
    logger.info("Order Items ETL job finished successfully.")

    job.commit()

if __name__ == "__main__":
    main()