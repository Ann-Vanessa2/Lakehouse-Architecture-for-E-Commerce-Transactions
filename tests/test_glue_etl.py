import sys
from unittest.mock import MagicMock

sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.transforms'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()

import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType, DateType
from glue_jobs.products_job import validate_schema, deduplicate, validate_nulls

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("GlueJobTests") \
        .getOrCreate()

# PRODUCTS Tests
def test_validate_schema_products(spark):
    data = [("1", "A", "Electronics", "Camera")]
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("department_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    validated_df = validate_schema(df, ["product_id", "department_id", "department", "product_name"])
    assert validated_df.count() == 1

def test_validate_nulls_products(spark):
    data = [("1", "A", "Electronics", "Camera"), (None, "B", "Books", "Notebook")]
    schema = ["product_id", "department_id", "department", "product_name"]
    df = spark.createDataFrame(data, schema)
    with pytest.raises(Exception, match="Nulls found in column product_id"):
        validate_nulls(df, ["product_id"])

def test_deduplicate_products(spark):
    data = [("1", "A"), ("1", "A")]
    schema = ["product_id", "department_id"]
    df = spark.createDataFrame(data, schema)
    deduped = deduplicate(df, ["product_id"])
    assert deduped.count() == 1

# ORDERS Tests
def test_validate_schema_orders(spark):
    data = [("1", "ORD001", "U1", "2024-01-01 12:00:00", 100.0, "2024-01-01")]
    schema = ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
    df = spark.createDataFrame(data, schema)
    validated_df = validate_schema(df, schema)
    assert validated_df.columns == schema

def test_validate_nulls_orders(spark):
    data = [("ORD001", "U1", None, 100.0, "2024-01-01")]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),  # explicitly declared as TimestampType
        StructField("total_amount", DoubleType(), True),
        StructField("date", StringType(), True),
    ])
    # schema = ["order_id", "user_id", "order_timestamp", "total_amount", "date"]
    df = spark.createDataFrame(data, schema)
    with pytest.raises(Exception, match="Nulls found in column order_timestamp"):
        validate_nulls(df, ["order_timestamp"])

# ORDER ITEMS Tests
def test_validate_schema_order_items(spark):
    schema = ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"]
    data = [("1", "ORD001", "U1", 5, "P123", 1, True, "2024-01-01 12:00:00", "2024-01-01")]
    df = spark.createDataFrame(data, schema)
    validated_df = validate_schema(df, schema)
    assert validated_df.count() == 1

def test_validate_nulls_order_items(spark):
    data = [("1", "ORD001", None, 5, "P123", 1, True, "2024-01-01 12:00:00", "2024-01-01")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", BooleanType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("date", StringType(), True),
    ])
    # schema = ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"]
    df = spark.createDataFrame(data, schema)
    with pytest.raises(Exception, match="Nulls found in column user_id"):
        validate_nulls(df, ["user_id"])

def test_deduplicate_order_items(spark):
    data = [("1", "ORD001"), ("1", "ORD001")]
    schema = ["id", "order_id"]
    df = spark.createDataFrame(data, schema)
    deduped = deduplicate(df, ["id"])
    assert deduped.count() == 1
