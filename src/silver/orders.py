# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date, xxhash64
from pyspark.sql.types import DecimalType

@dp.table
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .select(
            col("o_orderkey").alias("orderkey"),
            col("o_custkey").alias("custkey"),
            col("o_orderstatus").alias("orderstatus"),
            col("o_totalprice").cast(DecimalType(15, 2)).alias("totalprice"),
            to_date(col("o_orderdate")).alias("orderdate"),
            col("o_orderpriority").alias("orderpriority"),
            col("o_clerk").alias("clerk"),
            col("o_shippriority").alias("shippriority"),
            col("o_comment").alias("comment"),
            col("_source_file_path"),
            col("_source_file_modified_at"),
            col("_ingested_at"),
        )
        .withColumn("_processed_at", current_timestamp())
        .withColumn("_row_hash", xxhash64(
            col("orderkey"),      col("custkey"),       col("orderstatus"),
            col("totalprice"),    col("orderdate"),     col("orderpriority"),
            col("clerk"),         col("shippriority"),  col("comment")
        ))
    )