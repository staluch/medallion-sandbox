# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table
def bronze_orders():
    volume_path = spark.conf.get("volume_path")

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{volume_path}/batch=*")
        .withColumn("_source_file_path",        col("_metadata.file_path"))
        .withColumn("_source_file_modified_at", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at",             current_timestamp())
        .drop("_metadata")
    )