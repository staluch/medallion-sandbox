# Databricks notebook source
# =============================================================================
# src/bronze/orders.py
# Auto Loader: parquet z Volume → bronze_orders (Delta, append)
# Trigger: availableNow – przetwarza nowe pliki od ostatniego runu, potem kończy.
# =============================================================================

dbutils.widgets.text("catalog", "dev_medallion", "Target catalog")
dbutils.widgets.text("schema", "medallion", "Target schema")
dbutils.widgets.text("volume_path", "/Volumes/dev_medallion/medallion/landing", "Volume landing path")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_path = dbutils.widgets.get("volume_path")

target_table = f"`{catalog}`.`{schema}`.bronze_orders"
checkpoint_path = f"dbfs:/tmp/medallion_checkpoints/{catalog}/bronze_orders"
schema_location = f"{volume_path}/_schema_bronze"

print(f"Source: {volume_path}/batch=*")
print(f"Target: {target_table}")
print(f"Checkpoint: {checkpoint_path}")

# ---------------------------------------------------------------------------
# Ingestia
# ---------------------------------------------------------------------------

from pyspark.sql.functions import col, current_timestamp

stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .load(f"{volume_path}/batch=*")
    .withColumn("_source_file_path", col("_metadata.file_path"))
    .withColumn("_source_file_modified_at", col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", current_timestamp())
    .drop("_metadata")
)

(
    stream_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table)
    .awaitTermination()
)

print(f"Bronze zakończony: {target_table}")