dbutils.widgets.text("catalog", "dev_medallion", "Target catalog")
dbutils.widgets.text("schema", "medallion", "Target schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = f"`{catalog}`.`{schema}`.bronze_orders"
target_table = f"`{catalog}`.`{schema}`.silver_orders"
checkpoint_path = fdbfs:/tmp/medallion_checkpoints/{catalog}/silver_orders"

print(f"Source: {source_table}")
print(f"Target: {target_table}")
print(f"Checkpoint: {checkpoint_path}")

from pyspark.sql.functions import col, current_timestamp, to_date, xxhash64
from pyspark.sql.types import DecimalType

bronze_stream = (
    spark.readStream
    .format("delta")
    .table(source_table)
)

silver_df = (
    bronze_stream
    .select(
        col("o_orderkey").alias("orderkey"),
        col("o_custkey").alias("custkey"),
        col("o_orderstatus").alias("orderstatus"),
        col("o_totalprice").cast(DecimalType(15,2)).alias("totalprice"),
        to_date(col("o_orderdate")).alias("orderdate"),
        col("o_orderpriority").alias("orderpriority"),
        col("o_clerk").alias("clerk"),
        col("o_shippriority").alias("shippriority"),
        col("o_comment").alias("comment"),
        col("_source_file_path"),
        col("_source_file_modified_at"),
        col("_ingested_at")
    )
    .withColumn("_processed_at", current_timestamp())
    .withColumn("_row_hash", xxhash64(
        col("orderkey"),
        col("custkey"),
        col("orderstatus"),
        col("totalprice"),
        col("orderdate"),
        col("orderpriority"),
        col("clerk"),
        col("shippriority"),
        col("comment")
    ))
)

(
    silver_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table)
    .awaitTermination()
)

print(f"Silver zakończony: {target_table}")