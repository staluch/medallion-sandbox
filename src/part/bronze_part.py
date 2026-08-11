from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

landing_path = spark.conf.get("landing_root") + "/part"

@dp.table(
    name="bronze_part",
    comment="Raw part z landing volume, typy natywne z parquet, brak walidacji."
)
def bronze_part():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(landing_path)
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file_path"),
            col("_metadata.file_modification_time").alias("_source_file_mod_time"),
            current_timestamp().alias("_ingested_at"),
        )
    )