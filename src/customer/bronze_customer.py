from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

landing_path = spark.conf.get("landing_volume_path")

@dp.table(
    name="bronze_customer",
    comment="Raw customer z landing volume, typy natywne z parquet, brak walidacji.",
    # EXPECTATIONS - walidacje jakości danych runtime
    # Bronze: lekkie sprawdzenia - czy plik został odczytany, czy są podstawowe dane
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "c_custkey",  # Optymalizacja dla klucza
    },
    partition_cols=[],  # Bronze append-only, bez partycji
)
@dp.expect_or_drop("valid_custkey", "c_custkey IS NOT NULL")
@dp.expect_or_drop("valid_nationkey", "c_nationkey IS NOT NULL")
@dp.expect("has_source_metadata", "_source_file_path IS NOT NULL")
def bronze_customer():
    """
    Warstwa bronze - surowe dane z landing volume.
    
    Transformacje:
    - Auto Loader (cloudFiles) czyta parquet z volume
    - Dodaje kolumny metadanych (_source_file_path, _source_file_mod_time, _ingested_at)
    - Zachowuje wszystkie kolumny źródłowe (select "*")
    
    Expectations:
    - @expect_or_drop: wiersze z NULL w kluczach są USUWANE (złe dane nie wchodzą do systemu)
    - @expect: wiersze bez metadanych są LOGOWANE, ale zachowane (warn-only)
    
    Dlaczego expect_or_drop dla kluczy?
    - Wiersz bez custkey to broken record - nie możemy go połączyć z niczym
    - Lepiej go odrzuć na bronze niż propagować złe dane dalej
    - Pipeline metrics pokażą ile wierszy zostało odrzuconych
    """
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