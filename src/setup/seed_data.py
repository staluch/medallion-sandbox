# =============================================================================
# seed_data.py
# Uruchamiaj ręcznie przed każdym przebiegiem pipeline'u z nowym batchem.
# Parametr batch_id to rok (tpch orders obejmuje lata 1992–1998).
# =============================================================================

dbutils.widgets.text("batch_id", "1992", "Rok (1992-1998)")
dbutils.widgets.text("catalog", "dev_medallion", "Target catalog")
dbutils.widgets.text("schema", "medallion", "Target schema")

batch_id = dbutils.widgets.get("batch_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

volume_path = f"/Volumes/{catalog}/{schema}/landing"

print(f"Config: catalog={catalog}, schema={schema}, batch_id={batch_id}")

# ---------------------------------------------------------------------------
# 1. Tworzenie obiektów UC (idempotentne – bezpieczne przy każdym wywołaniu)
# ---------------------------------------------------------------------------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.landing")

print("UC objects: OK")

# ---------------------------------------------------------------------------
# 2. Odczyt z Delta Shares i zapis do Volume
# ---------------------------------------------------------------------------

from pyspark.sql.functions import year, col, to_date

orders_raw = spark.table("samples.tpch.orders")

batch_df = orders_raw.filter(year(to_date(col("o_orderdate"))) == int(batch_id))
output_dir = f"{volume_path}/batch={batch_id}"

batch_df.write.mode("overwrite").parquet(output_dir)

print(f"Batch {batch_id}: zapisano do: {output_dir}")