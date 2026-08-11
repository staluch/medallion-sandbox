import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "s_"

BUSINESS_COLUMNS = ["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"]

@dp.table(
    name="silver_supplier",
    comment="Supplier oczyszczone: prefiksy c_ zdjęte, kolumny biznesowe + lineage."
)
def silver_supplier():
    bronze=spark.readStream.table("bronze_supplier")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)