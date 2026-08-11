import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "ps_"

BUSINESS_COLUMNS = ["partkey", "suppkey", "availqty", "supplycost", "comment"]

@dp.table(
    name="silver_partsupp",
    comment="Customer oczyszczone: prefiksy ps_ zdjęte, kolumny biznesowe + lineage."
)
def silver_partsupp():
    bronze=spark.readStream.table("bronze_partsupp")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)