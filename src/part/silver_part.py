import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "p_"

BUSINESS_COLUMNS = ["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment"]

@dp.table(
    name="silver_part",
    comment="Part oczyszczone: prefiksy p_ zdjęte, kolumny biznesowe + lineage."
)
def silver_part():
    bronze=spark.readStream.table("bronze_part")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)