import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "r_"

BUSINESS_COLUMNS = ["regionkey", "name", "comment"]

@dp.table(
    name="silver_region",
    comment="Region oczyszczone: prefiksy r_ zdjęte, kolumny biznesowe + lineage."
)
def silver_region():
    bronze=spark.readStream.table("bronze_region")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)