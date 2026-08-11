import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "n_"

BUSINESS_COLUMNS = ["nationkey", "name", "regionkey", "comment"]

@dp.table(
    name="silver_nation",
    comment="Nation oczyszczone: prefiksy n_ zdjęte, kolumny biznesowe + lineage."
)
def silver_nation():
    bronze=spark.readStream.table("bronze_nation")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)