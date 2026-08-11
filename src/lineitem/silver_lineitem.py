import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "l_"

BUSINESS_COLUMNS = ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"]

@dp.table(
    name="silver_lineitem",
    comment="Lineitem oczyszczone: prefiksy l_ zdjęte, kolumny biznesowe + lineage."
)
def silver_lineitem():
    bronze = spark.readStream.table("bronze_lineitem")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)