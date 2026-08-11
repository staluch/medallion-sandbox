import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "o_"

BUSINESS_COLUMNS = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment",
]

@dp.table(
    name="silver_orders",
    comment="Orders oczyszczone: prefiksy o_ zdjęte, kolumny biznesowe + lineage."
)
def silver_orders():
    bronze = spark.readStream.table("bronze_orders")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)