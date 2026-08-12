import sys

sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from gold.build_supplier_parts import build_supplier_parts

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")

@dp.materialized_view(
    name="gold_supplier_parts",
    comment="Katalog zaopatrzenia: ziarno partsupp (część x dostawca), INNER x2. Logika w build_supplier_parts.",
)
def gold_supplier_parts():
    return build_supplier_parts(
        partsupp=spark.read.table(f"{catalog}.{schema}.silver_partsupp"),
        part=spark.read.table(f"{catalog}.{schema}.silver_part"),
        supplier=spark.read.table(f"{catalog}.{schema}.silver_supplier"),
    )