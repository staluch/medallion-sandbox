from pyspark import pipelines as dp

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")

@dp.materialized_view(
    name="gold_customer_orders",
    comment="Zamówienia wzbogacone o atrybuty klienta. Ziarno: wiersz per zamówienie. INNER join."
)
def gold_customer_orders():
    orders = spark.read.table(f"{catalog}.{schema}.silver_orders")
    customer = spark.read.table(f"{catalog}.{schema}.silver_customer")
    return(
        orders.join(customer, on = "custkey", how = "inner")
        .select(
            orders["*"],
            customer["name"].alias("customer_name"),
            customer["mktsegment"].alias("customer_mktsegment"),
            customer["nationkey"].alias("customer_nationkey"),
        )
    )