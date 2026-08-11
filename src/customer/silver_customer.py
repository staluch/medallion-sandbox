import sys
sys.path.append(spark.conf.get("src_root"))

from pyspark import pipelines as dp
from utils.remove_prefix import remove_prefix

PREFIX = "c_"

BUSINESS_COLUMNS = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"]

@dp.table(
    name="silver_customer",
    comment="Customer oczyszczone: prefiksy c_ zdjęte, kolumny biznesowe + lineage.",
    # EXPECTATIONS - walidacje jakości danych runtime
    # Silver: więcej sprawdzeń biznesowych - formatów, zakresów, poprawności
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "custkey",  # Optymalizacja dla klucza
        "delta.enableChangeDataFeed": "true",  # CDC dla silver (możliwe downstream CDC)
    },
)
# Klucze biznesowe - muszą być (fail hard)
@dp.expect_or_drop("valid_custkey", "custkey IS NOT NULL")
@dp.expect_or_drop("positive_custkey", "custkey > 0")
@dp.expect_or_drop("valid_nationkey", "nationkey IS NOT NULL")

# Format danych - warn jeśli złe, ale nie blokuj (może to legacy data)
@dp.expect("phone_format", "phone RLIKE '^[0-9]{2}-[0-9]{3}-[0-9]{3}-[0-9]{4}$'")
@dp.expect("name_not_empty", "LENGTH(name) > 0")
@dp.expect("address_not_empty", "LENGTH(address) > 0")

# Zakres wartości biznesowych - warn
@dp.expect("acctbal_reasonable", "acctbal BETWEEN -999999.99 AND 999999.99")
@dp.expect("mktsegment_valid", "mktsegment IN ('AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY')")

# Lineage - musi być
@dp.expect("has_lineage", "_ingested_at IS NOT NULL")
def silver_customer():
    """
    Warstwa silver - dane oczyszczone i zwalidowane.
    
    Transformacje:
    - Czyta streaming z bronze_customer
    - Usuwa prefiks "c_" z nazw kolumn (przez remove_prefix)
    - Zawęża do 8 kolumn biznesowych + _ingested_at (lineage)
    - Usuwa kolumny _source_* (nie są w BUSINESS_COLUMNS)
    
    Expectations:
    - @expect_or_drop: Kluczowe walidacje - wiersze złamujące te reguły są USUWANE
      * custkey NOT NULL i > 0 (klucz biznesowy musi być poprawny)
      * nationkey NOT NULL (wymóg integracji z tabelą nation)
    
    - @expect: Walidacje ostrzeżeniowe - wiersze są ZACHOWANE, ale metryki pokazują naruszenia
      * Format telefonu (regex pattern dla TPCH)
      * Niepuste name i address
      * Rozsądny zakres acctbal
      * Poprawny segment rynkowy (enum TPCH)
      * Obecność _ingested_at (lineage)
    
    Dlaczego niektóre expect, a nie expect_or_drop?
    - Legacy data mogą mieć stary format telefonu - nie chcemy ich tracić
    - Ale chcemy MONITOROWAĆ jakość - metryki pokażą % wierszy z problemami
    - Pozwala to na "soft migration" - widzimy problem, ale nie tracimy danych
    
    Monitorowanie:
    - Pipeline metrics pokazują dla każdej expectation:
      * % wierszy spełniających warunek
      * Liczbę odrzuconych wierszy (dla expect_or_drop)
      * Liczbę wierszy z naruszeniami (dla expect)
    - Użyj tych metryk do alertów (np. "jeśli > 5% wierszy ma zły format telefonu, wyślij alert")
    """
    bronze = spark.readStream.table("bronze_customer")
    return remove_prefix(bronze, PREFIX, BUSINESS_COLUMNS)