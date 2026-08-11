"""Zdejmuje prefiks z nazw kolumn i zawęża wynik do business + lineage."""

from pyspark.sql.functions import col

def remove_prefix(df, prefix, business_columns, lineage_columns=("_ingested_at",)):
    renamed = df.select(
        *[col(c).alias(c[len(prefix):]) if c.startswith(prefix) else col(c) for c in df.columns]
    )
    return renamed.select(*business_columns, *lineage_columns)