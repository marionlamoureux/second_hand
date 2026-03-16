# Gold: one row per Kiabi store (dedupe by name+city, latest lat/lng).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="gold_kiabi_stores", comment="Unified Kiabi store locations (one row per store)")
def gold_kiabi_stores():
    return (
        spark.read.table("silver_kiabi_stores")
        .groupBy("name", "city")
        .agg(
            F.first("address").alias("address"),
            F.first("postal_code").alias("postal_code"),
            F.first("phone").alias("phone"),
            F.first("lat").alias("lat"),
            F.first("lng").alias("lng"),
            F.max("_processed_at").alias("_processed_at"),
        )
    )
