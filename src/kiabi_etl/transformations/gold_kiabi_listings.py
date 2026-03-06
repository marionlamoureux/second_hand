# Gold: unified Kiabi listings view (one row per listing per source)
# first_seen_at / last_seen_at support ad lifetime (how long the ad stays online).
# latitude/longitude: derived in silver from location (French city/region match), carried to gold.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_kiabi_listings", comment="Unified Kiabi listings from all sources")
def gold_kiabi_listings():
    return (
        spark.read.table("silver_listings")
        .withColumn("listing_key", F.concat(F.col("source"), F.lit("_"), F.col("external_id")))
        .groupBy("listing_key", "source", "external_id")
        .agg(
            F.min("_processed_at").alias("first_seen_at"),
            F.max("_processed_at").alias("last_seen_at"),
            F.first("title").alias("title"),
            F.first("description").alias("description"),
            F.first("price").alias("price"),
            F.first("url").alias("url"),
            F.first("location").alias("location"),
            F.first("latitude").alias("latitude"),
            F.first("longitude").alias("longitude"),
            F.first("published_at").alias("published_at"),
            F.first("primary_image_url").alias("primary_image_url"),
            F.first("is_never_worn").alias("is_never_worn"),
        )
    )
