# Gold: unified Kiabi listings view (one row per listing per source)
# first_seen_at / last_seen_at support ad lifetime (how long the ad stays online).
# latitude/longitude: derived in silver from location (French city/region match), carried to gold.
# nearest_store_km / nearest_store_name: Haversine distance to the closest Kiabi store.
#   Stores (353 rows) are collected to driver and embedded in a UDF closure — avoids a
#   cross-join and any intra-pipeline MV read-before-commit issue.
#   Listings with no geocode (null lat/lng) get null distance.
import math

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

_EARTH_R_KM = 6371.0

_NEAREST_TYPE = StructType([
    StructField("store_name", StringType(), True),
    StructField("store_km", DoubleType(), True),
])


def _haversine_km(lat1, lon1, lat2, lon2):
    """Pure-Python haversine distance in km; returns None if any coord is None."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = (
        math.sin((rlat2 - rlat1) / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin((rlon2 - rlon1) / 2) ** 2
    )
    return round(2 * _EARTH_R_KM * math.asin(math.sqrt(max(0.0, a))), 1)


@dp.materialized_view(name="gold_kiabi_listings", comment="Unified Kiabi listings from all sources (incl. manual ebay/vestiaire), with distance to nearest Kiabi store")
def gold_kiabi_listings():
    all_listings = (
        spark.read.table("silver_listings")
        .unionByName(spark.read.table("silver_manual_listings"), allowMissingColumns=True)
    )
    listings = (
        all_listings
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

    # Collect stores to the driver (353 rows, ~35 KB serialised into UDF closure)
    stores = [
        (r["name"], r["lat"], r["lng"])
        for r in spark.read.table("gold_kiabi_stores").select("name", "lat", "lng").collect()
    ]

    @F.udf(returnType=_NEAREST_TYPE)
    def nearest_store(lat, lng):
        best_dist, best_name = None, None
        for name, slat, slng in stores:
            d = _haversine_km(lat, lng, slat, slng)
            if d is not None and (best_dist is None or d < best_dist):
                best_dist, best_name = d, name
        return (best_name, best_dist)

    result = nearest_store(F.col("latitude"), F.col("longitude"))
    return (
        listings
        .withColumn("nearest_store_name", result.getField("store_name"))
        .withColumn("nearest_store_km", result.getField("store_km"))
    )
