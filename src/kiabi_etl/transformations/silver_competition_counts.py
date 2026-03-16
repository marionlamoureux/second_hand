# Silver: cleaned competition search counts — MATERIALIZED VIEW (batch read from bronze).
# No category: one row per (brand, marketplace). Rows with null count are skipped.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="silver_competition_counts", comment="Cleaned competitor search counts per brand/marketplace")
def silver_competition_counts():
    df = spark.read.table("bronze_listings")
    if "brand" not in df.columns or "marketplace" not in df.columns:
        return spark.createDataFrame([], schema="brand string, marketplace string, count long, run_ts timestamp, _processed_at timestamp")
    return (
        df
        .filter(F.col("brand").isNotNull())
        .filter(F.col("marketplace").isNotNull())
        .filter(F.col("count").isNotNull())
        .withColumn("brand", F.trim(F.col("brand").cast("string")))
        .withColumn("marketplace", F.lower(F.trim(F.col("marketplace").cast("string"))))
        .withColumn("count", F.col("count").cast("long"))
        .filter(F.col("count").isNotNull())
        .withColumn("run_ts", F.coalesce(F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"), F.col("_ingested_at")))
        .withColumn("_processed_at", F.current_timestamp())
        .select("brand", "marketplace", "count", "run_ts", "_processed_at")
    )
