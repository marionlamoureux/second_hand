# Silver: cleaned competition search counts (from bronze_listings; competition rows have brand/marketplace)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="silver_competition_counts", comment="Cleaned competitor search counts per brand/category/marketplace")
def silver_competition_counts():
    return (
        spark.readStream.table("bronze_listings")
        .filter(F.col("brand").isNotNull())
        .filter(F.col("marketplace").isNotNull())
        .withColumn("brand", F.trim(F.col("brand").cast("string")))
        .withColumn("category", F.trim(F.coalesce(F.col("category").cast("string"), F.lit(""))))
        .withColumn("marketplace", F.lower(F.trim(F.col("marketplace").cast("string"))))
        .withColumn("count", F.coalesce(F.col("count").cast("long"), F.lit(0)))
        .withColumn("run_ts", F.coalesce(F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"), F.col("_ingested_at")))
        .withColumn("_processed_at", F.current_timestamp())
    )
