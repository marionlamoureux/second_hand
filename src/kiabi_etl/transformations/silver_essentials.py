# Silver: cleaned Kiabi essentials catalog — MATERIALIZED VIEW (batch read from bronze).
# Deduplicates by product_uid (keep latest scraped_at per product).
# Normalizes price to double, trims strings, keeps primary_image_url for vision analysis.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

@dp.materialized_view(name="silver_essentials", comment="Cleaned Kiabi essentials catalog with one row per product variant")
def silver_essentials():
    df = (
        spark.read.table("bronze_essentials")
        .filter(F.col("product_uid").isNotNull())
        .filter(F.trim(F.col("product_uid").cast("string")) != "")
        .withColumn("product_uid", F.trim(F.col("product_uid").cast("string")))
        .withColumn("product_code", F.trim(F.coalesce(F.col("product_code").cast("string"), F.lit(""))))
        .withColumn("title", F.trim(F.coalesce(F.col("title").cast("string"), F.lit(""))))
        .withColumn("universe", F.trim(F.coalesce(F.col("universe").cast("string"), F.lit(""))))
        .withColumn("universe_key", F.trim(F.coalesce(F.col("universe_key").cast("string"), F.lit(""))))
        .withColumn("category", F.trim(F.coalesce(F.col("category").cast("string"), F.lit(""))))
        .withColumn("color", F.trim(F.coalesce(F.col("color").cast("string"), F.lit(""))))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("list_price", F.col("list_price").cast("double"))
        .withColumn("currency", F.coalesce(F.col("currency").cast("string"), F.lit("EUR")))
        .withColumn("product_url", F.trim(F.coalesce(F.col("product_url").cast("string"), F.lit(""))))
        .withColumn("primary_image_url", F.trim(F.coalesce(F.col("primary_image_url").cast("string"), F.lit(""))))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("total_reviews", F.coalesce(F.col("total_reviews").cast("long"), F.lit(0)))
        .withColumn("scraped_at", F.to_timestamp(F.col("scraped_at")))
        .withColumn("_processed_at", F.current_timestamp())
    )
    # Keep most-recent scrape per product_uid (a product may appear in multiple scraper runs)
    w = Window.partitionBy("product_uid").orderBy(F.col("scraped_at").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
