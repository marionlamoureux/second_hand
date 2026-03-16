# Gold: Kiabi essentials catalog enriched with second-hand market presence and pricing.
# Unions scraper essentials + manual essentials_bebe + manual essentials_femme; dedupes by product_uid (keep latest scraped_at).
# Joins essentials with gold_kiabi_listings to find resells mentioning each product's title.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

@dp.materialized_view(name="gold_essentials", comment="Kiabi essentials catalog (scraper + manual bebe/femme, deduped) with second-hand comparison")
def gold_essentials():
    all_essentials = (
        spark.read.table("silver_essentials")
        .unionByName(spark.read.table("silver_essentials_bebe"), allowMissingColumns=True)
        .unionByName(spark.read.table("silver_essentials_femme"), allowMissingColumns=True)
    )
    # Dedupe by product_uid (same product may exist in scraper + manual); keep latest scraped_at
    w = Window.partitionBy("product_uid").orderBy(F.col("scraped_at").desc(), F.col("_processed_at").desc())
    essentials = (
        all_essentials
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    listings = spark.read.table("gold_kiabi_listings")

    # Build a lookup: for each listing, lower-case the combined text
    listings_text = listings.select(
        "listing_key",
        "source",
        "price",
        F.lower(
            F.concat(
                F.coalesce(F.col("title"), F.lit("")),
                F.lit(" "),
                F.coalesce(F.col("description"), F.lit(""))
            )
        ).alias("listing_text")
    )

    # For each essential: construct a search keyword from the title (first 3+ char words, lowercased)
    # We join on: listing_text LIKE '%<title_lower>%'  using a non-equi join.
    # Non-equi join on string containment is safe (no crossJoin.enabled needed in recent Spark versions).
    essentials_kw = essentials.withColumn("title_lower", F.lower(F.col("title")))

    matched = (
        listings_text
        .join(
            essentials_kw.select("product_uid", "title_lower"),
            F.col("listing_text").contains(F.col("title_lower")),
            how="inner"
        )
    )

    market_stats = (
        matched
        .groupBy("product_uid")
        .agg(
            F.count("listing_key").alias("resell_count"),
            F.avg("price").alias("avg_resell_price"),
            F.min("price").alias("min_resell_price"),
            F.max("price").alias("max_resell_price"),
            F.countDistinct("source").alias("marketplace_count"),
        )
    )

    return (
        essentials
        .join(market_stats, on="product_uid", how="left")
        .withColumn(
            "price_ratio",
            F.when(
                (F.col("price").isNotNull()) & (F.col("price") > 0) & F.col("avg_resell_price").isNotNull(),
                F.round(F.col("avg_resell_price") / F.col("price"), 3)
            )
        )
        .withColumn("resell_count", F.coalesce(F.col("resell_count"), F.lit(0)))
        .withColumn("_updated_at", F.current_timestamp())
        .select(
            "product_uid",
            "product_code",
            "title",
            "universe",
            "universe_key",
            "category",
            "color",
            "price",
            "list_price",
            "currency",
            "product_url",
            "primary_image_url",
            "rating",
            "total_reviews",
            "scraped_at",
            "resell_count",
            "avg_resell_price",
            "min_resell_price",
            "max_resell_price",
            "marketplace_count",
            "price_ratio",
            "_updated_at",
        )
    )
