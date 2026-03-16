# Silver: normalize manual scraping (eBay, Vestiaire) to same schema as silver_listings.
# Source from filename; external_id from sku (eBay) or URL (Vestiaire). No geocoding (lat/lon null).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LISTING_SOURCES = ("ebay", "vestiaire")


@dp.materialized_view(name="silver_manual_listings", comment="Manual scraping listings normalized to silver_listings schema")
def silver_manual_listings():
    base = spark.read.table("bronze_manual_listings")
    path = F.lower(F.coalesce(F.col("_source_file"), F.lit("")))
    source = (
        F.when(path.contains("ebay"), F.lit("ebay"))
        .when(path.contains("vestiaire"), F.lit("vestiaire"))
        .otherwise(F.lit("manual"))
    )
    base = base.filter(source.isin(*LISTING_SOURCES))

    # eBay: sku, item_page_link, product_name/item_page_title, price1/price, main_image/image, data2 (location)
    # Vestiaire: item_page_link (id in path), name/product_name, price/price_1, image, seller_location/description_1
    url_col = F.coalesce(
        F.col("item_page_link"),
        F.col("item_page_title"),  # fallback
        F.lit(""),
    ).cast("string")
    external_id = (
        F.when(source == "ebay", F.trim(F.coalesce(F.col("sku"), F.lit(""))))
        .when(
            source == "vestiaire",
            F.regexp_extract(url_col, r"[-/](\d+)\.(?:shtml|html)?(?:\?|$)", 1),
        )
        .otherwise(F.lit(""))
    )
    title = F.trim(
        F.coalesce(
            F.col("product_name"),
            F.col("item_page_title"),
            F.col("name"),
            F.lit(""),
        ).cast("string")
    )
    description = F.trim(
        F.coalesce(
            F.col("description"),
            F.col("data"),
            F.col("product_name"),
            F.lit(""),
        ).cast("string")
    )
    # Price: parse "64 €", "US $9.99" (eBay: price/price1; Vestiaire: price/price_1 — no product4_price, that's essentials only)
    price_clean = F.regexp_replace(
        F.coalesce(F.col("price_1"), F.col("price"), F.col("price1"), F.lit("0")).cast("string"),
        r"[,']",
        ".",
    )
    price_num = F.regexp_extract(price_clean, r"[\d.]+", 0).cast("double")
    primary_image = F.trim(
        F.coalesce(
            F.col("main_image"),
            F.col("image"),
            F.lit(""),
        ).cast("string")
    )
    location = F.trim(
        F.coalesce(
            F.col("data2"),  # eBay "Located in United States"
            F.col("seller_location"),
            F.col("data3"),
            F.lit(""),
        ).cast("string")
    )

    return (
        base.withColumn("source", source)
        .withColumn("external_id", external_id)
        .filter(F.length(F.trim(external_id)) > 0)
        .withColumn("title", title)
        .withColumn("description", description)
        .withColumn("price", price_num)
        .withColumn("url", url_col)
        .withColumn("location", location)
        .withColumn("latitude", F.lit(None).cast("double"))
        .withColumn("longitude", F.lit(None).cast("double"))
        .withColumn("published_at", F.col("_ingested_at"))
        .withColumn("primary_image_url", primary_image)
        .withColumn("is_never_worn", F.lit(False))
        .withColumn("_processed_at", F.current_timestamp())
        .select(
            "source",
            "external_id",
            "title",
            "description",
            "price",
            "url",
            "location",
            "latitude",
            "longitude",
            "published_at",
            "primary_image_url",
            "is_never_worn",
            "_processed_at",
        )
    )
