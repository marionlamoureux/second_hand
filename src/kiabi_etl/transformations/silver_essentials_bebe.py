# Silver: normalize essentials bébé manual CSV to silver_essentials schema.
# Maps: name -> title, price_1/product4_price -> price, item_page_link -> product_url, image_1 -> primary_image_url.
# product_uid from kiabi URL (e.g. P2876777C2876928) or hash of url; universe = bebe.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window


@dp.materialized_view(name="silver_essentials_bebe", comment="Essentials bébé manual scrape normalized to essentials schema")
def silver_essentials_bebe():
    base = spark.read.table("bronze_essentials_bebe")
    url_col = F.trim(F.coalesce(F.col("item_page_link"), F.lit("")).cast("string"))
    product_uid = F.coalesce(
        F.regexp_extract(url_col, r"[-_]([A-Z0-9]+C[0-9]+)(?:\?|$)", 1),
        F.regexp_extract(url_col, r"/([a-z0-9_-]+)_P\d", 1),
        F.sha1(url_col),
    )
    price_clean = F.regexp_replace(
        F.coalesce(F.col("price_1"), F.col("product4_price"), F.col("price"), F.lit("0")).cast("string"),
        r"[,']",
        ".",
    )
    price_num = F.regexp_extract(price_clean, r"[\d.]+", 0).cast("double")
    primary_image = F.trim(
        F.coalesce(F.col("image_1"), F.col("image"), F.lit("")).cast("string")
    )
    rating_str = F.regexp_extract(
        F.coalesce(F.col("rating"), F.col("product1_rating"), F.lit("0")).cast("string"),
        r"[\d.]+",
        0,
    )
    return (
        base.withColumn("product_uid", product_uid)
        .filter(F.length(F.trim(F.coalesce(F.col("product_uid"), F.lit("")))) > 0)
        .withColumn("product_code", F.col("product_uid"))
        .withColumn("title", F.trim(F.coalesce(F.col("name"), F.col("data"), F.lit("")).cast("string")))
        .withColumn("universe", F.lit("bebe"))
        .withColumn("universe_key", F.lit("bebe"))
        .withColumn("category", F.trim(F.coalesce(F.col("breadcrumbs_1"), F.col("breadcrumbs"), F.lit("")).cast("string")))
        .withColumn("color", F.trim(F.coalesce(F.col("Available_Colors"), F.lit("")).cast("string")))
        .withColumn("price", price_num)
        .withColumn("list_price", price_num)
        .withColumn("currency", F.lit("EUR"))
        .withColumn("product_url", url_col)
        .withColumn("primary_image_url", primary_image)
        .withColumn("rating", rating_str.cast("double"))
        .withColumn("total_reviews", F.lit(0))
        .withColumn("scraped_at", F.col("_ingested_at"))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_row_num", F.row_number().over(Window.partitionBy("product_uid").orderBy(F.col("_ingested_at").desc())))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
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
            "_processed_at",
        )
    )
