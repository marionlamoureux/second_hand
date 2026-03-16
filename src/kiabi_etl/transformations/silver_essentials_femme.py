# Silver: normalize essentials femme manual CSV to silver_essentials schema.
# Maps: name -> title, price_1/Product_4_Price/price -> price, item_page_link -> product_url, image_1/image -> primary_image_url.
# product_uid from kiabi URL (e.g. P1652924C2087341); universe = femme. Dedupes by product_uid (keep latest _ingested_at).
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window


@dp.materialized_view(name="silver_essentials_femme", comment="Essentials femme manual scrape normalized to essentials schema")
def silver_essentials_femme():
    base = spark.read.table("bronze_essentials_femme")
    url_col = F.trim(F.coalesce(F.col("item_page_link"), F.lit("")).cast("string"))
    product_uid = F.coalesce(
        F.regexp_extract(url_col, r"[-_]([A-Z0-9]+C[0-9]+)(?:\?|$)", 1),
        F.regexp_extract(url_col, r"/([a-z0-9_-]+)_P\d", 1),
        F.sha1(url_col),
    )
    price_clean = F.regexp_replace(
        F.coalesce(
            F.col("price_1"),
            F.col("Product_4_Price"),
            F.col("Product_2_Price"),
            F.col("price"),
            F.lit("0"),
        ).cast("string"),
        r"[,']",
        ".",
    )
    price_num = F.regexp_extract(price_clean, r"[\d.]+", 0).cast("double")
    primary_image = F.trim(
        F.coalesce(F.col("image_1"), F.col("image"), F.lit("")).cast("string")
    )
    rating_str = F.regexp_extract(
        F.coalesce(F.col("rating"), F.col("product_4_rating"), F.col("product_1_rating"), F.lit("0")).cast("string"),
        r"[\d.]+",
        0,
    )
    return (
        base.withColumn("product_uid", product_uid)
        .filter(F.length(F.trim(F.coalesce(F.col("product_uid"), F.lit("")))) > 0)
        .withColumn("product_code", F.col("product_uid"))
        .withColumn("title", F.trim(F.coalesce(F.col("name"), F.col("data"), F.col("Product_2_Name"), F.lit("")).cast("string")))
        .withColumn("universe", F.lit("femme"))
        .withColumn("universe_key", F.lit("femme"))
        .withColumn("category", F.trim(F.coalesce(F.col("breadcrumbs"), F.col("breadcrumb"), F.lit("")).cast("string")))
        .withColumn("color", F.trim(F.coalesce(F.col("current_color"), F.lit("")).cast("string")))
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
