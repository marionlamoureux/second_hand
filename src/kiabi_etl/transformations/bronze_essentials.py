# Bronze: raw Kiabi essentials catalog from landing volume (/essentials/ subdirectory).
# Separate from bronze_listings — different schema (product catalog, not marketplace ads).
# Uses explicit schema so Auto Loader can start even when essentials/ is empty.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

landing_base = spark.conf.get("landing_path")
schema_base = spark.conf.get("schema_location_base", "/tmp/kiabi_etl_schemas")

_essentials_path = f"{landing_base}/essentials"

_ESSENTIALS_SCHEMA = StructType([
    StructField("product_code",      StringType(), True),
    StructField("product_uid",       StringType(), True),
    StructField("title",             StringType(), True),
    StructField("universe",          StringType(), True),
    StructField("universe_key",      StringType(), True),
    StructField("category",          StringType(), True),
    StructField("color",             StringType(), True),
    StructField("price",             DoubleType(), True),
    StructField("list_price",        DoubleType(), True),
    StructField("currency",          StringType(), True),
    StructField("product_url",       StringType(), True),
    StructField("primary_image_url", StringType(), True),
    StructField("rating",            StringType(), True),
    StructField("total_reviews",     LongType(),   True),
    StructField("scraped_at",        StringType(), True),
])

@dp.table(name="bronze_essentials", comment="Raw Kiabi essentials catalog (femme/homme/fille/garcon) from landing volume")
def bronze_essentials():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.{json,jsonl}")
        .schema(_ESSENTIALS_SCHEMA)
        .load(_essentials_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
