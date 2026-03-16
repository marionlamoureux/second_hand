# Silver: cleaned Kiabi store locations — normalized types and trimmed strings.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="silver_kiabi_stores", comment="Cleaned Kiabi store locations with normalized types")
def silver_kiabi_stores():
    return (
        spark.read.table("bronze_kiabi_stores")
        .withColumn("name", F.trim(F.coalesce(F.col("name"), F.lit("")).cast("string")))
        .withColumn("city", F.trim(F.coalesce(F.col("city"), F.lit("")).cast("string")))
        .withColumn("address", F.trim(F.coalesce(F.col("address"), F.lit("")).cast("string")))
        .withColumn("postal_code", F.trim(F.coalesce(F.col("postal_code"), F.lit("")).cast("string")))
        .withColumn("phone", F.trim(F.coalesce(F.col("phone"), F.lit("")).cast("string")))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lng", F.col("lng").cast("double"))
        .withColumn("_processed_at", F.current_timestamp())
    )
