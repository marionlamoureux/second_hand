# Bronze: Kiabi store locations from landing volume (single JSON array file).
# Expects kiabi_stores.json at landing_path/kiabi_stores.json with format:
# [ {"name": "Kiabi Abbeville", "city": "Abbeville", "lat": 50.1059, "lng": 1.8352}, ... ]
# Uses Auto Loader (cloudFiles) so this streaming table picks up the file on each pipeline run.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

landing_base = spark.conf.get("landing_path")
stores_dir = landing_base  # Auto Loader watches the landing dir; filter below

_STORES_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("address", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
])


@dp.table(name="bronze_kiabi_stores", comment="Raw Kiabi store locations from landing volume (kiabi_stores.json)")
def bronze_kiabi_stores():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("multiLine", "true")
            .option("pathGlobFilter", "kiabi_stores.json")
            .schema(_STORES_SCHEMA)
            .load(stores_dir)
            .withColumn("_ingested_at", F.current_timestamp())
    )
