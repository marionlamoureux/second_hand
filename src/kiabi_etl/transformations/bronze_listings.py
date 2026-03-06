# Bronze: raw data from landing volume (listings + competition JSONL; single path required by cloudFiles).
# Silver layers filter: listings by source, competition by brand.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing_base = spark.conf.get("landing_path")
schema_base = spark.conf.get("schema_location_base", "/tmp/kiabi_etl_schemas")

@dp.table(name="bronze_listings", comment="Raw data from landing volume (listings and competition JSONL)")
def bronze_listings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_base}/bronze_listings")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing_base)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
