# Bronze: manual scraping CSVs (eBay, Vestiaire Collective) from landing volume.
# Upload from local output/manual_scraping/ to Volumes/.../kiabi_landing/manual_scraping/listings/
# Files: ebay-com-*.csv, *vestiaire*.csv (same folder; schema inferred from CSV).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing_base = spark.conf.get("landing_path")
schema_base = spark.conf.get("schema_location_base", "/tmp/kiabi_etl_schemas")
_manual_listings_path = f"{landing_base}/manual_scraping/listings"


@dp.table(name="bronze_manual_listings", comment="Raw manual scraping CSVs (eBay, Vestiaire) from landing volume")
def bronze_manual_listings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{schema_base}/bronze_manual_listings")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .load(_manual_listings_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
