# Bronze: Kiabi "Nos essentiels femme" manual scrape from landing volume.
# Upload any CSV(s) to Volumes/.../kiabi_landing/manual_scraping/essentials_femme/
# Cloud Files picks up all *.csv in that directory (schema evolution adds new columns).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing_base = spark.conf.get("landing_path")
schema_base = spark.conf.get("schema_location_base", "/tmp/kiabi_etl_schemas")
_essentials_femme_path = f"{landing_base}/manual_scraping/essentials_femme"


@dp.table(name="bronze_essentials_femme", comment="Raw essentials femme CSV(s) from manual scraping")
def bronze_essentials_femme():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{schema_base}/bronze_essentials_femme")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .load(_essentials_femme_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
