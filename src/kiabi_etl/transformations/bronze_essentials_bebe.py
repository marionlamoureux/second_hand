# Bronze: Kiabi "Nos essentiels bébé" manual scrape CSV from landing volume.
# Upload essentials_bebe.csv to Volumes/.../kiabi_landing/manual_scraping/essentials_bebe/
from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing_base = spark.conf.get("landing_path")
schema_base = spark.conf.get("schema_location_base", "/tmp/kiabi_etl_schemas")
_essentials_bebe_path = f"{landing_base}/manual_scraping/essentials_bebe"


@dp.table(name="bronze_essentials_bebe", comment="Raw essentials bébé CSV from manual scraping")
def bronze_essentials_bebe():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{schema_base}/bronze_essentials_bebe")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .load(_essentials_bebe_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
