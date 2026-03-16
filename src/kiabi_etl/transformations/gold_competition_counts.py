# Gold: latest competition search count per (brand, marketplace)
# One row per (brand, marketplace) with the most recent run's count.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_competition_counts", comment="Latest competitor search counts per brand/marketplace")
def gold_competition_counts():
    latest = F.max(F.struct(F.col("run_ts"), F.col("count"))).alias("latest")
    return (
        spark.read.table("silver_competition_counts")
        .groupBy("brand", "marketplace")
        .agg(latest)
        .withColumn("last_run_at", F.col("latest.run_ts"))
        .withColumn("count", F.col("latest.count"))
        .drop("latest")
    )
