# Databricks notebook source
# MAGIC %md
# MAGIC # Kiabi Essentials — Vector Search Index
# MAGIC Builds a Vector Search index on the `gold_essentials` table so the image-matching agent
# MAGIC can find the closest catalog item for any listing image.
# MAGIC
# MAGIC **Run once** (or whenever the essentials catalog changes).

# COMMAND ----------

CATALOG  = "nef_catalog"
SCHEMA   = "second_hand"
VS_ENDPOINT = "kiabi_knowledge_vs"          # reuse existing endpoint
INDEX_NAME  = f"{CATALOG}.{SCHEMA}.essentials_vs_index"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.essentials_for_search"
EMBEDDING_MODEL = "databricks-gte-large-en"

# COMMAND ----------
# MAGIC %md ## 1 — Build the source table
# MAGIC Combine text attributes into a single `search_text` column that will be auto-embedded.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SOURCE_TABLE} AS
SELECT
    product_uid                                                       AS id,
    product_code,
    title,
    universe,
    category,
    color,
    CAST(price AS DOUBLE)                                             AS price,
    product_url,
    primary_image_url,
    CONCAT_WS(' | ',
        COALESCE(title,    ''),
        COALESCE(category, ''),
        COALESCE(color,    ''),
        COALESCE(universe, '')
    )                                                                 AS search_text
FROM {CATALOG}.{SCHEMA}.gold_essentials
WHERE product_uid IS NOT NULL
""")

count = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").collect()[0][0]
print(f"Source table ready: {count} essentials")

# COMMAND ----------
# MAGIC %md ## 2 — Enable Change Data Feed (required for Delta Sync)

# COMMAND ----------

spark.sql(f"ALTER TABLE {SOURCE_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("Change Data Feed enabled")

# COMMAND ----------
# MAGIC %md ## 3 — Create (or recreate) the Vector Search index

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Drop existing index if present (idempotent re-run)
try:
    vsc.delete_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)
    print(f"Dropped existing index {INDEX_NAME}")
except Exception:
    pass

index = vsc.create_delta_sync_index(
    endpoint_name=VS_ENDPOINT,
    index_name=INDEX_NAME,
    source_table_name=SOURCE_TABLE,
    pipeline_type="TRIGGERED",          # manually triggered; use CONTINUOUS for prod
    primary_key="id",
    embedding_source_column="search_text",
    embedding_model_endpoint_name=EMBEDDING_MODEL,
)

print(f"Created index: {INDEX_NAME}")
print(index)

# COMMAND ----------
# MAGIC %md ## 4 — Trigger initial sync and wait

# COMMAND ----------

import time

index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)

# Wait for the index pipeline to initialise before calling sync().
# Immediately after create_delta_sync_index() the index is NOT_READY;
# we need to wait until it reaches PROVISIONING_PIPELINE or similar
# state that accepts a sync() call.
print("Waiting for index to finish initialising …")
for _ in range(60):
    desc = index.describe()
    status = desc.get("status", {}).get("detailed_state", "UNKNOWN")
    print(f"  pre-sync status: {status}")
    # States that are stable enough to accept a sync() call
    if status not in ("NOT_READY", "UNKNOWN", "INITIALIZING"):
        break
    time.sleep(10)

print(f"Index ready for sync — current state: {status}")

# Trigger sync only if not already online
if status not in ("ONLINE", "ONLINE_NO_PENDING_UPDATE"):
    try:
        index.sync()
        print("Sync triggered — waiting for ONLINE status …")
    except Exception as e:
        print(f"sync() call failed ({e}), index may self-sync — continuing poll …")

for _ in range(60):
    status = index.describe()["status"]["detailed_state"]
    print(f"  status: {status}")
    if status in ("ONLINE", "ONLINE_NO_PENDING_UPDATE"):
        break
    time.sleep(10)

print(f"Final status: {status}")

# COMMAND ----------
# MAGIC %md ## 5 — Quick smoke test

# COMMAND ----------

results = index.similarity_search(
    query_text="t-shirt blanc femme col rond",
    columns=["id", "title", "category", "color", "product_url"],
    num_results=3,
)
print("Test query — top 3 matches:")
for row in results["result"]["data_array"]:
    print(" ", row)
