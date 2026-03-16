# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Kiabi Knowledge Assistant
# MAGIC RAG over ESG/strategy documents.
# MAGIC Creates: chunks table → Vector Search index → RAG model → Serving endpoint.
# MAGIC Re-entrant: skips steps that already exist. Run cells in order or run specific steps.

# COMMAND ----------

# MAGIC %pip install pdfminer.six -q
# MAGIC %restart_python

# COMMAND ----------

# Config (edit for your workspace / catalog / schema)
CATALOG  = "nef_catalog"
SCHEMA   = "second_hand"
VOLUME_PATH       = f"/Volumes/{CATALOG}/{SCHEMA}/kiabi_landing/knowledge"
CHUNKS_TABLE      = f"{CATALOG}.{SCHEMA}.kiabi_knowledge_chunks"
VS_ENDPOINT       = "kiabi_knowledge_vs"
VS_INDEX          = f"{CATALOG}.{SCHEMA}.kiabi_knowledge_index"
EMBEDDING_MODEL   = "databricks-gte-large-en"
# LLM_ENDPOINT      = "databricks-claude-sonnet-4-6"
# SERVING_ENDPOINT  = "kiabi-knowledge-assistant"
# REGISTERED_MODEL  = f"{CATALOG}.{SCHEMA}.kiabi_knowledge_assistant"

# Set to True to skip a step when running later cells
SKIP_CHUNKS   = False
SKIP_VS       = False
SKIP_ENDPOINT = False

print(f"Volume: {VOLUME_PATH}")
print(f"Chunks table: {CHUNKS_TABLE}")
print(f"VS index: {VS_INDEX}")
# print(f"Serving endpoint: {SERVING_ENDPOINT}")

# COMMAND ----------

from pathlib import Path

files = [f.name for f in Path(VOLUME_PATH).iterdir() if f.is_file()]
display(spark.createDataFrame([(f,) for f in files], ['filename']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse documents into chunks
# MAGIC Reads `.md` and `.pdf` from the knowledge volume, chunks text, writes Delta table.

# COMMAND ----------

def chunk_text(text: str, chunk_size: int = 1500, overlap: int = 200) -> list:
    chunks, start = [], 0
    text_length = len(text)
    if chunk_size <= overlap:
        raise ValueError("chunk_size must be greater than overlap")
    while start < text_length:
        end = min(start + chunk_size, text_length)
        if end < text_length:
            for sep in ["\n\n", "\n", ". ", " "]:
                pos = text.rfind(sep, start, end)
                if pos != -1 and pos > start + chunk_size // 2:
                    end = pos + len(sep)
                    break
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        # Prevent infinite loop if overlap is too large or text is too short
        next_start = end - overlap
        if next_start <= start:
            start = end
        else:
            start = next_start
    return [c for c in chunks if len(c) > 50]

# COMMAND ----------

def load_documents(volume_path: str) -> list:
    import sys
    from pathlib import Path
    all_chunks = []
    for fpath in sorted(Path(volume_path).iterdir()):
        if fpath.name.startswith(".") or fpath.suffix not in (".md", ".pdf"):
            continue
        try:
            if fpath.suffix == ".md":
                text = fpath.read_text(encoding="utf-8")
            else:
                from pdfminer.high_level import extract_text
                text = extract_text(str(fpath))
            chunks = chunk_text(text)
            all_chunks.extend(
                {"source": fpath.name, "chunk_index": i, "content": c}
                for i, c in enumerate(chunks)
            )
            print(f"  {fpath.name}: {len(chunks)} chunks")
        except Exception as e:
            print(f"  {fpath.name}: ERROR {e}", file=sys.stderr)
    return all_chunks

# COMMAND ----------

def create_chunks_table(chunks: list, table: str):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import monotonically_increasing_id
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField("source",      StringType(),  False),
        StructField("chunk_index", IntegerType(), False),
        StructField("content",     StringType(),  False),
    ])
    df = (
        spark.createDataFrame(chunks, schema)
        .withColumn("id", monotonically_increasing_id().cast("string"))
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"Chunks table: {table} ({df.count()} rows)")

# COMMAND ----------

# Run Step 1: load documents from volume and write chunks table
if not SKIP_CHUNKS:
    print("Step 1: Loading documents...")
    try:
        chunks = load_documents(VOLUME_PATH)
        print(f"  Total: {len(chunks)} chunks")
        print("Writing chunks table...")
        create_chunks_table(chunks, CHUNKS_TABLE)
    except Exception as e:
        print(f"ERROR in Step 1: {e}")
        chunks = []
else:
    print("Step 1 skipped (SKIP_CHUNKS=True)")
    chunks = []  # for later cells that might reference it

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 & 3: Vector Search
# MAGIC Creates VS endpoint (if needed), then Delta-sync index on the chunks table.

# COMMAND ----------

def setup_vector_search(chunks_table: str, vs_endpoint: str, vs_index: str, embedding_model: str):
    import datetime
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.vectorsearch import (
        DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn,
        EndpointType, PipelineType, VectorIndexType,
    )
    w = WorkspaceClient()

    # Create endpoint if needed
    try:
        ep = w.vector_search_endpoints.get_endpoint(vs_endpoint)
        print(f"VS endpoint '{vs_endpoint}' exists: {ep.endpoint_status.state}")
    except Exception:
        print(f"Creating VS endpoint '{vs_endpoint}'...")
        w.vector_search_endpoints.create_endpoint_and_wait(
            name=vs_endpoint,
            endpoint_type=EndpointType.STANDARD,
            timeout=datetime.timedelta(minutes=20),
        )

    # Create index if needed
    try:
        idx = w.vector_search_indexes.get_index(vs_index)
        print(f"VS index '{vs_index}' exists — triggering sync")
        w.vector_search_indexes.sync_index(vs_index)
    except Exception:
        print(f"Creating VS index '{vs_index}'...")
        w.vector_search_indexes.create_index(
            name=vs_index,
            endpoint_name=vs_endpoint,
            primary_key="id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=chunks_table,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name="content",
                        embedding_model_endpoint_name=embedding_model,
                    )
                ],
            ),
        )
        print("VS index created — waiting 30s for initial sync...")
        import time
        time.sleep(30)
        w.vector_search_indexes.sync_index(vs_index)
    print("VS setup complete.")

# COMMAND ----------

# Run Step 2 & 3: Vector Search
if not SKIP_VS:
    print("Step 2 & 3: Setting up Vector Search...")
    setup_vector_search(CHUNKS_TABLE, VS_ENDPOINT, VS_INDEX, EMBEDDING_MODEL)
else:
    print("Vector Search step skipped (SKIP_VS=True)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: RAG serving endpoint
# MAGIC Registers an MLflow RAG model (retrieve from VS + generate with Claude) and creates/updates the serving endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC KA_INSTRUCTIONS = """You are a strategic advisor for Kiabi on second-hand fashion and sustainability.
# MAGIC You have access to Kiabi's DPEF 2024 (ESG report) and a strategic analysis document.
# MAGIC
# MAGIC Answer with:
# MAGIC - Specific numbers, percentages and dates from the documents
# MAGIC - Strategic recommendations grounded in the retrieved context
# MAGIC - Source citations (document name)
# MAGIC - French or English as appropriate
# MAGIC
# MAGIC Key reference facts (always accurate even if not in context):
# MAGIC - Kiabi revenue: €2.3B (+5%), 23.7M clients, 298M pieces sold in 2024
# MAGIC - Beebs acquisition: May 2024, 2M family users, 100 French stores with Beebs corners
# MAGIC - Second-hand target: from 0.43% → 50% of items by 2035
# MAGIC - CO2: 2.28 Mt eq. (-4.3% vs 2022), target -25% by 2035
# MAGIC """

# COMMAND ----------

# MAGIC %md
# MAGIC KA_MODEL_CODE = '''
# MAGIC import mlflow
# MAGIC from mlflow.pyfunc import PythonModel
# MAGIC
# MAGIC class KiabiKnowledgeAssistant(PythonModel):
# MAGIC     """RAG model: retrieves from VS index, generates with Claude."""
# MAGIC
# MAGIC     VS_INDEX   = "{vs_index}"
# MAGIC     LLM        = "{llm_endpoint}"
# MAGIC     SYSTEM_PROMPT = """{instructions}"""
# MAGIC
# MAGIC     def load_context(self, context):
# MAGIC         from databricks.sdk import WorkspaceClient
# MAGIC         self._w = WorkspaceClient()
# MAGIC
# MAGIC     def _retrieve(self, question: str, k: int = 5) -> list:
# MAGIC         results = self._w.vector_search_indexes.query_index(
# MAGIC             index_name=self.VS_INDEX,
# MAGIC             columns=["content", "source"],
# MAGIC             query_text=question,
# MAGIC             num_results=k,
# MAGIC         )
# MAGIC         return [
# MAGIC             {{"content": r[0], "source": r[1]}}
# MAGIC             for r in (results.result.data_array or [])
# MAGIC         ]
# MAGIC
# MAGIC     def predict(self, context, model_input, params=None):
# MAGIC         import pandas as pd
# MAGIC         if isinstance(model_input, pd.DataFrame):
# MAGIC             messages = model_input["messages"].iloc[0]
# MAGIC             question = messages[-1]["content"] if isinstance(messages, list) else str(messages)
# MAGIC         else:
# MAGIC             question = str(model_input)
# MAGIC
# MAGIC         chunks = self._retrieve(question)
# MAGIC         ctx_text = "\\n\\n".join(f"[{{c['source']}}] {{c['content']}}" for c in chunks)
# MAGIC         full_system = self.SYSTEM_PROMPT + "\\n\\nRetrieved context:\\n" + ctx_text
# MAGIC
# MAGIC         resp = self._w.serving_endpoints.query(
# MAGIC             name=self.LLM,
# MAGIC             messages=[
# MAGIC                 {{"role": "system",  "content": full_system}},
# MAGIC                 {{"role": "user",    "content": question}},
# MAGIC             ],
# MAGIC             max_tokens=1500,
# MAGIC         )
# MAGIC         return resp.choices[0].message.content
# MAGIC
# MAGIC
# MAGIC mlflow.models.set_model(KiabiKnowledgeAssistant())
# MAGIC '''
# MAGIC
# MAGIC
# MAGIC def deploy_serving_endpoint(
# MAGIC     vs_index: str,
# MAGIC     llm_endpoint: str,
# MAGIC     instructions: str,
# MAGIC     serving_endpoint: str,
# MAGIC     registered_model: str,
# MAGIC ):
# MAGIC     import os
# MAGIC     import tempfile
# MAGIC     import mlflow
# MAGIC     from databricks.sdk import WorkspaceClient
# MAGIC     from databricks.sdk.service.serving import (
# MAGIC         EndpointCoreConfigInput, ServedEntityInput,
# MAGIC     )
# MAGIC     w = WorkspaceClient()
# MAGIC
# MAGIC     code = KA_MODEL_CODE.format(
# MAGIC         vs_index=vs_index,
# MAGIC         llm_endpoint=llm_endpoint,
# MAGIC         instructions=instructions.replace('"""', "'''"),
# MAGIC     )
# MAGIC     with tempfile.TemporaryDirectory() as tmpdir:
# MAGIC         model_path = os.path.join(tmpdir, "model.py")
# MAGIC         with open(model_path, "w") as f:
# MAGIC             f.write(code)
# MAGIC
# MAGIC         mlflow.set_registry_uri("databricks-uc")
# MAGIC         with mlflow.start_run():
# MAGIC             mlflow.pyfunc.log_model(
# MAGIC                 artifact_path="model",
# MAGIC                 python_model=model_path,
# MAGIC                 registered_model_name=registered_model,
# MAGIC                 pip_requirements=["databricks-sdk", "mlflow"],
# MAGIC             )
# MAGIC         print(f"Model registered: {registered_model}")
# MAGIC
# MAGIC     client = mlflow.tracking.MlflowClient()
# MAGIC     versions = client.search_model_versions(f"name='{registered_model}'")
# MAGIC     latest = sorted(versions, key=lambda v: int(v.version))[-1].version
# MAGIC
# MAGIC     try:
# MAGIC         ep = w.serving_endpoints.get(serving_endpoint)
# MAGIC         print(f"Endpoint '{serving_endpoint}' exists — updating...")
# MAGIC         w.serving_endpoints.update_config_and_wait(
# MAGIC             name=serving_endpoint,
# MAGIC             served_entities=[ServedEntityInput(
# MAGIC                 entity_name=registered_model,
# MAGIC                 entity_version=latest,
# MAGIC                 scale_to_zero_enabled=True,
# MAGIC             )],
# MAGIC         )
# MAGIC     except Exception:
# MAGIC         print(f"Creating endpoint '{serving_endpoint}'...")
# MAGIC         w.serving_endpoints.create_and_wait(
# MAGIC             name=serving_endpoint,
# MAGIC             config=EndpointCoreConfigInput(
# MAGIC                 served_entities=[ServedEntityInput(
# MAGIC                     entity_name=registered_model,
# MAGIC                     entity_version=latest,
# MAGIC                     scale_to_zero_enabled=True,
# MAGIC                 )],
# MAGIC             ),
# MAGIC         )
# MAGIC     print(f"Endpoint ready: {serving_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Step 4: Deploy RAG serving endpoint
# MAGIC if not SKIP_ENDPOINT:
# MAGIC     print("Step 4: Deploying RAG serving endpoint...")
# MAGIC     deploy_serving_endpoint(
# MAGIC         vs_index=VS_INDEX,
# MAGIC         llm_endpoint=LLM_ENDPOINT,
# MAGIC         instructions=KA_INSTRUCTIONS,
# MAGIC         serving_endpoint=SERVING_ENDPOINT,
# MAGIC         registered_model=REGISTERED_MODEL,
# MAGIC     )
# MAGIC else:
# MAGIC     print("Endpoint step skipped (SKIP_ENDPOINT=True)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - **Chunks table:** `CHUNKS_TABLE`
# MAGIC - **VS index:** `VS_INDEX`
# MAGIC - **Serving endpoint:** `SERVING_ENDPOINT`
# MAGIC
# MAGIC Query from Python:
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.query("kiabi-knowledge-assistant", messages=[{"role":"user","content":"What is Kiabi's second-hand target?"}])
# MAGIC ```