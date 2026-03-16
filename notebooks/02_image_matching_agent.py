# Databricks notebook source
# MAGIC %md
# MAGIC # Kiabi — Listing Image → Essentials Matching Agent
# MAGIC
# MAGIC Given a second-hand listing image URL, this agent:
# MAGIC 1. Downloads the image and asks **Claude Sonnet 4.6** (multimodal) to extract structured
# MAGIC    clothing attributes (category, color, style, pattern, gender, age group).
# MAGIC 2. Queries the **essentials Vector Search index** for the closest catalog items.
# MAGIC 3. Returns the top-3 matches with similarity scores and product links.
# MAGIC
# MAGIC The agent is registered in MLflow and deployed as a **Model Serving endpoint**.

# COMMAND ----------
# Dependencies are declared in the job environment spec (jobs.yml: agent_env).
# No %pip install needed when running as a job.

# COMMAND ----------

CATALOG  = "nef_catalog"
SCHEMA   = "second_hand"
VS_ENDPOINT  = "kiabi_knowledge_vs"
INDEX_NAME   = f"{CATALOG}.{SCHEMA}.essentials_vs_index"
CLAUDE_MODEL = "databricks-claude-sonnet-4-6"
AGENT_MODEL_NAME = f"{CATALOG}.{SCHEMA}.kiabi_image_matching_agent"
SERVING_ENDPOINT = "kiabi-image-matching"

# COMMAND ----------
# MAGIC %md ## 1 — Define the agent

# COMMAND ----------

import os, json, base64, logging, urllib.request, urllib.error
from typing import Any

import mlflow
import mlflow.pyfunc

from databricks.vector_search.client import VectorSearchClient

log = logging.getLogger(__name__)


DESCRIBE_PROMPT = """You are a fashion catalog assistant. Analyse this clothing item image and reply with a JSON object ONLY — no prose, no markdown fences.

Required fields:
{
  "category": "e.g. T-shirt, Pantalon, Robe, Veste, Chaussures …",
  "color": "primary color in French, e.g. Blanc, Bleu marine, Rouge …",
  "style": "e.g. casual, sportswear, formel, basique …",
  "pattern": "e.g. uni, rayé, à fleurs, imprimé, carreaux … (uni if plain)",
  "gender": "Homme / Femme / Enfant / Bébé / Mixte",
  "age_group": "adulte / enfant / bébé",
  "search_text": "a concise French description combining all attributes for semantic search, e.g. 'T-shirt blanc uni col rond femme basique'"
}

If the image is unclear, make your best guess based on visible elements."""


class KiabiImageMatchingAgent(mlflow.pyfunc.PythonModel):
    """MLflow PythonModel — takes a listing image URL, returns matched Kiabi essentials."""

    # Baked in at pickle time so load_context works in model serving
    # where DATABRICKS_HOST is not injected automatically.
    _WORKSPACE_HOST = "fevm-nef.cloud.databricks.com"
    _VS_ENDPOINT = VS_ENDPOINT
    _INDEX_NAME = INDEX_NAME
    _CLAUDE_MODEL = CLAUDE_MODEL

    def load_context(self, context: mlflow.pyfunc.PythonModelContext) -> None:
        # Keep load_context empty so the container passes the startup health-check
        # immediately. All clients are initialised lazily on the first predict() call.
        self._ready = False

    def _ensure_ready(self) -> None:
        if self._ready:
            return
        from databricks.sdk import WorkspaceClient
        self._w = WorkspaceClient()
        self._host = self._w.config.host.rstrip("/")
        self._vsc = VectorSearchClient(disable_notice=True)
        self._index = self._vsc.get_index(
            endpoint_name=self._VS_ENDPOINT,
            index_name=self._INDEX_NAME,
        )
        self._ready = True

    def _get_token(self) -> str:
        """Get a fresh bearer token from the SDK credential provider."""
        _creds = self._w.config.authenticate()
        _headers = _creds() if callable(_creds) else _creds
        return _headers.get("Authorization", "").removeprefix("Bearer ").strip()

    # ── internal helpers ────────────────────────────────────────────────────

    def _http_get(self, url: str) -> bytes:
        """Download URL, returning raw bytes."""
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read(), r.headers.get("Content-Type", "image/jpeg")

    def _fetch_image_b64(self, url: str) -> tuple[str, str]:
        """Download image and return (base64_data, mime_type)."""
        try:
            content, ct = self._http_get(url)
            mime = ct.split(";")[0].strip()
            if mime not in ("image/jpeg", "image/png", "image/gif", "image/webp"):
                mime = "image/jpeg"
            return base64.standard_b64encode(content).decode(), mime
        except Exception as e:
            raise ValueError(f"Could not fetch image from {url}: {e}") from e

    def _describe_image(self, image_url: str) -> dict:
        """Call Claude Sonnet 4.6 vision via Databricks Foundation Model API (OpenAI format)."""
        b64, mime = self._fetch_image_b64(image_url)
        data_uri = f"data:{mime};base64,{b64}"
        payload = json.dumps({
            "model": self._CLAUDE_MODEL,
            "max_tokens": 400,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": data_uri}},
                    {"type": "text", "text": DESCRIBE_PROMPT},
                ],
            }],
        }).encode()
        req = urllib.request.Request(
            f"{self._host}/serving-endpoints/{self._CLAUDE_MODEL}/invocations",
            data=payload,
            headers={
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            resp_data = json.loads(r.read())
        raw = resp_data["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Could not parse Claude response as JSON: %s", raw)
            return {"search_text": raw, "category": "", "color": "", "gender": ""}

    def _search_essentials(self, search_text: str, num_results: int = 3) -> list[dict]:
        """Query Vector Search index and return top matching essentials."""
        results = self._index.similarity_search(
            query_text=search_text,
            columns=["id", "title", "category", "color", "universe",
                     "price", "product_url", "primary_image_url"],
            num_results=num_results,
        )
        cols = results["manifest"]["columns"]
        col_names = [c["name"] for c in cols]
        rows = []
        for row in results["result"]["data_array"]:
            rows.append(dict(zip(col_names, row)))
        # Add similarity score if returned
        score_col = next((c["name"] for c in cols if "score" in c["name"].lower()), None)
        if score_col:
            for r in rows:
                r["similarity_score"] = round(float(r.pop(score_col, 0)), 4)
        return rows

    # ── main predict ────────────────────────────────────────────────────────

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        model_input: Any,
        params: dict | None = None,
    ) -> dict:
        """
        Input (dict or single-row DataFrame):
          {
            "image_url": "https://...",          # listing photo
            "num_results": 3                      # optional, default 3
          }

        Output:
          {
            "image_url": "...",
            "extracted_attributes": { category, color, style, … },
            "matches": [
              { id, title, category, color, price, product_url, primary_image_url, similarity_score },
              …
            ]
          }
        """
        self._ensure_ready()
        # Accept dict or DataFrame
        if hasattr(model_input, "to_dict"):
            row = model_input.to_dict(orient="records")[0]
        else:
            row = model_input

        image_url  = row["image_url"]
        num_results = int(row.get("num_results", 3))

        # Step 1 — vision description
        attributes = self._describe_image(image_url)
        search_text = attributes.get("search_text") or " ".join(filter(None, [
            attributes.get("category"), attributes.get("color"), attributes.get("gender"),
        ]))

        # Step 2 — vector search
        matches = self._search_essentials(search_text, num_results=num_results)

        # Return as a list so MLflow wraps as {"predictions": [result]}
        # which is what ai_query expects (single-element array).
        return [{
            "image_url": image_url,
            "extracted_attributes": json.dumps(attributes, ensure_ascii=False),
            "matches": json.dumps(matches, ensure_ascii=False),
        }]


# COMMAND ----------
# MAGIC %md ## 2 — Smoke test (optional — skipped if image unreachable)

# COMMAND ----------

import os

# Use a real Vinted/LeBonCoin listing image for testing
TEST_IMAGE = "https://images1.vinted.net/t/03_01fde_CeC5T5U14CeUmkqVLqGCKe5j/f800/1739194819.jpeg?s=cc3e906ea41be6e4e59dd7ff9789be87e1f4ad1a"

os.environ.setdefault("DATABRICKS_HOST", "fevm-nef.cloud.databricks.com")

class _FakeContext:
    artifacts = {}

smoke_result = None
try:
    agent = KiabiImageMatchingAgent()
    agent.load_context(_FakeContext())
    smoke_result = agent.predict(_FakeContext(), {"image_url": TEST_IMAGE, "num_results": 3})
    print("=== Smoke test: Extracted attributes ===")
    print(json.dumps(smoke_result["extracted_attributes"], indent=2, ensure_ascii=False))
    print("\n=== Top matches ===")
    for i, m in enumerate(smoke_result["matches"], 1):
        print(f"  {i}. {m.get('title')} | {m.get('color')} | €{m.get('price')} | score={m.get('similarity_score')}")
        print(f"     {m.get('product_url')}")
except Exception as _e:
    print(f"Smoke test skipped (non-fatal): {_e}")

# COMMAND ----------
# MAGIC %md ## 3 — Log and register in MLflow

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
import pandas as pd

mlflow.set_registry_uri("databricks-uc")

sample_input = pd.DataFrame([{"image_url": TEST_IMAGE, "num_results": 3}])

# Build a minimal sample output (use smoke_result if available, else synthetic)
if smoke_result:
    _out_attrs = str(smoke_result["extracted_attributes"])
    _out_matches = str(smoke_result["matches"])
else:
    _out_attrs = '{"category": "T-shirt", "color": "Blanc", "gender": "Femme", "search_text": "T-shirt blanc femme"}'
    _out_matches = "[{}]"

from mlflow.models.resources import DatabricksVectorSearchIndex, DatabricksServingEndpoint

with mlflow.start_run(run_name="kiabi_image_matching_agent") as run:
    # Output is a list with one dict (strings for nested fields)
    signature = infer_signature(
        sample_input,
        [{"image_url": TEST_IMAGE,
          "extracted_attributes": _out_attrs,
          "matches": _out_matches}],
    )
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=KiabiImageMatchingAgent(),
        pip_requirements=[
            "databricks-vectorsearch",
            "databricks-sdk>=0.20",
        ],
        signature=signature,
        registered_model_name=AGENT_MODEL_NAME,
        # Declare resource dependencies so Databricks auto-provisions
        # DATABRICKS_HOST and DATABRICKS_TOKEN in the serving container.
        resources=[
            DatabricksVectorSearchIndex(index_name=INDEX_NAME),
            DatabricksServingEndpoint(endpoint_name=CLAUDE_MODEL),
        ],
    )

print(f"Logged model: {model_info.model_uri}")
print(f"Registered as: {AGENT_MODEL_NAME}")

# COMMAND ----------
# MAGIC %md ## 4 — Deploy to Model Serving

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedModelInput, ServedModelInputWorkloadSize,
)
import time

w = WorkspaceClient()

# Get latest version
client = mlflow.tracking.MlflowClient()
versions = client.search_model_versions(f"name='{AGENT_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Deploying version {latest_version} of {AGENT_MODEL_NAME}")

config = EndpointCoreConfigInput(
    served_models=[
        ServedModelInput(
            name="kiabi-image-matching-v1",
            model_name=AGENT_MODEL_NAME,
            model_version=str(latest_version),
            workload_size=ServedModelInputWorkloadSize.SMALL,
            scale_to_zero_enabled=True,
        )
    ]
)

try:
    w.serving_endpoints.create(name=SERVING_ENDPOINT, config=config)
    print(f"Creating endpoint {SERVING_ENDPOINT} …")
except Exception:
    w.serving_endpoints.update_config(name=SERVING_ENDPOINT, served_models=config.served_models)
    print(f"Updating endpoint {SERVING_ENDPOINT} …")

# Poll for ready
for _ in range(60):
    ep = w.serving_endpoints.get(name=SERVING_ENDPOINT)
    state = ep.state.config_update.value if ep.state.config_update else "UNKNOWN"
    print(f"  {state}")
    if state == "NOT_UPDATING":
        break
    time.sleep(15)

print(f"\nEndpoint ready: https://{os.environ['DATABRICKS_HOST']}/serving-endpoints/{SERVING_ENDPOINT}")

# COMMAND ----------
# MAGIC %md ## 5 — Test the deployed endpoint

# COMMAND ----------

import requests as req

host = os.environ["DATABRICKS_HOST"]
# DATABRICKS_TOKEN is not set in serverless jobs; fall back to dbutils notebook token
token = os.environ.get("DATABRICKS_TOKEN") or dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

resp = req.post(
    f"https://{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"dataframe_records": [{"image_url": TEST_IMAGE, "num_results": 3}]},
    timeout=60,
)
print("Status:", resp.status_code)
out = resp.json()
print("Attributes:", out.get("predictions", [{}])[0].get("extracted_attributes"))
print("Matches:")
for m in out.get("predictions", [{}])[0].get("matches", []):
    print(f"  - {m.get('title')} ({m.get('color')}) score={m.get('similarity_score')}")
