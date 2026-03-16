"""
Setup Kiabi Strategy Multi-Agent Supervisor.

Deploys a routing agent that combines:
  - Knowledge Assistant endpoint (kiabi-knowledge-assistant): ESG/strategy/competitor docs
  - Genie Space (Kiabi Second-Hand Market Explorer): market data, pricing, listing SQL

The supervisor uses Claude to classify the question and route it to the right agent,
then returns a unified response.

Creates:
  1. MLflow model registered as kiabi_strategy_multi_agent
  2. Model Serving endpoint: kiabi-strategy-multi-agent

Run as a Databricks job:
  databricks bundle run kiabi_mas_setup_job

Re-entrant: updates endpoint if it already exists.

Prerequisites:
  - kiabi-knowledge-assistant endpoint must be ONLINE
  - Genie Space 01f119653f9018bab6ee3e6e95e1605c must exist
"""
import sys

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG  = "nef_catalog"
SCHEMA   = "second_hand"

KA_ENDPOINT       = "kiabi-knowledge-assistant"
GENIE_SPACE_ID    = "01f119653f9018bab6ee3e6e95e1605c"
ROUTER_LLM        = "databricks-claude-sonnet-4-6"
SERVING_ENDPOINT  = "kiabi-strategy-multi-agent"
REGISTERED_MODEL  = f"{CATALOG}.{SCHEMA}.kiabi_strategy_multi_agent"

MAS_INSTRUCTIONS = """You are a strategic advisor for Kiabi, specialising in second-hand fashion.

You have access to two specialist agents:
1. **Knowledge Assistant** — answers questions about Kiabi's ESG strategy, DPEF 2024 report,
   sustainability targets, competitor positioning, and strategic documents.
2. **Market Data Agent** — answers questions about second-hand listing data, prices,
   resell counts, marketplace comparisons, and competitive intelligence from live data.

Route each question to the right agent and return its answer directly.
If a question spans both topics, call both agents and synthesise their answers.

Key context (always accurate):
- Kiabi revenue: €2.3B (+5%), 23.7M clients, 298M pieces sold in 2024
- Beebs acquisition: May 2024, 2M family users, 100 French stores with Beebs corners
- Second-hand target: from 0.43% → 50% of items by 2035
- CO2: 2.28 Mt eq. (-4.3% vs 2022), target -25% by 2035
"""

# ── Model code (embedded, formatted at deploy time) ───────────────────────────

MAS_MODEL_CODE = '''
import mlflow
from mlflow.pyfunc import PythonModel


ROUTER_PROMPT = """You are a routing agent. Given a user question, decide which agent(s) to call.

Available agents:
- "knowledge" : ESG strategy, DPEF report, sustainability targets, competitor analysis, Beebs acquisition, strategic documents
- "genie"     : Second-hand listing data, prices, resell counts, marketplace volumes, brand comparison data, SQL analytics
- "both"      : Questions that need both strategy context AND live market data

Reply with exactly one word: knowledge, genie, or both.
"""


class KiabiMultiAgentSupervisor(PythonModel):
    """Routes queries between KA endpoint (ESG/docs) and Genie Space (market data)."""

    KA_ENDPOINT    = "{ka_endpoint}"
    GENIE_SPACE_ID = "{genie_space_id}"
    ROUTER_LLM     = "{router_llm}"
    SYSTEM_PROMPT  = """{instructions}"""

    def load_context(self, context):
        from databricks.sdk import WorkspaceClient
        self._w = WorkspaceClient()

    def _extract_question(self, model_input) -> str:
        import pandas as pd
        if isinstance(model_input, pd.DataFrame):
            messages = model_input["messages"].iloc[0]
            if isinstance(messages, list):
                return messages[-1]["content"]
            return str(messages)
        return str(model_input)

    def _route(self, question: str) -> str:
        """Ask Claude which agent to use."""
        resp = self._w.serving_endpoints.query(
            name=self.ROUTER_LLM,
            messages=[
                {{"role": "system",  "content": ROUTER_PROMPT}},
                {{"role": "user",    "content": question}},
            ],
            max_tokens=10,
        )
        return resp.choices[0].message.content.strip().lower()

    def _call_knowledge_assistant(self, question: str) -> str:
        resp = self._w.serving_endpoints.query(
            name=self.KA_ENDPOINT,
            messages=[{{"role": "user", "content": question}}],
            max_tokens=1500,
        )
        return resp.choices[0].message.content

    def _call_genie(self, question: str) -> str:
        """Query Genie Space via Conversation API."""
        import time
        # Start conversation
        conv = self._w.genie.start_conversation(
            space_id=self.GENIE_SPACE_ID,
            content=question,
        )
        conv_id = conv.conversation_id
        msg_id  = conv.message_id

        # Poll until complete
        for _ in range(30):
            time.sleep(3)
            msg = self._w.genie.get_message(
                space_id=self.GENIE_SPACE_ID,
                conversation_id=conv_id,
                message_id=msg_id,
            )
            if msg.status.value in ("COMPLETED", "FAILED", "QUERY_RESULT_EXPIRED"):
                break

        # Extract result
        if msg.attachments:
            for att in msg.attachments:
                if att.query and att.query.result:
                    rows = att.query.result.data_array or []
                    cols = [c.name for c in (att.query.result.statement_response.manifest.schema.columns or [])]
                    if rows:
                        header = " | ".join(cols)
                        lines  = [" | ".join(str(v) for v in row) for row in rows[:20]]
                        return f"**Market data:**\\n{{header}}\\n" + "\\n".join(lines)
            # Fallback: text attachment
            for att in msg.attachments:
                if att.text:
                    return att.text.content or ""

        return "No data available from the market database for this query."

    def _synthesise(self, question: str, ka_answer: str, genie_answer: str) -> str:
        """Combine answers from both agents into a unified response."""
        combined_prompt = (
            f"User question: {{question}}\\n\\n"
            f"Strategy & ESG context:\\n{{ka_answer}}\\n\\n"
            f"Market data:\\n{{genie_answer}}\\n\\n"
            "Synthesise these into one clear, actionable answer."
        )
        resp = self._w.serving_endpoints.query(
            name=self.ROUTER_LLM,
            messages=[
                {{"role": "system",  "content": self.SYSTEM_PROMPT}},
                {{"role": "user",    "content": combined_prompt}},
            ],
            max_tokens=2000,
        )
        return resp.choices[0].message.content

    def predict(self, context, model_input, params=None):
        question = self._extract_question(model_input)
        route    = self._route(question)

        if "both" in route:
            ka_answer    = self._call_knowledge_assistant(question)
            genie_answer = self._call_genie(question)
            return self._synthesise(question, ka_answer, genie_answer)
        elif "genie" in route:
            return self._call_genie(question)
        else:
            return self._call_knowledge_assistant(question)


mlflow.models.set_model(KiabiMultiAgentSupervisor())
'''


def deploy_multi_agent_endpoint(
    ka_endpoint: str,
    genie_space_id: str,
    router_llm: str,
    instructions: str,
    serving_endpoint: str,
    registered_model: str,
):
    import mlflow
    import os
    import tempfile
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    w = WorkspaceClient()

    code = MAS_MODEL_CODE.format(
        ka_endpoint=ka_endpoint,
        genie_space_id=genie_space_id,
        router_llm=router_llm,
        instructions=instructions.replace('"""', "'''"),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "model.py")
        with open(model_path, "w") as f:
            f.write(code)

        mlflow.set_registry_uri("databricks-uc")
        with mlflow.start_run():
            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=model_path,
                registered_model_name=registered_model,
                pip_requirements=["databricks-sdk", "mlflow"],
            )
        print(f"Model registered: {registered_model}")

    # Get latest version
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{registered_model}'")
    latest = sorted(versions, key=lambda v: int(v.version))[-1].version

    # Create or update serving endpoint
    try:
        w.serving_endpoints.get(serving_endpoint)
        print(f"Endpoint '{serving_endpoint}' exists — updating...")
        w.serving_endpoints.update_config_and_wait(
            name=serving_endpoint,
            served_entities=[ServedEntityInput(
                entity_name=registered_model,
                entity_version=latest,
                scale_to_zero_enabled=True,
            )],
        )
    except Exception:
        print(f"Creating endpoint '{serving_endpoint}'...")
        w.serving_endpoints.create_and_wait(
            name=serving_endpoint,
            config=EndpointCoreConfigInput(
                served_entities=[ServedEntityInput(
                    entity_name=registered_model,
                    entity_version=latest,
                    scale_to_zero_enabled=True,
                )],
            ),
        )
    print(f"Endpoint ready: {serving_endpoint}")


def main():
    print("Deploying Kiabi Strategy Multi-Agent Supervisor...")
    deploy_multi_agent_endpoint(
        ka_endpoint=KA_ENDPOINT,
        genie_space_id=GENIE_SPACE_ID,
        router_llm=ROUTER_LLM,
        instructions=MAS_INSTRUCTIONS,
        serving_endpoint=SERVING_ENDPOINT,
        registered_model=REGISTERED_MODEL,
    )
    print("\nMulti-Agent Supervisor ready!")
    print(f"  Endpoint: {SERVING_ENDPOINT}")
    print(f"  Routes:   strategy/ESG → {KA_ENDPOINT}")
    print(f"            market data  → Genie Space {GENIE_SPACE_ID}")
    print(f"\nQuery with:")
    print(f'  w.serving_endpoints.query("{SERVING_ENDPOINT}", messages=[{{"role":"user","content":"..."}}])')


if __name__ == "__main__":
    main()
