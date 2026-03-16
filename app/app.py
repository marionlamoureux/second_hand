"""
Kiabi Second-Hand Market Intelligence Dashboard
"""

import os, re, math, base64, requests
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

# ─── Static assets ─────────────────────────────────────────────────────────────
_STATIC = Path(__file__).parent / "static"

def _b64(path: Path) -> str:
    try:
        return base64.b64encode(path.read_bytes()).decode()
    except Exception:
        return ""

_BG_B64        = _b64(_STATIC / "background.png")
_LOGO_B64      = _b64(_STATIC / "kiabi_logo.png")
_LBC_B64       = _b64(_STATIC / "LeBonCoin_logo.png")
_EBAY_B64      = _b64(_STATIC / "ebay.png")
_VESTIAIRE_B64 = _b64(_STATIC / "vestaire_collective_logo.png")
_VINTED_B64    = _b64(_STATIC / "Vinted-Logo.jpg")
_BEEBS_B64     = _b64(_STATIC / "logo_beebs.jpeg")
_MARION_B64    = _b64(_STATIC / "workpicture-marionlamoureux.png")

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Kiabi | Market Intelligence", page_icon="🏷️", layout="wide", initial_sidebar_state="collapsed")

# ─── Global CSS ───────────────────────────────────────────────────────────────
# 1) Background image (injected as f-string so we can embed base64)
if _BG_B64:
    st.markdown(
        "<style>"
        "[data-testid='stAppViewContainer']{"
        f"background-image:url('data:image/png;base64,{_BG_B64}');"
        "background-size:cover;background-position:center;background-attachment:fixed;}"
        "[data-testid='stAppViewContainer']::before{"
        "content:'';position:fixed;inset:0;"
        "background:rgba(6,12,28,0.82);z-index:0;pointer-events:none;}"
        "[data-testid='stAppViewContainer']>*{position:relative;z-index:1;}"
        "[data-testid='stHeader']{background:rgba(6,12,28,0.6);backdrop-filter:blur(8px);}"
        "</style>",
        unsafe_allow_html=True,
    )

# 2) Main component styles (plain string — no f-string needed)
st.markdown("""<style>
[data-testid="stAppViewContainer"] { background-color:#060C1C; }
[data-testid="stHeader"]           { background-color:#060C1C; }
.block-container                   { padding-top:1.2rem; max-width:100%; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background:#0A1225; border-radius:10px; padding:4px; gap:4px; border:none; }
.stTabs [data-baseweb="tab"]      { color:#E0E6F0; border-radius:8px; padding:10px 26px; font-weight:700; font-size:18px; }
.stTabs [aria-selected="true"]    { background:linear-gradient(135deg,#1E3A6E,#2563EB) !important; color:white !important; }

/* Metric cards */
div[data-testid="metric-container"] {
    background:linear-gradient(135deg,#0D1A35 0%,#0A1628 100%);
    border-radius:14px; padding:24px 26px; border:1px solid #1A2A45;
}
div[data-testid="metric-container"] label,
div[data-testid="metric-container"] label *,
div[data-testid="metric-container"] label div,
div[data-testid="metric-container"] label p,
div[data-testid="metric-container"] [data-testid="stMetricLabel"],
div[data-testid="metric-container"] [data-testid="stMetricLabel"] *,
div[data-testid="metric-container"] [data-testid="stMetricLabel"] div,
div[data-testid="metric-container"] [data-testid="stMetricLabel"] p { color:white !important; font-size:20px !important; text-transform:uppercase; letter-spacing:.8px; font-weight:700 !important; }
div[data-testid="metric-container"] [data-testid="stMetricValue"],
div[data-testid="metric-container"] [data-testid="stMetricValue"] *,
div[data-testid="metric-container"] [data-testid="stMetricValue"] div,
div[data-testid="metric-container"] [data-testid="stMetricValue"] p,
div[data-testid="metric-container"] > div:nth-child(2),
div[data-testid="metric-container"] > div:nth-child(2) *,
div[data-testid="metric-container"] > div > div:last-of-type,
div[data-testid="metric-container"] > div > div:last-of-type * { color:#2563EB !important; font-size:44px !important; font-weight:800 !important; }
div[data-testid="metric-container"] [data-testid="stMetricDelta"],
div[data-testid="metric-container"] [data-testid="stMetricDelta"] * { color:#22C55E !important; font-size:18px !important; }

/* Typography */
h1,h2,h3,h4,h5 { color:white !important; }
p, li { color:#EEF0F6; font-size:18px; }
hr { border-color:#1A2A45; margin:16px 0; }

/* Suggested-question buttons */
.stButton > button {
    background:#2563EB !important; color:white !important;
    border:none !important; border-radius:10px !important;
    font-size:15px !important; font-weight:600 !important;
    padding:12px 18px !important; line-height:1.4 !important;
    white-space:normal !important; text-align:left !important;
}
.stButton > button:hover { background:#1D4ED8 !important; }
.stButton > button:active, .stButton > button:focus { background:#1E40AF !important; }

/* Sidebar (Genie popup chat) */
[data-testid="stSidebar"] { background:rgba(6,12,28,0.98) !important; border-right:none !important; border-left:1px solid #1E3A6E !important; }
[data-testid="stSidebar"] [data-testid="stChatMessage"] { background:#0D1A35 !important; border-radius:10px; margin-bottom:8px; }

/* Floating Genie FAB */
.genie-fab-wrap { position:fixed; bottom:24px; right:24px; z-index:999999; }
.genie-fab {
    width:56px; height:56px; border-radius:50%;
    background:linear-gradient(135deg,#2563EB,#1E3A6E);
    border:2px solid #3B82F6; box-shadow:0 4px 20px rgba(37,99,235,0.5);
    cursor:pointer; display:flex; align-items:center; justify-content:center;
    font-size:24px; transition:all 0.3s ease; color:white;
}
.genie-fab:hover { transform:scale(1.1); box-shadow:0 6px 28px rgba(37,99,235,0.7); }
.genie-fab-tip {
    position:absolute; right:66px; top:50%; transform:translateY(-50%);
    background:#0D1A35; color:white; padding:6px 14px; border-radius:8px;
    font-size:13px; font-weight:600; white-space:nowrap; border:1px solid #1E3A6E;
    opacity:0; transition:opacity 0.3s; pointer-events:none;
}
.genie-fab-wrap:hover .genie-fab-tip { opacity:1; }

/* Inputs */
.stSelectbox > div > div, .stTextInput > div > div > input {
    background:#0A1628 !important; border-color:#1A2A45 !important; color:white !important;
}
.stSlider [data-testid="stTickBar"] { color:#E0E6F0; }

/* Hide streamlit default table styling */
.dataframe { display:none; }

/* Row card component */
.row-card {
    display:flex; align-items:center; gap:16px;
    padding:16px 20px; border-bottom:1px solid #0F1E38;
}
.row-card:last-child { border-bottom:none; }
.row-card:hover { background:rgba(59,130,246,.05); border-radius:10px; }
.logo-circle {
    width:50px; height:50px; border-radius:50%; flex-shrink:0;
    object-fit:contain; background:#0D1A35; padding:6px;
    border:1px solid #1E3A6E;
}
.logo-letter {
    width:50px; height:50px; border-radius:50%; flex-shrink:0;
    background:linear-gradient(135deg,#1E3A6E,#2563EB);
    display:flex; align-items:center; justify-content:center;
    font-size:20px; font-weight:800; color:white;
}
.cell-name { color:white; font-weight:600; font-size:18px; line-height:1.3; }
.cell-sub  { color:#E0E6F0; font-size:16px; margin-top:3px; }
.cell-num  { color:white; font-size:18px; font-weight:600; min-width:80px; text-align:right; }
.cell-num2 { color:#E0E6F0; font-size:15px; min-width:80px; text-align:right; margin-top:2px; }
.badge-g { background:rgba(34,197,94,.12); color:#22C55E; border-radius:6px; padding:4px 12px; font-size:12px; font-weight:600; white-space:nowrap; }
.badge-b { background:rgba(59,130,246,.12); color:#60A5FA; border-radius:6px; padding:4px 12px; font-size:12px; font-weight:600; white-space:nowrap; }
.badge-o { background:rgba(251,146,60,.12); color:#FB923C; border-radius:6px; padding:4px 12px; font-size:12px; font-weight:600; white-space:nowrap; }
.badge-r { background:rgba(239,68,68,.12); color:#F87171; border-radius:6px; padding:4px 12px; font-size:12px; font-weight:600; white-space:nowrap; }

.section-label { color:white; font-size:16px; text-transform:uppercase; letter-spacing:.8px; font-weight:700; margin-bottom:10px; }
.table-wrap { background:#08121E; border-radius:14px; border:1px solid #1A2A45; overflow:hidden; }
.table-head {
    display:flex; align-items:center; gap:16px;
    padding:10px 20px; border-bottom:1px solid #1A2A45;
}
.th { color:white; font-size:14px; text-transform:uppercase; letter-spacing:.8px; font-weight:700; }
.highlight-pill {
    display:inline-block; background:linear-gradient(135deg,#1E3A6E,#2563EB);
    color:white; border-radius:8px; padding:8px 16px;
    font-size:16px; font-weight:600; margin:2px 4px;
}
.insight-card {
    background:linear-gradient(135deg,#0D1A35,#0A1628);
    border:1px solid #1E3A6E; border-radius:14px; padding:20px 24px;
}
.big-num { color:#2563EB; font-size:42px; font-weight:800; letter-spacing:-1px; }
.big-label { color:white; font-size:16px; text-transform:uppercase; letter-spacing:.8px; margin-top:4px; }
</style>""", unsafe_allow_html=True)

# ─── Floating Genie FAB button ──────────────────────────────────────────────
st.markdown("""
<div class="genie-fab-wrap">
    <div class="genie-fab-tip">Ask Genie</div>
    <button class="genie-fab" onclick="
        var el = document.querySelector('[data-testid=&quot;stSidebarCollapsedControl&quot;]');
        if (el) { var b = el.querySelector('button'); if(b) b.click(); else el.click(); return; }
        var cl = document.querySelector('section[data-testid=&quot;stSidebar&quot;] button[kind=&quot;header&quot;]');
        if (cl) cl.click();
    ">✨</button>
</div>
""", unsafe_allow_html=True)

# ─── Constants ─────────────────────────────────────────────────────────────────
WAREHOUSE_ID    = os.getenv("WAREHOUSE_ID", "beb00aeaaa803c3e")
GENIE_SPACE_ID  = os.getenv("GENIE_SPACE_ID", "01f119653f9018bab6ee3e6e95e1605c")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "fevm-nef.cloud.databricks.com").replace("https://","").replace("http://","")
CATALOG, SCHEMA = "nef_catalog", "second_hand"
LLM_ENDPOINT    = os.getenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-6")
# App profile: "full" = all tabs; "light" = Market Overview + Strategy Assistant only (set APP_PROFILE=light)
APP_PROFILE     = (os.getenv("APP_PROFILE") or "full").strip().lower()
LIGHT_PROFILE_TABS = frozenset({"Market Overview", "Strategy Assistant"})
KNOWLEDGE_DOC_PATH = "/Volumes/nef_catalog/second_hand/kiabi_landing/knowledge/kiabi_secondhand_strategy_knowledge.md"
SOURCE_COLORS = {"leboncoin": "#3B82F6", "vinted": "#06B6D4", "vestiaire": "#9333EA", "ebay": "#F59E0B", "label_emmaus": "#22C55E", "facebook": "#2563EB"}
def _data_uri(b64: str, mime: str = "image/png") -> str:
    return f"data:{mime};base64,{b64}" if b64 else ""

LOGOS = {
    "leboncoin": _data_uri(_LBC_B64),
    "ebay":      _data_uri(_EBAY_B64),
    "vestiaire": _data_uri(_VESTIAIRE_B64),
    "vinted":    _data_uri(_VINTED_B64, "image/jpeg"),
    "beebs":     _data_uri(_BEEBS_B64, "image/jpeg"),
}

CATEGORY_EXPR = """CASE
  WHEN LOWER(title) RLIKE 'lot de|lot [0-9]|pack de|[0-9]+ pièces|[0-9]+ articles|x[0-9]' THEN 'Lot / Bundle'
  WHEN LOWER(title) RLIKE 'bébé|bebe|naissance|nouveau.né|0.mois|3.mois|6.mois|12.mois|18.mois' THEN 'Bébé'
  WHEN LOWER(title) RLIKE 'chaussure|basket|botte|sandale|sneaker|espadrille' THEN 'Chaussures'
  WHEN LOWER(title) RLIKE 'veste|manteau|blouson|anorak|doudoune|parka' THEN 'Vestes & Manteaux'
  WHEN LOWER(title) RLIKE 'robe|jupe|combinaison' THEN 'Robes & Jupes'
  WHEN LOWER(title) RLIKE 'jean|pantalon|legging|short|bermuda|jogging|cargo' THEN 'Pantalons & Jeans'
  WHEN LOWER(title) RLIKE 't-shirt|tshirt|polo|top|débardeur|chemise|pull|sweat|gilet|hoodie' THEN 'Hauts & Pulls'
  WHEN LOWER(title) RLIKE 'sac|bonnet|écharpe|gants|ceinture|chapeau|casquette|collant' THEN 'Accessoires'
  WHEN LOWER(title) RLIKE 'pyjama|sous.vête|lingerie|boxer|slip' THEN 'Sous-vêt. & Nuit'
  WHEN LOWER(title) RLIKE 'sport|yoga|running|fitness' THEN 'Sport'
  ELSE 'Vêtements divers'
END"""

# Approximate Kiabi retail price (new, €) per category — sourced from kiabi.com
RETAIL_PRICES = {
    "Pantalons & Jeans":  16.0, "Hauts & Pulls":  9.0, "Bébé":          14.0,
    "Lot / Bundle":       25.0, "Robes & Jupes": 12.0, "Vestes & Manteaux": 24.0,
    "Sous-vêt. & Nuit":   8.0, "Chaussures":    16.0, "Accessoires":    8.0,
    "Sport":              14.0, "Vêtements divers": 12.0,
}

# KIABI_STORES removed — stores loaded from gold_kiabi_stores table at runtime (see load_stores())
_KIABI_STORES_PLACEHOLDER = [
    {"name":"Kiabi Vélizy",        "city":"Vélizy",         "lat":48.7743, "lng":2.1760},
    {"name":"Kiabi Évry",          "city":"Évry",           "lat":48.6310, "lng":2.4459},
    {"name":"Kiabi Créteil",       "city":"Créteil",        "lat":48.7767, "lng":2.4596},
    {"name":"Kiabi Cergy",         "city":"Cergy",          "lat":49.0370, "lng":2.0785},
    {"name":"Kiabi Rosny",         "city":"Rosny-s-Bois",   "lat":48.8721, "lng":2.5128},
    {"name":"Kiabi Bobigny",       "city":"Bobigny",        "lat":48.9103, "lng":2.4432},
    {"name":"Kiabi Les Ulis",      "city":"Les Ulis",       "lat":48.6757, "lng":2.1718},
    {"name":"Kiabi Saint-Denis",   "city":"Saint-Denis",    "lat":48.9224, "lng":2.3579},
    {"name":"Kiabi Pontault",      "city":"Pontault-C.",    "lat":48.7875, "lng":2.6047},
    {"name":"Kiabi Roissy",        "city":"Roissy",         "lat":48.9908, "lng":2.5531},
    # Hauts-de-France
    {"name":"Kiabi Englos",        "city":"Englos",         "lat":50.6280, "lng":2.9640},
    {"name":"Kiabi Euralille",     "city":"Lille",          "lat":50.6373, "lng":3.0756},
    {"name":"Kiabi Amiens",        "city":"Amiens",         "lat":49.8953, "lng":2.2975},
    {"name":"Kiabi Lens",          "city":"Lens",           "lat":50.4311, "lng":2.8344},
    {"name":"Kiabi Valenciennes",  "city":"Valenciennes",   "lat":50.3589, "lng":3.5235},
    {"name":"Kiabi Dunkerque",     "city":"Dunkerque",      "lat":51.0342, "lng":2.3768},
    {"name":"Kiabi Douai",         "city":"Douai",          "lat":50.3714, "lng":3.0805},
    # Normandie
    {"name":"Kiabi Rouen",         "city":"Rouen",          "lat":49.4432, "lng":1.0993},
    {"name":"Kiabi Caen",          "city":"Caen",           "lat":49.1829, "lng":-0.3707},
    {"name":"Kiabi Le Havre",      "city":"Le Havre",       "lat":49.4938, "lng":0.1077},
    {"name":"Kiabi Cherbourg",     "city":"Cherbourg",      "lat":49.6333, "lng":-1.6167},
    {"name":"Kiabi Évreux",        "city":"Évreux",         "lat":49.0282, "lng":1.1506},
    # Bretagne
    {"name":"Kiabi Rennes",        "city":"Rennes",         "lat":48.0833, "lng":-1.6833},
    {"name":"Kiabi Brest",         "city":"Brest",          "lat":48.3905, "lng":-4.4860},
    {"name":"Kiabi Quimper",       "city":"Quimper",        "lat":48.0000, "lng":-4.0833},
    {"name":"Kiabi Lorient",       "city":"Lorient",        "lat":47.7500, "lng":-3.3667},
    {"name":"Kiabi Vannes",        "city":"Vannes",         "lat":47.6586, "lng":-2.7603},
    # Pays de la Loire
    {"name":"Kiabi Nantes",        "city":"Nantes",         "lat":47.2184, "lng":-1.5536},
    {"name":"Kiabi Saint-Nazaire", "city":"Saint-Nazaire",  "lat":47.2737, "lng":-2.2138},
    {"name":"Kiabi Angers",        "city":"Angers",         "lat":47.4784, "lng":-0.5632},
    {"name":"Kiabi Le Mans",       "city":"Le Mans",        "lat":48.0061, "lng":0.1996},
    {"name":"Kiabi La Roche",      "city":"La Roche-s-Yon", "lat":46.6705, "lng":-1.4262},
    # Centre-Val de Loire
    {"name":"Kiabi Tours",         "city":"Tours",          "lat":47.3941, "lng":0.6848},
    {"name":"Kiabi Orléans",       "city":"Orléans",        "lat":47.9029, "lng":1.9039},
    {"name":"Kiabi Bourges",       "city":"Bourges",        "lat":47.0810, "lng":2.3980},
    # Bourgogne-Franche-Comté
    {"name":"Kiabi Dijon",         "city":"Dijon",          "lat":47.3220, "lng":5.0415},
    {"name":"Kiabi Besançon",      "city":"Besançon",       "lat":47.2378, "lng":6.0241},
    {"name":"Kiabi Montbéliard",   "city":"Montbéliard",    "lat":47.5073, "lng":6.8056},
    # Grand Est
    {"name":"Kiabi Strasbourg",    "city":"Strasbourg",     "lat":48.5734, "lng":7.7521},
    {"name":"Kiabi Metz",          "city":"Metz",           "lat":49.1193, "lng":6.1757},
    {"name":"Kiabi Reims",         "city":"Reims",          "lat":49.2583, "lng":4.0317},
    {"name":"Kiabi Nancy",         "city":"Nancy",          "lat":48.6921, "lng":6.1844},
    {"name":"Kiabi Mulhouse",      "city":"Mulhouse",       "lat":47.7508, "lng":7.3359},
    {"name":"Kiabi Troyes",        "city":"Troyes",         "lat":48.3000, "lng":4.0756},
    # Auvergne-Rhône-Alpes
    {"name":"Kiabi Lyon Part-Dieu","city":"Lyon",           "lat":45.7606, "lng":4.8588},
    {"name":"Kiabi Lyon Vaise",    "city":"Lyon",           "lat":45.7748, "lng":4.8056},
    {"name":"Kiabi Lyon Confluence","city":"Lyon",          "lat":45.7430, "lng":4.8182},
    {"name":"Kiabi Grenoble",      "city":"Grenoble",       "lat":45.1885, "lng":5.7245},
    {"name":"Kiabi Clermont-Fd",   "city":"Clermont-Fd",    "lat":45.7772, "lng":3.0870},
    {"name":"Kiabi Chambéry",      "city":"Chambéry",       "lat":45.5646, "lng":5.9178},
    {"name":"Kiabi Annecy",        "city":"Annecy",         "lat":45.8992, "lng":6.1294},
    {"name":"Kiabi Saint-Étienne", "city":"Saint-Étienne",  "lat":45.4397, "lng":4.3872},
    {"name":"Kiabi Valence",       "city":"Valence",        "lat":44.9334, "lng":4.8922},
    {"name":"Kiabi Roanne",        "city":"Roanne",         "lat":46.0348, "lng":4.0736},
    # PACA
    {"name":"Kiabi Marseille GL",  "city":"Marseille",      "lat":43.3535, "lng":5.3353},
    {"name":"Kiabi Marseille V.",  "city":"Marseille",      "lat":43.2879, "lng":5.4421},
    {"name":"Kiabi Nice",          "city":"Nice",           "lat":43.7102, "lng":7.2620},
    {"name":"Kiabi Toulon",        "city":"Toulon",         "lat":43.1242, "lng":5.9280},
    {"name":"Kiabi Aix-en-Prov.", "city":"Aix-en-Provence","lat":43.5297, "lng":5.4474},
    {"name":"Kiabi Avignon",       "city":"Avignon",        "lat":43.9493, "lng":4.8055},
    {"name":"Kiabi Cannes",        "city":"Cannes",         "lat":43.5528, "lng":7.0174},
    # Occitanie
    {"name":"Kiabi Toulouse F.",   "city":"Toulouse",       "lat":43.7017, "lng":1.4061},
    {"name":"Kiabi Toulouse L.",   "city":"Toulouse",       "lat":43.5597, "lng":1.5273},
    {"name":"Kiabi Montpellier",   "city":"Montpellier",    "lat":43.6108, "lng":3.8767},
    {"name":"Kiabi Nîmes",         "city":"Nîmes",          "lat":43.8374, "lng":4.3601},
    {"name":"Kiabi Perpignan",     "city":"Perpignan",      "lat":42.6886, "lng":2.8948},
    {"name":"Kiabi Béziers",       "city":"Béziers",        "lat":43.3444, "lng":3.2159},
    {"name":"Kiabi Albi",          "city":"Albi",           "lat":43.9277, "lng":2.1481},
    # Nouvelle-Aquitaine
    {"name":"Kiabi Bordeaux M.",   "city":"Bordeaux",       "lat":44.8422, "lng":-0.5778},
    {"name":"Kiabi Bordeaux L.",   "city":"Bordeaux",       "lat":44.8900, "lng":-0.5830},
    {"name":"Kiabi Limoges",       "city":"Limoges",        "lat":45.8336, "lng":1.2611},
    {"name":"Kiabi Pau",           "city":"Pau",            "lat":43.2951, "lng":-0.3708},
    {"name":"Kiabi Bayonne",       "city":"Bayonne",        "lat":43.4929, "lng":-1.4748},
    {"name":"Kiabi Poitiers",      "city":"Poitiers",       "lat":46.5802, "lng":0.3404},
    {"name":"Kiabi La Rochelle",   "city":"La Rochelle",    "lat":46.1603, "lng":-1.1511},
    {"name":"Kiabi Angoulême",     "city":"Angoulême",      "lat":45.6500, "lng":0.1561},
    {"name":"Kiabi Agen",          "city":"Agen",           "lat":44.2040, "lng":0.6210},
    # Corse
    {"name":"Kiabi Ajaccio",       "city":"Ajaccio",        "lat":41.9267, "lng":8.7369},
]  # _KIABI_STORES_PLACEHOLDER — not used at runtime

# ─── DB helpers ────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST", "fevm-nef.cloud.databricks.com")
    if not host.startswith("https://"):
        host = "https://" + host
    token = os.getenv("DATABRICKS_TOKEN", "")
    return WorkspaceClient(host=host, token=token) if token else WorkspaceClient(host=host)

def _query(sql: str) -> pd.DataFrame:
    w = _client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    # Poll if still pending after 50s (cold warehouse can take 60-90s to start)
    if resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        import time
        for _ in range(24):  # up to ~2 more minutes
            time.sleep(5)
            resp = w.statement_execution.get_statement(resp.statement_id)
            if resp.status.state not in (StatementState.PENDING, StatementState.RUNNING):
                break
    if resp.status.state != StatementState.SUCCEEDED:
        err = getattr(resp.status, "error", None)
        msg = getattr(err, "message", str(resp.status.state)) if err else str(resp.status.state)
        raise RuntimeError(f"SQL failed ({resp.status.state}): {msg}")
    try:
        cols = [c.name for c in resp.manifest.schema.columns]
    except Exception:
        cols = []
    rows = []
    try:
        rows = resp.result.data_array or []
    except Exception:
        pass
    if not cols:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=cols)

def _parse_lat_lng(loc: str):
    if not loc: return None, None
    la = re.search(r"lat=([-\d.]+)", str(loc))
    lo = re.search(r"lng=([-\d.]+)", str(loc))
    return (float(la.group(1)), float(lo.group(1))) if la and lo else (None, None)

@st.cache_data(ttl=300, show_spinner=False)
def load_listings():
    df = _query(f"""
        SELECT source, title, price, location, latitude, longitude, published_at, is_never_worn
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price > 0 AND price < 500
    """)
    if df.empty:
        return pd.DataFrame(columns=["source","title","price","location","published_at","is_never_worn","lat","lng","week","is_bulk"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["lat"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["lng"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["week"]  = df["published_at"].dt.to_period("W").dt.start_time
    df["is_bulk"] = df["title"].str.lower().str.contains(r"lot de|lot [0-9]|pack de|[0-9]+ pièces|[0-9]+ articles", regex=True, na=False)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_categories():
    df = _query(f"""
        SELECT {CATEGORY_EXPR} AS category,
               COUNT(*) AS count,
               ROUND(AVG(CASE WHEN price<500 THEN price END),2) AS avg_price,
               SUM(CASE WHEN is_never_worn THEN 1 ELSE 0 END) AS never_worn
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        GROUP BY 1 ORDER BY 2 DESC
    """)
    for col in ("count", "avg_price", "never_worn"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_source_stats():
    df = _query(f"""
        SELECT source,
               COUNT(*) AS total,
               ROUND(AVG(CASE WHEN price<500 THEN price END),2) AS avg_price,
               SUM(CASE WHEN is_never_worn THEN 1 ELSE 0 END) AS never_worn
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        GROUP BY source ORDER BY total DESC
    """)
    for col in ("total", "avg_price", "never_worn"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_popular_items():
    df = _query(f"""
        SELECT title, COUNT(*) AS occurrences,
               ROUND(AVG(CASE WHEN price<500 THEN price END),2) AS avg_price, source
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price<500
        GROUP BY title, source HAVING COUNT(*)>2
        ORDER BY occurrences DESC LIMIT 20
    """)
    for col in ("occurrences", "avg_price"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_top_expensive():
    df = _query(f"""
        SELECT source, title, price, url, location, external_id AS seller_id
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price > 0 AND price < 500
        ORDER BY price DESC LIMIT 10
    """)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def load_price_distribution():
    return _query(f"""
        SELECT
            CASE
                WHEN price <  5 THEN '0–5€'
                WHEN price < 10 THEN '5–10€'
                WHEN price < 15 THEN '10–15€'
                WHEN price < 20 THEN '15–20€'
                WHEN price < 30 THEN '20–30€'
                ELSE '30€+'
            END AS price_range,
            CASE
                WHEN price <  5 THEN 1
                WHEN price < 10 THEN 2
                WHEN price < 15 THEN 3
                WHEN price < 20 THEN 4
                WHEN price < 30 THEN 5
                ELSE 6
            END AS sort_order,
            COUNT(*) AS cnt
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price > 0 AND price < 500
        GROUP BY 1, 2 ORDER BY 2
    """)

@st.cache_data(ttl=300, show_spinner=False)
def load_never_worn_stats():
    return _query(f"""
        SELECT
            is_never_worn,
            COUNT(*) AS cnt,
            PERCENTILE(CAST(price AS DOUBLE), 0.5) AS median_price
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price > 0 AND price < 500
        GROUP BY 1
    """)

@st.cache_data(ttl=300, show_spinner=False)
def load_top_cities():
    return _query(f"""
        SELECT
            CASE
                WHEN REGEXP_EXTRACT(location, '\\\\((\\\\d{{5}})\\\\)', 1) != ''
                THEN CONCAT(
                    TRIM(REGEXP_REPLACE(location, '\\\\s*\\\\(\\\\d{{5}}\\\\)\\\\s*$', '')),
                    ' (', REGEXP_EXTRACT(location, '\\\\((\\\\d{{5}})\\\\)', 1), ')')
                ELSE TRIM(SPLIT(location, ',')[0])
            END AS city,
            COUNT(*) AS cnt
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE location IS NOT NULL AND TRIM(location) != ''
          AND price IS NOT NULL AND price > 0
        GROUP BY 1 ORDER BY cnt DESC LIMIT 12
    """)

@st.cache_data(ttl=300, show_spinner=False)
def load_listing_age():
    # published_at may be an ISO string OR a Unix epoch integer string.
    # COALESCE: try ISO cast first, fall back to from_unixtime().
    return _query(f"""
        SELECT
            CASE
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 7   THEN '0–7 days'
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 30  THEN '8–30 days'
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 90  THEN '31–90 days'
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 180 THEN '91–180 days'
                ELSE '180+ days'
            END AS age_bucket,
            CASE
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 7   THEN 1
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 30  THEN 2
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 90  THEN 3
                WHEN DATEDIFF(CURRENT_DATE(), DATE(COALESCE(
                        TRY_CAST(published_at AS TIMESTAMP),
                        FROM_UNIXTIME(TRY_CAST(published_at AS BIGINT))
                     ))) <= 180 THEN 4
                ELSE 5
            END AS sort_order,
            COUNT(*) AS cnt
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE published_at IS NOT NULL
        GROUP BY 1, 2 ORDER BY 2
    """)

@st.cache_data(ttl=300, show_spinner=False)
def load_seller_profiles():
    return _query(f"""
        SELECT
            CASE
                WHEN listing_count = 1       THEN 'Occasional (1 listing)'
                WHEN listing_count <= 5      THEN 'Regular (2–5)'
                WHEN listing_count <= 20     THEN 'Active (6–20)'
                ELSE 'Power seller (21+)'
            END AS profile,
            CASE
                WHEN listing_count = 1       THEN 1
                WHEN listing_count <= 5      THEN 2
                WHEN listing_count <= 20     THEN 3
                ELSE 4
            END AS sort_order,
            COUNT(*) AS seller_count,
            ROUND(AVG(listing_count), 1) AS avg_listings
        FROM (
            SELECT location, source, COUNT(*) AS listing_count
            FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
            WHERE location IS NOT NULL AND location != ''
            GROUP BY location, source
        ) t
        GROUP BY 1, 2 ORDER BY 2
    """)

@st.cache_data(ttl=300, show_spinner=False)
def load_condition_dist():
    return _query(f"""
        SELECT
            CASE
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'jamais port|neuf avec .tiquette|never worn|sans .tiquette|neuf complet|brand new'
                     THEN 'Never Worn / New with Tags'
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'tr.s bon .tat|very good|parfait .tat|excellent .tat|impeccable'
                     THEN 'Very Good Condition'
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'bon .tat|good condition|propre|bien entretenu'
                     THEN 'Good Condition'
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE '.tat correct|acceptable|satisfaisant|petits d.fauts|l.ger'
                     THEN 'Fair / Minor Defects'
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'us.|worn|tach|trace|imperfection|abim|marqu'
                     THEN 'Worn / Defects'
                ELSE 'Condition Not Specified'
            END AS condition_label,
            CASE
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'jamais port|neuf avec .tiquette|never worn|sans .tiquette|neuf complet|brand new' THEN 1
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'tr.s bon .tat|very good|parfait .tat|excellent .tat|impeccable' THEN 2
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'bon .tat|good condition|propre|bien entretenu' THEN 3
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE '.tat correct|acceptable|satisfaisant|petits d.fauts|l.ger' THEN 4
                WHEN LOWER(CONCAT(COALESCE(title,''),' ',COALESCE(description,'')))
                     RLIKE 'us.|worn|tach|trace|imperfection|abim|marqu' THEN 5
                ELSE 6
            END AS sort_order,
            COUNT(*) AS cnt,
            ROUND(AVG(CASE WHEN price < 500 THEN price END), 2) AS avg_price
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        GROUP BY 1, 2 ORDER BY 2
    """)

@st.cache_data(ttl=300, show_spinner=False)
def load_stores() -> pd.DataFrame:
    df = _query(f"""
        SELECT name, city, lat, lng
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_stores
        WHERE lat IS NOT NULL AND lng IS NOT NULL
    """)
    if df.empty:
        return df
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")
    return df.dropna(subset=["lat", "lng"])

@st.cache_data(ttl=300, show_spinner=False)
def load_competition_counts():
    try:
        df = _query(f"""
            SELECT brand, marketplace, count, last_run_at AS timestamp
            FROM {CATALOG}.{SCHEMA}.gold_competition_counts
            ORDER BY brand, marketplace, last_run_at DESC
        """)
        df["count"] = pd.to_numeric(df["count"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_category_deep(category_filter: str = "All"):
    where = "" if category_filter == "All" else f"AND ({CATEGORY_EXPR}) = '{category_filter}'"
    return _query(f"""
        SELECT
            CASE
                WHEN price <  5 THEN '0–5€'
                WHEN price < 10 THEN '5–10€'
                WHEN price < 15 THEN '10–15€'
                WHEN price < 20 THEN '15–20€'
                WHEN price < 30 THEN '20–30€'
                ELSE '30€+'
            END AS price_range,
            CASE
                WHEN price <  5 THEN 1
                WHEN price < 10 THEN 2
                WHEN price < 15 THEN 3
                WHEN price < 20 THEN 4
                WHEN price < 30 THEN 5
                ELSE 6
            END AS sort_order,
            source,
            COUNT(*) AS cnt
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price > 0 AND price < 500 {where}
        GROUP BY 1, 2, 3 ORDER BY 2, 3
    """)

# ─── Chart theme helper ────────────────────────────────────────────────────────
CHART_LAYOUT = dict(
    paper_bgcolor="#08121E", plot_bgcolor="#08121E",
    font=dict(color="#8892A4"),
    margin=dict(l=0,r=0,t=20,b=0),
    xaxis=dict(gridcolor="#1A2A45", showgrid=True, zeroline=False, tickfont=dict(color="#6B7A99")),
    yaxis=dict(gridcolor="#1A2A45", showgrid=True, zeroline=False, tickfont=dict(color="#6B7A99")),
)

# ─── France map (listings + Kiabi stores) ─────────────────────────────────────
def france_map(df_pts: pd.DataFrame, stores: pd.DataFrame, height=500, show_stores=True) -> go.Figure:
    traces = []
    pts = df_pts.dropna(subset=["lat","lng"])
    # Listing dots
    traces.append(go.Scattergeo(
        lat=pts["lat"], lon=pts["lng"],
        mode="markers", name="Listings",
        marker=dict(size=6, color="#00CFFF", opacity=0.6, line=dict(width=0.8,color="#7DD3FC")),
        hovertemplate="<b>%{text}</b><extra>Listing</extra>",
        text=pts.get("title",""),
    ))
    if show_stores and not stores.empty:
        traces.append(go.Scattergeo(
            lat=stores["lat"], lon=stores["lng"],
            mode="markers", name="Kiabi Store",
            marker=dict(size=11, symbol="star", color="#FBBF24", opacity=0.9,
                        line=dict(width=1, color="#F59E0B")),
            hovertemplate="<b>%{text}</b><extra>Kiabi Store</extra>",
            text=stores["name"],
        ))
    fig = go.Figure(traces)
    fig.update_layout(
        geo=dict(
            scope="europe", center=dict(lat=46.5, lon=2.3), projection_scale=8,
            showland=True, landcolor="#0D1A35",
            showcoastlines=True, coastlinecolor="#1E3A6E",
            showframe=False, bgcolor="#08121E",
            showcountries=True, countrycolor="#1E3A6E",
            showlakes=False,
        ),
        paper_bgcolor="#08121E", height=height,
        margin=dict(l=0,r=0,t=0,b=0),
        legend=dict(
            orientation="v", x=0.01, y=0.98,
            bgcolor="rgba(8,18,30,0.8)", bordercolor="#1A2A45", borderwidth=1,
            font=dict(color="white",size=12),
        ),
    )
    return fig

# ─── Markup-style table helpers ───────────────────────────────────────────────
def table_wrap_open(cols: list[tuple]) -> str:
    ths = "".join(
        f'<div class="th" style="flex:{w};text-align:{a};">{label}</div>'
        for label, w, a in cols
    )
    return f'<div class="table-wrap"><div class="table-head">{ths}</div>'

TABLE_CLOSE = "</div>"

def logo_img(src_name: str, size=50) -> str:
    url = LOGOS.get(src_name.lower(), "")
    if url:
        return f'<img src="{url}" class="logo-circle" style="width:{size}px;height:{size}px;" onerror="this.outerHTML=\'<div class=logo-letter>{src_name[0].upper()}</div>\'">'
    return f'<div class="logo-letter" style="width:{size}px;height:{size}px;font-size:{size//2-4}px;">{src_name[0].upper()}</div>'

def initial_circle(letter: str, color="#2563EB", size=50) -> str:
    return (f'<div style="width:{size}px;height:{size}px;border-radius:50%;flex-shrink:0;'
            f'background:linear-gradient(135deg,{color}22,{color}44);border:1px solid {color}66;'
            f'display:flex;align-items:center;justify-content:center;'
            f'font-size:{size//2-4}px;font-weight:800;color:{color};">{letter}</div>')

# ─── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading market data from Databricks…"):
    try:
        df            = load_listings()
        cats          = load_categories()
        sources       = load_source_stats()
        popular       = load_popular_items()
        top_expensive = load_top_expensive()
        price_dist    = load_price_distribution()
        never_worn_stats = load_never_worn_stats()
        top_cities    = load_top_cities()
        listing_age        = load_listing_age()
        seller_profiles    = load_seller_profiles()
        condition_dist     = load_condition_dist()
        competition_counts = load_competition_counts()
        stores_df          = load_stores()
    except Exception as e:
        st.error(f"Could not load data: {e}")
        st.stop()

if df.empty or len(df) == 0:
    st.info("No listings found in `gold_kiabi_listings`. Run the scraper job then the ETL pipeline to populate data.")
    st.stop()

total        = len(df)
total_sources= int(pd.to_numeric(sources["total"], errors="coerce").sum()) if not sources.empty else 0
avg_price    = pd.to_numeric(df["price"], errors="coerce").mean()
bulk_pct     = df["is_bulk"].mean() * 100 if "is_bulk" in df.columns else 0.0
nw_pct       = pd.to_numeric(df.get("is_never_worn", pd.Series(dtype=float)), errors="coerce").mean() * 100

# ─── User profiles ────────────────────────────────────────────────────────────
_PROFILES = {
    "Marion Lamoureux": {
        "role": "Strategy Lead",
        "avatar_b64": _MARION_B64,
        "avatar_mime": "image/png",
        "view": "full",
    },
    "Jane Doe": {
        "role": "Analyst",
        "avatar_b64": "",
        "avatar_mime": "",
        "view": "analyst",
    },
}

if "active_profile" not in st.session_state:
    st.session_state.active_profile = "Marion Lamoureux"

_profile = _PROFILES[st.session_state.active_profile]
_is_analyst = _profile["view"] == "analyst"

# ─── Header ───────────────────────────────────────────────────────────────────
_logo_tag = (
    f'<div style="background:white;border-radius:10px;padding:6px 10px;display:inline-flex;'
    f'align-items:center;justify-content:center;box-shadow:0 2px 12px rgba(0,0,0,0.4);flex-shrink:0;">'
    f'<img src="data:image/png;base64,{_LOGO_B64}" style="height:40px;width:auto;display:block;" alt="Kiabi">'
    f'</div>'
) if _LOGO_B64 else (
    '<div style="background:white;border-radius:10px;padding:6px 14px;display:inline-flex;'
    'align-items:center;box-shadow:0 2px 12px rgba(0,0,0,0.4);flex-shrink:0;">'
    '<span style="color:#E30613;font-size:22px;font-weight:900;letter-spacing:1px;">Kiabi</span>'
    '</div>'
)

# Avatar
if _profile["avatar_b64"]:
    _avatar_html = (
        f'<img src="data:{_profile["avatar_mime"]};base64,{_profile["avatar_b64"]}" '
        f'style="width:44px;height:44px;border-radius:50%;object-fit:cover;border:2px solid #2563EB;">'
    )
else:
    _initials = "".join(w[0] for w in st.session_state.active_profile.split())
    _avatar_html = (
        f'<div style="width:44px;height:44px;border-radius:50%;background:linear-gradient(135deg,#1E3A6E,#2563EB);'
        f'display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:700;color:white;'
        f'border:2px solid #2563EB;flex-shrink:0;">{_initials}</div>'
    )

_header_left, _header_right = st.columns([8, 2])
with _header_left:
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:18px;padding:4px 0 8px 0;">'
        f'{_logo_tag}'
        f'<div>'
        f'<div style="color:white;font-size:20px;font-weight:700;letter-spacing:1px;line-height:1.2;">Second-Hand Market Intelligence</div>'
        f'<div style="color:#E0E6F0;font-size:18px;margin-top:2px;">France</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
with _header_right:
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:10px;justify-content:flex-end;padding:8px 0;">'
        f'{_avatar_html}'
        f'<div>'
        f'<div style="color:white;font-size:14px;font-weight:600;">{st.session_state.active_profile}</div>'
        f'<div style="color:#2563EB;font-size:12px;font-weight:500;">{_profile["role"]}</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    _selected = st.selectbox(
        "Switch profile",
        list(_PROFILES.keys()),
        index=list(_PROFILES.keys()).index(st.session_state.active_profile),
        key="profile_selector",
        label_visibility="collapsed",
    )
    if _selected != st.session_state.active_profile:
        st.session_state.active_profile = _selected
        st.rerun()

st.markdown("<hr>", unsafe_allow_html=True)

# ─── Genie chat helper ────────────────────────────────────────────────────────
def _genie_ask(question: str) -> str:
    """Send a question to the Genie space and return the response."""
    try:
        w = _client()
        space_id = GENIE_SPACE_ID

        # Check if we have an ongoing conversation
        conv_id = st.session_state.get("genie_conversation_id")

        if conv_id:
            result = w.genie.create_message_and_wait(
                space_id=space_id,
                conversation_id=conv_id,
                content=question,
            )
        else:
            result = w.genie.start_conversation_and_wait(
                space_id=space_id,
                content=question,
            )
            st.session_state["genie_conversation_id"] = result.conversation_id

        # Extract response text
        if hasattr(result, "attachments") and result.attachments:
            parts = []
            for att in result.attachments:
                if hasattr(att, "text") and att.text and hasattr(att.text, "content"):
                    parts.append(att.text.content)
                # Skip SQL query — only show text answers
            if parts:
                return "\n\n".join(parts)
        # Fallback: try to get content directly
        if hasattr(result, "content"):
            return result.content
        return str(result)
    except Exception as e:
        return f"⚠️ Genie error: {e}"

# Initialize Genie session state
if "genie_messages" not in st.session_state:
    st.session_state.genie_messages = []
if "genie_conversation_id" not in st.session_state:
    st.session_state.genie_conversation_id = None

# ─── Genie sidebar chat panel ────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div style="color:white;font-size:20px;font-weight:700;margin-bottom:4px;">✨ Ask Genie</div>'
        '<div style="color:#E0E6F0;font-size:14px;margin-bottom:16px;">Kiabi Second-Hand Market Explorer · natural language SQL</div>',
        unsafe_allow_html=True,
    )

    # Display conversation history
    for msg in st.session_state.genie_messages:
        with st.chat_message(msg["role"], avatar="✨" if msg["role"] == "assistant" else "👤"):
            st.markdown(msg["content"])

    # Input
    genie_input = st.chat_input("Ask a question about the data…", key="genie_chat_input")
    if genie_input:
        st.session_state.genie_messages.append({"role": "user", "content": genie_input})
        with st.chat_message("user", avatar="👤"):
            st.markdown(genie_input)
        with st.chat_message("assistant", avatar="✨"):
            with st.spinner("Genie is thinking…"):
                answer = _genie_ask(genie_input)
            st.markdown(answer)
        st.session_state.genie_messages.append({"role": "assistant", "content": answer})

    # New conversation button
    if st.session_state.genie_messages:
        if st.button("🔄 New conversation", key="genie_reset"):
            st.session_state.genie_messages = []
            st.session_state.genie_conversation_id = None
            st.rerun()

# ─── Knowledge context loader ─────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _load_knowledge_context() -> str:
    try:
        p = Path(KNOWLEDGE_DOC_PATH)
        return p.read_text(encoding="utf-8") if p.exists() else ""
    except Exception:
        return ""

# ─── LLM chat helper ───────────────────────────────────────────────────────────
def _ask_strategy(question: str, history: list) -> str:
    ctx = _load_knowledge_context()
    system_prompt = (
        "You are a strategic advisor for Kiabi on second-hand fashion and sustainability.\n"
        "You have access to Kiabi's DPEF 2024 ESG report and a competitive analysis of the "
        "French second-hand fashion market.\n\n"
        "Key facts:\n"
        "- Kiabi 2024 revenue: €2.3B (+5%), 23.7M clients, 298M pieces sold\n"
        "- Second-hand: 0.43% of items sold in 2024, target 50% by 2035\n"
        "- Beebs acquisition (May 2024): C2C family platform, 2M family users, 100 French stores\n"
        "- 75,000 pieces bought back in 43 stores; 1.3M pieces sold second-hand\n"
        "- CO₂: 2.28 Mt eq. (-4.3% vs 2022), target -25% by 2035\n"
        "- Experiments: KidKanaï (concept store), La Petite Braderie, Crushon partnership\n\n"
        "Answer concisely and strategically. Cite sources when referencing specific data. "
        "Respond in the same language as the question (French or English).\n\n"
    )
    if ctx:
        system_prompt += f"KNOWLEDGE BASE:\n{ctx[:12000]}"
    messages = [{"role": "system", "content": system_prompt}]
    for h in history[-6:]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": question})
    try:
        w = _client()
        payload = {"messages": messages, "max_tokens": 1200}
        data = w.api_client.do(
            "POST",
            f"/serving-endpoints/{LLM_ENDPOINT}/invocations",
            body=payload,
        )
        if isinstance(data, dict) and "choices" in data:
            return data["choices"][0]["message"]["content"]
        return str(data)
    except Exception as e:
        return f"⚠️ Could not reach LLM endpoint: {e}"

_AGENT_ENDPOINT_NAME = "mas-0eee0b16-endpoint"

@st.cache_data(ttl=600, show_spinner=False)
def _get_positioning_analysis() -> str:
    """Call the agent endpoint for a pre-loaded Kiabi positioning analysis."""
    try:
        w = _client()
        # Use the SDK's API client which handles OAuth auth automatically
        payload = {"input": [{"role": "user", "content":
            "whats a 3 point analysis that can be made of Kiabi's positioning as of now on major marketplaces compared to it's objectives, make it short"}]}
        resp = w.api_client.do(
            "POST",
            f"/serving-endpoints/{_AGENT_ENDPOINT_NAME}/invocations",
            body=payload,
        )
        # Agent endpoints return a list of conversation steps in "output"
        output = resp if isinstance(resp, list) else (resp.get("output") if isinstance(resp, dict) else None)
        if isinstance(output, list):
            # Walk backwards to find the last assistant message with output_text
            for step in reversed(output):
                if step.get("role") == "assistant" and isinstance(step.get("content"), list):
                    for block in step["content"]:
                        if block.get("type") == "output_text" and len(block.get("text", "")) > 100:
                            return block["text"]
            # Fallback: last output_text from any step
            for step in reversed(output):
                if isinstance(step.get("content"), list):
                    for block in step["content"]:
                        if block.get("type") == "output_text":
                            return block["text"]
        if isinstance(resp, dict):
            if "choices" in resp:
                return resp["choices"][0]["message"]["content"]
        return str(resp)
    except Exception as e:
        return f"⚠️ Could not reach analysis endpoint: {e}"

# ─── Tabs ──────────────────────────────────────────────────────────────────────
# APP_PROFILE=light shows only Market Overview + Strategy Assistant; other tabs show a short message.
def _show_tab(name: str) -> bool:
    return APP_PROFILE != "light" or name in LIGHT_PROFILE_TABS

if _is_analyst:
    t_overview = st.tabs(["Market Overview"])[0]
    t_vendors = t_strategy = t_items = t_images = None
else:
    t_overview, t_vendors, t_items, t_strategy, t_images = st.tabs([
        "Market Overview",
        "Vendors & Locations",
        "Items & Pricing",
        "Strategy Assistant",
        "Image Analysis",
    ])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 – MARKET OVERVIEW  (competitor analysis)
# ══════════════════════════════════════════════════════════════════════════════

# Static baseline counts (from manual research) — overridden by live DB data when available
_STATIC_COUNTS = {
    "Primark":       {"leboncoin": 45000, "vinted": 22000},
    "Dpam":          {"leboncoin": 42000, "vinted": 18000},
    "Camaïeu":       {"leboncoin": 42000, "vinted": 15000},
    "H&M":           {"leboncoin": 41000, "vinted": 30000},
    "Promod":        {"leboncoin": 36000, "vinted": 12000},
    "Catimini":      {"leboncoin": 35000, "vinted": 14000},
    "Zara":          {"leboncoin": 34000, "vinted": 28000},
    "Vertbaudet":    {"leboncoin": 31000, "vinted": 16000},
    "Sergent Major": {"leboncoin": 28000, "vinted": 11000},
    "Gémo":          {"leboncoin": 26000, "vinted":  9000},
    "Jacadi":        {"leboncoin": 25000, "vinted":  8000},
    "Okaïdi":        {"leboncoin": 24000, "vinted": 10000},
    "Orchestra":     {"leboncoin": 24000, "vinted":  9500},
    "Tape à l'Oeil": {"leboncoin": 23200, "vinted":  8800},
    "Absorba":       {"leboncoin": 23100, "vinted":  7500},
    "Kiabi":         {"leboncoin": 23000, "vinted": 14000, "ebay": 488, "vestiaire": 20},
    "Benetton":      {"leboncoin": 18000, "vinted":  7000},
    "La Redoute":    {"leboncoin": 15000, "vinted":  6000},
    "Jules":         {"leboncoin": 11000, "vinted":  4000},
    "Petit Bateau":  {"leboncoin":  8000, "vinted":  5000},
}

# Marketplace display config: key → (label, b64_logo, fallback_letter, color)
_MKT_CONFIG = {
    "leboncoin":           ("LeBonCoin",      _LBC_B64,       "L", "#FF6B35"),
    "vinted":              ("Vinted",          _VINTED_B64,    "V", "#06B6D4"),
    "vestiaire":           ("Vestiaire",       _VESTIAIRE_B64, "VC","#9333EA"),
    "ebay":                ("eBay",            _EBAY_B64,      "e", "#F59E0B"),
    "decathlon_occasion":  ("Decathlon Occ.",  "",             "D", "#0082C8"),
    "la_redoute_occasion": ("La Redoute Occ.", "",             "LR","#FF6B35"),
    "beebs":               ("Beebs",           _BEEBS_B64,    "B", "#22C55E"),
}

def _mkt_logo_cell(mkt_key: str, size: int = 22) -> str:
    label, b64, letter, color = _MKT_CONFIG.get(mkt_key, (mkt_key, "", mkt_key[:2].upper(), "#3B82F6"))
    if b64:
        return (f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:5px;">'
                f'<img src="data:image/png;base64,{b64}" style="height:{size}px;width:auto;border-radius:4px;">'
                f'<span style="color:#E0E6F0;font-size:15px;">{label}</span></div>')
    return (f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:5px;">'
            f'<div style="width:{size}px;height:{size}px;border-radius:5px;background:{color}22;border:1px solid {color}44;'
            f'display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;color:{color};">{letter}</div>'
            f'<span style="color:#E0E6F0;font-size:15px;">{label}</span></div>')

with t_overview:
    # Build live_lookup first so ranking can use it
    live_lookup: dict[tuple, int] = {}
    if not competition_counts.empty:
        latest = (competition_counts
                  .sort_values("timestamp", ascending=False)
                  .groupby(["brand","marketplace"], as_index=False)
                  .first())
        for _, r in latest.iterrows():
            v = r["count"]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                live_lookup[(str(r["brand"]), str(r["marketplace"]))] = int(v)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1,k2,k3,k4,k5 = st.columns(5)
    brands_tracked = len(_STATIC_COUNTS) if competition_counts.empty else competition_counts["brand"].nunique()
    mkts_tracked   = len(_MKT_CONFIG)
    # Compute Kiabi ranking dynamically from live + static data
    _brand_totals = {}
    for _b in (list(_STATIC_COUNTS.keys()) + ([b for b in (competition_counts["brand"].unique() if not competition_counts.empty else []) if b not in _STATIC_COUNTS])):
        _lbc = (live_lookup.get((_b, "leboncoin")) or _STATIC_COUNTS.get(_b, {}).get("leboncoin", 0)) or 0
        _vnt = (live_lookup.get((_b, "vinted")) or _STATIC_COUNTS.get(_b, {}).get("vinted", 0)) or 0
        _brand_totals[_b] = _lbc + _vnt
    _sorted_brands = sorted(_brand_totals.items(), key=lambda x: x[1], reverse=True)
    _kiabi_pos = next((i + 1 for i, (b, _) in enumerate(_sorted_brands) if b == "Kiabi"), len(_sorted_brands))
    _ordinal = lambda n: f"{n}{'th' if 11 <= n % 100 <= 13 else {1:'st',2:'nd',3:'rd'}.get(n % 10, 'th')}"
    kiabi_rank = f"{_ordinal(_kiabi_pos)} / {len(_sorted_brands)}"
    # Top 2 leaders for insight text
    _top_leaders = [b for b, _ in _sorted_brands[:2] if b != "Kiabi"][:2]
    _leaders_str = " & ".join(_top_leaders) if _top_leaders else "top brands"

    _kpi_html = lambda label, value: (
        f'<div style="background:linear-gradient(135deg,#0D1A35 0%,#0A1628 100%);'
        f'border-radius:14px;padding:24px 26px;border:1px solid #1A2A45;">'
        f'<div style="color:white;font-size:16px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;">{label}</div>'
        f'<div style="color:#2563EB;font-size:44px;font-weight:800;">{value}</div>'
        f'</div>'
    )
    k1.markdown(_kpi_html("Brands Tracked", brands_tracked), unsafe_allow_html=True)
    k2.markdown(_kpi_html("Marketplaces Monitored", mkts_tracked), unsafe_allow_html=True)
    k3.markdown(_kpi_html("Kiabi Ranking", kiabi_rank), unsafe_allow_html=True)
    k4.markdown(_kpi_html("Kiabi Scraped Listings", f"{total_sources:,}"), unsafe_allow_html=True)
    k5.markdown(_kpi_html("Avg Kiabi Price", f"€{avg_price:.2f}"), unsafe_allow_html=True)

    # ── AI Positioning Analysis ───────────────────────────────────────────────
    with st.spinner("Loading positioning analysis…"):
        _analysis_text = _get_positioning_analysis()
    st.markdown(
        '<div style="background:linear-gradient(135deg,#0D1A35,#0A1628);border:1px solid #1E3A6E;'
        'border-radius:14px;padding:22px 28px;margin:16px 0;">'
        '<div style="color:#2563EB;font-size:18px;font-weight:700;margin-bottom:12px;">🤖 AI Positioning Analysis</div>'
        f'<div style="color:#EEF0F6;font-size:16px;line-height:1.7;">{_analysis_text}</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown("")

    # ── Live competition table from delta table ────────────────────────────────
    st.markdown(
        '<div style="color:white;font-size:22px;font-weight:700;margin-bottom:4px;">Brand × Marketplace Search Counts</div>'
        '<div style="color:#E0E6F0;font-size:18px;margin-bottom:14px;">'
        'Latest keyword search result counts per brand per platform — from <b style="color:white;">gold_competition_counts</b> delta table. '
        'Static estimates used where scraper data is not yet available.</div>',
        unsafe_allow_html=True,
    )

    # Determine which marketplaces have any data (DB or static)
    all_mkts_with_data = list(_MKT_CONFIG.keys())

    # Build rows: all brands from static baseline + any extra from DB
    all_brands_db = set(competition_counts["brand"].unique()) if not competition_counts.empty else set()
    all_brands = list(_STATIC_COUNTS.keys()) + [b for b in sorted(all_brands_db) if b not in _STATIC_COUNTS]
    # Analyst view: only top 5 brands + Kiabi
    if _is_analyst:
        _top5 = sorted(all_brands, key=lambda b: sum((_STATIC_COUNTS.get(b, {}).get(m, 0) for m in ("leboncoin", "vinted"))), reverse=True)[:5]
        if "Kiabi" not in _top5:
            _top5.append("Kiabi")
        all_brands = [b for b in all_brands if b in _top5]

    # Determine which columns actually have any value (static or live)
    cols_with_data = []
    for m in all_mkts_with_data:
        has_static = any(_STATIC_COUNTS.get(b, {}).get(m) for b in all_brands)
        has_live   = any(live_lookup.get((b, m)) for b in all_brands)
        if has_static or has_live:
            cols_with_data.append(m)

    # HTML table header with logos
    header_html = (
        '<div class="table-wrap"><div class="table-head" style="gap:8px;">'
        '<div class="th" style="flex:2;">Brand</div>'
    )
    for m in cols_with_data:
        label, b64, letter, color = _MKT_CONFIG.get(m, (m, "", m[:2].upper(), "#3B82F6"))
        mime = "image/jpeg" if m in ("vinted", "beebs") else "image/png"
        if b64:
            logo_html = (f'<img src="data:{mime};base64,{b64}" '
                         f'style="height:32px;width:auto;border-radius:6px;display:block;margin:0 auto;" title="{label}">')
        else:
            logo_html = (f'<div style="width:32px;height:32px;border-radius:6px;background:{color}22;border:1px solid {color}44;'
                         f'display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;'
                         f'color:{color};margin:0 auto;" title="{label}">{letter}</div>')
        header_html += (f'<div class="th" style="flex:1;text-align:center;">'
                        f'{logo_html}</div>')
    header_html += '</div>'

    rows_html = ""
    for brand in all_brands:
        is_kiabi = brand == "Kiabi"
        row_bg = "background:rgba(0,207,255,.04);border-left:2px solid #00CFFF;" if is_kiabi else ""
        name_style = "color:#00CFFF;font-weight:700;" if is_kiabi else "color:white;"

        # Brand logo / initial
        brand_logo = logo_img(brand.lower().replace(" ","").replace("&","").replace("'",""), 34)

        row_cells = (f'<div class="row-card" style="{row_bg}gap:8px;">'
                     f'<div style="flex:2;display:flex;align-items:center;gap:10px;min-width:0;">'
                     f'{brand_logo}'
                     f'<div style="{name_style}font-size:14px;">{brand}{"  📍" if is_kiabi else ""}</div>'
                     f'</div>')

        for m in cols_with_data:
            live_val  = live_lookup.get((brand, m))
            static_val = _STATIC_COUNTS.get(brand, {}).get(m)
            val = live_val if live_val is not None else static_val
            is_live = live_val is not None

            if val is not None:
                display = f"{val:,}"
                num_color = "#00CFFF" if is_kiabi else "white"
                suffix = (' <span style="font-size:9px;color:#22C55E;" title="Live from scraper">●</span>'
                          if is_live else
                          ' <span style="font-size:9px;color:#2A3A55;" title="Static estimate">~</span>')
            else:
                display, num_color, suffix = "—", "#2A3A55", ""

            row_cells += (f'<div style="flex:1;text-align:center;">'
                          f'<div style="color:{num_color};font-size:16px;font-weight:600;">'
                          f'{display}{suffix}</div></div>')

        row_cells += "</div>"
        rows_html += row_cells

    st.markdown(header_html + rows_html + TABLE_CLOSE, unsafe_allow_html=True)

    # Legend
    st.markdown(
        '<div style="display:flex;gap:20px;padding:8px 4px;font-size:11px;color:#E0E6F0;flex-wrap:wrap;">'
        '<span><span style="color:#22C55E;">●</span> Live — from competition scraper delta table</span>'
        '<span><span style="color:#2A3A55;font-size:13px;">~</span> Estimated — static baseline (not yet scraped)</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    # --- Sections below hidden for analyst view ---
    _show_full = not _is_analyst

    if _show_full:
        st.markdown("<br>", unsafe_allow_html=True)

    # ── Brand positioning bar chart ────────────────────────────────────────────
    if _show_full:
        st.markdown(
            '<div style="color:white;font-size:22px;font-weight:700;margin-bottom:4px;">🏆 Brand Positioning — Total Estimated Listings</div>'
            '<div style="color:#E0E6F0;font-size:18px;margin-bottom:14px;">Sum of LeBonCoin + Vinted counts per brand (live data where available, estimates otherwise)</div>',
            unsafe_allow_html=True,
        )

    if _show_full:
        chart_data = []
        for brand in all_brands:
            lbc_v = live_lookup.get((brand,"leboncoin")) or _STATIC_COUNTS.get(brand,{}).get("leboncoin", 0)
            vnt_v = live_lookup.get((brand,"vinted"))    or _STATIC_COUNTS.get(brand,{}).get("vinted", 0)
            total_v = (lbc_v or 0) + (vnt_v or 0)
            if total_v > 0:
                chart_data.append({"brand": brand, "total": total_v})
        chart_df = pd.DataFrame(chart_data).sort_values("total", ascending=False)
        chart_df["color"] = chart_df["brand"].apply(lambda b: "#00CFFF" if b == "Kiabi" else "#1E3A6E")
    
        fig_pos = go.Figure(go.Bar(
            x=chart_df["total"], y=chart_df["brand"], orientation="h",
            marker=dict(color=chart_df["color"], cornerradius=5),
            text=chart_df["total"].apply(lambda v: f"{int(v)//1000:.0f}K" if v >= 1000 else str(int(v))),
            textposition="outside", textfont=dict(color="white", size=15),
            hovertemplate="<b>%{y}</b><br>%{x:,} listings<extra></extra>",
        ))
        kiabi_total = chart_df[chart_df["brand"]=="Kiabi"]["total"].values
        if len(kiabi_total):
            fig_pos.add_annotation(
                x=float(kiabi_total[0]) + 500, y="Kiabi",
                text=f"📍 Kiabi — {kiabi_rank}",
                showarrow=False, font=dict(color="#00CFFF", size=16), xanchor="left",
            )
        fig_pos.update_layout(**{**CHART_LAYOUT, "height": max(400, len(chart_df)*32), "showlegend": False,
            "xaxis": {**CHART_LAYOUT["xaxis"], "title": "LeBonCoin + Vinted listings"},
            "yaxis": {**CHART_LAYOUT["yaxis"], "title": "", "tickfont": dict(size=14, color="white"), "categoryorder": "array", "categoryarray": list(chart_df["brand"])[::-1]},
            "margin": dict(l=0, r=90, t=10, b=0),
        })
        st.plotly_chart(fig_pos, use_container_width=True)
        _kiabi_total_v = _brand_totals.get("Kiabi", 0)
        _leader_total_v = _sorted_brands[0][1] if _sorted_brands else 0
        _gap_pct = round((_leader_total_v - _kiabi_total_v) / _leader_total_v * 100) if _leader_total_v else 0
        st.markdown(
            f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;">'
            f'Kiabi ranks <b style="color:white;">{_ordinal(_kiabi_pos)}</b> out of {len(_sorted_brands)} tracked brands '
            f'({_kiabi_total_v:,} combined listings). '
            f'Gap vs leaders ({_leaders_str}) is {_gap_pct}% — Beebs acquisition and buy-back programme are key levers.'
            f'</div>', unsafe_allow_html=True,
        )
    
        # ── Brands with Active Second-Hand Programs ──────────────────────────────
        st.markdown("<hr style='border-color:#1A2A45;margin:20px 0;'>", unsafe_allow_html=True)
        st.markdown(
            '<div style="color:white;font-size:22px;font-weight:700;margin-bottom:4px;">🏷️ Brands with Active Second-Hand Programs</div>'
            '<div style="color:#E0E6F0;font-size:17px;margin-bottom:16px;">Benchmark platforms — Kiabi keyword searched where searchable, result count shown.</div>',
            unsafe_allow_html=True,
        )
    
        PROGRAMS = [
            {"brand": "Zara", "program": "Zara Pre-Owned", "url": "https://www.zara.com/fr/fr/preowned-resell/products/Femme-l1/%C3%89DITORIAL-l15200",
             "model": "In-house marketplace", "learning": "Integration model", "icon": "Z", "color": "#1A1A1A", "marketplace_key": "zara_preowned"},
            {"brand": "H&M", "program": "H&M Take-Back + Resale", "url": "https://www2.hm.com/fr_fr/femme/developpement-durable/hm-x-sellpy.html?srsltid=AfmBOopAVTsQEgiDFtmWl1zebM99r30NVrhhAQsTb6s8a7UJ1HcEz07",
             "model": "Take-back + resale", "learning": "Volume handling", "icon": "H", "color": "#E4003A", "marketplace_key": None},
            {"brand": "La Redoute", "program": "La Redoute & Moi", "url": "https://www.laredoute.fr/pplp/cat-300731.aspx",
             "model": "C2C marketplace on-site", "learning": "French market fit", "icon": "LR", "color": "#FF6B35", "marketplace_key": "la_redoute_occasion"},
            {"brand": "Decathlon", "program": "Decathlon Occasion", "url": "https://occasion.decathlon.fr",
             "model": "High-volume, low-price", "learning": "Best comparable ✅", "icon": "D", "color": "#0082C8", "marketplace_key": "decathlon_occasion"},
            {"brand": "Kiabi", "program": "Kiabi Seconde Main", "url": "https://www.kiabi.com",
             "model": "Beebs + in-store pilot", "learning": "Internal learnings", "icon": "K", "color": "#E30613", "marketplace_key": None},
        ]
    
        comp_latest = {}
        if not competition_counts.empty:
            competition_counts["count"] = pd.to_numeric(competition_counts["count"], errors="coerce")
            for mkt, grp in competition_counts.groupby("marketplace"):
                latest_row = grp.sort_values("timestamp", ascending=False).iloc[0]
                comp_latest[mkt] = int(latest_row["count"])
    
        html_prog = table_wrap_open([("Brand / Program","4","left"),("Model","2","left"),("Key Learning","2","left"),("Kiabi Results","1","right"),("Link","1","center")])
        for p in PROGRAMS:
            c = p["color"]
            count_val = comp_latest.get(p["marketplace_key"])
            count_str = f"{count_val:,}" if count_val is not None else "—"
            count_color = "#22C55E" if count_val and count_val > 0 else "#6B7A99"
            learning_badge = "badge-g" if "✅" in p["learning"] else "badge-b"
            is_kiabi = p["brand"] == "Kiabi"
            html_prog += f"""
        <div class="row-card" style="{'background:rgba(227,6,19,.04);' if is_kiabi else ''}">
          <div style="flex:4;display:flex;align-items:center;gap:12px;min-width:0;">
            <div style="width:44px;height:44px;border-radius:10px;flex-shrink:0;background:{c}22;border:1px solid {c}66;
                        display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:800;color:{c};">{p['icon']}</div>
            <div>
              <div class="cell-name" style="{'color:#E30613;' if is_kiabi else ''}">{p['brand']}</div>
              <div class="cell-sub">{p['program']}</div>
            </div>
          </div>
          <div style="flex:2;"><div class="cell-sub" style="color:#EEF0F6;">{p['model']}</div></div>
          <div style="flex:2;"><span class="{learning_badge}">{p['learning']}</span></div>
          <div style="flex:1;text-align:right;"><div class="cell-num" style="color:{count_color};">{count_str}</div></div>
          <div style="flex:1;text-align:center;">
            <a href="{p['url']}" target="_blank" style="color:#3B82F6;font-size:12px;text-decoration:none;">Visit →</a>
          </div>
        </div>"""
        st.markdown(html_prog + TABLE_CLOSE, unsafe_allow_html=True)
        st.markdown(
            '<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:12px 16px;font-size:12px;color:#E0E6F0;margin-top:8px;">'
            '💡 <b style="color:white;">Decathlon Occasion</b> is the most comparable model — high-volume, low-price, France-native, with strong buy-back logistics. '
            '"Kiabi Results" shows how many results the keyword <b style="color:white;">"Kiabi"</b> returns on each platform (sourced from competition scraper job).'
            '</div>', unsafe_allow_html=True,
        )
    
        # ── Social Listening Sources ─────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div style="color:white;font-size:22px;font-weight:700;margin-bottom:4px;">📡 Social Listening Sources</div>'
            '<div style="color:#E0E6F0;font-size:17px;margin-bottom:16px;">Organic signal channels to monitor — not yet integrated into the pipeline.</div>',
            unsafe_allow_html=True,
        )
        SOCIAL_SOURCES = [
            {"name": "Google Trends", "signal": "Search interest in «Kiabi seconde main»",
             "what": "Track weekly search volume spikes around back-to-school, sales events, and Beebs launch",
             "icon": "🔍", "url": "https://trends.google.fr/trends/explore?q=kiabi+seconde+main&geo=FR"},
            {"name": "TikTok / Instagram", "signal": "#kiabi #kiabioccasion #kiabiresale",
             "what": "Hashtag volume and engagement — UGC resale content signals consumer intent and brand perception",
             "icon": "📱", "url": None},
            {"name": "Reddit & Forums", "signal": "r/FrugalFrance, Mode & Beauté communities",
             "what": "Organic consumer conversations about Kiabi quality, sizing, and durability — key for resale appeal",
             "icon": "💬", "url": None},
            {"name": "Trustpilot / Avis Vérifiés", "signal": "Kiabi quality perception reviews",
             "what": "Sentiment on durability and quality — high durability = higher resale value and buyer confidence",
             "icon": "⭐", "url": "https://fr.trustpilot.com/review/www.kiabi.com"},
        ]
        sl_cols = st.columns(2)
        for i, src in enumerate(SOCIAL_SOURCES):
            with sl_cols[i % 2]:
                link_html = f'<a href="{src["url"]}" target="_blank" style="color:#3B82F6;font-size:11px;">Open →</a>' if src.get("url") else '<span style="color:#1A2A45;font-size:11px;">—</span>'
                st.markdown(
                    f'<div style="background:#0D1A35;border:1px solid #1E3A6E;border-radius:12px;padding:16px 18px;margin-bottom:10px;">'
                    f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">'
                    f'<span style="font-size:22px;">{src["icon"]}</span>'
                    f'<div style="flex:1;">'
                    f'<div style="color:white;font-weight:600;font-size:14px;">{src["name"]}</div>'
                    f'<div style="color:#E0E6F0;font-size:16px;">{src["signal"]}</div>'
                    f'</div>'
                    f'<span style="background:rgba(251,146,60,.12);color:#FB923C;border-radius:6px;padding:2px 8px;font-size:10px;font-weight:600;">ROADMAP</span>'
                    f'{link_html}'
                    f'</div>'
                    f'<div style="color:#EEF0F6;font-size:17px;line-height:1.5;">{src["what"]}</div>'
                    f'</div>', unsafe_allow_html=True,
                )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 – VENDORS & LOCATIONS (hidden in analyst view)
# ══════════════════════════════════════════════════════════════════════════════
if not _is_analyst:
 with t_vendors:
    if not _show_tab("Vendors & Locations"):
        st.info("This section is not available in summary view. Set **APP_PROFILE=full** to see all tabs.")
    else:
        v1,v2,v3,v4 = st.columns(4)
        geo_df = df.dropna(subset=["lat","lng"])
        v1.metric("Listings with Location", f"{len(geo_df):,}")
        v2.metric("Lot / Bulk Listings", f"{df['is_bulk'].sum():,}")
        v3.metric("Kiabi Stores on Map", str(len(stores_df)))
        v4.metric("Avg Price — Individual", f"€{df[~df['is_bulk']]['price'].mean():.2f}")

        st.markdown("")
        st.markdown('<div class="section-label">Second-Hand Listings vs Kiabi Store Locations — France</div>', unsafe_allow_html=True)
        st.markdown("""
    <div style="color:#E0E6F0;font-size:17px;margin-bottom:10px;">
      <span style="color:#00CFFF;">●</span> Scraped listings &nbsp;&nbsp;
      <span style="color:#FBBF24;">★</span> Kiabi stores (80+ locations)
    </div>
    """, unsafe_allow_html=True)

        # Large map
        st.plotly_chart(france_map(geo_df, stores_df, height=540), use_container_width=True)

        st.markdown("")
        col_mp, col_bulk = st.columns([1, 1], gap="medium")

        with col_mp:
            # Marketplace table
            st.markdown('<div class="section-label">Marketplace Performance</div>', unsafe_allow_html=True)
            total_all = int(sources["total"].sum())
            html = table_wrap_open([
                ("Marketplace","3","left"), ("Listings","1","right"),
                ("Avg Price","1","right"), ("Never Worn","1","right"), ("Share","1","right"),
            ])
            for _, row in sources.iterrows():
                src    = str(row["source"])
                cnt    = int(row["total"])
                avg    = float(row["avg_price"] or 0)
                nw     = int(row["never_worn"])
                share  = round(cnt/total_all*100,1)
                html += f"""
            <div class="row-card">
              {logo_img(src)}
              <div style="flex:3;min-width:0;">
                <div class="cell-name">{src.capitalize()}</div>
                <div class="cell-sub">France · {share}% market share</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num">{cnt:,}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num">€{avg:.2f}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num">{nw:,}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <span class="badge-b">{share}%</span>
              </div>
            </div>"""
            st.markdown(html + TABLE_CLOSE, unsafe_allow_html=True)

        with col_bulk:
            # Bulk vs individual
            st.markdown('<div class="section-label">Individual vs Bulk Listings</div>', unsafe_allow_html=True)
            bulk_counts = df.groupby("is_bulk").agg(count=("price","count"), avg_price=("price","mean")).reset_index()
            bulk_counts["label"] = bulk_counts["is_bulk"].map({True:"Lot / Bundle",False:"Article individuel"})
            fig_bulk = go.Figure(go.Pie(
                labels=bulk_counts["label"], values=bulk_counts["count"],
                hole=0.55,
                marker=dict(colors=["#FBBF24","#3B82F6"], line=dict(color="#060C1C",width=2)),
                textinfo="none",
                hovertemplate="<b>%{label}</b><br>%{value:,} listings<br>%{percent}<extra></extra>",
            ))
            fig_bulk.update_layout(**{**CHART_LAYOUT, "height":250, "showlegend":True,
                "legend":dict(font=dict(color="white",size=12),bgcolor="rgba(0,0,0,0)",orientation="h",y=-0.1)})
            st.plotly_chart(fig_bulk, use_container_width=True)

            # Avg price by type
            for _, row in bulk_counts.iterrows():
                badge = "badge-o" if row["is_bulk"] else "badge-b"
                label = "Lot / Bundle" if row["is_bulk"] else "Article individuel"
                icon  = "📦" if row["is_bulk"] else "👕"
                st.markdown(f"""
            <div class="row-card" style="padding:12px 16px;">
              <span style="font-size:24px;">{icon}</span>
              <div style="flex:1;">
                <div class="cell-name">{label}</div>
                <div class="cell-sub">{int(row['count']):,} listings</div>
              </div>
              <div style="text-align:right;">
                <div class="cell-num">€{row['avg_price']:.2f}</div>
                <div class="cell-num2">avg price</div>
              </div>
            </div>""", unsafe_allow_html=True)

        # ── Seller Profiles + Geographic Concentration (moved from Search Intelligence) ──
        st.markdown("<hr style='border-color:#1A2A45;margin:20px 0;'>", unsafe_allow_html=True)
        col_seller, col_geo = st.columns([1, 1], gap="medium")

        with col_seller:
            st.markdown('<div class="section-label">Seller Profiles (by Location × Source)</div>', unsafe_allow_html=True)
            st.markdown('<div style="color:#E0E6F0;font-size:16px;margin-bottom:8px;">Approximated: each unique location+source group treated as a seller proxy.</div>', unsafe_allow_html=True)
            if not seller_profiles.empty:
                seller_profiles["seller_count"] = pd.to_numeric(seller_profiles["seller_count"], errors="coerce")
                seller_profiles["avg_listings"] = pd.to_numeric(seller_profiles["avg_listings"], errors="coerce")
                total_sellers = seller_profiles["seller_count"].sum()
                SELLER_COLORS = ["#3B82F6","#22C55E","#FBBF24","#EF4444"]
                fig_seller = go.Figure(go.Bar(
                    y=seller_profiles["profile"],
                    x=seller_profiles["seller_count"],
                    orientation="h",
                    marker=dict(color=SELLER_COLORS[:len(seller_profiles)], cornerradius=5),
                    text=seller_profiles["seller_count"].apply(lambda v: f"{int(v):,}"),
                    textposition="outside", textfont=dict(color="white", size=12),
                    hovertemplate="<b>%{y}</b><br>%{x:,} sellers · avg %{customdata} listings<extra></extra>",
                    customdata=seller_profiles["avg_listings"],
                ))
                fig_seller.update_layout(**{**CHART_LAYOUT, "height": 220, "showlegend": False,
                    "xaxis": {**CHART_LAYOUT["xaxis"], "title": "Seller groups"},
                    "yaxis": {**CHART_LAYOUT["yaxis"], "title": "", "autorange": "reversed"},
                    "margin": dict(l=0, r=60, t=10, b=0),
                })
                st.plotly_chart(fig_seller, use_container_width=True)
                power_row = seller_profiles[seller_profiles["sort_order"].astype(str) == "4"]
                power_pct = float(power_row["seller_count"].values[0] / total_sellers * 100) if not power_row.empty and total_sellers > 0 else 0
                occ_row = seller_profiles[seller_profiles["sort_order"].astype(str) == "1"]
                occ_pct = float(occ_row["seller_count"].values[0] / total_sellers * 100) if not occ_row.empty and total_sellers > 0 else 0
                st.markdown(
                    f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;">'
                    f'<b style="color:#3B82F6;">{occ_pct:.0f}%</b> are occasional sellers (1 listing). '
                    f'<b style="color:#EF4444;">Power sellers</b> represent <b style="color:white;">{power_pct:.0f}%</b> of seller groups '
                    f'but drive disproportionate volume — key targets for a buyback or partner programme.'
                    f'</div>', unsafe_allow_html=True,
                )
            else:
                st.info("No seller data.")

        with col_geo:
            st.markdown('<div class="section-label">Geographic Concentration</div>', unsafe_allow_html=True)
            if not top_cities.empty:
                top_cities_geo = top_cities.copy()
                top_cities_geo["cnt"] = pd.to_numeric(top_cities_geo["cnt"], errors="coerce")
                total_geo = top_cities_geo["cnt"].sum()
                top_cities_geo["share"] = (top_cities_geo["cnt"] / total_geo * 100).round(1)
                top5_share = top_cities_geo.head(5)["share"].sum()
                geo_colors = ["#FBBF24","#F97316","#EF4444","#3B82F6","#3B82F6"] + ["#1E3A6E"] * 20
                fig_geo = go.Figure(go.Bar(
                    y=top_cities_geo["city"].head(12),
                    x=top_cities_geo["share"].head(12),
                    orientation="h",
                    marker=dict(color=geo_colors[:12], cornerradius=5),
                    text=top_cities_geo["share"].head(12).apply(lambda v: f"{v:.1f}%"),
                    textposition="outside", textfont=dict(color="white", size=11),
                    hovertemplate="<b>%{y}</b><br>%{x:.1f}% of located listings<extra></extra>",
                ))
                fig_geo.update_layout(**{**CHART_LAYOUT, "height": 320, "showlegend": False,
                    "xaxis": {**CHART_LAYOUT["xaxis"], "title": "Share of located listings (%)", "ticksuffix": "%"},
                    "yaxis": {**CHART_LAYOUT["yaxis"], "title": "", "autorange": "reversed"},
                    "margin": dict(l=0, r=50, t=10, b=0),
                })
                st.plotly_chart(fig_geo, use_container_width=True)
                st.markdown(
                    f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;">'
                    f'Top 5 cities account for <b style="color:#FBBF24;">{top5_share:.1f}%</b> of all geolocated listings — '
                    f'high urban concentration. Suburbs and mid-sized cities (Lens, Amiens, Tours) show strong per-capita resale activity near Kiabi stores.'
                    f'</div>', unsafe_allow_html=True,
                )
            else:
                st.info("No geographic data.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 – ITEMS & PRICING
# ══════════════════════════════════════════════════════════════════════════════
if not _is_analyst:
 with t_items:
    if not _show_tab("Items & Pricing"):
        st.info("This section is not available in summary view. Set **APP_PROFILE=full** to see all tabs.")
    else:
        # ── Scraped listings overview ──────────────────────────────────────────────
        st.markdown(
            '<div style="color:white;font-size:22px;font-weight:700;margin-bottom:14px;">📦 Scraped Listings Overview</div>',
            unsafe_allow_html=True,
        )

        col_cat, col_src = st.columns([1, 1], gap="medium")

        with col_cat:
            st.markdown('<div class="section-label">Category Breakdown</div>', unsafe_allow_html=True)
            BLUE_PALETTE = ["#1E3A6E","#2563EB","#3B82F6","#60A5FA","#93C5FD","#BAE6FD",
                            "#06B6D4","#0891B2","#0E7490","#164E63","#134E4A"]
            fig_donut = go.Figure(go.Pie(
                labels=cats["category"], values=cats["count"],
                hole=0.6, sort=True,
                marker=dict(colors=BLUE_PALETTE, line=dict(color="#060C1C", width=2)),
                textinfo="none",
                hovertemplate="<b>%{label}</b><br>%{value:,} listings<br>%{percent}<extra></extra>",
            ))
            fig_donut.update_layout(**{**CHART_LAYOUT, "height": 300, "showlegend": True,
                "legend": dict(font=dict(color="white", size=11), bgcolor="rgba(0,0,0,0)",
                               x=1.02, y=0.5, xanchor="left")})
            fig_donut.add_annotation(
                text=f"<b>{total:,}</b><br><span style='font-size:10px'>listings</span>",
                x=0.5, y=0.5, font=dict(size=16, color="white"), showarrow=False,
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_src:
            st.markdown('<div class="section-label">Listings by Marketplace</div>', unsafe_allow_html=True)
            src_colors = [SOURCE_COLORS.get(s, "#3B82F6") for s in sources["source"]]
            fig_src = go.Figure(go.Bar(
                x=sources["source"].str.capitalize(),
                y=sources["total"],
                marker=dict(color=src_colors, cornerradius=6),
                text=sources["total"].apply(lambda v: f"{int(v):,}"),
                textposition="outside", textfont=dict(color="white", size=13),
            ))
            fig_src.update_layout(**{**CHART_LAYOUT, "height": 300, "showlegend": False})
            st.plotly_chart(fig_src, use_container_width=True)

        st.markdown('<div class="section-label">Listings Trend Over Time</div>', unsafe_allow_html=True)
        trend = df.dropna(subset=["week"]).groupby(["week", "source"]).size().reset_index(name="count")
        fig_trend = px.line(trend, x="week", y="count", color="source",
                            color_discrete_map=SOURCE_COLORS, markers=True, template="plotly_dark")
        fig_trend.update_traces(line=dict(width=2))
        fig_trend.update_layout(**{**CHART_LAYOUT, "height": 220,
            "legend": dict(orientation="h", y=1.1, font=dict(color="white", size=11), bgcolor="rgba(0,0,0,0)"),
            "xaxis_title": "", "yaxis_title": ""})
        st.plotly_chart(fig_trend, use_container_width=True)

        st.markdown("<hr style='border-color:#1A2A45;margin:20px 0;'>", unsafe_allow_html=True)

        i1,i2,i3,i4 = st.columns(4)
        top_cat = cats.iloc[0]
        top_item = popular.iloc[0] if not popular.empty else None
        i1.metric("Top Category",     top_cat["category"])
        i2.metric("Top Category Avg", f"€{float(top_cat['avg_price']):.2f}")
        i3.metric("Most Listed Item", top_item["title"][:22]+"…" if top_item is not None else "—")
        i4.metric("Avg Discount vs New", "~55%", help="Avg second-hand price vs estimated Kiabi retail price")

        st.markdown("")

        col_pop, col_price = st.columns([1, 1], gap="medium")

        with col_pop:
            # Most popular items
            st.markdown('<div class="section-label">Most Listed Items</div>', unsafe_allow_html=True)
            html = table_wrap_open([("#","0 0 36px","center"),("Item","3","left"),("Listings","1","right"),("Avg Price","1","right")])
            RANK_COLORS = ["#FBBF24","#9CA3AF","#B45309","#3B82F6","#3B82F6"]
            for rank, (_, row) in enumerate(popular.head(10).iterrows(), 1):
                color = RANK_COLORS[min(rank-1, len(RANK_COLORS)-1)]
                badge_cls = ["badge-o","badge-r","badge-r","badge-b","badge-b","badge-b","badge-b","badge-b","badge-b","badge-b"][rank-1]
                html += f"""
            <div class="row-card">
              <div style="width:36px;text-align:center;color:{color};font-size:16px;font-weight:800;flex-shrink:0;">#{rank}</div>
              <div style="flex:3;min-width:0;">
                <div class="cell-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{row['title']}</div>
                <div class="cell-sub">{row['source'].capitalize()}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <span class="{badge_cls}">{int(row['occurrences'])} listings</span>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num">€{float(row['avg_price'] or 0):.2f}</div>
              </div>
            </div>"""
            st.markdown(html + TABLE_CLOSE, unsafe_allow_html=True)

        with col_price:
            # Second-hand vs retail price comparison
            st.markdown('<div class="section-label">Second-Hand vs Kiabi Retail Price</div>', unsafe_allow_html=True)
            html = table_wrap_open([("Category","3","left"),("2nd Hand","1","right"),("Retail","1","right"),("Discount","1","right")])
            for _, row in cats.iterrows():
                cat      = row["category"]
                sh_price = float(row["avg_price"] or 0)
                rt_price = RETAIL_PRICES.get(cat, 12.0)
                discount = round((1 - sh_price / rt_price) * 100) if rt_price > 0 and sh_price > 0 else 0
                discount = max(0, min(discount, 99))
                badge_cls= "badge-g" if discount>50 else "badge-b" if discount>25 else "badge-o"
                html += f"""
            <div class="row-card">
              <div style="flex:3;min-width:0;">
                <div class="cell-name">{cat}</div>
                <div class="cell-sub">{int(row['count']):,} listings · {int(row['never_worn'])} never worn</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num">€{sh_price:.2f}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num" style="color:#E0E6F0;">€{rt_price:.0f}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <span class="{badge_cls}">-{discount}%</span>
              </div>
            </div>"""
            st.markdown(html + TABLE_CLOSE, unsafe_allow_html=True)

        st.markdown("")

        # ── Category avg price bar ─────────────────────────────────────────────
        st.markdown('<div class="section-label">Average Price by Category</div>', unsafe_allow_html=True)
        cats["avg_price"] = pd.to_numeric(cats["avg_price"], errors="coerce")
        cats_sorted = cats.sort_values("avg_price", ascending=True)
        fig_cat_price = go.Figure(go.Bar(
            y=cats_sorted["category"],
            x=cats_sorted["avg_price"],
            orientation="h",
            marker=dict(
                color=cats_sorted["avg_price"],
                colorscale=[[0,"#1E3A6E"],[0.5,"#3B82F6"],[1,"#00CFFF"]],
                cornerradius=4,
            ),
            text=cats_sorted["avg_price"].apply(lambda v: f"€{v:.2f}"),
            textposition="outside", textfont=dict(color="white",size=12),
        ))
        fig_cat_price.update_layout(**{**CHART_LAYOUT, "height":360, "showlegend":False,
            "xaxis":{**CHART_LAYOUT["xaxis"],"title":"Avg Price (€)"},
            "yaxis":{**CHART_LAYOUT["yaxis"],"title":""}})
        st.plotly_chart(fig_cat_price, use_container_width=True)

        # ── Top 10 Most Expensive Listings ────────────────────────────────────────
        st.markdown("")
        st.markdown('<div class="section-label">Top 10 Most Expensive Listings</div>', unsafe_allow_html=True)
        if not top_expensive.empty:
            top_expensive["price"] = pd.to_numeric(top_expensive["price"], errors="coerce")
            html = table_wrap_open([
                ("#", "0 0 36px", "center"),
                ("Item", "4", "left"),
                ("Platform", "1", "center"),
                ("Seller ID", "2", "left"),
                ("Price", "1", "right"),
                ("Link", "1", "center"),
            ])
            RANK_COLORS = ["#FBBF24", "#9CA3AF", "#B45309"] + ["#3B82F6"] * 7
            for rank, (_, row) in enumerate(top_expensive.head(10).iterrows(), 1):
                color = RANK_COLORS[rank - 1]
                price_val = float(row.get("price") or 0)
                title_txt = str(row.get("title") or "")
                source_txt = str(row.get("source") or "").capitalize()
                location_txt = str(row.get("location") or "")
                seller_txt = str(row.get("seller_id") or "—")
                if len(seller_txt) > 18:
                    seller_txt = seller_txt[:16] + "…"
                ad_url = str(row.get("url") or "")
                link_html = (
                    f'<a href="{ad_url}" target="_blank" style="color:#3B82F6;text-decoration:none;font-size:13px;">View →</a>'
                    if ad_url else "—"
                )
                html += f"""
            <div class="row-card">
              <div style="width:36px;text-align:center;color:{color};font-size:16px;font-weight:800;flex-shrink:0;">#{rank}</div>
              <div style="flex:4;min-width:0;">
                <div class="cell-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="{title_txt}">{title_txt[:55]}{"…" if len(title_txt)>55 else ""}</div>
                <div class="cell-sub">{location_txt[:40]}</div>
              </div>
              <div style="flex:1;text-align:center;">
                <span class="badge-b">{source_txt}</span>
              </div>
              <div style="flex:2;min-width:0;">
                <div class="cell-sub" style="font-family:monospace;font-size:11px;">{seller_txt}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num" style="color:#FBBF24;font-weight:700;">€{price_val:.2f}</div>
              </div>
              <div style="flex:1;text-align:center;">{link_html}</div>
            </div>"""
            st.markdown(html + TABLE_CLOSE, unsafe_allow_html=True)
        else:
            st.info("No data available.")

        # ── Market Insights ────────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div style="color:white;font-size:18px;font-weight:700;margin-bottom:4px;">📈 Market Insights</div>'
            '<div style="color:#E0E6F0;font-size:18px;margin-bottom:18px;">Price dynamics, never-worn premium, competitive positioning and geography</div>',
            unsafe_allow_html=True,
        )

        ins_left, ins_right = st.columns([1, 1], gap="medium")

        # ── Price distribution bar chart ──────────────────────────────────────────
        with ins_left:
            st.markdown('<div class="section-label">Price Distribution</div>', unsafe_allow_html=True)
            if not price_dist.empty:
                price_dist["cnt"] = pd.to_numeric(price_dist["cnt"], errors="coerce")
                total_priced = price_dist["cnt"].sum()
                price_dist["pct"] = (price_dist["cnt"] / total_priced * 100).round(1)
                PRICE_COLORS = ["#2563EB","#3B82F6","#60A5FA","#93C5FD","#BAE6FD","#E0F2FE"]
                fig_price = go.Figure(go.Bar(
                    x=price_dist["price_range"],
                    y=price_dist["pct"],
                    marker=dict(
                        color=PRICE_COLORS[:len(price_dist)],
                        cornerradius=6,
                    ),
                    text=price_dist.apply(lambda r: f"{r['pct']:.1f}%<br><span style='font-size:10px'>{int(r['cnt']):,} items</span>", axis=1),
                    textposition="outside",
                    textfont=dict(color="white", size=11),
                    hovertemplate="<b>%{x}</b><br>%{y:.1f}% of listings<br>%{customdata:,} items<extra></extra>",
                    customdata=price_dist["cnt"],
                ))
                median_price = float(df["price"].median()) if not df.empty else 0
                fig_price.add_hline(y=0, line_color="rgba(0,0,0,0)")
                fig_price.update_layout(**{**CHART_LAYOUT, "height": 300, "showlegend": False,
                    "yaxis": {**CHART_LAYOUT["yaxis"], "title": "% of listings", "ticksuffix": "%"},
                    "xaxis": {**CHART_LAYOUT["xaxis"], "title": ""},
                })
                st.plotly_chart(fig_price, use_container_width=True)
                under10 = price_dist[price_dist["sort_order"].astype(int) <= 2]["pct"].sum() if "sort_order" in price_dist.columns else 0
                st.markdown(
                    f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;">'
                    f'<b style="color:white;">{under10:.0f}%</b> of listings sell for under €10 · '
                    f'Median price <b style="color:#00CFFF;">€{median_price:.2f}</b> · '
                    f'Ultra-affordable positioning vs retail'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("No price data.")

        # ── Never Worn premium ────────────────────────────────────────────────────
        with ins_right:
            st.markdown('<div class="section-label">"Never Worn" Premium</div>', unsafe_allow_html=True)
            if not never_worn_stats.empty:
                never_worn_stats["cnt"] = pd.to_numeric(never_worn_stats["cnt"], errors="coerce")
                never_worn_stats["median_price"] = pd.to_numeric(never_worn_stats["median_price"], errors="coerce")
                never_worn_stats["is_never_worn"] = never_worn_stats["is_never_worn"].astype(str).str.lower().isin(["true","1","yes"])
                nw = never_worn_stats[never_worn_stats["is_never_worn"] == True]
                used = never_worn_stats[never_worn_stats["is_never_worn"] == False]
                nw_median  = float(nw["median_price"].values[0])  if not nw.empty  else 8.0
                used_median= float(used["median_price"].values[0]) if not used.empty else 5.0
                nw_cnt  = int(nw["cnt"].values[0])   if not nw.empty   else 0
                used_cnt= int(used["cnt"].values[0])  if not used.empty else 0
                total_nw = nw_cnt + used_cnt
                nw_share = nw_cnt / total_nw * 100 if total_nw > 0 else 17.0
                premium  = (nw_median / used_median - 1) * 100 if used_median > 0 else 60.0

                fig_nw = go.Figure()
                fig_nw.add_trace(go.Bar(
                    x=["Used", "Never Worn"],
                    y=[used_median, nw_median],
                    marker=dict(color=["#3B82F6", "#FBBF24"], cornerradius=8),
                    text=[f"€{used_median:.2f}", f"€{nw_median:.2f}"],
                    textposition="outside", textfont=dict(color="white", size=14, family="monospace"),
                    hovertemplate="<b>%{x}</b><br>Median: €%{y:.2f}<extra></extra>",
                    width=0.4,
                ))
                fig_nw.update_layout(**{**CHART_LAYOUT, "height": 220, "showlegend": False,
                    "yaxis": {**CHART_LAYOUT["yaxis"], "title": "Median price (€)", "range": [0, nw_median * 1.35]},
                    "xaxis": {**CHART_LAYOUT["xaxis"], "title": ""},
                })
                st.plotly_chart(fig_nw, use_container_width=True)

                c1, c2, c3 = st.columns(3)
                c1.metric("Price premium", f"+{premium:.0f}%")
                c2.metric("Never-worn share", f"{nw_share:.1f}%")
                c3.metric("Median uplift", f"€{nw_median - used_median:.2f}")
                st.markdown(
                    f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;margin-top:8px;">'
                    f'<b style="color:#FBBF24;">Never-worn</b> items command a <b style="color:white;">{premium:.0f}% price premium</b> '
                    f'over used items, but represent only <b style="color:white;">{nw_share:.1f}%</b> of all listings — '
                    f'a clear opportunity to capture higher-value resale.'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("No never-worn data.")

        # ── Top cities ─────────────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-label">Top Cities for Kiabi Second-Hand Resellers</div>', unsafe_allow_html=True)
        if not top_cities.empty:
            top_cities["cnt"] = pd.to_numeric(top_cities["cnt"], errors="coerce")
            top_cities = top_cities.dropna(subset=["cnt"]).head(12)
            city_colors = ["#FBBF24" if i == 0 else "#3B82F6" for i in range(len(top_cities))]
            fig_cities = go.Figure(go.Bar(
                x=top_cities["city"],
                y=top_cities["cnt"],
                marker=dict(color=city_colors, cornerradius=6),
                text=top_cities["cnt"].astype(int),
                textposition="outside", textfont=dict(color="white", size=12),
                hovertemplate="<b>%{x}</b><br>%{y:,} listings<extra></extra>",
            ))
            fig_cities.update_layout(**{**CHART_LAYOUT, "height": 280, "showlegend": False,
                "yaxis": {**CHART_LAYOUT["yaxis"], "title": "Listings"},
                "xaxis": {**CHART_LAYOUT["xaxis"], "title": ""},
            })
            st.plotly_chart(fig_cities, use_container_width=True)
            unique_locs = df["location"].nunique() if not df.empty else 0
            top1 = top_cities.iloc[0]
            st.markdown(
                f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;">'
                f'Listings span <b style="color:white;">{unique_locs:,} unique locations</b>. '
                f'<b style="color:#FBBF24;">{top1["city"]}</b> leads with <b style="color:white;">{int(top1["cnt"]):,} listings</b>, '
                f'reflecting high population density and strong second-hand culture in major urban centres.'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("No city data.")

        # ── Listing Freshness + Condition (moved from Search Intelligence) ────────
        st.markdown("<hr style='border-color:#1A2A45;margin:20px 0;'>", unsafe_allow_html=True)
        col_age, col_cond = st.columns([1, 1], gap="medium")

        with col_age:
            st.markdown('<div class="section-label">Listing Freshness (Age Distribution)</div>', unsafe_allow_html=True)
            if not listing_age.empty:
                listing_age["cnt"] = pd.to_numeric(listing_age["cnt"], errors="coerce")
                total_age = listing_age["cnt"].sum()
                listing_age["pct"] = (listing_age["cnt"] / total_age * 100).round(1)
                AGE_COLORS = ["#22C55E","#3B82F6","#FBBF24","#F97316","#EF4444"]
                fig_age = go.Figure(go.Bar(
                    x=listing_age["age_bucket"],
                    y=listing_age["pct"],
                    marker=dict(color=AGE_COLORS[:len(listing_age)], cornerradius=6),
                    text=listing_age.apply(lambda r: f"{r['pct']:.1f}%<br><span style='font-size:10px'>{int(r['cnt']):,}</span>", axis=1),
                    textposition="outside", textfont=dict(color="white", size=11),
                    hovertemplate="<b>%{x}</b><br>%{y:.1f}% · %{customdata:,} listings<extra></extra>",
                    customdata=listing_age["cnt"],
                ))
                fig_age.update_layout(**{**CHART_LAYOUT, "height": 280, "showlegend": False,
                    "yaxis": {**CHART_LAYOUT["yaxis"], "title": "% of listings", "ticksuffix": "%"},
                    "xaxis": {**CHART_LAYOUT["xaxis"], "title": ""},
                })
                st.plotly_chart(fig_age, use_container_width=True)
                fresh_row = listing_age[listing_age["sort_order"].astype(str) == "1"]
                fresh_pct = float(fresh_row["pct"].values[0]) if not fresh_row.empty else 0
                old_rows = listing_age[listing_age["sort_order"].astype(int) >= 4]
                old_pct = float(old_rows["pct"].sum()) if not old_rows.empty else 0
                st.markdown(
                    f'<div style="background:#08121E;border:1px solid #1A2A45;border-radius:10px;padding:14px 18px;font-size:16px;color:#EEF0F6;">'
                    f'<b style="color:#22C55E;">{fresh_pct:.1f}%</b> of listings are &lt;7 days old (active market). '
                    f'<b style="color:#EF4444;">{old_pct:.1f}%</b> are older than 90 days — '
                    f'suggesting slow-moving stock that may need repricing.'
                    f'</div>', unsafe_allow_html=True,
                )
            else:
                st.info("No listing age data.")

        with col_cond:
            st.markdown('<div class="section-label">Condition Signal Distribution</div>', unsafe_allow_html=True)
            if not condition_dist.empty:
                condition_dist["cnt"] = pd.to_numeric(condition_dist["cnt"], errors="coerce")
                condition_dist["avg_price"] = pd.to_numeric(condition_dist["avg_price"], errors="coerce")
                total_cond = condition_dist["cnt"].sum()
                condition_dist["pct"] = (condition_dist["cnt"] / total_cond * 100).round(1)
                COND_COLORS = ["#FBBF24","#22C55E","#3B82F6","#F97316","#EF4444","#6B7A99"]
                fig_cond = go.Figure(go.Pie(
                    labels=condition_dist["condition_label"],
                    values=condition_dist["cnt"],
                    hole=0.55,
                    marker=dict(colors=COND_COLORS[:len(condition_dist)], line=dict(color="#060C1C", width=2)),
                    textinfo="none",
                    hovertemplate="<b>%{label}</b><br>%{value:,} listings · %{percent}<extra></extra>",
                ))
                fig_cond.update_layout(**{**CHART_LAYOUT, "height": 220, "showlegend": True,
                    "legend": dict(font=dict(color="white", size=10), bgcolor="rgba(0,0,0,0)",
                                   x=1.02, y=0.5, xanchor="left"),
                })
                st.plotly_chart(fig_cond, use_container_width=True)
                html_cond = table_wrap_open([("Condition","3","left"),("Listings","1","right"),("Avg Price","1","right")])
                for _, row in condition_dist.iterrows():
                    html_cond += f"""
                <div class="row-card" style="padding:10px 16px;">
                  <div style="flex:3;min-width:0;">
                    <div class="cell-name" style="font-size:13px;">{row['condition_label']}</div>
                  </div>
                  <div style="flex:1;text-align:right;"><div class="cell-num">{int(row['cnt']):,}</div></div>
                  <div style="flex:1;text-align:right;"><div class="cell-num">€{float(row['avg_price'] or 0):.2f}</div></div>
                </div>"""
                st.markdown(html_cond + TABLE_CLOSE, unsafe_allow_html=True)
            else:
                st.info("No condition data.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 – STRATEGY ASSISTANT (hidden in analyst view)
# ══════════════════════════════════════════════════════════════════════════════
if not _is_analyst:
 with t_strategy:
    st.markdown("""
    <div style="margin-bottom:1.5rem;">
      <div style="font-size:22px;font-weight:700;color:white;margin-bottom:6px;">
        🧠 Kiabi Second-Hand Strategy Assistant
      </div>
      <div style="color:#E0E6F0;font-size:17px;">
        Ask questions about Kiabi's ESG strategy, second-hand ambitions (Beebs, Vision 2035),
        competitor positioning (Vinted, Zara, Petit Bateau…) and market insights.
        Powered by <strong style="color:#00CFFF">Claude Sonnet</strong> + Kiabi DPEF 2024.
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Suggested questions
    suggested = [
        "What is Kiabi's second-hand target for 2035 and how realistic is it?",
        "How does Beebs compare to Vinted as a strategy?",
        "What are the top competitor second-hand strategies Kiabi should benchmark?",
        "How can the marketplace listing data inform Kiabi's pricing strategy?",
        "What does the DPEF 2024 say about circularity of the offer?",
        "Which KPIs should Kiabi track to measure second-hand progress?",
    ]
    st.markdown('<div style="color:#E0E6F0;font-size:16px;text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;">Suggested questions</div>', unsafe_allow_html=True)
    cols_s = st.columns(3)
    for i, q in enumerate(suggested):
        if cols_s[i % 3].button(q, key=f"suggest_{i}", use_container_width=True):
            st.session_state["strategy_input"] = q

    st.markdown("<hr style='border-color:#1E3A6E;margin:1rem 0;'>", unsafe_allow_html=True)

    # Chat history
    if "strategy_history" not in st.session_state:
        st.session_state.strategy_history = []

    # Display existing messages
    for msg in st.session_state.strategy_history:
        with st.chat_message(msg["role"], avatar="🧠" if msg["role"] == "assistant" else "👤"):
            st.markdown(msg["content"])

    # Input
    user_input = st.chat_input(
        "Ask about Kiabi's second-hand strategy, ESG data, or competitor analysis…",
        key="strategy_chat_input",
    )
    if not user_input and st.session_state.get("strategy_input"):
        user_input = st.session_state.pop("strategy_input")

    if user_input:
        st.session_state.strategy_history.append({"role": "user", "content": user_input})
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_input)
        with st.chat_message("assistant", avatar="🧠"):
            with st.spinner("Analysing…"):
                response = _ask_strategy(user_input, st.session_state.strategy_history[:-1])
            st.markdown(response)
        st.session_state.strategy_history.append({"role": "assistant", "content": response})

    if st.session_state.strategy_history:
        if st.button("🗑️ Clear conversation", key="clear_strategy"):
            st.session_state.strategy_history = []
            st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 – IMAGE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
_IMAGE_ENDPOINT = "kiabi-image-matching"

def _call_image_matching(image_url: str) -> list:
    """Call the image matching endpoint and return top results."""
    try:
        w = _client()
        payload = {"dataframe_records": [{"image_url": image_url, "num_results": 5}]}
        resp = w.api_client.do(
            "POST",
            f"/serving-endpoints/{_IMAGE_ENDPOINT}/invocations",
            body=payload,
        )
        # Handle various response formats
        if isinstance(resp, dict):
            if "predictions" in resp:
                return resp["predictions"]
            if "dataframe_records" in resp:
                return resp["dataframe_records"]
            if "outputs" in resp:
                return resp["outputs"] if isinstance(resp["outputs"], list) else [resp["outputs"]]
            # If the response itself looks like a list of results
            return [resp] if resp else []
        if isinstance(resp, list):
            return resp
        return []
    except Exception as e:
        st.error(f"⚠️ Image matching error: {e}")
        return []

if not _is_analyst:
 with t_images:
    st.markdown(
        '<div style="margin-bottom:1.5rem;">'
        '<div style="font-size:22px;font-weight:700;color:white;margin-bottom:6px;">🖼️ Image Analysis — Operational Use Case in Action</div>'
        '<div style="color:#E0E6F0;font-size:17px;">'
        'Submit an image URL to find matching second-hand Kiabi listings. '
        'Powered by the <strong style="color:#00CFFF">kiabi-image-matching</strong> serving endpoint.'
        '</div></div>',
        unsafe_allow_html=True,
    )

    col_input, col_btn = st.columns([5, 1])
    with col_input:
        _img_url = st.text_input(
            "Image URL",
            placeholder="Paste an image URL (e.g. https://example.com/photo.jpg)",
            key="image_url_input",
            label_visibility="collapsed",
        )
    with col_btn:
        _img_submit = st.button("🔍 Match", key="image_match_btn", use_container_width=True)

    # Show submitted image preview
    if _img_url:
        st.markdown(
            f'<div style="margin:12px 0;">'
            f'<img src="{_img_url}" style="max-height:260px;border-radius:12px;border:1px solid #1E3A6E;" '
            f'onerror="this.style.display=\'none\'">'
            f'</div>',
            unsafe_allow_html=True,
        )

    if _img_submit and _img_url:
        with st.spinner("Analysing image and searching for matches…"):
            results = _call_image_matching(_img_url)

        if results:
            st.markdown(
                f'<div style="color:white;font-size:18px;font-weight:700;margin:16px 0 10px;">Top {min(len(results), 2)} Matching Listings</div>',
                unsafe_allow_html=True,
            )
            html = table_wrap_open([
                ("#", "0 0 36px", "center"),
                ("Item", "4", "left"),
                ("Platform", "1", "center"),
                ("Price", "1", "right"),
                ("Score", "1", "right"),
                ("Link", "1", "center"),
            ])
            RANK_COLORS = ["#FBBF24", "#9CA3AF", "#B45309"]
            for rank, item in enumerate(results[:2], 1):
                color = RANK_COLORS[min(rank - 1, len(RANK_COLORS) - 1)]
                title_txt = str(item.get("title") or item.get("name") or "Listing")
                price_val = item.get("price") or item.get("predicted_price") or "—"
                price_str = f"€{float(price_val):.2f}" if isinstance(price_val, (int, float)) else str(price_val)
                source_txt = str(item.get("source") or item.get("platform") or "—").capitalize()
                score_val = item.get("score") or item.get("similarity") or item.get("distance") or "—"
                score_str = f"{float(score_val):.2f}" if isinstance(score_val, (int, float)) else str(score_val)
                ad_url = str(item.get("url") or item.get("link") or item.get("listing_url") or "")
                link_html = (
                    f'<a href="{ad_url}" target="_blank" style="color:#3B82F6;text-decoration:none;font-size:13px;">View →</a>'
                    if ad_url else "—"
                )
                img_thumb = item.get("image_url") or item.get("thumbnail") or ""
                thumb_html = (
                    f'<img src="{img_thumb}" style="width:44px;height:44px;border-radius:8px;object-fit:cover;border:1px solid #1E3A6E;margin-right:10px;">'
                    if img_thumb else ""
                )
                html += f"""
            <div class="row-card">
              <div style="width:36px;text-align:center;color:{color};font-size:16px;font-weight:800;flex-shrink:0;">#{rank}</div>
              <div style="flex:4;min-width:0;display:flex;align-items:center;">
                {thumb_html}
                <div>
                  <div class="cell-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="{title_txt}">{title_txt[:60]}{"…" if len(title_txt)>60 else ""}</div>
                </div>
              </div>
              <div style="flex:1;text-align:center;">
                <span class="badge-b">{source_txt}</span>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num" style="color:#FBBF24;font-weight:700;">{price_str}</div>
              </div>
              <div style="flex:1;text-align:right;">
                <div class="cell-num">{score_str}</div>
              </div>
              <div style="flex:1;text-align:center;">{link_html}</div>
            </div>"""
            st.markdown(html + TABLE_CLOSE, unsafe_allow_html=True)
        elif _img_url:
            st.warning("No matching listings found. Try a different image.")
