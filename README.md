# Second-hand Kiabi pipeline

Databricks Asset Bundle that **scrapes** Kiabi brand items from **Leboncoin**, **Vinted**, **Facebook Marketplace**, **eBay**, and **Label Emmaüs** (label-emmaus.co), then runs a **Spark Declarative Pipeline** to land and clean the data in a medallion layout (bronze → silver → gold).

## Prerequisites

- Databricks workspace with **Unity Catalog** and a catalog/schema (e.g. `main.second_hand_dev`).
- **Serverless** compute enabled if you use the default serverless pipeline.
- Configure `databricks.yml` targets with your workspace `host` (and profile if using CLI).

## Project layout

```
second-hand/
├── databricks.yml              # Bundle and targets (dev/prod)
├── resources/
│   ├── jobs.yml                # Kiabi scraper job (3 tasks: leboncoin, vinted, facebook)
│   ├── volumes.yml             # kiabi_landing, kiabi_etl_metadata
│   └── kiabi_etl.pipeline.yml  # SDP pipeline definition
├── scripts/
│   ├── run_local_scrapers.py   # Run Leboncoin, Vinted, eBay, Label Emmaüs, competition locally; upload to Volume
│   └── competition_search_counts.py  # Competitor search counts (Leboncoin/Vinted/eBay); JSONL for pipeline
├── src/
│   ├── scrapers/
│   │   ├── leboncoin_scraper.py   # Uses lbc
│   │   ├── vinted_scraper.py      # Uses requests / curl_cffi (catalog API)
│   │   ├── facebook_scraper.py   # Placeholder (see below)
│   │   ├── ebay_scraper.py        # eBay Finding API (EBAY_APP_ID)
│   │   └── label_emmaus_scraper.py  # Label Emmaüs catalogue (BeautifulSoup)
│   └── kiabi_etl/
│       └── transformations/
│           ├── bronze_listings.py
│           ├── silver_listings.py
│           ├── silver_competition_counts.py
│           ├── gold_kiabi_listings.py
│           └── gold_competition_counts.py
└── requirements.txt            # For local runs (optional)
```

## Scrapers

- **Leboncoin**: uses the unofficial [lbc](https://pypi.org/project/lbc/) client. The Leboncoin API returns at most **100 pages per search** (~3,500 items at 35/page). To get toward **~50k items**, run with `--max-items 50000` (or `--multi-query`): the scraper then runs several category-based searches (clothing, shoes, kids, baby, etc.), merges and dedupes by ad ID. A delay between pages helps reduce Datadome blocking.
- **Vinted**: uses `requests` against Vinted’s catalog-style endpoints; Direct API calls from datacenter IPs often get 403; consider a proxy or partner API for production.
- **Facebook Marketplace**: **placeholder only**. Facebook’s ToS prohibit automated scraping. Use an official integration (e.g. Commerce API) or an approved data provider and feed JSONL into the same landing path; the pipeline schema supports a `facebook` source.
- **eBay**: uses the eBay Finding API via `ebaysdk-python`. Set **EBAY_APP_ID** (env or `--app-id`) for real searches; without it the scraper runs in demo mode.
- **Label Emmaüs**: scrapes [label-emmaus.co](https://www.label-emmaus.co/fr/catalogue) with `?q=brand` and parses product cards (BeautifulSoup).

## Deploy and run

1. **Configure workspace**  
   Set `workspace.host` (and optionally `workspace.profile`) in `databricks.yml` for each target.

2. **Validate and deploy**
   ```bash
   databricks bundle validate
   databricks bundle deploy
   ```

3. **Create the landing volume**  
   The bundle creates `kiabi_landing` and `kiabi_etl_metadata` in the target schema. Ensure the job and pipeline have permission to read/write.

4. **Run the scraper job**
   ```bash
   databricks bundle run kiabi_scraper_job
   ```
   This runs the three tasks (leboncoin, vinted, facebook) and writes JSONL under:
   `/Volumes/<catalog>/<schema>/kiabi_landing/{leboncoin,vinted,facebook}/`.

5. **Run the pipeline**
   ```bash
   databricks bundle run kiabi_etl
   ```
   The pipeline reads from the landing volume, writes **bronze** → **silver** → **gold** in the same catalog/schema.

## Competition search counts

To compare Kiabi with competitors (kids: Petit Bateau, Okaïdi, etc.; women’s: Comptoir des Cotonniers, Mango, etc.) on Leboncoin, Vinted, and eBay:

```bash
pip install -r requirements.txt   # lbc, requests, ebaysdk-python
python scripts/competition_search_counts.py
python scripts/competition_search_counts.py --output competition.json   # also writes JSON + CSV
python scripts/competition_search_counts.py --no-ebay --kids-only      # Leboncoin + Vinted, kids only
```

Set `EBAY_APP_ID` for eBay counts. Vinted’s public API may not expose total count (script returns what’s available).

To load competition into the pipeline: run `run_local_scrapers.py` (runs competition script, uploads to landing volume), then `databricks bundle run kiabi_etl -t dev`. Use `--skip-competition` or `--no-ebay` as needed.

## Scheduling

- The scraper job is scheduled (in `resources/jobs.yml`) with a daily cron in `Europe/Paris` (e.g. 08:00). Adjust or disable as needed.
- The pipeline is **triggered** (not continuous by default). Trigger it after the scraper job or on a schedule via a separate job with a `pipeline_task`.

## Output tables

| Layer  | Table                     | Description |
|--------|---------------------------|-------------|
| Bronze | `bronze_listings`         | Raw listing JSONL + `_ingested_at`, `_source_file` |
| Silver | `silver_listings`         | Cleaned listing rows, normalized types |
| Gold   | `gold_kiabi_listings`     | One row per listing per source (deduplicated by `source` + `external_id`) |
| Silver | `silver_competition_counts` | Competition rows from bronze (brand, category, marketplace, count), cleaned with `run_ts` |
| Gold   | `gold_competition_counts`   | Latest count per (brand, category, marketplace) |

## Scrape locally and upload to Databricks

To avoid Datadome/403, run **Leboncoin** and **Vinted** scrapers on your machine, then upload the JSONL to the Databricks Volume. The pipeline in Databricks will then ingest into Delta (bronze/silver/gold).

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   (includes `lbc`, `requests`, `databricks-sdk`)

2. **Authenticate with Databricks** (one of):
   - `databricks configure` and use the default profile, or
   - Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in the environment

3. **Run the local script** (from the project root)
   ```bash
   python scripts/run_local_scrapers.py
   ```
   This will:
   - Scrape Leboncoin and Vinted for “kiabi” (no `--demo`), writing to `output/leboncoin/` and `output/vinted/`
   - Upload all `*.jsonl` files to `/Volumes/<catalog>/<schema>/kiabi_landing/{leboncoin,vinted}/`

4. **Optional arguments**
   ```bash
   python scripts/run_local_scrapers.py --catalog nef_catalog --schema second_hand
   python scripts/run_local_scrapers.py --skip-vinted          # only Leboncoin
   python scripts/run_local_scrapers.py --skip-upload          # only scrape, no upload
   ```

5. **Refresh Delta tables in Databricks**  
   After upload, run the pipeline so bronze/silver/gold are updated:
   ```bash
   databricks bundle run kiabi_etl
   ```

6. **Reload all landing files (full backfill)**  
   The pipeline streams from the landing volume and uses a checkpoint, so it only processes *new* files after the first run. To **reprocess every file** (e.g. after re-upload or to fix missing rows):
   - In the workspace, open **Pipelines** → your Kiabi ETL pipeline.
   - Start an update and choose **Reset streaming flow checkpoints** (for the bronze flow, and optionally silver/gold so they reprocess from bronze).
   - Run the pipeline again; it will re-read all JSONL under the landing volume and refill bronze/silver/gold.

## Troubleshooting

- **Gold still not up to date**  
  1. Run the pipeline: `databricks bundle run kiabi_etl -t dev`.  
  2. If gold has too few rows (e.g. only one Leboncoin row), bronze may not have reprocessed all landing files. In **Pipelines** → Kiabi ETL → start an update with **Reset streaming flow checkpoints** (bronze), then run the pipeline again.  
  3. Optionally run local scrapers and upload (`python scripts/run_local_scrapers.py`), then run the pipeline (and reset checkpoints if you need a full backfill).

## Local development

- Scrapers write to a local path when `--output-base` is a local directory (e.g. `output/leboncoin`). In Databricks they write to the job’s Volume path.

## Compliance and limits

- **Leboncoin / Vinted**: Running from cloud (e.g. Databricks) often triggers anti-bot (Leboncoin Datadome) or 403 (Vinted). Use `--delay`, residential IPs, or third-party APIs (Apify, Browsable) for production.
- **Facebook Marketplace**: Do not scrape; use only approved data sources and update `facebook_scraper.py` (or replace with an API export step) accordingly.
