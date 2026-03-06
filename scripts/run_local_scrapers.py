#!/usr/bin/env python3
"""
Run Leboncoin and Vinted scrapers locally (no --demo), then upload JSONL to a
Databricks Unity Catalog Volume. Use this from your machine so scraping isn't
blocked by Datadome/403.

Prerequisites:
  - pip install -r requirements.txt   # lbc, requests, databricks-sdk
  - Databricks auth: set DATABRICKS_HOST and DATABRICKS_TOKEN, or use
    databricks configure / ~/.databrickscfg profile

Usage:
  python scripts/run_local_scrapers.py
  python scripts/run_local_scrapers.py --catalog main --schema second_hand
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# Project root (parent of scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
SCRAPERS_DIR = PROJECT_ROOT / "src" / "scrapers"


def _parse_args():
    p = argparse.ArgumentParser(description="Run Leboncoin + Vinted scrapers locally and upload to Databricks Volume")
    p.add_argument("--brand", default="kiabi", help="Brand to search")
    p.add_argument("--catalog", default="nef_catalog", help="Unity Catalog catalog name")
    p.add_argument("--schema", default="second_hand", help="Schema name")
    p.add_argument("--volume", default="kiabi_landing", help="Volume name")
    p.add_argument("--max-items", type=int, default=50000, help="Target Leboncoin items (default 50000)")
    p.add_argument("--max-items-vinted", type=int, default=960, help="Target Vinted items (default 960, site cap)")
    p.add_argument("--max-items-ebay", type=int, default=5000, help="Target eBay items (default 5000)")
    p.add_argument("--skip-leboncoin", action="store_true", help="Skip Leboncoin scraper")
    p.add_argument("--skip-vinted", action="store_true", help="Skip Vinted scraper")
    p.add_argument("--skip-ebay", action="store_true", help="Skip eBay scraper")
    p.add_argument("--skip-label-emmaus", action="store_true", help="Skip Label Emmaüs scraper")
    p.add_argument("--skip-competition", action="store_true", help="Skip competition search counts (Leboncoin/Vinted/eBay)")
    p.add_argument("--no-ebay", action="store_true", help="When running competition script: skip eBay (no EBAY_APP_ID)")
    p.add_argument("--skip-upload", action="store_true", help="Only scrape; do not upload to Databricks")
    return p.parse_args()


def run_scraper(name: str, script: str, brand: str, extra_args: list[str]) -> bool:
    """Run a scraper script; return True on success."""
    cmd = [sys.executable, str(SCRAPERS_DIR / script), "--brand", brand, "--output-base", str(OUTPUT_DIR / name)] + extra_args
    print(f"\n--- {name} ---")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return result.returncode == 0


def run_competition(output_dir: Path, no_ebay: bool) -> bool:
    """Run competition search counts script; write JSONL to output_dir/competition. Return True on success."""
    comp_dir = output_dir / "competition"
    comp_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "competition_search_counts.py"), "--output-dir", str(comp_dir)]
    if no_ebay:
        cmd.append("--no-ebay")
    print("\n--- competition (search counts) ---")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return result.returncode == 0


def upload_to_volume(catalog: str, schema: str, volume: str) -> None:
    """Upload output/*/*.jsonl (leboncoin, vinted, ebay, label_emmaus, competition) to the UC Volume."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        sys.exit(1)

    w = WorkspaceClient()
    base_volume = f"/Volumes/{catalog}/{schema}/{volume}"
    uploaded = 0
    sources = ("leboncoin", "vinted", "ebay", "label_emmaus", "competition")

    for source in sources:
        local_dir = OUTPUT_DIR / source
        if not local_dir.exists():
            continue
        for f in local_dir.glob("*.jsonl"):
            volume_path = f"{base_volume}/{source}/{f.name}"
            try:
                w.files.upload_from(volume_path, str(f), overwrite=True)
                print(f"Uploaded {f.name} -> {volume_path}")
                uploaded += 1
            except Exception as e:
                print(f"Upload failed {f}: {e}", file=sys.stderr)

    if uploaded == 0:
        print("No files to upload. Run scrapers without --skip-upload.", file=sys.stderr)
    else:
        print(f"\nUploaded {uploaded} file(s) to {base_volume}")
        print("Run the pipeline in Databricks to refresh bronze/silver/gold (e.g. databricks bundle run kiabi_etl).")


if __name__ == "__main__":
    args = _parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not args.skip_leboncoin:
        # Leboncoin API caps at 100 pages (~3.5k) per search. Use --multi-query to run several category searches and reach ~50k unique.
        leboncoin_args = ["--max-items", str(args.max_items)]
        if args.max_items > 4000:
            leboncoin_args.append("--multi-query")
        run_scraper("leboncoin", "leboncoin_scraper.py", args.brand, leboncoin_args)
    if not args.skip_vinted:
        run_scraper("vinted", "vinted_scraper.py", args.brand, ["--max-items", str(args.max_items_vinted)])
    if not args.skip_ebay:
        run_scraper("ebay", "ebay_scraper.py", args.brand, ["--max-items", str(args.max_items_ebay)])
    if not args.skip_label_emmaus:
        run_scraper("label_emmaus", "label_emmaus_scraper.py", args.brand, [])

    if not args.skip_competition:
        run_competition(OUTPUT_DIR, no_ebay=args.no_ebay)

    if not args.skip_upload:
        upload_to_volume(args.catalog, args.schema, args.volume)
