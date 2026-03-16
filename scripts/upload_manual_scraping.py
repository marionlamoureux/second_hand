#!/usr/bin/env python3
"""
Upload manual scraping files from local output/manual_scraping/ to the Databricks
landing volume so the Kiabi ETL pipeline can ingest them.

Layout:
  - ebay*.csv, *vestiaire*.csv → {volume}/manual_scraping/listings/
  - essentials_bebe.csv → {volume}/manual_scraping/essentials_bebe/
  - essentials_femme.csv → {volume}/manual_scraping/essentials_femme/

Requires: databricks-sdk, run from repo root.
Usage:
  python scripts/upload_manual_scraping.py
  python scripts/upload_manual_scraping.py --catalog nef_catalog --schema second_hand --volume kiabi_landing
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

def _parse_args():
    p = argparse.ArgumentParser(description="Upload manual_scraping CSVs to UC landing volume")
    p.add_argument("--catalog", default="nef_catalog", help="Unity Catalog catalog")
    p.add_argument("--schema", default="second_hand", help="Schema name")
    p.add_argument("--volume", default="kiabi_landing", help="Landing volume name")
    p.add_argument("--source-dir", default=None, help="Local folder (default: repo output/manual_scraping)")
    return p.parse_args()


def main():
    args = _parse_args()
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        raise SystemExit("Install databricks-sdk: pip install databricks-sdk")

    repo_root = Path(__file__).resolve().parent.parent
    source_dir = Path(args.source_dir) if args.source_dir else repo_root / "output" / "manual_scraping"
    if not source_dir.is_dir():
        raise SystemExit(f"Source directory not found: {source_dir}")

    base = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    listings_path = f"{base}/manual_scraping/listings"
    essentials_bebe_path = f"{base}/manual_scraping/essentials_bebe"
    essentials_femme_path = f"{base}/manual_scraping/essentials_femme"

    w = WorkspaceClient()
    uploaded = 0

    for f in source_dir.iterdir():
        if not f.is_file() or f.suffix.lower() != ".csv":
            continue
        name = f.name.lower()
        if "essentials_bebe" in name:
            dest = f"{essentials_bebe_path}/{f.name}"
        elif "essentials_femme" in name:
            dest = f"{essentials_femme_path}/{f.name}"
        elif "ebay" in name or "vestiaire" in name:
            dest = f"{listings_path}/{f.name}"
        else:
            continue
        with open(f, "rb") as fp:
            w.files.upload(dest, fp, overwrite=True)
        print(f"Uploaded {f.name} -> {dest}")
        uploaded += 1

    if uploaded == 0:
        print("No matching CSV files in", source_dir)
        return
    print(f"Done. Uploaded {uploaded} file(s). Run the Kiabi ETL pipeline, then 'Kiabi - download listing images' for all sources.")


if __name__ == "__main__":
    main()
