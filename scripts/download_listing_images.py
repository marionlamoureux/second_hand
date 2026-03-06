#!/usr/bin/env python3
"""
Download primary listing images from gold_kiabi_listings into a Unity Catalog volume
for later image analysis. Reads listing_key, source, external_id, primary_image_url from
the gold table, downloads each image, and writes to:
  /Volumes/{catalog}/{schema}/{images_volume}/{source}/{external_id}/0.{ext}

Run after the ETL pipeline so gold has primary_image_url. Requires:
  - databricks-sdk, requests
  - Volume kiabi_images (or --images-volume) must exist; create it in the schema if needed.
  - DATABRICKS_HOST and DATABRICKS_TOKEN (or profile) for workspace auth.

Usage:
  python scripts/download_listing_images.py
  python scripts/download_listing_images.py --catalog nef_catalog --schema second_hand --warehouse-id beb00aeaaa803c3e
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import time
from urllib.parse import urlparse

def _parse_args():
    p = argparse.ArgumentParser(description="Download listing images from gold table into UC volume")
    p.add_argument("--catalog", default="nef_catalog", help="Unity Catalog catalog")
    p.add_argument("--schema", default="second_hand", help="Schema name")
    p.add_argument("--warehouse-id", default="beb00aeaaa803c3e", help="SQL warehouse for querying gold table")
    p.add_argument("--images-volume", default="kiabi_images", help="Volume name for storing images")
    p.add_argument("--delay", type=float, default=0.3, help="Seconds between downloads (rate limit)")
    p.add_argument("--skip-existing", action="store_true", help="Skip if image file already exists in volume")
    p.add_argument("--limit", type=int, default=0, help="Max number of images to download (0 = no limit)")
    return p.parse_args()


def _safe_path_part(s: str) -> str:
    r"""Sanitize source or external_id for use in volume path (no / or \)."""
    if not s:
        return "unknown"
    return re.sub(r"[/\\]", "_", s)[:200]


def _ext_from_url(url: str) -> str:
    path = urlparse(url).path or ""
    m = re.search(r"\.(jpe?g|png|webp|gif)(\?|$)", path, re.I)
    return m.group(1).lower().replace("jpeg", "jpg") if m else "jpg"


def main() -> None:
    args = _parse_args()
    try:
        from databricks.sdk import WorkspaceClient
        import requests
    except ImportError as e:
        print(f"Missing dependency: {e}. Install: pip install databricks-sdk requests", file=sys.stderr)
        sys.exit(1)

    w = WorkspaceClient()
    catalog, schema, warehouse_id = args.catalog, args.schema, args.warehouse_id
    images_volume = args.images_volume
    base_path = f"/Volumes/{catalog}/{schema}/{images_volume}"

    # Query gold for rows with a primary image URL
    sql = f"""
    SELECT listing_key, source, external_id, primary_image_url
    FROM {catalog}.{schema}.gold_kiabi_listings
    WHERE COALESCE(TRIM(primary_image_url), '') != ''
    """
    print(f"Querying gold table for listings with primary_image_url...")
    try:
        resp = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            wait_timeout="30s",
        )
    except Exception as e:
        print(f"SQL execution failed: {e}", file=sys.stderr)
        sys.exit(1)

    status = getattr(resp, "status", None)
    if status and getattr(status, "state", None) == "FAILED":
        err = getattr(status, "error", None) or "Unknown error"
        print(f"Query failed: {err}", file=sys.stderr)
        sys.exit(1)
    if not getattr(resp, "manifest", None) or not getattr(resp, "result", None):
        print("No result from query (pipeline may not have run yet or table empty).", file=sys.stderr)
        return

    manifest = resp.manifest
    result = resp.result
    columns = [c.name for c in manifest.schema.columns] if manifest.schema else []
    rows = result.data_array or []

    # Handle chunked results (fetch additional chunks if any)
    if getattr(manifest, "total_chunk_count", 1) and manifest.total_chunk_count > 1:
        for chunk_index in range(1, manifest.total_chunk_count):
            chunk = w.statement_execution.get_statement_result_chunk_n(
                statement_id=resp.statement_id,
                chunk_index=chunk_index,
            )
            if chunk.data_array:
                rows.extend(chunk.data_array)

    if not rows:
        print("No listings with primary_image_url found.")
        return

    def row_to_dict(row):
        return dict(zip(columns, row, strict=True))

    listings = [row_to_dict(r) for r in rows]
    if args.limit:
        listings = listings[: args.limit]
    print(f"Downloading up to {len(listings)} images to {base_path}/...")

    session = requests.Session()
    session.headers["User-Agent"] = "KiabiSecondHand/1.0 (image download; analytics)"
    downloaded = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(listings):
        listing_key = row.get("listing_key") or ""
        source = _safe_path_part(str(row.get("source") or "unknown"))
        external_id = _safe_path_part(str(row.get("external_id") or ""))
        url = (row.get("primary_image_url") or "").strip()
        if not url:
            continue
        ext = _ext_from_url(url)
        rel_path = f"{source}/{external_id}/0.{ext}"
        volume_path = f"{base_path}/{rel_path}"

        if args.skip_existing:
            try:
                w.files.get_metadata(volume_path)
                skipped += 1
                continue
            except Exception:
                pass

        try:
            r = session.get(url, timeout=15, stream=True)
            r.raise_for_status()
            content = r.content
            # Keep ext from URL so skip_existing and write use the same path
            w.files.upload(volume_path, io.BytesIO(content), overwrite=True)
            downloaded += 1
            if downloaded % 50 == 0:
                print(f"  {downloaded} downloaded...")
        except Exception as e:
            failed += 1
            print(f"  Failed {listing_key}: {e}", file=sys.stderr)

        if args.delay > 0:
            time.sleep(args.delay)

    print(f"Done: {downloaded} downloaded, {skipped} skipped (existing), {failed} failed.")


if __name__ == "__main__":
    main()
