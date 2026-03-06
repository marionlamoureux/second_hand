#!/usr/bin/env python3
"""
Get the number of search results on Leboncoin, Vinted, and eBay for Kiabi's competitors.
Covers kids (Petit Bateau, Okaïdi, etc.) and women's apparel brands.
Output: table to stdout and optional JSON/CSV file.

Usage:
  python scripts/competition_search_counts.py
  python scripts/competition_search_counts.py --output results.json
  python scripts/competition_search_counts.py --no-ebay   # skip eBay (no EBAY_APP_ID)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Competitor brands: kids space and women's apparel (Kiabi competition)
BRANDS_KIDS = [
    "Petit Bateau",
    "Okaïdi",
    "Okaidi",
    "Tape à l'œil",
    "Gémo",
    "Cyrillus",
    "Catimini",
    "Du Pareil au Même",
    "DPAM",
    "Kiabi",  # reference
]
BRANDS_WOMEN = [
    "Comptoir des Cotonniers",
    "Mango",
    "Gémo",
    "Petit Bateau",
    "Okaïdi",
    "Kiabi",  # reference
]


def get_leboncoin_count(brand: str) -> int | None:
    """Return total search result count for brand on Leboncoin (one API call)."""
    try:
        import lbc
    except ImportError:
        return None
    try:
        client = lbc.Client()
        result = client.search(
            text=brand,
            page=1,
            limit=1,
            sort=lbc.Sort.NEWEST,
            ad_type=lbc.AdType.OFFER,
        )
        return int(getattr(result, "total", 0) or 0)
    except Exception as e:
        print(f"Leboncoin {brand!r}: {e}", file=sys.stderr)
        return None


def get_vinted_count(brand: str) -> int | None:
    """Return total search result count for brand on Vinted if available; else approximate from first page."""
    try:
        import requests
    except ImportError:
        return None
    try:
        session = requests.Session()
        session.get("https://www.vinted.fr/", headers={"Accept-Language": "fr-FR,fr;q=0.9"}, timeout=15)
        r = session.get(
            "https://www.vinted.fr/api/v2/catalog/items",
            params={"search_text": brand, "page": 1, "per_page": 20, "order": "newest_first"},
            headers={"Accept": "application/json", "Referer": "https://www.vinted.fr/"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        # Vinted may expose total in catalog or pagination
        total = (
            data.get("total")
            or data.get("pagination", {}).get("total_entries")
            or data.get("catalog", {}).get("total")
        )
        if total is not None:
            return int(total)
        items = data.get("items") or data.get("catalog", {}).get("items") or []
        if items:
            return len(items)  # at least one page; real total not exposed
        return 0
    except Exception as e:
        print(f"Vinted {brand!r}: {e}", file=sys.stderr)
        return None


def get_ebay_count(brand: str, app_id: str | None) -> int | None:
    """Return total search result count for brand on eBay (France)."""
    if not app_id:
        return None
    try:
        from ebaysdk.finding import Connection
    except ImportError:
        return None
    try:
        api = Connection(appid=app_id, config_file=None)
        response = api.execute(
            "findItemsAdvanced",
            {
                "keywords": brand,
                "paginationInput": {"pageNumber": 1, "entriesPerPage": 1},
                "itemFilter": [{"name": "LocatedIn", "value": "FR"}],
            },
        )
        if getattr(response.reply, "ack", None) != "Success":
            return None
        pag = getattr(response.reply, "paginationOutput", None)
        if pag is None:
            return None
        return int(getattr(pag, "totalEntries", 0) or 0)
    except Exception as e:
        print(f"eBay {brand!r}: {e}", file=sys.stderr)
        return None


def run_counts(brands: list[tuple[str, str]], use_ebay: bool, delay: float = 1.0) -> list[dict]:
    """Run count for each (brand, category) on each marketplace. Returns list of {brand, category, marketplace, count}."""
    import time
    app_id = os.environ.get("EBAY_APP_ID", "").strip() if use_ebay else ""
    results = []
    for brand, category in brands:
        print(f"  {brand} ({category})...", file=sys.stderr, flush=True)
        lbc_count = get_leboncoin_count(brand)
        results.append({"brand": brand, "category": category, "marketplace": "leboncoin", "count": lbc_count})
        time.sleep(delay)
        vinted_count = get_vinted_count(brand)
        results.append({"brand": brand, "category": category, "marketplace": "vinted", "count": vinted_count})
        time.sleep(delay)
        if use_ebay and app_id:
            ebay_count = get_ebay_count(brand, app_id)
            results.append({"brand": brand, "category": category, "marketplace": "ebay", "count": ebay_count})
            time.sleep(delay)
    return results


def main():
    ap = argparse.ArgumentParser(description="Get search result counts for Kiabi competitors on Leboncoin, Vinted, eBay")
    ap.add_argument("--output", "-o", default="", help="Write results to JSON file (and CSV if possible)")
    ap.add_argument("--output-dir", default="", help="Write JSONL for pipeline to this dir (e.g. output/competition); creates run_YYYYMMDD_HHMMSS.jsonl")
    ap.add_argument("--no-ebay", action="store_true", help="Skip eBay (no EBAY_APP_ID needed)")
    ap.add_argument("--kids-only", action="store_true", help="Only kids brands")
    ap.add_argument("--women-only", action="store_true", help="Only women's apparel brands")
    args = ap.parse_args()

    brands_with_cat = []
    if not args.women_only:
        brands_with_cat.extend((b, "kids") for b in BRANDS_KIDS)
    if not args.kids_only:
        # Avoid duplicate (brand, category) for brands in both lists
        seen = {(b, "kids") for b in BRANDS_KIDS}
        for b in BRANDS_WOMEN:
            if (b, "women") not in seen and (b, "kids") != (b, "women"):
                brands_with_cat.append((b, "women"))
                seen.add((b, "women"))

    print("Competition search counts (Kiabi competitors)", file=sys.stderr)
    print("Marketplaces: Leboncoin, Vinted" + (", eBay" if not args.no_ebay else " (eBay skipped)"), file=sys.stderr)
    print("", file=sys.stderr)

    results = run_counts(brands_with_cat, use_ebay=not args.no_ebay)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Table: group by brand/category, then marketplaces
    brands_seen = set()
    print("\nBrand (category)     | Leboncoin | Vinted | eBay")
    print("-" * 55)
    for r in results:
        key = (r["brand"], r["category"])
        if key not in brands_seen:
            brands_seen.add(key)
            row_results = {x["marketplace"]: x["count"] for x in results if x["brand"] == r["brand"] and x["category"] == r["category"]}
            lbc = row_results.get("leboncoin")
            v = row_results.get("vinted")
            ebay = row_results.get("ebay")
            lbc_s = f"{lbc:,}" if lbc is not None else "—"
            v_s = f"{v:,}" if v is not None else "—"
            ebay_s = f"{ebay:,}" if ebay is not None else "—"
            label = f"{r['brand']} ({r['category']})"
            print(f"{label:<20} | {lbc_s:>9} | {v_s:>6} | {ebay_s}")

    out = {"timestamp": ts, "brands_kids": BRANDS_KIDS, "brands_women": BRANDS_WOMEN, "results": results}
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        print(f"\nWrote {out_path}", file=sys.stderr)
        try:
            import csv
            csv_path = out_path.with_suffix(".csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["brand", "category", "marketplace", "count"])
                w.writeheader()
                w.writerows(results)
            print(f"Wrote {csv_path}", file=sys.stderr)
        except Exception:
            pass

    # JSONL for pipeline: one line per row (timestamp, brand, category, marketplace, count)
    if args.output_dir:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_name = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        jsonl_path = out_dir / f"run_{run_name}.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for r in results:
                row = {"timestamp": ts, "brand": r["brand"], "category": r["category"], "marketplace": r["marketplace"], "count": r["count"]}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"Wrote pipeline JSONL {jsonl_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
