"""
Facebook Marketplace scraper placeholder for a given brand (e.g. Kiabi).

Facebook's Terms of Service prohibit automated scraping of Marketplace. For production,
use an official integration (e.g. Facebook Commerce API, Meta Business Suite) or a
compliant third-party data provider. This script writes an empty or manually-provided
JSONL file so the pipeline schema stays consistent; replace with your approved source.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

def _parse_args():
    p = argparse.ArgumentParser(description="Facebook Marketplace placeholder for brand")
    p.add_argument("--brand", default="kiabi", help="Brand name")
    p.add_argument("--output-base", required=True, help="Base path for output")
    p.add_argument("--input-file", default="", help="Optional: path to JSONL file from approved source")
    p.add_argument("--demo", action="store_true", help="Use synthetic data for pipeline testing")
    return p.parse_args()


# Paris, Lyon, Montpellier, Brest, Caen
_DEMO_COORDS = [(48.8566, 2.3522), (45.7640, 4.8357), (43.6108, 3.8767), (48.3905, -4.4860), (49.1829, -0.3707)]


def _demo_rows(source: str, brand: str, count: int = 5) -> list[dict]:
    """Synthetic listings for pipeline testing."""
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    templates = [
        {"title": f"Lot vêtements {brand} 2-4 ans", "price": 20.0, "location": "Paris"},
        {"title": f"Chaussures {brand} 28", "price": 14.0, "location": "Lyon"},
        {"title": f"Body {brand} nouveau-né", "price": 6.0, "location": "Montpellier"},
        {"title": f"Pantalon {brand} fille 12 ans", "price": 8.0, "location": "Brest"},
        {"title": f"Gilet {brand} homme M", "price": 12.0, "location": "Caen"},
    ][:count]
    return [
        {
            "source": source,
            "external_id": f"{source}_demo_{i}",
            "title": t["title"],
            "description": f"Article {brand} (démo).",
            "price": t["price"],
            "url": f"https://example.com/{source}/demo/{i}",
            "location": t["location"],
            "latitude": _DEMO_COORDS[i - 1][0],
            "longitude": _DEMO_COORDS[i - 1][1],
            "published_at": ts,
            "scraped_at": ts,
            "primary_image_url": "",
        }
        for i, t in enumerate(templates, 1)
    ]


def _normalize(item: dict) -> dict:
    loc_text = (item.get("location") or "").strip() if isinstance(item.get("location"), str) else ""
    lat, lng = None, None
    if loc_text:
        try:
            from . import geo
            lat, lng = geo.geocode(loc_text)
        except Exception:
            pass
    return {
        "source": "facebook",
        "external_id": str(item.get("id", item.get("listing_id", ""))),
        "title": item.get("title", ""),
        "description": item.get("description", ""),
        "price": item.get("price", {}).get("amount") if isinstance(item.get("price"), dict) else item.get("price"),
        "url": item.get("url", ""),
        "location": loc_text,
        "latitude": lat,
        "longitude": lng,
        "published_at": item.get("created_time", ""),
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "primary_image_url": item.get("primary_image_url", item.get("image_url", "") or ""),
    }


def load_facebook_items(input_file: str) -> list[dict]:
    """Load from an optional JSONL file (e.g. from approved API export)."""
    if not input_file:
        return []
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return [_normalize(json.loads(line)) for line in f if line.strip()]
    except FileNotFoundError:
        return []
    except Exception:
        return []


def write_output(rows: list[dict], path: str) -> None:
    """Write JSONL to path. Uses dbutils in Databricks (serverless-safe); else local file."""
    try:
        dbutils  # noqa: F821
    except NameError:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        import os
        os.makedirs(path, exist_ok=True)
        with open(f"{path.rstrip('/')}/run_{ts}.jsonl", "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_file = f"{path.rstrip('/')}/run_{ts}.jsonl"
    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    dbutils.fs.put(out_file, content, overwrite=True)  # noqa: F821


def main():
    args = _parse_args()
    if args.demo:
        rows = _demo_rows("facebook", args.brand)
        print(f"Demo mode: {len(rows)} synthetic items for Facebook brand={args.brand}")
    else:
        rows = load_facebook_items(args.input_file)
        print(f"Facebook Marketplace: {len(rows)} items (use --input-file or approved API for real data)")
    write_output(rows, args.output_base)


if __name__ == "__main__":
    main()
