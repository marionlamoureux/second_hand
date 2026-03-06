"""
Scrape eBay for a given brand (e.g. Kiabi).
Uses eBay Finding API (ebaysdk). Set EBAY_APP_ID or pass --app-id. Writes JSONL to landing volume.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

PER_PAGE = 100
MAX_PAGES_DEFAULT = 50

def _parse_args():
    p = argparse.ArgumentParser(description="Scrape eBay for brand")
    p.add_argument("--brand", default="kiabi", help="Brand name to search")
    p.add_argument("--output-base", required=True, help="Base path for output")
    p.add_argument("--max-pages", type=int, default=None, help="Max pages (default from --max-items)")
    p.add_argument("--max-items", type=int, default=5000, help="Target items (default 5000)")
    p.add_argument("--app-id", default=os.environ.get("EBAY_APP_ID", ""), help="eBay App ID (or EBAY_APP_ID env)")
    p.add_argument("--delay", type=float, default=0.5, help="Seconds between pages")
    p.add_argument("--demo", action="store_true", help="Use synthetic data when API not configured")
    return p.parse_args()


_DEMO_COORDS = [(48.8566, 2.3522), (45.7640, 4.8357), (43.2965, 5.3698), (44.8378, -0.5792), (43.6047, 1.4442)]


def _demo_rows(source: str, brand: str, count: int = 5) -> list[dict]:
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    templates = [
        {"title": f"Veste {brand} enfant", "price": 12.0, "location": "Paris"},
        {"title": f"Jean {brand} taille 8 ans", "price": 8.0, "location": "Lyon"},
        {"title": f"Robe {brand} fille 6 ans", "price": 15.0, "location": "Marseille"},
        {"title": f"Pull {brand} garçon", "price": 10.0, "location": "Bordeaux"},
        {"title": f"Manteau {brand} bébé", "price": 18.0, "location": "Toulouse"},
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


def _normalize_item(item) -> dict:
    """Map eBay Finding API item to common schema. Item can be object or dict."""
    def _get(o, key, default=None):
        if o is None:
            return default
        if isinstance(o, dict):
            return o.get(key, default)
        return getattr(o, key, default)

    item_id = str(_get(item, "itemId", "") or "")
    title = str(_get(item, "title") or "")
    url = str(_get(item, "viewItemURL") or _get(item, "itemWebUrl") or f"https://www.ebay.fr/itm/{item_id}")
    location = str(_get(item, "location") or "")
    # Price: sellingStatus.convertedCurrentPrice or sellingStatus.currentPrice
    selling = _get(item, "sellingStatus") or {}
    price_val = _get(selling, "convertedCurrentPrice") or _get(selling, "currentPrice")
    if price_val is not None:
        if isinstance(price_val, (dict, object)):
            price = float(_get(price_val, "value") or _get(price_val, "__value__") or 0)
        else:
            price = float(price_val)
    else:
        price = None
    # Image
    gallery = _get(item, "galleryURL") or _get(item, "image", {}).get("imageUrl") or ""
    if isinstance(gallery, dict):
        gallery = gallery.get("url", gallery.get("imageUrl", "")) or ""
    primary_image_url = str(gallery).strip() if gallery else ""
    # End time for published_at
    listing_info = _get(item, "listingInfo") or {}
    end_time = _get(listing_info, "endTime") or ""
    if end_time:
        published_at = str(end_time)
    else:
        published_at = ""

    lat, lng = None, None
    if location:
        try:
            from . import geo
            lat, lng = geo.geocode(location)
        except Exception:
            pass

    return {
        "source": "ebay",
        "external_id": item_id,
        "title": title,
        "description": str(_get(item, "condition", {}).get("conditionDisplayName") or ""),
        "price": price,
        "url": url,
        "location": location,
        "latitude": lat,
        "longitude": lng,
        "published_at": published_at,
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "primary_image_url": primary_image_url,
    }


def scrape_ebay(brand: str, max_pages: int, app_id: str, delay: float) -> list[dict]:
    try:
        from ebaysdk.finding import Connection
    except ImportError:
        print("Install with: pip install ebaysdk-python", file=sys.stderr)
        raise

    api = Connection(appid=app_id, config_file=None)
    all_items = []
    page = 1
    while page <= max_pages:
        try:
            response = api.execute("findItemsAdvanced", {
                "keywords": brand,
                "paginationInput": {"pageNumber": page, "entriesPerPage": min(PER_PAGE, 100)},
                "itemFilter": [{"name": "LocatedIn", "value": "FR"}],
            })
            if getattr(response.reply, "ack", None) != "Success":
                break
            search_result = getattr(response.reply, "searchResult", None)
            if not search_result:
                break
            items = getattr(search_result, "item", None)
            if items is None:
                break
            if not isinstance(items, list):
                items = [items] if items is not None else []
            for item in items:
                try:
                    all_items.append(_normalize_item(item))
                except Exception as e:
                    print(f"Skip item: {e}", file=sys.stderr)
            if len(items) < PER_PAGE:
                break
            page += 1
            time.sleep(delay)
        except Exception as e:
            print(f"Page {page} error: {e}", file=sys.stderr)
            break
    return all_items


def write_output(rows: list[dict], path: str) -> None:
    try:
        dbutils  # noqa: F821
    except NameError:
        import os
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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
    if args.demo or not args.app_id:
        rows = _demo_rows("ebay", args.brand)
        print(f"Demo mode: {len(rows)} synthetic items for eBay brand={args.brand}", file=sys.stderr)
    else:
        max_pages = args.max_pages or max(1, (args.max_items + PER_PAGE - 1) // PER_PAGE)
        print(f"Target ~{args.max_items} items -> {max_pages} pages", file=sys.stderr)
        rows = scrape_ebay(args.brand, max_pages, args.app_id, args.delay)
        print(f"Scraped {len(rows)} items from eBay for brand={args.brand}")
    write_output(rows, args.output_base)


if __name__ == "__main__":
    main()
