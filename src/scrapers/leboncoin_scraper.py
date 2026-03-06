"""
Scrape Leboncoin for a given brand (e.g. Kiabi).
Uses the unofficial 'lbc' client. Writes one JSONL file per run to the landing volume.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone

PER_PAGE = 35  # lbc API limit per page
# Leboncoin API returns at most 100 pages per search (~3,500 items). To get ~50k we run multiple queries (categories) and dedupe.
LBC_MAX_PAGES_PER_QUERY = 100

def _parse_args():
    p = argparse.ArgumentParser(description="Scrape Leboncoin for brand")
    p.add_argument("--brand", default="kiabi", help="Brand name to search")
    p.add_argument(
        "--output-base",
        required=True,
        help="Base path for output (e.g. /Volumes/catalog/schema/volume/leboncoin)",
    )
    p.add_argument("--max-pages", type=int, default=None, help="Max pages per query (default: min of --max-items/35 and 100; API cap is 100)")
    p.add_argument("--max-items", type=int, default=10000, help="Target items per query (default 10000); capped at 100 pages = ~3500 per query")
    p.add_argument("--delay", type=float, default=2.0, help="Seconds between pages (reduces Datadome blocking)")
    p.add_argument("--multi-query", action="store_true", help="Run multiple category searches and merge (up to ~50k unique items; each query capped at 100 pages)")
    p.add_argument("--demo", action="store_true", help="Use synthetic data (when blocked by Datadome/cloud IP)")
    return p.parse_args()


# Paris, Lyon, Marseille, Bordeaux, Toulouse approximate centroids
_DEMO_COORDS = [(48.8566, 2.3522), (45.7640, 4.8357), (43.2965, 5.3698), (44.8378, -0.5792), (43.6047, 1.4442)]


def _demo_rows(source: str, brand: str, count: int = 5) -> list[dict]:
    """Synthetic listings when live scraping is blocked (Datadome/403)."""
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


def _id_from_url(url: str) -> str:
    """Extract ad id from Leboncoin URL (e.g. .../ad/vetements/3155730669 -> 3155730669)."""
    if not url:
        return ""
    m = re.search(r"/([0-9]+)(?:\?|$)", url.strip())
    return m.group(1) if m else ""


def _lat_lng_from_leboncoin_loc(loc) -> tuple[float | None, float | None]:
    """Get (lat, lng) from lbc Location object or its string repr; fallback geocode from city/zipcode."""
    from . import geo
    lat, lng = None, None
    if loc is not None and not isinstance(loc, (str, int, float, type(None))):
        lat = getattr(loc, "lat", None)
        lng = getattr(loc, "lng", None)
        if lat is not None and lng is not None:
            try:
                return (float(lat), float(lng))
            except (TypeError, ValueError):
                pass
    loc_str = str(loc) if loc is not None else ""
    lat, lng = geo.parse_lat_lng_from_leboncoin_location_string(loc_str)
    if lat is not None and lng is not None:
        return (lat, lng)
    # Geocode from city_label or zipcode
    if loc is not None and not isinstance(loc, (str, int, float, type(None))):
        city_label = getattr(loc, "city_label", None) or ""
        zipcode = getattr(loc, "zipcode", None) or ""
        city = getattr(loc, "city", None) or ""
        query = (zipcode or city_label or city or "").strip()
        if query:
            lat, lng = geo.geocode(query)
    if lat is None and loc_str:
        lat, lng = geo.geocode(loc_str)
    return (lat, lng)


def _normalize(ad) -> dict:
    """Convert lbc Ad object to common schema. lbc returns objects, not dicts — use getattr only. Coerce to JSON-serializable types."""
    loc = getattr(ad, "location", None)
    latitude, longitude = _lat_lng_from_leboncoin_loc(loc)
    loc_str = str(loc) if loc is not None else ""
    images = getattr(ad, "images", None)
    primary_image_url = ""
    if images and isinstance(images, (list, tuple)) and len(images) > 0:
        primary_image_url = str(images[0]) if images[0] else ""
    url = str(getattr(ad, "url", None) or "")
    external_id = str(getattr(ad, "list_id", "") or "").strip()
    if not external_id and url:
        external_id = _id_from_url(url)
    return {
        "source": "leboncoin",
        "external_id": external_id,
        "title": str(getattr(ad, "subject", None) or ""),
        "description": str(getattr(ad, "body", None) or ""),
        "price": getattr(ad, "price", None),
        "url": url,
        "location": loc_str,
        "latitude": latitude,
        "longitude": longitude,
        "published_at": str(getattr(ad, "index_date", None) or ""),
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "primary_image_url": primary_image_url,
    }


def _scrape_one_query(client, brand: str, max_pages: int, delay: float, category=None) -> list[dict]:
    """Run a single search (optional category). Returns list of normalized ads."""
    import lbc
    import time
    all_ads = []
    for page in range(1, max_pages + 1):
        if page > 1:
            time.sleep(delay)
        try:
            kwargs = dict(
                text=brand,
                page=page,
                limit=PER_PAGE,
                sort=lbc.Sort.NEWEST,
                ad_type=lbc.AdType.OFFER,
            )
            if category is not None:
                kwargs["category"] = category
            result = client.search(**kwargs)
            if not result.ads:
                break
            for ad in result.ads:
                try:
                    all_ads.append(_normalize(ad))
                except Exception as e:
                    print(f"Skip ad: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Page {page} (category={category}) error: {e}", file=sys.stderr)
            break
    return all_ads


def scrape_leboncoin(brand: str, max_pages: int, delay: float, category=None) -> list[dict]:
    try:
        import lbc
    except ImportError:
        print("Install with: pip install lbc", file=sys.stderr)
        raise
    client = lbc.Client()
    return _scrape_one_query(client, brand, max_pages, delay, category=category)


def scrape_leboncoin_multi_query(brand: str, delay: float) -> list[dict]:
    """Run several category-based searches (each capped at 100 pages), merge and dedupe by ad id. Gets toward ~50k unique items."""
    try:
        import lbc
        import time
    except ImportError:
        print("Install with: pip install lbc", file=sys.stderr)
        raise
    # Categories relevant to Kiabi (clothing, shoes, kids, baby). Each query can return up to 100*35 = 3500.
    CATEGORIES = [
        None,  # no filter first
        lbc.Category.MODE_VETEMENTS,
        lbc.Category.MODE_CHAUSSURES,
        lbc.Category.FAMILLE_VETEMENTS_BEBE,
        lbc.Category.FAMILLE_VETEMENTS_ENFANTS,
        lbc.Category.MODE_ACCESSOIRES_ET_BAGAGERIE,
        lbc.Category.FAMILLE_VETEMENTS_MATERNITE,
        lbc.Category.FAMILLE_CHAUSSURES_ENFANTS,
        lbc.Category.MODE,  # broad fashion
    ]
    seen_ids = set()
    merged = []
    client = lbc.Client()
    pages_per_query = LBC_MAX_PAGES_PER_QUERY
    for i, cat in enumerate(CATEGORIES):
        label = cat.name if cat is not None else "all"
        print(f"Query {i + 1}/{len(CATEGORIES)}: category={label} (max {pages_per_query} pages)", file=sys.stderr)
        rows = _scrape_one_query(client, brand, pages_per_query, delay, category=cat)
        new = 0
        for r in rows:
            aid = (r.get("external_id") or "").strip() or _id_from_url(r.get("url") or "")
            if aid and aid not in seen_ids:
                seen_ids.add(aid)
                merged.append(r)
                new += 1
        print(f"  got {len(rows)} rows, {new} new unique (total unique {len(merged)})", file=sys.stderr)
        if i < len(CATEGORIES) - 1:
            time.sleep(delay * 2)  # extra pause between queries to reduce blocking
    return merged


def write_output(rows: list[dict], path: str) -> None:
    """Write JSONL to path. Uses dbutils in Databricks (serverless-safe); else local file."""
    try:
        dbutils  # noqa: F821
    except NameError:
        # Fallback when not in Databricks: write locally
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_file = f"{path.rstrip('/')}/run_{ts}.jsonl"
        import os
        os.makedirs(path, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
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
        rows = _demo_rows("leboncoin", args.brand)
        print(f"Demo mode: {len(rows)} synthetic items for Leboncoin brand={args.brand}")
    elif args.multi_query:
        rows = scrape_leboncoin_multi_query(args.brand, args.delay)
        print(f"Scraped {len(rows)} unique items from Leboncoin (multi-query) for brand={args.brand}")
    else:
        max_pages = args.max_pages
        if max_pages is None:
            max_pages = max(1, (args.max_items + PER_PAGE - 1) // PER_PAGE)
            max_pages = min(max_pages, LBC_MAX_PAGES_PER_QUERY)
            print(f"Target ~{args.max_items} items -> {max_pages} pages (API cap {LBC_MAX_PAGES_PER_QUERY} = ~{LBC_MAX_PAGES_PER_QUERY * PER_PAGE} items)", file=sys.stderr)
        else:
            max_pages = min(max_pages, LBC_MAX_PAGES_PER_QUERY)
        rows = scrape_leboncoin(args.brand, max_pages, args.delay)
        print(f"Scraped {len(rows)} items from Leboncoin for brand={args.brand}")
    write_output(rows, args.output_base)


if __name__ == "__main__":
    main()
