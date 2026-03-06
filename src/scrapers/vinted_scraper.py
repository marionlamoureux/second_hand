"""
Scrape Vinted for a given brand (e.g. Kiabi).
Uses Vinted's internal catalog API with requests. Writes one JSONL file per run.
Respects rate limits; use reasonable delays in production.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

PER_PAGE = 20
MAX_PAGES_CAP = 48  # Vinted ~960 items max

def _parse_args():
    p = argparse.ArgumentParser(description="Scrape Vinted for brand")
    p.add_argument("--brand", default="kiabi", help="Brand name to search")
    p.add_argument("--output-base", required=True, help="Base path for output")
    p.add_argument("--max-pages", type=int, default=None, help="Max pages (default: from --max-items, cap 48)")
    p.add_argument("--max-items", type=int, default=960, help="Target items (default 960 = Vinted cap)")
    p.add_argument("--delay", type=float, default=1.0, help="Seconds between requests")
    p.add_argument("--demo", action="store_true", help="Use synthetic data (when 403 from cloud IP)")
    p.add_argument("--debug-first-item", action="store_true", help="Print first raw API item (keys + price-related) to stderr for debugging")
    return p.parse_args()


def _demo_rows(source: str, brand: str, count: int = 5) -> list[dict]:
    """Synthetic listings when live scraping is blocked (403)."""
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    templates = [
        {"title": f"Sweat {brand} femme S", "price": 9.0, "location": "Lille"},
        {"title": f"Short {brand} garçon 10 ans", "price": 5.0, "location": "Nantes"},
        {"title": f"T-shirt {brand} unisexe", "price": 4.0, "location": "Strasbourg"},
        {"title": f"Pyjama {brand} 2 ans", "price": 7.0, "location": "Nice"},
        {"title": f"Combinaison {brand} bébé", "price": 11.0, "location": "Rennes"},
    ][:count]
    # Demo coords: Lille, Nantes, Strasbourg, Nice, Rennes
    _demo_coords = [(50.6292, 3.0573), (47.2184, -1.5536), (48.5734, 7.7521), (43.7102, 7.2620), (48.1173, -1.6778)]
    return [
        {
            "source": source,
            "external_id": f"{source}_demo_{i}",
            "title": t["title"],
            "description": f"Article {brand} (démo).",
            "price": t["price"],
            "url": f"https://example.com/{source}/demo/{i}",
            "location": t["location"],
            "latitude": _demo_coords[i - 1][0],
            "longitude": _demo_coords[i - 1][1],
            "published_at": ts,
            "scraped_at": ts,
            "primary_image_url": "",
        }
        for i, t in enumerate(templates, 1)
    ]


def _price_from_item(item: dict):
    """Extract numeric price from Vinted item; API shape varies (price_numeric, price.numeric.amount, etc.)."""
    # Direct numeric or string
    p = item.get("price")
    if isinstance(p, (int, float)):
        return float(p)
    if isinstance(p, str):
        try:
            return float(p.replace(",", ".").strip())
        except (TypeError, ValueError):
            pass
    if isinstance(p, dict):
        # price.numeric.amount or price.amount
        num = p.get("numeric") if isinstance(p.get("numeric"), dict) else p
        if isinstance(num, dict):
            amt = num.get("amount") or num.get("float")
            if amt is not None:
                return float(amt)
        amt = p.get("amount") or p.get("current_value")
        if amt is not None:
            return float(amt)
    # Some catalog responses use price_numeric (string or number)
    pn = item.get("price_numeric")
    if pn is not None:
        try:
            return float(pn)
        except (TypeError, ValueError):
            pass
    # price_string sometimes present
    ps = item.get("price_string")
    if isinstance(ps, str):
        try:
            return float("".join(c for c in ps.replace(",", ".") if c in "0123456789."))
        except (TypeError, ValueError):
            pass
    return None


def _first_photo_url(item: dict) -> str:
    """Extract first photo URL from Vinted item (structure varies)."""
    photo = item.get("photo")
    if isinstance(photo, dict):
        for key in ("url", "full_size_url", "high_resolution", "original_url"):
            val = photo.get(key)
            if isinstance(val, dict) and "url" in val:
                return str(val.get("url", ""))
            if isinstance(val, str) and val.startswith("http"):
                return val
    photos = item.get("photos")
    if isinstance(photos, list) and photos:
        first = photos[0]
        if isinstance(first, dict):
            return str(first.get("url", first.get("full_size_url", "")))
        if isinstance(first, str) and first.startswith("http"):
            return first
    return ""


def _location_text_and_coords(item: dict) -> tuple[str, float | None, float | None]:
    """Get location string and (lat, lng) from Vinted item. Geocode if we have city/country."""
    loc_text = (item.get("city") or "").strip() if isinstance(item.get("city"), str) else ""
    user = item.get("user") if isinstance(item.get("user"), dict) else {}
    if not loc_text:
        loc_text = (user.get("city") or user.get("country") or "").strip() if isinstance(user.get("city"), str) or isinstance(user.get("country"), str) else str(user.get("city") or user.get("country") or "").strip()
    country = (item.get("country") or user.get("country") or "France")
    if not isinstance(country, str):
        country = "France"
    lat, lng = None, None
    query = f"{loc_text}, {country}".strip(" ,") if (loc_text or country) else ""
    if query:
        try:
            from . import geo
            lat, lng = geo.geocode(query)
        except Exception:
            pass
    return (loc_text or "", lat, lng)


def _normalize(item: dict) -> dict:
    """Map Vinted API item to common schema."""
    loc_text, latitude, longitude = _location_text_and_coords(item)
    return {
        "source": "vinted",
        "external_id": str(item.get("id", "")),
        "title": item.get("title", ""),
        "description": item.get("description", ""),
        "price": _price_from_item(item),
        "url": item.get("url", "") or f"https://www.vinted.fr/catalog/{item.get('id', '')}",
        "location": loc_text,
        "latitude": latitude,
        "longitude": longitude,
        "published_at": item.get("photo", {}).get("high_resolution", {}).get("timestamp") if isinstance(item.get("photo"), dict) else "",
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "primary_image_url": _first_photo_url(item),
    }


def scrape_vinted(brand: str, max_pages: int, delay: float, *, debug_first_item: bool = False) -> list[dict]:
    # Use curl_cffi to mimic Chrome TLS fingerprint; session + homepage visit to get cookies.
    try:
        from curl_cffi import requests as req_lib
        use_curl_cffi = True
    except ImportError:
        import requests as req_lib
        use_curl_cffi = False

    base_url = "https://www.vinted.fr/api/v2/catalog/items"
    all_items = []
    page = 1
    per_page = 20

    headers = {
        "Accept": "application/json",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Referer": "https://www.vinted.fr/",
        "Origin": "https://www.vinted.fr",
    }

    # Session: hit homepage first so we get cookies, then call API with same session
    if use_curl_cffi:
        session = req_lib.Session(impersonate="chrome120")
    else:
        session = req_lib.Session()

    try:
        session.get("https://www.vinted.fr/", headers={"Accept-Language": "fr-FR,fr;q=0.9"}, timeout=15)
        time.sleep(0.5)
    except Exception as e:
        print(f"Session init (homepage): {e}", file=sys.stderr)

    while page <= max_pages:
        try:
            params = {
                "search_text": brand,
                "page": page,
                "per_page": per_page,
                "order": "newest_first",
            }
            r = session.get(base_url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            items = data.get("items") or data.get("catalog", {}).get("items") or []
            if not items:
                break
            if debug_first_item and page == 1 and items:
                import json as _json
                first = items[0]
                debug = {k: v for k, v in first.items() if "price" in k.lower() or k in ("id", "title")}
                print(f"Debug first item (price-related keys): {_json.dumps(debug, indent=2, default=str)}", file=sys.stderr)
            for it in items:
                try:
                    all_items.append(_normalize(it))
                except Exception as e:
                    print(f"Skip item: {e}", file=sys.stderr)
            page += 1
            time.sleep(delay)
        except Exception as e:
            print(f"Request error page {page}: {e}", file=sys.stderr)
            break

    return all_items


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
    max_pages = args.max_pages
    if max_pages is None:
        max_pages = min(MAX_PAGES_CAP, max(1, (args.max_items + PER_PAGE - 1) // PER_PAGE))
        print(f"Target ~{args.max_items} items -> {max_pages} pages (up to {max_pages * PER_PAGE} items)", file=sys.stderr)
    if args.demo:
        rows = _demo_rows("vinted", args.brand)
        print(f"Demo mode: {len(rows)} synthetic items for Vinted brand={args.brand}")
    else:
        rows = scrape_vinted(args.brand, max_pages, args.delay, debug_first_item=args.debug_first_item)
        print(f"Scraped {len(rows)} items from Vinted for brand={args.brand}")
    write_output(rows, args.output_base)


if __name__ == "__main__":
    main()
