"""
Scrape Label Emmaüs (label-emmaus.co) for a given brand (e.g. Kiabi).
Uses catalogue search ?q=brand and parses product cards. Writes JSONL to landing volume.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

BASE_URL = "https://www.label-emmaus.co"
CATALOGUE_URL = f"{BASE_URL}/fr/catalogue"

def _parse_args():
    p = argparse.ArgumentParser(description="Scrape Label Emmaüs for brand")
    p.add_argument("--brand", default="kiabi", help="Brand name to search")
    p.add_argument("--output-base", required=True, help="Base path for output")
    p.add_argument("--max-pages", type=int, default=20, help="Max catalogue pages (default 20)")
    p.add_argument("--delay", type=float, default=1.5, help="Seconds between requests")
    p.add_argument("--demo", action="store_true", help="Use synthetic data")
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


def _id_from_url(href: str) -> str:
    """Extract product id from URL like /fr/haut-ml-kiabi-taille-l-102833181/ -> 102833181."""
    if not href:
        return ""
    m = re.search(r"-(\d+)/?$", href.strip())
    return m.group(1) if m else ""


def _parse_price(text: str) -> float | None:
    """Parse price from '7,50 €' or '5,60 € 8,00 €' (take first)."""
    if not text:
        return None
    m = re.search(r"([0-9]+[,\.][0-9]+)\s*€", text.replace(" ", ""))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def _parse_catalogue_html(html: str, base: str) -> list[dict]:
    """Extract product entries from catalogue HTML. Returns list of {url, title, price, external_id}."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    items = []
    # Links to product pages: /fr/slug-numeric_id/
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/fr/" not in href or "/nos-boutiques/" in href or "/catalogue/" in href and href.count("/") <= 4:
            continue
        id_ = _id_from_url(href)
        if not id_:
            continue
        full_url = urljoin(base, href)
        title = (a.get_text(strip=True) or "").strip()
        if not title or len(title) > 500:
            continue
        # Price often in same card; look for sibling or parent with price
        parent = a.parent
        price = None
        for _ in range(5):
            if parent is None:
                break
            text = parent.get_text() or ""
            price = _parse_price(text)
            if price is not None:
                break
            parent = getattr(parent, "parent", None)
        items.append({"url": full_url, "title": title, "price": price, "external_id": id_})
    # Dedupe by external_id
    seen = set()
    unique = []
    for x in items:
        if x["external_id"] in seen:
            continue
        seen.add(x["external_id"])
        unique.append(x)
    return unique


def scrape_label_emmaus(brand: str, max_pages: int, delay: float) -> list[dict]:
    try:
        import requests
    except ImportError:
        print("Install with: pip install requests beautifulsoup4", file=sys.stderr)
        raise

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) KiabiSecondHand/1.0"
    session.headers["Accept-Language"] = "fr-FR,fr;q=0.9"
    all_items = []
    page = 1
    while page <= max_pages:
        try:
            url = f"{CATALOGUE_URL}?q={brand}"
            if page > 1:
                url += f"&page={page}"
            r = session.get(url, timeout=30)
            r.raise_for_status()
            raw = _parse_catalogue_html(r.text, BASE_URL)
            if not raw:
                break
            for row in raw:
                all_items.append({
                    "source": "label_emmaus",
                    "external_id": row["external_id"],
                    "title": row["title"],
                    "description": "",
                    "price": row["price"],
                    "url": row["url"],
                    "location": "France",
                    "latitude": None,
                    "longitude": None,
                    "published_at": "",
                    "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "primary_image_url": "",
                })
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
    if args.demo:
        rows = _demo_rows("label_emmaus", args.brand)
        print(f"Demo mode: {len(rows)} synthetic items for Label Emmaüs brand={args.brand}")
    else:
        rows = scrape_label_emmaus(args.brand, args.max_pages, args.delay)
        print(f"Scraped {len(rows)} items from Label Emmaüs for brand={args.brand}")
    write_output(rows, args.output_base)


if __name__ == "__main__":
    main()
