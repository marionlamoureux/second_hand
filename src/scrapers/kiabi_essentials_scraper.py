"""
Scrape Kiabi "Nos essentiels" product catalog (femme, homme, fille, garçon).
Parses __NEXT_DATA__ JSON embedded in server-rendered HTML.
Writes one JSONL file per run to the landing volume under /essentials/.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

CATEGORIES = [
    ("femme",  "https://www.kiabi.com/nos-essentiels-femme_461199",  "Femme"),
    ("homme",  "https://www.kiabi.com/nos-essentiels-homme_461200",  "Homme"),
    ("fille",  "https://www.kiabi.com/nos-essentiels-fille_461201",  "Fille"),
    ("garcon", "https://www.kiabi.com/nos-essentiels-garcon_461202", "Garçon"),
]

_STATIC_IMAGE_BASE = "https://static.kiabi.com"
_SITE_BASE = "https://www.kiabi.com"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


def _parse_args():
    p = argparse.ArgumentParser(description="Scrape Kiabi essentials catalog")
    p.add_argument(
        "--output-base",
        required=True,
        help="Base path for output (e.g. /Volumes/catalog/schema/volume/essentials)",
    )
    p.add_argument("--delay", type=float, default=2.0, help="Seconds between requests")
    p.add_argument("--demo", action="store_true", help="Use synthetic data (avoids Datadome)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Demo mode: synthetic data so the pipeline can run without live scraping
# ---------------------------------------------------------------------------

def _demo_rows() -> list[dict]:
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows = []
    templates = [
        ("T-shirt oversize à col rond", "Femme", "femme", "T-shirt manches courtes femme", 8.0,  "Blanc", "BRY18_10", "https://static.kiabi.com/images/t-shirt-oversize-a-col-rond-blanc-bry18_10_hd1.jpg", 998),
        ("Jean Wide leg taille haute",  "Femme", "femme", "Pantalon taille haute",          18.0, "Marron","DMK67_11", "https://static.kiabi.com/images/jean-wide-leg-taille-haute-marron-dmk67_11_hd1.jpg",  1493),
        ("Sweat à capuche uni",          "Femme", "femme", "Sweat à capuche femme, hoodie",  13.0, "Gris",  "CNN25_26", "https://static.kiabi.com/images/sweat-a-capuche-uni-gris-cnn25_26_hd1.jpg",            1454),
        ("Jogging de sport taille std",  "Femme", "femme", "Jogging Femme",                  13.0, "Noir",  "DMA23_14", "https://static.kiabi.com/images/jogging-de-sport-taille-standard-noir-dma23_14_hd1.jpg",1417),
        ("T-shirt col V homme",          "Homme", "homme", "T-shirt manches courtes homme",   8.0,  "Blanc", "ZZZ01_10", "https://static.kiabi.com/images/placeholder.jpg", 320),
        ("Jean slim homme",              "Homme", "homme", "Jean homme",                      18.0, "Bleu",  "ZZZ02_01", "https://static.kiabi.com/images/placeholder.jpg", 210),
        ("Robe fille à fleurs",          "Fille", "fille", "Robe fille",                      12.0, "Rose",  "ZZZ03_05", "https://static.kiabi.com/images/placeholder.jpg", 150),
        ("Jean slim garçon",           "Garçon","garcon","Jean garçon",                      14.0, "Bleu",  "ZZZ04_01", "https://static.kiabi.com/images/placeholder.jpg", 180),
    ]
    for i, (title, universe, cat_key, category, price, color, code, img, reviews) in enumerate(templates):
        rows.append({
            "product_code": code,
            "product_uid": f"demo_{i}",
            "title": title,
            "universe": universe,
            "universe_key": cat_key,
            "category": category,
            "color": color,
            "price": price,
            "list_price": price,
            "currency": "EUR",
            "product_url": f"https://www.kiabi.com/demo-product-{i}",
            "primary_image_url": img,
            "rating": "4.5",
            "total_reviews": reviews,
            "scraped_at": ts,
        })
    return rows


# ---------------------------------------------------------------------------
# Live scraping
# ---------------------------------------------------------------------------

def _fetch_next_data(url: str) -> dict | None:
    """Fetch a Kiabi page and extract __NEXT_DATA__ JSON from the HTML."""
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        print("Install with: pip install requests beautifulsoup4", file=sys.stderr)
        raise

    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code} for {url}", file=sys.stderr)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script_tag or not script_tag.string:
        print(f"No __NEXT_DATA__ found at {url} (Datadome block?)", file=sys.stderr)
        return None

    return json.loads(script_tag.string)


def _normalize_item(item: dict, universe: str, universe_key: str, scraped_at: str) -> dict:
    display = item.get("display") or {}
    price_info = display.get("price") or {}
    images = display.get("images") or {}
    image_paths = images.get("productImagesSource") or []
    primary_image = (_STATIC_IMAGE_BASE + image_paths[0]) if image_paths else None

    seo = item.get("seo") or {}
    breadcrumbs = seo.get("breadcrumbs") or []
    # Second-to-last breadcrumb is the category (last is the product name itself)
    category = breadcrumbs[-2]["label"] if len(breadcrumbs) >= 2 else None

    raw_url = item.get("productUrl") or ""
    product_url = _SITE_BASE + ("/" if not raw_url.startswith("/") else "") + raw_url

    return {
        "product_code": item.get("productCode"),
        "product_uid": str(item.get("productUidpk") or ""),
        "title": item.get("productLabel"),
        "universe": universe,
        "universe_key": universe_key,
        "category": category,
        "color": item.get("colorLabel"),
        "price": price_info.get("salePrice"),
        "list_price": price_info.get("listPrice"),
        "currency": price_info.get("currency") or "EUR",
        "product_url": product_url,
        "primary_image_url": primary_image,
        "rating": item.get("rate"),
        "total_reviews": display.get("totalReviewCount") or 0,
        "scraped_at": scraped_at,
    }


def scrape_category(base_url: str, universe: str, universe_key: str, delay: float) -> list[dict]:
    scraped_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    all_rows: list[dict] = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}?pn={page}"
        print(f"  Fetching {universe_key} page {page}: {url}", file=sys.stderr)

        data = _fetch_next_data(url)
        if data is None:
            print(f"  Failed to fetch {url} — stopping category", file=sys.stderr)
            break

        category_data = (
            data.get("props", {})
            .get("pageProps", {})
            .get("queryProductResponse", {})
            .get("category") or {}
        )
        items = category_data.get("items") or []
        total_pages = category_data.get("totalPages") or 1

        if not items:
            break

        for item in items:
            try:
                all_rows.append(_normalize_item(item, universe, universe_key, scraped_at))
            except Exception as e:
                print(f"  Skip item: {e}", file=sys.stderr)

        print(f"  Got {len(items)} items (page {page}/{total_pages})", file=sys.stderr)

        if page >= total_pages:
            break
        page += 1
        time.sleep(delay)

    return all_rows


def scrape_all(delay: float) -> list[dict]:
    all_rows: list[dict] = []
    for i, (key, url, universe) in enumerate(CATEGORIES):
        print(f"Category {i+1}/{len(CATEGORIES)}: {universe}", file=sys.stderr)
        rows = scrape_category(url, universe, key, delay)
        print(f"  Total for {universe}: {len(rows)} items", file=sys.stderr)
        all_rows.extend(rows)
        if i < len(CATEGORIES) - 1:
            time.sleep(delay * 2)
    return all_rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_output(rows: list[dict], path: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_file = f"{path.rstrip('/')}/run_{ts}.jsonl"
    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)

    try:
        dbutils  # noqa: F821
        dbutils.fs.put(out_file, content, overwrite=True)  # noqa: F821
        return
    except NameError:
        pass

    import os
    os.makedirs(path, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(content)


def main():
    args = _parse_args()

    if args.demo:
        rows = _demo_rows()
        print(f"Demo mode: {len(rows)} synthetic essentials rows")
    else:
        rows = scrape_all(args.delay)
        print(f"Scraped {len(rows)} essentials items across all categories")

    if not rows:
        print("No rows to write — exiting", file=sys.stderr)
        sys.exit(1)

    write_output(rows, args.output_base)
    print(f"Written to {args.output_base}")


if __name__ == "__main__":
    main()
