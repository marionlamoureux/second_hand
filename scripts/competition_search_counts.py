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
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
import requests

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



def get_vestiaire_count(brand: str) -> int | None:
    """Return total search result count for brand on Vestiaire Collective."""
    import json
    import re
    try:
        try:
            from curl_cffi import requests as req_lib
            session = req_lib.Session(impersonate="chrome120")
        except ImportError:
            import requests as req_lib
            session = req_lib.Session()
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9",
        }
        r = session.get(
            "https://www.vestiairecollective.com/search/",
            params={"q": brand},
            headers=headers,
            timeout=20,
        )
        if r.status_code != 200:
            print(f"Vestiaire {brand!r}: HTTP {r.status_code}", file=sys.stderr)
            return None

        # Extract total from __NEXT_DATA__ JSON
        m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                def _find_total(obj, depth=0):
                    if depth > 8:
                        return None
                    if isinstance(obj, dict):
                        for key in ("totalCount", "total_count", "totalItems", "totalResults", "nbHits", "total", "count"):
                            v = obj.get(key)
                            if isinstance(v, int) and v > 0:
                                return v
                        for v in obj.values():
                            r2 = _find_total(v, depth + 1)
                            if r2 is not None:
                                return r2
                    elif isinstance(obj, list):
                        for item in obj[:5]:
                            r2 = _find_total(item, depth + 1)
                            if r2 is not None:
                                return r2
                    return None
                total = _find_total(data)
                if total is not None:
                    return total
            except Exception:
                pass

        # Fallback: regex in raw HTML
        for pattern in (r'"totalCount"\s*:\s*(\d+)', r'"nbHits"\s*:\s*(\d+)', r'"total"\s*:\s*(\d+)'):
            m2 = re.search(pattern, r.text)
            if m2:
                return int(m2.group(1))

        return None
    except Exception as e:
        print(f"Vestiaire {brand!r}: {e}", file=sys.stderr)
        return None


def _beebs_slug(keyword: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFD", keyword.lower().strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace(" ", "-").replace("'", "").replace("&", "")
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "", s)).strip("-")


_BEEBS_CATALOG_TOTAL = 6_734_195


def get_beebs_count(brand: str) -> int | None:
    """Return total listing count for brand on Beebs via brand page (per-brand nbHits in HTML)."""
    import re as _re
    slug = _beebs_slug(brand)
    if not slug:
        return None
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9",
    })
    for path in (
        f"/fr/f-brand/vetement-{slug}-seconde-main",
        f"/fr/brand/{slug}",
    ):
        try:
            r = session.get("https://www.beebs.app" + path, timeout=15)
            if r.status_code != 200:
                continue
            for pattern in (r'\\"nbHits\\":\s*(\d+)', r'"nbHits":\s*(\d+)', r'nbHits[^0-9]*(\d+)'):
                m = _re.search(pattern, r.text)
                if m:
                    n = int(m.group(1))
                    if n != _BEEBS_CATALOG_TOTAL:
                        return n
        except Exception as e:
            print(f"Beebs {brand!r} {path}: {e}", file=sys.stderr)
    return None


def get_ebay_count(brand: str, app_id: str | None = None) -> int | None:
    """Return approximate listing count for brand on eBay France (HTML scrape, no API key needed).
    eBay displays 'Plus de X' where X is a per-brand approximate lower bound."""
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "fr-FR,fr;q=0.9",
        })
        r = session.get(
            "https://www.ebay.fr/sch/i.html",
            params={"_nkw": brand, "_ipg": 1},
            timeout=20,
        )
        if r.status_code != 200:
            print(f"eBay {brand!r}: HTTP {r.status_code}", file=sys.stderr)
            return None
        # eBay embeds the count as a numeric textSpan (French thousands: \xa0 separator)
        spans = re.findall(r'"text"\s*:\s*"([\d\xa0\s]+)"', r.text)
        for span in spans:
            clean = span.strip().replace("\xa0", "").replace(" ", "")
            if re.match(r"^\d+$", clean) and len(clean) >= 2:
                return int(clean)
        return None
    except Exception as e:
        print(f"eBay {brand!r}: {e}", file=sys.stderr)
        return None


def run_counts(brands: list[tuple[str, str]], delay: float = 1.0) -> list[dict]:
    """Run count for each (brand, category) on each marketplace. Returns list of {brand, category, marketplace, count}."""
    import time
    results = []
    for brand, category in brands:
        print(f"  {brand} ({category})...", file=sys.stderr, flush=True)
        lbc_count = get_leboncoin_count(brand)
        results.append({"brand": brand, "category": category, "marketplace": "leboncoin", "count": lbc_count})
        time.sleep(delay)
        vestiaire_count = get_vestiaire_count(brand)
        results.append({"brand": brand, "category": category, "marketplace": "vestiaire", "count": vestiaire_count})
        time.sleep(delay)
        beebs_count = get_beebs_count(brand)
        results.append({"brand": brand, "category": category, "marketplace": "beebs", "count": beebs_count})
        time.sleep(delay)
        ebay_count = get_ebay_count(brand)
        results.append({"brand": brand, "category": category, "marketplace": "ebay", "count": ebay_count})
        time.sleep(delay)
    return results


def main():
    ap = argparse.ArgumentParser(description="Get search result counts for Kiabi competitors on Leboncoin, Beebs, eBay")
    ap.add_argument("--output", "-o", default="", help="Write results to JSON file (and CSV if possible)")
    ap.add_argument("--output-dir", default="", help="Write JSONL for pipeline to this dir (e.g. output/competition); creates run_YYYYMMDD_HHMMSS.jsonl")
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
    print("Marketplaces: Leboncoin, Vestiaire Collective, Beebs, eBay France", file=sys.stderr)
    print("", file=sys.stderr)

    results = run_counts(brands_with_cat)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Table: group by brand/category, then marketplaces
    brands_seen = set()
    print("\nBrand (category)     | Leboncoin | Vestiaire |     Beebs |  eBay FR")
    print("-" * 72)
    for r in results:
        key = (r["brand"], r["category"])
        if key not in brands_seen:
            brands_seen.add(key)
            row_results = {x["marketplace"]: x["count"] for x in results if x["brand"] == r["brand"] and x["category"] == r["category"]}
            lbc   = row_results.get("leboncoin")
            vc    = row_results.get("vestiaire")
            beebs = row_results.get("beebs")
            ebay  = row_results.get("ebay")
            lbc_s   = f"{lbc:,}"   if lbc   is not None else "—"
            vc_s    = f"{vc:,}"    if vc    is not None else "—"
            beebs_s = f"{beebs:,}" if beebs is not None else "—"
            ebay_s  = f"{ebay:,}" if ebay is not None else "—"
            label = f"{r['brand']} ({r['category']})"
            print(f"{label:<20} | {lbc_s:>9} | {vc_s:>9} | {beebs_s:>9} | {ebay_s:>8}")

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
