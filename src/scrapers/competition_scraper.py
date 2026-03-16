"""
Competition search-count scraper.

For each brand × marketplace combination, fetches the total number of listings
returned by the marketplace's search API for that brand keyword.
Output: one JSONL file per run → landing volume → bronze_listings → silver_competition_counts → gold_competition_counts

Brands covered:
  - Kiabi (primary)
  - Children & women's fashion competitors (H&M, Zara, Primark, Vertbaudet, Orchestra,
    Okaïdi, Dpam, Petit Bateau, Absorba, Tape à l'Oeil, Sergent Major, Catimini,
    La Redoute, Promod, Morgan, Jennyfer, Cache Cache)

Marketplaces:
  - LeBonCoin (unofficial lbc client)
  - Vinted (vinted-api client)
  - Vestiaire Collective
  - Decathlon Occasion
  - La Redoute Occasion
  - Beebs (www.beebs.app)

Usage (local):
    PYTHONPATH=src python3 -m scrapers.competition_scraper \
        --output-base /Volumes/nef_catalog/second_hand/kiabi_landing/competition

Usage (Databricks job – cluster with lbc + vinted-api installed):
    python competition_scraper.py --output-base /Volumes/...
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Brand list ────────────────────────────────────────────────────────────────
BRANDS = [
    "Kiabi",
    "Vertbaudet", "Orchestra", "Okaïdi", "Du Pareil au Même", "Dpam",
    "Petit Bateau", "Absorba", "Tape à l'Oeil", "Sergent Major", "Catimini", "Jacadi",
    "H&M", "Zara", "Primark", "La Redoute", "Promod", "Morgan", "Jennyfer",
    "Cache Cache", "Camaïeu", "Shein", "Vero Moda", "Jules",
]

# ─── Scrapers ──────────────────────────────────────────────────────────────────

def _count_leboncoin(keyword: str, delay: float = 2.0) -> int | None:
    """Return total result count from LeBonCoin for keyword, or None on error.
    lbc.Search has .total (total matching ads, e.g. ~355874 for Kiabi)."""
    try:
        import lbc
        client = lbc.Client()
        results = client.search(
            text=keyword,
            page=1,
            limit=1,
            sort=lbc.Sort.NEWEST,
            ad_type=lbc.AdType.OFFER,
        )
        # lbc returns Search with .total (total count), .total_all, .ads (page of ads)
        total = getattr(results, "total", None) or getattr(results, "total_all", None)
        if total is None:
            total = getattr(results, "total_count", None)
        if total is None and hasattr(results, "ads"):
            total = len(results.ads)  # fallback: only current page (wrong for full count)
        time.sleep(delay)
        return int(total) if total is not None else None
    except Exception as e:
        log.warning("LeBonCoin count failed for %s: %s", keyword, e)
        return None


def _count_vinted(keyword: str, delay: float = 2.0) -> int | None:
    """Return total result count from Vinted for keyword, or None on error.
    Uses session + Referer; prefers curl_cffi (Chrome impersonation) to avoid 403 when possible."""
    try:
        try:
            from curl_cffi import requests as req_lib
            session = req_lib.Session(impersonate="chrome120")
        except ImportError:
            import requests as req_lib
            session = req_lib.Session()
        session.get(
            "https://www.vinted.fr/",
            headers={"Accept-Language": "fr-FR,fr;q=0.9"},
            timeout=15,
        )
        resp = session.get(
            "https://www.vinted.fr/api/v2/catalog/items",
            params={"search_text": keyword, "page": 1, "per_page": 20, "order": "newest_first"},
            headers={"Accept": "application/json", "Referer": "https://www.vinted.fr/"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            total = (
                data.get("total")
                or data.get("pagination", {}).get("total_entries")
                or data.get("catalog", {}).get("total")
            )
            time.sleep(delay)
            return int(total) if total is not None else None
        log.warning("Vinted HTTP %s for %s", resp.status_code, keyword)
        return None
    except Exception as e:
        log.warning("Vinted count failed for %s: %s", keyword, e)
        return None


def _count_vestiaire(keyword: str, delay: float = 2.0) -> int | None:
    """Return total result count from Vestiaire Collective for keyword, or None on error."""
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
            params={"q": keyword},
            headers=headers,
            timeout=20,
        )
        if r.status_code != 200:
            log.warning("Vestiaire HTTP %s for %s", r.status_code, keyword)
            return None

        # Try __NEXT_DATA__ JSON first
        m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                def _find_total(obj, depth=0):
                    if depth > 8:
                        return None
                    if isinstance(obj, dict):
                        for key in ("totalCount", "total_count", "totalItems", "totalResults", "nbHits", "total"):
                            v = obj.get(key)
                            if isinstance(v, int) and v > 0:
                                return v
                        for v in obj.values():
                            result = _find_total(v, depth + 1)
                            if result is not None:
                                return result
                    elif isinstance(obj, list):
                        for item in obj[:5]:
                            result = _find_total(item, depth + 1)
                            if result is not None:
                                return result
                    return None
                total = _find_total(data)
                if total is not None:
                    time.sleep(delay)
                    return total
            except Exception:
                pass

        # Fallback: regex in raw HTML
        for pattern in (r'"totalCount"\s*:\s*(\d+)', r'"nbHits"\s*:\s*(\d+)', r'"total"\s*:\s*(\d+)'):
            m2 = re.search(pattern, r.text)
            if m2:
                time.sleep(delay)
                return int(m2.group(1))

        log.warning("Vestiaire: could not extract count for %s", keyword)
        return None
    except Exception as e:
        log.warning("Vestiaire count failed for %s: %s", keyword, e)
        return None


def _count_decathlon_occasion(keyword: str, delay: float = 2.0) -> int | None:
    """Return total result count from Decathlon Occasion for keyword, or None on error."""
    import re
    try:
        import requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9",
        }
        # Try the JSON API first
        api_url = "https://www.decathlon.fr/en/sports/occasion/search"
        resp = requests.get(
            "https://occasion.decathlon.fr/recherche",
            params={"q": keyword},
            headers=headers, timeout=15,
        )
        if resp.status_code == 200:
            # Look for total count patterns in HTML/JSON
            for pattern in (
                r'"totalResults"\s*:\s*(\d+)',
                r'"total"\s*:\s*(\d+)',
                r'"nbHits"\s*:\s*(\d+)',
                r'(\d+)\s+r[ée]sultat',
                r'(\d+)\s+article',
            ):
                m = re.search(pattern, resp.text, re.IGNORECASE)
                if m:
                    time.sleep(delay)
                    return int(m.group(1))
        log.warning("Decathlon Occasion: could not extract count for %s (HTTP %s)", keyword, resp.status_code)
        return None
    except Exception as e:
        log.warning("Decathlon Occasion count failed for %s: %s", keyword, e)
        return None


_LA_REDOUTE_COUNT_PATTERNS = (
    r'"totalCount"\s*:\s*(\d+)',
    r'"total"\s*:\s*(\d+)',
    r'"productCount"\s*:\s*(\d+)',
    r'"nbResults?"\s*:\s*(\d+)',
    r'(\d+)\s*r[ée]sultat[s]?',
    r'(\d+)\s*article[s]?',
)


def _la_redoute_parse_count(text: str) -> int | None:
    """Extract result count from La Redoute HTML/JSON. Returns None if not found."""
    for pattern in _LA_REDOUTE_COUNT_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n >= 0:
                return n
    return None


def _count_la_redoute_occasion_playwright(keyword: str, delay: float = 2.0) -> int | None:
    """Return La Redoute Occasion count by rendering the search page in a browser. Fallback when HTTP is blocked."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.debug("Playwright not installed; skipping La Redoute browser fallback")
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="fr-FR",
            )
            page = context.new_page()
            url = "https://www.laredoute.fr/ppdp/cat-450702.aspx?keyword=" + requests.utils.quote(keyword, safe="")
            page.goto(url, wait_until="networkidle", timeout=30_000)
            time.sleep(2)
            html = page.content()
            count = _la_redoute_parse_count(html)
            if count is not None:
                browser.close()
                time.sleep(delay)
                return count
            count = page.evaluate("""() => {
                const body = document.body.innerText;
                const m = body.match(/(\\d[\\d\\s]*)\\s*r[eé]sultat[s]?/i) || body.match(/(\\d[\\d\\s]*)\\s*article[s]?/i);
                if (m) return parseInt((m[1] || m[2]).replace(/\\s/g, ''), 10);
                return null;
            }""")
            browser.close()
            if isinstance(count, int) and count >= 0:
                time.sleep(delay)
                return count
    except Exception as e:
        log.debug("La Redoute Playwright fallback failed for %s: %s", keyword, e)
    return None


def _count_la_redoute_occasion(keyword: str, delay: float = 2.0) -> int | None:
    """Return total result count from La Redoute Seconde Main (ppdp) for keyword, or None on error.
    Uses session + Referer; prefers curl_cffi (Chrome impersonation). On 403 or parse failure, tries Playwright."""
    try:
        try:
            from curl_cffi import requests as req_lib
            session = req_lib.Session(impersonate="chrome120")
        except ImportError:
            session = requests.Session()
        session.get(
            "https://www.laredoute.fr/",
            headers={"Accept-Language": "fr-FR,fr;q=0.9"},
            timeout=15,
        )
        resp = session.get(
            "https://www.laredoute.fr/ppdp/cat-450702.aspx",
            params={"keyword": keyword},
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9",
                "Referer": "https://www.laredoute.fr/",
            },
            timeout=20,
        )
        count = _la_redoute_parse_count(resp.text)
        if count is not None:
            time.sleep(delay)
            return count
        if resp.status_code != 200:
            log.debug(
                "La Redoute Occasion: HTTP %s for %s, trying Playwright",
                resp.status_code,
                keyword,
            )
        else:
            log.debug("La Redoute Occasion: no count in response for %s, trying Playwright", keyword)

        count = _count_la_redoute_occasion_playwright(keyword, delay)
        if count is not None:
            return count
        log.warning("La Redoute Occasion: could not extract count for %s", keyword)
        return None
    except Exception as e:
        log.warning("La Redoute Occasion count failed for %s: %s", keyword, e)
        return None


def _beebs_slug(keyword: str) -> str:
    """Normalize brand name to Beebs URL slug (lowercase, hyphen, no accents)."""
    import unicodedata
    s = keyword.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace(" ", "-").replace("'", "").replace("&", "")
    s = re.sub(r"[^a-z0-9-]", "", re.sub(r"-+", "-", s)).strip("-")
    return s


def _beebs_parse_nb_hits(text: str) -> int | None:
    """Extract first nbHits (or total) number from Beebs HTML/JSON. Returns None if not found."""
    for pattern in (
        r'\\"nbHits\\"\s*:\s*(\d+)',
        r'"nbHits"\s*:\s*(\d+)',
        r'nbHits[^0-9]*(\d+)',
        r'"totalCount"\s*:\s*(\d+)',
        r'"total"\s*:\s*(\d+)',
        r'(\d+)\s+r[ée]sultat',
    ):
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


# Beebs catalog total in initial HTML when no brand filter; we treat this as "no per-brand count".
_BEEBS_CATALOG_TOTAL = 6_734_195


def _count_beebs_brand_pages(keyword: str, session: requests.Session, delay: float) -> int | None:
    """Try Beebs brand pages (per-brand nbHits). Returns count or None."""
    slug = _beebs_slug(keyword)
    if not slug:
        return None
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
    }
    # Try f-brand (clothing) then brand (all categories)
    for path in (
        f"/fr/f-brand/vetement-{slug}-seconde-main",
        f"/fr/brand/{slug}",
    ):
        try:
            resp = session.get("https://www.beebs.app" + path, headers=headers, timeout=15)
            if resp.status_code != 200:
                continue
            total = _beebs_parse_nb_hits(resp.text)
            if total is not None and total != _BEEBS_CATALOG_TOTAL:
                time.sleep(delay)
                return total
        except Exception:
            continue
    return None


def _count_beebs_playwright(keyword: str, delay: float = 2.0) -> int | None:
    """Return Beebs search result count by rendering the search page (Playwright). Fallback when brand pages fail."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.debug("Playwright not installed; skipping Beebs browser fallback")
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="fr-FR",
            )
            page = context.new_page()
            url = f"https://www.beebs.app/fr/search?q={requests.utils.quote(keyword)}"
            page.goto(url, wait_until="networkidle", timeout=25_000)
            time.sleep(1.5)

            # 1) Parse nbHits from rendered HTML (client may inject filtered count after load)
            html = page.content()
            count = _beebs_parse_nb_hits(html)
            if count is not None and count != _BEEBS_CATALOG_TOTAL:
                browser.close()
                time.sleep(delay)
                return count

            # 2) Try DOM text: "X résultats" or result-count element
            count = page.evaluate("""() => {
                const body = document.body.innerText;
                const m = body.match(/(\\d[\\d\\s]*)\\s*r[eé]sultat[s]?/i);
                if (m) return parseInt(m[1].replace(/\\s/g, ''), 10);
                const el = document.querySelector('[data-testid="search-results-count"], [class*="result-count"], [class*="ResultCount"]');
                if (el) return parseInt(el.innerText.replace(/\\D/g, ''), 10) || null;
                return null;
            }""")
            browser.close()
            if isinstance(count, int) and count >= 0:
                time.sleep(delay)
                return count
    except Exception as e:
        log.debug("Beebs Playwright fallback failed for %s: %s", keyword, e)
    return None


def _count_beebs(keyword: str, delay: float = 2.0) -> int | None:
    """Return total result count from Beebs (www.beebs.app) for keyword, or None on error.

    Uses two methods for per-brand count:
    1) Brand pages: /fr/f-brand/vetement-{slug}-seconde-main and /fr/brand/{slug} (per-brand nbHits in HTML).
    2) Playwright: render search?q= and read count from DOM if 1) fails or returns catalog total.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9",
        }
        session = requests.Session()
        session.headers.update(headers)

        # Method 1: brand pages (per-brand count)
        count = _count_beebs_brand_pages(keyword, session, delay)
        if count is not None:
            return count

        # Method 2: search page HTML (often returns catalog total when search is client-side)
        resp = session.get(
            "https://www.beebs.app/fr/search",
            params={"q": keyword},
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning("Beebs HTTP %s for %s", resp.status_code, keyword)
            return None
        text = resp.text
        count = _beebs_parse_nb_hits(text)
        if count is not None and count != _BEEBS_CATALOG_TOTAL:
            time.sleep(delay)
            return count
        if count == _BEEBS_CATALOG_TOTAL:
            log.debug("Beebs search page returned catalog total for %s; trying Playwright", keyword)

        # Method 3: Playwright (render search page and read count from DOM / rendered HTML)
        count = _count_beebs_playwright(keyword, delay)
        if count is not None:
            return count
        # Last resort: use search-page nbHits (may be catalog total when search is client-side)
        fallback = _beebs_parse_nb_hits(text)
        if fallback is not None:
            time.sleep(delay)
            if fallback == _BEEBS_CATALOG_TOTAL:
                log.info("Beebs: using catalog total for %s (no per-brand count available)", keyword)
            return fallback
        log.warning("Beebs: could not extract count for %s", keyword)
        return None
    except Exception as e:
        log.warning("Beebs count failed for %s: %s", keyword, e)
        return None

# ─── Main ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape brand search counts from marketplaces")
    p.add_argument("--output-base", required=True,
                   help="Base path for JSONL output (e.g. /Volumes/.../competition)")
    p.add_argument("--append-to",
                   help="Also append all results to this JSONL file (e.g. output/competition/competition_20260306T215511Z.jsonl)")
    p.add_argument("--marketplace",
                   help="Run only this marketplace (e.g. vinted, leboncoin). Default: all.")
    p.add_argument("--delay", type=float, default=2.0,
                   help="Seconds between API calls (default 2.0)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    ts = datetime.now(timezone.utc)
    ts_str = ts.isoformat().replace("+00:00", "Z")
    ts_file = ts.strftime("%Y%m%dT%H%M%SZ")

    # Primary output: always write to output_base (Databricks Volume when run as job; no --append-to in job)
    out_path = Path(args.output_base)
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / f"competition_{ts_file}.jsonl"

    rows: list[dict] = []
    all_marketplaces = [
        ("leboncoin",          _count_leboncoin),
        ("vinted",             _count_vinted),
        ("vestiaire",          _count_vestiaire),
        ("decathlon_occasion", _count_decathlon_occasion),
        ("la_redoute_occasion",_count_la_redoute_occasion),
        ("beebs",              _count_beebs),
    ]
    only = (args.marketplace or "").strip().lower()
    if only:
        marketplaces = [(k, fn) for k, fn in all_marketplaces if k == only]
        if not marketplaces:
            log.error("Unknown marketplace %r. Choose one of: %s", args.marketplace,
                      ", ".join(k for k, _ in all_marketplaces))
            sys.exit(1)
    else:
        marketplaces = all_marketplaces

    for brand in BRANDS:
        for marketplace, count_fn in marketplaces:
            log.info("Counting '%s' on %s …", brand, marketplace)
            count = count_fn(brand)
            if count is None:
                log.warning("  Skipping %s/%s — no result", brand, marketplace)
                continue
            log.info("  %s/%s → %s results", brand, marketplace, count)
            rows.append({
                "brand":       brand,
                "marketplace": marketplace,
                "count":       count,
                "timestamp":   ts_str,
            })
            time.sleep(args.delay)

    with open(out_file, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    log.info("Wrote %d rows to %s", len(rows), out_file)

    if getattr(args, "append_to", None):
        append_path = Path(args.append_to)
        append_path.parent.mkdir(parents=True, exist_ok=True)
        with open(append_path, "a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        log.info("Appended %d rows to %s", len(rows), append_path)


if __name__ == "__main__":
    main()
