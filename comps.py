#!/usr/bin/env python3
"""
comps.py — Zillow "Recently Sold" scraper for SF ZIP 94114
Playwright (stealth) + BeautifulSoup | production-grade

═══════════════════════════════════════════════════════════════════
STEP-BY-STEP NAVIGATION LOGIC
═══════════════════════════════════════════════════════════════════

PHASE 1 — Collect listing URLs from paginated search results
  • Start at https://www.zillow.com/san-francisco-ca-94114/sold/
  • Subsequent pages follow the pattern …/sold/2_p/, …/sold/3_p/ etc.
  • On each search-results page, extract detail-page URLs from the
    embedded __NEXT_DATA__ JSON first (reliable, no layout dependency).
    Fall back to scraping <a href="/homedetails/…"> anchors if the
    JSON path changes.
  • Stop paginating when a page returns zero listings or MAX_PAGES is hit.

PHASE 2 — Scrape each individual detail page
  • Navigate to each URL collected in Phase 1.
  • Parse __NEXT_DATA__ for structured property fields (lat/lon, beds,
    baths, sqft, homeType, description, dateSold, price).
  • Parse the rendered HTML with BeautifulSoup as a fallback / supplement:
      - "Price History" section  → most-recent Sold date + price
      - "Facts & Features" section → Zoning, Land Use, Condition, Floor Level
  • If Zoning or Land Use are absent from Facts & Features, run regex
    against the full Description text as a last-resort extraction.

PHASE 3 — Export to Excel
  • Coerce numeric columns, derive Price/SqFt, auto-fit column widths.

═══════════════════════════════════════════════════════════════════
XPATH / SELECTOR CHEATSHEET — Zoning & Land Use
═══════════════════════════════════════════════════════════════════

  Zoning (try in order):
    //span[contains(text(),'Zoning')]/following-sibling::span[1]
    //dt[contains(text(),'Zoning')]/following-sibling::dd[1]
    //li[contains(.,'Zoning')]/span[last()]
    //span[normalize-space()='Zoning:']/following-sibling::span[1]

  Land Use (try in order):
    //span[contains(text(),'Land use')]/following-sibling::span[1]
    //dt[contains(text(),'Land use')]/following-sibling::dd[1]
    //span[contains(text(),'Use code')]/following-sibling::span[1]
    //li[contains(.,'Land use')]/span[last()]

  Price History "Sold" row:
    //h4[contains(text(),'Price history')]/ancestor::section//tr[td[contains(.,'Sold')]]
    //h4[contains(text(),'Price history')]/ancestor::section//li[contains(.,'Sold')]

═══════════════════════════════════════════════════════════════════
"""

import json
import math
import random
import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ─── Configuration ────────────────────────────────────────────────────────────

# Center property — all results are filtered to within RADIUS_MILES of this point
CENTER_LAT   = 37.7665    # 683 14th St, San Francisco
CENTER_LON   = -122.4270
RADIUS_MILES = 0.5        # adjust as needed (0.25 / 0.5 / 1.0)

# ZIP codes whose sold listings overlap the search radius.
# Each is searched independently; duplicates across ZIPs are removed.
SEARCH_ZIPS = ["94114", "94117", "94110", "94131"]

MAX_PAGES_PER_ZIP = 10
OUTPUT            = "683_14th_St_Comps.xlsx"


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# ─── Distance helper ──────────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles between two lat/lon points."""
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))

# ─── Generic helpers ──────────────────────────────────────────────────────────

def _to_float(val) -> float | None:
    try:
        return float(re.sub(r"[,$+]", "", str(val)).strip())
    except (TypeError, ValueError):
        return None

def _format_date(val) -> str | None:
    """Normalise Unix-ms timestamps or pass-through date strings."""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)) and val > 1_000_000_000:
            return datetime.fromtimestamp(val / 1000).strftime("%Y-%m-%d")
    except (OSError, OverflowError):
        pass
    return str(val)

# ─── __NEXT_DATA__ parsing ────────────────────────────────────────────────────

def _parse_next_data(html: str) -> dict:
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}


def _urls_from_next_data(nd: dict) -> list[tuple[str, dict]]:
    """
    Extract detail-page URLs *and* pre-seed data from a search-results
    __NEXT_DATA__ blob.  listResults already carries price, date, beds,
    baths, sqft, homeType, lat/lon, address, and unit — grabbing them here
    avoids redundant detail-page requests for those fields.
    """
    items: list[tuple[str, dict]] = []
    try:
        results = nd["props"]["pageProps"]["searchPageState"]["cat1"]["searchResults"]["listResults"]
        for r in results:
            u = r.get("detailUrl") or r.get("hdpData", {}).get("homeInfo", {}).get("detailUrl")
            if not u:
                continue
            if not u.startswith("http"):
                u = "https://www.zillow.com" + u

            hi      = r.get("hdpData", {}).get("homeInfo", {})
            lat_lon = r.get("latLong", {})
            seed = {
                "Street Address": hi.get("streetAddress"),
                "Unit #":         hi.get("unit"),
                "Latitude":       _to_float(lat_lon.get("latitude")  or hi.get("latitude")),
                "Longitude":      _to_float(lat_lon.get("longitude") or hi.get("longitude")),
                "Sold Price ($)": _to_float(hi.get("price") or r.get("unformattedPrice")),
                "Sold Date":      _format_date(hi.get("dateSold")),
                "SqFt":           _to_float(hi.get("livingArea") or r.get("area")),
                "Property Type":  hi.get("homeType"),
                "Beds":           _to_float(hi.get("bedrooms") or r.get("beds")),
                "Baths":          _to_float(hi.get("bathrooms") or r.get("baths")),
            }
            items.append((u, seed))
    except (KeyError, TypeError):
        pass
    return items


# ─── HTML parsers for detail pages ────────────────────────────────────────────

def _parse_price_history(soup: BeautifulSoup) -> tuple[str | None, float | None]:
    """
    Locate the Price History section and return the most recent Sold entry.
    Handles both <table> and <ul>/<li> Zillow layouts.
    """
    for heading in soup.find_all(string=re.compile(r"Price\s+history", re.I)):
        section = heading.find_parent(["section", "div", "article"])
        if not section:
            continue

        table = section.find("table")
        rows  = table.find_all("tr") if table else section.find_all("li")

        for row in rows:
            text = row.get_text(" ", strip=True)
            if not re.search(r'\bsold\b', text, re.I):
                continue
            date_m  = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', text)
            price_m = re.search(r'\$([\d,]+)', text)
            return (
                date_m.group(1)           if date_m  else None,
                _to_float(price_m.group(1)) if price_m else None,
            )
    return None, None

def _parse_facts(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract all label→value pairs from the Facts & Features section.
    Cascades through three HTML patterns Zillow has used over time.
    """
    facts: dict[str, str] = {}

    # Pattern A: <dl><dt>label</dt><dd>value</dd></dl>
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            k = dt.get_text(" ", strip=True).rstrip(":")
            v = dd.get_text(" ", strip=True)
            if k:
                facts.setdefault(k, v)

    # Pattern B: consecutive <span> siblings — <span>Label</span><span>Value</span>
    for span in soup.find_all("span"):
        label = span.get_text(strip=True)
        if not label or len(label) > 80:
            continue
        sib = span.find_next_sibling("span")
        if sib:
            facts.setdefault(label, sib.get_text(strip=True))

    # Pattern C: <li> items formatted as "Label: Value"
    for li in soup.find_all("li"):
        text = li.get_text(" ", strip=True)
        if ":" in text and len(text) < 200:
            k, _, v = text.partition(":")
            facts.setdefault(k.strip(), v.strip())

    return facts

# ─── Bot-detection helper ─────────────────────────────────────────────────────

_CAPTCHA_SIGNALS = re.compile(
    r"press\s*&\s*hold|access to this page has been|are you a robot|"
    r"perimeterx|please verify you are a human|cf-challenge|just a moment",
    re.IGNORECASE,
)

def _is_blocked(html: str) -> bool:
    """Return True if Zillow served a bot-challenge / CAPTCHA page."""
    return bool(_CAPTCHA_SIGNALS.search(html[:4000]))

def _seed_to_row(seed: dict, url: str) -> dict:
    """Convert a search-results seed dict into an output row."""
    return {
        "Street Address": seed.get("Street Address"),
        "Unit #":         seed.get("Unit #"),
        "Latitude":       seed.get("Latitude"),
        "Longitude":      seed.get("Longitude"),
        "Sold Price ($)": seed.get("Sold Price ($)"),
        "Sold Date":      seed.get("Sold Date"),
        "SqFt":           seed.get("SqFt"),
        "Property Type":  seed.get("Property Type"),
        "Beds":           seed.get("Beds"),
        "Baths":          seed.get("Baths"),
        "Distance (mi)":  seed.get("Distance (mi)"),
        "URL":            url,
    }

# ─── Search-results pagination (plain HTTP — no browser) ──────────────────────

def collect_listings() -> list[tuple[str, dict]]:
    """
    Search each ZIP in SEARCH_ZIPS, keep only listings within RADIUS_MILES
    of the center property, and attach a Distance (mi) field to each seed.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Cache-Control":             "max-age=0",
    })

    all_items: list[tuple[str, dict]] = []

    for zip_code in SEARCH_ZIPS:
        base = f"https://www.zillow.com/san-francisco-ca-{zip_code}/sold/"
        print(f"\n── ZIP {zip_code} ──────────────────────────────────────────")

        for page_num in range(1, MAX_PAGES_PER_ZIP + 1):
            target = base if page_num == 1 else f"{base}{page_num}_p/"
            print(f"  [page {page_num}] {target}")

            try:
                resp = session.get(target, timeout=30)
                html = resp.text
            except requests.RequestException as exc:
                print(f"  Request error: {exc} — stopping ZIP.")
                break

            if _is_blocked(html):
                print(f"  Blocked on page {page_num} — stopping ZIP.")
                break

            nd = _parse_next_data(html)
            soup = BeautifulSoup(html, "html.parser")
            title = soup.find("title")
            print(f"    title: {title.get_text(strip=True) if title else '(none)'}  __NEXT_DATA__: {bool(nd)}")

            page_items = _urls_from_next_data(nd)
            if not page_items:
                print(f"  No listings on page {page_num} — stopping ZIP.")
                break

            # Filter by radius and attach distance
            nearby = []
            for u, d in page_items:
                lat, lon = d.get("Latitude"), d.get("Longitude")
                if lat and lon:
                    dist = round(_haversine(CENTER_LAT, CENTER_LON, lat, lon), 2)
                    if dist <= RADIUS_MILES:
                        d["Distance (mi)"] = dist
                        nearby.append((u, d))

            all_items.extend(nearby)
            print(f"  +{len(nearby)} within {RADIUS_MILES} mi  (running total: {len(all_items)})")

            time.sleep(random.uniform(3, 7))

    seen: set[str] = set()
    return [(u, d) for u, d in all_items if not (u in seen or seen.add(u))]  # type: ignore[func-returns-value]

# ─── Excel export ─────────────────────────────────────────────────────────────

def export_to_excel(rows: list[dict], filename: str = OUTPUT) -> None:
    if not rows:
        print("No data to export.")
        return

    df = pd.DataFrame(rows)

    for col in ("Sold Price ($)", "SqFt", "Latitude", "Longitude", "Beds", "Baths", "Distance (mi)"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("Distance (mi)")

    # Drop rows with missing or zero SqFt
    before = len(df)
    df = df[df["SqFt"].notna() & (df["SqFt"] > 0)]
    dropped = before - len(df)
    if dropped:
        print(f"  Dropped {dropped} record(s) with missing/zero SqFt")

    # Insert derived Price/SqFt column right after SqFt
    idx = df.columns.get_loc("SqFt") + 1
    df.insert(idx, "Price/SqFt ($)", (df["Sold Price ($)"] / df["SqFt"]).round(2))

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
        ws = writer.sheets["Sheet1"]
        for col_cells in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col_cells if c.value is not None),
                default=10,
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 60)

    df.to_csv("data.csv", index=False)
    print(f"\nExported {len(df)} records → {filename} + data.csv")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("═══ PHASE 1: Collecting listings from search results ════════")
    listings = collect_listings()
    print(f"\nTotal unique listings: {len(listings)}")

    print("\n═══ PHASE 2: Export ═════════════════════════════════════════")
    rows = [_seed_to_row(seed, url) for url, seed in listings]
    export_to_excel(rows)


if __name__ == "__main__":
    main()
