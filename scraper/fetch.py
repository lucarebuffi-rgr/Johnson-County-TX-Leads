#!/usr/bin/env python3
"""
Johnson County TX – Motivated Seller Lead Scraper
Uses Playwright to render JavaScript pages on PublicSearch.
Targets React div-based results with class doc-preview-group__summary-group-item
"""

import asyncio
import csv
import io
import json
import logging
import re
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://johnson.tx.publicsearch.us"

DOC_TYPES = {
    "LiPn":  ("pre_foreclosure", "Lis Pendens"),
    "ReoLPn":("pre_foreclosure", "Release of Lis Pendens"),
    "FeTLe": ("lien",            "Federal Tax Lien"),
    "StTLe": ("lien",            "State Tax Lien"),
    "Jun":   ("judgment",        "Judgment"),
    "AboJn": ("judgment",        "Abstract of Judgment"),
    "Prt":   ("probate",         "Probate"),
    "Lie":   ("lien",            "Lien"),
    "NooLe": ("lien",            "Notice of Lien"),
    "MeLCc": ("lien",            "Mechanics Lien Contract"),
    "HoLe":  ("lien",            "Hospital Lien"),
    "ChSLe": ("lien",            "Child Support Lien"),
}

LOOKBACK_DAYS   = 14
REQUEST_TIMEOUT = 30

def parse_date(raw: str) -> Optional[str]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def name_variants(full: str) -> list[str]:
    full = full.strip().upper()
    parts = full.split()
    if len(parts) < 2:
        return [full]
    first, last = parts[0], parts[-1]
    return [full, f"{last} {first}", f"{last}, {first}", f"{last},{first}"]

# ── PARCEL LOOKUP ─────────────────────────────────────────────────────────

def build_parcel_lookup() -> dict:
    lookup: dict[str, dict] = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    dbf_data = None

    for url in [
        "https://www.johnsoncad.com/data/download/property.zip",
        "https://www.johnsoncad.com/data/download/parcel.zip",
        "https://johnsoncad.com/downloads/JohnsonCAD_Parcels.zip",
    ]:
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                dbf_data = r.content
                log.info(f"Got parcel data from {url}")
                break
        except Exception:
            continue

    if not dbf_data or not HAS_DBF:
        log.warning("Parcel data not available")
        return lookup

    try:
        if dbf_data[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(dbf_data)) as z:
                for name in z.namelist():
                    if name.lower().endswith(".dbf"):
                        dbf_data = z.read(name)
                        break

        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(dbf_data)
        records = list(DBF(str(tmp), load=True, ignore_missing_memofile=True))
        log.info(f"Loaded {len(records):,} parcel records")

        for rec in records:
            rec = {k.upper(): (v.strip() if isinstance(v, str) else v) for k, v in rec.items()}
            owner = (rec.get("OWNER") or rec.get("OWN1") or "").upper().strip()
            if not owner:
                continue
            parcel = {
                "prop_address": rec.get("SITE_ADDR") or rec.get("SITEADDR") or "",
                "prop_city":    rec.get("SITE_CITY") or "Cleburne",
                "prop_state":   "TX",
                "prop_zip":     str(rec.get("SITE_ZIP") or rec.get("SITEZIP") or ""),
                "mail_address": rec.get("ADDR_1") or rec.get("MAILADR1") or "",
                "mail_city":    rec.get("CITY") or rec.get("MAILCITY") or "",
                "mail_state":   rec.get("STATE") or "TX",
                "mail_zip":     str(rec.get("ZIP") or rec.get("MAILZIP") or ""),
            }
            for variant in name_variants(owner):
                lookup[variant] = parcel
    except Exception:
        log.error(f"Parcel error:\n{traceback.format_exc()}")

    return lookup

# ── HTML PARSER ───────────────────────────────────────────────────────────

def parse_html_table(html: str, doc_code: str, cat: str, cat_label: str, dt_from: str, dt_to: str) -> tuple[list[dict], bool]:
    records = []
    soup = BeautifulSoup(html, "lxml")

    search_url = f"{BASE_URL}/results?department=RP&docTypes={doc_code}&recordedDateRange={dt_from},{dt_to}&searchType=advancedSearch"

    # Try React div-based results first
    result_items = (
        soup.select("[class*='doc-preview-group__summary-group-item']") or
        soup.select("[class*='result-item']") or
        soup.select("[class*='ResultItem']") or
        soup.select("[class*='document-row']") or
        soup.select("[class*='docPreview']")
    )

    if result_items:
        log.info(f"    Found {len(result_items)} React result items")
        for item in result_items:
            texts = [t.strip() for t in item.stripped_strings if t.strip()]
            if len(texts) < 3:
                continue

            link = ""
            for a in item.find_all("a", href=True):
                href = a["href"]
                link = BASE_URL + href if href.startswith("/") else href
                break

            date_idx = -1
            for i, t in enumerate(texts):
                if re.match(r"\d{1,2}/\d{1,2}/\d{4}", t):
                    date_idx = i
                    break

            grantor = grantee = filed = doc_num = legal = ""
            if date_idx >= 2:
                grantor  = texts[date_idx - 2]
                grantee  = texts[date_idx - 1]
                filed    = texts[date_idx]
                doc_num  = texts[date_idx + 1] if date_idx + 1 < len(texts) else ""
                legal    = texts[date_idx + 3] if date_idx + 3 < len(texts) else ""
            elif len(texts) >= 3:
                grantor  = texts[0]
                grantee  = texts[1]
                filed    = next((t for t in texts if re.match(r"\d{1,2}/\d{1,2}/\d{4}", t)), "")
                doc_num  = texts[3] if len(texts) > 3 else ""

            if not grantor:
                continue

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_code,
                "cat":       cat,
                "cat_label": cat_label,
                "filed":     parse_date(filed) or filed,
                "grantor":   grantor,
                "grantee":   grantee,
                "legal":     legal,
                "amount":    None,
                "clerk_url": link or search_url,
                "_demo":     False,
            })

    else:
        # Fallback: try regular HTML table
        rows = soup.select("table tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            texts = [c.get_text(strip=True) for c in cells]
            if not texts or texts[0].upper() in ["GRANTOR", ""]:
                continue

            link = ""
            for a in row.find_all("a", href=True):
                href = a["href"]
                if any(k in href for k in ["/doc/", "/instrument/", "/record/"]):
                    link = BASE_URL + href if href.startswith("/") else href
                    break

            date_idx = -1
            for i, t in enumerate(texts):
                if re.match(r"\d{1,2}/\d{1,2}/\d{4}", t):
                    date_idx = i
                    break

            grantor = grantee = filed = doc_num = legal = ""
            if date_idx >= 2:
                grantor  = texts[date_idx - 2]
                grantee  = texts[date_idx - 1]
                filed    = texts[date_idx]
                doc_num  = texts[date_idx + 1] if date_idx + 1 < len(texts) else ""
                legal    = texts[date_idx + 3] if date_idx + 3 < len(texts) else ""

            if not grantor:
                continue

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_code,
                "cat":       cat,
                "cat_label": cat_label,
                "filed":     parse_date(filed) or filed,
                "grantor":   grantor,
                "grantee":   grantee,
                "legal":     legal,
                "amount":    None,
                "clerk_url": link or search_url,
                "_demo":     False,
            })

    has_next = bool(
        soup.find("a", string=re.compile(r"next|›|»", re.I)) or
        soup.find("a", attrs={"aria-label": re.compile(r"next", re.I)}) or
        soup.find(class_=re.compile(r"next", re.I))
    )

    return records, has_next

# ── PLAYWRIGHT SCRAPER ────────────────────────────────────────────────────

async def scrape_all_playwright(date_from: str, date_to: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.error("Playwright not available!")
        return []

    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y%m%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y%m%d")
    except Exception:
        dt_from = date_from.replace("/","")
        dt_to   = date_to.replace("/","")

    all_records: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            url = (f"{BASE_URL}/results"
                   f"?department=RP"
                   f"&docTypes={doc_code}"
                   f"&recordedDateRange={dt_from},{dt_to}"
                   f"&searchType=advancedSearch")

            log.info(f"  Scraping {doc_code} ({cat_label}) …")

            page_num = 1
            while True:
                page_url = url if page_num == 1 else f"{url}&page={page_num}"

                try:
                    page = await context.new_page()
                    await page.goto(page_url, timeout=30_000)

                    # Wait for React to render results
                    try:
                        await page.wait_for_selector(
                            "[class*='doc-preview'], [class*='result'], table tr td, [class*='summary-group-item']",
                            timeout=15_000
                        )
                    except PWTimeout:
                        # Even if timeout, still grab HTML and try to parse
                        pass

                    # Extra wait for JS to finish rendering
                    await asyncio.sleep(3)

                    html = await page.content()
                    await page.close()

                    records, has_next = parse_html_table(html, doc_code, cat, cat_label, dt_from, dt_to)
                    all_records.extend(records)
                    log.info(f"    Page {page_num}: {len(records)} records")

                    if not records or not has_next:
                        break

                    page_num += 1
                    if page_num > 20:
                        break

                    await asyncio.sleep(2)

                except Exception as e:
                    log.warning(f"    Error on {doc_code} page {page_num}: {e}")
                    try:
                        await page.close()
                    except Exception:
                        pass
                    break

        await browser.close()

    return all_records

# ── DEMO DATA ─────────────────────────────────────────────────────────────

def generate_demo_records(date_from: str, date_to: str) -> list[dict]:
    samples = [
        ("LiPn",  "pre_foreclosure", "Lis Pendens",            "ROCKET MORTGAGE LLC",   "WRIGHT ROSEANN",        0),
        ("Jun",   "judgment",        "Judgment",                "JONES MARY B",          "CAPITAL ONE NA",    87500),
        ("FeTLe", "lien",            "Federal Tax Lien",        "WILLIAMS DAVID",        "IRS",               45200),
        ("AboJn", "judgment",        "Abstract of Judgment",    "GARCIA PROPERTIES LLC", "CLEBURNE SUPPLY CO",18700),
        ("MeLCc", "lien",            "Mechanics Lien Contract", "BROWN PATRICIA",        "LONE STAR CONTR",   22000),
        ("Prt",   "probate",         "Probate",                 "ESTATE OF DAVIS JAMES", "JOHNSON CO PROBATE",    0),
        ("StTLe", "lien",            "State Tax Lien",          "HENDERSON ROBERT",      "STATE OF TEXAS",     9800),
        ("NooLe", "lien",            "Notice of Lien",          "MARTINEZ CARLOS",       "BELCLAIRE RESID",    3500),
        ("HoLe",  "lien",            "Hospital Lien",           "THOMPSON SARAH",        "TEXAS HEALTH",       2100),
        ("ChSLe", "lien",            "Child Support Lien",      "RODRIGUEZ JUAN",        "ATTY/GEN",           5000),
    ]
    base = datetime.strptime(date_from, "%m/%d/%Y")
    recs = []
    for i, (code, cat, cat_label, grantor, grantee, amt) in enumerate(samples):
        filed_dt = base + timedelta(days=i % LOOKBACK_DAYS)
        recs.append({
            "doc_num":   f"2026-DEMO-{i+1:04d}",
            "doc_type":  code,
            "cat":       cat,
            "cat_label": cat_label,
            "filed":     filed_dt.strftime("%Y-%m-%d"),
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     "DEMO RECORD",
            "amount":    float(amt) if amt else None,
            "clerk_url": f"{BASE_URL}/results?department=RP&docTypes={code}&recordedDateRange=20260328,20260411&searchType=advancedSearch",
            "_demo":     True,
        })
    return recs

# ── ENRICHMENT ────────────────────────────────────────────────────────────

def enrich_with_parcel(records: list[dict], lookup: dict) -> list[dict]:
    for rec in records:
        owner = rec.get("grantor", "").upper().strip()
        parcel = None
        for variant in name_variants(owner):
            parcel = lookup.get(variant)
            if parcel:
                break
        if parcel:
            rec.update(parcel)
        else:
            rec.setdefault("prop_address", "")
            rec.setdefault("prop_city",    "")
            rec.setdefault("prop_state",   "TX")
            rec.setdefault("prop_zip",     "")
            rec.setdefault("mail_address", "")
            rec.setdefault("mail_city",    "")
            rec.setdefault("mail_state",   "TX")
            rec.setdefault("mail_zip",     "")
    return records

# ── SCORING ───────────────────────────────────────────────────────────────

def score_record(rec: dict) -> tuple[int, list[str]]:
    score = 30
    flags: list[str] = []
    dtype  = rec.get("doc_type", "")
    amount = rec.get("amount") or 0

    if dtype in ("LiPn","ReoLPn"): flags.append("Lis pendens")
    if dtype in ("FeTLe","StTLe"): flags.append("Tax lien")
    if dtype in ("Jun","AboJn"):   flags.append("Judgment lien")
    if dtype == "Prt":    flags.append("Probate / estate")
    if dtype == "MeLCc":  flags.append("Mechanic lien")
    if dtype == "NooLe":  flags.append("Notice of lien")
    if dtype == "HoLe":   flags.append("Hospital lien")
    if dtype == "ChSLe":  flags.append("Child support lien")
    if dtype == "Lie":    flags.append("Lien")

    owner = rec.get("grantor", "").upper()
    if any(x in owner for x in ("LLC","INC","CORP","LTD","LP ","L.P.")):
        flags.append("LLC / corp owner")

    try:
        filed = datetime.strptime(rec.get("filed",""), "%Y-%m-%d")
        if (datetime.today() - filed).days <= 14:
            flags.append("New this week")
    except Exception:
        pass

    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    score += 10 * len(flags)
    if "Lis pendens" in flags: score += 20
    if amount and amount > 100_000: score += 15
    elif amount and amount > 50_000: score += 10
    if "New this week" in flags: score += 5
    if has_addr: score += 5
    return min(score, 100), flags

# ── OUTPUT ────────────────────────────────────────────────────────────────

def build_output(raw_records: list[dict], date_from: str, date_to: str) -> dict:
    out_records = []
    for raw in raw_records:
        try:
            score, flags = score_record(raw)
            out_records.append({
                "doc_num":      raw.get("doc_num",""),
                "doc_type":     raw.get("doc_type",""),
                "filed":        raw.get("filed",""),
                "cat":          raw.get("cat","other"),
                "cat_label":    raw.get("cat_label",""),
                "owner":        raw.get("grantor",""),
                "grantee":      raw.get("grantee",""),
                "amount":       raw.get("amount"),
                "legal":        raw.get("legal",""),
                "prop_address": raw.get("prop_address",""),
                "prop_city":    raw.get("prop_city",""),
                "prop_state":   raw.get("prop_state","TX"),
                "prop_zip":     raw.get("prop_zip",""),
                "mail_address": raw.get("mail_address",""),
                "mail_city":    raw.get("mail_city",""),
                "mail_state":   raw.get("mail_state","TX"),
                "mail_zip":     raw.get("mail_zip",""),
                "clerk_url":    raw.get("clerk_url",""),
                "flags":        flags,
                "score":        score,
                "_demo":        raw.get("_demo",False),
            })
        except Exception:
            log.warning(f"Skipping: {traceback.format_exc()}")

    out_records.sort(key=lambda r: (-r["score"], r.get("filed","") or ""))
    with_address = sum(1 for r in out_records if r["prop_address"] or r["mail_address"])

    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Johnson County TX – PublicSearch",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(out_records),
        "with_address": with_address,
        "records":      out_records,
    }

def save_output(data: dict):
    for path in ["dashboard/records.json", "data/records.json"]:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2))
        log.info(f"Saved {data['total']} records → {path}")

def export_ghl_csv(data: dict):
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in data["records"]:
        parts = (r.get("owner","")).split()
        writer.writerow({
            "First Name":             parts[0] if parts else "",
            "Last Name":              " ".join(parts[1:]) if len(parts)>1 else "",
            "Mailing Address":        r.get("mail_address",""),
            "Mailing City":           r.get("mail_city",""),
            "Mailing State":          r.get("mail_state","TX"),
            "Mailing Zip":            r.get("mail_zip",""),
            "Property Address":       r.get("prop_address",""),
            "Property City":          r.get("prop_city",""),
            "Property State":         r.get("prop_state","TX"),
            "Property Zip":           r.get("prop_zip",""),
            "Lead Type":              r.get("cat_label",""),
            "Document Type":          r.get("doc_type",""),
            "Date Filed":             r.get("filed",""),
            "Document Number":        r.get("doc_num",""),
            "Amount/Debt Owed":       str(r.get("amount","") or ""),
            "Seller Score":           str(r.get("score","")),
            "Motivated Seller Flags": "|".join(r.get("flags",[])),
            "Source":                 "Johnson County TX",
            "Public Records URL":     r.get("clerk_url",""),
        })
    Path("data/ghl_export.csv").write_text(buf.getvalue())
    log.info("GHL CSV saved")

# ── MAIN ──────────────────────────────────────────────────────────────────

async def main():
    today     = datetime.today()
    start     = today - timedelta(days=LOOKBACK_DAYS)
    date_from = start.strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")

    log.info("=== Johnson County TX Lead Scraper ===")
    log.info(f"Date range: {date_from} → {date_to}")

    log.info("Building parcel lookup …")
    parcel_lookup = build_parcel_lookup()
    log.info(f"  {len(parcel_lookup):,} name variants indexed")

    log.info("Scraping with Playwright …")
    raw_records = await scrape_all_playwright(date_from, date_to)
    log.info(f"Total raw records: {len(raw_records)}")

    if not raw_records:
        log.warning("No live records – using demo data")
        raw_records = generate_demo_records(date_from, date_to)

    raw_records = enrich_with_parcel(raw_records, parcel_lookup)
    data = build_output(raw_records, date_from, date_to)
    save_output(data)
    export_ghl_csv(data)
    log.info(f"Done. {data['total']} leads | {data['with_address']} with address")


if __name__ == "__main__":
    asyncio.run(main())
