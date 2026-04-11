#!/usr/bin/env python3
"""
Johnson County TX – Motivated Seller Lead Scraper
Targets: johnson.tx.publicsearch.us (Tyler Technologies PublicSearch)
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

import requests
from bs4 import BeautifulSoup

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://johnson.tx.publicsearch.us"
API_URL  = "https://johnson.tx.publicsearch.us/api/search"

CAD_BULK_URLS = [
    "https://www.johnsoncad.com/data/download/",
    "https://www.johnsoncad.com/downloads/",
    "https://johnsoncad.com/appraisaldata/",
]

LOOKBACK_DAYS   = 7
RETRY_ATTEMPTS  = 3
RETRY_DELAY     = 3
REQUEST_TIMEOUT = 30

DOC_TYPE_MAP = {
    "LP":       ("pre_foreclosure", "Lis Pendens"),
    "NOFC":     ("pre_foreclosure", "Notice of Foreclosure"),
    "TAXDEED":  ("tax",             "Tax Deed"),
    "JUD":      ("judgment",        "Judgment"),
    "CCJ":      ("judgment",        "Certified Judgment"),
    "DRJUD":    ("judgment",        "Domestic Judgment"),
    "LNCORPTX": ("lien",            "Corp Tax Lien"),
    "LNIRS":    ("lien",            "IRS Lien"),
    "LNFED":    ("lien",            "Federal Lien"),
    "LN":       ("lien",            "Lien"),
    "LNMECH":   ("lien",            "Mechanic Lien"),
    "LNHOA":    ("lien",            "HOA Lien"),
    "MEDLN":    ("lien",            "Medicaid Lien"),
    "PRO":      ("probate",         "Probate Document"),
    "NOC":      ("construction",    "Notice of Commencement"),
    "RELLP":    ("pre_foreclosure", "Release Lis Pendens"),
}

TARGET_TYPES = set(DOC_TYPE_MAP.keys())

def retry(fn, *args, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY, **kwargs):
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning(f"Attempt {i+1}/{attempts} failed: {exc}")
            if i < attempts - 1:
                time.sleep(delay)
    return None

def parse_amount(raw) -> Optional[float]:
    try:
        cleaned = re.sub(r"[^\d.]", "", str(raw))
        return float(cleaned) if cleaned else None
    except Exception:
        return None

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
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"})
    dbf_data: Optional[bytes] = None

    guesses = [
        "https://www.johnsoncad.com/data/download/property.zip",
        "https://www.johnsoncad.com/data/download/parcel.zip",
        "https://johnsoncad.com/downloads/JohnsonCAD_Parcels.zip",
        "https://johnsoncad.com/downloads/parcel_data.zip",
    ]
    for url in guesses:
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                dbf_data = r.content
                log.info(f"Got parcel data from {url}")
                break
        except Exception:
            continue

    if not dbf_data or not HAS_DBF:
        log.warning("Parcel data not available – addresses will be empty")
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
        log.error(f"Parcel parse error:\n{traceback.format_exc()}")

    return lookup

# ── PUBLICSEARCH API ──────────────────────────────────────────────────────

def scrape_publicsearch(date_from: str, date_to: str) -> list[dict]:
    """
    Query the Tyler Technologies PublicSearch API directly.
    """
    records: list[dict] = []
    session = requests.Session()
    session.headers.update({
        "User-Agent":   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        "Accept":       "application/json, text/plain, */*",
        "Referer":      BASE_URL,
        "Origin":       BASE_URL,
    })

    # First hit the main page to get any cookies/tokens
    try:
        session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
    except Exception:
        pass

    # Convert dates to format API expects: YYYYMMDD
    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y%m%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y%m%d")
    except Exception:
        dt_from = date_from
        dt_to   = date_to

    # Try each doc type
    for doc_code in TARGET_TYPES:
        log.info(f"  Querying PublicSearch for {doc_code} …")

        # Try API endpoint
        params = {
            "county":       "johnson",
            "state":        "tx",
            "docType":      doc_code,
            "dateFrom":     dt_from,
            "dateTo":       dt_to,
            "page":         0,
            "size":         100,
        }

        try:
            r = session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                try:
                    data = r.json()
                    hits = data.get("hits", {}).get("hits", []) or data.get("results", []) or []
                    for hit in hits:
                        src = hit.get("_source", hit)
                        rec = parse_publicsearch_record(src, doc_code)
                        if rec:
                            records.append(rec)
                    log.info(f"    API → {len(hits)} hits")
                    continue
                except Exception:
                    pass
        except Exception:
            pass

        # Try alternate API paths
        alt_urls = [
            f"{BASE_URL}/api/instruments",
            f"{BASE_URL}/api/documents",
            f"{BASE_URL}/results",
        ]
        for url in alt_urls:
            try:
                r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                    data = r.json()
                    hits = (data.get("hits", {}).get("hits", []) or
                            data.get("results", []) or
                            data.get("instruments", []) or [])
                    for hit in hits:
                        src = hit.get("_source", hit)
                        rec = parse_publicsearch_record(src, doc_code)
                        if rec:
                            records.append(rec)
                    if hits:
                        log.info(f"    Alt API {url} → {len(hits)} hits")
                        break
            except Exception:
                continue

        time.sleep(0.5)

    return records


def parse_publicsearch_record(src: dict, doc_code: str) -> Optional[dict]:
    try:
        filed_raw = (src.get("recordedDate") or src.get("filedDate") or
                     src.get("instrumentDate") or src.get("date") or "")
        doc_num = (src.get("instrumentNumber") or src.get("docNumber") or
                   src.get("instrument") or src.get("id") or "")
        grantor = ""
        grantee = ""

        parties = src.get("parties", [])
        for p in parties:
            role = str(p.get("role", "")).upper()
            name = p.get("name", "")
            if "GRANTOR" in role or "SELLER" in role or "OWNER" in role:
                grantor = name
            elif "GRANTEE" in role or "BUYER" in role:
                grantee = name

        if not grantor:
            grantor = src.get("grantor", "") or src.get("grantorName", "")
        if not grantee:
            grantee = src.get("grantee", "") or src.get("granteeName", "")

        dtype = (src.get("docType") or src.get("instrumentType") or doc_code).upper()
        amount_raw = src.get("consideration") or src.get("amount") or src.get("docAmount") or ""
        legal = src.get("legalDescription") or src.get("legal") or ""
        doc_id = src.get("id") or src.get("instrumentId") or doc_num

        return {
            "doc_num":   str(doc_num),
            "doc_type":  dtype,
            "filed":     parse_date(str(filed_raw)) or str(filed_raw),
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     legal,
            "amount":    parse_amount(amount_raw),
            "clerk_url": f"{BASE_URL}/doc/{doc_id}" if doc_id else BASE_URL,
        }
    except Exception:
        return None


# ── PLAYWRIGHT FALLBACK ───────────────────────────────────────────────────

async def scrape_playwright(date_from: str, date_to: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        return []

    records: list[dict] = []
    log.info("Trying Playwright on PublicSearch …")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        captured = []

        async def handle_response(response):
            try:
                if "publicsearch" in response.url and response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        hits = (data.get("hits", {}).get("hits", []) or
                                data.get("results", []) or [])
                        for hit in hits:
                            src = hit.get("_source", hit)
                            dtype = (src.get("docType") or src.get("instrumentType") or "").upper()
                            if dtype in TARGET_TYPES:
                                rec = parse_publicsearch_record(src, dtype)
                                if rec:
                                    captured.append(rec)
            except Exception:
                pass

        page.on("response", handle_response)

        try:
            await page.goto(BASE_URL, timeout=30_000)
            await page.wait_for_load_state("networkidle", timeout=15_000)

            # Try to find and use the search form
            for doc_code in list(TARGET_TYPES)[:5]:  # Try first 5 to save time
                try:
                    # Look for doc type input
                    for sel in ['input[placeholder*="Type"]', 'input[name*="type"]',
                                'select[name*="type"]']:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.fill(doc_code)
                            break

                    # Look for date fields
                    for sel in ['input[placeholder*="Start"]', 'input[name*="from"]',
                                'input[placeholder*="From"]']:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.fill(date_from)
                            break

                    for sel in ['input[placeholder*="End"]', 'input[name*="to"]',
                                'input[placeholder*="To"]']:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.fill(date_to)
                            break

                    # Submit
                    for sel in ['button[type="submit"]', 'button:has-text("Search")']:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.click()
                            await page.wait_for_load_state("networkidle", timeout=10_000)
                            break

                    await asyncio.sleep(2)
                except Exception:
                    pass

        except Exception:
            log.error(f"Playwright error:\n{traceback.format_exc()}")
        finally:
            await browser.close()

    records.extend(captured)
    log.info(f"Playwright captured {len(records)} records")
    return records


# ── DEMO DATA ─────────────────────────────────────────────────────────────

def generate_demo_records(date_from: str, date_to: str) -> list[dict]:
    samples = [
        ("LP",       "2024-LPJOHN-0001", "SMITH JOHN A",         "FIRST NATIONAL BANK",      125000, "LT 14 BLK 7 OAK MEADOWS"),
        ("NOFC",     "2024-NOFC-0002",   "JONES MARY B",          "MORTGAGE SOLUTIONS LLC",    87500, "LT 3 BLK 2 CLEBURNE HEIGHTS"),
        ("LNIRS",    "2024-IRS-0003",    "WILLIAMS DAVID",        "INTERNAL REVENUE SERVICE",  45200, "TRACT 22 AB 341 JOHNSON CTY"),
        ("JUD",      "2024-JUD-0004",    "GARCIA PROPERTIES LLC", "CLEBURNE SUPPLY CO",        18700, "LT 9 BLK 4 RIVER OAKS ADD"),
        ("LNMECH",   "2024-MECH-0005",   "BROWN PATRICIA",        "LONE STAR CONTRACTORS",     22000, "LT 1 BLK 1 CREEKSIDE EST"),
        ("PRO",      "2024-PRO-0006",    "ESTATE OF DAVIS JAMES", "JOHNSON COUNTY PROBATE",        0, "LT 6 BLK 12 HERITAGE HILLS"),
        ("TAXDEED",  "2024-TAX-0007",    "HENDERSON ROBERT",      "JOHNSON COUNTY TAX",         9800, "LT 17 BLK 3 SUNSET RIDGE"),
        ("LNHOA",    "2024-HOA-0008",    "MARTINEZ CARLOS",       "LAKE RIDGE HOA",             3500, "LT 22 BLK 8 LAKE RIDGE ADD"),
        ("LNCORPTX", "2024-CTX-0009",    "APEX VENTURES LLC",     "TX COMPTROLLER",            67300, "TRACT 5 AB 112 JOHNSON CTY"),
        ("NOC",      "2024-NOC-0010",    "TAYLOR CONSTRUCTION",   "CITYBANK NA",                   0, "LT 4 BLK 2 GRAND PRAIRIE EST"),
    ]
    base = datetime.strptime(date_from, "%m/%d/%Y")
    recs = []
    for i, (dtype, docnum, grantor, grantee, amt, legal) in enumerate(samples):
        filed_dt = base + timedelta(days=i % LOOKBACK_DAYS)
        recs.append({
            "doc_num":   docnum,
            "doc_type":  dtype,
            "filed":     filed_dt.strftime("%Y-%m-%d"),
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     legal,
            "amount":    float(amt) if amt else None,
            "clerk_url": f"{BASE_URL}#demo-{docnum}",
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
    dtype  = rec.get("doc_type", "").upper()
    amount = rec.get("amount") or 0

    if dtype in ("LP", "RELLP"):    flags.append("Lis pendens")
    if dtype == "NOFC":             flags.append("Pre-foreclosure")
    if dtype in ("JUD","CCJ","DRJUD"): flags.append("Judgment lien")
    if dtype in ("LNCORPTX","LNIRS","LNFED","TAXDEED"): flags.append("Tax lien")
    if dtype == "LNMECH":           flags.append("Mechanic lien")
    if dtype == "LNHOA":            flags.append("HOA lien")
    if dtype == "MEDLN":            flags.append("Medical lien")
    if dtype == "PRO":              flags.append("Probate / estate")

    owner = rec.get("grantor", "").upper()
    if any(x in owner for x in ("LLC","INC","CORP","LTD","LP ","L.P.")):
        flags.append("LLC / corp owner")

    try:
        filed = datetime.strptime(rec.get("filed",""), "%Y-%m-%d")
        if (datetime.today() - filed).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    score += 10 * len(flags)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score += 20
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
            dtype = raw.get("doc_type", "").upper()
            cat, cat_label = DOC_TYPE_MAP.get(dtype, ("other", dtype))
            score, flags = score_record(raw)
            out_records.append({
                "doc_num":      raw.get("doc_num", ""),
                "doc_type":     dtype,
                "filed":        raw.get("filed", ""),
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        raw.get("grantor", ""),
                "grantee":      raw.get("grantee", ""),
                "amount":       raw.get("amount"),
                "legal":        raw.get("legal", ""),
                "prop_address": raw.get("prop_address", ""),
                "prop_city":    raw.get("prop_city", ""),
                "prop_state":   raw.get("prop_state", "TX"),
                "prop_zip":     raw.get("prop_zip", ""),
                "mail_address": raw.get("mail_address", ""),
                "mail_city":    raw.get("mail_city", ""),
                "mail_state":   raw.get("mail_state", "TX"),
                "mail_zip":     raw.get("mail_zip", ""),
                "clerk_url":    raw.get("clerk_url", ""),
                "flags":        flags,
                "score":        score,
                "_demo":        raw.get("_demo", False),
            })
        except Exception:
            log.warning(f"Skipping bad record: {traceback.format_exc()}")

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
    log.info(f"GHL CSV saved")


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

    log.info("Scraping PublicSearch API …")
    raw_records = scrape_publicsearch(date_from, date_to)
    log.info(f"  API returned {len(raw_records)} records")

    if not raw_records and HAS_PLAYWRIGHT:
        log.info("Trying Playwright fallback …")
        raw_records = await scrape_playwright(date_from, date_to)

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
