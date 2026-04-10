#!/usr/bin/env python3
"""
Johnson County TX – Motivated Seller Lead Scraper
"""

import asyncio
import csv
import io
import json
import logging
import os
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
    logging.warning("playwright not installed – clerk portal scraping disabled")

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    logging.warning("dbfread not installed – parcel lookup disabled")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CLERK_PORTAL = "https://www.johnsoncountytx.org/government/county-clerk/online-records"

CLERK_SEARCH_URLS = [
    "https://countyclerk.johnsoncountytx.org/",
    "https://www.corelogic.com/solutions/",
    "https://texassearch.net/johnsoncounty/",
]

CAD_BULK_URLS = [
    "https://www.johnsoncad.com/data/download/",
    "https://www.johnsoncad.com/downloads/",
    "https://johnsoncad.com/appraisaldata/",
]

LOOKBACK_DAYS = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY = 3
REQUEST_TIMEOUT = 30
HEADLESS = True

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

def safe_get(url: str, session: requests.Session, **kwargs) -> Optional[requests.Response]:
    def _get():
        r = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        r.raise_for_status()
        return r
    return retry(_get)

def parse_amount(raw: str) -> Optional[float]:
    try:
        cleaned = re.sub(r"[^\d.]", "", str(raw))
        return float(cleaned) if cleaned else None
    except Exception:
        return None

def parse_date(raw: str) -> Optional[str]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
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
    return [
        full,
        f"{last} {first}",
        f"{last}, {first}",
        f"{last},{first}",
    ]

def build_parcel_lookup() -> dict:
    lookup: dict[str, dict] = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"})

    dbf_data: Optional[bytes] = None

    for cad_url in CAD_BULK_URLS:
        log.info(f"Trying CAD bulk data at {cad_url}")
        resp = safe_get(cad_url, session)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if ".dbf" in href or ("property" in href and ".zip" in href) or "parcel" in href:
                full = a["href"] if a["href"].startswith("http") else cad_url.rstrip("/") + "/" + a["href"].lstrip("/")
                log.info(f"  Found candidate: {full}")
                r = safe_get(full, session)
                if r:
                    dbf_data = r.content
                    break
        if dbf_data:
            break

    if not dbf_data:
        guesses = [
            "https://www.johnsoncad.com/data/download/property.zip",
            "https://www.johnsoncad.com/data/download/parcel.zip",
            "https://johnsoncad.com/downloads/JohnsonCAD_Parcels.zip",
            "https://johnsoncad.com/downloads/parcel_data.zip",
            "https://www.johnsoncad.com/appraisaldata/Export.zip",
        ]
        for url in guesses:
            log.info(f"Trying CAD guess: {url}")
            r = safe_get(url, session)
            if r and len(r.content) > 1000:
                dbf_data = r.content
                log.info(f"  Got data from {url}")
                break

    if not dbf_data:
        log.warning("Could not retrieve CAD parcel data – address enrichment disabled")
        return lookup

    try:
        if dbf_data[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(dbf_data)) as z:
                for name in z.namelist():
                    if name.lower().endswith(".dbf"):
                        dbf_data = z.read(name)
                        log.info(f"  Extracted DBF: {name}")
                        break

        if not HAS_DBF:
            log.warning("dbfread not available – cannot parse DBF")
            return lookup

        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(dbf_data)
        records = list(DBF(str(tmp), load=True, ignore_missing_memofile=True))
        log.info(f"  Loaded {len(records):,} parcel records")

        for rec in records:
            rec = {k.upper(): (v.strip() if isinstance(v, str) else v) for k, v in rec.items()}
            owner = (rec.get("OWNER") or rec.get("OWN1") or rec.get("OWNER1") or "").upper().strip()
            if not owner:
                continue
            parcel = {
                "prop_address": rec.get("SITE_ADDR") or rec.get("SITEADDR") or rec.get("SITE_ADDRESS") or "",
                "prop_city":    rec.get("SITE_CITY") or rec.get("SITECITY") or "Cleburne",
                "prop_state":   rec.get("SITE_STATE") or "TX",
                "prop_zip":     str(rec.get("SITE_ZIP") or rec.get("SITEZIP") or ""),
                "mail_address": rec.get("ADDR_1") or rec.get("MAILADR1") or rec.get("MAIL_ADDR") or "",
                "mail_city":    rec.get("CITY") or rec.get("MAILCITY") or rec.get("MAIL_CITY") or "",
                "mail_state":   rec.get("STATE") or rec.get("MAILSTATE") or "TX",
                "mail_zip":     str(rec.get("ZIP") or rec.get("MAILZIP") or rec.get("MAIL_ZIP") or ""),
            }
            for variant in name_variants(owner):
                lookup[variant] = parcel

    except Exception:
        log.error(f"Error parsing parcel data:\n{traceback.format_exc()}")

    return lookup

async def scrape_clerk_playwright(date_from: str, date_to: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.warning("Playwright unavailable – skipping clerk scrape")
        return []

    records: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            log.info(f"Loading clerk portal: {CLERK_PORTAL}")
            await page.goto(CLERK_PORTAL, timeout=60_000)
            await page.wait_for_load_state("networkidle", timeout=30_000)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            iframe = soup.find("iframe")
            embed_link = None
            if iframe and iframe.get("src"):
                embed_link = iframe["src"]

            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                if any(k in href for k in ["search", "records", "clerkrecords",
                                            "idocvault", "inpho", "tyler", "granicus"]):
                    embed_link = a["href"]
                    break

            search_url = embed_link or CLERK_PORTAL
            if search_url != CLERK_PORTAL:
                log.info(f"Following to search portal: {search_url}")
                await page.goto(search_url, timeout=60_000)
                await page.wait_for_load_state("networkidle", timeout=30_000)

            for doc_code in TARGET_TYPES:
                try:
                    recs = await _search_one_type(page, doc_code, date_from, date_to)
                    records.extend(recs)
                    log.info(f"  {doc_code}: {len(recs)} records")
                except Exception as exc:
                    log.warning(f"  {doc_code} search failed: {exc}")

        except Exception:
            log.error(f"Clerk playwright error:\n{traceback.format_exc()}")
        finally:
            await browser.close()

    return records

async def _search_one_type(page, doc_code: str, date_from: str, date_to: str) -> list[dict]:
    records: list[dict] = []

    selectors_doctype = [
        'select[name*="DocType"]', 'select[id*="doctype"]',
        'select[name*="InstrType"]', 'select[id*="instrument"]',
        'input[name*="doctype"]', 'input[placeholder*="Document Type"]',
    ]
    for sel in selectors_doctype:
        el = page.locator(sel).first
        if await el.count() > 0:
            tag = await el.evaluate("el => el.tagName.toLowerCase()")
            if tag == "select":
                try:
                    await el.select_option(value=doc_code)
                except Exception:
                    try:
                        await el.select_option(label=doc_code)
                    except Exception:
                        pass
            else:
                await el.fill(doc_code)
            break

    for sel in ['input[name*="DateFrom"]', 'input[id*="dateFrom"]',
                'input[name*="StartDate"]', 'input[placeholder*="From"]']:
        el = page.locator(sel).first
        if await el.count() > 0:
            await el.fill(date_from)
            break

    for sel in ['input[name*="DateTo"]', 'input[id*="dateTo"]',
                'input[name*="EndDate"]', 'input[placeholder*="To"]']:
        el = page.locator(sel).first
        if await el.count() > 0:
            await el.fill(date_to)
            break

    for sel in ['button[type="submit"]', 'input[type="submit"]',
                'button:has-text("Search")', 'a:has-text("Search")']:
        el = page.locator(sel).first
        if await el.count() > 0:
            await el.click()
            break

    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass

    html = await page.content()
    records = _parse_results_table(html, doc_code, page.url)

    page_num = 1
    while True:
        next_sel = page.locator('a:has-text("Next"), a[title="Next Page"], .pagination-next').first
        if await next_sel.count() == 0:
            break
        await next_sel.click()
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except PWTimeout:
            break
        html = await page.content()
        new_recs = _parse_results_table(html, doc_code, page.url)
        if not new_recs:
            break
        records.extend(new_recs)
        page_num += 1
        if page_num > 50:
            break

    return records

def _parse_results_table(html: str, doc_code: str, base_url: str) -> list[dict]:
    records: list[dict] = []
    soup = BeautifulSoup(html, "lxml")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not headers:
            continue

        def col(row_cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(row_cells):
                        return row_cells[i].get_text(strip=True)
            return ""

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue

            dtype_raw = col(cells, "type", "doc", "instrument")
            if dtype_raw and dtype_raw.upper() not in TARGET_TYPES:
                if any(t in dtype_raw.upper() for t in TARGET_TYPES):
                    pass
                else:
                    continue

            link = ""
            for a in row.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    from urllib.parse import urlparse
                    p = urlparse(base_url)
                    link = f"{p.scheme}://{p.netloc}{href}"
                break

            doc_num   = col(cells, "doc#", "docnum", "instrument#", "number", "doc no")
            filed_raw = col(cells, "filed", "record", "date")
            grantor   = col(cells, "grantor", "owner", "from")
            grantee   = col(cells, "grantee", "to", "party")
            legal     = col(cells, "legal", "description")
            amount    = col(cells, "amount", "consideration")

            rec = {
                "doc_num":   doc_num,
                "doc_type":  dtype_raw.upper() if dtype_raw else doc_code,
                "filed":     parse_date(filed_raw) or filed_raw,
                "grantor":   grantor,
                "grantee":   grantee,
                "legal":     legal,
                "amount":    parse_amount(amount),
                "clerk_url": link,
            }
            records.append(rec)

    return records

def scrape_clerk_requests(date_from: str, date_to: str) -> list[dict]:
    records: list[dict] = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; JohnsonCountyLeadScraper/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    resp = safe_get(CLERK_PORTAL, session)
    if not resp:
        log.error("Cannot reach clerk portal via requests")
        return records

    soup = BeautifulSoup(resp.text, "lxml")

    search_base = CLERK_PORTAL
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href.lower() for k in ["clerkrecords", "search", "idocvault",
                                             "odyseyfiles", "tyler"]):
            if href.startswith("http"):
                search_base = href
            break

    iframes = soup.find_all("iframe")
    if iframes and iframes[0].get("src"):
        search_base = iframes[0]["src"]

    log.info(f"Requests-based search using: {search_base}")

    viewstate    = soup.find("input", {"name": "__VIEWSTATE"})
    eventval     = soup.find("input", {"name": "__EVENTVALIDATION"})
    viewstategen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})

    vs  = viewstate["value"]    if viewstate    else ""
    ev  = eventval["value"]     if eventval     else ""
    vsg = viewstategen["value"] if viewstategen else ""

    for doc_code in TARGET_TYPES:
        log.info(f"  Requesting {doc_code} …")
        payload = {
            "__EVENTTARGET":        "",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          vs,
            "__EVENTVALIDATION":    ev,
            "__VIEWSTATEGENERATOR": vsg,
            "ctl00$MainContent$ddlDocType":   doc_code,
            "ctl00$MainContent$txtDateFrom":  date_from,
            "ctl00$MainContent$txtDateTo":    date_to,
            "ctl00$MainContent$btnSearch":    "Search",
            "DocType":   doc_code,
            "DateFrom":  date_from,
            "DateTo":    date_to,
        }

        def _post():
            r = session.post(search_base, data=payload, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r

        r = retry(_post)
        if not r:
            continue

        recs = _parse_results_table(r.text, doc_code, search_base)
        records.extend(recs)
        log.info(f"    → {len(recs)} records")
        time.sleep(1)

    return records

def generate_demo_records(date_from: str, date_to: str) -> list[dict]:
    samples = [
        ("LP",       "2024-LPJOHN-0001", "SMITH JOHN A",         "FIRST NATIONAL BANK",      125000, "LT 14 BLK 7 OAK MEADOWS"),
        ("NOFC",     "2024-NOFC-0002",   "JONES MARY B",          "MORTGAGE SOLUTIONS LLC",    87500, "LT 3 BLK 2 CLEBURNE HEIGHTS"),
        ("LNIRS",    "2024-IRS-0003",    "WILLIAMS DAVID",        "INTERNAL REVENUE SERVICE",  45200, "TRACT 22 AB 341 JOHNSON CTY"),
        ("JUD",      "2024-JUD-0004",    "GARCIA PROPERTIES LLC", "CLEBURNE SUPPLY CO",        18700, "LT 9 BLK 4 RIVER OAKS ADD"),
        ("LNMECH",   "2024-MECH-0005",  "BROWN PATRICIA",        "LONE STAR CONTRACTORS",     22000, "LT 1 BLK 1 CREEKSIDE EST"),
        ("PRO",      "2024-PRO-0006",    "ESTATE OF DAVIS JAMES", "JOHNSON COUNTY PROBATE",        0, "LT 6 BLK 12 HERITAGE HILLS"),
        ("TAXDEED",  "2024-TAX-0007",   "HENDERSON ROBERT",      "JOHNSON COUNTY TAX",         9800, "LT 17 BLK 3 SUNSET RIDGE"),
        ("LNHOA",    "2024-HOA-0008",   "MARTINEZ CARLOS",       "LAKE RIDGE HOA",             3500, "LT 22 BLK 8 LAKE RIDGE ADD"),
        ("LNCORPTX", "2024-CTX-0009",   "APEX VENTURES LLC",     "TX COMPTROLLER",            67300, "TRACT 5 AB 112 JOHNSON CTY"),
        ("NOC",      "2024-NOC-0010",   "TAYLOR CONSTRUCTION",   "CITYBANK NA",                   0, "LT 4 BLK 2 GRAND PRAIRIE EST"),
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
            "clerk_url": f"https://www.johnsoncountytx.org/government/county-clerk/online-records#demo-{docnum}",
            "_demo":     True,
        })
    return recs

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

def score_record(rec: dict) -> tuple[int, list[str]]:
    score = 30
    flags: list[str] = []
    dtype = rec.get("doc_type", "").upper()
    amount = rec.get("amount") or 0

    if dtype in ("LP", "RELLP"):
        flags.append("Lis pendens")
    if dtype == "NOFC":
        flags.append("Pre-foreclosure")
    if dtype in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if dtype in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if dtype == "LNMECH":
        flags.append("Mechanic lien")
    if dtype == "LNHOA":
        flags.append("HOA lien")
    if dtype == "MEDLN":
        flags.append("Medical lien")
    if dtype == "PRO":
        flags.append("Probate / estate")

    owner = rec.get("grantor", "").upper()
    if any(x in owner for x in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.")):
        flags.append("LLC / corp owner")

    try:
        filed = datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")
        if (datetime.today() - filed).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    score += 10 * len(flags)

    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount and amount > 100_000:
        score += 15
    elif amount and amount > 50_000:
        score += 10
    if "New this week" in flags:
        score += 5
    if has_addr:
        score += 5

    score = min(score, 100)
    return score, flags

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

    out_records.sort(key=lambda r: (-r["score"], r.get("filed", "") or ""))
    with_address = sum(1 for r in out_records if r["prop_address"] or r["mail_address"])

    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Johnson County TX Clerk + CAD",
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

def export_ghl_csv(data: dict) -> str:
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()

    for r in data["records"]:
        owner = r.get("owner", "")
        parts = owner.split() if owner else ["", ""]
        first = parts[0] if len(parts) > 0 else ""
        last  = " ".join(parts[1:]) if len(parts) > 1 else ""
        writer.writerow({
            "First Name":             first,
            "Last Name":              last,
            "Mailing Address":        r.get("mail_address", ""),
            "Mailing City":           r.get("mail_city", ""),
            "Mailing State":          r.get("mail_state", "TX"),
            "Mailing Zip":            r.get("mail_zip", ""),
            "Property Address":       r.get("prop_address", ""),
            "Property City":          r.get("prop_city", ""),
            "Property State":         r.get("prop_state", "TX"),
            "Property Zip":           r.get("prop_zip", ""),
            "Lead Type":              r.get("cat_label", ""),
            "Document Type":          r.get("doc_type", ""),
            "Date Filed":             r.get("filed", ""),
            "Document Number":        r.get("doc_num", ""),
            "Amount/Debt Owed":       str(r.get("amount", "") or ""),
            "Seller Score":           str(r.get("score", "")),
            "Motivated Seller Flags": "|".join(r.get("flags", [])),
            "Source":                 "Johnson County TX",
            "Public Records URL":     r.get("clerk_url", ""),
        })

    csv_str = buf.getvalue()
    p = Path("data/ghl_export.csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(csv_str)
    log.info(f"GHL CSV → data/ghl_export.csv ({len(data['records'])} rows)")
    return csv_str

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

    log.info("Scraping clerk portal …")
    raw_records: list[dict] = []

    if HAS_PLAYWRIGHT:
        try:
            raw_records = await scrape_clerk_playwright(date_from, date_to)
        except Exception:
            log.error(f"Playwright scrape failed:\n{traceback.format_exc()}")

    if not raw_records:
        log.info("Falling back to requests-based scraper …")
        raw_records = scrape_clerk_requests(date_from, date_to)

    if not raw_records:
        log.warning("No live records obtained – using demo seed data")
        raw_records = generate_demo_records(date_from, date_to)

    log.info(f"Total raw records: {len(raw_records)}")

    log.info("Enriching with parcel data …")
    raw_records = enrich_with_parcel(raw_records, parcel_lookup)

    data = build_output(raw_records, date_from, date_to)
    save_output(data)
    export_ghl_csv(data)

    log.info(f"Done. {data['total']} leads | {data['with_address']} with address")
    log.info(f"Top score: {data['records'][0]['score'] if data['records'] else 'N/A'}")

if __name__ == "__main__":
    asyncio.run(main())
