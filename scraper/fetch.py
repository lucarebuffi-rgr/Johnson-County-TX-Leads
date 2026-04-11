#!/usr/bin/env python3
"""
Johnson County TX – Motivated Seller Lead Scraper
Intercepts API calls made by the PublicSearch React app.
"""

import asyncio
import csv
import io
import json
import logging
import re
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

# ── API INTERCEPTOR ───────────────────────────────────────────────────────

def parse_api_response(data: dict, doc_code: str, cat: str, cat_label: str) -> list[dict]:
    """Parse JSON API response from PublicSearch."""
    records = []

    # Try different response structures
    hits = (
        data.get("hits", {}).get("hits", []) or
        data.get("results", []) or
        data.get("documents", []) or
        data.get("instruments", []) or
        []
    )

    if not hits and isinstance(data, list):
        hits = data

    log.info(f"    API response: {len(hits)} hits")

    for hit in hits:
        try:
            src = hit.get("_source", hit)

            # Extract parties
            grantor = ""
            grantee = ""
            parties = src.get("parties", [])
            for party in parties:
                role = str(party.get("role", "")).upper()
                name = str(party.get("name", "")).strip()
                if any(r in role for r in ["GRANTOR","SELLER","OWNER","DEBTOR"]):
                    if not grantor:
                        grantor = name
                elif any(r in role for r in ["GRANTEE","BUYER","CREDITOR","SECURED"]):
                    if not grantee:
                        grantee = name

            # Fallback party fields
            if not grantor:
                grantor = (src.get("grantor") or src.get("grantorName") or
                          src.get("party1Name") or src.get("ownerName") or "")
            if not grantee:
                grantee = (src.get("grantee") or src.get("granteeName") or
                          src.get("party2Name") or "")

            # Date
            filed_raw = (
                src.get("recordedDate") or src.get("filedDate") or
                src.get("instrumentDate") or src.get("date") or
                src.get("bookDate") or ""
            )

            # Doc number
            doc_num = (
                src.get("instrumentNumber") or src.get("docNumber") or
                src.get("documentNumber") or src.get("instrument") or
                src.get("id") or ""
            )

            # Amount
            amount_raw = (
                src.get("consideration") or src.get("amount") or
                src.get("docAmount") or src.get("consideration_amount") or ""
            )
            try:
                amount = float(re.sub(r"[^\d.]", "", str(amount_raw))) if amount_raw else None
            except Exception:
                amount = None

            # Legal
            legal = src.get("legalDescription") or src.get("legal") or ""

            # Link
            doc_id = src.get("id") or src.get("instrumentId") or doc_num
            link = f"{BASE_URL}/doc/{doc_id}" if doc_id else BASE_URL

            if not grantor and not doc_num:
                continue

            records.append({
                "doc_num":   str(doc_num),
                "doc_type":  doc_code,
                "cat":       cat,
                "cat_label": cat_label,
                "filed":     parse_date(str(filed_raw)) or str(filed_raw),
                "grantor":   grantor,
                "grantee":   grantee,
                "legal":     legal,
                "amount":    amount,
                "clerk_url": link,
                "_demo":     False,
            })
        except Exception:
            continue

    return records


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

        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            url = (f"{BASE_URL}/results"
                   f"?department=RP"
                   f"&docTypes={doc_code}"
                   f"&recordedDateRange={dt_from},{dt_to}"
                   f"&searchType=advancedSearch")

            log.info(f"  Scraping {doc_code} ({cat_label}) …")

            captured_responses = []

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            async def handle_response(response):
                try:
                    rurl = response.url
                    # Capture any JSON response that looks like search results
                    if (response.status == 200 and
                        any(k in rurl for k in ["search", "instrument", "document",
                                                 "result", "query", "api", "elastic"])):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            try:
                                data = await response.json()
                                log.info(f"    Captured API: {rurl[:80]}")
                                captured_responses.append((rurl, data))
                            except Exception:
                                pass
                except Exception:
                    pass

            page.on("response", handle_response)

            try:
                await page.goto(url, timeout=30_000)
                # Wait longer for React to load and API calls to complete
                await asyncio.sleep(8)

                # Log what we captured
                log.info(f"    Captured {len(captured_responses)} API responses")
                for rurl, data in captured_responses:
                    log.info(f"      URL: {rurl[:100]}")
                    recs = parse_api_response(data, doc_code, cat, cat_label)
                    all_records.extend(recs)
                    log.info(f"      → {len(recs)} records")

                # If no API captured, try to read from page's JavaScript state
                if not captured_responses:
                    log.info(f"    No API captured, trying JS state …")
                    try:
                        # Try to extract data from React's window state
                        js_data = await page.evaluate("""
                            () => {
                                // Try common React state locations
                                if (window.__INITIAL_STATE__) return JSON.stringify(window.__INITIAL_STATE__);
                                if (window.__REDUX_STATE__) return JSON.stringify(window.__REDUX_STATE__);
                                if (window.__APP_STATE__) return JSON.stringify(window.__APP_STATE__);
                                // Try to find data in React fiber
                                const results = document.querySelectorAll('[class*="result"], [class*="doc-preview"], [class*="instrument"]');
                                const texts = [];
                                results.forEach(el => texts.push(el.innerText));
                                return JSON.stringify({texts: texts});
                            }
                        """)
                        if js_data:
                            data = json.loads(js_data)
                            log.info(f"    JS state keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                            # If we got texts, parse them
                            texts = data.get("texts", []) if isinstance(data, dict) else []
                    for text in texts:
                        if not text:
                            continue
                        # Split by tab - data is tab-separated
                        parts = [p.strip() for p in text.split("\t") if p.strip()]
                        if len(parts) < 4:
                            continue
                        log.info(f"    Parsing text block: {parts[:6]}")
                        try:
                            grantor  = parts[0] if len(parts) > 0 else ""
                            grantee  = parts[1] if len(parts) > 1 else ""
                            doc_type_raw = parts[2] if len(parts) > 2 else doc_code
                            filed_raw = parts[3] if len(parts) > 3 else ""
                            doc_num   = parts[4] if len(parts) > 4 else ""
                            legal     = parts[6] if len(parts) > 6 else ""

                            if not grantor:
                                continue

                            all_records.append({
                                "doc_num":   doc_num,
                                "doc_type":  doc_code,
                                "cat":       cat,
                                "cat_label": cat_label,
                                "filed":     parse_date(filed_raw) or filed_raw,
                                "grantor":   grantor,
                                "grantee":   grantee,
                                "legal":     legal,
                                "amount":    None,
                                "clerk_url": f"{BASE_URL}/results?department=RP&docTypes={doc_code}&recordedDateRange={dt_from},{dt_to}&searchType=advancedSearch",
                                "_demo":     False,
                            })
                        except Exception as e:
                            log.warning(f"    Parse error: {e}")
                            continue
                    except Exception as e:
                        log.warning(f"    JS state error: {e}")

            except Exception as e:
                log.warning(f"    Page error for {doc_code}: {e}")
            finally:
                await page.close()
                await context.close()

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

    log.info("Scraping with Playwright (API intercept) …")
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
