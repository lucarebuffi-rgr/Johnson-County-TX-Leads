#!/usr/bin/env python3
"""
Johnson County TX – Motivated Seller Lead Scraper
Intercepts API calls and parses JS state from PublicSearch React app.
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

# For these types the GRANTEE is the property owner
GRANTEE_IS_OWNER = {"NooLe", "Lie", "HoLe", "ChSLe", "FeTLe", "StTLe", "Jun", "AboJn", "LiPn", "ReoLPn"}

LOOKBACK_DAYS   = 14
REQUEST_TIMEOUT = 60


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
    zip_data = None

    for url in [
        "https://johnsoncad.com/wp-content/uploads/2026/04/JCAD-2026-Preliminary-Data-2026-04-06.zip",
        "https://www.johnsoncad.com/data/download/property.zip",
        "https://www.johnsoncad.com/data/download/parcel.zip",
    ]:
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                zip_data = r.content
                log.info(f"Got parcel data from {url}")
                break
        except Exception:
            continue

    if not zip_data:
        log.warning("Parcel data not available")
        return lookup

    try:
        with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
            log.info(f"ZIP contents: {z.namelist()}")

            # ── Read owner file (externalowner.tab) ──────────────────────
            owner_rows: dict[str, dict] = {}
            owner_file = next((n for n in z.namelist() if "owner" in n.lower()), None)
            if owner_file:
                log.info(f"Reading owner file: {owner_file}")
                lines = z.read(owner_file).decode("latin-1").splitlines()
                log.info(f"Owner file: {len(lines)} lines")
                if lines:
                    hdrs = [h.strip().upper() for h in lines[0].split("\t")]
                    log.info(f"Owner headers: {hdrs}")
                    for line in lines[1:]:
                        parts = line.split("\t")
                        row = {hdrs[i]: parts[i].strip() for i in range(min(len(hdrs), len(parts)))}
                        key = row.get("PARCEL_ID") or row.get("PROP_ID") or row.get("ACCOUNT") or ""
                        if key:
                            owner_rows[key] = row

            # ── Read NAL file (externalnal.tab) – Name Address Legal ──────
            nal_rows: dict[str, dict] = {}
            nal_file = next((n for n in z.namelist() if n.lower() == "externalnal.tab"), None)
            if not nal_file:
                nal_file = next((n for n in z.namelist() if "nal" in n.lower()), None)
            if nal_file:
                log.info(f"Reading NAL file: {nal_file}")
                lines = z.read(nal_file).decode("latin-1").splitlines()
                log.info(f"NAL file: {len(lines)} lines")
                if lines:
                    hdrs = [h.strip().upper() for h in lines[0].split("\t")]
                    log.info(f"NAL headers: {hdrs}")
                    if len(lines) > 1:
                        log.info(f"NAL sample: {lines[1][:300]}")
                    for line in lines[1:]:
                        parts = line.split("\t")
                        row = {hdrs[i]: parts[i].strip() for i in range(min(len(hdrs), len(parts)))}
                        key = row.get("PARCEL_ID") or row.get("PROP_ID") or row.get("ACCOUNT") or ""
                        if key:
                            nal_rows[key] = row

            # ── Read all other tab files to find address columns ───────────
            all_rows: dict[str, dict] = {}
            for fname in z.namelist():
                if fname.lower().endswith(".tab") and fname not in [owner_file, nal_file]:
                    try:
                        lines = z.read(fname).decode("latin-1").splitlines()
                        if not lines:
                            continue
                        hdrs = [h.strip().upper() for h in lines[0].split("\t")]
                        # Only process if it has address-like columns
                        addr_cols = [h for h in hdrs if any(k in h for k in
                                     ["ADDR", "SITUS", "STREET", "CITY", "ZIP", "STATE"])]
                        if addr_cols:
                            log.info(f"Found address cols in {fname}: {addr_cols}")
                            for line in lines[1:]:
                                parts = line.split("\t")
                                row = {hdrs[i]: parts[i].strip() for i in range(min(len(hdrs), len(parts)))}
                                key = row.get("PARCEL_ID") or row.get("PROP_ID") or row.get("ACCOUNT") or ""
                                if key:
                                    all_rows.setdefault(key, {}).update(row)
                    except Exception:
                        continue

            log.info(f"owner_rows: {len(owner_rows)}, nal_rows: {len(nal_rows)}, all_rows: {len(all_rows)}")

            # ── Build lookup by owner name ────────────────────────────────
            for key, orow in owner_rows.items():
                owner_name = (
                    orow.get("OWN_NAME") or orow.get("NAME") or
                    orow.get("OWNER") or orow.get("OWNER_NAME") or ""
                ).upper().strip()

                if not owner_name:
                    continue

                nrow = nal_rows.get(key, {})
                arow = all_rows.get(key, {})

                # Merge all available data
                merged = {**arow, **nrow, **orow}

                # Property address
                prop_address = (
                    merged.get("SITUS_NUM","") + " " + merged.get("SITUS_STREET","")
                ).strip()
                if not prop_address:
                    prop_address = (
                        merged.get("SITUS","") or merged.get("SITE_ADDR","") or
                        merged.get("PROP_ADDR","") or merged.get("ADDRESS","") or
                        merged.get("STREET_ADDR","") or ""
                    )

                prop_city = (
                    merged.get("SITUS_CITY","") or merged.get("SITE_CITY","") or
                    merged.get("PROP_CITY","") or "Cleburne"
                )
                prop_zip = (
                    merged.get("SITUS_ZIP","") or merged.get("SITE_ZIP","") or
                    merged.get("PROP_ZIP","") or ""
                )

                # Mailing address
                mail_address = (
                    merged.get("ADDR1","") or merged.get("MAIL_ADDR","") or
                    merged.get("MAILING_ADDRESS","") or merged.get("MAIL_ADDRESS","") or
                    merged.get("ADDRESS1","") or ""
                )
                mail_city = (
                    merged.get("CITY","") or merged.get("MAIL_CITY","") or
                    merged.get("MAILING_CITY","") or ""
                )
                mail_state = (
                    merged.get("STATE","") or merged.get("MAIL_STATE","") or "TX"
                )
                mail_zip = (
                    merged.get("ZIP","") or merged.get("MAIL_ZIP","") or
                    merged.get("MAILING_ZIP","") or ""
                )

                parcel = {
                    "prop_address": prop_address,
                    "prop_city":    prop_city,
                    "prop_state":   "TX",
                    "prop_zip":     prop_zip,
                    "mail_address": mail_address,
                    "mail_city":    mail_city,
                    "mail_state":   mail_state,
                    "mail_zip":     mail_zip,
                }

                for variant in name_variants(owner_name):
                    lookup[variant] = parcel

            log.info(f"Built parcel lookup: {len(lookup):,} name variants")

    except Exception:
        log.error(f"Parcel error:\n{traceback.format_exc()}")

    return lookup


# ── TEXT BLOCK PARSER ─────────────────────────────────────────────────────

def parse_text_block(text: str, doc_code: str, cat: str, cat_label: str, dt_from: str, dt_to: str) -> Optional[dict]:
    try:
        if not text:
            return None
        parts = [p.strip() for p in text.split("\t")]
        parts = [p for p in parts if p]
        if len(parts) < 3:
            return None

        grantor   = parts[0] if len(parts) > 0 else ""
        grantee   = parts[1] if len(parts) > 1 else ""
        filed_raw = ""
        doc_num   = ""
        legal     = ""

        for i, p in enumerate(parts):
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", p):
                filed_raw = p
                doc_num   = parts[i + 1] if i + 1 < len(parts) else ""
                legal     = parts[i + 3] if i + 3 < len(parts) else ""
                break

        if not grantor:
            return None

        search_url = (f"{BASE_URL}/results?department=RP&docTypes={doc_code}"
                      f"&recordedDateRange={dt_from},{dt_to}&searchType=advancedSearch")

        return {
            "doc_num":   doc_num,
            "doc_type":  doc_code,
            "cat":       cat,
            "cat_label": cat_label,
            "filed":     parse_date(filed_raw) or filed_raw,
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     legal,
            "amount":    None,
            "clerk_url": search_url,
            "_demo":     False,
        }
    except Exception:
        return None


# ── PLAYWRIGHT SCRAPER ────────────────────────────────────────────────────

async def scrape_all_playwright(date_from: str, date_to: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.error("Playwright not available!")
        return []

    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y%m%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y%m%d")
    except Exception:
        dt_from = date_from.replace("/", "")
        dt_to   = date_to.replace("/", "")

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

            captured_api: list[dict] = []
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            async def handle_response(response):
                try:
                    rurl = response.url
                    if response.status == 200 and any(
                        k in rurl for k in ["search", "instrument", "document",
                                            "result", "query", "api", "elastic"]
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            captured_api.append(data)
                            log.info(f"    Captured API: {rurl[:80]}")
                except Exception:
                    pass

            page.on("response", handle_response)

            try:
                await page.goto(url, timeout=30_000)
                await asyncio.sleep(8)

                for data in captured_api:
                    hits = (
                        data.get("hits", {}).get("hits", []) or
                        data.get("results", []) or
                        data.get("documents", []) or
                        []
                    )
                    if hits:
                        log.info(f"    API hits: {len(hits)}")
                        for hit in hits:
                            src = hit.get("_source", hit)
                            grantor = ""
                            grantee = ""
                            for party in src.get("parties", []):
                                role = str(party.get("role", "")).upper()
                                name = str(party.get("name", "")).strip()
                                if any(r in role for r in ["GRANTOR", "SELLER", "DEBTOR", "OWNER"]):
                                    grantor = grantor or name
                                else:
                                    grantee = grantee or name
                            if not grantor:
                                grantor = src.get("grantor") or src.get("grantorName") or ""
                            if not grantee:
                                grantee = src.get("grantee") or src.get("granteeName") or ""

                            filed_raw = (src.get("recordedDate") or src.get("filedDate") or
                                        src.get("instrumentDate") or "")
                            doc_num   = (src.get("instrumentNumber") or src.get("docNumber") or
                                        src.get("id") or "")
                            legal     = src.get("legalDescription") or src.get("legal") or ""
                            doc_id    = src.get("id") or doc_num

                            if grantor or doc_num:
                                all_records.append({
                                    "doc_num":   str(doc_num),
                                    "doc_type":  doc_code,
                                    "cat":       cat,
                                    "cat_label": cat_label,
                                    "filed":     parse_date(str(filed_raw)) or str(filed_raw),
                                    "grantor":   grantor,
                                    "grantee":   grantee,
                                    "legal":     legal,
                                    "amount":    None,
                                    "clerk_url": f"{BASE_URL}/doc/{doc_id}" if doc_id else url,
                                    "_demo":     False,
                                })

                if not captured_api or not all_records:
                    log.info(f"    No API data, trying JS state …")
                    js_result = await page.evaluate("""
                        () => {
                            const texts = [];
                            const rows = document.querySelectorAll('tbody tr');
                            if (rows.length > 0) {
                                rows.forEach(row => {
                                    const cells = row.querySelectorAll('td');
                                    if (cells.length >= 4) {
                                        const parts = [];
                                        cells.forEach(td => parts.push(td.innerText.trim()));
                                        texts.push(parts.join('\\t'));
                                    }
                                });
                                return texts;
                            }
                            const items = document.querySelectorAll(
                                '[class*="group-item"], [class*="doc-row"], [class*="instrument-row"]'
                            );
                            items.forEach(el => {
                                const cells = el.querySelectorAll('[class*="cell"], [class*="col"], td, [class*="field"]');
                                if (cells.length >= 3) {
                                    const parts = [];
                                    cells.forEach(c => parts.push(c.innerText.trim()));
                                    texts.push(parts.join('\\t'));
                                } else {
                                    const t = el.innerText.trim();
                                    if (t && t.length > 10 && t.length < 500) texts.push(t);
                                }
                            });
                            return texts;
                        }
                    """)

                    if js_result:
                        log.info(f"    JS returned {len(js_result)} text blocks")
                        for text in js_result:
                            rec = parse_text_block(text, doc_code, cat, cat_label, dt_from, dt_to)
                            if rec:
                                all_records.append(rec)
                    else:
                        log.info(f"    JS returned nothing")

            except Exception as e:
                log.warning(f"    Error for {doc_code}: {e}")
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
    matched = 0
    for rec in records:
        owner = rec.get("grantor", "").upper().strip()
        parcel = None
        for variant in name_variants(owner):
            parcel = lookup.get(variant)
            if parcel:
                break
        if parcel:
            rec.update(parcel)
            matched += 1
        else:
            rec.setdefault("prop_address", "")
            rec.setdefault("prop_city",    "")
            rec.setdefault("prop_state",   "TX")
            rec.setdefault("prop_zip",     "")
            rec.setdefault("mail_address", "")
            rec.setdefault("mail_city",    "")
            rec.setdefault("mail_state",   "TX")
            rec.setdefault("mail_zip",     "")
    log.info(f"Parcel enrichment: {matched}/{len(records)} records matched")
    return records


# ── SCORING ───────────────────────────────────────────────────────────────

def score_record(rec: dict) -> tuple[int, list[str]]:
    score = 30
    flags: list[str] = []
    dtype  = rec.get("doc_type", "")
    amount = rec.get("amount") or 0

    if dtype in ("LiPn", "ReoLPn"): flags.append("Lis pendens")
    if dtype in ("FeTLe", "StTLe"): flags.append("Tax lien")
    if dtype in ("Jun", "AboJn"):   flags.append("Judgment lien")
    if dtype == "Prt":   flags.append("Probate / estate")
    if dtype == "MeLCc": flags.append("Mechanic lien")
    if dtype == "NooLe": flags.append("Notice of lien")
    if dtype == "HoLe":  flags.append("Hospital lien")
    if dtype == "ChSLe": flags.append("Child support lien")
    if dtype == "Lie":   flags.append("Lien")

    owner = rec.get("owner", "").upper()
    if any(x in owner for x in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.")):
        flags.append("LLC / corp owner")

    try:
        filed = datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")
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
            dtype = raw.get("doc_type", "")

            if dtype in GRANTEE_IS_OWNER:
                owner   = raw.get("grantee", "")
                grantee = raw.get("grantor", "")
            else:
                owner   = raw.get("grantor", "")
                grantee = raw.get("grantee", "")

            score, flags = score_record({**raw, "owner": owner})

            out_records.append({
                "doc_num":      raw.get("doc_num", ""),
                "doc_type":     dtype,
                "filed":        raw.get("filed", ""),
                "cat":          raw.get("cat", "other"),
                "cat_label":    raw.get("cat_label", ""),
                "owner":        owner,
                "grantee":      grantee,
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
            log.warning(f"Skipping: {traceback.format_exc()}")

    out_records.sort(key=lambda r: (-r["score"], r.get("filed", "") or ""))
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
        "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in data["records"]:
        parts = (r.get("owner", "")).split()
        writer.writerow({
            "First Name":             parts[0] if parts else "",
            "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
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

    log.info("Scraping with Playwright (API intercept + JS state) …")
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
