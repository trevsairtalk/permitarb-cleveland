import os
import json
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, request, render_template

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("permitarb")

app = Flask(__name__, static_folder="static", template_folder="templates")

# -------------------------------------------------------------------
# ENV
# -------------------------------------------------------------------
# Option A: Socrata (optional)
SOCRATA_ENDPOINT = os.environ.get("SOCRATA_ENDPOINT", "").strip()  # e.g. https://data.clevelandohio.gov/resource/abcd-1234.json
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "").strip()

# Option B: ArcGIS FeatureServer Layer (recommended for Cleveland)
# Cleveland verified FeatureServer base:
# https://services2.arcgis.com/CyVvlIiUfRBmMQuu/arcgis/rest/services/Building_Permits_Applications_view/FeatureServer
ARCGIS_FEATURESERVER_URL = os.environ.get(
    "ARCGIS_FEATURESERVER_URL",
    "https://services2.arcgis.com/CyVvlIiUfRBmMQuu/arcgis/rest/services/Building_Permits_Applications_view/FeatureServer/0"
).strip()

CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "21600"))  # 6 hours
DEFAULT_DAYS = int(os.environ.get("DEFAULT_DAYS", "60"))
DEFAULT_MIN_VALUE = int(os.environ.get("DEFAULT_MIN_VALUE", "500000"))

MAX_LIMIT = 5000
PAGE_SIZE = 1000

# In-memory cache: { key: (timestamp_utc, payload_dict) }
CACHE = {}

# -------------------------------------------------------------------
# UTIL
# -------------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)

def safe_int(x, default=0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(default)

def norm_text(x) -> str:
    if x is None:
        return ""
    return str(x).strip()

def iso_date_days_ago(days_back: int) -> str:
    d = (now_utc() - timedelta(days=days_back)).date()
    return d.isoformat()

def cache_get(key: str):
    item = CACHE.get(key)
    if not item:
        return None, None
    ts, val = item
    age = (now_utc() - ts).total_seconds()
    return val, age

def cache_set(key: str, val):
    CACHE[key] = (now_utc(), val)

def choose_backend() -> str:
    if ARCGIS_FEATURESERVER_URL:
        return "arcgis"
    if SOCRATA_ENDPOINT:
        return "socrata"
    return "none"

# -------------------------------------------------------------------
# FIELD AUTO-DETECTION
# -------------------------------------------------------------------
PERMIT_ID_CANDIDATES = [
    "permit_number", "permitno", "permit_id", "permit", "application_number",
    "job_number", "OBJECTID", "objectid", "id"
]
DATE_CANDIDATES = [
    "issue_date", "issued_date", "permit_issued_date", "issuedate", "issue_dt",
    "date_issued", "ISSUE_DATE", "ISSUED_DATE", "issued"
]
VALUE_CANDIDATES = [
    "estimated_cost", "declared_valuation", "valuation", "job_cost", "estimated_value",
    "ESTIMATED_COST", "VALUATION", "estimatedcost"
]
DESC_CANDIDATES = [
    "work_description", "description", "scope_of_work", "work_desc", "project_description",
    "WORK_DESCRIPTION", "WORKDESC"
]
STATUS_CANDIDATES = [
    "status", "permit_status", "current_status", "application_status", "STATUS"
]

def detect_fields(sample_rows: list[dict]) -> dict:
    keys = set()
    for r in sample_rows[:60]:
        keys |= set(r.keys())

    lower_map = {k.lower(): k for k in keys}

    def pick(cands):
        for c in cands:
            if c in keys:
                return c
        for c in cands:
            if c.lower() in lower_map:
                return lower_map[c.lower()]
        return ""

    return {
        "permit_id": pick(PERMIT_ID_CANDIDATES),
        "issue_date": pick(DATE_CANDIDATES),
        "value": pick(VALUE_CANDIDATES),
        "desc": pick(DESC_CANDIDATES),
        "status": pick(STATUS_CANDIDATES),
    }

# -------------------------------------------------------------------
# FETCH: SOCRATA (paged)
# -------------------------------------------------------------------
def socrata_get(url: str, headers: dict):
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.json()

def fetch_socrata_paged(days_back: int, min_val: int, limit: int):
    if not SOCRATA_ENDPOINT:
        raise RuntimeError("SOCRATA_ENDPOINT not set")

    headers = {"Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    # detect fields from sample
    sample_url = f"{SOCRATA_ENDPOINT}?$limit=60"
    sample = socrata_get(sample_url, headers)
    if not isinstance(sample, list):
        return [], detect_fields([])

    fields = detect_fields(sample)
    if not fields["issue_date"] or not fields["value"]:
        logger.warning("Socrata: could not detect date/value fields; returning newest without filters.")
        fallback = socrata_get(f"{SOCRATA_ENDPOINT}?$limit={min(min(limit,200),200)}", headers)
        return fallback, fields

    cutoff = iso_date_days_ago(days_back)
    where = f"{fields['issue_date']} >= '{cutoff}' AND {fields['value']} >= {min_val}"

    out = []
    offset = 0
    page = min(PAGE_SIZE, limit)

    while len(out) < limit:
        params = {
            "$where": where,
            "$order": f"{fields['value']} DESC",
            "$limit": str(page),
            "$offset": str(offset),
        }
        url = f"{SOCRATA_ENDPOINT}?{urlencode(params)}"
        chunk = socrata_get(url, headers)
        if not chunk:
            break
        out.extend(chunk)
        offset += page
        if len(chunk) < page:
            break

    return out[:limit], fields

# -------------------------------------------------------------------
# FETCH: ARCGIS (paged) + client-side filtering
# -------------------------------------------------------------------
def arcgis_query(qurl: str, params: dict):
    r = requests.get(qurl, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(json.dumps(data["error"]))
    return data

def fetch_arcgis_paged(days_back: int, min_val: int, limit: int):
    if not ARCGIS_FEATURESERVER_URL:
        raise RuntimeError("ARCGIS_FEATURESERVER_URL not set")

    qurl = ARCGIS_FEATURESERVER_URL.rstrip("/") + "/query"

    # sample for field detection
    sample = arcgis_query(qurl, {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "false",
        "resultRecordCount": "60",
        "resultOffset": "0",
    })
    feats = sample.get("features", [])
    sample_rows = [f.get("attributes", {}) for f in feats]
    fields = detect_fields(sample_rows)

    # page through ordered by value if possible; else no order
    order_by = ""
    if fields["value"]:
        order_by = f"{fields['value']} DESC"

    out = []
    offset = 0
    page = min(PAGE_SIZE, limit)

    while len(out) < limit:
        params = {
            "f": "json",
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultRecordCount": str(page),
            "resultOffset": str(offset),
        }
        if order_by:
            params["orderByFields"] = order_by

        data = arcgis_query(qurl, params)
        feats = data.get("features", [])
        chunk = [f.get("attributes", {}) for f in feats]
        if not chunk:
            break
        out.extend(chunk)
        offset += page
        if len(chunk) < page:
            break

    # client-side filter by date/min_val (best effort)
    cutoff_iso = iso_date_days_ago(days_back)
    cutoff_dt = datetime.fromisoformat(cutoff_iso).replace(tzinfo=timezone.utc)

    def parse_arcgis_date(v):
        if v is None:
            return None
        # epoch ms
        if isinstance(v, (int, float)) and v > 10_000_000_000:
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        # ISO-ish
        try:
            s = str(v).replace("Z", "+00:00")
            dtx = datetime.fromisoformat(s)
            if dtx.tzinfo is None:
                dtx = dtx.replace(tzinfo=timezone.utc)
            return dtx
        except Exception:
            return None

    filtered = []
    for p in out:
        # value filter
        v = safe_float(p.get(fields["value"]), 0) if fields["value"] else safe_float(
            p.get("estimated_cost") or p.get("valuation") or p.get("ESTIMATED_COST") or 0, 0
        )
        if v < min_val:
            continue

        # date filter (if detectable)
        if fields["issue_date"]:
            dtv = parse_arcgis_date(p.get(fields["issue_date"]))
            if dtv is not None and dtv < cutoff_dt:
                continue

        filtered.append(p)

    return filtered[:limit], fields

# -------------------------------------------------------------------
# FETCH wrapper: cache + stale fallback
# -------------------------------------------------------------------
def fetch_permits(days_back: int, min_val: int, limit: int):
    backend = choose_backend()
    if backend == "none":
        raise RuntimeError("No data source configured. Set ARCGIS_FEATURESERVER_URL or SOCRATA_ENDPOINT.")

    limit = max(1, min(limit, MAX_LIMIT))
    cache_key = f"{backend}:{days_back}:{min_val}:{limit}"

    cached, age = cache_get(cache_key)
    if cached is not None and age <= CACHE_TTL_SECONDS:
        return cached["rows"], cached["fields"], backend, True

    try:
        if backend == "arcgis":
            rows, fields = fetch_arcgis_paged(days_back, min_val, limit)
        else:
            rows, fields = fetch_socrata_paged(days_back, min_val, limit)

        payload = {"rows": rows, "fields": fields}
        cache_set(cache_key, payload)
        return rows, fields, backend, False

    except Exception as e:
        logger.exception("Fetch failed")
        if cached is not None:
            logger.warning("Serving stale cache due to error: %s", e)
            return cached["rows"], cached["fields"], backend, True
        raise

# -------------------------------------------------------------------
# SCORING
# -------------------------------------------------------------------
COMMERCIAL_KEYWORDS = {
    "restaurant": 6, "tenant improvement": 7, "ti ": 4, "sprinkler": 8, "elevator": 8,
    "hvac": 5, "retail": 4, "warehouse": 4, "industrial": 4, "multifamily": 4,
    "hotel": 6, "office": 4, "fire alarm": 7, "zoning": 5
}
SPECIALIZED = ["sprinkler", "elevator", "hvac", "fire alarm"]

def score_permit(p: dict, fields: dict) -> dict:
    pid_field = fields.get("permit_id") or ""
    desc_field = fields.get("desc") or ""
    status_field = fields.get("status") or ""
    val_field = fields.get("value") or ""

    permit_id = norm_text(p.get(pid_field)) if pid_field else norm_text(p.get("permit_number") or p.get("OBJECTID") or "")
    desc = norm_text(p.get(desc_field)) if desc_field else norm_text(p.get("work_description") or p.get("description") or "")
    status = norm_text(p.get(status_field)) if status_field else norm_text(p.get("status") or p.get("permit_status") or "")
    val = safe_float(p.get(val_field), 0) if val_field else safe_float(p.get("estimated_cost") or p.get("valuation") or p.get("ESTIMATED_COST") or 0)

    # Value score (0-40)
    value_score = 5
    if val >= 5_000_000:
        value_score = 40
    elif val >= 2_000_000:
        value_score = 30
    elif val >= 1_000_000:
        value_score = 20
    elif val >= 500_000:
        value_score = 15

    # Urgency score (0-30)
    s = status.lower()
    urgency_score = 5
    if "plan review" in s or "objection" in s or "review" in s:
        urgency_score = 25
    elif "issued" in s or "approved" in s:
        urgency_score = 15
    elif "open" in s or "pending" in s:
        urgency_score = 10

    # Commercial probability (0-30)
    d = desc.lower()
    kw = 0
    for k, pts in COMMERCIAL_KEYWORDS.items():
        if k in d:
            kw += pts
    commercial_score = min(30, kw)

    # Competition modifier
    comp_mod = 0
    if any(k in d for k in SPECIALIZED):
        comp_mod += 10
    elif any(g in d for g in ["renovation", "alteration", "repair"]):
        comp_mod -= 5

    total = max(0, min(100, int(value_score + urgency_score + commercial_score + comp_mod)))

    out = dict(p)
    out["_norm"] = {
        "permit_id": permit_id,
        "description": desc,
        "status": status,
        "value": val
    }
    out["score"] = total
    out["score_breakdown"] = {
        "value_score": value_score,
        "urgency_score": urgency_score,
        "commercial_score": commercial_score,
        "competition_modifier": comp_mod
    }
    return out

# -------------------------------------------------------------------
# PAGES
# -------------------------------------------------------------------
@app.get("/")
def home():
    return render_template("index.html")

@app.get("/permit/<permit_id>")
def permit_detail_page(permit_id):
    return render_template("detail.html", permit_id=permit_id)

@app.get("/crm")
def crm_page():
    return render_template("crm.html")

# -------------------------------------------------------------------
# API
# -------------------------------------------------------------------
@app.get("/health")
def health():
    return jsonify({"status": "ok", "backend": choose_backend()})

@app.get("/api/permits")
def api_permits():
    days = safe_int(request.args.get("days", DEFAULT_DAYS), DEFAULT_DAYS)
    min_val = safe_int(request.args.get("min_val", DEFAULT_MIN_VALUE), DEFAULT_MIN_VALUE)
    limit = safe_int(request.args.get("limit", 2000), 2000)

    rows, fields, backend, from_cache = fetch_permits(days, min_val, limit)
    scored = [score_permit(p, fields) for p in rows]
    scored.sort(key=lambda x: x.get("score", 0), reverse=True)

    return jsonify({
        "meta": {
            "backend": backend,
            "from_cache": from_cache,
            "fields": fields,
            "count": len(scored)
        },
        "rows": scored[:50]
    })

@app.get("/api/permit/<permit_id>")
def api_permit(permit_id):
    permit_id = str(permit_id)

    # Broader fetch window to make detail lookup robust
    rows, fields, backend, from_cache = fetch_permits(DEFAULT_DAYS, 0, 3000)
    pid_field = fields.get("permit_id") or ""

    hit = None
    for p in rows:
        pid = norm_text(p.get(pid_field)) if pid_field else norm_text(p.get("permit_number") or p.get("OBJECTID") or "")
        if pid == permit_id:
            hit = p
            break

    if not hit:
        return jsonify({"error": "Permit not found in cached window."}), 404

    return jsonify({
        "meta": {
            "backend": backend,
            "from_cache": from_cache,
            "fields": fields
        },
        "row": score_permit(hit, fields)
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
