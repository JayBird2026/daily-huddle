#!/usr/bin/env python3
"""
Jay Is On The Way — Daily Huddle Report Generator
Runs via GitHub Actions (Mon–Fri, ~8 AM ET) to pull live Google Sheets data
and produce index.html served by GitHub Pages at huddle.jayisontheway.com.

Environment variable required:
  GOOGLE_CREDENTIALS_JSON  — JSON content of the service account key file

The service account must have Viewer access to both spreadsheets below.
"""

import os, sys, json, math, calendar, datetime
from pathlib import Path

# ── Google API ────────────────────────────────────────────────────────────────
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ── Jinja2 ────────────────────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader

# =============================================================================
# CONFIGURATION — update these when budgets or tab GIDs change
# =============================================================================
SHEET_ID        = "1BXKrUl5L6Bge_D7Pcv4dwxEcdXw5lYb43BoZY2ab3kQ"
ESC_SHEET_ID    = "1XlpTSOFRByX9E_c0y-ICudIUZJdIgZAX"
SCOPES          = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Monthly budgets (April 2026) — update each new month/year
MONTHLY_BUDGETS = {
    "hvac":         225_044,
    "plumbing":     133_086,
    "install":      705_198,
    "sales":        775_718,
    "consolidated": 1_063_328,
}
BREAK_EVEN_DAILY = 48_332   # consolidated daily break-even

# Annual budgets (2026)
ANNUAL_BUDGETS = {
    "hvac":         2_700_531,
    "plumbing":     1_597_031,
    "install":      8_462_379,
    "sales":        9_308_616,
    "consolidated": 12_759_941,
}
ANNUAL_BIZ_DAYS = 252   # standard M-F count used in projections

# Tab GIDs — add future months as needed
MONTH_GIDS_2026 = {1: 159900154, 2: 310943781, 3: 1300190556, 4: 160735500}
MONTH_GIDS_2025 = {1: 2080406158, 2: 2016535787, 3: 1370114770, 4: 317639730}

# Row offsets (1-based sheet rows, converted to 0-based in fetch functions)
# Based on the GD Huddle Worksheet layout
ROW = {
    "header":           1,   # date headers in col H+
    "hvac_commit":      5,
    "plumb_commit":     10,
    "sales_commit":     15,
    "hvac_monthly":     41,
    "hvac_actual":      42,
    "plumb_monthly":    53,
    "plumb_actual":     54,
    "sales_tgl":        59,   # rows 59/60/61/62/63/64 = TGL/Mkt/Other pairs
    "sales_monthly":    65,
    "sales_actual":     66,
    "install_monthly":  75,
    "install_actual":   76,
    "permits":          92,
    "nc_reviews":       98,
    "va_reviews":       99,
}

# =============================================================================
# HELPERS
# =============================================================================
def fmt_dollar(n):
    return f"${int(round(n)):,}"

def safe(val):
    """Convert a cell value to float, returning 0 if blank/invalid."""
    try:
        return float(str(val).replace(",", "").replace("$", "").replace("%", ""))
    except:
        return 0.0

def biz_days_in_month(year, month):
    num = calendar.monthrange(year, month)[1]
    return sum(1 for d in range(1, num + 1) if datetime.date(year, month, d).weekday() < 5)

def biz_day_number(year, month, day):
    return sum(1 for d in range(1, day + 1) if datetime.date(year, month, d).weekday() < 5)

def biz_day_to_col(biz_day):
    """Return 0-based column index for the given business day (col H = index 7 = day 1)."""
    return 7 + (biz_day - 1)

# =============================================================================
# GOOGLE SHEETS
# =============================================================================
def get_service():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        print("ERROR: GOOGLE_CREDENTIALS_JSON not set.", file=sys.stderr)
        sys.exit(1)
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def sheet_title(service, spreadsheet_id, gid):
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta["sheets"]:
        if s["properties"]["sheetId"] == gid:
            return s["properties"]["title"]
    raise ValueError(f"GID {gid} not found in {spreadsheet_id}")

def get_range(service, spreadsheet_id, tab, rng):
    full = f"'{tab}'!{rng}"
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=full
    ).execute()
    return result.get("values", [])

def pad_row(row, length):
    """Ensure a row has at least `length` elements."""
    return list(row) + [""] * max(0, length - len(row))

# =============================================================================
# DATA FETCHING
# =============================================================================
def fetch_main_tab(service, year, month, biz_day):
    """Fetch the main data block from the current month's tab."""
    gid = MONTH_GIDS_2026.get(month) or MONTH_GIDS_2025.get(month)
    if gid is None:
        raise ValueError(f"No GID configured for {year}-{month:02d}")
    tab = sheet_title(service, SHEET_ID, gid)
    # Column span: A through enough columns to cover all business days (22 max = col H + 21 = col AD)
    data = get_range(service, SHEET_ID, tab, "A1:AD115")

    # Pad all rows to at least 30 cols
    rows = [pad_row(r, 30) for r in data]

    # Today's column index (0-based): col H = index 7 = business day 1
    today_col = biz_day_to_col(biz_day)

    def cell(row_1based, col_0based):
        r = row_1based - 1
        if r < len(rows) and col_0based < len(rows[r]):
            return rows[r][col_0based]
        return ""

    def daily_val(row_1based, day):
        return safe(cell(row_1based, biz_day_to_col(day)))

    # MTD actuals from col B of the "Actual" rows
    hvac_mtd    = safe(cell(ROW["hvac_actual"],   1))
    plumb_mtd   = safe(cell(ROW["plumb_actual"],  1))
    install_mtd = safe(cell(ROW["install_actual"],1))
    sales_mtd   = safe(cell(ROW["sales_actual"],  1))

    # Today's commitments from today_col
    hvac_commit   = safe(cell(ROW["hvac_commit"],   today_col))
    plumb_commit  = safe(cell(ROW["plumb_commit"],  today_col))
    sales_commit  = safe(cell(ROW["sales_commit"],  today_col))

    # Install commitment: use "Monthly" row for today's column
    install_commit_raw = cell(ROW["install_monthly"], today_col)
    install_commit = safe(install_commit_raw)
    install_commit_entered = bool(install_commit_raw.strip()) if isinstance(install_commit_raw, str) else (install_commit_raw not in ("", None))

    # Sales breakdown rows 59–64 (1-based)
    # Structure: pair of rows per source (header/label + data row)
    # Revenue in col D (index 3); pct in col F (index 5); jobs in col G (index 6)
    def sales_row(row_1based):
        r = rows[row_1based - 1] if (row_1based - 1) < len(rows) else []
        r = pad_row(r, 10)
        rev_raw = r[3]; pct_raw = r[5]; jobs_raw = r[6]
        rev  = safe(rev_raw)
        pct  = safe(pct_raw)
        jobs = int(safe(jobs_raw)) if safe(jobs_raw) > 0 else 0
        return rev, pct, jobs

    tgl_rev,   tgl_pct,  tgl_jobs  = sales_row(59)
    mkt_rev,   mkt_pct,  mkt_jobs  = sales_row(61)
    other_rev, other_pct,other_jobs = sales_row(63)

    # Permits row 92 (1-based): col D=total, E=scheduled, F=closed, G=open
    perm_row = pad_row(rows[91] if len(rows) > 91 else [], 10)
    permit_total     = int(safe(perm_row[3])) if perm_row[3] else 0
    permit_scheduled = int(safe(perm_row[4])) if perm_row[4] else 0
    permit_closed    = int(safe(perm_row[5])) if perm_row[5] else 0
    permit_open      = int(safe(perm_row[6])) if perm_row[6] else 0

    # Reviews: cumulative counts in col H+ (same column mapping as daily data)
    # NC row 98, VA row 99 — use today's column or last non-blank before today
    def last_review_count(row_1based):
        """Find the most recent non-blank cumulative count up through today."""
        for day in range(biz_day, 0, -1):
            v = safe(cell(row_1based, biz_day_to_col(day)))
            if v > 0:
                return int(v)
        return 0

    def first_review_count(row_1based):
        """Day 1 cumulative count (used for MTD calc)."""
        return int(safe(cell(row_1based, 7)))   # col H = index 7 = day 1

    nc_total = last_review_count(ROW["nc_reviews"])
    nc_start = first_review_count(ROW["nc_reviews"])
    nc_mtd   = nc_total - nc_start

    va_total = last_review_count(ROW["va_reviews"])
    va_start = first_review_count(ROW["va_reviews"])
    va_mtd   = va_total - va_start

    return {
        "hvac_mtd": hvac_mtd, "plumb_mtd": plumb_mtd,
        "install_mtd": install_mtd, "sales_mtd": sales_mtd,
        "hvac_commit": hvac_commit, "plumb_commit": plumb_commit,
        "sales_commit": sales_commit,
        "install_commit": install_commit,
        "install_commit_entered": install_commit_entered,
        "tgl_rev": tgl_rev, "tgl_pct": tgl_pct, "tgl_jobs": tgl_jobs,
        "mkt_rev": mkt_rev, "mkt_pct": mkt_pct, "mkt_jobs": mkt_jobs,
        "other_rev": other_rev, "other_pct": other_pct, "other_jobs": other_jobs,
        "permit_total": permit_total, "permit_scheduled": permit_scheduled,
        "permit_closed": permit_closed, "permit_open": permit_open,
        "nc_total": nc_total, "nc_mtd": nc_mtd,
        "va_total": va_total, "va_mtd": va_mtd,
    }

def fetch_prior_months_ytd(service, year, month):
    """
    Sum service-dept (HVAC+Plumbing+Install) actuals for Jan through (month-1).
    Returns dict: {hvac, plumb, install, sales} YTD totals for prior months.
    """
    totals = {"hvac": 0, "plumb": 0, "install": 0, "sales": 0}

    gids_map = MONTH_GIDS_2026 if year == 2026 else MONTH_GIDS_2025

    for m in range(1, month):
        gid = gids_map.get(m)
        if gid is None:
            continue
        try:
            tab = sheet_title(service, SHEET_ID, gid)
            data = get_range(service, SHEET_ID, tab, "A1:B80")
            rows = [pad_row(r, 5) for r in data]
            # Col B (index 1) of "Actual" rows (rows 42, 54, 66, 76)
            for key, row_1based in [("hvac",42),("plumb",54),("sales",66),("install",76)]:
                v = safe(rows[row_1based-1][1]) if (row_1based-1) < len(rows) else 0
                totals[key] += abs(v)   # some sheets store as negative
        except Exception as e:
            print(f"Warning: could not fetch {year}-{m:02d} tab: {e}", file=sys.stderr)

    return totals

def fetch_prior_year_april(service, month):
    """Fetch April 2025 full-month service totals (HVAC+Plumbing+Install)."""
    gid_2025 = MONTH_GIDS_2025.get(month)
    if not gid_2025:
        return 0
    try:
        tab = sheet_title(service, SHEET_ID, gid_2025)
        data = get_range(service, SHEET_ID, tab, "A1:B80")
        rows = [pad_row(r, 5) for r in data]
        total = 0
        for row_1based in [42, 54, 76]:   # HVAC, Plumbing, Install actuals
            v = safe(rows[row_1based-1][1]) if (row_1based-1) < len(rows) else 0
            total += abs(v)
        return total
    except Exception as e:
        print(f"Warning: could not fetch 2025-{month:02d} tab: {e}", file=sys.stderr)
        return 641_482  # fallback April 2025 value

def fetch_prior_year_ytd(service, year, month, biz_day):
    """
    YTD service total for the prior year through the same relative date.
    Returns the sum of Jan–(month-1) full months + current month through biz_day.
    """
    prior_year = year - 1
    gids_map_prior = MONTH_GIDS_2025 if prior_year == 2025 else {}
    if not gids_map_prior:
        return 1_784_354  # fallback

    total = 0
    # Full prior months
    for m in range(1, month):
        gid = gids_map_prior.get(m)
        if not gid:
            continue
        try:
            tab = sheet_title(service, SHEET_ID, gid)
            data = get_range(service, SHEET_ID, tab, "A1:B80")
            rows = [pad_row(r, 5) for r in data]
            for row_1based in [42, 54, 76]:
                total += abs(safe(rows[row_1based-1][1]) if (row_1based-1) < len(rows) else 0)
        except Exception as e:
            print(f"Warning: prior year {prior_year}-{m:02d}: {e}", file=sys.stderr)

    # Current month through equivalent biz_day in prior year
    gid_cur = gids_map_prior.get(month)
    if gid_cur:
        try:
            tab = sheet_title(service, SHEET_ID, gid_cur)
            today_col = biz_day_to_col(biz_day)
            # Fetch enough columns
            data = get_range(service, SHEET_ID, tab, "A1:AD80")
            rows = [pad_row(r, today_col + 2) for r in data]
            for row_1based in [42, 54, 76]:
                # Col B of "Actual" row = MTD at end of month; but we want through biz_day
                # Sum daily values from "Monthly" row (cols H through today_col)
                monthly_row = rows[row_1based - 2] if (row_1based - 2) < len(rows) else []  # -2 because Monthly is one row above Actual
                monthly_row = pad_row(monthly_row, today_col + 2)
                day_sum = sum(safe(monthly_row[biz_day_to_col(d)]) for d in range(1, biz_day + 1))
                total += day_sum
        except Exception as e:
            print(f"Warning: prior year current month {prior_year}-{month:02d}: {e}", file=sys.stderr)

    return total if total > 0 else 1_784_354

def fetch_escalations(service):
    """Fetch escalation counts from the customer concerns sheet."""
    try:
        # Try primary gid first, then fallback
        for gid in [1849681724, 370202499]:
            try:
                tab = sheet_title(service, ESC_SHEET_ID, gid)
                data = get_range(service, ESC_SHEET_ID, tab, "A1:G200")
                if data:
                    break
            except:
                continue

        # Count rows by status (col B = Status column, typically)
        total = 0; closed = 0; open_count = 0
        today_month = datetime.date.today().month
        this_month = 0
        for row in data[1:]:  # skip header
            if not any(c.strip() for c in row):
                continue
            total += 1
            row_p = pad_row(row, 8)
            status = row_p[1].strip().upper() if len(row_p) > 1 else ""
            if "CLOSE" in status or "RESOLVED" in status:
                closed += 1
            elif status:
                open_count += 1
        return {"total": total, "closed": closed, "open": max(0, total - closed)}
    except Exception as e:
        print(f"Warning: escalations fetch failed: {e}", file=sys.stderr)
        return {"total": 23, "closed": 22, "open": 1}  # fallback

# =============================================================================
# CALCULATIONS
# =============================================================================
def proj_finish(mtd, days_with_data, days_remaining):
    if days_with_data <= 0:
        return mtd
    return mtd + (mtd / days_with_data) * days_remaining

def revised_daily(budget, mtd, days_remaining):
    if days_remaining <= 0:
        return 0
    return (budget - mtd) / days_remaining

def ytd_proj_dec31(ytd_actual, biz_days_elapsed):
    if biz_days_elapsed <= 0:
        return 0
    return (ytd_actual / biz_days_elapsed) * ANNUAL_BIZ_DAYS

def ytd_avg(ytd_actual, biz_days_elapsed):
    if biz_days_elapsed <= 0:
        return 0
    return ytd_actual / biz_days_elapsed

def pct(val, total):
    return round(val / total * 100, 1) if total > 0 else 0.0

# =============================================================================
# MAIN
# =============================================================================
def main():
    today = datetime.date.today()
    # Skip weekends
    if today.weekday() >= 5:
        print(f"Today is {today.strftime('%A')} — skipping weekend report.")
        return

    year  = today.year
    month = today.month
    day   = today.day

    total_biz_days = biz_days_in_month(year, month)
    biz_day        = biz_day_number(year, month, day)
    days_remaining = total_biz_days - biz_day

    # Days with service data: may be biz_day or biz_day-1 if today not yet entered
    # We detect this from the actual MTD values vs. yesterday's
    days_with_service_data = biz_day  # refined below after fetch

    month_names = ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    month_abbrs = ["Jan","Feb","Mar","Apr","May","Jun",
                   "Jul","Aug","Sep","Oct","Nov","Dec"]
    dow_names   = ["Monday","Tuesday","Wednesday","Thursday","Friday"]

    full_date   = f"{dow_names[today.weekday()]}, {month_names[month-1]} {day}, {year}"
    report_date = f"{month_names[month-1]} {day}, {year}"
    month_year  = f"{month_names[month-1]} {year}"
    month_abbr  = month_abbrs[month - 1]
    ytd_label   = f"{month_abbr} {day}"

    print(f"Generating report for {full_date} (Day {biz_day} of {total_biz_days})")

    # ── Authenticate ──────────────────────────────────────────────────────────
    service = get_service()

    # ── Fetch current month data ───────────────────────────────────────────────
    d = fetch_main_tab(service, year, month, biz_day)

    hvac_mtd    = d["hvac_mtd"]
    plumb_mtd   = d["plumb_mtd"]
    install_mtd = d["install_mtd"]
    sales_mtd   = d["sales_mtd"]
    cons_mtd    = hvac_mtd + plumb_mtd + install_mtd

    # Detect days_with_service_data: if today's daily value is 0/blank, use biz_day-1
    # Proxy: compare MTD to what we'd expect if today had data vs. not
    # Simple heuristic: if service MTD matches exactly prior day's sum, use biz_day-1
    days_with_service_data = biz_day if hvac_mtd > 0 else max(1, biz_day - 1)

    # ── Monthly projections ────────────────────────────────────────────────────
    hvac_proj    = proj_finish(hvac_mtd,  days_with_service_data, days_remaining)
    plumb_proj   = proj_finish(plumb_mtd, days_with_service_data, days_remaining)
    install_proj = proj_finish(install_mtd,days_with_service_data,days_remaining)
    sales_proj   = proj_finish(sales_mtd, biz_day,                days_remaining)
    cons_proj    = hvac_proj + plumb_proj + install_proj

    # ── Revised daily targets ─────────────────────────────────────────────────
    B = MONTHLY_BUDGETS
    hvac_rev_daily  = revised_daily(B["hvac"],    hvac_mtd,    days_remaining)
    plumb_rev_daily = revised_daily(B["plumbing"],plumb_mtd,   days_remaining)
    inst_rev_daily  = revised_daily(B["install"],  install_mtd, days_remaining)
    sales_rev_daily = revised_daily(B["sales"],    sales_mtd,   days_remaining)
    cons_rev_daily  = revised_daily(B["consolidated"], cons_mtd, days_remaining)

    # ── Commitments ───────────────────────────────────────────────────────────
    hvac_commit   = d["hvac_commit"]
    plumb_commit  = d["plumb_commit"]
    install_commit= d["install_commit"]
    sales_commit  = d["sales_commit"]
    service_total = hvac_commit + plumb_commit + install_commit

    be_diff = service_total - BREAK_EVEN_DAILY
    if be_diff >= 0:
        be_badge_style = "background:rgba(16,185,129,0.2);color:#34D399;"
        be_badge_text  = f"+{fmt_dollar(be_diff)} above break-even ✅"
    else:
        be_badge_style = "background:rgba(239,68,68,0.2);color:#F87171;"
        be_badge_text  = f"-{fmt_dollar(abs(be_diff))} below break-even ⚠️"

    install_commit_html = (
        fmt_dollar(install_commit) if d["install_commit_entered"]
        else '$0 <span style="font-size:9px;opacity:0.6;">not entered</span>'
    )

    # ── Sales breakdown ────────────────────────────────────────────────────────
    tgl_rev   = d["tgl_rev"];   tgl_jobs  = d["tgl_jobs"]
    mkt_rev   = d["mkt_rev"];   mkt_jobs  = d["mkt_jobs"]
    other_rev = d["other_rev"]; other_jobs= d["other_jobs"]
    total_jobs= tgl_jobs + mkt_jobs + other_jobs

    def avg_ticket(rev, jobs):
        return fmt_dollar(rev / jobs) if jobs > 0 else "—"

    def pct_of_sales(rev):
        return f"{pct(rev, sales_mtd)}%" if sales_mtd > 0 else "—"

    # Top channel by revenue
    channels = [("Marketing", mkt_rev, mkt_jobs), ("TGL", tgl_rev, tgl_jobs), ("Other", other_rev, other_jobs)]
    top = max(channels, key=lambda x: x[1])
    top_channel     = top[0]
    top_channel_sub = f"{fmt_dollar(top[1])} · {pct(top[1], sales_mtd)}% of total"

    # Best avg ticket
    avgs = [("TGL", tgl_rev, tgl_jobs), ("Marketing", mkt_rev, mkt_jobs), ("Other", other_rev, other_jobs)]
    avgs_valid = [(n, r/j, j) for n,r,j in avgs if j > 0]
    if avgs_valid:
        best = max(avgs_valid, key=lambda x: x[1])
        best_avg_ticket     = fmt_dollar(best[1])
        best_avg_ticket_sub = f"{best[0]} · {best[2]} job{'s' if best[2]!=1 else ''}"
    else:
        best_avg_ticket     = "—"
        best_avg_ticket_sub = "—"

    total_avg_ticket = fmt_dollar(sales_mtd / total_jobs) if total_jobs > 0 else "—"
    sales_gap        = fmt_dollar(max(0, B["sales"] - sales_mtd))

    # ── YTD ───────────────────────────────────────────────────────────────────
    # Prior full months (Jan through last month) + current month MTD
    prior = fetch_prior_months_ytd(service, year, month)

    # Count YTD business days: sum of biz_days for completed months + current biz_day
    ytd_biz_days = sum(biz_days_in_month(year, m) for m in range(1, month)) + biz_day

    hvac_ytd    = prior["hvac"]    + hvac_mtd
    plumb_ytd   = prior["plumb"]   + plumb_mtd
    install_ytd = prior["install"] + install_mtd
    sales_ytd   = prior["sales"]   + sales_mtd
    cons_ytd    = hvac_ytd + plumb_ytd + install_ytd

    AB = ANNUAL_BUDGETS

    # ── YoY ───────────────────────────────────────────────────────────────────
    apr_2025_full = fetch_prior_year_april(service, month)
    ytd_2025      = fetch_prior_year_ytd(service, year, month, biz_day)
    ytd_2026_svc  = cons_ytd   # service depts only

    yoy_raw     = (cons_proj - apr_2025_full) / apr_2025_full * 100 if apr_2025_full else 0
    ytd_yoy_raw = (ytd_2026_svc - ytd_2025)  / ytd_2025 * 100      if ytd_2025   else 0

    def yoy_fmt(val):
        arrow = "▲" if val >= 0 else "▼"
        color = "#34D399" if val >= 0 else "#F87171"
        return arrow, color, f"{abs(val):.1f}%"

    yoy_arrow, yoy_color, yoy_pct_s   = yoy_fmt(yoy_raw)
    ytd_yoy_arrow, ytd_yoy_color, ytd_yoy_pct_s = yoy_fmt(ytd_yoy_raw)

    # ── Escalations ───────────────────────────────────────────────────────────
    esc = fetch_escalations(service)

    # ── Permits ───────────────────────────────────────────────────────────────
    permit_total     = d["permit_total"]
    permit_scheduled = d["permit_scheduled"]
    permit_closed    = d["permit_closed"]
    permit_open      = d["permit_open"]
    permit_rate      = int(round(permit_closed / permit_total * 100)) if permit_total else 0

    # ── Reviews ───────────────────────────────────────────────────────────────
    nc_mtd   = max(0, d["nc_mtd"])
    nc_total = d["nc_total"]
    va_mtd   = max(0, d["va_mtd"])
    va_total = d["va_total"]

    # ── Progress bar percentages ───────────────────────────────────────────────
    def prog(mtd_val, budget):
        return round(min(100, mtd_val / budget * 100), 1) if budget else 0

    # ── YTD per-dept projections ──────────────────────────────────────────────
    def ytd_pct_str(ytd_val, annual_budget):
        return f"{pct(ytd_val, annual_budget)}"

    def ytd_proj_str(ytd_val):
        return fmt_dollar(ytd_proj_dec31(ytd_val, ytd_biz_days))

    def ytd_avg_str(ytd_val):
        avg = ytd_avg(ytd_val, ytd_biz_days)
        return f"${int(round(avg)):,}"

    # ── Build template context ─────────────────────────────────────────────────
    ctx = {
        # Dates
        "report_date":         report_date,
        "full_date":           full_date,
        "month_year":          month_year,
        "month_num":           month,
        "month_name_short":    month_abbr,
        "month_abbr":          month_abbr,
        "biz_day":             biz_day,
        "total_biz_days":      total_biz_days,
        "days_remaining":      days_remaining,
        "ytd_label":           ytd_label,

        # MTDs
        "hvac_mtd":            fmt_dollar(hvac_mtd),
        "plumbing_mtd":        fmt_dollar(plumb_mtd),
        "install_mtd":         fmt_dollar(install_mtd),
        "sales_mtd":           fmt_dollar(sales_mtd),
        "consolidated_mtd":    fmt_dollar(cons_mtd),

        # Progress bars (monthly pacing)
        "hvac_progress_pct":         prog(hvac_mtd,    B["hvac"]),
        "plumbing_progress_pct":     prog(plumb_mtd,   B["plumbing"]),
        "install_progress_pct":      prog(install_mtd, B["install"]),
        "sales_progress_pct":        prog(sales_mtd,   B["sales"]),
        "consolidated_progress_pct": prog(cons_mtd,    B["consolidated"]),

        # Projected finishes
        "hvac_proj":         fmt_dollar(hvac_proj),
        "plumbing_proj":     fmt_dollar(plumb_proj),
        "install_proj":      fmt_dollar(install_proj),
        "sales_proj":        fmt_dollar(sales_proj),
        "consolidated_proj": fmt_dollar(cons_proj),

        # Revised daily targets
        "hvac_revised_daily":         fmt_dollar(hvac_rev_daily),
        "plumbing_revised_daily":     fmt_dollar(plumb_rev_daily),
        "install_revised_daily":      fmt_dollar(inst_rev_daily),
        "sales_revised_daily":        fmt_dollar(sales_rev_daily),
        "consolidated_revised_daily": fmt_dollar(cons_rev_daily),

        # Commitments
        "hvac_commit":         fmt_dollar(hvac_commit),
        "plumbing_commit":     fmt_dollar(plumb_commit),
        "install_commit_html": install_commit_html,
        "sales_commit":        fmt_dollar(sales_commit),
        "service_commit_total":fmt_dollar(service_total),
        "be_badge_style":      be_badge_style,
        "be_badge_text":       be_badge_text,

        # YoY
        "apr_2025_full":  fmt_dollar(apr_2025_full),
        "yoy_pct":        yoy_pct_s,
        "yoy_arrow":      yoy_arrow,
        "yoy_color":      yoy_color,
        "ytd_2025":       fmt_dollar(ytd_2025),
        "ytd_2026":       fmt_dollar(ytd_2026_svc),
        "ytd_yoy_pct":    ytd_yoy_pct_s,
        "ytd_yoy_arrow":  ytd_yoy_arrow,
        "ytd_yoy_color":  ytd_yoy_color,

        # YTD
        "hvac_ytd":      fmt_dollar(hvac_ytd),
        "hvac_ytd_pct":  ytd_pct_str(hvac_ytd, AB["hvac"]),
        "hvac_ytd_proj": ytd_proj_str(hvac_ytd),
        "hvac_ytd_avg":  ytd_avg_str(hvac_ytd),

        "plumbing_ytd":      fmt_dollar(plumb_ytd),
        "plumbing_ytd_pct":  ytd_pct_str(plumb_ytd, AB["plumbing"]),
        "plumbing_ytd_proj": ytd_proj_str(plumb_ytd),
        "plumbing_ytd_avg":  ytd_avg_str(plumb_ytd),

        "install_ytd":      fmt_dollar(install_ytd),
        "install_ytd_pct":  ytd_pct_str(install_ytd, AB["install"]),
        "install_ytd_proj": ytd_proj_str(install_ytd),
        "install_ytd_avg":  ytd_avg_str(install_ytd),

        "sales_ytd":      fmt_dollar(sales_ytd),
        "sales_ytd_pct":  ytd_pct_str(sales_ytd, AB["sales"]),
        "sales_ytd_proj": ytd_proj_str(sales_ytd),
        "sales_ytd_avg":  ytd_avg_str(sales_ytd),
        "sales_gap":      fmt_dollar(max(0, B["sales"] - sales_mtd)),

        "consolidated_ytd":      fmt_dollar(cons_ytd),
        "consolidated_ytd_pct":  ytd_pct_str(cons_ytd, AB["consolidated"]),
        "consolidated_ytd_proj": ytd_proj_str(cons_ytd),
        "consolidated_ytd_avg":  ytd_avg_str(cons_ytd),

        # Sales breakdown
        "tgl_revenue": fmt_dollar(tgl_rev), "tgl_jobs": tgl_jobs,
        "tgl_avg":     avg_ticket(tgl_rev, tgl_jobs), "tgl_pct": pct_of_sales(tgl_rev),

        "mkt_revenue": fmt_dollar(mkt_rev), "mkt_jobs": mkt_jobs,
        "mkt_avg":     avg_ticket(mkt_rev, mkt_jobs), "mkt_pct": pct_of_sales(mkt_rev),

        "other_revenue": fmt_dollar(other_rev), "other_jobs": other_jobs,
        "other_avg":     avg_ticket(other_rev, other_jobs), "other_pct": pct_of_sales(other_rev),

        "total_jobs":    total_jobs,
        "total_avg":     total_avg_ticket,
        "top_channel":   top_channel,
        "top_channel_sub": top_channel_sub,
        "best_avg_ticket":     best_avg_ticket,
        "best_avg_ticket_sub": best_avg_ticket_sub,
        "sales_gap":     sales_gap,

        # Reviews
        "nc_reviews_mtd":   nc_mtd,
        "nc_reviews_total": f"{nc_total:,}",
        "va_reviews_mtd":   va_mtd,
        "va_reviews_total": f"{va_total:,}",
        "combined_reviews_mtd": nc_mtd + va_mtd,

        # Permits
        "permit_total":     permit_total,
        "permit_scheduled": permit_scheduled,
        "permit_closed":    permit_closed,
        "permit_open":      permit_open,
        "permit_close_rate":permit_rate,

        # Escalations
        "esc_total":      esc["total"],
        "esc_closed":     esc["closed"],
        "esc_open":       esc["open"],
        "esc_this_month": 0,  # would need date parsing; kept as 0 for now
    }

    # ── Render ────────────────────────────────────────────────────────────────
    script_dir = Path(__file__).parent
    env = Environment(
        loader=FileSystemLoader(str(script_dir)),
        autoescape=False,
    )
    template = env.get_template("template_base.html")
    html_out = template.render(**ctx)

    out_path = script_dir / "index.html"
    out_path.write_text(html_out, encoding="utf-8")
    print(f"Report written to {out_path}")

if __name__ == "__main__":
    main()
