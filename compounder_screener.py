"""
compounder_screener.py — Weekly Fundamental Compounder Screen

Filters stocks from Screener.in CSV export against:
  - 10Y PAT CAGR  > 20%
  - 10Y Rev CAGR  > 15%
  - 5Y  PAT CAGR  > 20%
  - 5Y  Rev CAGR  > 15%
  - 3Y  PAT CAGR  > 20%
  - 3Y  Rev CAGR  > 15%
  - Current PE    < 30x
  - D/E           < 0.75x
  - ROE           > 10%

Runs every Friday at 16:30 IST, emails results over the weekend.

Usage:
  python compounder_screener.py           # Run on schedule (every Friday)
  python compounder_screener.py --now     # Run immediately
  python compounder_screener.py --dry-run # Run but skip email (print only)
"""

import os
import sys
import logging
import argparse
import smtplib
import schedule
import time
import pandas as pd
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
EMAIL_SENDER    = os.environ.get("EMAIL_SENDER",    "02abhij@gmail.com")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD",  "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "abhi@ajassoc.in")
SMTP_HOST       = "smtp.gmail.com"
SMTP_PORT       = 587
RUN_TIME        = "16:30"   # IST, Friday post-market close

SCREENER_CSV    = os.environ.get("SCREENER_CSV", "Updated_claude_100_Market_Cap.csv")

# ── Thresholds ────────────────────────────────────────────────────────────────
FILTERS = {
    "Profit growth 10Years": (">", 20),
    "Sales growth 10Years":  (">", 15),
    "Profit growth 5Years":  (">", 20),
    "Sales growth 5Years":   (">", 15),
    "Profit growth 3Years":  (">", 20),
    "Sales growth 3Years":   (">", 15),
    "Price to Earning":      ("<", 30),
    "Debt to equity":        ("<", 0.75),
    "Return on equity":      (">", 10),
}

# Display labels for the email
LABELS = {
    "Profit growth 10Years": "PAT 10Y%",
    "Sales growth 10Years":  "Rev 10Y%",
    "Profit growth 5Years":  "PAT 5Y%",
    "Sales growth 5Years":   "Rev 5Y%",
    "Profit growth 3Years":  "PAT 3Y%",
    "Sales growth 3Years":   "Rev 3Y%",
    "Price to Earning":      "PE",
    "Debt to equity":        "D/E",
    "Return on equity":      "ROE%",
    "Market Capitalization": "Mcap(Cr)",
    "Current Price":         "Price(₹)",
}


# ── Screener ──────────────────────────────────────────────────────────────────

def load_data(csv_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        log.error(f"CSV not found: {csv_path}")
        log.error("Download from Screener.in → Portfolio → Export and place in same folder.")
        return pd.DataFrame()

    for col in FILTERS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["Market Capitalization", "Current Price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info(f"Loaded {len(df)} stocks from {csv_path}")
    return df


def run_screen(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    mask = pd.Series(True, index=df.index)
    filter_log = []

    for col, (op, threshold) in FILTERS.items():
        if col not in df.columns:
            log.warning(f"Column '{col}' not found in CSV — filter skipped")
            continue

        before = mask.sum()

        if op == ">":
            col_mask = df[col].notna() & (df[col] > threshold)
        else:  # "<"
            col_mask = df[col].notna() & (df[col] < threshold)

        mask = mask & col_mask
        after = mask.sum()
        filter_log.append(
            f"  {LABELS.get(col, col):<12} {op}{threshold:<6} → {before:>4} → {after:>4} stocks"
        )

    log.info("Filter funnel:")
    for line in filter_log:
        log.info(line)

    result = df[mask].copy()

    # Sort by 10Y PAT CAGR descending
    if "Profit growth 10Years" in result.columns:
        result = result.sort_values("Profit growth 10Years", ascending=False)

    result = result.reset_index(drop=True)
    result.index += 1
    return result


# ── Email ─────────────────────────────────────────────────────────────────────

def build_html(result: pd.DataFrame, run_date: str) -> str:
    n = len(result)

    display_cols = [
        "Name", "Industry Group",
        "Profit growth 10Years", "Sales growth 10Years",
        "Profit growth 5Years",  "Sales growth 5Years",
        "Profit growth 3Years",  "Sales growth 3Years",
        "Price to Earning", "Debt to equity", "Return on equity",
        "Market Capitalization", "Current Price",
    ]
    display_cols = [c for c in display_cols if c in result.columns]

    # Header row
    headers = ""
    for col in display_cols:
        label = LABELS.get(col, col.replace(" growth ", " ").replace("Years", "Y"))
        headers += f'<th style="padding:8px 10px;background:#1e40af;color:white;font-size:11px;white-space:nowrap;">{label}</th>'

    # Data rows
    rows_html = ""
    for rank, row in result.iterrows():
        bg = "#f9fafb" if rank % 2 == 0 else "#ffffff"
        cells = ""
        for col in display_cols:
            val = row.get(col, "")
            if col == "Name":
                cell = f'<td style="padding:8px 10px;font-weight:700;color:#111827;white-space:nowrap;">{val}</td>'
            elif col == "Industry Group":
                cell = f'<td style="padding:8px 10px;font-size:11px;color:#6b7280;white-space:nowrap;">{val}</td>'
            elif col == "Price to Earning":
                color = "#16a34a" if pd.notna(val) and val < 20 else "#b45309" if pd.notna(val) and val < 25 else "#111827"
                cell = f'<td style="padding:8px 10px;text-align:right;color:{color};font-weight:600;">{f"{val:.1f}x" if pd.notna(val) else "—"}</td>'
            elif col == "Debt to equity":
                cell = f'<td style="padding:8px 10px;text-align:right;">{f"{val:.2f}x" if pd.notna(val) else "—"}</td>'
            elif col == "Market Capitalization":
                cell = f'<td style="padding:8px 10px;text-align:right;font-size:11px;">₹{val:,.0f}' if pd.notna(val) else '<td style="padding:8px 10px;text-align:right;">—'
                cell += '</td>'
            elif col == "Current Price":
                cell = f'<td style="padding:8px 10px;text-align:right;">₹{val:,.1f}</td>' if pd.notna(val) else '<td style="padding:8px 10px;text-align:right;">—</td>'
            elif isinstance(val, float) and pd.notna(val):
                color = "#16a34a" if val >= 25 else "#111827"
                cell = f'<td style="padding:8px 10px;text-align:right;color:{color};font-weight:600;">{val:.1f}%</td>'
            else:
                cell = f'<td style="padding:8px 10px;text-align:right;">{val if pd.notna(val) else "—"}</td>'
            cells += cell

        rows_html += f'<tr style="background:{bg};">{cells}</tr>'

    if n == 0:
        body = '''
        <div style="padding:40px;text-align:center;color:#6b7280;">
            <div style="font-size:48px;">🔍</div>
            <div style="font-size:18px;font-weight:600;margin-top:12px;">No stocks passed all filters this week</div>
            <div style="font-size:13px;margin-top:8px;">The screen is working correctly — the bar is intentionally high.</div>
        </div>'''
    else:
        body = f'''
        <div style="overflow-x:auto;padding:0 16px 24px;">
            <table style="width:100%;border-collapse:collapse;font-size:12px;">
                <thead><tr>{headers}</tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>'''

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
<div style="max-width:1100px;margin:30px auto;background:#fff;border-radius:12px;
            box-shadow:0 2px 12px rgba(0,0,0,0.08);overflow:hidden;">

  <div style="background:linear-gradient(135deg,#1e40af 0%,#3b82f6 100%);padding:28px 32px;color:white;">
    <div style="font-size:22px;font-weight:800;">📊 Weekly Compounder Screen</div>
    <div style="font-size:14px;opacity:0.85;margin-top:4px;">{run_date} · {n} stock{"s" if n != 1 else ""} passed all filters</div>
    <div style="margin-top:14px;font-size:12px;background:rgba(255,255,255,0.15);
                display:inline-block;padding:8px 16px;border-radius:8px;line-height:1.8;">
      PAT 10Y &gt;20% · Rev 10Y &gt;15% · PAT 5Y &gt;20% · Rev 5Y &gt;15%<br>
      PAT 3Y &gt;20% · Rev 3Y &gt;15% · PE &lt;30x · D/E &lt;0.75x · ROE &gt;10%
    </div>
  </div>

  {body}

  <div style="padding:16px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;
              font-size:11px;color:#9ca3af;line-height:1.6;">
    <b>Data source:</b> Screener.in export · Updated weekly.<br>
    <b>Note:</b> This is a mechanical screen, not a buy recommendation.
    Verify thesis, governance, and OCF before acting on any name.
  </div>
</div>
</body>
</html>"""
    return html


def send_email(result: pd.DataFrame, dry_run: bool = False) -> bool:
    run_date = datetime.now().strftime("%d %b %Y")
    n = len(result)
    subject = f"📊 Compounder Screen · {n} stock{'s' if n != 1 else ''} · {run_date}"
    html_body = build_html(result, run_date)

    if dry_run:
        print("\n" + "="*80)
        print(f"DRY RUN — Email subject: {subject}")
        print(f"Stocks passing: {n}")
        if n > 0:
            preview_cols = [c for c in [
                "Name", "Profit growth 10Years", "Sales growth 10Years",
                "Profit growth 5Years", "Sales growth 5Years",
                "Profit growth 3Years", "Sales growth 3Years",
                "Price to Earning", "Debt to equity", "Return on equity"
            ] if c in result.columns]
            print(result[preview_cols].to_string())
        print("="*80)
        return True

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECIPIENT
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
        log.info(f"✅ Email sent to {EMAIL_RECIPIENT}")
        return True
    except Exception as e:
        log.error(f"❌ Email failed: {e}")
        return False


# ── Main job ──────────────────────────────────────────────────────────────────

def job(dry_run: bool = False):
    log.info("=" * 60)
    log.info(f"COMPOUNDER SCREEN — {datetime.now().strftime('%d %b %Y %H:%M')}")
    log.info("=" * 60)

    df = load_data(SCREENER_CSV)
    if df.empty:
        log.error("No data loaded. Aborting.")
        return

    result = run_screen(df)
    log.info(f"\n{'─'*40}")
    log.info(f"RESULT: {len(result)} stocks passed all filters")
    log.info(f"{'─'*40}")

    send_email(result, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Weekly Compounder Screener")
    parser.add_argument("--now",     action="store_true", help="Run immediately")
    parser.add_argument("--dry-run", action="store_true", help="Run but skip email (print only)")
    args = parser.parse_args()

    if args.now or args.dry_run:
        job(dry_run=args.dry_run)
        return

    log.info(f"Scheduler started. Will run every Friday at {RUN_TIME} IST.")
    log.info("Press Ctrl+C to stop.")
    schedule.every().friday.at(RUN_TIME).do(job)

    # Run immediately if today is Friday and past scheduled time
    if datetime.now().strftime("%A") == "Friday":
        if datetime.now().strftime("%H:%M") >= RUN_TIME:
            log.info("It's Friday and past run time — running now.")
            job()

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
