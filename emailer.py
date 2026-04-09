"""
emailer.py — Format and send the daily momentum report as a styled HTML email.
"""

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

import pandas as pd
import config

log = logging.getLogger(__name__)


def _signal_bar(score: float, max_score: float = 100) -> str:
    """Returns a simple HTML progress bar."""
    pct = min(int((score / max_score) * 100), 100)
    color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 45 else "#ef4444"
    return (
        f'<div style="background:#e5e7eb;border-radius:4px;height:8px;width:120px;display:inline-block;">'
        f'<div style="background:{color};width:{pct}%;height:8px;border-radius:4px;"></div></div>'
    )


def _index_badge(index_name: str) -> str:
    colors = {
        "Nifty 500":          ("#dbeafe", "#1d4ed8"),
        "Nifty Microcap 250": ("#fef3c7", "#b45309"),
        "NSE SME Emerge":     ("#f3e8ff", "#7c3aed"),
    }
    bg, fg = colors.get(index_name, ("#f3f4f6", "#374151"))
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:12px;font-size:11px;font-weight:600;">'
        f'{index_name}</span>'
    )


def build_html(df: pd.DataFrame, run_date: str) -> str:
    top = df.head(config.TOP_N).copy()

    rows_html = ""
    for rank, row in top.iterrows():
        bg = "#f9fafb" if rank % 2 == 0 else "#ffffff"
        ticker_clean = row["ticker"].replace(".NS", "")
        rows_html += f"""
        <tr style="background:{bg};">
          <td style="padding:10px 8px;font-weight:700;color:#6b7280;text-align:center;">{rank}</td>
          <td style="padding:10px 8px;">
            <div style="font-weight:700;font-size:14px;color:#111827;">{ticker_clean}</div>
            <div style="margin-top:3px;">{_index_badge(row['index'])}</div>
          </td>
          <td style="padding:10px 8px;font-weight:600;color:#111827;">₹{row['price']:,.2f}</td>
          <td style="padding:10px 8px;text-align:center;">
            <span style="font-weight:700;font-size:15px;color:#1d4ed8;">{row['momentum_score']}</span><br>
            {_signal_bar(row['momentum_score'])}
          </td>
          <td style="padding:10px 8px;text-align:center;color:{'#16a34a' if row['return_1m'].startswith('-') == False else '#dc2626'};font-weight:600;">{row['return_1m']}</td>
          <td style="padding:10px 8px;text-align:center;color:{'#16a34a' if row['return_3m'].startswith('-') == False else '#dc2626'};font-weight:600;">{row['return_3m']}</td>
          <td style="padding:10px 8px;text-align:center;color:{'#16a34a' if row['return_6m'].startswith('-') == False else '#dc2626'};font-weight:600;">{row['return_6m']}</td>
          <td style="padding:10px 8px;text-align:center;">{row['pct_from_52w']}</td>
          <td style="padding:10px 8px;text-align:center;">{row['rsi']}</td>
          <td style="padding:10px 8px;text-align:center;">{row['vol_ratio']}</td>
        </tr>"""

    index_counts = df.head(config.TOP_N)["index"].value_counts().to_dict()
    index_summary = " · ".join(
        [f"<b>{v}</b> {k.replace('Nifty ', '').replace('NSE ', '')}"
         for k, v in index_counts.items()]
    )

    html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">

<div style="max-width:900px;margin:30px auto;background:#ffffff;border-radius:12px;
            box-shadow:0 2px 12px rgba(0,0,0,0.08);overflow:hidden;">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1e40af 0%,#3b82f6 100%);
              padding:28px 32px;color:white;">
    <div style="font-size:22px;font-weight:800;letter-spacing:-0.3px;">
      📈 Daily Momentum Scanner
    </div>
    <div style="font-size:14px;opacity:0.85;margin-top:4px;">
      {run_date} · Top {config.TOP_N} stocks · Nifty 500 + Microcap 250 + SME Emerge
    </div>
    <div style="margin-top:12px;font-size:13px;background:rgba(255,255,255,0.15);
                display:inline-block;padding:6px 14px;border-radius:20px;">
      {index_summary}
    </div>
  </div>

  <!-- Score legend -->
  <div style="padding:14px 32px;background:#eff6ff;border-bottom:1px solid #dbeafe;
              font-size:12px;color:#1d4ed8;">
    <b>Score guide:</b>&nbsp;&nbsp;
    🟢 70–100 Strong momentum &nbsp;|&nbsp;
    🟡 45–69 Building &nbsp;|&nbsp;
    🔴 0–44 Weak
    &nbsp;&nbsp;·&nbsp;&nbsp;
    <b>Vol Ratio:</b> 20d avg vol ÷ 60d avg vol (>1.2x = accelerating)
  </div>

  <!-- Table -->
  <div style="overflow-x:auto;padding:0 16px 24px;">
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <thead>
        <tr style="border-bottom:2px solid #e5e7eb;">
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">#</th>
          <th style="padding:12px 8px;text-align:left;color:#6b7280;font-size:12px;">STOCK</th>
          <th style="padding:12px 8px;text-align:left;color:#6b7280;font-size:12px;">PRICE</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">SCORE</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">1M</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">3M</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">6M</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">vs 52W Hi</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">RSI</th>
          <th style="padding:12px 8px;text-align:center;color:#6b7280;font-size:12px;">VOL RATIO</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>

  <!-- Footer -->
  <div style="padding:16px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;
              font-size:11px;color:#9ca3af;line-height:1.6;">
    <b>Methodology:</b> Composite score = weighted percentile rank across 7 signals
    (1M/3M/6M returns, 52W high proximity, RSI-14 sweet zone 55–75, OBV slope, volume acceleration).
    All scores are cross-sectional — relative to today's universe, not absolute thresholds.<br>
    <b>Not investment advice.</b> Run your own thesis before acting on any name.
  </div>

</div>
</body>
</html>
"""
    return html


def send_email(df: pd.DataFrame) -> bool:
    run_date = datetime.now().strftime("%d %b %Y")
    subject = f"📈 Momentum Scanner · Top {config.TOP_N} · {run_date}"

    html_body = build_html(df, run_date)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.EMAIL_SENDER
    msg["To"] = config.EMAIL_RECIPIENT
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
            server.sendmail(config.EMAIL_SENDER, config.EMAIL_RECIPIENT, msg.as_string())
        log.info(f"Email sent to {config.EMAIL_RECIPIENT}")
        return True
    except Exception as e:
        log.error(f"Email send failed: {e}")
        return False
