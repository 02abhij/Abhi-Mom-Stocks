"""
emailer.py — Format and send the daily momentum report as a styled HTML email.

Layout:
  1. Fresh Breakouts strip — NEW entrants today (not on yesterday's list)
  2. Main ranking table    — with NEW badge, streak, rank-change, turnover
  3. Extended section      — blowoff names (RSI/3M threshold) pulled out of ranking
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
    pct = min(int((score / max_score) * 100), 100)
    color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 45 else "#ef4444"
    return (
        f'<div style="background:#e5e7eb;border-radius:4px;height:8px;width:110px;display:inline-block;">'
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


def _ret_color(val: str) -> str:
    return "#dc2626" if str(val).startswith("-") else "#16a34a"


def _col(row, name, default="N/A"):
    try:
        v = row[name]
        return default if pd.isna(v) else v
    except (KeyError, TypeError):
        return default


def _new_badge() -> str:
    return ('<span style="background:#dcfce7;color:#15803d;padding:2px 7px;'
            'border-radius:10px;font-size:10px;font-weight:800;">NEW</span>')


def _momentum_cell(row) -> str:
    """Combined memory cell: NEW badge or rank-change arrow + streak days."""
    if _col(row, "is_new", False) is True:
        badge = _new_badge()
    else:
        rc = _col(row, "rank_change", None)
        if rc is None or rc == "N/A":
            badge = '<span style="color:#9ca3af;">—</span>'
        else:
            rc = int(rc)
            if rc > 0:
                badge = f'<span style="color:#16a34a;font-weight:700;">▲{rc}</span>'
            elif rc < 0:
                badge = f'<span style="color:#dc2626;font-weight:700;">▼{abs(rc)}</span>'
            else:
                badge = '<span style="color:#9ca3af;">=</span>'
    streak = _col(row, "streak_days", 1)
    return f'{badge}<br><span style="font-size:10px;color:#6b7280;">d{streak}</span>'


def _row_html(rank, row, extended_style: bool = False) -> str:
    bg = "#fff7ed" if extended_style else ("#f9fafb" if rank % 2 == 0 else "#ffffff")
    ticker_clean = row["ticker"].replace(".NS", "")

    ret_2w = _col(row, "return_2w")
    ret_1m = _col(row, "return_1m")
    ret_3m = _col(row, "return_3m")
    ret_6m = _col(row, "return_6m")

    return f"""
    <tr style="background:{bg};">
      <td style="padding:9px 7px;font-weight:700;color:#6b7280;text-align:center;">{rank}</td>
      <td style="padding:9px 7px;text-align:center;">{_momentum_cell(row)}</td>
      <td style="padding:9px 7px;">
        <div style="font-weight:700;font-size:14px;color:#111827;">{ticker_clean}{' 🔥' if extended_style else ''}</div>
        <div style="margin-top:3px;">{_index_badge(row['index'])}</div>
      </td>
      <td style="padding:9px 7px;font-weight:600;color:#111827;">₹{row['price']:,.2f}</td>
      <td style="padding:9px 7px;text-align:center;">
        <span style="font-weight:700;font-size:15px;color:#1d4ed8;">{row['momentum_score']}</span><br>
        {_signal_bar(row['momentum_score'])}
      </td>
      <td style="padding:9px 7px;text-align:center;color:{_ret_color(ret_2w)};font-weight:700;">{ret_2w}</td>
      <td style="padding:9px 7px;text-align:center;color:{_ret_color(ret_1m)};font-weight:600;">{ret_1m}</td>
      <td style="padding:9px 7px;text-align:center;color:{_ret_color(ret_3m)};font-weight:600;">{ret_3m}</td>
      <td style="padding:9px 7px;text-align:center;color:{_ret_color(ret_6m)};font-weight:600;">{ret_6m}</td>
      <td style="padding:9px 7px;text-align:center;font-weight:600;">{_col(row, 'pct_from_20d_high')}</td>
      <td style="padding:9px 7px;text-align:center;">{_col(row, 'pct_from_52w')}</td>
      <td style="padding:9px 7px;text-align:center;font-weight:600;">{_col(row, 'vol_surge')}</td>
      <td style="padding:9px 7px;text-align:center;">{_col(row, 'rsi')}</td>
      <td style="padding:9px 7px;text-align:center;color:#6b7280;">{_col(row, 'turnover_l')}</td>
    </tr>"""


_THEAD = """
    <tr style="border-bottom:2px solid #e5e7eb;">
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">#</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">Δ</th>
      <th style="padding:11px 7px;text-align:left;color:#6b7280;font-size:11px;">STOCK</th>
      <th style="padding:11px 7px;text-align:left;color:#6b7280;font-size:11px;">PRICE</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">SCORE</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">2W</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">1M</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">3M</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">6M</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">vs 20D Hi</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">vs 52W Hi</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">VOL SURGE</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">RSI</th>
      <th style="padding:11px 7px;text-align:center;color:#6b7280;font-size:11px;">TURNOVER</th>
    </tr>"""


def build_html(df: pd.DataFrame, run_date: str) -> str:
    top = df.head(config.TOP_N).copy()

    has_ext = "extended" in top.columns
    main = top[~top["extended"]] if has_ext else top
    ext = top[top["extended"]] if has_ext else top.iloc[0:0]

    # Fresh breakouts strip: NEW names in the main list
    new_names = []
    if "is_new" in top.columns:
        new_names = [r["ticker"].replace(".NS", "") + (" 🔥" if _col(r, "extended", False) else "")
                     for _, r in top[top["is_new"] == True].iterrows()]

    fresh_strip = ""
    if new_names:
        chips = " ".join(
            f'<span style="background:#dcfce7;color:#15803d;padding:4px 10px;'
            f'border-radius:14px;font-size:12px;font-weight:700;display:inline-block;'
            f'margin:2px;">{n}</span>' for n in new_names
        )
        fresh_strip = f"""
  <div style="padding:14px 32px;background:#f0fdf4;border-bottom:1px solid #bbf7d0;">
    <div style="font-size:12px;font-weight:800;color:#15803d;margin-bottom:6px;">
      ⚡ FRESH BREAKOUTS — new on the list today ({len(new_names)})
    </div>
    {chips}
  </div>"""

    main_rows = "".join(_row_html(rank, row) for rank, row in main.iterrows())
    ext_rows = "".join(_row_html(rank, row, extended_style=True) for rank, row in ext.iterrows())

    ext_section = ""
    if len(ext) > 0:
        ext_section = f"""
  <div style="padding:14px 32px 4px;font-size:13px;font-weight:800;color:#b45309;">
    🔥 EXTENDED — momentum present, persistence odds poor
    (RSI &gt; {config.EXTENDED_RSI} or 3M &gt; {int(config.EXTENDED_3M*100)}%) · {len(ext)} names
  </div>
  <div style="overflow-x:auto;padding:0 16px 24px;">
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead>{_THEAD}</thead>
      <tbody>{ext_rows}</tbody>
    </table>
  </div>"""

    index_counts = top["index"].value_counts().to_dict()
    index_summary = " · ".join(
        [f"<b>{v}</b> {k.replace('Nifty ', '').replace('NSE ', '')}"
         for k, v in index_counts.items()]
    )

    html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">

<div style="max-width:1100px;margin:30px auto;background:#ffffff;border-radius:12px;
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
{fresh_strip}
  <!-- Legend -->
  <div style="padding:14px 32px;background:#eff6ff;border-bottom:1px solid #dbeafe;
              font-size:12px;color:#1d4ed8;line-height:1.7;">
    <b>Δ column:</b> NEW = first day on list · ▲▼ = rank change vs previous run · dN = consecutive days on list
    &nbsp;·&nbsp; <b>vs 20D Hi:</b> 100% = at/breaking 20-day high
    &nbsp;·&nbsp; <b>Vol Surge:</b> today's volume ÷ 20d avg (&gt;2x = breakout confirmation)
    &nbsp;·&nbsp; <b>Turnover:</b> 20d median daily traded value
  </div>

  <!-- Main table -->
  <div style="overflow-x:auto;padding:0 16px 24px;">
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead>{_THEAD}</thead>
      <tbody>{main_rows}</tbody>
    </table>
  </div>
{ext_section}
  <!-- Footer -->
  <div style="padding:16px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;
              font-size:11px;color:#9ca3af;line-height:1.6;">
    <b>Methodology:</b> Composite score = weighted percentile rank across 10 signals
    (2W/1M/3M/6M returns, 20D &amp; 52W high proximity, volume surge, RSI-14 sweet zone 55–75,
    OBV slope, volume acceleration). ~40% of weight on sub-1-month signals.
    Liquidity floor: 20d median turnover ≥ ₹{config.MIN_TURNOVER_LAKH}L/day.
    Extended names (blowoff profile) are ranked but shown separately.
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
    n_new = int(df.head(config.TOP_N)["is_new"].sum()) if "is_new" in df.columns else 0
    new_tag = f" · ⚡{n_new} new" if n_new else ""
    subject = f"📈 Momentum Scanner · Top {config.TOP_N}{new_tag} · {run_date}"

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
