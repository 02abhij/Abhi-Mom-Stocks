"""
indian_market_analysis.py — What is common among 20%+ CAGR compounders?
"""
 
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import os
import logging
import warnings
warnings.filterwarnings('ignore')
 
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
 
EMAIL_SENDER    = os.environ.get("EMAIL_SENDER", "02abhij@gmail.com")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "abhi@ajassoc.in")
SMTP_HOST       = "smtp.gmail.com"
SMTP_PORT       = 587
FUNDAMENTALS_CSV = "screener_fundamentals.csv"
BATCH_SIZE       = 100
CAGR_THRESHOLD   = 20.0
 
def load_fundamentals():
    df = pd.read_csv(FUNDAMENTALS_CSV)
    df = df[df['NSE Code'].notna() & (df['NSE Code'].astype(str).str.strip() != '')]
    df['ticker'] = df['NSE Code'].astype(str).str.strip() + '.NS'
    log.info(f"Loaded {len(df)} stocks from Screener")
    return df
 
def compute_cagr(start_price, end_price, years):
    if pd.isna(start_price) or pd.isna(end_price) or start_price <= 0 or end_price <= 0:
        return np.nan
    return ((end_price / start_price) ** (1 / years) - 1) * 100
 
def download_and_compute_cagrs(tickers):
    log.info(f"Downloading price history for {len(tickers)} tickers...")
    all_prices = {}
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    for i, batch in enumerate(batches, 1):
        log.info(f"  Batch {i}/{len(batches)}...")
        try:
            raw = yf.download(batch, start="2013-12-01", end="2025-01-15",
                              auto_adjust=True, progress=False, threads=True)
            if len(batch) == 1:
                t = batch[0]
                if not raw.empty:
                    all_prices[t] = raw['Close']
            else:
                for t in batch:
                    try:
                        s = raw['Close'][t].dropna()
                        if len(s) > 200:
                            all_prices[t] = s
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"Batch {i} failed: {e}")
 
    log.info(f"Got price history for {len(all_prices)} tickers")
 
    def get_avg_price(series, start, end):
        window = series[(series.index >= start) & (series.index <= end)]
        return float(window.mean()) if len(window) >= 5 else np.nan
 
    records = []
    for ticker, prices in all_prices.items():
        p2014 = get_avg_price(prices, '2014-01-01', '2014-03-31')
        p2019 = get_avg_price(prices, '2019-01-01', '2019-03-31')
        p2024 = get_avg_price(prices, '2024-10-01', '2024-12-31')
        records.append({
            'ticker':     ticker,
            'price_2014': round(p2014, 2) if pd.notna(p2014) else np.nan,
            'price_2019': round(p2019, 2) if pd.notna(p2019) else np.nan,
            'price_2024': round(p2024, 2) if pd.notna(p2024) else np.nan,
            'cagr_10y':   round(compute_cagr(p2014, p2024, 10), 1) if pd.notna(p2014) and pd.notna(p2024) else np.nan,
            'cagr_5y':    round(compute_cagr(p2019, p2024, 5), 1)  if pd.notna(p2019) and pd.notna(p2024) else np.nan,
        })
    return pd.DataFrame(records)
 
def classify(cagr_df, fundamentals):
    df = cagr_df.merge(fundamentals, on='ticker', how='left')
    df['compounder_5y']     = df['cagr_5y']  >= CAGR_THRESHOLD
    df['compounder_10y']    = df['cagr_10y'] >= CAGR_THRESHOLD
    df['compounder_either'] = df['compounder_5y'] | df['compounder_10y']
    log.info(f"5Y 20%+ compounders: {df['compounder_5y'].sum()} | "
             f"10Y 20%+: {df['compounder_10y'].sum()} | "
             f"Either: {df['compounder_either'].sum()} out of {len(df)}")
    return df
 
NUMERIC_VARS = [
    ('Price to Earning',                  'PE ratio'),
    ('Return on equity',                  'ROE %'),
    ('Debt to equity',                    'Debt/Equity'),
    ('Sales growth 5Years',               'Revenue CAGR 5Y %'),
    ('Profit growth 5Years',              'PAT CAGR 5Y %'),
    ('Promoter holding',                  'Promoter holding %'),
    ('Change in promoter holding 3Years', 'Promoter chg 3Y %'),
    ('Pledged percentage',                'Pledged %'),
    ('Dividend yield',                    'Dividend yield %'),
    ('Price to book value',               'P/B ratio'),
    ('Market Capitalization',             'Market cap (Cr)'),
]
 
def compare_groups(df, group_col):
    comp  = df[df[group_col] == True]
    ncomp = df[df[group_col] == False]
    rows = []
    for col, label in NUMERIC_VARS:
        if col not in df.columns:
            continue
        cv  = comp[col].dropna()
        ncv = ncomp[col].dropna()
        if len(cv) < 10 or len(ncv) < 10:
            continue
        c_med  = cv.median()
        nc_med = ncv.median()
        pct_diff = (c_med - nc_med) / abs(nc_med) * 100 if nc_med != 0 else np.nan
        row = {
            'Variable':               label,
            'Compounders median':     round(c_med, 2),
            'Non-compounders median': round(nc_med, 2),
            'Difference %':           round(pct_diff, 1) if pd.notna(pct_diff) else 'N/A',
            'Compounders count':      len(cv),
        }
        if col == 'Profit growth 5Years':
            row['% compounders with 20%+ PAT growth'] = round((cv > 20).mean() * 100, 1)
            row['% rest with 20%+ PAT growth']        = round((ncv > 20).mean() * 100, 1)
        if col == 'Sales growth 5Years':
            row['% compounders with 15%+ rev growth'] = round((cv > 15).mean() * 100, 1)
            row['% rest with 15%+ rev growth']        = round((ncv > 15).mean() * 100, 1)
        if col == 'Return on equity':
            row['% compounders with ROE 15%+'] = round((cv > 15).mean() * 100, 1)
            row['% rest with ROE 15%+']        = round((ncv > 15).mean() * 100, 1)
        rows.append(row)
    result = pd.DataFrame(rows)
    result['_abs'] = pd.to_numeric(result['Difference %'], errors='coerce').abs()
    return result.sort_values('_abs', ascending=False).drop('_abs', axis=1).reset_index(drop=True)
 
def cagr_distribution(df):
    bins   = [-np.inf, 0, 10, 15, 20, 25, 30, np.inf]
    labels = ['<0%','0–10%','10–15%','15–20%','20–25%','25–30%','>30%']
    d5  = pd.cut(df['cagr_5y'].dropna(),  bins=bins, labels=labels).value_counts().sort_index()
    d10 = pd.cut(df['cagr_10y'].dropna(), bins=bins, labels=labels).value_counts().sort_index()
    out = pd.DataFrame({'CAGR bucket': labels, 'Count 5Y': d5.values, 'Count 10Y': d10.values})
    out['% of universe 5Y']  = (out['Count 5Y']  / out['Count 5Y'].sum()  * 100).round(1)
    out['% of universe 10Y'] = (out['Count 10Y'] / out['Count 10Y'].sum() * 100).round(1)
    return out
 
def sector_breakdown(df, group_col):
    comp = df[df[group_col] == True]
    total = df.groupby('Industry Group').size().reset_index(name='Total')
    comps = comp.groupby('Industry Group').size().reset_index(name='Compounders')
    merged = total.merge(comps, on='Industry Group', how='left').fillna(0)
    merged['Compounders'] = merged['Compounders'].astype(int)
    merged['Hit rate %']  = (merged['Compounders'] / merged['Total'] * 100).round(1)
    return merged.sort_values('Hit rate %', ascending=False).head(20).reset_index(drop=True)
 
def build_excel(df, var_5y, var_10y, dist, sec5, sec10, comp_list):
    wb = Workbook()
    BLUE = "1e40af"; WHITE = "ffffff"; LGREY = "f9fafb"
    hf = Font(name='Arial', bold=True, color=WHITE, size=11)
    hfill = PatternFill('solid', start_color=BLUE)
    nf = Font(name='Arial', size=10)
    af = PatternFill('solid', start_color=LGREY)
    ctr = Alignment(horizontal='center', vertical='center')
 
    def write_sheet(ws, title, data):
        ws.title = title
        cols = data.columns.tolist()
        for j, col in enumerate(cols, 1):
            c = ws.cell(row=1, column=j, value=col)
            c.font = hf; c.fill = hfill; c.alignment = ctr
        for i, row in enumerate(data.itertuples(index=False), 2):
            for j, val in enumerate(row, 1):
                c = ws.cell(row=i, column=j, value='' if pd.isna(val) else val)
                c.font = nf; c.alignment = ctr
                if i % 2 == 0: c.fill = af
        for j in range(1, len(cols)+1):
            ws.column_dimensions[get_column_letter(j)].width = 24
        ws.freeze_panes = 'A2'
 
    ws1 = wb.active; write_sheet(ws1, "5Y Compounders vs Rest", var_5y)
    ws2 = wb.create_sheet(); write_sheet(ws2, "10Y Compounders vs Rest", var_10y)
    ws3 = wb.create_sheet(); write_sheet(ws3, "CAGR Distribution", dist)
    ws4 = wb.create_sheet(); write_sheet(ws4, "Sector Hit Rates 5Y", sec5)
    ws5 = wb.create_sheet(); write_sheet(ws5, "Sector Hit Rates 10Y", sec10)
    ws6 = wb.create_sheet(); write_sheet(ws6, "All Compounders", comp_list)
 
    path = "indian_market_analysis.xlsx"
    wb.save(path)
    log.info(f"Excel saved: {path}")
    return path
 
def send_email(excel_path, var_5y):
    top5 = var_5y.head(5)
    rows_html = ''.join(
        f"<tr><td style='padding:8px;'>{r['Variable']}</td>"
        f"<td style='padding:8px;text-align:center;font-weight:bold;'>{r['Compounders median']}</td>"
        f"<td style='padding:8px;text-align:center;'>{r['Non-compounders median']}</td>"
        f"<td style='padding:8px;text-align:center;font-weight:bold;color:#16a34a;'>{r['Difference %']}%</td></tr>"
        for _, r in top5.iterrows()
    )
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;">
      <div style="background:linear-gradient(135deg,#1e40af,#3b82f6);padding:28px;color:white;border-radius:12px 12px 0 0;">
        <div style="font-size:22px;font-weight:800;">What makes a 20%+ CAGR compounder?</div>
        <div style="font-size:14px;opacity:0.85;margin-top:4px;">Indian market · 5Y and 10Y analysis · {len(var_5y)} variables tested</div>
      </div>
      <div style="padding:24px;background:white;border:1px solid #e5e7eb;">
        <h3 style="color:#1e40af;">Top discriminating variables (5Y compounders vs rest)</h3>
        <table style="border-collapse:collapse;width:100%;font-size:13px;">
          <tr style="background:#dbeafe;">
            <th style="padding:8px;text-align:left;">Variable</th>
            <th style="padding:8px;text-align:center;">Compounders</th>
            <th style="padding:8px;text-align:center;">Rest</th>
            <th style="padding:8px;text-align:center;">Difference</th>
          </tr>
          {rows_html}
        </table>
        <p style="font-size:12px;color:#9ca3af;margin-top:20px;">Full analysis attached. 6 sheets: 5Y + 10Y variable comparison, CAGR distribution, sector hit rates, full compounder list.</p>
      </div>
    </div>"""
    msg = MIMEMultipart()
    msg['Subject'] = "What makes a 20%+ CAGR compounder — India 2014–2024"
    msg['From'] = EMAIL_SENDER; msg['To'] = EMAIL_RECIPIENT
    msg.attach(MIMEText(html, 'html'))
    with open(excel_path, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="indian_market_analysis.xlsx"')
        msg.attach(part)
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
        log.info("Email sent")
    except Exception as e:
        log.error(f"Email failed: {e}")
 
def main():
    log.info("=" * 60)
    log.info("WHAT MAKES A 20%+ CAGR COMPOUNDER — INDIA 2014–2024")
    log.info("=" * 60)
    fundamentals = load_fundamentals()
    tickers = fundamentals['ticker'].tolist()
    cagr_df = download_and_compute_cagrs(tickers)
    df = classify(cagr_df, fundamentals)
    var_5y  = compare_groups(df, 'compounder_5y')
    var_10y = compare_groups(df, 'compounder_10y')
    log.info("\n5Y COMPOUNDERS vs REST")
    log.info("\n" + var_5y.to_string(index=False))
    log.info("\n10Y COMPOUNDERS vs REST")
    log.info("\n" + var_10y.to_string(index=False))
    dist   = cagr_distribution(df)
    log.info("\nCAGR DISTRIBUTION\n" + dist.to_string(index=False))
    sec5   = sector_breakdown(df, 'compounder_5y')
    sec10  = sector_breakdown(df, 'compounder_10y')
    log.info("\nSECTOR HIT RATES 5Y\n" + sec5.to_string(index=False))
    comp_cols = ['ticker','Name','Industry Group','cagr_5y','cagr_10y',
                 'Price to Earning','Return on equity','Sales growth 5Years',
                 'Profit growth 5Years','Debt to equity','Promoter holding',
                 'Pledged percentage','Market Capitalization']
    comp_cols = [c for c in comp_cols if c in df.columns]
    comp_list = df[df['compounder_either']==True][comp_cols].sort_values('cagr_5y', ascending=False)
    log.info(f"\nTotal compounders: {len(comp_list)}")
    excel_path = build_excel(df, var_5y, var_10y, dist, sec5, sec10, comp_list)
    send_email(excel_path, var_5y)
    log.info("DONE.")
 
if __name__ == "__main__":
    main()
 
