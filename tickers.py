"""
tickers.py — Fetch constituents from:
  1. Nifty 500          (NSE official CSV)
  2. Nifty Microcap 250 (NSE official CSV)
  3. NSE SME Emerge     (scraped from NSE website)
"""

import requests
import pandas as pd
import io
import logging
import time

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

NSE_INDEX_URLS = {
    "Nifty 500": "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    "Nifty Microcap 250": "https://archives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
}


def _fetch_nse_csv(name: str, url: str) -> list[str]:
    """Download an NSE index CSV and return list of Yahoo Finance tickers (.NS suffix)."""
    try:
        session = requests.Session()
        # NSE requires a cookie — seed it with a homepage visit
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
        time.sleep(1)
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        # Column is "Symbol" in NSE CSVs
        col = next((c for c in df.columns if "symbol" in c.lower()), None)
        if col is None:
            log.warning(f"{name}: No 'Symbol' column found. Columns: {df.columns.tolist()}")
            return []
        syms = df[col].dropna().str.strip().tolist()
        log.info(f"{name}: {len(syms)} tickers fetched")
        return [s + ".NS" for s in syms]
    except Exception as e:
        log.warning(f"{name}: fetch failed — {e}")
        return []


def _fetch_sme_emerge() -> list[str]:
    """
    Fetch NSE SME Emerge listed stocks.
    NSE provides a downloadable list via their emerge portal.
    Falls back to scraping if the direct URL changes.
    """
    urls_to_try = [
        # Primary: NSE Emerge CSV (periodically updated)
        "https://www.nseindia.com/emerge/homepage/equityListDownload.json",
        # Fallback: direct CSV sometimes exposed here
        "https://archives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv",
    ]

    session = requests.Session()
    try:
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
        time.sleep(1)
    except Exception:
        pass

    # Try the JSON endpoint first (NSE Emerge homepage download)
    try:
        r = session.get(urls_to_try[0], headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        # The JSON contains a list of dicts with "symbol" key
        syms = [item["symbol"].strip() for item in data if "symbol" in item]
        if syms:
            log.info(f"NSE SME Emerge: {len(syms)} tickers fetched (JSON)")
            return [s + ".NS" for s in syms]
    except Exception as e:
        log.warning(f"SME Emerge JSON fetch failed: {e}")

    # Try CSV fallback
    try:
        r = session.get(urls_to_try[1], headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        col = next((c for c in df.columns if "symbol" in c.lower()), None)
        if col:
            syms = df[col].dropna().str.strip().tolist()
            log.info(f"NSE SME Emerge: {len(syms)} tickers fetched (CSV fallback)")
            return [s + ".NS" for s in syms]
    except Exception as e:
        log.warning(f"SME Emerge CSV fallback failed: {e}")

    log.warning("SME Emerge: Could not fetch tickers — index will be excluded this run")
    return []


def get_all_tickers() -> tuple[list[str], dict[str, str]]:
    """
    Returns:
        tickers     — deduplicated list of Yahoo Finance ticker strings
        ticker_meta — dict mapping ticker -> index name (for reporting)
    """
    ticker_meta: dict[str, str] = {}

    for name, url in NSE_INDEX_URLS.items():
        for t in _fetch_nse_csv(name, url):
            if t not in ticker_meta:
                ticker_meta[t] = name

    for t in _fetch_sme_emerge():
        if t not in ticker_meta:
            ticker_meta[t] = "NSE SME Emerge"

    tickers = list(ticker_meta.keys())
    log.info(f"Total unique tickers across all indices: {len(tickers)}")
    return tickers, ticker_meta
