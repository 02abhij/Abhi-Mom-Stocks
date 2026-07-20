"""
tickers.py — Fetch constituents from:
  1. Nifty 500          (NSE official CSV)
  2. Nifty Microcap 250 (NSE official CSV)
  3. Nifty Smallcap 250 / 50 (NSE official CSVs)
  4. NSE SME Emerge     (scraped from NSE website)

Now also captures the Industry column from NSE index CSVs, used by the
scanner for residual (sector-relative) momentum.

ticker_meta values are dicts: {"index": <index name>, "industry": <industry>}
(scanner.py tolerates old-style string values for backward compatibility).
"""

import requests
import pandas as pd
import io
import logging
import time

import config

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
    "Nifty Smallcap 250": "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    "Nifty Smallcap 50": "https://archives.nseindia.com/content/indices/ind_niftysmallcap50list.csv",
}


def _fetch_nse_csv(name: str, url: str) -> list[tuple[str, str]]:
    """Download an NSE index CSV. Returns list of (yahoo_ticker, industry)."""
    try:
        session = requests.Session()
        # NSE requires a cookie — seed it with a homepage visit
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
        time.sleep(1)
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        sym_col = next((c for c in df.columns if "symbol" in c.lower()), None)
        ind_col = next((c for c in df.columns if "industry" in c.lower()), None)
        if sym_col is None:
            log.warning(f"{name}: No 'Symbol' column found. Columns: {df.columns.tolist()}")
            return []
        out = []
        for _, row in df.iterrows():
            sym = str(row[sym_col]).strip()
            if not sym or sym.lower() == "nan":
                continue
            industry = str(row[ind_col]).strip() if ind_col and pd.notna(row.get(ind_col)) else "Unknown"
            out.append((sym + ".NS", industry))
        log.info(f"{name}: {len(out)} tickers fetched")
        return out
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


def get_all_tickers() -> tuple[list[str], dict]:
    """
    Returns:
        tickers     — deduplicated list of Yahoo Finance ticker strings
        ticker_meta — dict mapping ticker -> {"index": name, "industry": industry}
    """
    ticker_meta: dict = {}

    for name, url in NSE_INDEX_URLS.items():
        for t, industry in _fetch_nse_csv(name, url):
            if t not in ticker_meta:
                ticker_meta[t] = {"index": name, "industry": industry}

    for t in _fetch_sme_emerge():
        if t not in ticker_meta:
            ticker_meta[t] = {"index": "NSE SME Emerge", "industry": "SME (unclassified)"}

    for t in getattr(config, "EXTRA_TICKERS", []):
        if t not in ticker_meta:
            ticker_meta[t] = {"index": "Watchlist", "industry": "Unknown"}

    tickers = list(ticker_meta.keys())
    log.info(f"Total unique tickers across all indices: {len(tickers)}")

    cache_file = getattr(config, "UNIVERSE_CACHE_FILE", "history/universe_cache.csv")
    min_size = getattr(config, "MIN_UNIVERSE_SIZE", 500)

    if len(tickers) >= min_size:
        # Healthy fetch — refresh the cache
        try:
            import os
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            rows = []
            for tk, m in ticker_meta.items():
                if isinstance(m, dict):
                    rows.append({"ticker": tk, "index": m.get("index", "Unknown"),
                                 "industry": m.get("industry", "Unknown")})
                else:
                    rows.append({"ticker": tk, "index": str(m), "industry": "Unknown"})
            pd.DataFrame(rows).to_csv(cache_file, index=False)
            log.info(f"Universe cache refreshed: {len(rows)} tickers")
        except Exception as e:
            log.warning(f"Could not write universe cache: {e}")
    else:
        # Collapsed fetch — fall back to last good universe if available
        try:
            cached = pd.read_csv(cache_file)
            if len(cached) >= min_size:
                log.warning(f"NSE sources down ({len(tickers)} fetched) — "
                            f"FALLING BACK to cached universe of {len(cached)} tickers "
                            f"(from last successful run)")
                # Merge: cached universe + whatever live extras we did fetch
                for _, r in cached.iterrows():
                    if r["ticker"] not in ticker_meta:
                        ticker_meta[r["ticker"]] = {"index": r["index"], "industry": r["industry"]}
                tickers = list(ticker_meta.keys())
            else:
                log.warning("Universe cache too small to use as fallback")
        except FileNotFoundError:
            log.warning("No universe cache found — run once while NSE is up to create it")
        except Exception as e:
            log.warning(f"Universe cache fallback failed: {e}")

    return tickers, ticker_meta
