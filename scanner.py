"""
scanner.py — Download price/volume history and compute momentum scores.

Momentum score components (weights defined in config.py):
  Price signals:
    return_2w          — 10-day total return (fresh momentum)
    return_1m          — 21-day total return
    return_3m          — 63-day total return
    return_6m          — 126-day total return
    pct_from_52w       — how close price is to 52-week high (0–100%)
    pct_from_20d_high  — how close price is to 20-day high (breakout signal)
    rsi                — RSI-14, scored highest in 55–75 sweet zone
  Volume signals:
    vol_surge          — latest day volume ÷ 20d avg volume (breakout confirmation)
    obv_slope          — normalised OBV linear regression slope (20 days)
    vol_ratio          — 20d avg volume ÷ 60d avg volume (acceleration signal)

Each raw signal is cross-sectionally ranked (percentile 0–1) across all
valid stocks, then weighted and summed → final composite score 0–100.

Additional outputs:
  turnover_l   — 20d median daily traded value in ₹ lakh (liquidity)
  extended     — blowoff flag (RSI > EXTENDED_RSI or 3M > EXTENDED_3M);
                 shown separately in the email, not mixed into main ranking
  is_new / streak_days / rank_change — from history.py (repo-committed memory)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
import history

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 380   # ~1.5 years of calendar days to cover 252 trading days


# ── Technical helpers ─────────────────────────────────────────────────────────

def _rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    return float(rsi_series.iloc[-1]) if len(rsi_series) >= period else np.nan


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def _obv_slope(obv_series: pd.Series, lookback: int = 20) -> float:
    """Normalised OBV slope: linear regression slope ÷ mean(|OBV|)."""
    if len(obv_series) < lookback:
        return np.nan
    y = obv_series.values[-lookback:].astype(float)
    x = np.arange(lookback)
    try:
        slope = np.polyfit(x, y, 1)[0]
    except Exception:
        return np.nan
    mean_abs = np.abs(y).mean()
    return float(slope / mean_abs) if mean_abs != 0 else np.nan


def _pct_return(close: pd.Series, days: int) -> float:
    if len(close) < days + 1:
        return np.nan
    start = close.iloc[-(days + 1)]
    end = close.iloc[-1]
    return float((end - start) / start) if start != 0 else np.nan


# ── Per-ticker extraction ─────────────────────────────────────────────────────

def _extract_signals(ticker: str, hist_df: pd.DataFrame) -> dict | None:
    try:
        close = hist_df["Close"].dropna()
        volume = hist_df["Volume"].dropna()
        close, volume = close.align(volume, join="inner")

        young = False
        if len(close) < config.MIN_HISTORY_DAYS:
            if len(close) >= getattr(config, "YOUNG_MIN_DAYS", 30):
                young = True   # compute what the data supports; shown separately
            else:
                return None

        price = float(close.iloc[-1])
        avg_vol_20 = float(volume.tail(20).mean())

        if price < config.MIN_PRICE:
            return None
        if avg_vol_20 < config.MIN_AVG_VOLUME:
            return None

        # Liquidity: 20d MEDIAN daily traded value (median resists one-day spikes)
        turnover_series = (close.tail(20) * volume.tail(20))
        turnover_l = float(turnover_series.median()) / 1e5   # ₹ lakh
        if turnover_l < config.MIN_TURNOVER_LAKH:
            return None

        high_52w = float(close.tail(252).max())
        pct_from_52w = (price / high_52w) * 100 if high_52w > 0 else np.nan

        # Breakout signal: proximity to 20-day high (100% = at/breaking the high)
        high_20d = float(close.tail(20).max())
        pct_from_20d_high = (price / high_20d) * 100 if high_20d > 0 else np.nan

        obv = _obv(close, volume)
        vol_60 = float(volume.tail(60).mean())
        vol_ratio = (avg_vol_20 / vol_60) if vol_60 > 0 else np.nan

        # Breakout confirmation: latest day's volume vs 20d average
        last_vol = float(volume.iloc[-1])
        vol_surge = (last_vol / avg_vol_20) if avg_vol_20 > 0 else np.nan

        # Persistence signals (7-15 session evidence, immune to one-day prints):
        vol_10_med = float(volume.tail(10).median())
        vol_prior40_med = float(volume.iloc[-50:-10].median()) if len(volume) >= 50 else np.nan
        vol_persist_10d = (vol_10_med / vol_prior40_med) if (vol_prior40_med and vol_prior40_med > 0) else np.nan

        last10_close = close.tail(11)          # 11 closes -> 10 day-over-day moves
        last10_vol = volume.tail(10)
        chg = last10_close.diff().dropna()
        up_vol = float(last10_vol[chg.values > 0].sum())
        dn_vol = float(last10_vol[chg.values < 0].sum())
        accum_10d = (up_vol / dn_vol) if dn_vol > 0 else (10.0 if up_vol > 0 else np.nan)

        days_at_high = int((close.tail(10) >= 0.98 * high_20d).sum())

        # Realized 3M volatility and vol-adjusted momentum (Barroso/Santa-Clara spirit):
        # return per unit of same-period volatility. Rewards Schneider-style low-vol
        # trends over lottery-ticket spikes of equal magnitude.
        daily_rets = close.pct_change().dropna()
        vol_3m = float(daily_rets.tail(63).std()) * np.sqrt(63) if len(daily_rets) >= 63 else np.nan
        ret_3m = _pct_return(close, 63)
        vol_adj_3m = (ret_3m / vol_3m) if (vol_3m and vol_3m > 0 and not pd.isna(ret_3m)) else np.nan

        return {
            "ticker":            ticker,
            "young":             young,
            "price":             round(price, 2),
            "avg_vol_20d":       int(avg_vol_20),
            "turnover_l":        turnover_l,
            "return_2w":         _pct_return(close, 10),
            "return_1m":         _pct_return(close, 21),
            "return_3m":         ret_3m,
            "return_6m":         _pct_return(close, 126),
            "pct_from_52w":      pct_from_52w,
            "pct_from_20d_high": pct_from_20d_high,
            "rsi":               _rsi(close),
            "obv_slope":         _obv_slope(obv),
            "vol_ratio":         vol_ratio,
            "vol_surge":         vol_surge,
            "vol_persist_10d":   vol_persist_10d,
            "accum_10d":         accum_10d,
            "days_at_high":      days_at_high,
            "vol_adj_3m":        vol_adj_3m,
            "52w_high":          round(high_52w, 2),
        }
    except Exception as e:
        log.debug(f"{ticker} signal extraction failed: {e}")
        return None


# ── Batch download ────────────────────────────────────────────────────────────

def _download_batch(tickers: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:
    """Download a batch of tickers. Returns dict ticker→DataFrame."""
    results = {}
    try:
        raw = yf.download(
            tickers,
            start=start,
            end=end,
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if len(tickers) == 1:
            t = tickers[0]
            results[t] = raw if not raw.empty else pd.DataFrame()
        else:
            for t in tickers:
                try:
                    df = raw[t].dropna(how="all")
                    if not df.empty:
                        results[t] = df
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"Batch download error: {e}")
    return results


# ── Main scan function ────────────────────────────────────────────────────────

def run_scan(tickers: list[str], ticker_meta: dict[str, str]) -> pd.DataFrame:
    """
    Downloads history for all tickers in batches, scores each,
    cross-sectionally ranks signals, annotates with history memory,
    saves today's snapshot, returns sorted DataFrame.
    """
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    log.info(f"Downloading history for {len(tickers)} tickers ({start_date} → {end_date})")

    # Download in batches of 100 to avoid yfinance timeouts
    BATCH = 100
    all_hist: dict[str, pd.DataFrame] = {}
    batches = [tickers[i:i + BATCH] for i in range(0, len(tickers), BATCH)]

    for i, batch in enumerate(batches, 1):
        log.info(f"Downloading batch {i}/{len(batches)} ({len(batch)} tickers)...")
        hist_batch = _download_batch(batch, start_date, end_date)
        all_hist.update(hist_batch)

    # Retry missing NSE Emerge names with Yahoo's -SM suffix (e.g. AKIKO-SM.NS).
    # Yahoo lists many SME Emerge tickers only under SYMBOL-SM.NS.
    missing_sme = [t for t in tickers
                   if t not in all_hist and t.endswith(".NS") and "-SM" not in t
                   and (ticker_meta.get(t, {}).get("index") if isinstance(ticker_meta.get(t), dict)
                        else ticker_meta.get(t)) == "NSE SME Emerge"]
    if missing_sme:
        retry_map = {t.replace(".NS", "-SM.NS"): t for t in missing_sme}
        log.info(f"Retrying {len(retry_map)} SME tickers with -SM suffix...")
        retry_batches = [list(retry_map.keys())[i:i + BATCH]
                         for i in range(0, len(retry_map), BATCH)]
        recovered = 0
        for batch in retry_batches:
            hist_batch = _download_batch(batch, start_date, end_date)
            for sm_ticker, df_hist in hist_batch.items():
                orig = retry_map[sm_ticker]
                all_hist[sm_ticker] = df_hist
                ticker_meta[sm_ticker] = ticker_meta.get(orig, {"index": "NSE SME Emerge",
                                                                "industry": "SME (unclassified)"})
                recovered += 1
        log.info(f"Recovered {recovered} SME tickers via -SM suffix")

    log.info(f"Got data for {len(all_hist)}/{len(tickers)} tickers (incl. -SM recoveries)")

    # Extract signals
    records = []
    for ticker, hist_df in all_hist.items():
        signals = _extract_signals(ticker, hist_df)
        if signals:
            meta = ticker_meta.get(ticker, {})
            if isinstance(meta, dict):
                signals["index"] = meta.get("index", "Unknown")
                signals["industry"] = meta.get("industry", "Unknown")
            else:  # legacy string meta
                signals["index"] = meta or "Unknown"
                signals["industry"] = "Unknown"
            records.append(signals)

    if not records:
        log.error("No valid signals extracted. Check data availability.")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    log.info(f"Valid stocks after quality/liquidity filters: {len(df)}")

    # ── Cross-sectional percentile ranking ────────────────────────────────────
    signal_cols = ["return_2w", "return_1m", "return_3m", "return_6m",
                   "pct_from_52w", "pct_from_20d_high", "days_at_high",
                   "obv_slope", "vol_persist_10d", "accum_10d"]

    for col in signal_cols:
        df[col + "_rank"] = df[col].rank(pct=True, na_option="bottom")

    # RSI: score highest in sweet zone (55–75), penalise overbought/oversold
    def rsi_score(rsi_val):
        if pd.isna(rsi_val):
            return 0.0
        if config.RSI_SWEET_LOW <= rsi_val <= config.RSI_SWEET_HIGH:
            return 1.0
        elif rsi_val < config.RSI_SWEET_LOW:
            return max(0, rsi_val / config.RSI_SWEET_LOW)
        else:  # overbought
            return max(0, 1 - (rsi_val - config.RSI_SWEET_HIGH) / (100 - config.RSI_SWEET_HIGH))

    df["rsi_rank"] = df["rsi"].apply(rsi_score)

    # ── Composite score (0–100) ───────────────────────────────────────────────
    w = config.WEIGHTS
    df["momentum_score"] = (
        df["return_2w_rank"]         * w["return_2w"]         +
        df["return_1m_rank"]         * w["return_1m"]         +
        df["return_3m_rank"]         * w["return_3m"]         +
        df["return_6m_rank"]         * w["return_6m"]         +
        df["pct_from_52w_rank"]      * w["pct_from_52w"]      +
        df["pct_from_20d_high_rank"] * w["pct_from_20d_high"] +
        df["days_at_high_rank"]      * w["days_at_high"]      +
        df["vol_persist_10d_rank"]   * w["vol_persist_10d"]   +
        df["accum_10d_rank"]         * w["accum_10d"]         +
        df["rsi_rank"]               * w["rsi"]               +
        df["obv_slope_rank"]         * w["obv_slope"]
    ) * 100

    # ── Residual (sector-relative) momentum ──────────────────────────────────
    # Stock's 3M return minus its industry's median 3M return across the FULL
    # valid universe (not just the top N) — how much of the move is the stock,
    # not the sector costume. Industries with <3 names fall back to 0 residual
    # weight of evidence (resid vs a 1-2 name "median" is noise).
    ind_counts = df.groupby("industry")["ticker"].transform("count")
    ind_median = df.groupby("industry")["return_3m"].transform("median")
    df["resid_3m"] = np.where(ind_counts >= 3, df["return_3m"] - ind_median, np.nan)

    # Numeric copies preserved for the emailer's sector-cluster panel and history
    df["return_3m_num"] = df["return_3m"]
    df["return_1m_num"] = df["return_1m"]

    # Young listings carry no composite score — their signal set is incomplete
    # and cross-sectional ranks vs the mature universe would be misleading.
    df.loc[df["young"], "momentum_score"] = np.nan

    # ── Blowoff / extended flag (computed on RAW numerics, before formatting) ─
    df["extended"] = (
        (df["rsi"] > config.EXTENDED_RSI) |
        (df["return_3m"] > config.EXTENDED_3M)
    )

    df = df.sort_values("momentum_score", ascending=False).reset_index(drop=True)
    df.index += 1  # rank starts at 1
    df["momentum_score"] = df["momentum_score"].round(1)  # NaN (young) stays NaN, sorts last

    # ── History memory: annotate + save today's snapshot ─────────────────────
    try:
        df = history.annotate(df)
        history.save_today(df)
    except Exception as e:
        log.warning(f"History step failed (non-fatal): {e}")
        for col, default in [("is_new", False), ("streak_days", 1), ("rank_change", pd.NA)]:
            if col not in df.columns:
                df[col] = default

    # ── Display formatting (strings) ──────────────────────────────────────────
    for col in ["return_2w", "return_1m", "return_3m", "return_6m"]:
        df[col] = df[col].map(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")

    df["pct_from_52w"] = df["pct_from_52w"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
    df["pct_from_20d_high"] = df["pct_from_20d_high"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
    df["rsi"] = df["rsi"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    df["vol_ratio"] = df["vol_ratio"].map(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")
    df["vol_surge"] = df["vol_surge"].map(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")
    df["vol_persist_10d"] = df["vol_persist_10d"].map(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")
    df["accum_10d"] = df["accum_10d"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    # days_at_high stays an integer 0-10
    df["vol_adj_3m"] = df["vol_adj_3m"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    df["resid_3m"] = df["resid_3m"].map(
        lambda x: (f"+{x*100:.0f}%" if x >= 0 else f"{x*100:.0f}%") if pd.notna(x) else "—"
    )
    df["turnover_l"] = df["turnover_l"].map(
        lambda x: (f"₹{x/100:.1f}Cr" if x >= 100 else f"₹{x:.0f}L") if pd.notna(x) else "N/A"
    )

    return df
