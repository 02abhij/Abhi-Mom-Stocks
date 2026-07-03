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
"""

import yfinance as yf
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import config

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

def _extract_signals(ticker: str, hist: pd.DataFrame) -> dict | None:
    try:
        close = hist["Close"].dropna()
        volume = hist["Volume"].dropna()
        close, volume = close.align(volume, join="inner")

        if len(close) < config.MIN_HISTORY_DAYS:
            return None

        price = float(close.iloc[-1])
        avg_vol_20 = float(volume.tail(20).mean())

        if price < config.MIN_PRICE:
            return None
        if avg_vol_20 < config.MIN_AVG_VOLUME:
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

        return {
            "ticker":            ticker,
            "price":             round(price, 2),
            "avg_vol_20d":       int(avg_vol_20),
            "return_2w":         _pct_return(close, 10),
            "return_1m":         _pct_return(close, 21),
            "return_3m":         _pct_return(close, 63),
            "return_6m":         _pct_return(close, 126),
            "pct_from_52w":      pct_from_52w,
            "pct_from_20d_high": pct_from_20d_high,
            "rsi":               _rsi(close),
            "obv_slope":         _obv_slope(obv),
            "vol_ratio":         vol_ratio,
            "vol_surge":         vol_surge,
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
    cross-sectionally ranks signals, returns sorted DataFrame.
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
        hist = _download_batch(batch, start_date, end_date)
        all_hist.update(hist)

    log.info(f"Got data for {len(all_hist)}/{len(tickers)} tickers")

    # Extract signals
    records = []
    for ticker, hist in all_hist.items():
        signals = _extract_signals(ticker, hist)
        if signals:
            signals["index"] = ticker_meta.get(ticker, "Unknown")
            records.append(signals)

    if not records:
        log.error("No valid signals extracted. Check data availability.")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    log.info(f"Valid stocks after quality filters: {len(df)}")

    # ── Cross-sectional percentile ranking ────────────────────────────────────
    signal_cols = ["return_2w", "return_1m", "return_3m", "return_6m",
                   "pct_from_52w", "pct_from_20d_high",
                   "obv_slope", "vol_ratio", "vol_surge"]

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
        df["vol_surge_rank"]         * w["vol_surge"]         +
        df["rsi_rank"]               * w["rsi"]               +
        df["obv_slope_rank"]         * w["obv_slope"]         +
        df["vol_ratio_rank"]         * w["vol_ratio"]
    ) * 100

    df = df.sort_values("momentum_score", ascending=False).reset_index(drop=True)
    df.index += 1  # rank starts at 1

    # Format return columns as %
    for col in ["return_2w", "return_1m", "return_3m", "return_6m"]:
        df[col] = df[col].map(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")

    df["pct_from_52w"] = df["pct_from_52w"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
    df["pct_from_20d_high"] = df["pct_from_20d_high"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
    df["rsi"] = df["rsi"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    df["momentum_score"] = df["momentum_score"].round(1)
    df["vol_ratio"] = df["vol_ratio"].map(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")
    df["vol_surge"] = df["vol_surge"].map(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")

    return df
