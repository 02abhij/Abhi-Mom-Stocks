"""
backtest.py — Historical validation of the two mechanical buy rules against
~3 years of full-universe data. Run via GitHub Actions (workflow_dispatch).

RULE 1 (Quiet Accumulation):
    VOL10D >= 3.0  AND  -3% <= 2W return <= +8%  AND  RSI14 <= 75
    AND 20d median turnover >= Rs 25 Cr

RULE 2 (Proven Persistence):
    In top-20 of daily composite for 5 consecutive days  AND  VOL-ADJ(3M) >= 2.5
    AND RSI14 <= 75  AND turnover >= Rs 25 Cr

For every historical signal (deduplicated: a stock can only re-signal after
20 trading days), measures 10/30/45-day forward returns and the EXCESS over
the universe median forward return in the same window. Also sweeps thresholds
so the cutoffs are chosen by data, not by prior.

KNOWN LIMITATION (printed in the report): the universe is TODAY'S index
membership — survivorship bias inflates absolute returns. Excess-vs-universe-
median partially controls for it (the survivors are also the benchmark), but
treat absolute numbers as optimistic and rely on the relative ones.
"""

import sys
import logging
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime

import config
import tickers as tickers_mod

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("backtest")

START = "2023-06-01"
FWD_HORIZONS = [10, 30, 45]
DEDUPE_DAYS = 20
MIN_TURN_LAKH = 2500          # Rs 25 Cr in lakh
RULE1_VOL10D = 3.0
RULE1_2W_LO, RULE1_2W_HI = -0.03, 0.08
RSI_CAP = 75
RULE2_TOPN = 20
RULE2_DAYS = 5
RULE2_VOLADJ = 2.5

BATCH = 100


# ── data ─────────────────────────────────────────────────────────────────────

def download_universe() -> dict[str, pd.DataFrame]:
    tks, meta = tickers_mod.get_all_tickers()
    log.info(f"Universe: {len(tks)} tickers; downloading from {START}")
    frames = {}
    for i in range(0, len(tks), BATCH):
        batch = tks[i:i + BATCH]
        try:
            raw = yf.download(batch, start=START, group_by="ticker",
                              auto_adjust=True, progress=False, threads=True)
        except Exception as e:
            log.warning(f"batch {i//BATCH+1} failed: {e}")
            continue
        for t in batch:
            try:
                df = raw[t].dropna(how="all") if len(batch) > 1 else raw
                if df is not None and len(df) >= 150:
                    frames[t] = df[["Close", "Volume"]].copy()
            except Exception:
                pass
        log.info(f"batch {i//BATCH+1}/{(len(tks)-1)//BATCH+1}: cumulative {len(frames)} ok")
    log.info(f"Downloaded usable history for {len(frames)} tickers")
    return frames


# ── per-stock signal series ──────────────────────────────────────────────────

def signal_frame(df: pd.DataFrame) -> pd.DataFrame:
    c, v = df["Close"], df["Volume"]
    out = pd.DataFrame(index=df.index)
    out["close"] = c
    out["ret2w"] = c.pct_change(10)
    out["ret1m"] = c.pct_change(21)
    out["ret3m"] = c.pct_change(63)
    out["ret6m"] = c.pct_change(126)
    out["vol10d"] = v.rolling(10).median() / v.rolling(40).median().shift(10)
    out["turn20"] = (c * v).rolling(20).median() / 1e5     # Rs lakh
    d = c.diff()
    ag = d.clip(lower=0).ewm(com=13, min_periods=14).mean()
    al = (-d.clip(upper=0)).ewm(com=13, min_periods=14).mean()
    out["rsi"] = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    vol63 = c.pct_change().rolling(63).std() * np.sqrt(63)
    out["voladj"] = out["ret3m"] / vol63
    hi20 = c.rolling(20).max()
    out["p20hi"] = c / hi20
    out["days_at_high"] = (c >= 0.98 * hi20).rolling(10).sum()
    out["p52"] = c / c.rolling(252, min_periods=126).max()
    direction = np.sign(d).fillna(0)
    obv = (direction * v).cumsum()
    out["obv_slope"] = (obv - obv.shift(20)) / 20 / obv.abs().rolling(60).mean()
    upv = v.where(d > 0, 0.0).rolling(10).sum()
    dnv = v.where(d < 0, 0.0).rolling(10).sum()
    out["accum"] = upv / dnv.replace(0, np.nan)
    return out


def build_panels(frames: dict) -> dict[str, pd.DataFrame]:
    """dict of signal-name -> DataFrame[date x ticker]."""
    per = {t: signal_frame(df) for t, df in frames.items()}
    cols = next(iter(per.values())).columns
    panels = {}
    for col in cols:
        panels[col] = pd.DataFrame({t: sf[col] for t, sf in per.items()})
    return panels


# ── composite score (mirrors scanner weights) ────────────────────────────────

def composite(panels: dict) -> pd.DataFrame:
    w = config.WEIGHTS
    mapping = {
        "return_2w": "ret2w", "return_1m": "ret1m", "return_3m": "ret3m",
        "return_6m": "ret6m", "pct_from_52w": "p52", "pct_from_20d_high": "p20hi",
        "days_at_high": "days_at_high", "vol_persist_10d": "vol10d",
        "accum_10d": "accum", "obv_slope": "obv_slope",
    }
    score = None
    for wkey, pkey in mapping.items():
        r = panels[pkey].rank(axis=1, pct=True)
        term = r * w[wkey]
        score = term if score is None else score.add(term, fill_value=0)
    rsi = panels["rsi"]
    lo, hi = config.RSI_SWEET_LOW, config.RSI_SWEET_HIGH
    rsi_score = pd.DataFrame(np.select(
        [rsi.isna(), (rsi >= lo) & (rsi <= hi), rsi < lo],
        [0.0, 1.0, (rsi / lo).clip(lower=0)],
        default=(1 - (rsi - hi) / (100 - hi)).clip(lower=0)),
        index=rsi.index, columns=rsi.columns)
    score = score.add(rsi_score * w["rsi"], fill_value=0)
    return score * 100


# ── rule masks & events ──────────────────────────────────────────────────────

def rule1_mask(panels, vol10d_th=RULE1_VOL10D, rsi_cap=RSI_CAP):
    return ((panels["vol10d"] >= vol10d_th) &
            (panels["ret2w"] >= RULE1_2W_LO) & (panels["ret2w"] <= RULE1_2W_HI) &
            (panels["rsi"] <= rsi_cap) &
            (panels["turn20"] >= MIN_TURN_LAKH))


def rule2_mask(panels, score, days=RULE2_DAYS, topn=RULE2_TOPN,
               voladj_th=RULE2_VOLADJ, rsi_cap=RSI_CAP):
    in_top = score.rank(axis=1, ascending=False) <= topn
    streak = in_top.rolling(days).sum() >= days
    return (streak &
            (panels["voladj"] >= voladj_th) &
            (panels["rsi"] <= rsi_cap) &
            (panels["turn20"] >= MIN_TURN_LAKH))


def dedupe_events(mask: pd.DataFrame) -> pd.DataFrame:
    """Keep only first signal day; suppress re-signals for DEDUPE_DAYS."""
    m = mask.fillna(False)
    fired_recently = m.shift(1).rolling(DEDUPE_DAYS, min_periods=1).max().fillna(0) > 0
    return m & ~fired_recently


def forward_excess(close: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """One row per event with forward and excess returns per horizon."""
    rows = []
    med = {}
    fwd = {}
    for n in FWD_HORIZONS:
        f = close.shift(-n) / close - 1
        fwd[n] = f
        med[n] = f.median(axis=1)
    ev = events.stack()
    ev = ev[ev].index  # (date, ticker) pairs
    for date, tkr in ev:
        row = {"date": date, "ticker": tkr, "price": close.at[date, tkr]}
        ok = True
        for n in FWD_HORIZONS:
            r = fwd[n].at[date, tkr]
            if pd.isna(r):
                ok = False
                break
            row[f"fwd{n}"] = r
            row[f"ex{n}"] = r - med[n].loc[date]
        if ok:
            rows.append(row)
    return pd.DataFrame(rows)


def summarize(name: str, ev: pd.DataFrame) -> str:
    if ev.empty:
        return f"\n== {name}: 0 measurable signals ==\n"
    lines = [f"\n== {name}: {len(ev)} signals "
             f"({ev['date'].min().date()} → {ev['date'].max().date()}) =="]
    for n in FWD_HORIZONS:
        ex, fw = ev[f"ex{n}"], ev[f"fwd{n}"]
        lines.append(
            f"  {n:>2}d fwd: hit-vs-median {(ex > 0).mean()*100:5.1f}% | "
            f"median excess {ex.median()*100:+6.2f}% | mean excess {ex.mean()*100:+6.2f}% | "
            f"abs>0 {(fw > 0).mean()*100:5.1f}% | worst {fw.min()*100:+6.1f}%")
    lines.append("  by year (30d median excess, n):")
    for yr, g in ev.groupby(ev["date"].dt.year):
        lines.append(f"    {yr}: {g['ex30'].median()*100:+6.2f}%  (n={len(g)})")
    return "\n".join(lines)


def main():
    frames = download_universe()
    if len(frames) < 100:
        log.error("Too few tickers downloaded — aborting.")
        sys.exit(1)
    panels = build_panels(frames)
    close = panels["close"]
    log.info(f"Panel: {close.shape[0]} days x {close.shape[1]} stocks")
    score = composite(panels)

    report = ["=" * 70,
              "MOMENTUM RULE BACKTEST — " + datetime.now().strftime("%d %b %Y %H:%M"),
              f"Universe: {close.shape[1]} stocks (TODAY'S membership — survivorship",
              "bias inflates absolute returns; trust EXCESS-vs-median numbers).",
              f"Dedupe: {DEDUPE_DAYS} trading days. Liquidity floor Rs 25 Cr.",
              "=" * 70]

    all_events = {}
    r1 = forward_excess(close, dedupe_events(rule1_mask(panels)))
    report.append(summarize("RULE 1 (VOL10D>=3.0, flat 2W, RSI<=75)", r1))
    all_events["rule1"] = r1

    r2 = forward_excess(close, dedupe_events(rule2_mask(panels, score)))
    report.append(summarize("RULE 2 (top-20 x 5d, VOL-ADJ>=2.5, RSI<=75)", r2))
    all_events["rule2"] = r2

    report.append("\n--- THRESHOLD SWEEPS (30d median excess | hit% | n) ---")
    for th in [2.0, 2.5, 3.0, 4.0]:
        e = forward_excess(close, dedupe_events(rule1_mask(panels, vol10d_th=th)))
        if len(e):
            report.append(f"  R1 VOL10D>={th}: {e['ex30'].median()*100:+6.2f}% | "
                          f"{(e['ex30']>0).mean()*100:4.1f}% | n={len(e)}")
    for d in [3, 5, 7]:
        e = forward_excess(close, dedupe_events(rule2_mask(panels, score, days=d)))
        if len(e):
            report.append(f"  R2 streak d{d}:  {e['ex30'].median()*100:+6.2f}% | "
                          f"{(e['ex30']>0).mean()*100:4.1f}% | n={len(e)}")
    for cap in [70, 75, 80]:
        e = forward_excess(close, dedupe_events(rule1_mask(panels, rsi_cap=cap)))
        if len(e):
            report.append(f"  R1 RSI<={cap}:   {e['ex30'].median()*100:+6.2f}% | "
                          f"{(e['ex30']>0).mean()*100:4.1f}% | n={len(e)}")

    text = "\n".join(report)
    print(text)
    import os
    os.makedirs("backtests", exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    with open(f"backtests/summary_{stamp}.txt", "w") as f:
        f.write(text)
    for name, ev in all_events.items():
        if len(ev):
            ev.to_csv(f"backtests/{name}_events_{stamp}.csv", index=False)
    log.info("Backtest complete; results in backtests/")


if __name__ == "__main__":
    main()
