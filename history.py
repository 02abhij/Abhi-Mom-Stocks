"""
history.py — Persist daily scan results and annotate today's list with memory.

Stores one CSV (history/momentum_history.csv, committed back to the repo by
the workflow) with columns: date, ticker, rank, momentum_score.

Annotations added to today's DataFrame:
  is_new       — True if ticker was NOT in yesterday's saved list
  streak_days  — consecutive run-days the ticker has appeared (incl. today)
  rank_change  — yesterday's rank minus today's rank (+ve = moved up)
"""

import os
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

import config

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
HISTORY_FILE = getattr(config, "HISTORY_FILE", "history/momentum_history.csv")
KEEP_DAYS = getattr(config, "HISTORY_KEEP_DAYS", 180)


def _today_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def load_history() -> pd.DataFrame:
    if not os.path.exists(HISTORY_FILE):
        return pd.DataFrame(columns=["date", "ticker", "rank", "momentum_score"])
    try:
        h = pd.read_csv(HISTORY_FILE, dtype={"ticker": str})
        h["date"] = h["date"].astype(str)
        return h
    except Exception as e:
        log.warning(f"Could not read history file ({e}) — starting fresh")
        return pd.DataFrame(columns=["date", "ticker", "rank", "momentum_score"])


def annotate(df: pd.DataFrame) -> pd.DataFrame:
    """Add is_new / streak_days / rank_change columns using saved history.
    Expects df sorted by momentum_score with index = today's rank (1-based)."""
    hist = load_history()
    today = _today_str()

    # Exclude any rows already saved for today (re-runs on the same day)
    hist = hist[hist["date"] != today]

    df = df.copy()

    if hist.empty:
        df["is_new"] = False          # first ever run: nothing is meaningfully "new"
        df["streak_days"] = 1
        df["rank_change"] = pd.NA
        return df

    dates = sorted(hist["date"].unique(), reverse=True)
    last_date = dates[0]
    last_ranks = (hist[hist["date"] == last_date]
                  .set_index("ticker")["rank"].to_dict())

    # Sets of tickers per past date, newest first, for streak counting
    date_sets = [set(hist.loc[hist["date"] == d, "ticker"]) for d in dates]

    is_new, streaks, rank_chg = [], [], []
    for today_rank, row in df.iterrows():
        t = row["ticker"]
        new = t not in last_ranks
        is_new.append(new)
        rank_chg.append(pd.NA if new else int(last_ranks[t]) - int(today_rank))

        streak = 1
        for s in date_sets:
            if t in s:
                streak += 1
            else:
                break
        streaks.append(streak)

    df["is_new"] = is_new
    df["streak_days"] = streaks
    df["rank_change"] = rank_chg
    return df


def save_today(df: pd.DataFrame) -> None:
    """Append today's list to the history file (overwrites same-day rows)."""
    today = _today_str()
    hist = load_history()
    hist = hist[hist["date"] != today]

    snapshot = pd.DataFrame({
        "date": today,
        "ticker": df["ticker"].values,
        "rank": df.index.values,
        "momentum_score": df["momentum_score"].values,
    })
    # Log diagnostics for the forward-return backtest (tolerant if absent)
    for col in ("vol_adj_3m", "resid_3m", "return_3m_num"):
        if col in df.columns:
            snapshot[col] = df[col].values
    hist = pd.concat([hist, snapshot], ignore_index=True)

    # Trim to KEEP_DAYS most recent dates
    keep = sorted(hist["date"].unique(), reverse=True)[:KEEP_DAYS]
    hist = hist[hist["date"].isin(keep)]

    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    hist.to_csv(HISTORY_FILE, index=False)
    log.info(f"History saved: {len(snapshot)} rows for {today} "
             f"({hist['date'].nunique()} dates on file)")
