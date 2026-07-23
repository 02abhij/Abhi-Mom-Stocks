"""
diagnose_akiko.py — One-off diagnostic. Answers a single question:
WHY does yfinance return no history for AKIKO-SM.NS / VIGOR-SM.NS while
returning full history for structurally identical SME names?

Tests every plausible request pattern against the two broken symbols and
two known-working controls, and prints exactly what each returns.

Run via GitHub Actions (workflow_dispatch). Read the output in the log.
"""

import sys
import traceback
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

BROKEN   = ["AKIKO-SM.NS", "VIGOR-SM.NS"]
CONTROLS = ["VINSYS-SM.NS", "UNIHEALTH-SM.NS"]     # confirmed working, same suffix/exchange
VARIANTS = ["AKIKO.NS", "AKIKO-ST.NS", "AKIKO.BO", "AKIKO-SM.BO",
            "VIGOR.NS", "VIGOR-ST.NS", "VIGOR.BO"]

END   = datetime.today()
START = END - timedelta(days=450)
S_STR = START.strftime("%Y-%m-%d")
E_STR = END.strftime("%Y-%m-%d")


def _describe(df) -> str:
    if df is None:
        return "None returned"
    if not isinstance(df, pd.DataFrame):
        return f"non-DataFrame: {type(df)}"
    if df.empty:
        return "EMPTY (0 rows)"
    try:
        first, last = df.index[0], df.index[-1]
        tz = getattr(df.index, "tz", None)
        cols = ",".join([str(c) for c in df.columns][:6])
        nn = int(df["Close"].notna().sum()) if "Close" in df.columns else -1
        return (f"{len(df)} rows | {first} → {last} | tz={tz} | "
                f"non-null Close={nn} | cols=[{cols}]")
    except Exception as e:
        return f"{len(df)} rows but describe failed: {e}"


def probe(label, fn):
    print(f"    {label:<44} ", end="")
    try:
        print(_describe(fn()))
    except Exception as e:
        print(f"EXCEPTION {type(e).__name__}: {str(e)[:140]}")


def full_battery(ticker: str):
    print(f"\n{'=' * 78}\n  {ticker}\n{'=' * 78}")

    # 1. Metadata: does Yahoo know the symbol at all?
    try:
        t = yf.Ticker(ticker)
        info = {}
        try:
            fi = t.fast_info
            for k in ("last_price", "currency", "exchange", "timezone",
                      "shares", "market_cap"):
                try:
                    info[k] = getattr(fi, k, None)
                except Exception:
                    info[k] = "err"
        except Exception as e:
            info["fast_info_error"] = str(e)[:100]
        print(f"    fast_info: {info}")
    except Exception as e:
        print(f"    fast_info EXCEPTION: {e}")

    # 2. Every request pattern
    probe("download(start,end)",
          lambda: yf.download(ticker, start=S_STR, end=E_STR,
                              progress=False, auto_adjust=True))
    probe("download(period=1y)",
          lambda: yf.download(ticker, period="1y",
                              progress=False, auto_adjust=True))
    probe("download(period=max)",
          lambda: yf.download(ticker, period="max",
                              progress=False, auto_adjust=True))
    probe("Ticker.history(start,end)",
          lambda: yf.Ticker(ticker).history(start=S_STR, end=E_STR,
                                            auto_adjust=True))
    probe("Ticker.history(period=1y)",
          lambda: yf.Ticker(ticker).history(period="1y", auto_adjust=True))
    probe("Ticker.history(period=max)",
          lambda: yf.Ticker(ticker).history(period="max", auto_adjust=True))
    probe("Ticker.history(period=1mo)",
          lambda: yf.Ticker(ticker).history(period="1mo", auto_adjust=True))
    probe("Ticker.history(2y, auto_adjust=False)",
          lambda: yf.Ticker(ticker).history(period="2y", auto_adjust=False))
    probe("Ticker.history(1y, interval=1wk)",
          lambda: yf.Ticker(ticker).history(period="1y", interval="1wk"))
    probe("history_metadata",
          lambda: pd.DataFrame([yf.Ticker(ticker).history_metadata])
          if getattr(yf.Ticker(ticker), "history_metadata", None) else None)


def main():
    print("=" * 78)
    print(f"  AKIKO / SME HISTORY DIAGNOSTIC — {datetime.now():%d %b %Y %H:%M}")
    print(f"  yfinance version: {getattr(yf, '__version__', 'unknown')}")
    print(f"  pandas   version: {pd.__version__}")
    print(f"  date range tested: {S_STR} → {E_STR}")
    print("=" * 78)

    print("\n\n########## CONTROLS (known working) ##########")
    for t in CONTROLS:
        full_battery(t)

    print("\n\n########## BROKEN SYMBOLS ##########")
    for t in BROKEN:
        full_battery(t)

    print("\n\n########## TICKER VARIANTS (which spelling has data?) ##########")
    for t in VARIANTS:
        print(f"\n  {t}")
        probe("Ticker.history(period=1y)",
              lambda t=t: yf.Ticker(t).history(period="1y", auto_adjust=True))
        probe("download(start,end)",
              lambda t=t: yf.download(t, start=S_STR, end=E_STR,
                                      progress=False, auto_adjust=True))

    # 3. Batch behaviour: does AKIKO survive alongside a working name?
    print("\n\n########## BATCH TEST (multi-ticker call) ##########")
    combo = ["VINSYS-SM.NS", "AKIKO-SM.NS", "VIGOR-SM.NS"]
    try:
        raw = yf.download(combo, start=S_STR, end=E_STR, group_by="ticker",
                          progress=False, auto_adjust=True, threads=True)
        print(f"    combined frame shape: {raw.shape}")
        for t in combo:
            try:
                sub = raw[t].dropna(how="all")
                print(f"    {t:<18} {_describe(sub)}")
            except Exception as e:
                print(f"    {t:<18} extract failed: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"    batch EXCEPTION: {e}")
        traceback.print_exc()

    print("\n" + "=" * 78)
    print("  INTERPRETATION GUIDE")
    print("=" * 78)
    print("""
  - Controls return rows, broken symbols EMPTY on every pattern
        -> Yahoo genuinely has no daily history for these symbols.
           No code fix possible; NSE bhavcopy is the only route.

  - Some pattern returns rows for AKIKO (e.g. period=max, or interval=1wk,
    or a variant spelling)
        -> that pattern goes into scanner.py; problem solved today.

  - fast_info shows a last_price but every history call is EMPTY
        -> quote feed and history feed are separate at Yahoo; history
           was never backfilled for this listing. Bhavcopy route.

  - Controls ALSO come back empty here
        -> the failure is environmental (rate limiting / IP block on the
           runner), not symbol-specific. Retry logic + backoff is the fix.
""")


if __name__ == "__main__":
    main()
