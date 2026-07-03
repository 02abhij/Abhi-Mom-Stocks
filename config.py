# ═══════════════════════════════════════════════════════════════
#  MOMENTUM SCANNER — USER CONFIGURATION
#  Fill in your details below, then run: python scheduler.py
# ═══════════════════════════════════════════════════════════════
# ── Email settings (read from environment / GitHub Secrets) ──────
import os
EMAIL_SENDER     = os.environ.get("EMAIL_SENDER", "02abhij@gmail.com")
EMAIL_PASSWORD   = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT  = os.environ.get("EMAIL_RECIPIENT", "abhi@ajassoc.in")
SMTP_HOST        = "smtp.gmail.com"
SMTP_PORT        = 587

# ── Scan settings ────────────────────────────────────────────────
TOP_N            = 100         # Number of top momentum stocks to report
RUN_TIME         = "16:15"     # 24h format, IST — when to run daily (post-close)

# ── Momentum score weights (must sum to 1.0) ─────────────────────
# Rebalanced July 2026: ~40% of weight now sits on sub-1-month
# signals (return_2w, pct_from_20d_high, vol_surge) to catch fresh
# momentum and active breakouts. The 3M/6M anchor is retained so
# one-day noise doesn't dominate the top of the list.
WEIGHTS = {
    "return_2w":         0.15,   # 10-day return — fresh momentum
    "return_1m":         0.10,   # 21-day return
    "return_3m":         0.15,   # 63-day return
    "return_6m":         0.10,   # 126-day return
    "pct_from_52w":      0.10,   # % proximity to 52-week high
    "pct_from_20d_high": 0.15,   # % proximity to 20-day high — breakout signal
    "vol_surge":         0.10,   # latest day volume ÷ 20d avg — breakout confirmation
    "rsi":               0.05,   # RSI (14-day), rewarded in 55–75 zone
    "obv_slope":         0.05,   # OBV linear regression slope (normalised)
    "vol_ratio":         0.05,   # 20d avg volume / 60d avg volume
}

assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, "WEIGHTS must sum to 1.0"

# ── Data quality filters ─────────────────────────────────────────
MIN_PRICE        = 10          # Minimum stock price in ₹ (filter penny stocks)
MIN_AVG_VOLUME   = 5_000       # Minimum 20-day avg daily volume
MIN_HISTORY_DAYS = 130         # Minimum trading days of history required

# ── RSI scoring band ─────────────────────────────────────────────
RSI_SWEET_LOW    = 55          # RSI below this = losing momentum points
RSI_SWEET_HIGH   = 75          # RSI above this = overbought penalty kicks in
