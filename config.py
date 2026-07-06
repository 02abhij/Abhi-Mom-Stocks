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
    "pct_from_20d_high": 0.10,   # % proximity to 20-day high — breakout signal
    "days_at_high":      0.05,   # closes within 2% of 20d high in last 10 sessions
                                 #   — breakout HELD, not just touched
    "vol_persist_10d":   0.10,   # 10d median volume ÷ prior 40d median
                                 #   — sustained participation (block deals can't fake this)
    "accum_10d":         0.05,   # up-day volume ÷ down-day volume, last 10 sessions
                                 #   — direction of the elevated tape
    "rsi":               0.05,   # RSI (14-day), rewarded in 55–75 zone
    "obv_slope":         0.05,   # OBV linear regression slope (normalised)
}
# NOTE (Jul 2026): vol_surge (1-day) and vol_ratio removed from scoring after the
# SUMICHEM false positive — a parent-company catalyst produced a 15.8x one-day
# print with zero relevance to the listed entity. One-day surges remain DISPLAYED
# as an event flag but carry zero weight. Persistence signals replaced them.
assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, "WEIGHTS must sum to 1.0"

# ── Data quality / liquidity filters ─────────────────────────────
MIN_PRICE            = 10      # Minimum stock price in ₹ (filter penny stocks)
MIN_AVG_VOLUME       = 5_000   # Minimum 20-day avg daily volume (sanity floor)
MIN_TURNOVER_LAKH    = 50      # Minimum 20d MEDIAN daily traded value, in ₹ lakh
                               # — hard filter. (₹50L/day ≈ can exit a ₹10L
                               #  position in ~2 days at 10% volume participation)
MIN_HISTORY_DAYS     = 130     # Minimum trading days of history required

# ── RSI scoring band ─────────────────────────────────────────────
RSI_SWEET_LOW    = 55          # RSI below this = losing momentum points
RSI_SWEET_HIGH   = 75          # RSI above this = overbought penalty kicks in

# ── Blowoff / extended guard ─────────────────────────────────────
# Names breaching either threshold are pulled out of the main ranking
# and shown in a separate "Extended" section — momentum is present but
# 30-60 day persistence odds are poor (mean-reversion profile).
EXTENDED_RSI     = 85          # RSI-14 above this = extended
EXTENDED_3M      = 1.50        # 3M return above 150% = extended

# ── History / memory settings ────────────────────────────────────
HISTORY_FILE      = "history/momentum_history.csv"
HISTORY_KEEP_DAYS = 180        # Rolling window of daily snapshots kept on file
