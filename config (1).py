# ═══════════════════════════════════════════════════════════════
#  MOMENTUM SCANNER — USER CONFIGURATION
#  Fill in your details below, then run: python scheduler.py
# ═══════════════════════════════════════════════════════════════

# ── Email settings ───────────────────────────────────────────────
EMAIL_SENDER     = "your_gmail@gmail.com"       # Gmail you'll send FROM
EMAIL_PASSWORD   = "your_app_password_here"     # Gmail App Password (not login password)
                                                # Get it at: myaccount.google.com/apppasswords
EMAIL_RECIPIENT  = "your_email@gmail.com"       # Where to receive the daily report

SMTP_HOST        = "smtp.gmail.com"
SMTP_PORT        = 587

# ── Scan settings ────────────────────────────────────────────────
TOP_N            = 30          # Number of top momentum stocks to report
RUN_TIME         = "16:15"     # 24h format, IST — when to run daily (post-close)

# ── Momentum score weights (must sum to 1.0) ─────────────────────
WEIGHTS = {
    "return_1m":     0.15,   # 1-month price return
    "return_3m":     0.20,   # 3-month price return
    "return_6m":     0.20,   # 6-month price return
    "pct_from_52w":  0.15,   # % proximity to 52-week high
    "rsi":           0.10,   # RSI (14-day), rewarded in 55–75 zone
    "obv_slope":     0.10,   # OBV linear regression slope (normalised)
    "vol_ratio":     0.10,   # 20d avg volume / 60d avg volume
}

# ── Data quality filters ─────────────────────────────────────────
MIN_PRICE        = 10          # Minimum stock price in ₹ (filter penny stocks)
MIN_AVG_VOLUME   = 5_000       # Minimum 20-day avg daily volume
MIN_HISTORY_DAYS = 130         # Minimum trading days of history required

# ── RSI scoring band ─────────────────────────────────────────────
RSI_SWEET_LOW    = 55          # RSI below this = losing momentum points
RSI_SWEET_HIGH   = 75          # RSI above this = overbought penalty kicks in
