"""
scheduler.py — Runs the momentum scan daily at RUN_TIME (IST, from config.py).

Usage:
    python scheduler.py              # Runs daily at configured time
    python scheduler.py --now        # Run immediately (test mode)
    python scheduler.py --dry-run    # Run scan but skip email (preview only)
"""

import sys
import logging
import schedule
import time
import argparse
from datetime import datetime

import config
from tickers import get_all_tickers
from scanner import run_scan
from emailer import send_email

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def job(dry_run: bool = False):
    log.info("=" * 60)
    log.info(f"MOMENTUM SCAN STARTED — {datetime.now().strftime('%d %b %Y %H:%M')}")
    log.info("=" * 60)

    try:
        # Step 1: Fetch tickers
        tickers, ticker_meta = get_all_tickers()
        if not tickers:
            log.error("No tickers fetched. Aborting.")
            return

        # Step 2: Run scan
        df = run_scan(tickers, ticker_meta)
        if df.empty:
            log.error("Scan returned no results. Aborting.")
            return

        top = df.head(config.TOP_N)
        log.info(f"\n{'─'*60}")
        log.info(f"TOP {config.TOP_N} MOMENTUM STOCKS")
        log.info(f"{'─'*60}")
        preview_cols = ["ticker", "index", "price", "momentum_score",
                        "return_1m", "return_3m", "return_6m", "rsi"]
        log.info("\n" + top[preview_cols].to_string())

        # Step 3: Send email (unless dry-run)
        if dry_run:
            log.info("DRY RUN — email skipped.")
        else:
            success = send_email(df)
            if success:
                log.info("✅ Email sent successfully.")
            else:
                log.error("❌ Email failed.")

        log.info("SCAN COMPLETE")

    except Exception as e:
        log.exception(f"Unexpected error during scan: {e}")


def main():
    parser = argparse.ArgumentParser(description="Nifty 750 Momentum Scanner")
    parser.add_argument("--now",     action="store_true", help="Run scan immediately")
    parser.add_argument("--dry-run", action="store_true", help="Run scan, skip email")
    args = parser.parse_args()

    if args.now or args.dry_run:
        job(dry_run=args.dry_run)
        return

    # Schedule daily run
    log.info(f"Scheduler started. Scan will run daily at {config.RUN_TIME} IST.")
    log.info("Press Ctrl+C to stop.")
    schedule.every().day.at(config.RUN_TIME).do(job)

    # Also run immediately on startup if it's past the scheduled time today
    now_str = datetime.now().strftime("%H:%M")
    if now_str >= config.RUN_TIME:
        log.info("Past today's scheduled time — running now for today's data.")
        job()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
