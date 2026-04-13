# Weekly Compounder Screener

Screens stocks from a Screener.in CSV export against sustained growth filters.

## Filters Applied
- 10Y PAT CAGR > 20%
- 10Y Revenue CAGR > 15%
- 5Y PAT CAGR > 20%
- 5Y Revenue CAGR > 15%
- 3Y PAT CAGR > 20%
- 3Y Revenue CAGR > 15%
- Current PE < 30x
- D/E < 0.75x
- ROE > 10%

## Setup

### 1. Update the CSV weekly
Export from Screener.in → your stock universe → Export CSV.
Place the file in the repo root as `Updated_claude_100_Market_Cap.csv`.

### 2. GitHub Secrets required
- `EMAIL_SENDER` — your Gmail address
- `EMAIL_RECIPIENT` — where to send results
- `EMAIL_PASSWORD` — Gmail App Password (not your login password)

### 3. Workflow
Place `compounder_screener_workflow.yml` in `.github/workflows/`.
Runs every Friday at 16:30 IST automatically.

## Run locally
```bash
pip install pandas schedule
python compounder_screener.py --dry-run   # test, no email
python compounder_screener.py --now       # run and send email
python compounder_screener.py             # run on Friday schedule
```
