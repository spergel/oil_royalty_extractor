#!/usr/bin/env bash
# run_update.sh — Pull latest 10-K filings and refresh parsed CSV + valuation table.
#
# Usage:
#   bash run_update.sh                   # default: 5 years back
#   bash run_update.sh --years-back 3    # 3 years only
#
# Schedule: run weekly (see README) via Windows Task Scheduler or cron.

set -e
cd "$(dirname "$0")"

YEARS_BACK="${1:---years-back 5}"
LOG="logs/update_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

echo "=== Oil Royalty Extractor Update: $(date) ===" | tee "$LOG"

echo "--- Step 1: extracting standardized measure sections ---" | tee -a "$LOG"
python extract_standardized_measure.py $YEARS_BACK 2>&1 | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "--- Step 2: parsing to CSV ---" | tee -a "$LOG"
python parse_to_csv.py 2>&1 | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "--- Step 3: valuation model (WTI=70, HH=2.50) ---" | tee -a "$LOG"
python valuation_model.py --oil 70 --gas 2.50 2>&1 | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "=== Done: $(date) ===" | tee -a "$LOG"
echo "Log saved to $LOG"
