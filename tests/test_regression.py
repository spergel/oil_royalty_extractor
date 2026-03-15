#!/usr/bin/env python3
"""
Regression test: re-runs parse_to_csv.py and checks key output values
against the locked fixtures in tests/fixtures.json.

Usage:
    python tests/test_regression.py
    python tests/test_regression.py --no-reparse   # skip re-running the parser

Exit code 0 = all assertions pass; 1 = failures detected.
"""

import argparse
import csv
import json
import math
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / "fixtures.json"
LONG_CSV = ROOT / "output" / "standardized_measure_data.csv"

REL_TOL = 0.001   # 0.1% — tight enough to catch parser regressions
ABS_TOL = 0.01    # floor for very small values (e.g. per-unit prices)


def close_enough(actual: float, expected: float) -> bool:
    if math.isnan(actual) or math.isnan(expected):
        return False
    tol = max(ABS_TOL, abs(expected) * REL_TOL)
    return abs(actual - expected) <= tol


def run_parser() -> None:
    print("Running parse_to_csv.py ...", flush=True)
    result = subprocess.run(
        [sys.executable, str(ROOT / "parse_to_csv.py")],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print("parse_to_csv.py failed:\n", result.stderr)
        sys.exit(1)


def load_csv() -> dict:
    """Return dict keyed by (ticker, filing_date, report_year, section, label)."""
    index = {}
    with open(LONG_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (
                row["ticker"],
                row["filing_date"],
                row["report_year"],
                row["section"],
                row["label"],
            )
            index[key] = row
    return index


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-reparse", action="store_true", help="Skip re-running parse_to_csv.py")
    args = ap.parse_args()

    if not args.no_reparse:
        run_parser()

    fixtures = json.loads(FIXTURES.read_text())
    index = load_csv()

    failures = []
    passes = 0

    for fix in fixtures:
        key = (fix["ticker"], fix["filing_date"], str(fix["report_year"]), fix["section"], fix["label"])
        label = f"{fix['ticker']}/{fix['filing_date']}/{fix['report_year']} {fix['section']}.{fix['label']}"

        if key not in index:
            failures.append(f"MISSING  {label}")
            continue

        row = index[key]
        try:
            actual_val = float(row["value_thousands"])
        except (ValueError, KeyError):
            failures.append(f"BAD_VAL  {label}: could not parse '{row.get('value_thousands')}'")
            continue

        expected_val = float(fix["value_thousands"])
        if not close_enough(actual_val, expected_val):
            failures.append(
                f"MISMATCH {label}: expected {expected_val}, got {actual_val} "
                f"(diff {actual_val - expected_val:+.3f})"
            )
            continue

        expected_src = fix.get("source")
        actual_src = row.get("source", "")
        if expected_src and actual_src != expected_src:
            failures.append(
                f"SOURCE   {label}: expected source={expected_src}, got source={actual_src}"
            )
            continue

        passes += 1

    print(f"\nRegression: {passes} passed, {len(failures)} failed  ({len(fixtures)} total fixtures)")
    if failures:
        print()
        for f in failures:
            print(" ", f)
        sys.exit(1)
    else:
        print("All assertions passed.")


if __name__ == "__main__":
    main()
