#!/usr/bin/env python3
"""
parse_to_csv.py — Parse extracted standardized measure text files into structured CSV.

Reads output/<TICKER>/<date>_standardized_measure.txt and extracts:
  - Main table: future cash flows, PV10 discount, standardized measure
  - Changes table: beginning/ending SM, accretion, sales, revisions, etc.
  - Commodity prices (SEC reference prices from intro text or price table)

Output:
  output/standardized_measure_data.csv  — long format (one row per ticker/date/year/label)
  output/standardized_measure_wide.csv  — wide format (one row per ticker/filing, key metrics as cols)

Usage:
  python parse_to_csv.py [--output-dir output/] [--verbose]
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).parent / "output"
LONG_CSV = "standardized_measure_data.csv"
WIDE_CSV = "standardized_measure_wide.csv"

# Unicode zero-width / non-breaking characters to strip
ZERO_WIDTH_RE = re.compile(
    r"[\u200b\u200c\u200d\ufeff\u00a0\u00ad\u200e\u200f\u2060\u3000\u200a​]"
)

YEAR_RE = re.compile(r"\b(20\d{2})\b")

# Financial number patterns — applied via sequential scan (see extract_numbers())
NEG_RE    = re.compile(r"\(\s*\$?\s*([\d,]+(?:\.\d+)?)\s*\)")
DOLLAR_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
COMMA_RE  = re.compile(r"(?<![.\d$])([\d]{1,3}(?:,[\d]{3})+(?:\.\d+)?)(?![.\d])")
EMDASH_RE = re.compile(r"(?<![a-zA-Z0-9])[—–](?![a-zA-Z0-9])")

# Changes-section heading triggers
CHANGES_HEADING_RE = re.compile(
    r"changes?\s+in\s+(the\s+)?standardized\s+measure|"
    r"change\s+in\s+(the\s+)?standardized\s+measure|"
    r"principal\s+changes?\s+in\s+(the\s+)?standardized",
    re.IGNORECASE,
)

# Commodity price extraction from prose
OIL_PRICE_RE = re.compile(
    r"\$([\d]+\.[\d]+)\s+per\s+(bbl|barrel)", re.IGNORECASE
)
GAS_PRICE_RE = re.compile(
    r"\$([\d]+\.[\d]+)\s+per\s+(mcf|mmbtu|million\s+british)", re.IGNORECASE
)
NGL_PRICE_RE = re.compile(
    r"\$([\d]+\.[\d]+)\s+per\s+bbl\s+of\s+ngl", re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

def clean(s: str) -> str:
    """Remove zero-width / non-breaking chars."""
    return ZERO_WIDTH_RE.sub(" ", s)


def norm(s: str) -> str:
    """Normalize: clean, lowercase, collapse whitespace."""
    return " ".join(clean(s).lower().split())


def parse_num(s: str) -> float:
    return float(s.replace(",", ""))


def extract_numbers(line: str) -> List[float]:
    """
    Extract all financial numbers from a line in left-to-right order.

    Handles:
      - (123,456) / ($123,456)  -> negative
      - $123,456                -> positive
      - 123,456                 -> bare comma-separated number
      - — / –                   -> zero (em-dash)

    NOTE: Bare integers without commas (e.g. 355) are NOT captured to
    avoid picking up page numbers, footnote refs, etc.  Small parenthesized
    integers (e.g. (578)) ARE captured because they carry the neg-sign marker.
    """
    c = clean(line)
    results: List[Tuple[int, float]] = []  # (position, value)
    pos = 0
    n = len(c)

    while pos < n:
        m = NEG_RE.match(c, pos)
        if m:
            results.append((pos, -parse_num(m.group(1))))
            pos = m.end()
            continue
        m = DOLLAR_RE.match(c, pos)
        if m:
            results.append((pos, parse_num(m.group(1))))
            pos = m.end()
            continue
        m = COMMA_RE.match(c, pos)
        if m:
            val = parse_num(m.group(1))
            # Filter out year-like sequences (shouldn't happen since comma required)
            results.append((pos, val))
            pos = m.end()
            continue
        m = EMDASH_RE.match(c, pos)
        if m:
            results.append((pos, 0.0))
            pos = m.end()
            continue
        pos += 1

    return [v for _, v in results]


# ---------------------------------------------------------------------------
# Unit-scale detection
# ---------------------------------------------------------------------------

def detect_scale(lines: List[str]) -> float:
    """
    Return multiplier so that result * scale -> $thousands.

    'in millions'  -> 1000
    'in thousands' -> 1
    default        -> 0.001  (raw dollars -> thousands)
    """
    for line in lines[:40]:
        n = norm(line)
        if "in millions" in n or "millions of dollars" in n:
            return 1000.0
        if "in thousands" in n or "dollars in thousands" in n or "thousands of dollars" in n:
            return 1.0
    return 0.001  # raw dollars (VOC, MVO style)


# ---------------------------------------------------------------------------
# Year-header detection
# ---------------------------------------------------------------------------

def detect_years(lines: List[str]) -> List[int]:
    """
    Find the first line with 2+ distinct year numbers; return them in
    left-to-right order (preserving the filing's column order).
    """
    for line in lines:
        years = YEAR_RE.findall(clean(line))
        # Deduplicate, preserve order
        seen: set = set()
        unique = []
        for y in years:
            if y not in seen:
                seen.add(y)
                unique.append(int(y))
        if len(unique) >= 2:
            return unique
    return []


def detect_first_year_header_idx(lines: List[str]) -> int:
    """
    Return the index of the first line that appears to be a year header
    (contains 2+ distinct years), else -1.
    """
    for i, line in enumerate(lines):
        years = YEAR_RE.findall(clean(line))
        seen: set = set()
        unique = []
        for y in years:
            if y not in seen:
                seen.add(y)
                unique.append(y)
        if len(unique) >= 2:
            return i
    return -1


# ---------------------------------------------------------------------------
# Label matching
# ---------------------------------------------------------------------------

def match_main_label(nline: str) -> Optional[str]:
    """Map a normalized line to a canonical main-table label, or None."""
    has_sm   = "standardized measure" in nline
    has_fut  = "future" in nline

    # Standardized measure (the final number — bottom line of main table).
    # Must START with "standardized measure" to avoid matching prose sentences
    # like "...in the calculation of the standardized measure because..."
    if has_sm and nline.startswith("standardized measure"):
        if not any(x in nline for x in ["change", "principal change"]):
            if not any(x in nline for x in ["beginning", "january", "end of",
                                              "december 31", "start of", "balance,"]):
                return "standardized_measure"

    # BSM (and similar) uses bare "Total" as the last row of the main table.
    # nline includes the numbers, so check: starts with "total" and no other alpha text follows.
    if nline.startswith("total") and not re.search(r"[a-z]", nline[5:]):
        return "standardized_measure"

    # Future cash inflows (top line)
    if has_fut and ("cash inflow" in nline or "estimated gross revenue" in nline):
        return "future_cash_inflows"

    # Sub-lines
    if has_fut and "production cost" in nline:
        return "future_production_costs"
    if has_fut and "production tax" in nline:
        return "future_production_taxes"
    if has_fut and "development cost" in nline and "incurred" not in nline:
        return "future_development_costs"
    # Keep this narrow to avoid matching prose like:
    # "...future conditions... no provision for income taxes..."
    if "future income tax" in nline or "future federal income tax" in nline:
        return "future_income_tax"

    # PV10 discount line
    if ("10%" in nline or "10 percent" in nline or "annual discount" in nline) and (
        "discount" in nline or "timing" in nline
    ):
        return "pv10_discount"
    if "discount of future" in nline or "less 10%" in nline or "less 10 %" in nline:
        return "pv10_discount"

    # Undiscounted net cash flows (before discount line)
    if ("future net cash flow" in nline or "future estimated net revenue" in nline) and "discount" not in nline:
        return "future_net_cash_flows"

    return None


def match_changes_label(nline: str) -> Optional[str]:
    """Map a normalized line to a canonical changes-table label, or None."""
    has_sm = "standardized measure" in nline

    # Beginning / ending SM
    if has_sm:
        if any(x in nline for x in ["beginning", "january 1", "start of", "beginning of"]):
            return "beg_standardized_measure"
        if any(x in nline for x in ["end of year", "end of period", "december 31",
                                      "end of the period"]):
            return "end_standardized_measure"
        if "net increase" in nline or "net decrease" in nline or "net change in stand" in nline:
            return "net_change_total"
        # No qualifier — skip; will be caught by context below
        return None

    # Bare "January 1" or "Balance, January 1" rows (PBT / SJT style)
    stripped = nline.strip()
    if stripped in ("january 1", "balance, january 1", "balance january 1"):
        return "beg_standardized_measure"
    if stripped in ("december 31", "balance, december 31", "balance december 31"):
        return "end_standardized_measure"

    # Accretion
    if "accretion" in nline:
        return "accretion_of_discount"

    # Sales / production
    if (
        ("sales" in nline and "production cost" in nline)
        or ("sales of oil" in nline and "net of" in nline)
        or ("royalty income" in nline and "distributable" not in nline and "quarterly" not in nline)
        or ("net proceeds" in nline and ("trust" in nline or "royalty" in nline))
    ):
        return "sales_net"

    # Price changes
    if ("change" in nline or "net change" in nline) and "price" in nline:
        if "standardized" not in nline:
            return "net_price_changes"

    # Extensions / discoveries
    if "extension" in nline and any(x in nline for x in ["discover", "addition", "improved"]):
        return "extensions_discoveries"

    # Revisions
    if "revision" in nline:
        if "development" in nline:
            return "dev_cost_revisions"
        return "quantity_revisions"  # covers "revisions of previous estimates and other"

    # Development costs incurred
    if "development cost" in nline and "incurred" in nline:
        return "dev_costs_incurred"

    # Purchases / divestitures of reserves
    if "purchase" in nline and ("reserve" in nline or "mineral" in nline):
        return "purchases_reserves"
    if "divestiture" in nline or ("sale" in nline and "reserve" in nline and "place" in nline):
        return "sales_reserves"

    # Income tax changes
    if "income tax" in nline and ("change" in nline or "net" in nline):
        return "income_tax_changes"

    # Timing / other
    if "timing" in nline or ("production rate" in nline and "other" in nline):
        return "timing_other"

    # Net total change
    if "net increase" in nline or "net decrease" in nline:
        return "net_change_total"

    return None


# ---------------------------------------------------------------------------
# Commodity price extraction from text
# ---------------------------------------------------------------------------

def extract_prices_from_text(text: str, year: int) -> Dict[str, float]:
    """
    Pull SEC reference commodity prices from the intro paragraph.
    Returns dict like {'oil_per_bbl': 75.48, 'gas_per_mcf': 2.13, ...}
    """
    prices: Dict[str, float] = {}

    # Look for patterns like "$75.48 per Bbl" or "$2.13 per Mcf"
    # Capture year-scoped prices if present ("For 2024, $X.XX per Mcf ... $X.XX per Bbl")
    # Simple approach: scan full text for first oil and gas price mentions

    # NGL first (must check before generic oil/bbl to avoid false matches)
    for m in NGL_PRICE_RE.finditer(text):
        prices.setdefault("ngl_per_bbl", float(m.group(1)))

    for m in OIL_PRICE_RE.finditer(text):
        # Skip if this match is inside a "NGL" context
        start = max(0, m.start() - 40)
        ctx = text[start : m.end()].lower()
        if "ngl" in ctx:
            continue
        prices.setdefault("oil_per_bbl", float(m.group(1)))

    for m in GAS_PRICE_RE.finditer(text):
        prices.setdefault("gas_per_mcf", float(m.group(1)))

    return prices


def extract_prices_from_table_rows(lines: List[str]) -> Dict[str, float]:
    """
    Extract prices from a dedicated price table.
    Looks for rows like:
        Oil (per Bbl)           $ 64.80    $ 75.61    $ 77.93
        Natural gas (per Mcf)   $  1.31    $  0.49    $  1.54
    Returns dict of {canonical_name: [val_col0, val_col1, ...]}
    """
    prices: Dict[str, List[float]] = {}
    for line in lines:
        n = norm(line)
        # Skip long prose paragraphs — genuine price table rows are short
        if len(n) > 200:
            continue
        nums = extract_numbers(line)
        # Commodity prices are always positive; filter footnote markers like (1)(2) -> -1,-2
        pos_nums = [x for x in nums if x > 0]
        if not pos_nums:
            continue
        if ("oil" in n or "crude" in n) and "per bbl" in n and "ngl" not in n and "liquids" not in n:
            prices["oil_per_bbl"] = pos_nums
        elif "natural gas" in n and ("per mcf" in n or "per mmbtu" in n):
            prices["gas_per_mcf"] = pos_nums
        elif "natural gas liquid" in n and "per bbl" in n:
            prices["ngl_per_bbl"] = pos_nums
    return prices


# ---------------------------------------------------------------------------
# Core file parser
# ---------------------------------------------------------------------------

Row = Dict  # {ticker, filing_date, report_year, section, label, value_thousands}


def parse_file(path: Path) -> List[Row]:
    """Parse one standardized measure .txt file; return list of data rows."""
    ticker = path.parent.name
    filing_date = path.stem.replace("_standardized_measure", "")
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    scale = detect_scale(lines)
    years = detect_years(lines)
    main_start_idx = detect_first_year_header_idx(lines)
    if not years:
        print(f"  [{ticker}/{filing_date}] WARNING: no year header found — skipping", file=sys.stderr)
        return []

    rows: List[Row] = []
    captured: set = set()  # (section, label, report_year) — prevents dup rows

    def add(section: str, label: str, nums: List[float]) -> None:
        """Align extracted numbers to detected year columns."""
        n_years = len(years)
        if not nums:
            return
        # If more numbers than years, take the last n_years (handles CRT multi-col Total)
        if len(nums) > n_years:
            nums = nums[-n_years:]
        for i, val in enumerate(nums[:n_years]):
            yr = years[i]
            key = (section, label, yr)
            if key in captured:
                continue  # skip duplicate (e.g. CRT Underlying Properties section)
            captured.add(key)
            rows.append({
                "ticker": ticker,
                "filing_date": filing_date,
                "report_year": yr,
                "section": section,
                "label": label,
                "value_thousands": round(val * scale, 3),
            })

    in_changes = False
    changes_start_idx = -1
    in_changes_table = False
    full_text = text  # for prose price extraction

    for idx, line in enumerate(lines):
        n = norm(line)

        # Detect section boundary
        if CHANGES_HEADING_RE.search(n):
            in_changes = True
            changes_start_idx = idx
            continue

        # Before the first year header, skip financial row matching entirely.
        if main_start_idx >= 0 and idx <= main_start_idx and not in_changes:
            continue

        # In changes section, wait until we reach its year header row.
        if in_changes and not in_changes_table:
            if idx <= changes_start_idx:
                continue
            # Enter changes table when we encounter a year header line.
            yrs_here = YEAR_RE.findall(clean(line))
            uniq = []
            seen = set()
            for y in yrs_here:
                if y not in seen:
                    seen.add(y)
                    uniq.append(y)
            if len(uniq) >= 2:
                in_changes_table = True
            continue

        nums = extract_numbers(line)

        if in_changes and in_changes_table:
            label = match_changes_label(n)
            if label and nums:
                add("changes", label, nums)
        else:
            label = match_main_label(n)
            if label and nums:
                add("main", label, nums)

    # --- Commodity prices ---
    # First try dedicated price table rows (VNOM / DMLP style)
    price_table = extract_prices_from_table_rows(lines)
    if price_table:
        for price_label, vals in price_table.items():
            for i, val in enumerate(vals[:len(years)]):
                rows.append({
                    "ticker": ticker,
                    "filing_date": filing_date,
                    "report_year": years[i],
                    "section": "prices",
                    "label": price_label,
                    "value_thousands": val,
                })
    else:
        # Fall back to prose extraction (single price per filing date — most recent year)
        prose_prices = extract_prices_from_text(full_text, years[0])
        for price_label, val in prose_prices.items():
            rows.append({
                "ticker": ticker,
                "filing_date": filing_date,
                "report_year": years[0],  # most recent year in this filing
                "section": "prices",
                "label": price_label,
                "value_thousands": val,
            })

    return rows


# ---------------------------------------------------------------------------
# Wide format builder
# ---------------------------------------------------------------------------

KEY_METRICS = [
    ("main",    "standardized_measure"),
    ("main",    "future_cash_inflows"),
    ("main",    "pv10_discount"),
    ("changes", "beg_standardized_measure"),
    ("changes", "end_standardized_measure"),
    ("changes", "accretion_of_discount"),
    ("changes", "sales_net"),
    ("changes", "net_price_changes"),
    ("prices",  "oil_per_bbl"),
    ("prices",  "gas_per_mcf"),
]


def build_wide(all_rows: List[Row]) -> List[Dict]:
    """Build wide-format table: one row per (ticker, filing_date, report_year)."""
    # Index by (ticker, filing_date, report_year, section, label)
    idx: Dict[Tuple, float] = {}
    for r in all_rows:
        key = (r["ticker"], r["filing_date"], r["report_year"], r["section"], r["label"])
        idx[key] = r["value_thousands"]

    # Collect all (ticker, filing_date, report_year) combos
    combos = sorted({(r["ticker"], r["filing_date"], r["report_year"]) for r in all_rows})

    wide_rows = []
    for ticker, filing_date, report_year in combos:
        row = {"ticker": ticker, "filing_date": filing_date, "report_year": report_year}
        for section, label in KEY_METRICS:
            col = f"{section}_{label}"
            row[col] = idx.get((ticker, filing_date, report_year, section, label), "")
        wide_rows.append(row)
    return wide_rows


# ---------------------------------------------------------------------------
# QA checks
# ---------------------------------------------------------------------------

def run_qa_checks(all_rows: List[Row]) -> List[str]:
    """
    Run lightweight QA checks on parsed rows.
    Returns a list of warning strings.
    """
    warnings: List[str] = []

    # Index by filing-year for completeness checks.
    by_filing_year: Dict[Tuple[str, str, int], List[Row]] = {}
    for r in all_rows:
        k = (r["ticker"], r["filing_date"], int(r["report_year"]))
        by_filing_year.setdefault(k, []).append(r)

    for (ticker, filing_date, year), rows in sorted(by_filing_year.items()):
        main = {r["label"]: r["value_thousands"] for r in rows if r["section"] == "main"}
        prices = {r["label"]: r["value_thousands"] for r in rows if r["section"] == "prices"}

        # Core fields
        if "standardized_measure" not in main:
            warnings.append(f"{ticker}/{filing_date}/{year}: missing main.standardized_measure")
        if "future_cash_inflows" not in main:
            warnings.append(f"{ticker}/{filing_date}/{year}: missing main.future_cash_inflows")

        # Sign sanity checks
        if "standardized_measure" in main and main["standardized_measure"] < 0:
            warnings.append(f"{ticker}/{filing_date}/{year}: negative standardized_measure ({main['standardized_measure']})")
        if "future_cash_inflows" in main and main["future_cash_inflows"] < 0:
            warnings.append(f"{ticker}/{filing_date}/{year}: negative future_cash_inflows ({main['future_cash_inflows']})")
        if "pv10_discount" in main and main["pv10_discount"] > 0:
            warnings.append(f"{ticker}/{filing_date}/{year}: positive pv10_discount ({main['pv10_discount']})")

        # Price plausibility (SEC average annual prices should be in broad realistic ranges)
        oil = prices.get("oil_per_bbl")
        gas = prices.get("gas_per_mcf")
        ngl = prices.get("ngl_per_bbl")
        if oil is not None and not (15 <= oil <= 200):
            warnings.append(f"{ticker}/{filing_date}/{year}: oil_per_bbl out of range ({oil})")
        if gas is not None and not (0.25 <= gas <= 25):
            warnings.append(f"{ticker}/{filing_date}/{year}: gas_per_mcf out of range ({gas})")
        if ngl is not None and not (5 <= ngl <= 150):
            warnings.append(f"{ticker}/{filing_date}/{year}: ngl_per_bbl out of range ({ngl})")

        # Common prose-parsing failure signature: tiny positive "future_income_tax"
        # that looks like a commodity price token rather than a tax-flow line.
        fit = main.get("future_income_tax")
        if fit is not None and 0 < fit < 200:
            warnings.append(
                f"{ticker}/{filing_date}/{year}: suspiciously small future_income_tax ({fit}) - verify row mapping"
            )

    return warnings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Parse standardized measure .txt files to CSV")
    ap.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Root output directory")
    ap.add_argument("--verbose", "-v", action="store_true")
    ap.add_argument("--qa", action="store_true", help="Run QA checks and print warnings summary")
    args = ap.parse_args()

    out_root = Path(args.output_dir)
    txt_files = sorted(out_root.glob("*/*_standardized_measure.txt"))

    if not txt_files:
        print("No .txt files found under", out_root, file=sys.stderr)
        sys.exit(1)

    all_rows: List[Row] = []
    for path in txt_files:
        ticker = path.parent.name
        if ticker == "MARPS":
            continue  # no SFAS 69 data
        filing = path.stem.replace("_standardized_measure", "")
        print(f"  Parsing {ticker}/{filing} ...", end="  ")
        rows = parse_file(path)
        print(f"{len(rows)} data points")
        if args.verbose:
            for r in rows:
                print(f"    {r['report_year']}  {r['section']:8s}  {r['label']:35s}  {r['value_thousands']}")
        all_rows.extend(rows)

    # Write long CSV
    long_path = out_root / LONG_CSV
    fieldnames = ["ticker", "filing_date", "report_year", "section", "label", "value_thousands"]
    with open(long_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\nLong CSV -> {long_path}  ({len(all_rows)} rows)")

    # Write wide CSV
    wide_rows = build_wide(all_rows)
    wide_path = out_root / WIDE_CSV
    if wide_rows:
        with open(wide_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(wide_rows[0].keys()))
            w.writeheader()
            w.writerows(wide_rows)
        print(f"Wide CSV  -> {wide_path}  ({len(wide_rows)} rows)")

    if args.qa:
        qa_warnings = run_qa_checks(all_rows)
        print("\nQA summary")
        print(f"  warnings: {len(qa_warnings)}")
        for w in qa_warnings[:100]:
            print(f"  - {w}")
        if len(qa_warnings) > 100:
            print(f"  ... {len(qa_warnings) - 100} more warnings")


if __name__ == "__main__":
    main()
