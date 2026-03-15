#!/usr/bin/env python3
"""
parse_to_csv.py — Parse extracted standardized measure text files into structured CSV.

Reads output/<TICKER>/<date>_standardized_measure.txt and extracts:
  - Main table: future cash flows, PV10 discount, standardized measure
  - Changes table: beginning/ending SM, accretion, sales, revisions, etc.
  - Commodity prices (SEC reference prices from intro text or price table)
  - Operations totals (royalty income, distributable income, distribution/unit)
  - Reserve headline prose fields when present (proved oil/gas, net/discounted value)

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

# Reserve/distribution extraction
RESERVE_PROSE_RE = re.compile(
    r"approximately\s+([\d]+(?:\.\d+)?)\s+million\s+barrels?\s+of\s+oil"
    r".{0,120}?"
    r"([\d]+(?:\.\d+)?)\s+billion\s+cubic\s+feet\s+of\s+gas"
    r".{0,200}?"
    r"future\s+net\s+value.{0,80}?\$([\d,]+(?:\.\d+)?)"
    r".{0,120}?"
    r"discounted\s+value.{0,80}?\$([\d,]+(?:\.\d+)?)",
    re.IGNORECASE | re.DOTALL,
)

RESERVE_SIMPLE_RE = re.compile(
    r"approximately\s+([\d]+(?:\.\d+)?)\s+million\s+barrels?\s+of\s+oil"
    r".{0,140}?"
    r"([\d]+(?:\.\d+)?)\s+billion\s+cubic\s+feet\s+of\s+gas",
    re.IGNORECASE | re.DOTALL,
)

DIST_YEAR_HEADER_RE = re.compile(
    r"^\s*(20\d{2})\s+royalty\s+income.*distributable\s+income.*distribution\s+per\s+unit",
    re.IGNORECASE,
)

PRODUCTION_PROSE_RE = re.compile(
    r"(?:for|during)\s+(?:the\s+year\s+ended\s+)?(?:december\s+31,\s*)?(20\d{2})"
    r".{0,180}?"
    r"(?:production|produced)"
    r".{0,120}?"
    r"([\d]+(?:\.\d+)?)\s+million\s+barrels?\s+of\s+oil"
    r".{0,120}?"
    r"([\d]+(?:\.\d+)?)\s+billion\s+cubic\s+feet\s+of\s+gas",
    re.IGNORECASE | re.DOTALL,
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


def extract_distribution_totals(lines: List[str]) -> List[Tuple[int, float, float, float]]:
    """
    Extract annual totals from quarterly distributable-income tables.
    Returns tuples:
      (report_year, royalty_income_thousands, distributable_income_thousands, distribution_per_unit)
    """
    out: List[Tuple[int, float, float, float]] = []
    current_year: Optional[int] = None
    for line in lines:
        c = clean(line)
        n = norm(line)
        m = DIST_YEAR_HEADER_RE.match(n)
        if m:
            current_year = int(m.group(1))
            continue

        if current_year is None:
            continue

        if n.startswith("total"):
            # Prefer explicit 3-column parse (handles per-unit values like ".230042")
            m_total = re.search(
                r"total.*?\$?\s*([0-9,]+(?:\.\d+)?)\s+\$?\s*([0-9,]+(?:\.\d+)?)\s+\$?\s*([0-9]*\.\d+)",
                c,
                re.IGNORECASE,
            )
            if m_total:
                royalty_income = parse_num(m_total.group(1))
                distributable_income = parse_num(m_total.group(2))
                distribution_per_unit = float(m_total.group(3))
                if 0 <= distribution_per_unit <= 20:
                    out.append((current_year, royalty_income, distributable_income, distribution_per_unit))
                continue

            # Fallback generic parse
            nums = extract_numbers(c)
            if len(nums) >= 3:
                royalty_income = abs(nums[0])        # table says in thousands
                distributable_income = abs(nums[1])  # table says in thousands
                distribution_per_unit = abs(nums[2]) # dollars per unit
                if 0 <= distribution_per_unit <= 20:
                    out.append((current_year, royalty_income, distributable_income, distribution_per_unit))
    return out


def extract_distribution_totals_text(text: str) -> List[Tuple[int, float, float, float]]:
    """
    Regex fallback for quarterly distributable-income totals across messy whitespace.
    """
    out: List[Tuple[int, float, float, float]] = []
    pat = re.compile(
        r"(20\d{2})\s+royalty\s+income\s+distributable\s+income.*?"
        r"total\s*\$?\s*([0-9,]+(?:\.\d+)?)\s*\$?\s*([0-9,]+(?:\.\d+)?)\s*\$?\s*([0-9]*\.\d+)",
        re.IGNORECASE | re.DOTALL,
    )
    for m in pat.finditer(text):
        yr = int(m.group(1))
        royalty_income = parse_num(m.group(2))
        distributable_income = parse_num(m.group(3))
        distribution_per_unit = float(m.group(4))
        if 0 <= distribution_per_unit <= 20:
            out.append((yr, royalty_income, distributable_income, distribution_per_unit))
    return out


def extract_reserve_prose(text: str, default_year: Optional[int]) -> List[Tuple[int, str, float]]:
    """
    Extract reserve headline prose, when present.
    Returns list of (report_year, label, value).
    """
    out: List[Tuple[int, str, float]] = []
    if default_year is None:
        return out
    for m in RESERVE_PROSE_RE.finditer(text):
        oil_mmbbl = float(m.group(1))
        gas_bcf = float(m.group(2))
        future_net_value_thousands = parse_num(m.group(3)) / 1000.0
        discounted_value_thousands = parse_num(m.group(4)) / 1000.0
        out.extend([
            (default_year, "proved_oil_mmbbl", oil_mmbbl),
            (default_year, "proved_gas_bcf", gas_bcf),
            (default_year, "future_net_value_thousands", future_net_value_thousands),
            (default_year, "discounted_value_thousands", discounted_value_thousands),
        ])
    # Fallback: just oil/gas proved quantities.
    if not out:
        for m in RESERVE_SIMPLE_RE.finditer(text):
            out.extend([
                (default_year, "proved_oil_mmbbl", float(m.group(1))),
                (default_year, "proved_gas_bcf", float(m.group(2))),
            ])
    return out


def extract_production_prose(text: str) -> List[Tuple[int, str, float]]:
    """
    Extract production headline prose when explicitly expressed as:
    '... production ... X million barrels of oil ... Y billion cubic feet of gas'
    Returns list of (report_year, label, value).
    """
    out: List[Tuple[int, str, float]] = []
    for m in PRODUCTION_PROSE_RE.finditer(text):
        yr = int(m.group(1))
        oil_mmbbl = float(m.group(2))
        gas_bcf = float(m.group(3))
        out.append((yr, "annual_oil_production_mmbbl", oil_mmbbl))
        out.append((yr, "annual_gas_production_bcf", gas_bcf))
    return out


def extract_reserve_table(text: str, default_year: Optional[int]) -> List[Tuple[int, str, float]]:
    """
    Parse reserve quantities from table-like rows in model_inputs text.
    Returns (report_year, label, value) where oil/gas are normalized to MMbbl/BCF.

    Handles two formats:
    1. Inline header: "oil (mstb) gas (mcf)" on same line, then "total proved" row.
    2. State-by-state (DeGolyer/SBR style): one-value-per-line layout where column
       headers span multiple lines (Oil and Condensate (Mbbl) / NGL (Mbbl) / Total
       Liquids (Mbbl) / Sales Gas (MMcf)) and state rows lead to a bare "Total" line.
       We collect all lone numeric lines after "Total" until the next prose line to
       get [oil_mbbl, ngl_mbbl, total_liquids_mbbl, gas_mmcf].
    """
    out: List[Tuple[int, str, float]] = []
    if default_year is None:
        return out

    lines = text.splitlines()
    candidates: List[Tuple[float, float]] = []
    in_reserve_qty_table = False
    in_pd_by_date_table = False

    # --- state-by-state table detection ---
    in_state_table = False
    after_total = False
    state_table_has_states = False
    state_table_nums: List[float] = []
    STATE_NAMES = {"florida", "louisiana", "mississippi", "new mexico", "oklahoma", "texas",
                   "wyoming", "utah", "colorado", "north dakota", "montana", "kansas"}
    LONE_NUM_RE = re.compile(r"^\s*[\d,]+\s*$")

    for i, line in enumerate(lines):
        c = clean(line)
        n = norm(line)
        stripped = line.strip()

        # --- Format 1: inline headers ---
        if "oil (mstb)" in n and "gas (mcf)" in n:
            in_reserve_qty_table = True
            continue
        if "proved developed reserves" in n and "oil (barrels)" in n and "gas (mcf)" in n:
            in_pd_by_date_table = True
            continue

        if in_reserve_qty_table and "total proved" in n:
            nums = extract_numbers(c)
            if len(nums) >= 2:
                candidates.append((abs(nums[0]), abs(nums[1])))
            in_reserve_qty_table = False
            continue

        if in_pd_by_date_table and f"december 31, {default_year}" in n:
            nums = extract_numbers(c)
            if len(nums) >= 2:
                candidates.append((abs(nums[0]), abs(nums[1])))
            in_pd_by_date_table = False
            continue

        # --- Format 2: state-by-state (SBR / DeGolyer style) ---
        # Trigger: standalone header line (short, not buried in a prose sentence).
        if (
            "net proved developed producing reserves" in n
            and len(stripped) <= 60
            and not in_state_table
        ):
            in_state_table = True
            after_total = False
            state_table_has_states = False
            state_table_nums = []
            continue

        if in_state_table:
            # Track whether we've seen at least one state name (confirms we're in the right table).
            if n in STATE_NAMES:
                state_table_has_states = True

            # "Total" on its own line signals the last aggregated row.
            if stripped.lower() == "total" and not after_total and state_table_has_states:
                after_total = True
                state_table_nums = []
                continue

            if after_total:
                if LONE_NUM_RE.match(stripped):
                    val = float(stripped.replace(",", ""))
                    state_table_nums.append(val)
                elif stripped and not stripped.isspace():
                    # First non-numeric non-blank line ends the number block.
                    if len(state_table_nums) >= 4:
                        # Layout: oil_mbbl, ngl_mbbl, total_liquids_mbbl, gas_mmcf
                        oil_mbbl = state_table_nums[0]
                        gas_mmcf = state_table_nums[3]
                        candidates.append((oil_mbbl, gas_mmcf))
                    elif len(state_table_nums) >= 2:
                        candidates.append((state_table_nums[0], state_table_nums[-1]))
                    in_state_table = False
                    after_total = False
                    state_table_has_states = False
                    state_table_nums = []

    # Flush state-table if file ended while collecting.
    if after_total and len(state_table_nums) >= 4:
        candidates.append((state_table_nums[0], state_table_nums[3]))

    if not candidates:
        return out

    oil_kbbl, gas_mmcf = max(candidates, key=lambda x: x[0])
    out.append((default_year, "proved_oil_mmbbl", oil_kbbl / 1000.0))
    out.append((default_year, "proved_gas_bcf", gas_mmcf / 1000.0))
    return out


def extract_production_table(lines: List[str]) -> List[Tuple[int, str, float]]:
    """
    Parse annual production rows with a nearby year header:
      Oil (barrels) ... [y1 y2 y3 values]
      Gas (Mcf)     ... [y1 y2 y3 values]
    """
    out: List[Tuple[int, str, float]] = []
    for idx, line in enumerate(lines):
        n = norm(line)
        if "oil (barrel" not in n and "gas (mcf" not in n and "natural gas (mcf" not in n:
            continue
        if "proved developed reserves" in n:
            continue

        years: List[int] = []
        for back in range(1, 5):
            j = idx - back
            if j < 0:
                break
            ys = YEAR_RE.findall(clean(lines[j]))
            uniq = []
            seen = set()
            for y in ys:
                if y not in seen:
                    seen.add(y)
                    uniq.append(int(y))
            if len(uniq) >= 2:
                years = uniq
                break
        if not years:
            continue

        nums = extract_numbers(line)
        if len(nums) < len(years):
            continue
        vals = nums[-len(years):]
        label = "annual_oil_production_barrels" if "oil (barrel" in n else "annual_gas_production_mcf"
        for i, yr in enumerate(years):
            out.append((yr, label, vals[i]))
    return out


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

    # --- Distribution totals from quarterly schedule tables ---
    dist_totals = extract_distribution_totals(lines)
    for yr, royalty_income_k, distributable_income_k, distribution_per_unit in dist_totals:
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": "royalty_income_total",
            "value_thousands": royalty_income_k,
        })
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": "distributable_income_total",
            "value_thousands": distributable_income_k,
        })
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": "distribution_per_unit_total",
            "value_thousands": distribution_per_unit,
        })

    # --- Reserve prose headline fields ---
    reserve_rows = extract_reserve_prose(full_text, years[0] if years else None)
    for yr, label, val in reserve_rows:
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "reserves",
            "label": label,
            "value_thousands": val,
        })

    return rows


def parse_model_inputs_file(path: Path) -> List[Row]:
    """
    Parse one *_model_inputs.txt file for supplemental reserves/operations fields.
    """
    ticker = path.parent.name
    filing_date = path.stem.replace("_model_inputs", "")
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    years = detect_years(lines)
    rows: List[Row] = []
    filing_year = None
    try:
        filing_year = int(filing_date.split("-")[0])
    except Exception:
        filing_year = None

    # Distribution totals
    dist_rows = extract_distribution_totals(lines)
    if not dist_rows:
        dist_rows = extract_distribution_totals_text(text)
    for yr, royalty_income_k, distributable_income_k, distribution_per_unit in dist_rows:
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": "royalty_income_total",
            "value_thousands": royalty_income_k,
        })
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": "distributable_income_total",
            "value_thousands": distributable_income_k,
        })
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": "distribution_per_unit_total",
            "value_thousands": distribution_per_unit,
        })

    # Reserve table/prose fields — prefer filing_year; fall back to detected years.
    default_year = filing_year if filing_year else (years[0] if years else None)
    for yr, label, val in extract_reserve_table(text, default_year):
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "reserves",
            "label": label,
            "value_thousands": val,
        })
    for yr, label, val in extract_reserve_prose(text, default_year):
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "reserves",
            "label": label,
            "value_thousands": val,
        })

    # Production table/prose fields
    for yr, label, val in extract_production_table(lines):
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": label,
            "value_thousands": val,
        })
    for yr, label, val in extract_production_prose(text):
        rows.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "report_year": yr,
            "section": "operations",
            "label": label,
            "value_thousands": val,
        })

    # De-duplicate rows by exact key while keeping first seen value.
    deduped: Dict[Tuple[str, int, str], Row] = {}
    for r in rows:
        k = (r["section"], int(r["report_year"]), r["label"])
        if k not in deduped:
            deduped[k] = r
    return list(deduped.values())


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
    ("operations", "royalty_income_total"),
    ("operations", "distributable_income_total"),
    ("operations", "distribution_per_unit_total"),
    ("operations", "annual_oil_production_barrels"),
    ("operations", "annual_gas_production_mcf"),
    ("reserves", "proved_oil_mmbbl"),
    ("reserves", "proved_gas_bcf"),
    ("reserves", "future_net_value_thousands"),
    ("reserves", "discounted_value_thousands"),
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
    std_files = sorted(out_root.glob("*/*_standardized_measure.txt"))
    model_files = sorted(out_root.glob("*/*_model_inputs.txt"))

    if not std_files:
        print("No .txt files found under", out_root, file=sys.stderr)
        sys.exit(1)

    all_rows: List[Row] = []
    for path in std_files:
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

    # Parse supplemental model-input windows (if present)
    for path in model_files:
        ticker = path.parent.name
        if ticker == "MARPS":
            continue
        filing = path.stem.replace("_model_inputs", "")
        rows = parse_model_inputs_file(path)
        if rows:
            print(f"  Parsing {ticker}/{filing} model-inputs ...  {len(rows)} data points")
            all_rows.extend(rows)

    # Deduplicate exact rows from overlapping sources.
    deduped: Dict[Tuple[str, str, int, str, str], Row] = {}
    for r in all_rows:
        key = (r["ticker"], r["filing_date"], int(r["report_year"]), r["section"], r["label"])
        if key not in deduped:
            deduped[key] = r
    all_rows = list(deduped.values())

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
