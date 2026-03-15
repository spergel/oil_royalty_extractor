#!/usr/bin/env python3
"""
valuation_model.py -- Simple PV10-based NAV valuation for oil royalty trusts.

Reads the parsed CSV produced by parse_to_csv.py and computes:
  - PV10 (standardized measure) per trust, most recent year
  - Price-to-PV10 ratio (market_cap / PV10) using hardcoded trust metadata
  - Strip-adjusted PV10: scales PV10 by (current_oil_price / sec_oil_price)
    to approximate value at today's strip vs the SEC 12-month average

Commodity price sources (in priority order):
  1. --price-url URL  : fetch JSON from Cloudflare Worker or any compatible endpoint
                        expects: {"wti_per_bbl": float, "hh_per_mmbtu": float, ...}
  2. --eia-key KEY    : fetch directly from EIA API (api.eia.gov)
                        env var: EIA_API_KEY
  3. --oil / --gas   : explicit override (default: 72.00 / 2.50)

Usage:
  python valuation_model.py
  python valuation_model.py --oil 70 --gas 2.40
  python valuation_model.py --eia-key YOUR_KEY
  python valuation_model.py --price-url https://oil-price-oracle.YOUR_SUBDOMAIN.workers.dev
  python valuation_model.py --update-prices          # refresh unit prices via yfinance
"""

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# Trust metadata
# Prices and units as of March 2026 (update periodically).
# units_M: units / shares outstanding in millions
# price:   recent market price in USD per unit
# ---------------------------------------------------------------------------

TRUST_META = {
    # ticker: {"units_M": float, "price": float, "description": str}
    "SBR":  {"units_M": 14.70,  "price": 18.50, "desc": "Sabine Royalty Trust"},
    "PBT":  {"units_M": 46.61,  "price":  7.60, "desc": "Permian Basin Royalty Trust"},
    "CRT":  {"units_M":  6.00,  "price": 17.00, "desc": "Cross Timbers Royalty Trust"},
    "SJT":  {"units_M": 46.61,  "price":  4.20, "desc": "San Juan Basin Royalty Trust"},
    "VOC":  {"units_M": 22.49,  "price":  8.80, "desc": "VOC Energy Trust"},
    "MVO":  {"units_M":  3.60,  "price":  6.50, "desc": "MV Oil Trust"},
    "PVL":  {"units_M": 30.83,  "price":  3.10, "desc": "Permianville Royalty Trust"},
    "PRT":  {"units_M": 11.55,  "price":  6.20, "desc": "PermRock Royalty Trust"},
    "BSM":  {"units_M": 210.00, "price": 14.80, "desc": "Black Stone Minerals LP"},
    "VNOM": {"units_M": 430.00, "price": 34.00, "desc": "Viper Energy Inc"},
    "DMLP": {"units_M":  54.00, "price": 24.50, "desc": "Dorchester Minerals LP"},
    "PHX":  {"units_M":  16.50, "price": 18.00, "desc": "PHX Minerals / Prairie Operating"},
    "TPL":  {"units_M":  22.30, "price":1100.00, "desc": "Texas Pacific Land"},
}

# SEC 12-month average prices used for the most recent PV10 calculation.
# These are populated from the parsed CSV; fallback defaults below.
SEC_OIL_DEFAULTS = {
    "SBR": 76.32, "PBT": 75.48, "CRT": 75.48, "SJT": 66.35,
    "VOC": 74.00, "MVO": 74.00, "PVL": 75.48, "PRT": 75.48,
    "BSM": 75.00, "VNOM": 75.61, "DMLP": 61.19, "TPL": 75.00,
}

DATA_CSV = Path(__file__).parent / "output" / "standardized_measure_data.csv"

# EIA API endpoint
_EIA_V2_BASE = "https://api.eia.gov/v2"


# ---------------------------------------------------------------------------
# Live commodity price fetching
# ---------------------------------------------------------------------------

def _get(url: str, timeout: int = 15):
    """Simple HTTP GET; returns parsed JSON or raises."""
    import urllib.request, json
    req = urllib.request.Request(url, headers={"User-Agent": "oil-royalty-valuation/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _eia_v2_price_seriesid(api_key: str, series_id: str) -> Tuple[float, str]:
    """
    Fetch most recent monthly value from EIA API v2 seriesid endpoint.
    Returns (price, period_string).
    """
    cleaned_key = str(api_key).strip().strip("'").strip('"')
    if not cleaned_key:
        raise ValueError("missing EIA API key")
    url = f"{_EIA_V2_BASE}/seriesid/{series_id}?api_key={cleaned_key}"
    data = _get(url)
    rows = data.get("response", {}).get("data", [])
    if not rows:
        raise ValueError(f"EIA v2 returned no data for {series_id}")
    # Rows are typically descending; max() is defensive.
    row = max(rows, key=lambda r: str(r.get("period", "")))
    return float(row["value"]), str(row["period"])


def fetch_eia_prices(api_key: str) -> Tuple[float, float]:
    """
    Returns (wti_per_bbl, hh_per_mmbtu) from EIA.
    Uses EIA v2 seriesid endpoints.
    """
    # WTI crude oil
    try:
        wti, wti_period = _eia_v2_price_seriesid(api_key, "PET.RWTC.M")
        print(f"  EIA WTI  {wti_period}: ${wti:.2f}/bbl  (v2 seriesid)")
    except Exception as e:
        print(f"  EIA WTI v2 seriesid failed ({e}), using default $72.00", file=sys.stderr)
        wti = 72.0

    # Henry Hub natural gas
    try:
        hh, hh_period = _eia_v2_price_seriesid(api_key, "NG.RNGWHHD.M")
        print(f"  EIA HH   {hh_period}: ${hh:.3f}/MMBtu  (v2 seriesid)")
    except Exception as e:
        print(f"  EIA HH v2 seriesid failed ({e}), using default $2.50", file=sys.stderr)
        hh = 2.50

    return wti, hh


def fetch_prices_from_url(url: str) -> Tuple[float, float]:
    """
    Fetch prices from a JSON endpoint (e.g. the Cloudflare Worker).
    Expects keys: wti_per_bbl, hh_per_mmbtu.
    """
    data = _get(url)
    wti = float(data["wti_per_bbl"])
    hh  = float(data["hh_per_mmbtu"])
    period = data.get("period", "unknown")
    print(f"  Price oracle ({period}): WTI ${wti:.2f}/bbl  HH ${hh:.3f}/MMBtu")
    return wti, hh


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(path: Path = DATA_CSV) -> Dict:
    """
    Load parsed CSV; return:
      data[ticker][report_year][section][label] = value_thousands
    """
    data: Dict = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t  = row["ticker"]
            yr = int(row["report_year"])
            s  = row["section"]
            lb = row["label"]
            try:
                val = float(row["value_thousands"])
            except ValueError:
                continue
            data.setdefault(t, {}).setdefault(yr, {}).setdefault(s, {})[lb] = val
    return data


def latest_pv10(data: Dict, ticker: str) -> Optional[tuple]:
    """Return (report_year, pv10_thousands) for the most recent year with SM data."""
    years = sorted(data.get(ticker, {}).keys(), reverse=True)
    for yr in years:
        pv10 = data[ticker][yr].get("main", {}).get("standardized_measure")
        if pv10 and pv10 > 0:
            return yr, pv10
    return None


def latest_sec_oil_price(data: Dict, ticker: str) -> Optional[float]:
    """Return SEC oil price used for the most recent PV10 calculation.
    Only looks within 1 year of the PV10 year to avoid returning stale historical prices."""
    result = latest_pv10(data, ticker)
    if result is None:
        return None
    pv10_year, _ = result
    years = sorted(data.get(ticker, {}).keys(), reverse=True)
    for yr in years:
        if yr < pv10_year - 1:
            break  # don't use a price from >1 year before the SM year
        price = data[ticker][yr].get("prices", {}).get("oil_per_bbl")
        if price and price > 30:   # sanity: oil must be > $30
            return price
    return None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_m(v: float) -> str:
    """Format thousands -> millions string."""
    return f"${v/1000:>8,.1f}M"


def fmt_pct(v: float) -> str:
    return f"{v:>7.1f}%"


def fmt_opt_pct(v: Optional[float], width: int = 8, decimals: int = 1) -> str:
    if v is None:
        return f"{'n/a':>{width}s}"
    return f"{v * 100:>{width}.{decimals}f}%"


def fmt_opt_x(v: Optional[float], width: int = 8, decimals: int = 2) -> str:
    if v is None:
        return f"{'n/a':>{width}s}"
    return f"{v:>{width}.{decimals}f}x"


def safe_div(n: Optional[float], d: Optional[float], min_abs_d: float = 1e-9) -> Optional[float]:
    if n is None or d is None or abs(d) < min_abs_d:
        return None
    return n / d


def bar(ratio: float, width: int = 20) -> str:
    """Visual ratio bar (ratio = market_cap / PV10)."""
    filled = min(int(ratio * width / 2), width)
    return "|" + "#" * filled + " " * (width - filled) + "|"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="PV10 NAV valuation for oil royalty trusts")
    ap.add_argument("--oil",       type=float, default=None,
                    help="WTI strip price $/bbl (overrides EIA/Worker fetch)")
    ap.add_argument("--gas",       type=float, default=None,
                    help="HH strip price $/MMBtu (overrides EIA/Worker fetch)")
    ap.add_argument("--eia-key",   default=os.environ.get("EIA_API_KEY"),
                    metavar="KEY",
                    help="EIA API key (or set EIA_API_KEY env var)")
    ap.add_argument("--price-url", default=None, metavar="URL",
                    help="Cloudflare Worker / price oracle URL returning JSON with "
                         "wti_per_bbl and hh_per_mmbtu fields")
    ap.add_argument("--data",      default=str(DATA_CSV),
                    help="Path to standardized_measure_data.csv")
    ap.add_argument("--update-prices", action="store_true",
                    help="Refresh unit prices via yfinance")
    args = ap.parse_args()

    data_path = Path(args.data)
    if not data_path.exists():
        print(f"ERROR: {data_path} not found. Run parse_to_csv.py first.", file=sys.stderr)
        sys.exit(1)

    data = load_data(data_path)

    # --- Resolve commodity strip prices ---
    # Priority: explicit --oil/--gas > --price-url > --eia-key > defaults
    current_oil: float
    current_gas: float

    if args.oil is not None and args.gas is not None:
        current_oil, current_gas = args.oil, args.gas
        print(f"Strip prices (manual): WTI ${current_oil:.2f}/bbl  HH ${current_gas:.3f}/MMBtu")

    elif args.price_url:
        print(f"Fetching prices from {args.price_url} ...")
        try:
            current_oil, current_gas = fetch_prices_from_url(args.price_url)
        except Exception as e:
            print(f"  WARNING: price URL failed ({e}), using defaults", file=sys.stderr)
            current_oil, current_gas = args.oil or 72.0, args.gas or 2.50

    elif args.eia_key:
        print("Fetching prices from EIA ...")
        current_oil, current_gas = fetch_eia_prices(args.eia_key)
        # Allow --oil / --gas to override individual legs
        if args.oil is not None: current_oil = args.oil
        if args.gas is not None: current_gas = args.gas

    else:
        current_oil = args.oil or 72.0
        current_gas = args.gas or 2.50
        print(f"Strip prices (defaults -- use --eia-key or --price-url for live data): "
              f"WTI ${current_oil:.2f}/bbl  HH ${current_gas:.3f}/MMBtu")

    # --- Optionally update unit prices with live market data ---
    meta = {k: dict(v) for k, v in TRUST_META.items()}
    if args.update_prices:
        try:
            import yfinance as yf
            for ticker in meta:
                try:
                    t = yf.Ticker(ticker)
                    hist = t.history(period="1d")
                    if not hist.empty:
                        meta[ticker]["price"] = float(hist["Close"].iloc[-1])
                        print(f"  {ticker}: updated price -> ${meta[ticker]['price']:.2f}")
                except Exception as e:
                    print(f"  {ticker}: yfinance error ({e}), using default", file=sys.stderr)
        except ImportError:
            print("yfinance not installed; using hardcoded prices. "
                  "Install with: pip install yfinance", file=sys.stderr)

    print(f"\nStrip: WTI ${current_oil:.2f}/bbl  |  HH ${current_gas:.3f}/MMBtu")
    print()

    # Header
    header = (
        f"{'Ticker':6s}  {'Desc':30s}  {'Year':4s}  "
        f"{'PV10 ($M)':>10s}  {'Mkt Cap($M)':>11s}  "
        f"{'P/PV10':>7s}  {'SEC Oil':>8s}  {'Adj PV10':>10s}  {'P/AdjPV10':>10s}"
    )
    print(header)
    print("-" * len(header))

    tickers_with_data = sorted(t for t in meta if t in data)
    extra_rows = []
    for ticker in tickers_with_data:
        result = latest_pv10(data, ticker)
        if result is None:
            print(f"{ticker:6s}  {'(no standardized measure data)':30s}")
            continue
        yr, pv10_k = result

        m = meta[ticker]
        units_m = m["units_M"]
        price   = m["price"]
        mktcap_k = price * units_m * 1_000   # -> thousands

        # Unadjusted P/PV10
        p_pv10 = mktcap_k / pv10_k if pv10_k else float("nan")

        # Strip-adjusted PV10: simple linear reprice
        sec_oil = latest_sec_oil_price(data, ticker) or SEC_OIL_DEFAULTS.get(ticker, 75.0)
        if sec_oil > 0:
            adj_pv10_k = pv10_k * (current_oil / sec_oil)
        else:
            adj_pv10_k = pv10_k
        p_adj_pv10 = mktcap_k / adj_pv10_k if adj_pv10_k else float("nan")

        print(
            f"{ticker:6s}  {m['desc']:30s}  {yr}  "
            f"{pv10_k/1000:>10,.1f}  {mktcap_k/1000:>11,.1f}  "
            f"{p_pv10:>7.2f}x  ${sec_oil:>6.2f}  "
            f"{adj_pv10_k/1000:>10,.1f}  {p_adj_pv10:>10.2f}x"
        )

        # Additional valuation diagnostics from SFAS 69 components
        yr_main = data[ticker].get(yr, {}).get("main", {})
        yr_changes = data[ticker].get(yr, {}).get("changes", {})

        future_cash_inflows = yr_main.get("future_cash_inflows")
        future_net_cash_flows = yr_main.get("future_net_cash_flows")
        future_prod_costs = yr_main.get("future_production_costs")
        future_dev_costs = yr_main.get("future_development_costs")
        future_prod_taxes = yr_main.get("future_production_taxes")
        future_income_tax = yr_main.get("future_income_tax")
        pv10_discount = yr_main.get("pv10_discount")
        end_sm = yr_changes.get("end_standardized_measure")
        if end_sm is None:
            end_sm = pv10_k

        # Costs/taxes in this dataset are often negative cash outflows.
        total_cost_outflow = None
        if any(v is not None for v in [future_prod_costs, future_dev_costs, future_prod_taxes]):
            total_cost_outflow = sum(abs(v or 0.0) for v in [future_prod_costs, future_dev_costs, future_prod_taxes])
        # Denominator is in $thousands; skip ratios when inflows are tiny/noisy.
        cost_burden = safe_div(total_cost_outflow, future_cash_inflows, min_abs_d=1_000.0)
        tax_burden = safe_div(abs(future_income_tax) if future_income_tax is not None else None, future_cash_inflows, min_abs_d=1_000.0)
        discount_drag = safe_div(abs(pv10_discount) if pv10_discount is not None else None, future_net_cash_flows)

        sales_reserves = yr_changes.get("sales_reserves")
        extensions_discoveries = yr_changes.get("extensions_discoveries")
        purchases_reserves = yr_changes.get("purchases_reserves")
        reserve_additions = (extensions_discoveries or 0.0) + (purchases_reserves or 0.0)
        reserve_replacement = safe_div(reserve_additions, abs(sales_reserves) if sales_reserves is not None else None)

        net_price_changes = yr_changes.get("net_price_changes")
        price_leverage = safe_div(net_price_changes, end_sm)

        # Suppress extreme outliers often caused by tiny/noisy denominators.
        if cost_burden is not None and cost_burden > 5.0:
            cost_burden = None
        if tax_burden is not None and tax_burden > 2.0:
            tax_burden = None
        if discount_drag is not None and discount_drag > 2.0:
            discount_drag = None
        if reserve_replacement is not None and reserve_replacement > 10.0:
            reserve_replacement = None

        extra_rows.append({
            "ticker": ticker,
            "year": yr,
            "p_adj_pv10": p_adj_pv10,
            "cost_burden": cost_burden,
            "tax_burden": tax_burden,
            "discount_drag": discount_drag,
            "reserve_replacement": reserve_replacement,
            "price_leverage": price_leverage,
        })

    print()
    if extra_rows:
        print("Additional diagnostics (latest standardized-measure year)")
        diag_header = (
            f"{'Ticker':6s}  {'Year':4s}  {'P/AdjPV10':>9s}  {'Cost/Inflow':>11s}  "
            f"{'Tax/Inflow':>10s}  {'DiscDrag':>9s}  {'ResRepl':>8s}  {'PriceLev':>9s}"
        )
        print(diag_header)
        print("-" * len(diag_header))

        for row in sorted(extra_rows, key=lambda r: r["p_adj_pv10"]):
            print(
                f"{row['ticker']:6s}  {row['year']}  "
                f"{row['p_adj_pv10']:>9.2f}x  "
                f"{fmt_opt_pct(row['cost_burden'], 11, 1)}  "
                f"{fmt_opt_pct(row['tax_burden'], 10, 1)}  "
                f"{fmt_opt_pct(row['discount_drag'], 9, 1)}  "
                f"{fmt_opt_x(row['reserve_replacement'], 8, 2)}  "
                f"{fmt_opt_pct(row['price_leverage'], 9, 1)}"
            )
        print()

        # A practical "best ideas" screen: cheap + not structurally ugly.
        def passes_if_present(val: Optional[float], threshold: float) -> bool:
            return val is None or val <= threshold

        screened = [
            r for r in extra_rows
            if r["p_adj_pv10"] <= 1.6
            and passes_if_present(r["cost_burden"], 0.65)
            and passes_if_present(r["tax_burden"], 0.20)
            and passes_if_present(r["discount_drag"], 0.55)
        ]
        screened.sort(key=lambda r: r["p_adj_pv10"])

        print("Screened ideas (cheap + lower cost/tax/discount burden; missing fields allowed)")
        if screened:
            print("  " + ", ".join(
                f"{r['ticker']} ({r['p_adj_pv10']:.2f}x)" for r in screened
            ))
        else:
            print("  none pass current thresholds")
        print()

    print(
        "Notes:\n"
        "  PV10      = standardized measure (SEC 12-mo avg prices, 10% discount)\n"
        "  Adj PV10  = PV10 scaled by (current_oil / sec_oil) -- rough oil repricing only\n"
        "  P/PV10    = market_cap / PV10 (< 1.0x = trading below SEC reserve value)\n"
        "  Cost/Inflow = (prod + dev + prod taxes) / future cash inflows (lower is better)\n"
        "  Tax/Inflow  = future income tax / future cash inflows (lower is better)\n"
        "  DiscDrag    = PV10 discount impact / future net cash flows (lower is better)\n"
        "  ResRepl     = (extensions + purchases) / sales of reserves (>1.0x is favorable)\n"
        "  PriceLev    = net price changes / ending standardized measure (higher = more price-sensitive)\n"
        "  Prices and units outstanding are approximate -- update TRUST_META in script\n"
        f"  Strip prices used: WTI ${current_oil:.2f}/bbl, HH ${current_gas:.2f}/MMBtu"
    )


if __name__ == "__main__":
    main()
