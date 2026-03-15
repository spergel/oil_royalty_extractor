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

# EIA API endpoints
_EIA_V2_BASE = "https://api.eia.gov/v2"
_EIA_V1_BASE = "https://api.eia.gov/series"


# ---------------------------------------------------------------------------
# Live commodity price fetching
# ---------------------------------------------------------------------------

def _get(url: str, timeout: int = 15):
    """Simple HTTP GET; returns parsed JSON or raises."""
    import urllib.request, json
    req = urllib.request.Request(url, headers={"User-Agent": "oil-royalty-valuation/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _eia_v2_price(api_key: str, route: str, series: str) -> Tuple[float, str]:
    """
    Fetch most recent monthly value from EIA API v2.
    Returns (price, period_string).
    """
    url = (
        f"{_EIA_V2_BASE}/{route}/data/"
        f"?api_key={api_key}"
        f"&frequency=monthly"
        f"&data[0]=value"
        f"&facets[series][]={series}"
        f"&sort[0][column]=period&sort[0][direction]=desc"
        f"&length=1"
    )
    data = _get(url)
    row = data["response"]["data"][0]
    return float(row["value"]), row["period"]


def _eia_v1_price(api_key: str, series_id: str) -> Tuple[float, str]:
    """Fetch most recent monthly value from EIA API v1 (fallback)."""
    url = f"{_EIA_V1_BASE}/?api_key={api_key}&series_id={series_id}&num=1"
    data = _get(url)
    point = data["series"][0]["data"][0]   # ["2024-12", 69.97]
    return float(point[1]), str(point[0])


def fetch_eia_prices(api_key: str) -> Tuple[float, float]:
    """
    Returns (wti_per_bbl, hh_per_mmbtu) from EIA.
    Tries v2 first; falls back to v1 for each commodity independently.
    """
    # WTI crude oil
    try:
        wti, wti_period = _eia_v2_price(api_key, "petroleum/pri/spt", "RWTC")
        print(f"  EIA WTI  {wti_period}: ${wti:.2f}/bbl  (v2)")
    except Exception as e:
        print(f"  EIA WTI v2 failed ({e}), trying v1 ...")
        try:
            wti, wti_period = _eia_v1_price(api_key, "PET.RWTC.M")
            print(f"  EIA WTI  {wti_period}: ${wti:.2f}/bbl  (v1)")
        except Exception as e2:
            print(f"  EIA WTI v1 also failed ({e2}), using default $72.00", file=sys.stderr)
            wti = 72.0

    # Henry Hub natural gas
    try:
        hh, hh_period = _eia_v2_price(api_key, "natural-gas/pri/sum", "RNGWHHD")
        print(f"  EIA HH   {hh_period}: ${hh:.3f}/MMBtu  (v2)")
    except Exception as e:
        print(f"  EIA HH v2 failed ({e}), trying v1 ...")
        try:
            hh, hh_period = _eia_v1_price(api_key, "NG.RNGWHHD.M")
            print(f"  EIA HH   {hh_period}: ${hh:.3f}/MMBtu  (v1)")
        except Exception as e2:
            print(f"  EIA HH v1 also failed ({e2}), using default $2.50", file=sys.stderr)
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

    print()
    print(
        "Notes:\n"
        "  PV10      = standardized measure (SEC 12-mo avg prices, 10% discount)\n"
        "  Adj PV10  = PV10 scaled by (current_oil / sec_oil) -- rough oil repricing only\n"
        "  P/PV10    = market_cap / PV10 (< 1.0x = trading below SEC reserve value)\n"
        "  Prices and units outstanding are approximate -- update TRUST_META in script\n"
        f"  Strip prices used: WTI ${current_oil:.2f}/bbl, HH ${current_gas:.2f}/MMBtu"
    )


if __name__ == "__main__":
    main()
