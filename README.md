# Oil Royalty Trust — Standardized Measure Extractor

Downloads 10-K filings for oil royalty trusts from SEC EDGAR, extracts the
**Standardized Measure of Discounted Future Net Cash Flows** (SFAS 69 / ASC 932),
parses the numbers into a structured CSV, and runs a PV10-based NAV valuation.

## Scripts

| Script | What it does |
|--------|--------------|
| `extract_standardized_measure.py` | Downloads 10-Ks, extracts the SM section → `output/<TICKER>/<date>_standardized_measure.{txt,html}` |
| `sec_client.py` | Standalone SEC EDGAR client (CIK lookup, filing index, retry logic) |
| `parse_to_csv.py` | Parses extracted `.txt` files into structured CSV |
| `valuation_model.py` | P/PV10 NAV model with live EIA price fetching |
| `run_update.sh` | Weekly update wrapper: extract → parse → model |

## Tickers covered

Classic royalty trusts: `PBT SBR SJT CRT PVL PRT VOC MVO`
Mineral rights aggregators: `BSM VNOM DMLP PHX`
Other: `TPL`

## Quick start

```bash
pip install requests beautifulsoup4 lxml

# Extract last 5 years of 10-Ks (set your email for SEC User-Agent)
export SEC_USER_AGENT="Your Name your@email.com"
python extract_standardized_measure.py --years-back 5

# Parse to CSV
python parse_to_csv.py

# Valuation (defaults)
python valuation_model.py --oil 70 --gas 2.50

# Valuation with live EIA prices
python valuation_model.py --eia-key YOUR_EIA_KEY

# Valuation via Cloudflare Worker (no local EIA key needed)
python valuation_model.py --price-url https://oil-price-oracle.YOUR_SUBDOMAIN.workers.dev
```

Get a free EIA API key at [eia.gov/opendata/register.php](https://www.eia.gov/opendata/register.php).

## Output

```
output/
├── SBR/
│   ├── 2025-12-31_standardized_measure.txt
│   ├── 2025-12-31_standardized_measure.html
│   └── ...
├── standardized_measure_data.csv   ← long format (ticker/year/label/value)
└── standardized_measure_wide.csv   ← wide format (one row per ticker/filing)
```

## Cloudflare Worker — price oracle

`worker.js` + `wrangler.toml` define a Cloudflare Worker that runs a **monthly cron** to fetch
WTI and HH prices from EIA and cache them in KV storage. The valuation model
can query this endpoint instead of hitting EIA directly.

### Deploy

```bash
# 1. Authenticate
npx wrangler login

# 2. Create KV namespace, paste the returned id into wrangler.toml
npx wrangler kv:namespace create PRICES

# 3. Deploy
npx wrangler deploy
```

Then in the Cloudflare dashboard → Workers & Pages → oil-price-oracle → Settings → Variables and Secrets, add `EIA_API_KEY`.

## Automated updates

```bash
bash run_update.sh          # run manually
```

For scheduled runs, add a Windows Task Scheduler entry (PowerShell, run as admin):

```powershell
$action  = New-ScheduledTaskAction -Execute "bash" -Argument "-c 'bash run_update.sh'" -WorkingDirectory "C:\path\to\oil_royalty_extractor"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At "8:23am"
Register-ScheduledTask -TaskName "OilRoyaltyExtractor" -Action $action -Trigger $trigger -RunLevel Highest
```

## Worker valuation app (Cloudflare)

The Worker now serves both cached commodity prices and a simple interactive trust model UI.

### Worker routes

- `GET /` or `GET /prices/latest` → latest cached WTI/HH snapshot
- `GET /history` or `GET /prices/history` → cached price history
- `GET /app` → interactive valuation frontend (browser UI)
- `GET /valuation/latest` → full valuation payload (JSON)
- `GET /valuation/ranked` → ranked rows by metric/order/top (JSON)
- `GET /valuation/{ticker}` → single ticker valuation + diagnostics (JSON)

### Query params (valuation routes)

- `oil` and `gas` (optional): override assumptions for scenario modeling
- `metric` (ranked only): `p_adj_pv10`, `p_pv10`, `market_cap_m`, `pv10_m`, `adj_pv10_m`
- `order` (ranked only): `asc` or `desc`
- `top` (ranked only): max rows to return

### Local test with Wrangler

```bash
npx wrangler dev --local --port 8787
```

Then open:

- [http://127.0.0.1:8787/app](http://127.0.0.1:8787/app)

Or call APIs:

```bash
curl "http://127.0.0.1:8787/valuation/latest?oil=70&gas=3"
curl "http://127.0.0.1:8787/valuation/ranked?metric=p_adj_pv10&order=asc&top=5&oil=70&gas=3"
curl "http://127.0.0.1:8787/valuation/SBR?oil=70&gas=3"
```
