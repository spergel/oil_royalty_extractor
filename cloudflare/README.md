# oil-price-oracle — Cloudflare Worker

Monthly cron that fetches WTI crude + Henry Hub gas prices from EIA and
serves them as JSON. The Python valuation model queries this endpoint so it
always uses live strip prices without needing an EIA key locally.

## Architecture

```
[Cloudflare Worker cron: 1st of month]
        |
        v
   EIA API (api.eia.gov)
   WTI spot + HH spot
        |
        v
   Cloudflare KV (cached)
        |
        v
   HTTP GET /  ->  {"wti_per_bbl": 70.5, "hh_per_mmbtu": 2.85, "period": "2026-02", ...}
        ^
        |
   valuation_model.py --price-url https://oil-price-oracle.YOUR.workers.dev
```

## Prerequisites

- [Cloudflare account](https://dash.cloudflare.com/) (free tier is fine)
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/): `npm install -g wrangler`
- Free [EIA API key](https://www.eia.gov/opendata/register.php)

## Deploy (one-time setup)

```bash
cd cloudflare/

# 1. Authenticate wrangler with Cloudflare
wrangler login

# 2. Create the KV namespace and paste the returned ID into wrangler.toml
wrangler kv:namespace create PRICES
# → outputs something like: id = "abc123def456..."
# Edit wrangler.toml: replace REPLACE_WITH_KV_NAMESPACE_ID with that id

# 3. Store your EIA API key as an encrypted secret (never committed to git)
wrangler secret put EIA_API_KEY
# (paste your key when prompted)

# 4. Deploy
wrangler deploy

# 5. Trigger the cron manually to populate KV right away
wrangler dev   # then in another terminal:
curl "http://localhost:8787/__scheduled?cron=17+14+1+*+*"
# Or after deploy, trigger via dashboard: Workers & Pages -> oil-price-oracle -> Triggers -> Test
```

## Endpoints

| Path       | Description                            |
|------------|----------------------------------------|
| `GET /`    | Latest price snapshot (JSON)           |
| `GET /history` | Last 24 monthly snapshots (JSON)  |

### Example response

```json
{
  "updated": "2026-03-01T14:17:03.421Z",
  "period": "2026-02",
  "wti_per_bbl": 70.45,
  "hh_per_mmbtu": 2.83,
  "sources": {
    "wti": "eia-v2",
    "hh": "eia-v2"
  }
}
```

## Using the Worker with the valuation model

```bash
python valuation_model.py --price-url https://oil-price-oracle.YOUR_SUBDOMAIN.workers.dev
```

Or set it permanently in `run_update.sh`:
```bash
python valuation_model.py --price-url https://oil-price-oracle.YOUR_SUBDOMAIN.workers.dev
```

## Cron schedule

`17 14 1 * *` — 1st of every month at 14:17 UTC.
Cloudflare Workers cron triggers are always UTC.
Data typically lags EIA by 1-4 weeks so the 1st-of-month fetch reliably
gets the prior month's published price.

## Updating the cron schedule

Edit `wrangler.toml`:
```toml
[triggers]
crons = ["17 14 1 * *"]   # monthly on the 1st
# crons = ["17 14 * * 1"]  # or weekly on Mondays
```
Then `wrangler deploy` to push the change.
