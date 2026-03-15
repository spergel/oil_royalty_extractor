/**
 * oil-price-oracle — Cloudflare Worker
 *
 * Monthly cron: fetches WTI crude oil + Henry Hub natural gas spot prices
 * from the EIA API and stores them in KV storage.
 *
 * HTTP GET /           → returns latest cached prices as JSON
 * HTTP GET /history    → returns last 12 monthly snapshots
 *
 * Setup:
 *   wrangler secret put EIA_API_KEY
 *   wrangler kv:namespace create PRICES
 *   (paste the KV namespace ID into wrangler.toml)
 *   wrangler deploy
 */

// ---------------------------------------------------------------------------
// EIA API fetching
// ---------------------------------------------------------------------------

const EIA_V2_BASE = "https://api.eia.gov/v2";

/**
 * Fetch latest monthly value from an EIA v2 series id.
 * Example ids:
 *   PET.RWTC.M    -> WTI spot
 *   NG.RNGWHHD.M  -> Henry Hub spot
 */
async function fetchLatestFromSeriesId(apiKey, seriesId) {
  const cleanedKey = String(apiKey ?? "")
    .trim()
    .replace(/^"(.*)"$/, "$1")
    .replace(/^'(.*)'$/, "$1");
  if (!cleanedKey) {
    throw new Error("Missing EIA_API_KEY");
  }

  const url = `${EIA_V2_BASE}/seriesid/${seriesId}?api_key=${encodeURIComponent(cleanedKey)}`;
  const res = await fetch(url, {
    headers: {
      // EIA is stricter with anonymous API clients; send an explicit UA.
      "User-Agent": "oil-price-oracle/1.0",
      Accept: "application/json",
    },
  });
  if (!res.ok) throw new Error(`EIA v2 seriesid ${seriesId} HTTP ${res.status}`);

  const json = await res.json();
  const rows = json?.response?.data;
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error(`EIA v2 seriesid ${seriesId}: no data`);
  }

  // Prefer the most recent "YYYY-MM" period, regardless of API sort defaults.
  let latest = rows[0];
  for (const row of rows) {
    if (typeof row?.period === "string" && row.period > latest.period) {
      latest = row;
    }
  }
  if (latest?.value == null) {
    throw new Error(`EIA v2 seriesid ${seriesId}: latest row has no value`);
  }

  const value = parseFloat(latest.value);
  if (!Number.isFinite(value)) {
    throw new Error(`EIA v2 seriesid ${seriesId}: invalid numeric value`);
  }
  return { value, period: latest.period, source: "eia-v2-seriesid" };
}

/**
 * Fetch WTI spot price ($/bbl) — most recent monthly value.
 */
async function fetchWTI(apiKey) {
  return fetchLatestFromSeriesId(apiKey, "PET.RWTC.M");
}

/**
 * Fetch Henry Hub natural gas spot price ($/MMBtu) — most recent monthly value.
 */
async function fetchHenryHub(apiKey) {
  return fetchLatestFromSeriesId(apiKey, "NG.RNGWHHD.M");
}

/**
 * Pull both prices and return a snapshot object.
 */
async function fetchPrices(apiKey) {
  const [wti, hh] = await Promise.all([
    fetchWTI(apiKey),
    fetchHenryHub(apiKey),
  ]);
  return {
    updated: new Date().toISOString(),
    period: wti.period,          // e.g. "2026-02"
    wti_per_bbl: wti.value,
    hh_per_mmbtu: hh.value,
    sources: { wti: wti.source, hh: hh.source },
  };
}

// ---------------------------------------------------------------------------
// KV helpers
// ---------------------------------------------------------------------------

const LATEST_KEY  = "latest";
const HISTORY_KEY = "history";
const MAX_HISTORY = 24; // keep 2 years of monthly snapshots

async function storeSnapshot(kv, snapshot) {
  // Update latest
  await kv.put(LATEST_KEY, JSON.stringify(snapshot));

  // Append to history (keep last MAX_HISTORY entries)
  let history = [];
  try {
    const raw = await kv.get(HISTORY_KEY);
    if (raw) history = JSON.parse(raw);
  } catch (_) {}
  history.unshift(snapshot);
  if (history.length > MAX_HISTORY) history = history.slice(0, MAX_HISTORY);
  await kv.put(HISTORY_KEY, JSON.stringify(history));
}

// ---------------------------------------------------------------------------
// HTTP handler + cron
// ---------------------------------------------------------------------------

export default {
  // ---- HTTP requests ----
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/history") {
      const raw = await env.OIL_KV_BINDING.get(HISTORY_KEY);
      if (!raw) return Response.json({ error: "No history yet" }, { status: 404 });
      return Response.json(JSON.parse(raw), {
        headers: { "Cache-Control": "public, max-age=3600" },
      });
    }

    // Default: serve latest snapshot
    const raw = await env.OIL_KV_BINDING.get(LATEST_KEY);
    if (!raw) {
      return Response.json(
        { error: "No data yet — cron hasn't run, or manually POST /refresh" },
        { status: 404 }
      );
    }
    return Response.json(JSON.parse(raw), {
      headers: { "Cache-Control": "public, max-age=3600" },
    });
  },

  // ---- Scheduled cron ----
  async scheduled(event, env, ctx) {
    console.log("Cron fired:", event.cron, "at", new Date().toISOString());
    try {
      const snapshot = await fetchPrices(env.EIA_API_KEY);
      await storeSnapshot(env.OIL_KV_BINDING, snapshot);
      console.log("Stored snapshot:", JSON.stringify(snapshot));
    } catch (err) {
      console.error("Failed to fetch prices:", err);
      throw err; // causes the scheduled invocation to be marked as failed
    }
  },
};
