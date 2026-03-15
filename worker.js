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
const EIA_V1_BASE = "https://api.eia.gov/series";

/**
 * Fetch WTI spot price ($/bbl) — most recent monthly value.
 * Tries EIA v2 first, falls back to v1.
 */
async function fetchWTI(apiKey) {
  // v2 attempt
  try {
    const url =
      `${EIA_V2_BASE}/petroleum/pri/spt/data/` +
      `?api_key=${apiKey}` +
      `&frequency=monthly` +
      `&data[0]=value` +
      `&facets[series][]=RWTC` +
      `&sort[0][column]=period&sort[0][direction]=desc` +
      `&length=1`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`EIA v2 WTI HTTP ${res.status}`);
    const json = await res.json();
    const row = json?.response?.data?.[0];
    if (row?.value != null) {
      return { value: parseFloat(row.value), period: row.period, source: "eia-v2" };
    }
    throw new Error("EIA v2 WTI: no data in response");
  } catch (err) {
    console.warn("WTI v2 failed:", err.message, "— trying v1");
  }

  // v1 fallback
  const url = `${EIA_V1_BASE}/?api_key=${apiKey}&series_id=PET.RWTC.M&num=1`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`EIA v1 WTI HTTP ${res.status}`);
  const json = await res.json();
  const dataPoint = json?.series?.[0]?.data?.[0]; // ["2024-12", 69.97]
  if (!dataPoint) throw new Error("EIA v1 WTI: no data");
  return { value: parseFloat(dataPoint[1]), period: dataPoint[0], source: "eia-v1" };
}

/**
 * Fetch Henry Hub natural gas spot price ($/MMBtu) — most recent monthly value.
 * Tries EIA v2 first, falls back to v1.
 */
async function fetchHenryHub(apiKey) {
  // v2 attempt
  try {
    const url =
      `${EIA_V2_BASE}/natural-gas/pri/sum/data/` +
      `?api_key=${apiKey}` +
      `&frequency=monthly` +
      `&data[0]=value` +
      `&facets[series][]=RNGWHHD` +
      `&sort[0][column]=period&sort[0][direction]=desc` +
      `&length=1`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`EIA v2 HH HTTP ${res.status}`);
    const json = await res.json();
    const row = json?.response?.data?.[0];
    if (row?.value != null) {
      return { value: parseFloat(row.value), period: row.period, source: "eia-v2" };
    }
    throw new Error("EIA v2 HH: no data in response");
  } catch (err) {
    console.warn("HH v2 failed:", err.message, "— trying v1");
  }

  // v1 fallback
  const url = `${EIA_V1_BASE}/?api_key=${apiKey}&series_id=NG.RNGWHHD.M&num=1`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`EIA v1 HH HTTP ${res.status}`);
  const json = await res.json();
  const dataPoint = json?.series?.[0]?.data?.[0];
  if (!dataPoint) throw new Error("EIA v1 HH: no data");
  return { value: parseFloat(dataPoint[1]), period: dataPoint[0], source: "eia-v1" };
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
      const raw = await env.PRICES.get(HISTORY_KEY);
      if (!raw) return Response.json({ error: "No history yet" }, { status: 404 });
      return Response.json(JSON.parse(raw), {
        headers: { "Cache-Control": "public, max-age=3600" },
      });
    }

    // Default: serve latest snapshot
    const raw = await env.PRICES.get(LATEST_KEY);
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
      await storeSnapshot(env.PRICES, snapshot);
      console.log("Stored snapshot:", JSON.stringify(snapshot));
    } catch (err) {
      console.error("Failed to fetch prices:", err);
      throw err; // causes the scheduled invocation to be marked as failed
    }
  },
};
