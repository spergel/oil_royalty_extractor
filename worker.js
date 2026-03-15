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
// Valuation model data (latest parsed filing snapshot)
// ---------------------------------------------------------------------------

const MODEL_BASE_ROWS = [
  { ticker: "SBR", desc: "Sabine Royalty Trust", year: 2025, pv10_m: 301.151, market_cap_m: 271.95, sec_oil: 66.01 },
  { ticker: "PBT", desc: "Permian Basin Royalty Trust", year: 2024, pv10_m: 371.366, market_cap_m: 354.236, sec_oil: 75.48 },
  { ticker: "PVL", desc: "Permianville Royalty Trust", year: 2024, pv10_m: 91.362, market_cap_m: 95.573, sec_oil: 75.48 },
  { ticker: "PRT", desc: "PermRock Royalty Trust", year: 2024, pv10_m: 64.445, market_cap_m: 71.61, sec_oil: 75.48 },
  { ticker: "MVO", desc: "MV Oil Trust", year: 2024, pv10_m: 18.889403, market_cap_m: 23.4, sec_oil: 74.0 },
  { ticker: "VNOM", desc: "Viper Energy Inc", year: 2025, pv10_m: 6647.0, market_cap_m: 14620.0, sec_oil: 64.8 },
  { ticker: "SJT", desc: "San Juan Basin Royalty Trust", year: 2024, pv10_m: 76.89, market_cap_m: 195.762, sec_oil: 66.35 },
  { ticker: "CRT", desc: "Cross Timbers Royalty Trust", year: 2024, pv10_m: 31.33, market_cap_m: 102.0, sec_oil: 75.48 },
  { ticker: "BSM", desc: "Black Stone Minerals LP", year: 2025, pv10_m: 889.199, market_cap_m: 3108.0, sec_oil: 75.0 },
  { ticker: "DMLP", desc: "Dorchester Minerals LP", year: 2025, pv10_m: 277.82, market_cap_m: 1323.0, sec_oil: 61.19 },
  { ticker: "PHX", desc: "PHX Minerals / Prairie Operating", year: 2024, pv10_m: 76.254888, market_cap_m: 297.0, sec_oil: 75.0 },
  { ticker: "VOC", desc: "VOC Energy Trust", year: 2024, pv10_m: 34.780518, market_cap_m: 197.912, sec_oil: 74.0 },
];

const MODEL_DIAGNOSTICS = {
  SBR: { cost_burden: null, tax_burden: null, discount_drag: null, reserve_replacement: null, price_leverage: 0.0623142543 },
  PBT: { cost_burden: null, tax_burden: null, discount_drag: null, reserve_replacement: null, price_leverage: null },
  PVL: { cost_burden: 0.0806651524, tax_burden: null, discount_drag: 0.4972485748, reserve_replacement: null, price_leverage: null },
  PRT: { cost_burden: null, tax_burden: null, discount_drag: 0.4565455711, reserve_replacement: null, price_leverage: -0.1829156645 },
  MVO: { cost_burden: null, tax_burden: null, discount_drag: 0.0651803670, reserve_replacement: null, price_leverage: -0.2252791155 },
  VNOM: { cost_burden: 0.0702347662, tax_burden: 0.0903297132, discount_drag: 0.4850480322, reserve_replacement: null, price_leverage: -0.0419738228 },
  SJT: { cost_burden: null, tax_burden: null, discount_drag: 0.4169402844, reserve_replacement: null, price_leverage: -0.0644947327 },
  CRT: { cost_burden: 0.0820300922, tax_burden: null, discount_drag: 0.4049608752, reserve_replacement: null, price_leverage: -0.3518672199 },
  BSM: { cost_burden: 0.1325612867, tax_burden: 0.0031469350, discount_drag: null, reserve_replacement: null, price_leverage: 0.0497841316 },
  DMLP: { cost_burden: 0.0634275367, tax_burden: null, discount_drag: 0.4585197096, reserve_replacement: null, price_leverage: null },
  PHX: { cost_burden: 0.2938328417, tax_burden: 0.0386744820, discount_drag: 0.4409794339, reserve_replacement: 3.8162334651, price_leverage: null },
  VOC: { cost_burden: null, tax_burden: null, discount_drag: 0.2435164036, reserve_replacement: null, price_leverage: -0.1646462540 },
};

const FRONTEND_HTML = `<!doctype html>
<html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Oil Trust Model</title>
<style>body{font-family:Arial;margin:0;background:#111;color:#eee}main{max-width:1100px;margin:0 auto;padding:20px}input,select,button{padding:8px;border-radius:8px;border:1px solid #444;background:#1a1a1a;color:#eee}button{background:#2d66f6;border:none}table{width:100%;border-collapse:collapse;margin-top:10px}th,td{padding:8px;border-bottom:1px solid #333;text-align:right}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}.row{display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:10px}.pill{display:inline-block;background:#253046;padding:4px 8px;border-radius:999px;margin-right:6px}</style>
</head><body><main><h2>Oil Royalty Trust Model</h2><div class="row"><div><label>WTI</label><input id="oil" type="number" step="0.01" value="64.51"></div><div><label>HH</label><input id="gas" type="number" step="0.01" value="3.62"></div><div><label>Metric</label><select id="metric"><option value="p_adj_pv10">P/AdjPV10</option><option value="p_pv10">P/PV10</option><option value="market_cap_m">Mkt Cap</option><option value="pv10_m">PV10</option><option value="adj_pv10_m">Adj PV10</option></select></div><div><label>Order</label><select id="order"><option value="asc">asc</option><option value="desc">desc</option></select></div><div><label>Top</label><input id="top" type="number" min="1" max="100" value="12"></div><div><label>&nbsp;</label><button id="run">Run</button></div></div><p id="meta"></p><div id="screened"></div><table><thead><tr><th>Ticker</th><th>Description</th><th>Year</th><th>PV10</th><th>Mkt Cap</th><th>P/PV10</th><th>SEC Oil</th><th>Adj PV10</th><th>P/AdjPV10</th></tr></thead><tbody id="tbody"></tbody></table></main>
<script>
const $=id=>document.getElementById(id);
const n=(v,d=2)=>v==null?'n/a':Number(v).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const x=v=>v==null?'n/a':n(v,2)+'x';
async function run(){const oil=$('oil').value,gas=$('gas').value,metric=$('metric').value,order=$('order').value,top=$('top').value;
const ranked=await fetch('/valuation/ranked?oil='+encodeURIComponent(oil)+'&gas='+encodeURIComponent(gas)+'&metric='+metric+'&order='+order+'&top='+top).then(r=>r.json());
const latest=await fetch('/valuation/latest?oil='+encodeURIComponent(oil)+'&gas='+encodeURIComponent(gas)).then(r=>r.json());
$('meta').textContent='source='+ranked.inputs.price_source+' | WTI $'+n(ranked.inputs.wti_per_bbl,2)+' | HH $'+n(ranked.inputs.hh_per_mmbtu,3)+' | '+ranked.as_of;
$('screened').innerHTML=(latest.screened_ideas||[]).map(r=>'<span class="pill">'+r.ticker+' ('+x(r.p_adj_pv10)+')</span>').join('')||'<span class="pill">none</span>';
const tb=$('tbody');tb.innerHTML='';for(const r of ranked.rows||[]){const tr=document.createElement('tr');tr.innerHTML='<td>'+r.ticker+'</td><td>'+r.desc+'</td><td>'+r.year+'</td><td>'+n(r.pv10_m,1)+'</td><td>'+n(r.market_cap_m,1)+'</td><td>'+x(r.p_pv10)+'</td><td>'+n(r.sec_oil,2)+'</td><td>'+n(r.adj_pv10_m,1)+'</td><td>'+x(r.p_adj_pv10)+'</td>';tb.appendChild(tr);}
}
$('run').addEventListener('click',run);run();
</script></body></html>`;

function fmtNow() {
  return new Date().toISOString();
}

function computeModel(oil, gas, source) {
  const rows = MODEL_BASE_ROWS.map((r) => {
    const p_pv10 = r.market_cap_m / r.pv10_m;
    const adj_pv10_m = r.sec_oil > 0 ? r.pv10_m * (oil / r.sec_oil) : r.pv10_m;
    const p_adj_pv10 = r.market_cap_m / adj_pv10_m;
    return { ...r, p_pv10, adj_pv10_m, p_adj_pv10 };
  }).sort((a, b) => a.p_adj_pv10 - b.p_adj_pv10);

  const diagnostics = rows.map((r) => ({
    ticker: r.ticker,
    year: r.year,
    p_adj_pv10: r.p_adj_pv10,
    ...MODEL_DIAGNOSTICS[r.ticker],
  }));

  const pass = (v, t) => v == null || v <= t;
  const screened_ideas = diagnostics.filter((d) =>
    d.p_adj_pv10 <= 1.6 &&
    pass(d.cost_burden, 0.65) &&
    pass(d.tax_burden, 0.20) &&
    pass(d.discount_drag, 0.55)
  );

  return {
    as_of: fmtNow(),
    inputs: {
      wti_per_bbl: oil,
      hh_per_mmbtu: gas,
      price_source: source,
    },
    rows,
    diagnostics,
    screened_ideas,
    no_standardized_measure_data: ["TPL"],
  };
}

function sortRowsByMetric(rows, metric, order) {
  const dir = order === "desc" ? -1 : 1;
  return [...rows].sort((a, b) => {
    const av = a?.[metric];
    const bv = b?.[metric];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    return av < bv ? -1 * dir : av > bv ? 1 * dir : 0;
  });
}

async function resolveModelInputs(url, env) {
  const qOil = Number(url.searchParams.get("oil"));
  const qGas = Number(url.searchParams.get("gas"));
  if (Number.isFinite(qOil) && Number.isFinite(qGas)) {
    return { oil: qOil, gas: qGas, source: "manual" };
  }
  try {
    const raw = await env.OIL_KV_BINDING.get(LATEST_KEY);
    if (raw) {
      const p = JSON.parse(raw);
      if (Number.isFinite(p?.wti_per_bbl) && Number.isFinite(p?.hh_per_mmbtu)) {
        return { oil: Number(p.wti_per_bbl), gas: Number(p.hh_per_mmbtu), source: "kv_latest" };
      }
    }
  } catch (_) {}
  return { oil: 72.0, gas: 2.5, source: "defaults" };
}

// ---------------------------------------------------------------------------
// HTTP handler + cron
// ---------------------------------------------------------------------------

export default {
  // ---- HTTP requests ----
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/app") {
      return new Response(FRONTEND_HTML, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    if (url.pathname === "/valuation/latest") {
      const { oil, gas, source } = await resolveModelInputs(url, env);
      return Response.json(computeModel(oil, gas, source), {
        headers: { "Cache-Control": "no-store" },
      });
    }

    if (url.pathname === "/valuation/ranked") {
      const metric = url.searchParams.get("metric") || "p_adj_pv10";
      const order = url.searchParams.get("order") || "asc";
      const top = Math.max(1, Math.min(100, Number(url.searchParams.get("top") || 10)));
      const { oil, gas, source } = await resolveModelInputs(url, env);
      const report = computeModel(oil, gas, source);
      const rows = sortRowsByMetric(report.rows, metric, order).slice(0, top);
      return Response.json({
        as_of: report.as_of,
        inputs: report.inputs,
        rank: { metric, order, top },
        rows,
      }, { headers: { "Cache-Control": "no-store" } });
    }

    if (url.pathname.startsWith("/valuation/") && url.pathname.split("/").length === 3) {
      const ticker = url.pathname.split("/")[2].toUpperCase();
      if (ticker === "latest" || ticker === "ranked") {
        return Response.json({ error: "Not found" }, { status: 404 });
      }
      const { oil, gas, source } = await resolveModelInputs(url, env);
      const report = computeModel(oil, gas, source);
      const row = report.rows.find((r) => r.ticker === ticker);
      if (!row) return Response.json({ error: `Ticker not found: ${ticker}` }, { status: 404 });
      const diag = report.diagnostics.find((d) => d.ticker === ticker) || null;
      return Response.json({
        as_of: report.as_of,
        inputs: report.inputs,
        ticker,
        valuation: row,
        diagnostics: diag,
      }, { headers: { "Cache-Control": "no-store" } });
    }

    if (url.pathname === "/history" || url.pathname === "/prices/history") {
      const raw = await env.OIL_KV_BINDING.get(HISTORY_KEY);
      if (!raw) return Response.json({ error: "No history yet" }, { status: 404 });
      return Response.json(JSON.parse(raw), {
        headers: { "Cache-Control": "public, max-age=3600" },
      });
    }

    if (url.pathname !== "/" && url.pathname !== "/prices/latest") {
      return Response.json({ error: "Not found" }, { status: 404 });
    }

    // Default prices endpoint
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
