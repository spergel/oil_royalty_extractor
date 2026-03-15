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

const LANDING_HTML = `<!doctype html>
<html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Oil Royalty Extractor</title>
<style>body{font-family:Arial,Helvetica,sans-serif;background:#0f1115;color:#e8eaf0;margin:0}main{max-width:900px;margin:0 auto;padding:24px}.card{background:#171a21;border:1px solid #2b3140;border-radius:10px;padding:14px;margin:10px 0}a{color:#84a8ff;text-decoration:none}.muted{color:#b8becc;font-size:.95rem}button{padding:8px 12px;border-radius:8px;border:none;background:#2d66f6;color:white;cursor:pointer}</style>
</head><body><main><h2>Oil Royalty Extractor Worker</h2>
<p class="muted">Use the links below to view prices, run an immediate refresh, or open the valuation model UI.</p>
<div class="card"><strong>Valuation App</strong><div><a href="/app">/app</a></div></div>
<div class="card"><strong>Latest Prices (JSON)</strong><div><a href="/prices/latest">/prices/latest</a></div></div>
<div class="card"><strong>Price History (JSON)</strong><div><a href="/prices/history">/prices/history</a></div></div>
<div class="card"><strong>Manual Refresh</strong><div class="muted">POST /refresh fetches latest EIA prices and stores snapshot in KV.</div><button id="refreshBtn">Run refresh</button><pre id="out"></pre></div>
<script>document.getElementById('refreshBtn').addEventListener('click',async()=>{const o=document.getElementById('out');o.textContent='Running...';try{const r=await fetch('/refresh',{method:'POST'});o.textContent=await r.text();}catch(e){o.textContent=String(e);}});</script>
</main></body></html>`;

const FRONTEND_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Oil Royalty Trust Screener</title>
<style>
:root{--bg:#0d1117;--bg2:#161b22;--bg3:#21262d;--bd:#30363d;--tx:#e6edf3;--mu:#8b949e;--ac:#388bfd;--gn:#3fb950;--ye:#d29922;--or:#db6d28;--rd:#f85149}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--tx);font-size:14px;line-height:1.5}
.wrap{max-width:1200px;margin:0 auto;padding:16px 20px}
/* header */
header{display:flex;align-items:center;gap:14px;margin-bottom:18px;flex-wrap:wrap}
h1{font-size:1.2rem;font-weight:700;white-space:nowrap}
.price-bar{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:6px 12px;font-size:12px;color:var(--mu)}
.price-bar strong{color:var(--tx)}
/* controls */
.controls{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:12px 16px;margin-bottom:22px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.inputs{display:flex;gap:10px;align-items:flex-end}
.inputs label{font-size:11px;color:var(--mu);display:flex;flex-direction:column;gap:3px}
.inputs input{width:88px;padding:5px 8px;background:var(--bg3);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:13px}
.gobtn{padding:6px 16px;background:var(--ac);border:none;border-radius:6px;color:#fff;cursor:pointer;font-size:13px;font-weight:600;align-self:flex-end}
.gobtn:hover{opacity:.85}
/* section */
.sh{display:flex;align-items:baseline;gap:10px;margin-bottom:10px}
.sh h2{font-size:.95rem;font-weight:600}
.sh small{font-size:11px;color:var(--mu)}
/* cards */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px;margin-bottom:26px}
.card{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:14px;cursor:pointer;transition:border-color .15s}
.card:hover{border-color:var(--ac)}
.ct{font-size:1.05rem;font-weight:700}
.cd{font-size:11px;color:var(--mu);margin-top:2px}
.cv{margin-top:10px}
.cvl{font-size:11px;color:var(--mu)}
.cval{font-size:1.5rem;font-weight:700}
.none{color:var(--mu);font-size:13px;padding:8px 0}
/* table */
.tw{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{padding:8px 10px;text-align:right;font-size:11px;color:var(--mu);border-bottom:1px solid var(--bd);cursor:pointer;white-space:nowrap;user-select:none}
th:first-child,th:nth-child(2){text-align:left}
th:hover{color:var(--tx)}
th.on{color:var(--ac)}
td{padding:9px 10px;text-align:right;border-bottom:1px solid var(--bd);font-size:13px}
td:first-child,td:nth-child(2){text-align:left}
tr:hover td{background:var(--bg2)}
tr{cursor:pointer}
/* color scale */
.ga{color:#3fb950;font-weight:700}.gb{color:#56d364;font-weight:700}
.gc{color:#d29922;font-weight:600}.gd{color:#db6d28}.gf{color:#f85149}
/* panel */
.ov{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:100}
.ov.open{display:block}
.panel{position:fixed;right:0;top:0;bottom:0;width:min(460px,100vw);background:var(--bg2);border-left:1px solid var(--bd);overflow-y:auto;z-index:101;transform:translateX(100%);transition:transform .22s ease}
.panel.open{transform:none}
.ph{padding:18px 20px;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;align-items:flex-start}
.ptk{font-size:1.5rem;font-weight:800}
.pds{font-size:12px;color:var(--mu);margin-top:2px}
.xbtn{background:none;border:none;color:var(--mu);cursor:pointer;font-size:1.3rem;padding:2px 4px}
.pb{padding:18px 20px}
.mg{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:18px}
.mb{background:var(--bg3);border-radius:8px;padding:11px}
.mbl{font-size:11px;color:var(--mu);margin-bottom:3px}
.mbv{font-size:1.2rem;font-weight:700}
.st{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--mu);margin:16px 0 8px}
.cl{display:flex;flex-direction:column;gap:6px}
.ci{display:flex;align-items:center;gap:7px;font-size:13px}
.ck{font-size:15px}
.ck.ok{color:var(--gn)}.ck.no{color:var(--rd)}.ck.na{color:var(--mu)}
.sg{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.sb{background:var(--bg3);border-radius:6px;padding:9px;text-align:center}
.sbl{font-size:11px;color:var(--mu)}
.sbv{font-size:1.05rem;font-weight:700;margin-top:3px}
.dl{border-bottom:1px solid var(--bd);padding:7px 0;display:flex;justify-content:space-between;font-size:13px}
.dl span:first-child{color:var(--mu)}
.tag{display:inline-block;font-size:11px;padding:2px 8px;border-radius:4px;background:var(--bg3);color:var(--mu);margin-right:4px}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Oil Royalty Trust Screener</h1>
    <div class="price-bar" id="pbar">Fetching prices&hellip;</div>
  </header>

  <div class="controls">
    <div class="inputs">
      <label>WTI $/bbl<input id="oil" type="number" step="0.5" value="70"></label>
      <label>HH $/MMBtu<input id="gas" type="number" step="0.05" value="2.50"></label>
    </div>
    <button class="gobtn" id="run">Update</button>
  </div>

  <div class="sh">
    <h2>Passing Screen</h2>
    <small>P/AdjPV10 &le; 1.6 &bull; cost burden &le; 65% &bull; tax burden &le; 20% &bull; discount drag &le; 55%</small>
  </div>
  <div class="cards" id="cards"><div class="none">Loading&hellip;</div></div>

  <div class="sh" style="margin-top:4px"><h2>All Trusts</h2><small>Click a row for details &bull; click column headers to sort</small></div>
  <div class="tw">
    <table>
      <thead><tr>
        <th data-c="ticker">Ticker</th>
        <th data-c="desc">Name</th>
        <th data-c="year">Year</th>
        <th data-c="pv10_m">PV10 ($M)</th>
        <th data-c="market_cap_m">Mkt Cap ($M)</th>
        <th data-c="p_pv10">P/PV10</th>
        <th data-c="sec_oil">SEC Oil</th>
        <th data-c="adj_pv10_m">Adj PV10 ($M)</th>
        <th data-c="p_adj_pv10">P/AdjPV10</th>
      </tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<!-- detail panel -->
<div class="ov" id="ov"></div>
<div class="panel" id="panel">
  <div class="ph">
    <div>
      <div class="ptk" id="ptk"></div>
      <div class="pds" id="pds"></div>
    </div>
    <button class="xbtn" id="xbtn">&times;</button>
  </div>
  <div class="pb">
    <div class="mg">
      <div class="mb"><div class="mbl">P / AdjPV10</div><div class="mbv" id="p-padj">--</div></div>
      <div class="mb"><div class="mbl">P / PV10</div><div class="mbv" id="p-ppv10">--</div></div>
      <div class="mb"><div class="mbl">PV10</div><div class="mbv" id="p-pv10">--</div></div>
      <div class="mb"><div class="mbl">Market Cap</div><div class="mbv" id="p-mc">--</div></div>
    </div>
    <div class="st">Screen Criteria</div>
    <div class="cl" id="p-crit"></div>
    <div class="st">Price Sensitivity &mdash; P/AdjPV10</div>
    <div class="sg" id="p-sens"></div>
    <div class="st">Diagnostics</div>
    <div id="p-diag"></div>
    <div class="st">Filing</div>
    <div><span class="tag" id="p-yr"></span><span class="tag" id="p-so"></span></div>
  </div>
</div>

<script>
const f=(v,d=1)=>v==null?'n/a':Number(v).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const fx=v=>v==null?'n/a':f(v,2)+'x';
const fp=v=>v==null?'n/a':(v*100).toFixed(1)+'%';
const el=id=>document.getElementById(id);

function gc(v){
  if(v==null)return'';
  if(v<0.8)return'ga';if(v<1.2)return'gb';if(v<1.6)return'gc';if(v<2.0)return'gd';return'gf';
}

let _d=null,_sc='p_adj_pv10',_sd=1;

async function load(){
  const oil=parseFloat(el('oil').value)||70;
  const gas=parseFloat(el('gas').value)||2.5;
  try{
    const res=await fetch('/valuation/latest?oil='+encodeURIComponent(oil)+'&gas='+encodeURIComponent(gas));
    _d=await res.json();
    render();
  }catch(e){console.error(e);}
}

function render(){
  if(!_d)return;
  const{rows,diagnostics,screened_ideas,inputs,as_of}=_d;

  // price bar
  const src=inputs.price_source==='kv_latest'?'live (KV)':inputs.price_source;
  el('pbar').innerHTML='WTI <strong>$'+f(inputs.wti_per_bbl,2)+'</strong> &nbsp;|&nbsp; HH <strong>$'+f(inputs.hh_per_mmbtu,3)+'</strong> &nbsp;|&nbsp; '+src+' &nbsp;|&nbsp; '+new Date(as_of).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});

  // build diag map
  const dm={};(diagnostics||[]).forEach(d=>dm[d.ticker]=d);

  // screened cards
  const ce=el('cards');
  if(!screened_ideas||screened_ideas.length===0){
    ce.innerHTML='<div class="none">No trusts pass all screen criteria at current prices.</div>';
  }else{
    ce.innerHTML=screened_ideas.map(s=>{
      const r=rows.find(x=>x.ticker===s.ticker)||{};
      return '<div class="card" data-ticker="'+s.ticker+'">'+
        '<div class="ct">'+s.ticker+'</div>'+
        '<div class="cd">'+r.desc+'</div>'+
        '<div class="cv"><div class="cvl">P / Adj PV10</div>'+
        '<div class="cval '+gc(s.p_adj_pv10)+'">'+fx(s.p_adj_pv10)+'</div></div>'+
        '</div>';
    }).join('');
  }

  // sort table rows
  const sorted=[...rows].sort((a,b)=>{
    const av=a[_sc],bv=b[_sc];
    if(av==null&&bv==null)return 0;
    if(av==null)return 1;if(bv==null)return -1;
    return av<bv?-_sd:av>bv?_sd:0;
  });

  el('tbody').innerHTML=sorted.map(r=>'<tr data-ticker="'+r.ticker+'">'+
    '<td><strong>'+r.ticker+'</strong></td>'+
    '<td style="color:var(--mu);font-size:12px">'+r.desc+'</td>'+
    '<td style="color:var(--mu)">'+r.year+'</td>'+
    '<td>$'+f(r.pv10_m)+'M</td>'+
    '<td>$'+f(r.market_cap_m)+'M</td>'+
    '<td class="'+gc(r.p_pv10)+'">'+fx(r.p_pv10)+'</td>'+
    '<td style="color:var(--mu)">$'+f(r.sec_oil,2)+'</td>'+
    '<td>$'+f(r.adj_pv10_m)+'M</td>'+
    '<td class="'+gc(r.p_adj_pv10)+'" style="font-size:15px">'+fx(r.p_adj_pv10)+'</td>'+
  '</tr>').join('');

  document.querySelectorAll('th[data-c]').forEach(th=>th.classList.toggle('on',th.dataset.c===_sc));
}

function detail(ticker){
  if(!_d)return;
  const row=_d.rows.find(r=>r.ticker===ticker)||{};
  const dm={};(_d.diagnostics||[]).forEach(d=>dm[d.ticker]=d);
  const diag=dm[ticker]||{};

  el('ptk').textContent=ticker;
  el('pds').textContent=row.desc||'';
  el('p-padj').innerHTML='<span class="'+gc(row.p_adj_pv10)+'">'+fx(row.p_adj_pv10)+'</span>';
  el('p-ppv10').innerHTML='<span class="'+gc(row.p_pv10)+'">'+fx(row.p_pv10)+'</span>';
  el('p-pv10').textContent=row.pv10_m!=null?'$'+f(row.pv10_m)+'M':'--';
  el('p-mc').textContent=row.market_cap_m!=null?'$'+f(row.market_cap_m)+'M':'--';
  el('p-yr').textContent='Filing year: '+(row.year||'--');
  el('p-so').textContent='SEC oil: $'+f(row.sec_oil,2)+'/bbl';

  // criteria
  const pass=(v,t)=>v==null?null:v<=t;
  const crit=[
    {lbl:'P/AdjPV10 \u2264 1.6',     v:pass(diag.p_adj_pv10,1.6)},
    {lbl:'Cost burden \u2264 65%',    v:pass(diag.cost_burden,0.65)},
    {lbl:'Tax burden \u2264 20%',     v:pass(diag.tax_burden,0.20)},
    {lbl:'Discount drag \u2264 55%',  v:pass(diag.discount_drag,0.55)},
  ];
  el('p-crit').innerHTML=crit.map(c=>{
    const icon=c.v===null?'\u25CB':c.v?'\u2713':'\u2717';
    const cls=c.v===null?'na':c.v?'ok':'no';
    const note=c.v===null?'<span style="color:var(--mu);font-size:11px"> (no data)</span>':'';
    return '<div class="ci"><span class="ck '+cls+'">'+icon+'</span>'+c.lbl+note+'</div>';
  }).join('');

  // sensitivity
  const sens=[{lbl:'Bear $55',oil:55},{lbl:'Base $70',oil:70},{lbl:'Bull $85',oil:85}];
  if(row.pv10_m&&row.sec_oil>0&&row.market_cap_m){
    el('p-sens').innerHTML=sens.map(s=>{
      const adj=row.pv10_m*(s.oil/row.sec_oil);
      const p=row.market_cap_m/adj;
      return '<div class="sb"><div class="sbl">'+s.lbl+'</div><div class="sbv '+gc(p)+'">'+fx(p)+'</div></div>';
    }).join('');
  }else{
    el('p-sens').innerHTML='<div class="none">SEC oil price unavailable</div>';
  }

  // diagnostics
  const dfs=[
    {k:'cost_burden',        l:'Cost Burden',        f:fp},
    {k:'tax_burden',         l:'Tax Burden',         f:fp},
    {k:'discount_drag',      l:'Discount Drag',      f:fp},
    {k:'reserve_replacement',l:'Reserve Replacement',f:v=>f(v,2)+'x'},
    {k:'price_leverage',     l:'Price Leverage',     f:v=>f(v,4)},
  ];
  el('p-diag').innerHTML=dfs.map(df=>{
    const v=diag[df.k];
    return '<div class="dl"><span>'+df.l+'</span><span>'+(v==null?'<span style="color:var(--mu)">n/a</span>':df.f(v))+'</span></div>';
  }).join('');

  el('ov').classList.add('open');el('panel').classList.add('open');
}

function closePanel(){el('ov').classList.remove('open');el('panel').classList.remove('open');}
el('xbtn').addEventListener('click',closePanel);
el('ov').addEventListener('click',closePanel);

// card + row clicks via delegation
document.addEventListener('click',e=>{
  const t=e.target.closest('[data-ticker]');
  if(t)detail(t.dataset.ticker);
});

// sort
document.querySelectorAll('th[data-c]').forEach(th=>{
  th.addEventListener('click',()=>{
    if(_sc===th.dataset.c)_sd*=-1;else{_sc=th.dataset.c;_sd=1;}
    render();
  });
});


el('run').addEventListener('click',load);

// init: try to pull live prices from KV to pre-fill inputs
fetch('/prices/latest').then(r=>r.ok?r.json():null).then(p=>{
  if(p?.wti_per_bbl){el('oil').value=p.wti_per_bbl.toFixed(2);el('gas').value=p.hh_per_mmbtu.toFixed(2);}
}).catch(()=>{}).finally(()=>load());
</script>
</body>
</html>`;

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

    if (url.pathname === "/") {
      return Response.redirect(new URL("/app", request.url).toString(), 302);
    }

    if (url.pathname === "/app") {
      return new Response(FRONTEND_HTML, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    if (url.pathname === "/refresh") {
      if (request.method !== "POST") {
        return Response.json({ error: "Method not allowed. Use POST /refresh" }, { status: 405 });
      }
      try {
        const snapshot = await fetchPrices(env.EIA_API_KEY);
        await storeSnapshot(env.OIL_KV_BINDING, snapshot);
        return Response.json({ ok: true, message: "Refreshed", snapshot }, {
          headers: { "Cache-Control": "no-store" },
        });
      } catch (err) {
        return Response.json({ ok: false, error: String(err?.message || err) }, { status: 500 });
      }
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

    if (url.pathname !== "/prices/latest") {
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
