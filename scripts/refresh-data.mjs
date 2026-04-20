// scripts/refresh-data.mjs
// Local-only refresh — pulls Redfin + Census and writes data/dataset.json.
// In production, Vercel Cron calls /api/refresh which commits to GitHub instead.

import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { promisify } from "node:util";

const gunzip = promisify(zlib.gunzip);

const SOURCES = {
  state: "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/state_market_tracker.tsv000.gz",
  county: "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz",
  zip: "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz"
};

const WINDOWS = [
  { key: "30d", days: 30 },
  { key: "90d", days: 90 },
  { key: "180d", days: 180 },
  { key: "1y", days: 365 }
];

async function fetchTsv(url) {
  console.log(`fetch ${url}`);
  const res = await fetch(url, {
    headers: { "User-Agent": "silverstone-dashboard/1.0" }
  });
  if (!res.ok) throw new Error(`${url} → ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const unzipped = await gunzip(buf);
  return unzipped.toString("utf-8");
}

function parseTsv(text) {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const header = lines[0].split("\t");
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = lines[i].split("\t");
    const obj = {};
    for (let c = 0; c < header.length; c++) obj[header[c]] = cells[c];
    rows.push(obj);
  }
  return rows;
}

function toNum(v) {
  if (v == null || v === "" || v === "NA") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function round(n, d) {
  const m = Math.pow(10, d);
  return Math.round(n * m) / m;
}

function cleanRegionName(name, geoType) {
  if (!name) return "";
  if (geoType === "zip") return name.replace(/^Zip Code:\s*/i, "").trim();
  return name.trim();
}

function aggregateForGeo(rows, geoType) {
  const byGeo = new Map();
  for (const r of rows) {
    const dur = toNum(r.period_duration);
    if (dur !== 7) continue;
    const key = geoType === "zip" ? r.region : `${r.region}|${r.state_code || r.state}`;
    if (!key) continue;
    if (!byGeo.has(key)) byGeo.set(key, []);
    byGeo.get(key).push(r);
  }
  const today = new Date();
  const results = [];
  for (const [key, weeks] of byGeo) {
    weeks.sort((a, b) => new Date(b.period_end).getTime() - new Date(a.period_end).getTime());
    const newest = weeks[0];
    if (!newest) continue;
    const asOf = newest.period_end;
    for (const w of WINDOWS) {
      const cutoff = new Date(today);
      cutoff.setDate(cutoff.getDate() - w.days);
      const slice = weeks.filter(x => new Date(x.period_end) >= cutoff);
      if (slice.length === 0) continue;
      let homesSold = 0, pendingSum = 0, soldForPending = 0;
      let domSum = 0, domN = 0, domSub60 = 0;
      let priceVolumeWeightedSum = 0, priceWeightTotal = 0;
      for (const x of slice) {
        const sold = toNum(x.homes_sold);
        const pend = toNum(x.pending_sales);
        const dom = toNum(x.median_days_on_market);
        const price = toNum(x.median_sale_price);
        if (sold != null) homesSold += sold;
        if (pend != null && sold != null && sold > 0) { pendingSum += pend; soldForPending += sold; }
        if (dom != null) { domSum += dom; domN++; if (dom < 60) domSub60++; }
        if (price != null && sold != null && sold > 0) {
          priceVolumeWeightedSum += price * sold;
          priceWeightTotal += sold;
        }
      }
      const pending_pct = soldForPending > 0 ? (pendingSum / soldForPending) * 100 : 0;
      const median_dom = domN > 0 ? domSum / domN : 0;
      const dom_sub60_share = domN > 0 ? domSub60 / domN : 0;
      const median_sale_price = priceWeightTotal > 0 ? priceVolumeWeightedSum / priceWeightTotal : null;
      const name = newest.region || "";
      const stateCode = newest.state_code || newest.state || "";
      const geoId = geoType === "zip" ? name.replace(/[^\d]/g, "") : key;
      results.push({
        geo_type: geoType,
        geo_id: geoId,
        name: cleanRegionName(name, geoType),
        state: stateCode,
        population: null,
        median_sale_price,
        pending_pct: round(pending_pct, 2),
        median_dom: round(median_dom, 1),
        dom_sub60_share: round(dom_sub60_share, 3),
        homes_sold: Math.round(homesSold),
        window: w.key,
        as_of: asOf
      });
    }
  }
  return results;
}

const STATE_NAMES = new Map([
  ["AL","Alabama"],["AK","Alaska"],["AZ","Arizona"],["AR","Arkansas"],["CA","California"],
  ["CO","Colorado"],["CT","Connecticut"],["DE","Delaware"],["FL","Florida"],["GA","Georgia"],
  ["HI","Hawaii"],["ID","Idaho"],["IL","Illinois"],["IN","Indiana"],["IA","Iowa"],
  ["KS","Kansas"],["KY","Kentucky"],["LA","Louisiana"],["ME","Maine"],["MD","Maryland"],
  ["MA","Massachusetts"],["MI","Michigan"],["MN","Minnesota"],["MS","Mississippi"],["MO","Missouri"],
  ["MT","Montana"],["NE","Nebraska"],["NV","Nevada"],["NH","New Hampshire"],["NJ","New Jersey"],
  ["NM","New Mexico"],["NY","New York"],["NC","North Carolina"],["ND","North Dakota"],["OH","Ohio"],
  ["OK","Oklahoma"],["OR","Oregon"],["PA","Pennsylvania"],["RI","Rhode Island"],["SC","South Carolina"],
  ["SD","South Dakota"],["TN","Tennessee"],["TX","Texas"],["UT","Utah"],["VT","Vermont"],
  ["VA","Virginia"],["WA","Washington"],["WV","West Virginia"],["WI","Wisconsin"],["WY","Wyoming"],
  ["DC","District of Columbia"]
]);

async function enrich(rows) {
  try {
    const sRes = await fetch("https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=state:*");
    if (sRes.ok) {
      const arr = await sRes.json();
      const m = new Map(arr.slice(1).map(x => [x[0].toUpperCase(), Number(x[1])]));
      for (const r of rows) if (r.geo_type === "state") { const p = m.get(r.name.toUpperCase()); if (p != null) r.population = p; }
    }
    const cRes = await fetch("https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=county:*");
    if (cRes.ok) {
      const arr = await cRes.json();
      const m = new Map(arr.slice(1).map(x => [x[0].toLowerCase(), Number(x[1])]));
      for (const r of rows) {
        if (r.geo_type !== "county") continue;
        const sn = STATE_NAMES.get(r.state.toUpperCase());
        if (!sn) continue;
        const p = m.get(`${r.name}, ${sn}`.toLowerCase());
        if (p != null) r.population = p;
      }
    }
  } catch (e) { console.warn("census enrichment failed:", e.message); }
}

async function main() {
  const all = [];
  for (const [geo, url] of Object.entries(SOURCES)) {
    const tsv = await fetchTsv(url);
    const parsed = parseTsv(tsv);
    const agg = aggregateForGeo(parsed, geo);
    console.log(`${geo}: ${agg.length} aggregated rows`);
    all.push(...agg);
  }
  await enrich(all);
  const out = { generated_at: new Date().toISOString(), source: "Redfin Data Center + US Census ACS 5yr", rows: all };
  const p = path.join(process.cwd(), "data", "dataset.json");
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(out));
  console.log(`wrote ${all.length} rows → ${p}`);
}

main().catch(e => { console.error(e); process.exit(1); });
