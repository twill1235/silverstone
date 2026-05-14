// lib/refresh.ts
// Pulls Redfin Data Center monthly CSVs (state/county/zip) from the NEW
// Data Center bucket (relaunched 2026-05-12) and aggregates the THREE
// most-recent monthly snapshots per geo into stable 90-day-equivalent
// trends. Output fields are AVERAGED across those three snapshots.
//
// Format changes vs the legacy redfin_market_tracker TSVs:
//   - URLs:    redfin_data_center/housing_market/monthly/all_*.csv
//   - Format:  plain CSV (uncompressed), uppercase quoted headers
//   - Sort:    descending by PERIOD BEGIN (newest rows first)
//   - Schema:  FREQUENCY (string) replaces period_duration (number);
//              PROPERTY_TYPE is gone (every row is residential aggregate);
//              REGION_TYPE_ID / TABLE_ID / IS_SEASONALLY_ADJUSTED dropped.
//   - State:   only "REGION NAME" - no STATE_CODE column (derive from name).
//   - County:  "REGION NAME" = "Bergen County, NJ" - state code in last 2 chars.
//   - Zip:     "REGION NAME" = "07002"; "METRO" = "New York, NY metro area";
//              FREQUENCY = "Rolling 3 Months" (trailing 90-day window).
//
// Because the zip CSV is ~447 MB and sorted newest-first, we stream the
// HTTP body line by line and bail out once we've seen enough distinct
// PERIOD BEGIN values to cover every geo's 3 retained samples.

import { Readable } from "node:stream";
import readline from "node:readline";
import type { ReadableStream as NodeWebReadableStream } from "node:stream/web";
import type { MarketRow, TimeWindow } from "./scoring";
import type { Dataset } from "./data";

const S3_BASE =
  "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_data_center";

export const SOURCES: Record<"state" | "county" | "zip", string> = {
  state:  `${S3_BASE}/housing_market/monthly/all_states.csv`,
  county: `${S3_BASE}/housing_market/monthly/all_counties.csv`,
  zip:    `${S3_BASE}/housing_market/monthly/all_zips.csv`
};

const WINDOW_KEYS: TimeWindow[] = ["30d", "90d", "180d", "1y"];

// Streaming readline lets us process the 447 MB zip file without buffering
// it into a single string (V8's ~512 MB max-string-length would error). The
// file is sorted by (REGION, PERIOD BEGIN DESC), so we let the snapshot
// dedupe + spacing logic naturally cap each geo at 3 samples.

const STATE_NAME_TO_CODE: Map<string, string> = buildStateNameToCode();

function buildStateNameToCode(): Map<string, string> {
  const m = new Map<string, string>();
  const pairs: [string, string][] = [
    ["AL", "Alabama"], ["AK", "Alaska"], ["AZ", "Arizona"], ["AR", "Arkansas"],
    ["CA", "California"], ["CO", "Colorado"], ["CT", "Connecticut"], ["DE", "Delaware"],
    ["FL", "Florida"], ["GA", "Georgia"], ["HI", "Hawaii"], ["ID", "Idaho"],
    ["IL", "Illinois"], ["IN", "Indiana"], ["IA", "Iowa"], ["KS", "Kansas"],
    ["KY", "Kentucky"], ["LA", "Louisiana"], ["ME", "Maine"], ["MD", "Maryland"],
    ["MA", "Massachusetts"], ["MI", "Michigan"], ["MN", "Minnesota"], ["MS", "Mississippi"],
    ["MO", "Missouri"], ["MT", "Montana"], ["NE", "Nebraska"], ["NV", "Nevada"],
    ["NH", "New Hampshire"], ["NJ", "New Jersey"], ["NM", "New Mexico"], ["NY", "New York"],
    ["NC", "North Carolina"], ["ND", "North Dakota"], ["OH", "Ohio"], ["OK", "Oklahoma"],
    ["OR", "Oregon"], ["PA", "Pennsylvania"], ["RI", "Rhode Island"], ["SC", "South Carolina"],
    ["SD", "South Dakota"], ["TN", "Tennessee"], ["TX", "Texas"], ["UT", "Utah"],
    ["VT", "Vermont"], ["VA", "Virginia"], ["WA", "Washington"], ["WV", "West Virginia"],
    ["WI", "Wisconsin"], ["WY", "Wyoming"], ["DC", "District of Columbia"],
    ["PR", "Puerto Rico"]
  ];
  for (const [code, name] of pairs) m.set(name.toLowerCase(), code);
  return m;
}

function buildStateCodeToName(): Map<string, string> {
  const m = new Map<string, string>();
  for (const [name, code] of STATE_NAME_TO_CODE) m.set(code, name);
  return m;
}

function toNum(v: string | undefined): number | null {
  if (v == null) return null;
  const trimmed = v.replace(/^"|"$/g, "");
  if (trimmed === "" || trimmed === "NA") return null;
  const n = Number(trimmed);
  return Number.isFinite(n) ? n : null;
}

function stripQuotes(v: string | undefined): string {
  if (v == null) return "";
  return v.replace(/^"|"$/g, "");
}

function round(n: number, d: number): number {
  const m = Math.pow(10, d);
  return Math.round(n * m) / m;
}

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else { inQ = false; }
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') inQ = true;
      else if (ch === ",") { out.push(cur); cur = ""; }
      else cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function deriveStateCode(
  geoType: "state" | "county" | "zip",
  regionName: string,
  metro: string
): string {
  if (geoType === "state") {
    const code = STATE_NAME_TO_CODE.get(regionName.trim().toLowerCase());
    return code ?? "";
  }
  if (geoType === "county") {
    const m = regionName.match(/,\s*([A-Z]{2})\s*$/);
    return m ? m[1] : "";
  }
  const m = metro.match(/,\s*([A-Z]{2})\s+metro/i);
  return m ? m[1].toUpperCase() : "";
}

function cleanRegionName(name: string, geoType: "state" | "county" | "zip"): string {
  if (!name) return "";
  if (geoType === "county") {
    return name.replace(/,\s*[A-Z]{2}\s*$/, "").trim();
  }
  return name.trim();
}

interface Sample {
  asOf: string;
  asOfMs: number;
  homesSold: number;
  pendingSales: number | null;
  activeListings: number | null;
  soldAboveListPct: number | null;
  medianDom: number | null;
  medianSalePrice: number | null;
}

interface Snap {
  name: string;
  stateCode: string;
  samples: Sample[];
}

const SAMPLE_MIN_SPACING_DAYS = 25;
const SAMPLES_PER_GEO = 3;

async function fetchSourceLastModified(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, { method: "HEAD" });
    if (!res.ok) return null;
    return res.headers.get("last-modified");
  } catch {
    return null;
  }
}

export interface SourceMeta {
  state: string | null;
  county: string | null;
  zip: string | null;
}

export async function fetchAllSourceLastModified(): Promise<SourceMeta> {
  const [state, county, zip] = await Promise.all([
    fetchSourceLastModified(SOURCES.state),
    fetchSourceLastModified(SOURCES.county),
    fetchSourceLastModified(SOURCES.zip)
  ]);
  return { state, county, zip };
}

function maxLastModified(meta: SourceMeta): string | null {
  const dates = [meta.state, meta.county, meta.zip]
    .filter((v): v is string => !!v)
    .map((s) => ({ raw: s, ms: Date.parse(s) }))
    .filter((x) => Number.isFinite(x.ms))
    .sort((a, b) => b.ms - a.ms);
  return dates.length > 0 ? dates[0].raw : null;
}

async function streamAndAggregate(
  url: string,
  geoType: "state" | "county" | "zip"
): Promise<MarketRow[]> {
  console.log(`[refresh ${geoType}] GET ${url}`);
  const res = await fetch(url, {
    headers: { "User-Agent": "silverstone-dashboard/1.0" }
  });
  if (!res.ok || !res.body) {
    throw new Error(`${url} -> ${res.status} ${res.statusText}`);
  }

  const webStream = res.body as unknown as NodeWebReadableStream<Uint8Array>;
  const nodeStream = Readable.fromWeb(webStream);
  const rl = readline.createInterface({ input: nodeStream, crlfDelay: Infinity });

  const byGeo = new Map<string, Snap>();
  let headerIdx: Record<string, number> | null = null;
  let lineNum = 0;
  let kept = 0;

  for await (const line of rl) {
    lineNum++;
    if (!line) continue;

    if (headerIdx == null) {
      const cols = parseCsvLine(line);
      headerIdx = {};
      for (let i = 0; i < cols.length; i++) {
        const norm = cols[i]
          .trim()
          .toLowerCase()
          .replace(/\(.*?\)/g, "")
          .trim()
          .replace(/[^a-z0-9]+/g, "_")
          .replace(/^_|_$/g, "");
        if (norm) headerIdx[norm] = i;
      }
      console.log(
        `[refresh ${geoType}] header parsed: ${cols.length} cols ` +
          `(period_end=${headerIdx.period_end}, ` +
          `region_name=${headerIdx.region_name}, ` +
          `frequency=${headerIdx.frequency})`
      );
      continue;
    }

    const cells = parseCsvLine(line);
    const getCol = (k: string): string | undefined => {
      const idx = headerIdx![k];
      if (idx == null || idx >= cells.length) return undefined;
      return cells[idx];
    };

    const periodEnd = stripQuotes(getCol("period_end"));
    if (!periodEnd) continue;
    const periodEndMs = Date.parse(periodEnd);
    if (!Number.isFinite(periodEndMs)) continue;

    const regionName = stripQuotes(getCol("region_name"));
    if (!regionName) continue;
    const metro = stripQuotes(getCol("metro"));

    const stateCode = deriveStateCode(geoType, regionName, metro);
    const cleanedName = cleanRegionName(regionName, geoType);

    const key =
      geoType === "zip"
        ? regionName.replace(/[^\d]/g, "")
        : `${cleanedName}|${stateCode}`;
    if (!key) continue;

    const homesSoldNum = toNum(getCol("homes_sold")) ?? 0;
    const pendingSalesNum = toNum(getCol("pending_sales"));
    const activeListingsNum = toNum(getCol("active_listings"));
    const soldAboveRaw = toNum(getCol("share_sold_above_original_list"));
    const medianDomNum = toNum(getCol("median_days_on_market"));
    const medianSalePriceNum = toNum(getCol("median_sale_price"));

    const sample: Sample = {
      asOf: periodEnd,
      asOfMs: periodEndMs,
      homesSold: Math.round(homesSoldNum),
      pendingSales: pendingSalesNum,
      activeListings: activeListingsNum,
      soldAboveListPct: soldAboveRaw,
      medianDom: medianDomNum,
      medianSalePrice: medianSalePriceNum
    };

    let snap = byGeo.get(key);
    if (!snap) {
      snap = { name: cleanedName, stateCode, samples: [sample] };
      byGeo.set(key, snap);
      kept++;
    } else {
      if (snap.samples.length === 0 || periodEndMs > snap.samples[0].asOfMs) {
        snap.name = cleanedName;
        snap.stateCode = stateCode;
      }
      const minSpacingMs = SAMPLE_MIN_SPACING_DAYS * 86_400_000;
      let tooClose = false;
      for (let i = 0; i < snap.samples.length; i++) {
        const s = snap.samples[i];
        if (Math.abs(periodEndMs - s.asOfMs) < minSpacingMs) {
          if (periodEndMs > s.asOfMs) snap.samples[i] = sample;
          tooClose = true;
          break;
        }
      }
      if (!tooClose) {
        snap.samples.push(sample);
        snap.samples.sort((a, b) => b.asOfMs - a.asOfMs);
        if (snap.samples.length > SAMPLES_PER_GEO) {
          snap.samples.length = SAMPLES_PER_GEO;
        }
        kept++;
      }
    }

  }

  try { rl.close(); } catch {}

  console.log(
    `[refresh ${geoType}] lines=${lineNum} kept=${kept} geos=${byGeo.size}`
  );

  return materializeRows(byGeo, geoType);
}

function materializeRows(
  byGeo: Map<string, Snap>,
  geoType: "state" | "county" | "zip"
): MarketRow[] {
  const out: MarketRow[] = [];
  const pendingDivisor = geoType === "zip" ? 3 : 1;

  for (const [key, s] of byGeo) {
    if (s.samples.length === 0) continue;

    const avg = (pick: (x: Sample) => number | null): number | null => {
      let sum = 0;
      let n = 0;
      for (const sample of s.samples) {
        const v = pick(sample);
        if (v != null) { sum += v; n++; }
      }
      return n > 0 ? sum / n : null;
    };

    const pendingAvg = avg((x) => {
      if (x.pendingSales == null || x.activeListings == null || x.activeListings <= 0) {
        return null;
      }
      return ((x.pendingSales / pendingDivisor) / x.activeListings) * 100;
    });
    const soldAboveAvg = avg((x) => x.soldAboveListPct);
    const domAvg = avg((x) => x.medianDom);
    const priceAvg = avg((x) => x.medianSalePrice);

    let homesSoldTotal = 0;
    for (const sample of s.samples) homesSoldTotal += sample.homesSold;

    const pending_pct = pendingAvg ?? 0;
    const median_dom = domAvg ?? 0;
    const dom_sub60_share = domAvg != null ? (domAvg < 60 ? 1 : 0) : 0;

    const newestAsOf = s.samples[0].asOf;
    const geoId = geoType === "zip" ? s.name.replace(/[^\d]/g, "") : key;
    const cleanedName = s.name;

    for (const winKey of WINDOW_KEYS) {
      out.push({
        geo_type: geoType,
        geo_id: geoId,
        name: cleanedName,
        state: s.stateCode,
        population: null,
        median_sale_price: priceAvg,
        pending_pct: round(pending_pct, 2),
        median_dom: round(median_dom, 1),
        dom_sub60_share,
        homes_sold: homesSoldTotal,
        window: winKey,
        as_of: newestAsOf
      });
    }
  }
  return out;
}

async function enrichWithCensus(rows: MarketRow[]): Promise<void> {
  try {
    const stateRes = await fetch(
      "https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=state:*"
    );
    if (stateRes.ok) {
      const arr = (await stateRes.json()) as string[][];
      const statePop = new Map<string, number>();
      for (let i = 1; i < arr.length; i++) {
        statePop.set(arr[i][0].toUpperCase(), Number(arr[i][1]));
      }
      let stateMatched = 0;
      for (const r of rows) {
        if (r.geo_type !== "state") continue;
        const p = statePop.get(r.name.toUpperCase());
        if (p != null) { r.population = p; stateMatched++; }
      }
      console.log(`[census] states matched: ${stateMatched}`);
    } else {
      console.warn(`[census] state fetch failed: ${stateRes.status}`);
    }

    const countyRes = await fetch(
      "https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=county:*"
    );
    if (countyRes.ok) {
      const arr = (await countyRes.json()) as string[][];
      const countyPop = new Map<string, number>();
      for (let i = 1; i < arr.length; i++) {
        const full = arr[i][0];
        const pop = Number(arr[i][1]);
        const lower = full.toLowerCase();
        countyPop.set(lower, pop);
        const stripped = lower
          .replace(/\s+county,/, ",")
          .replace(/\s+parish,/, ",")
          .replace(/\s+borough,/, ",")
          .replace(/\s+census area,/, ",")
          .replace(/\s+municipality,/, ",")
          .replace(/\s+city and borough,/, ",")
          .replace(/\s+city,/, ",");
        if (stripped !== lower) countyPop.set(stripped, pop);
      }
      const stateNameByCode = buildStateCodeToName();
      let countyMatched = 0;
      for (const r of rows) {
        if (r.geo_type !== "county") continue;
        const stateName = stateNameByCode.get(r.state.toUpperCase());
        if (!stateName) continue;
        const rawKey = `${r.name}, ${stateName}`.toLowerCase();
        const strippedKey = rawKey
          .replace(/\s+county,/, ",")
          .replace(/\s+parish,/, ",")
          .replace(/\s+borough,/, ",")
          .replace(/\s+census area,/, ",")
          .replace(/\s+municipality,/, ",")
          .replace(/\s+city and borough,/, ",");
        const p = countyPop.get(rawKey) ?? countyPop.get(strippedKey);
        if (p != null) { r.population = p; countyMatched++; }
      }
      console.log(`[census] counties matched: ${countyMatched}`);
    } else {
      console.warn(`[census] county fetch failed: ${countyRes.status}`);
    }
  } catch (e: unknown) {
    console.warn(`[census] enrichment error: ${(e as Error).message}`);
  }
}

export interface BuildResult {
  dataset: Dataset;
  source_last_modified: SourceMeta;
  upstream_last_modified: string | null;
}

export async function buildDataset(): Promise<BuildResult> {
  console.log("[refresh] buildDataset v=2026-05-data-center");
  const sourceMeta = await fetchAllSourceLastModified();
  console.log(`[manifest] source last-modified: ${JSON.stringify(sourceMeta)}`);

  const all: MarketRow[] = [];
  for (const geo of ["state", "county", "zip"] as const) {
    const rows = await streamAndAggregate(SOURCES[geo], geo);
    // Avoid `all.push(...rows)` — spread of large arrays exceeds V8's
    // max-arguments limit (~65K) and throws "Maximum call stack size exceeded".
    for (let i = 0; i < rows.length; i++) all.push(rows[i]);
  }
  await enrichWithCensus(all);

  const dataset: Dataset = {
    generated_at: new Date().toISOString(),
    source: "Redfin Data Center (relaunched 2026-05-12) + US Census ACS 5yr",
    source_last_modified: sourceMeta,
    rows: all
  };

  return {
    dataset,
    source_last_modified: sourceMeta,
    upstream_last_modified: maxLastModified(sourceMeta)
  };
}
