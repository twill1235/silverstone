// lib/refresh.ts
// Pulls Redfin Data Center TSVs (state/county/zip), streams them (avoids V8's
// 512MB string cap on the ~1GB ZIP file), picks the MOST RECENT row per geo,
// and emits Redfin's pre-computed values directly. Time-window keys exist for
// UI compatibility but all carry the same underlying Redfin snapshot.
//
// "Pending %" in the UI = Redfin's `off_market_in_two_weeks` column: the
// percentage of homes that went under contract within two weeks of listing.
// Real 0-100% market-heat metric — high = hot market.

import zlib from "node:zlib";
import { Readable } from "node:stream";
import readline from "node:readline";
import type { ReadableStream as NodeWebReadableStream } from "node:stream/web";
import type { MarketRow, TimeWindow } from "./scoring";
import type { Dataset } from "./data";

const SOURCES: Record<"state" | "county" | "zip", string> = {
  state:
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/state_market_tracker.tsv000.gz",
  county:
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz",
  zip:
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz"
};

const WINDOW_KEYS: TimeWindow[] = ["30d", "90d", "180d", "1y"];

/** period_duration Redfin uses per file; verified from production logs. */
const PERIOD_DURATION_BY_GEO: Record<"state" | "county" | "zip", number> = {
  state: 30,
  county: 30,
  zip: 90
};

function stripQuotes(v: string | undefined): string | undefined {
  if (v == null) return undefined;
  return v.replace(/^"|"$/g, "");
}

function toNum(v: string | undefined): number | null {
  if (v == null || v === "" || v === "NA") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function round(n: number, d: number): number {
  const m = Math.pow(10, d);
  return Math.round(n * m) / m;
}

function cleanRegionName(name: string, geoType: "state" | "county" | "zip") {
  if (!name) return "";
  if (geoType === "zip") return name.replace(/^Zip Code:\s*/i, "").trim();
  return name.trim();
}

interface Snap {
  name: string;
  stateCode: string;
  asOf: string;
  asOfMs: number;
  homesSold: number;
  offMarketInTwoWeeksPct: number | null; // 0-100
  medianDom: number | null;
  medianSalePrice: number | null;
}

async function streamAndAggregate(
  url: string,
  geoType: "state" | "county" | "zip"
): Promise<MarketRow[]> {
  const res = await fetch(url, {
    headers: { "User-Agent": "silverstone-dashboard/1.0" }
  });
  if (!res.ok || !res.body) throw new Error(`${url} → ${res.status}`);

  const webStream = res.body as unknown as NodeWebReadableStream<Uint8Array>;
  const nodeStream = Readable.fromWeb(webStream);
  const gunzipStream = nodeStream.pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: gunzipStream, crlfDelay: Infinity });

  const byGeo = new Map<string, Snap>();
  let headerIdx: Record<string, number> | null = null;
  let lineNum = 0;
  let kept = 0;

  for await (const line of rl) {
    lineNum++;
    if (!line) continue;

    if (headerIdx == null) {
      const cols = line.split("\t");
      headerIdx = {};
      for (let i = 0; i < cols.length; i++) {
        const key = (cols[i] ?? "").replace(/^"|"$/g, "").toLowerCase();
        headerIdx[key] = i;
      }
      console.log(`[refresh ${geoType}] header parsed: ${cols.length} cols`);
      continue;
    }

    const cells = line.split("\t");
    const getCol = (name: string): string | undefined => {
      const idx = headerIdx![name];
      if (idx == null) return undefined;
      return stripQuotes(cells[idx]);
    };

    const dur = toNum(getCol("period_duration"));
    if (dur !== PERIOD_DURATION_BY_GEO[geoType]) continue;

    const region = getCol("region") ?? "";
    if (!region) continue;

    const stateCode = getCol("state_code") || getCol("state") || "";
    const periodEnd = getCol("period_end") ?? "";
    if (!periodEnd) continue;
    const periodEndMs = Date.parse(periodEnd);
    if (!Number.isFinite(periodEndMs)) continue;

    const key = geoType === "zip" ? region : `${region}|${stateCode}`;
    const existing = byGeo.get(key);
    if (existing && periodEndMs <= existing.asOfMs) continue;

    const homesSoldNum = toNum(getCol("homes_sold")) ?? 0;
    const offMarketRaw = toNum(getCol("off_market_in_two_weeks"));
    const medianDomNum = toNum(getCol("median_dom"));
    const medianSalePriceNum = toNum(getCol("median_sale_price"));

    // Redfin publishes off_market_in_two_weeks as a 0-1 fraction. Normalize to 0-100.
    // Defensive: if a row is already >1, assume it's already percent-scaled.
    let offMarketPct: number | null = null;
    if (offMarketRaw != null) {
      offMarketPct = offMarketRaw <= 1 ? offMarketRaw * 100 : offMarketRaw;
    }

    byGeo.set(key, {
      name: region,
      stateCode,
      asOf: periodEnd,
      asOfMs: periodEndMs,
      homesSold: Math.round(homesSoldNum),
      offMarketInTwoWeeksPct: offMarketPct,
      medianDom: medianDomNum,
      medianSalePrice: medianSalePriceNum
    });
    kept++;
  }

  console.log(`[refresh ${geoType}] lines=${lineNum} kept=${kept} geos=${byGeo.size}`);

  const out: MarketRow[] = [];
  for (const [key, s] of byGeo) {
    const pending_pct = s.offMarketInTwoWeeksPct ?? 0;
    const median_dom = s.medianDom ?? 0;
    const dom_sub60_share =
      s.medianDom != null ? (s.medianDom < 60 ? 1 : 0) : 0;

    const geoId = geoType === "zip" ? s.name.replace(/[^\d]/g, "") : key;
    const cleanedName = cleanRegionName(s.name, geoType);

    for (const winKey of WINDOW_KEYS) {
      out.push({
        geo_type: geoType,
        geo_id: geoId,
        name: cleanedName,
        state: s.stateCode,
        population: null,
        median_sale_price: s.medianSalePrice,
        pending_pct: round(pending_pct, 2),
        median_dom: round(median_dom, 1),
        dom_sub60_share,
        homes_sold: s.homesSold,
        window: winKey,
        as_of: s.asOf
      });
    }
  }

  return out;
}

async function enrichWithCensus(rows: MarketRow[]): Promise<void> {
  try {
    // ----- States -----
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
        if (p != null) {
          r.population = p;
          stateMatched++;
        }
      }
      console.log(`[census] states matched: ${stateMatched}`);
    } else {
      console.warn(`[census] state fetch failed: ${stateRes.status}`);
    }

    // ----- Counties -----
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
      const stateNameByCode = buildStateNameMap();
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
        if (p != null) {
          r.population = p;
          countyMatched++;
        }
      }
      console.log(`[census] counties matched: ${countyMatched}`);
    } else {
      console.warn(`[census] county fetch failed: ${countyRes.status}`);
    }
  } catch (e: unknown) {
    console.warn(`[census] enrichment error: ${(e as Error).message}`);
  }
}

function buildStateNameMap(): Map<string, string> {
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
    ["WI", "Wisconsin"], ["WY", "Wyoming"], ["DC", "District of Columbia"]
  ];
  for (const [c, n] of pairs) m.set(c, n);
  return m;
}

export async function buildDataset(): Promise<Dataset> {
  console.log("[refresh] buildDataset v=offmarket-2wk");
  const all: MarketRow[] = [];
  for (const geo of ["state", "county", "zip"] as const) {
    const rows = await streamAndAggregate(SOURCES[geo], geo);
    all.push(...rows);
  }
  await enrichWithCensus(all);
  return {
    generated_at: new Date().toISOString(),
    source: "Redfin Data Center + US Census ACS 5yr",
    rows: all
  };
}
