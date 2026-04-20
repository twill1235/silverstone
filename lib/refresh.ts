// lib/refresh.ts
// Pulls Redfin Data Center weekly TSVs (state/county/zip), aggregates per
// (geo, time-window) in a SINGLE STREAMING PASS to avoid V8's 512MB string
// cap on the ~1GB uncompressed ZIP file.

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

const WINDOWS: { key: TimeWindow; days: number }[] = [
  { key: "30d", days: 30 },
  { key: "90d", days: 90 },
  { key: "180d", days: 180 },
  { key: "1y", days: 365 }
];

/** Redfin's period_duration varies by file. Verified from production logs. */
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

interface Accum {
  homesSold: number;
  pendingSum: number;
  soldForPending: number;
  domSum: number;
  domN: number;
  domSub60: number;
  priceVolumeWeightedSum: number;
  priceWeightTotal: number;
}

interface GeoState {
  name: string;
  stateCode: string;
  newestAsOf: string;
  newestAsOfMs: number;
  accs: Map<TimeWindow, Accum>;
}

function newAccum(): Accum {
  return {
    homesSold: 0,
    pendingSum: 0,
    soldForPending: 0,
    domSum: 0,
    domN: 0,
    domSub60: 0,
    priceVolumeWeightedSum: 0,
    priceWeightTotal: 0
  };
}

async function streamAndAggregate(
  url: string,
  geoType: "state" | "county" | "zip"
): Promise<MarketRow[]> {
  const res = await fetch(url, {
    headers: { "User-Agent": "silverstone-dashboard/1.0" }
  });
  if (!res.ok || !res.body) {
    throw new Error(`${url} → ${res.status}`);
  }

  const nowMs = Date.now();
  const cutoffsMs: { key: TimeWindow; cutoffMs: number }[] = WINDOWS.map(
    (w) => ({ key: w.key, cutoffMs: nowMs - w.days * 86_400_000 })
  );

  const webStream = res.body as unknown as NodeWebReadableStream<Uint8Array>;
  const nodeStream = Readable.fromWeb(webStream);
  const gunzipStream = nodeStream.pipe(zlib.createGunzip());
  const rl = readline.createInterface({
    input: gunzipStream,
    crlfDelay: Infinity
  });

  const byGeo = new Map<string, GeoState>();
  let headerIdx: Record<string, number> | null = null;
  let lineNum = 0;
  let kept = 0;
  const durationSamples: string[] = [];

  for await (const line of rl) {
    lineNum++;
    if (!line) continue;

    // Header: strip quotes, lowercase for defensive lookup.
    if (headerIdx == null) {
      const cols = line.split("\t");
      headerIdx = {};
      for (let i = 0; i < cols.length; i++) {
        const key = (cols[i] ?? "").replace(/^"|"$/g, "").toLowerCase();
        headerIdx[key] = i;
      }
      console.log(
        `[refresh ${geoType}] header parsed: ${cols.length} cols`
      );
      continue;
    }

    const cells = line.split("\t");
    const getCol = (name: string): string | undefined => {
      const idx = headerIdx![name];
      if (idx == null) return undefined;
      return stripQuotes(cells[idx]);
    };

    const durRaw = getCol("period_duration");
    // One-time diagnostic: capture the first few period_duration values we see
    // so Vercel logs show the real distribution if the filter ever drops rows.
    if (durationSamples.length < 5 && durRaw != null) {
      durationSamples.push(durRaw);
    }

    const dur = toNum(durRaw);
    if (dur !== PERIOD_DURATION_BY_GEO[geoType]) continue;

    const region = getCol("region") ?? "";
    if (!region) continue;

    const stateCode = getCol("state_code") || getCol("state") || "";
    const periodEnd = getCol("period_end") ?? "";
    if (!periodEnd) continue;

    const periodEndMs = Date.parse(periodEnd);
    if (!Number.isFinite(periodEndMs)) continue;

    const key = geoType === "zip" ? region : `${region}|${stateCode}`;

    let state = byGeo.get(key);
    if (!state) {
      state = {
        name: region,
        stateCode,
        newestAsOf: periodEnd,
        newestAsOfMs: periodEndMs,
        accs: new Map()
      };
      byGeo.set(key, state);
    } else if (periodEndMs > state.newestAsOfMs) {
      state.newestAsOf = periodEnd;
      state.newestAsOfMs = periodEndMs;
      state.name = region;
      state.stateCode = stateCode;
    }

    const sold = toNum(getCol("homes_sold"));
    const pend = toNum(getCol("pending_sales"));
    const dom = toNum(getCol("median_days_on_market"));
    const price = toNum(getCol("median_sale_price"));

    let matchedAny = false;
    for (const c of cutoffsMs) {
      if (periodEndMs < c.cutoffMs) continue;
      matchedAny = true;
      let acc = state.accs.get(c.key);
      if (!acc) {
        acc = newAccum();
        state.accs.set(c.key, acc);
      }
      if (sold != null) acc.homesSold += sold;
      if (pend != null && sold != null && sold > 0) {
        acc.pendingSum += pend;
        acc.soldForPending += sold;
      }
      if (dom != null) {
        acc.domSum += dom;
        acc.domN++;
        if (dom < 60) acc.domSub60++;
      }
      if (price != null && sold != null && sold > 0) {
        acc.priceVolumeWeightedSum += price * sold;
        acc.priceWeightTotal += sold;
      }
    }
    if (matchedAny) kept++;
  }

  console.log(
    `[refresh ${geoType}] lines=${lineNum} kept=${kept} geos=${byGeo.size} dur_samples=${JSON.stringify(durationSamples)}`
  );

  const out: MarketRow[] = [];
  for (const [key, state] of byGeo) {
    for (const [winKey, acc] of state.accs) {
      const pending_pct =
        acc.soldForPending > 0
          ? (acc.pendingSum / acc.soldForPending) * 100
          : 0;
      const median_dom = acc.domN > 0 ? acc.domSum / acc.domN : 0;
      const dom_sub60_share = acc.domN > 0 ? acc.domSub60 / acc.domN : 0;
      const median_sale_price =
        acc.priceWeightTotal > 0
          ? acc.priceVolumeWeightedSum / acc.priceWeightTotal
          : null;

      const geoId =
        geoType === "zip" ? state.name.replace(/[^\d]/g, "") : key;

      out.push({
        geo_type: geoType,
        geo_id: geoId,
        name: cleanRegionName(state.name, geoType),
        state: state.stateCode,
        population: null,
        median_sale_price,
        pending_pct: round(pending_pct, 2),
        median_dom: round(median_dom, 1),
        dom_sub60_share: round(dom_sub60_share, 3),
        homes_sold: Math.round(acc.homesSold),
        window: winKey,
        as_of: state.newestAsOf
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
      for (const r of rows) {
        if (r.geo_type === "state") {
          const p = statePop.get(r.name.toUpperCase());
          if (p != null) r.population = p;
        }
      }
    }

    const countyRes = await fetch(
      "https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=county:*"
    );
    if (countyRes.ok) {
      const arr = (await countyRes.json()) as string[][];
      const countyPop = new Map<string, number>();
      for (let i = 1; i < arr.length; i++) {
        countyPop.set(arr[i][0].toLowerCase(), Number(arr[i][1]));
      }
      const stateNameByCode = buildStateNameMap();
      for (const r of rows) {
        if (r.geo_type !== "county") continue;
        const stateName = stateNameByCode.get(r.state.toUpperCase());
        if (!stateName) continue;
        const key = `${r.name}, ${stateName}`.toLowerCase();
        const p = countyPop.get(key);
        if (p != null) r.population = p;
      }
    }
  } catch {
    // non-fatal
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
