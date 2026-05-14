// lib/refresh.ts
// Pulls Redfin's new Data Center monthly CSVs (state/county/zip).
//
// Format notes (post May 2026 Data Center relaunch):
//   - Plain comma-separated, every header and text cell double-quoted.
//   - Served uncompressed. We use HTTP Range requests to grab only the
//     newest slice (file is sorted DESCENDING by PERIOD BEGIN), which
//     avoids pulling the full 446 MB ZIP file.
//   - State/county are calendar-monthly. ZIP is rolling 3-month — that's
//     the only ZIP granularity Redfin publishes now.
//   - Property type breakdown is gone; everything is "all residential".
//
// We emit one row per (geo, window) keeping the existing 30d/90d/180d/1y
// window keys so the dashboard UI keeps working — but every window
// carries the same monthly snapshot since per-window aggregation no
// longer exists at the source.

import type { MarketRow, TimeWindow } from "./scoring";
import type { Dataset } from "./data";

const BASE = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_data_center";

const SOURCES: Record<"state" | "county" | "zip", { url: string; rangeBytes: number }> = {
  // file ~1.7 MB total — grab the whole thing
  state:  { url: `${BASE}/housing_market/monthly/all_states.csv`,   rangeBytes: 2_000_000 },
  // file ~100 MB — first 4 MB covers many months of all counties
  county: { url: `${BASE}/housing_market/monthly/all_counties.csv`, rangeBytes: 4_000_000 },
  // file ~446 MB — first 12 MB covers the newest rolling-3-month slice for all zips
  zip:    { url: `${BASE}/housing_market/monthly/all_zips.csv`,     rangeBytes: 12_000_000 }
};

const MANIFEST_URL = `${BASE}/index.json`;

const WINDOW_KEYS: TimeWindow[] = ["30d", "90d", "180d", "1y"];

function toNum(v: string | undefined): number | null {
  if (v == null) return null;
  const stripped = v.replace(/^"|"$/g, "").trim();
  if (stripped === "" || stripped === "NA" || stripped.toUpperCase() === "N/A") return null;
  const n = Number(stripped.replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function stripQuotes(v: string | undefined): string {
  if (v == null) return "";
  return v.replace(/^"|"$/g, "").trim();
}

function round(n: number, d: number): number {
  const m = Math.pow(10, d);
  return Math.round(n * m) / m;
}

function normalizeHeader(h: string): string {
  return stripQuotes(h).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
}

/**
 * Parse a CSV line that may contain quoted fields with embedded commas.
 * Redfin's new files quote every text cell, so the parser must respect quotes.
 */
function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        // Escaped quote (RFC 4180): "" inside quoted field → literal "
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === ",") {
        out.push(cur);
        cur = "";
      } else if (ch === '"') {
        inQuotes = true;
      } else {
        cur += ch;
      }
    }
  }
  out.push(cur);
  return out;
}

/**
 * Counties (and ZIPs) embed the state inside REGION NAME as
 * "Bergen County, NJ" or similar. Pull it out.
 */
function parseRegionState(
  region: string,
  geoType: "state" | "county" | "zip"
): { name: string; stateCode: string } {
  if (geoType === "state") {
    return { name: region, stateCode: "" };
  }
  // ZIP names commonly come as "Zip Code: 07030, NJ" or just "07030, NJ" — try both.
  const cleaned = region.replace(/^Zip Code:\s*/i, "").trim();
  const m = cleaned.match(/^(.*?),\s*([A-Z]{2})\s*$/);
  if (m) {
    return { name: m[1].trim(), stateCode: m[2] };
  }
  return { name: cleaned, stateCode: "" };
}

interface ParsedRow {
  geoType: "state" | "county" | "zip";
  name: string;
  stateCode: string;
  asOf: string;
  homesSold: number | null;
  pendingSales: number | null;
  activeListings: number | null;
  medianDom: number | null;
  medianSalePrice: number | null;
  sharedAboveListPct: number | null;
}

async function fetchAndParseSlice(
  geoType: "state" | "county" | "zip"
): Promise<ParsedRow[]> {
  const { url, rangeBytes } = SOURCES[geoType];
  console.log(`[refresh ${geoType}] fetching first ${rangeBytes} bytes of ${url}`);

  const res = await fetch(url, {
    headers: {
      "User-Agent": "silverstone-dashboard/2.0",
      Range: `bytes=0-${rangeBytes - 1}`
    }
  });
  // S3 returns 206 Partial Content for ranged requests; 200 if it gave us the whole file.
  if (!res.ok && res.status !== 206) {
    throw new Error(`${url} → ${res.status}`);
  }
  const text = await res.text();

  const lines = text.split(/\r?\n/);
  if (lines.length < 2) {
    throw new Error(`${geoType}: too few lines in response`);
  }
  // Drop trailing empty lines, then drop the last (possibly truncated) line.
  while (lines.length > 0 && lines[lines.length - 1].trim() === "") lines.pop();
  if (lines.length > 1) lines.pop();

  const headerCells = parseCsvLine(lines[0]).map(normalizeHeader);
  const idx: Record<string, number> = {};
  for (let i = 0; i < headerCells.length; i++) idx[headerCells[i]] = i;
  console.log(
    `[refresh ${geoType}] header: ${headerCells.length} cols, sample: ${headerCells.slice(0, 6).join(",")}`
  );

  const col = (...candidates: string[]): number | null => {
    for (const c of candidates) {
      if (idx[c] != null) return idx[c];
    }
    return null;
  };
  const iPeriodEnd = col("period_end");
  const iRegion = col("region_name", "region");
  const iHomesSold = col("homes_sold");
  const iPending = col("pending_sales");
  const iActive = col("active_listings", "inventory");
  const iDom = col("median_days_on_market_days", "median_days_on_market", "median_dom");
  const iPrice = col("median_sale_price", "median_sale_price_");
  const iAboveList = col(
    "share_sold_above_original_list",
    "share_sold_above_original_list_",
    "sold_above_list"
  );

  if (iPeriodEnd == null || iRegion == null) {
    throw new Error(
      `${geoType}: missing required columns. Got headers: ${headerCells.join(",")}`
    );
  }

  const out: ParsedRow[] = [];
  let kept = 0;
  let skipped = 0;
  for (let li = 1; li < lines.length; li++) {
    const line = lines[li];
    if (!line) continue;
    const cells = parseCsvLine(line);
    if (cells.length < headerCells.length / 2) {
      skipped++;
      continue;
    }

    const periodEnd = stripQuotes(cells[iPeriodEnd]);
    const region = stripQuotes(cells[iRegion]);
    if (!periodEnd || !region) {
      skipped++;
      continue;
    }

    const { name, stateCode } = parseRegionState(region, geoType);

    out.push({
      geoType,
      name,
      stateCode,
      asOf: periodEnd,
      homesSold: iHomesSold != null ? toNum(cells[iHomesSold]) : null,
      pendingSales: iPending != null ? toNum(cells[iPending]) : null,
      activeListings: iActive != null ? toNum(cells[iActive]) : null,
      medianDom: iDom != null ? toNum(cells[iDom]) : null,
      medianSalePrice: iPrice != null ? toNum(cells[iPrice]) : null,
      sharedAboveListPct: iAboveList != null ? toNum(cells[iAboveList]) : null
    });
    kept++;
  }
  console.log(`[refresh ${geoType}] lines=${lines.length - 1} kept=${kept} skipped=${skipped}`);
  return out;
}

/**
 * For each geo, keep only the most-recent period. The source file is sorted
 * descending by PERIOD BEGIN, so the first occurrence of each (name, stateCode)
 * key is the newest.
 */
function pickLatestPerGeo(rows: ParsedRow[]): ParsedRow[] {
  const seen = new Map<string, ParsedRow>();
  for (const r of rows) {
    const key = r.geoType === "zip" ? r.name : `${r.name}|${r.stateCode}`;
    if (!seen.has(key)) seen.set(key, r);
  }
  return Array.from(seen.values());
}

function toMarketRows(latest: ParsedRow[]): MarketRow[] {
  const out: MarketRow[] = [];
  for (const r of latest) {
    // Pending % = pending_sales / active_listings * 100. Same definition as before.
    let pendingPct = 0;
    if (
      r.pendingSales != null &&
      r.activeListings != null &&
      r.activeListings > 0
    ) {
      pendingPct = (r.pendingSales / r.activeListings) * 100;
    }
    const medianDom = r.medianDom ?? 0;
    const dom_sub60_share = r.medianDom != null && r.medianDom < 60 ? 1 : 0;
    const geoId = r.geoType === "zip" ? r.name.replace(/[^\d]/g, "") : `${r.name}|${r.stateCode}`;

    // Same row replicated across windows. Dashboard UI keeps its window
    // selector intact, but every window points at the same monthly snapshot
    // because the new feed no longer publishes per-window aggregations.
    for (const winKey of WINDOW_KEYS) {
      out.push({
        geo_type: r.geoType,
        geo_id: geoId,
        name: r.name,
        state: r.stateCode,
        population: null,
        median_sale_price: r.medianSalePrice,
        pending_pct: round(pendingPct, 2),
        median_dom: round(medianDom, 1),
        dom_sub60_share,
        homes_sold: r.homesSold != null ? Math.round(r.homesSold) : 0,
        window: winKey,
        as_of: r.asOf
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
        if (p != null) {
          r.population = p;
          stateMatched++;
        }
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

/**
 * Best-effort manifest check. Returns true if reachable + parseable; otherwise
 * logs and continues. Manifest content isn't strictly required for the build —
 * it's mainly a sanity probe so cron logs flag upstream changes early.
 */
async function probeManifest(): Promise<void> {
  try {
    const res = await fetch(MANIFEST_URL, {
      headers: { "User-Agent": "silverstone-dashboard/2.0" }
    });
    if (!res.ok) {
      console.warn(`[manifest] ${MANIFEST_URL} → ${res.status}`);
      return;
    }
    const json = (await res.json()) as { date_ranges?: unknown };
    if (json.date_ranges) {
      console.log(
        `[manifest] ok, date_ranges keys: ${Object.keys(json.date_ranges as object).length}`
      );
    } else {
      console.log(`[manifest] ok (no date_ranges block)`);
    }
  } catch (e) {
    console.warn(`[manifest] probe failed: ${(e as Error).message}`);
  }
}

/**
 * Lightweight HEAD probe — returns the newest Last-Modified across the three
 * monthly CSVs. Used by /api/refresh to decide whether to skip a rebuild.
 */
export async function probeLastModified(): Promise<{
  newestLastModified: string | null;
  perFile: Record<string, string | null>;
}> {
  const perFile: Record<string, string | null> = {};
  let newestMs = -Infinity;
  let newestStr: string | null = null;
  for (const [geo, { url }] of Object.entries(SOURCES)) {
    try {
      const res = await fetch(url, {
        method: "HEAD",
        headers: { "User-Agent": "silverstone-dashboard/2.0" }
      });
      const lm = res.headers.get("last-modified");
      perFile[geo] = lm;
      if (lm) {
        const ms = Date.parse(lm);
        if (Number.isFinite(ms) && ms > newestMs) {
          newestMs = ms;
          newestStr = lm;
        }
      }
    } catch (e) {
      perFile[geo] = null;
      console.warn(`[head ${geo}] ${(e as Error).message}`);
    }
  }
  return { newestLastModified: newestStr, perFile };
}

export async function buildDataset(): Promise<Dataset> {
  console.log("[refresh] buildDataset v=data-center-2026-05");
  await probeManifest();

  const all: MarketRow[] = [];
  for (const geo of ["state", "county", "zip"] as const) {
    const rows = await fetchAndParseSlice(geo);
    const latest = pickLatestPerGeo(rows);
    console.log(`[refresh ${geo}] unique geos in slice: ${latest.length}`);
    const marketRows = toMarketRows(latest);
    all.push(...marketRows);
  }
  await enrichWithCensus(all);
  return {
    generated_at: new Date().toISOString(),
    source: "Redfin Data Center (new pipeline, May 2026) + US Census ACS 5yr",
    rows: all
  };
}
