// scripts/refresh-data.mjs
// Local-only refresh — mirrors lib/refresh.ts but writes to disk instead of
// committing to GitHub. Useful for local debugging.
//
// In production, Vercel Cron calls /api/refresh which uses lib/refresh.ts
// and commits to GitHub.
//
// New Data Center format (post May 2026 relaunch):
//   - Plain CSV, every header and text cell double-quoted
//   - Served uncompressed (Content-Length ~1.7 MB / 100 MB / 446 MB)
//   - Sorted DESCENDING by PERIOD BEGIN
//   - We use HTTP Range to grab only the newest slice (no gunzip, no
//     ERR_STRING_TOO_LONG, no temp files)

import fs from "node:fs";
import path from "node:path";

const BASE = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_data_center";

const SOURCES = {
  state:  { url: `${BASE}/housing_market/monthly/all_states.csv`,   rangeBytes: 2_000_000 },
  county: { url: `${BASE}/housing_market/monthly/all_counties.csv`, rangeBytes: 4_000_000 },
  zip:    { url: `${BASE}/housing_market/monthly/all_zips.csv`,     rangeBytes: 12_000_000 }
};

const WINDOW_KEYS = ["30d", "90d", "180d", "1y"];

function stripQuotes(v) {
  if (v == null) return "";
  return v.replace(/^"|"$/g, "").trim();
}

function toNum(v) {
  if (v == null) return null;
  const s = stripQuotes(v);
  if (s === "" || s === "NA" || s.toUpperCase() === "N/A") return null;
  const n = Number(s.replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function round(n, d) {
  const m = Math.pow(10, d);
  return Math.round(n * m) / m;
}

function normalizeHeader(h) {
  return stripQuotes(h).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else { inQuotes = false; }
      } else {
        cur += ch;
      }
    } else {
      if (ch === ",") { out.push(cur); cur = ""; }
      else if (ch === '"') { inQuotes = true; }
      else { cur += ch; }
    }
  }
  out.push(cur);
  return out;
}

function parseRegionState(region, geoType) {
  if (geoType === "state") return { name: region, stateCode: "" };
  const cleaned = region.replace(/^Zip Code:\s*/i, "").trim();
  const m = cleaned.match(/^(.*?),\s*([A-Z]{2})\s*$/);
  if (m) return { name: m[1].trim(), stateCode: m[2] };
  return { name: cleaned, stateCode: "" };
}

async function fetchAndParseSlice(geoType) {
  const { url, rangeBytes } = SOURCES[geoType];
  console.log(`[refresh ${geoType}] fetching first ${rangeBytes} bytes of ${url}`);

  const res = await fetch(url, {
    headers: {
      "User-Agent": "silverstone-dashboard/2.0",
      Range: `bytes=0-${rangeBytes - 1}`
    }
  });
  if (!res.ok && res.status !== 206) {
    throw new Error(`${url} → ${res.status}`);
  }
  const text = await res.text();

  const lines = text.split(/\r?\n/);
  if (lines.length < 2) throw new Error(`${geoType}: too few lines in response`);
  while (lines.length > 0 && lines[lines.length - 1].trim() === "") lines.pop();
  if (lines.length > 1) lines.pop();

  const headerCells = parseCsvLine(lines[0]).map(normalizeHeader);
  const idx = {};
  for (let i = 0; i < headerCells.length; i++) idx[headerCells[i]] = i;
  console.log(`[refresh ${geoType}] header: ${headerCells.length} cols, sample: ${headerCells.slice(0, 6).join(",")}`);

  const col = (...candidates) => {
    for (const c of candidates) if (idx[c] != null) return idx[c];
    return null;
  };
  const
