// lib/data.ts
import fs from "node:fs";
import path from "node:path";
import type { MarketRow, TimeWindow } from "./scoring";
import { scoreRow, scoreRowStatesOnly, type ScoredRow } from "./scoring";

export interface Dataset {
  generated_at: string;
  source: string;
  rows: MarketRow[];
}

let cache: Dataset | null = null;

export function loadDataset(): Dataset {
  if (cache) return cache;
  const filePath = path.join(process.cwd(), "data", "dataset.json");
  try {
    const raw = fs.readFileSync(filePath, "utf-8");
    cache = JSON.parse(raw) as Dataset;
    return cache;
  } catch {
    cache = { generated_at: new Date().toISOString(), source: "seed", rows: [] };
    return cache;
  }
}

export function getRows(
  geoType: MarketRow["geo_type"],
  window: TimeWindow
): ScoredRow[] {
  const ds = loadDataset();
  return ds.rows
    .filter((r) => r.geo_type === geoType && r.window === window)
    .map(scoreRow);
}

export function findState(query: string, window: TimeWindow): ScoredRow | null {
  const q = query.trim().toLowerCase();
  if (!q) return null;
  const rows = getRows("state", window);
  return (
    rows.find(
      (r) => r.state.toLowerCase() === q || r.name.toLowerCase() === q
    ) ?? null
  );
}

export function findCounty(query: string, window: TimeWindow): ScoredRow[] {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  const rows = getRows("county", window);
  return rows
    .filter((r) => {
      const full = `${r.name}, ${r.state}`.toLowerCase();
      return (
        full.includes(q) ||
        r.name.toLowerCase().includes(q) ||
        r.name.toLowerCase().replace(/ county$/, "").includes(q)
      );
    })
    .slice(0, 10);
}

export function findZip(zip: string, window: TimeWindow): ScoredRow | null {
  const q = zip.trim();
  if (!/^\d{5}$/.test(q)) return null;
  const rows = getRows("zip", window);
  return rows.find((r) => r.geo_id === q) ?? null;
}

export function topCountiesByState(
  state: string,
  window: TimeWindow,
  limit = 15
): ScoredRow[] {
  const s = state.trim().toUpperCase();
  if (!s) return [];
  // Sort by pending_pct desc as default; client can re-sort.
  return getRows("county", window)
    .filter((r) => r.state.toUpperCase() === s)
    .sort((a, b) => b.pending_pct - a.pending_pct)
    .slice(0, limit);
}

export function allStates(window: TimeWindow): ScoredRow[] {
  const ds = loadDataset();
  return ds.rows
    .filter((r) => r.geo_type === "state" && r.window === window)
    .map(scoreRowStatesOnly)
    .sort((a, b) => b.marketing_score - a.marketing_score);
}

export function listStates(window: TimeWindow): { code: string; name: string }[] {
  const rows = getRows("state", window);
  return rows
    .map((r) => ({ code: r.state, name: r.name }))
    .sort((a, b) => a.name.localeCompare(b.name));
}
