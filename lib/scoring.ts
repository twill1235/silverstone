// lib/scoring.ts
// Marketing score: equal 1/3 weights on Pending %, DOM (sub-60 strength), Transaction Volume.
// Thresholds per site spec:
//   Pending %: 25%+ = Good
//   Transaction Volume: 15,000+ = Good; 10,000–15,000 = Medium; else Low
//   DOM sub-60: the share (0–1) of weeks in window where median DOM < 60

export type TimeWindow = "30d" | "90d" | "180d" | "1y";

export interface MarketRow {
  geo_type: "state" | "county" | "zip";
  geo_id: string;
  name: string;
  state: string;
  population: number | null;
  median_sale_price: number | null;
  pending_pct: number;          // 0–100
  median_dom: number;           // days
  dom_sub60_share: number;      // 0–1
  homes_sold: number;           // transaction volume in window
  window: TimeWindow;
  as_of: string;                // ISO date of newest week used
}

export interface ScoredRow extends MarketRow {
  pending_score: number;        // 0–100
  dom_score: number;            // 0–100
  volume_score: number;         // 0–100
  marketing_score: number;      // 0–100 (equal weights)
  pending_tier: "Good" | "Low";
  volume_tier: "Good" | "Medium" | "Low";
}

export function scorePending(pendingPct: number): number {
  // Linear ramp: 0% → 0, 25% → 70 (threshold for "Good"), 50%+ → 100
  if (pendingPct <= 0) return 0;
  if (pendingPct >= 50) return 100;
  if (pendingPct <= 25) return (pendingPct / 25) * 70;
  return 70 + ((pendingPct - 25) / 25) * 30;
}

export function scoreDom(domSub60Share: number): number {
  // Share of weeks with DOM < 60 directly maps 0–1 → 0–100
  return Math.max(0, Math.min(100, domSub60Share * 100));
}

export function scoreVolume(homesSold: number): number {
  // Piecewise: <10k climbs to 50, 10k–15k climbs to 70 (Medium→Good boundary),
  // 15k+ climbs to 100 at 30k, caps there.
  if (homesSold <= 0) return 0;
  if (homesSold < 10000) return (homesSold / 10000) * 50;
  if (homesSold < 15000) return 50 + ((homesSold - 10000) / 5000) * 20;
  if (homesSold < 30000) return 70 + ((homesSold - 15000) / 15000) * 30;
  return 100;
}

export function scoreRow(row: MarketRow): ScoredRow {
  const pending_score = scorePending(row.pending_pct);
  const dom_score = scoreDom(row.dom_sub60_share);
  const volume_score = scoreVolume(row.homes_sold);
  const marketing_score = (pending_score + dom_score + volume_score) / 3;

  const pending_tier: ScoredRow["pending_tier"] =
    row.pending_pct >= 25 ? "Good" : "Low";
  const volume_tier: ScoredRow["volume_tier"] =
    row.homes_sold >= 15000 ? "Good" : row.homes_sold >= 10000 ? "Medium" : "Low";

  return {
    ...row,
    pending_score,
    dom_score,
    volume_score,
    marketing_score,
    pending_tier,
    volume_tier
  };
}

/**
 * Volume weight that fades small-market skew without ignoring them entirely.
 * Log-scaled so:
 *   <500 homes sold        → near 0   (very low weight)
 *   ~5,000 homes sold       → ~0.55   (modest weight)
 *   ~30,000 homes sold      → ~0.90   (near full weight)
 *   60,000+ homes sold      → 1.00    (full weight)
 */
export function volumeWeight(homesSold: number): number {
  if (homesSold <= 500) return 0;
  const LOW = 500;
  const HIGH = 60000;
  const numerator = Math.log10(homesSold / LOW);
  const denominator = Math.log10(HIGH / LOW);
  const w = numerator / denominator;
  return Math.max(0, Math.min(1, w));
}

/**
 * Score variant used by the Marketing Spend Insights states table:
 * Blends Pending % + DOM, then multiplies by a volume weight so small states
 * with high percentages but low volume are de-prioritized in rankings.
 */
export function scoreRowStatesOnly(row: MarketRow): ScoredRow {
  const base = scoreRow(row);
  const raw = (base.pending_score + base.dom_score) / 2;
  const weight = volumeWeight(row.homes_sold);
  const marketing_score = raw * weight;
  return { ...base, marketing_score };
}

export const WINDOW_LABELS: Record<TimeWindow, string> = {
  "30d": "Last 30 days",
  "90d": "Last 90 days",
  "180d": "Last 180 days",
  "1y": "Last 1 year"
};
