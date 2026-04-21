// components/DashboardClient.tsx
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { ScoredRow, TimeWindow } from "@/lib/scoring";
import { WINDOW_LABELS } from "@/lib/scoring";

type Meta = {
  generated_at: string;
  source: string;
  row_count: number;
  state_as_of: string | null;
  county_as_of: string | null;
  zip_as_of: string | null;
};

export default function DashboardClient({ meta }: { meta: Meta }) {
  const [win, setWin] = useState<TimeWindow>("90d");

  // Searches
  const [stateQ, setStateQ] = useState("");
  const [stateResult, setStateResult] = useState<ScoredRow | null>(null);
  const [stateLoading, setStateLoading] = useState(false);
  const [stateErr, setStateErr] = useState<string | null>(null);

  const [countyQ, setCountyQ] = useState("");
  const [countyResults, setCountyResults] = useState<ScoredRow[]>([]);
  const [countyLoading, setCountyLoading] = useState(false);
  const [countyErr, setCountyErr] = useState<string | null>(null);

  const [zipQ, setZipQ] = useState("");
  const [zipResult, setZipResult] = useState<ScoredRow | null>(null);
  const [zipLoading, setZipLoading] = useState(false);
  const [zipErr, setZipErr] = useState<string | null>(null);

  // Top 15 counties by state
  const [stateList, setStateList] = useState<{ code: string; name: string }[]>([]);
  const [topState, setTopState] = useState<string>("");
  const [topRows, setTopRows] = useState<ScoredRow[]>([]);

  // Marketing spend — all 50 states
  const [allStatesRows, setAllStatesRows] = useState<ScoredRow[]>([]);

  const fetchJSON = useCallback(async <T,>(url: string): Promise<T> => {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`${res.status}`);
    return (await res.json()) as T;
  }, []);

  // States list loads once
  useEffect(() => {
    (async () => {
      const data = await fetchJSON<{ results: { code: string; name: string }[] }>(
        `/api/search?mode=listStates&window=${win}`
      );
      setStateList(data.results);
      if (!topState && data.results.length) setTopState(data.results[0].code);
    })();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // All-states spend table reloads when window changes
  useEffect(() => {
    (async () => {
      const data = await fetchJSON<{ results: ScoredRow[] }>(
        `/api/search?mode=allStates&window=${win}`
      );
      setAllStatesRows(data.results);
    })();
  }, [win, fetchJSON]);

  // Top counties reload when state or window changes
  useEffect(() => {
    if (!topState) return;
    (async () => {
      const data = await fetchJSON<{ results: ScoredRow[] }>(
        `/api/search?mode=topByState&q=${encodeURIComponent(topState)}&window=${win}`
      );
      setTopRows(data.results);
    })();
  }, [topState, win, fetchJSON]);

  // When window changes, re-run active searches
  useEffect(() => {
    if (stateResult) void runStateSearch(stateResult.state || stateQ);
    if (countyResults.length) void runCountySearch(countyQ);
    if (zipResult) void runZipSearch(zipResult.geo_id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [win]);

  async function runStateSearch(q: string) {
    setStateErr(null); setStateLoading(true);
    try {
      const data = await fetchJSON<{ result: ScoredRow | null }>(
        `/api/search?mode=state&q=${encodeURIComponent(q)}&window=${win}`
      );
      if (!data.result) setStateErr(`No state match for “${q}”`);
      setStateResult(data.result);
    } catch { setStateErr("Search failed"); }
    finally { setStateLoading(false); }
  }

  async function runCountySearch(q: string) {
    setCountyErr(null); setCountyLoading(true);
    try {
      const data = await fetchJSON<{ results: ScoredRow[] }>(
        `/api/search?mode=county&q=${encodeURIComponent(q)}&window=${win}`
      );
      if (!data.results.length) setCountyErr(`No county match for “${q}”`);
      setCountyResults(data.results);
    } catch { setCountyErr("Search failed"); }
    finally { setCountyLoading(false); }
  }

  async function runZipSearch(q: string) {
    setZipErr(null); setZipLoading(true);
    try {
      const data = await fetchJSON<{ result: ScoredRow | null }>(
        `/api/search?mode=zip&q=${encodeURIComponent(q)}&window=${win}`
      );
      if (!data.result) setZipErr(`No ZIP data for “${q}”`);
      setZipResult(data.result);
    } catch { setZipErr("Search failed"); }
    finally { setZipLoading(false); }
  }

  const generatedLabel = useMemo(() => {
    try { return new Date(meta.generated_at).toUTCString(); }
    catch { return meta.generated_at; }
  }, [meta.generated_at]);

  return (
    <div className="shell">
      {/* MASTHEAD */}
      <header className="masthead">
        <div className="mast-left">
          <div className="mast-logo">S</div>
          <div>
            <div className="mast-title">
              Silverstone <em>Market Intelligence</em>
            </div>
            <div className="mast-sub">
              Disposition-focused scoring · State · County · ZIP
            </div>
          </div>
        </div>
        <div />
        <div className="mast-meta">
          <div>VOL. 1 · ISSUE {isoWeek()}</div>
          <div>Live blend · Redfin + US Census</div>
          {meta.state_as_of && (
            <div><strong>State / County</strong> · 30-day rolling through {meta.state_as_of}</div>
          )}
          {meta.zip_as_of && (
            <div><strong>ZIP</strong> · 90-day rolling through {meta.zip_as_of}</div>
          )}
          <div><strong>Last refresh</strong> · {generatedLabel}</div>
          <div><strong>Rows</strong> · {meta.row_count.toLocaleString()}</div>
        </div>
      </header>

      {/* SEARCH TRIAD */}
      <section className="section">
        <div className="section-head">
          <span className="section-num">§ 01</span>
          <h2 className="section-title">Geographic lookup</h2>
          <span className="section-note">Type any state, county/city/town, or 5-digit ZIP — the full Redfin dataset for that location appears below.</span>
        </div>
        <div className="search-grid">
          <SearchCard
            title="State"
            hint="Search by full name or two-letter code, e.g. “Virginia” or “VA”."
            value={stateQ}
            onChange={setStateQ}
            onSubmit={() => runStateSearch(stateQ)}
            placeholder="Virginia · VA"
          />
          <SearchCard
            title="County · City · Town"
            hint="Search by county or place name, e.g. “Orange County, CA” or “Fairfax, VA”."
            value={countyQ}
            onChange={setCountyQ}
            onSubmit={() => runCountySearch(countyQ)}
            placeholder="Orange County, CA"
          />
          <SearchCard
            title="ZIP Code"
            hint="Enter any 5-digit ZIP code currently covered by Redfin."
            value={zipQ}
            onChange={setZipQ}
            onSubmit={() => runZipSearch(zipQ)}
            placeholder="10014"
            inputMode="numeric"
          />
        </div>

        {(stateLoading || stateResult || stateErr) && (
          <SingleResult label="State" loading={stateLoading} error={stateErr} row={stateResult} />
        )}
        {(countyLoading || countyResults.length > 0 || countyErr) && (
          <CountyResultList loading={countyLoading} error={countyErr} rows={countyResults} />
        )}
        {(zipLoading || zipResult || zipErr) && (
          <SingleResult label="ZIP" loading={zipLoading} error={zipErr} row={zipResult} />
        )}
      </section>

      {/* (time-window section removed — Redfin only publishes at fixed rolling periods) */}

      {/* TOP 15 BY STATE — ranked by Pending % desc */}
      <section className="section">
        <div className="section-head">
          <span className="section-num">§ 02</span>
          <h2 className="section-title">Top 15 hottest counties by state</h2>
          <span className="section-note">Ranked by Pending %</span>
        </div>
        <div className="state-picker">
          <label htmlFor="top-state">State:</label>
          <select
            id="top-state"
            value={topState}
            onChange={(e) => setTopState(e.target.value)}
          >
            {stateList.map((s) => (
              <option key={s.code} value={s.code}>{s.name} ({s.code})</option>
            ))}
          </select>
        </div>
        {topRows.length ? (
          <TopCountyTable rows={topRows} />
        ) : (
          <div className="empty">No county data for this state in the current window.</div>
        )}
      </section>

      {/* MARKETING SPEND — all 50 states */}
      <section className="section">
        <div className="section-head">
          <span className="section-num">§ 03</span>
          <h2 className="section-title">Marketing spend insights — all states</h2>
          <span className="section-note">Equal-weighted score · Pending % + DOM sub-60 share</span>
        </div>
        <div className="legend">
          <span><strong>Pending %:</strong> pending sales ÷ inventory, 90-day avg (3 monthly snapshots, All Residential)</span>
          <span><strong>Avg DOM:</strong> median days on market, weekly blend</span>
          <span><strong>Homes Sold:</strong> shown for context · not scored</span>
        </div>
        {allStatesRows.length ? (
          <StatesTable rows={allStatesRows} />
        ) : (
          <div className="empty">No data available for the current window.</div>
        )}
      </section>

      <footer className="foot">
        <span>© Silverstone Market Intelligence · Published by Tyler Williams</span>
        <span>Source: {meta.source}</span>
      </footer>
    </div>
  );
}

/* ---------- components ---------- */

function SearchCard({
  title, hint, value, onChange, onSubmit, placeholder, inputMode
}: {
  title: string; hint: string;
  value: string; onChange: (v: string) => void;
  onSubmit: () => void;
  placeholder?: string; inputMode?: "numeric" | "text";
}) {
  return (
    <div className="search-card">
      <h3>{title}</h3>
      <div className="hint">{hint}</div>
      <div className="search-row">
        <input
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") onSubmit(); }}
          placeholder={placeholder}
          inputMode={inputMode}
        />
        <button onClick={onSubmit}>Find</button>
      </div>
    </div>
  );
}

function SingleResult({
  label, loading, error, row
}: {
  label: string; loading: boolean; error: string | null; row: ScoredRow | null;
}) {
  if (loading) return <div className="result"><span className="dots">Searching</span></div>;
  if (error) return <div className="result"><em style={{ color: "var(--bad)" }}>{error}</em></div>;
  if (!row) return null;
  return (
    <div className="result">
      <div className="result-head">
        <div className="result-name">
          {row.name}{row.geo_type === "county" ? `, ${row.state}` : row.geo_type === "zip" ? ` · ${row.state}` : ""}
        </div>
        <div className="result-kind">{label} · As of {row.as_of}</div>
      </div>
      <FullMetricGrid row={row} />
    </div>
  );
}

function CountyResultList({
  loading, error, rows
}: {
  loading: boolean; error: string | null; rows: ScoredRow[];
}) {
  if (loading) return <div className="result"><span className="dots">Searching</span></div>;
  if (error) return <div className="result"><em style={{ color: "var(--bad)" }}>{error}</em></div>;
  if (!rows.length) return null;
  if (rows.length === 1) {
    return (
      <div className="result">
        <div className="result-head">
          <div className="result-name">{rows[0].name}, {rows[0].state}</div>
          <div className="result-kind">County · As of {rows[0].as_of}</div>
        </div>
        <FullMetricGrid row={rows[0]} />
      </div>
    );
  }
  return (
    <div style={{ marginTop: 16 }}>
      <div style={{
        fontFamily: "JetBrains Mono, monospace", fontSize: 10.5,
        color: "var(--ink-3)", letterSpacing: "0.1em",
        textTransform: "uppercase", marginBottom: 8
      }}>
        {rows.length} matches · click a county to see full details
      </div>
      <div className="table-wrap">
        <table className="data">
          <thead>
            <tr>
              <th>County</th><th>State</th>
              <th className="num">Pending %</th>
              <th className="num">Avg DOM</th>
              <th className="num">Population</th>
              <th className="num">Avg Sale Price</th>
              <th className="num">Homes Sold</th>
              <th className="num">Score</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.geo_id}>
                <td className="name-cell">{r.name}</td>
                <td className="state-cell">{r.state}</td>
                <td className="num">{r.pending_pct.toFixed(1)}%</td>
                <td className="num">{r.median_dom.toFixed(0)}d</td>
                <td className="num">{r.population != null ? r.population.toLocaleString() : "—"}</td>
                <td className="num">{r.median_sale_price ? "$" + formatPrice(r.median_sale_price) : "—"}</td>
                <td className="num">{r.homes_sold.toLocaleString()}</td>
                <td className="num"><ScoreBar score={r.marketing_score} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Full details panel shown below any single-match search result. */
function FullMetricGrid({ row }: { row: ScoredRow }) {
  return (
    <div className="metric-grid">
      <Metric primary label="Marketing Score" value={row.marketing_score.toFixed(1)} unit="/100" />
      <Metric
        label="Pending %"
        value={row.pending_pct.toFixed(1)} unit="%"
        tier={row.pending_tier === "Good" ? "good" : "low"}
      />
      <Metric label="Avg DOM" value={row.median_dom.toFixed(0)} unit="d" />
      <Metric label="DOM <60 share" value={(row.dom_sub60_share * 100).toFixed(0)} unit="%" />
      <Metric
        label="Homes Sold"
        value={row.homes_sold.toLocaleString()}
        tier={row.volume_tier === "Good" ? "good" : row.volume_tier === "Medium" ? "medium" : "low"}
      />
      <Metric
        label="Avg Sale Price"
        value={row.median_sale_price ? "$" + formatPrice(row.median_sale_price) : "—"}
      />
      <Metric
        label="Population"
        value={row.population != null ? row.population.toLocaleString() : "—"}
      />
    </div>
  );
}

function Metric({
  label, value, unit, primary, tier
}: {
  label: string; value: string | number; unit?: string;
  primary?: boolean; tier?: "good" | "medium" | "low";
}) {
  return (
    <div className={"metric" + (primary ? " primary" : "")}>
      <span className="label">{label}</span>
      <span className="value">
        {value}{unit ? <span className="unit">{unit}</span> : null}
      </span>
      {tier && (
        <span className={`pill ${tier}`} style={{ marginTop: 4, alignSelf: "flex-start" }}>
          {tier === "good" ? "Good" : tier === "medium" ? "Medium" : "Low"}
        </span>
      )}
    </div>
  );
}

/** Top-15 county table — ranked by Pending % desc, with supporting columns. */
function TopCountyTable({ rows }: { rows: ScoredRow[] }) {
  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            <th className="rank">#</th>
            <th>County</th>
            <th className="num">Pending %</th>
            <th className="num">Avg DOM</th>
            <th className="num">Population</th>
            <th className="num">Avg Sale Price</th>
            <th className="num">Homes Sold</th>
            <th className="num">Score</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={`${r.geo_id}-${r.window}-${i}`}>
              <td className="rank">{(i + 1).toString().padStart(2, "0")}</td>
              <td className="name-cell">{r.name}</td>
              <td className="num">{r.pending_pct.toFixed(1)}%</td>
              <td className="num">{r.median_dom.toFixed(0)}d</td>
              <td className="num">{r.population != null ? r.population.toLocaleString() : "—"}</td>
              <td className="num">{r.median_sale_price ? "$" + formatPrice(r.median_sale_price) : "—"}</td>
              <td className="num">{r.homes_sold.toLocaleString()}</td>
              <td className="num"><ScoreBar score={r.marketing_score} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Marketing-spend table — all 50 states, click any column to sort. */
type StatesSortKey =
  | "pending_pct"
  | "median_dom"
  | "homes_sold"
  | "median_sale_price"
  | "marketing_score"
  | "name";

function StatesTable({ rows }: { rows: ScoredRow[] }) {
  const [sortKey, setSortKey] = useState<StatesSortKey>("marketing_score");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  function toggleSort(k: StatesSortKey) {
    if (k === sortKey) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortKey(k);
      // DOM is "lower = hotter", so default ascending. Name ascending. Others descending.
      setSortDir(k === "median_dom" || k === "name" ? "asc" : "desc");
    }
  }

  const sorted = useMemo(() => {
    const arr = [...rows];
    arr.sort((a, b) => {
      if (sortKey === "name") {
        const av = a.name.toLowerCase();
        const bv = b.name.toLowerCase();
        if (av === bv) return 0;
        const cmp = av < bv ? -1 : 1;
        return sortDir === "asc" ? cmp : -cmp;
      }
      const av = (a[sortKey] ?? 0) as number;
      const bv = (b[sortKey] ?? 0) as number;
      return sortDir === "desc" ? bv - av : av - bv;
    });
    return arr;
  }, [rows, sortKey, sortDir]);

  const arrow = (k: StatesSortKey) =>
    sortKey === k ? (sortDir === "desc" ? " ↓" : " ↑") : "";

  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            <th className="rank">#</th>
            <th className="sortable" onClick={() => toggleSort("name")}>
              State{arrow("name")}
            </th>
            <th className="num sortable" onClick={() => toggleSort("pending_pct")}>
              Pending %{arrow("pending_pct")}
            </th>
            <th className="num sortable" onClick={() => toggleSort("median_dom")}>
              Avg DOM{arrow("median_dom")}
            </th>
            <th className="num sortable" onClick={() => toggleSort("homes_sold")}>
              Homes Sold{arrow("homes_sold")}
            </th>
            <th className="num sortable" onClick={() => toggleSort("median_sale_price")}>
              Avg Sale Price{arrow("median_sale_price")}
            </th>
            <th className="num sortable" onClick={() => toggleSort("marketing_score")}>
              Score{arrow("marketing_score")}
            </th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((r, i) => (
            <tr key={`${r.geo_id}-${r.window}-${i}`}>
              <td className="rank">{(i + 1).toString().padStart(2, "0")}</td>
              <td className="name-cell">
                {r.name} <span className="state-cell">· {r.state}</span>
              </td>
              <td className="num">{r.pending_pct.toFixed(1)}%</td>
              <td className="num">{r.median_dom.toFixed(0)}d</td>
              <td className="num">{r.homes_sold.toLocaleString()}</td>
              <td className="num">{r.median_sale_price ? "$" + formatPrice(r.median_sale_price) : "—"}</td>
              <td className="num"><ScoreBar score={r.marketing_score} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ScoreBar({ score }: { score: number }) {
  const tier = score >= 60 ? "good" : score >= 40 ? "medium" : "";
  return (
    <span className={`score-bar ${tier}`}>
      <span className="track">
        <span className="fill" style={{ width: `${Math.min(100, Math.max(0, score))}%` }} />
      </span>
      {score.toFixed(1)}
    </span>
  );
}

function formatPrice(n: number) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(0) + "k";
  return n.toLocaleString();
}

function isoWeek() {
  const d = new Date();
  const onejan = new Date(d.getFullYear(), 0, 1);
  const week = Math.ceil(
    ((d.getTime() - onejan.getTime()) / 86400000 + onejan.getDay() + 1) / 7
  );
  return `W${week.toString().padStart(2, "0")}`;
}
