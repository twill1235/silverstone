# Silverstone Market Intelligence

Disposition-focused market scoring for state, county, and ZIP targeting. Live blend of Redfin Data Center (weekly CSVs) + US Census ACS 5yr.

## What it does

- **Search** any state, county/city/town, or 5-digit ZIP
- **Top 15 Hottest Counties by State** ranked by composite marketing score
- **Time-window filter** — 30 / 90 / 180 / 365 days
- **Marketing Spend Insights** table with tiering:
  - Pending % ≥ 25% → Good
  - Transaction Volume ≥ 15,000 → Good · 10,000–15,000 → Medium
  - DOM sub-60 share contributes to composite score
- **Equal-weighted** composite score across Pending %, DOM sub-60 share, Transaction Volume (homes sold).

## How refresh works

Every Thursday at 09:00 UTC, Vercel Cron hits `/api/refresh`, which:

1. Downloads the latest Redfin weekly TSVs (state / county / zip). Redfin publishes new weekly data every **Wednesday**, so Thursday gives a safe buffer.
2. Aggregates per geo + time-window.
3. Enriches with Census population.
4. Writes `data/dataset.json`.
5. On next request, Next.js reads the new file.

Configured in `vercel.json`:
```json
{ "crons": [{ "path": "/api/refresh", "schedule": "0 9 * * 4" }] }
```

## Deploy to Vercel

1. Push this folder to a GitHub repo (public or private, doesn't matter).
2. Go to vercel.com → **Add New… → Project** → import the repo.
3. Framework preset: **Next.js** (auto-detected).
4. In **Settings → Environment Variables**, add:
   - `CRON_SECRET` = any long random string. Required so only Vercel Cron can hit the refresh endpoint.
   - `GITHUB_TOKEN` = a fine-grained personal access token with **Contents: read/write** on this repo only. Generate at github.com/settings/tokens?type=beta.
   - `GITHUB_REPO` = `owner/repo` format, e.g. `yourname/silverstone`.
   - `GITHUB_BRANCH` = optional, defaults to `main`.
5. Deploy.

## How the weekly refresh actually works

Serverless functions on Vercel have a read-only filesystem — you can't just overwrite `data/dataset.json` at runtime. So the refresh flow is:

1. Every Thursday 09:00 UTC, Vercel Cron → `GET /api/refresh` (with the bearer secret).
2. The route pulls fresh Redfin TSVs + Census population in-process.
3. It commits the new `data/dataset.json` to your GitHub repo via the GitHub Contents API.
4. That commit triggers a fresh Vercel deploy.
5. The new deploy serves the updated dataset.

Why this design: it gives you an actual version history of the dataset in Git (one commit per week), zero extra infra, zero cost beyond Vercel + Census (both free).

## Manual refresh

Locally (writes to your working `data/dataset.json`):

```bash
npm run refresh-data
```

In production (triggers a GitHub commit → deploy):

```bash
curl -H "Authorization: Bearer $CRON_SECRET" \
  https://YOUR-DOMAIN.vercel.app/api/refresh
```

## Stack

- Next.js 14 (App Router)
- TypeScript
- Zero external UI libs (pure CSS)
- Data persisted as committed `data/dataset.json`

## File map

```
app/
  layout.tsx           root
  page.tsx             server entry, loads dataset meta
  globals.css          editorial styles
  api/
    refresh/route.ts   cron endpoint (secret-guarded)
    search/route.ts    search + rankings + meta
components/
  DashboardClient.tsx  entire interactive UI
lib/
  scoring.ts           equal-weight composite score
  data.ts              dataset loader + query helpers
scripts/
  refresh-data.mjs     pulls Redfin + Census, writes data/dataset.json
  seed.mjs             generates placeholder dataset (already run)
data/
  dataset.json         committed, refreshed weekly
vercel.json            cron schedule
```

## Notes

- If you ever want to swap the weighting, edit `scoreRow()` in `lib/scoring.ts`.
- Redfin's TSV schema is stable but if they ever change column names, update `aggregateForGeo()` in `scripts/refresh-data.mjs`.
- The dataset committed here is a seed; first cron run replaces it with real Redfin + Census data.
