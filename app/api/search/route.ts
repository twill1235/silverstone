// app/api/refresh/route.ts
// Triggered by Vercel Cron (Thursdays 09:00 UTC, see vercel.json) and
// manually via `?force=1`. Compares Redfin's S3 Last-Modified against
// what's already committed in data/dataset.json and short-circuits when
// nothing has changed upstream.

import { NextResponse } from "next/server";
import {
  buildDataset,
  fetchAllSourceLastModified,
  SOURCES,
  type SourceMeta
} from "@/lib/refresh";

export const runtime = "nodejs";
export const maxDuration = 300;

interface CommittedFile { sha: string; url: string }

export async function GET(request: Request) {
  const auth = request.headers.get("authorization");
  const expected = process.env.CRON_SECRET;
  if (expected && auth !== `Bearer ${expected}`) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }

  const url = new URL(request.url);
  const force = url.searchParams.get("force") === "1";

  try {
    const upstream = await fetchAllSourceLastModified();
    let precheckSkipped = false;
    let previousMeta: SourceMeta | null = null;

    if (!force) {
      previousMeta = await readCommittedSourceMeta();
      if (previousMeta && metaMatches(previousMeta, upstream)) {
        precheckSkipped = true;
      }
    }

    if (precheckSkipped) {
      return NextResponse.json({
        ok: true,
        skipped: true,
        reason: "no upstream change since last commit",
        per_file: upstream,
        upstream_last_modified: maxLastMod(upstream),
        previous_source_last_modified: previousMeta,
        committed: null
      });
    }

    const { dataset, source_last_modified, upstream_last_modified } =
      await buildDataset();
    const payload = JSON.stringify(dataset);
    const committed = await commitToGitHub(payload);

    return NextResponse.json({
      ok: true,
      skipped: false,
      forced: force,
      rows: dataset.rows.length,
      generated_at: dataset.generated_at,
      per_file: source_last_modified,
      upstream_last_modified,
      committed
    });
  } catch (e: unknown) {
    const err = e as Error;
    return NextResponse.json(
      { ok: false, error: err.message ?? "refresh failed" },
      { status: 500 }
    );
  }
}

function maxLastMod(meta: SourceMeta): string | null {
  const ts = [meta.state, meta.county, meta.zip]
    .filter((v): v is string => !!v)
    .map((s) => ({ raw: s, ms: Date.parse(s) }))
    .filter((x) => Number.isFinite(x.ms))
    .sort((a, b) => b.ms - a.ms);
  return ts[0]?.raw ?? null;
}

function metaMatches(a: SourceMeta, b: SourceMeta): boolean {
  return a.state === b.state && a.county === b.county && a.zip === b.zip;
}

async function readCommittedSourceMeta(): Promise<SourceMeta | null> {
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPO;
  const branch = process.env.GITHUB_BRANCH ?? "main";
  if (!token || !repo) return null;

  try {
    const res = await fetch(
      `https://raw.githubusercontent.com/${repo}/${branch}/data/dataset.json`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "User-Agent": "silverstone-refresh"
        },
        cache: "no-store"
      }
    );
    if (!res.ok) return null;

    const reader = res.body?.getReader();
    if (!reader) return null;
    const decoder = new TextDecoder("utf-8");
    let buf = "";
    const cap = 8192;
    while (buf.length < cap) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const m = buf.match(/"source_last_modified"\s*:\s*\{[^}]*\}/);
      if (m) {
        try {
          const parsed = JSON.parse(`{${m[0]}}`) as {
            source_last_modified: SourceMeta;
          };
          try { reader.cancel(); } catch {}
          return parsed.source_last_modified;
        } catch {
          break;
        }
      }
    }
    try { reader.cancel(); } catch {}
    return null;
  } catch {
    return null;
  }
}

async function commitToGitHub(
  fileContent: string
): Promise<CommittedFile | null> {
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPO;
  const branch = process.env.GITHUB_BRANCH ?? "main";
  if (!token || !repo) {
    console.warn("[commit] GITHUB_TOKEN/GITHUB_REPO not set - skipping");
    return null;
  }

  const path = "data/dataset.json";
  const apiBase = `https://api.github.com/repos/${repo}/contents/${path}`;

  let currentSha: string | undefined;
  const head = await fetch(`${apiBase}?ref=${branch}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "silverstone-refresh"
    }
  });
  if (head.ok) {
    const j = (await head.json()) as { sha?: string };
    currentSha = j.sha;
  }

  const body = {
    message: `chore(data): refresh ${new Date().toISOString().slice(0, 10)}`,
    content: Buffer.from(fileContent, "utf-8").toString("base64"),
    branch,
    ...(currentSha ? { sha: currentSha } : {})
  };

  const put = await fetch(apiBase, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "silverstone-refresh"
    },
    body: JSON.stringify(body)
  });

  if (!put.ok) {
    const text = await put.text();
    throw new Error(`GitHub commit failed: ${put.status} ${text}`);
  }

  const json = (await put.json()) as {
    content?: { sha: string; html_url: string };
  };
  return json.content
    ? { sha: json.content.sha, url: json.content.html_url }
    : null;
}

void SOURCES;
