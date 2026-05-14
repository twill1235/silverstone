// app/api/refresh/route.ts
import { NextResponse } from "next/server";
import { buildDataset, probeLastModified } from "@/lib/refresh";

export const runtime = "nodejs";
export const maxDuration = 300;

/**
 * Refresh handler.
 *
 * Behavior:
 *   1. HEAD each upstream CSV and find the newest Last-Modified across them.
 *   2. Compare against the last successful build (stored as a sidecar file in
 *      the repo at data/.last-upstream). If unchanged AND ?force=1 is not set,
 *      skip rebuild and return early.
 *   3. Otherwise rebuild and commit to GitHub.
 *
 * Why this matters:
 *   The cron runs daily but Redfin republishes the monthlies roughly once a
 *   month. Most daily runs short-circuit at step 2, costing only the HEAD
 *   requests (~milliseconds) and producing no GitHub commit noise.
 */
export async function GET(request: Request) {
  const auth = request.headers.get("authorization");
  const expected = process.env.CRON_SECRET;
  if (expected && auth !== `Bearer ${expected}`) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }

  const url = new URL(request.url);
  const force = url.searchParams.get("force") === "1";

  try {
    // Step 1: probe Last-Modified.
    const probe = await probeLastModified();
    const upstreamTag = probe.newestLastModified;

    // Step 2: skip if unchanged.
    if (!force) {
      const lastSeen = await readLastSeenTag();
      if (upstreamTag && lastSeen && lastSeen === upstreamTag) {
        return NextResponse.json({
          ok: true,
          skipped: true,
          reason: "upstream unchanged since last refresh",
          upstream_last_modified: upstreamTag,
          per_file: probe.perFile
        });
      }
    }

    // Step 3: rebuild + commit.
    const dataset = await buildDataset();
    const payload = JSON.stringify(dataset);
    const committed = await commitToGitHub(payload, upstreamTag ?? "unknown");
    return NextResponse.json({
      ok: true,
      skipped: false,
      rows: dataset.rows.length,
      generated_at: dataset.generated_at,
      upstream_last_modified: upstreamTag,
      per_file: probe.perFile,
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

/**
 * Read the last-seen upstream Last-Modified tag we recorded. Kept as a tiny
 * sidecar file in the repo so it's durable across cold starts without
 * requiring external storage.
 */
async function readLastSeenTag(): Promise<string | null> {
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPO;
  const branch = process.env.GITHUB_BRANCH ?? "main";
  if (!token || !repo) return null;

  const path = "data/.last-upstream";
  const apiBase = `https://api.github.com/repos/${repo}/contents/${path}?ref=${branch}`;
  try {
    const res = await fetch(apiBase, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "silverstone-refresh"
      }
    });
    if (!res.ok) return null;
    const j = (await res.json()) as { content?: string };
    if (!j.content) return null;
    const decoded = Buffer.from(j.content, "base64").toString("utf-8").trim();
    return decoded || null;
  } catch {
    return null;
  }
}

async function commitToGitHub(
  fileContent: string,
  upstreamTag: string
): Promise<{ sha: string; url: string } | null> {
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPO;
  const branch = process.env.GITHUB_BRANCH ?? "main";
  if (!token || !repo) return null;

  // First, the dataset itself.
  const datasetResult = await putFile({
    token,
    repo,
    branch,
    path: "data/dataset.json",
    content: fileContent,
    message: `chore(data): refresh ${new Date().toISOString().slice(0, 10)} (upstream ${upstreamTag})`
  });

  // Then, the sidecar tag (best-effort; don't fail the whole refresh if this fails).
  try {
    await putFile({
      token,
      repo,
      branch,
      path: "data/.last-upstream",
      content: upstreamTag,
      message: `chore(data): record upstream tag ${upstreamTag}`
    });
  } catch (e) {
    console.warn("sidecar tag commit failed:", (e as Error).message);
  }

  return datasetResult;
}

async function putFile(args: {
  token: string;
  repo: string;
  branch: string;
  path: string;
  content: string;
  message: string;
}): Promise<{ sha: string; url: string } | null> {
  const { token, repo, branch, path, content, message } = args;
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
    message,
    content: Buffer.from(content, "utf-8").toString("base64"),
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
    throw new Error(`GitHub commit failed (${path}): ${put.status} ${text}`);
  }

  const json = (await put.json()) as { content?: { sha: string; html_url: string } };
  return json.content
    ? { sha: json.content.sha, url: json.content.html_url }
    : null;
}
