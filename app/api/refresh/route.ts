// app/api/refresh/route.ts
import { NextResponse } from "next/server";
import { buildDataset } from "@/lib/refresh";

export const runtime = "nodejs";
export const maxDuration = 300;

export async function GET(request: Request) {
  const auth = request.headers.get("authorization");
  const expected = process.env.CRON_SECRET;
  if (expected && auth !== `Bearer ${expected}`) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }

  try {
    const dataset = await buildDataset();
    const payload = JSON.stringify(dataset);
    const committed = await commitToGitHub(payload);
    return NextResponse.json({
      ok: true,
      rows: dataset.rows.length,
      generated_at: dataset.generated_at,
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

async function commitToGitHub(
  fileContent: string
): Promise<{ sha: string; url: string } | null> {
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPO;
  const branch = process.env.GITHUB_BRANCH ?? "main";
  if (!token || !repo) return null;

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
    message: `chore(data): weekly refresh ${new Date().toISOString().slice(0, 10)}`,
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

  const json = (await put.json()) as { content?: { sha: string; html_url: string } };
  return json.content
    ? { sha: json.content.sha, url: json.content.html_url }
    : null;
}
