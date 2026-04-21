// app/api/search/route.ts
import { NextResponse } from "next/server";
import {
  findCounty,
  findState,
  findZip,
  listStates,
  topCountiesByState,
  allStates,
  loadDataset
} from "@/lib/data";
import type { TimeWindow } from "@/lib/scoring";

export const runtime = "nodejs";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const mode = searchParams.get("mode") ?? "";
  const q = searchParams.get("q") ?? "";
  const win = (searchParams.get("window") as TimeWindow) ?? "90d";

  switch (mode) {
    case "state": {
      return NextResponse.json({ result: findState(q, win) });
    }
    case "county": {
      return NextResponse.json({ results: findCounty(q, win) });
    }
    case "zip": {
      return NextResponse.json({ result: findZip(q, win) });
    }
    case "topByState": {
      return NextResponse.json({
        results: topCountiesByState(q, win, 15)
      });
    }
    case "allStates": {
      return NextResponse.json({ results: allStates(win) });
    }
    case "listStates": {
      return NextResponse.json({ results: listStates(win) });
    }
    case "meta": {
      const ds = loadDataset();
      // Newest Redfin as_of dates per geo type (tells the user how fresh the data is).
      let stateAsOf = "";
      let countyAsOf = "";
      let zipAsOf = "";
      for (const r of ds.rows) {
        if (r.geo_type === "state" && r.as_of > stateAsOf) stateAsOf = r.as_of;
        else if (r.geo_type === "county" && r.as_of > countyAsOf) countyAsOf = r.as_of;
        else if (r.geo_type === "zip" && r.as_of > zipAsOf) zipAsOf = r.as_of;
      }
      return NextResponse.json({
        generated_at: ds.generated_at,
        source: ds.source,
        row_count: ds.rows.length,
        state_as_of: stateAsOf || null,
        county_as_of: countyAsOf || null,
        zip_as_of: zipAsOf || null
      });
    }
    default:
      return NextResponse.json({ error: "unknown mode" }, { status: 400 });
  }
}
