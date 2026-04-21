// app/page.tsx
import DashboardClient from "@/components/DashboardClient";
import { loadDataset } from "@/lib/data";

export const dynamic = "force-dynamic";

export default function Page() {
  const ds = loadDataset();
  let stateAsOf = "";
  let countyAsOf = "";
  let zipAsOf = "";
  for (const r of ds.rows) {
    if (r.geo_type === "state" && r.as_of > stateAsOf) stateAsOf = r.as_of;
    else if (r.geo_type === "county" && r.as_of > countyAsOf) countyAsOf = r.as_of;
    else if (r.geo_type === "zip" && r.as_of > zipAsOf) zipAsOf = r.as_of;
  }
  return (
    <DashboardClient
      meta={{
        generated_at: ds.generated_at,
        source: ds.source,
        row_count: ds.rows.length,
        state_as_of: stateAsOf || null,
        county_as_of: countyAsOf || null,
        zip_as_of: zipAsOf || null
      }}
    />
  );
}
