// app/page.tsx
import DashboardClient from "@/components/DashboardClient";
import { loadDataset } from "@/lib/data";

export const dynamic = "force-dynamic";

export default function Page() {
  const ds = loadDataset();
  return (
    <DashboardClient
      meta={{
        generated_at: ds.generated_at,
        source: ds.source,
        row_count: ds.rows.length
      }}
    />
  );
}
