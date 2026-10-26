import type { WorkstreamsRollupRead } from "@t3tools/contracts/jones/workstreamsRollups";
import * as Schema from "effect/Schema";
import { WorkstreamsRollupRead as ReadSchema } from "@t3tools/contracts/jones/workstreamsRollups";
const isRead = Schema.is(ReadSchema);
export function WorkstreamSummaryPanel({
  workstreamId,
  read,
  loading = false,
}: {
  workstreamId: string;
  read: typeof WorkstreamsRollupRead.Type | null;
  loading?: boolean;
}) {
  if (loading)
    return (
      <p role="status" className="text-sm text-muted-foreground">
        Loading summary…
      </p>
    );
  if (
    !read ||
    !isRead(read) ||
    read.state !== "ready" ||
    !read.summary ||
    read.summary.workstream_id !== workstreamId
  )
    return (
      <p role="status" className="text-sm text-muted-foreground">
        Summary unavailable.
      </p>
    );
  const { summary, freshness } = read;
  if (freshness === "inaccessible")
    return (
      <p role="status" className="text-sm text-muted-foreground">
        Summary unavailable.
      </p>
    );
  const sections = [
    { title: "Blockers", items: summary.result.blockers },
    { title: "Decisions", items: summary.result.decisions },
    { title: "Next steps", items: summary.result.next_steps },
    { title: "Follow-ups", items: summary.result.follow_up_candidates },
  ];
  const generated = new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "America/New_York",
  }).format(new Date(summary.generated_at));
  const zone = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    timeZoneName: "short",
  })
    .formatToParts(new Date(summary.generated_at))
    .find((p) => p.type === "timeZoneName")?.value;
  return (
    <section aria-label="Workstream summary" className="space-y-3 text-sm">
      <div className="flex items-center justify-between gap-3">
        <h3 className="font-medium">Summary</h3>
        <span className="text-xs text-muted-foreground">{summary.result.semantic_status}</span>
      </div>
      {freshness !== "current" ? (
        <p role="status" className="text-muted-foreground">
          {freshness === "stale"
            ? "This summary may be out of date."
            : "Summary freshness is unknown."}
        </p>
      ) : null}
      <p className="whitespace-pre-wrap break-words">{summary.result.outcome.text}</p>
      {sections
        .filter((s) => s.items.length > 0)
        .map((s) => (
          <div key={s.title} className="space-y-1">
            <h4 className="font-medium">{s.title}</h4>
            <ul className="list-disc space-y-1 pl-5">
              {s.items.map((item) => (
                <li key={JSON.stringify(item)} className="whitespace-pre-wrap break-words">
                  {item.text}
                </li>
              ))}
            </ul>
          </div>
        ))}
      <p className="text-xs text-muted-foreground">
        Updated {generated} {zone}
      </p>
    </section>
  );
}
