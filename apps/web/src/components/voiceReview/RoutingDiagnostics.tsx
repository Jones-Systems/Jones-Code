import type { VoiceReviewDiagnostics, VoiceReviewDraft } from "@t3tools/contracts";
import { useEffect, useState } from "react";
import { Button } from "../ui/button";

export function RoutingDiagnostics({
  drafts,
  fetchDiagnostics,
}: {
  drafts: readonly VoiceReviewDraft[];
  fetchDiagnostics: (id: string) => Promise<VoiceReviewDiagnostics>;
}) {
  const [selected, setSelected] = useState("");
  const [data, setData] = useState<VoiceReviewDiagnostics | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const id = drafts.some((draft) => draft.id === selected) ? selected : (drafts[0]?.id ?? "");
  useEffect(() => {
    let active = true;
    setData(null);
    setError(false);
    if (!id) return;
    setLoading(true);
    void fetchDiagnostics(id)
      .then(
        (value) => {
          if (active) setData(value);
        },
        () => {
          if (active) setError(true);
        },
      )
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [id, fetchDiagnostics, refresh]);
  return (
    <section className="flex flex-col gap-4" aria-label="Routing diagnostics">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-sm">
          Prompt{" "}
          <select
            aria-label="Routing prompt"
            value={id}
            onChange={(event) => setSelected(event.target.value)}
            className="rounded-md border bg-background p-2"
          >
            {drafts.map((draft) => (
              <option key={draft.id} value={draft.id}>
                {draft.id} · {draft.state}
              </option>
            ))}
          </select>
        </label>
        <Button
          variant="outline"
          size="compact"
          disabled={!id || loading}
          onClick={() => setRefresh((value) => value + 1)}
        >
          Refresh diagnostics
        </Button>
      </div>
      <p className="text-sm text-muted-foreground">
        Routing jobs are temporary inference tasks. They do not have an interactive chat.
      </p>
      <div className="flex flex-wrap gap-2">
        <Button variant="outline" size="compact" disabled>
          Pause classifier
        </Button>
        <Button variant="outline" size="compact" disabled>
          Re-run held prompt
        </Button>
        <Button variant="outline" size="compact" disabled>
          Refresh thread context now
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        Classifier controls are unavailable through this environment’s review connection.
      </p>
      {!id ? <p className="text-sm">No prompt available for routing diagnostics.</p> : null}
      {loading ? <p role="status">Loading routing diagnostics…</p> : null}
      {error ? (
        <p role="alert">Routing diagnostics unavailable. Refresh to try reading them again.</p>
      ) : null}
      {data ? (
        <>
          <p className="text-sm">
            Routing: {data.routing_state} · prompt revision {data.draft_revision}
          </p>
          {data.unavailable.length > 0 ? (
            <p className="text-sm">Unavailable: {data.unavailable.join(", ")}</p>
          ) : null}
          {data.jobs.length === 0 ? (
            <p className="text-sm text-muted-foreground">No routing jobs observed.</p>
          ) : null}
          {data.jobs.map((job) => (
            <article key={job.job_id} className="flex flex-col gap-2 rounded-lg border p-4 text-sm">
              <h3 className="font-medium">
                {job.job_id} · {job.state}
              </h3>
              <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
                <dt>Requested model</dt>
                <dd>
                  {job.requested_model ?? "Unavailable"} ·{" "}
                  {job.requested_effort ?? "effort unavailable"}
                </dd>
                <dt>Observed model</dt>
                <dd>
                  {job.observed_model ?? "Unavailable"} ·{" "}
                  {job.observed_effort ?? "effort unavailable"}
                </dd>
                <dt>Validation</dt>
                <dd>{job.validation_outcome ?? "Not observed"}</dd>
                <dt>Void reason</dt>
                <dd>{job.void_reason ?? "None reported"}</dd>
                <dt>Input digest</dt>
                <dd className="break-all">{job.input_digest}</dd>
              </dl>
              <details>
                <summary>Context coverage and provenance</summary>
                <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs">
                  {JSON.stringify(job.manifest, null, 2)}
                </pre>
              </details>
              <details>
                <summary>Reported usage</summary>
                <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs">
                  {JSON.stringify(job.usage, null, 2)}
                </pre>
              </details>
            </article>
          ))}
        </>
      ) : null}
    </section>
  );
}
