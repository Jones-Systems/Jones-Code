import type { VoiceReviewDiagnostics } from "@t3tools/contracts";
import { useEffect, useState } from "react";
import { Button } from "../ui/button";

/** Read-only routing diagnostics for one draft, loaded when its disclosure opens. */
export function RoutingDiagnostics({
  draftId,
  fetchDiagnostics,
}: {
  draftId: string;
  fetchDiagnostics: (id: string) => Promise<VoiceReviewDiagnostics>;
}) {
  const [data, setData] = useState<VoiceReviewDiagnostics | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);

  const [request, setRequest] = useState({ id: draftId, refresh, fetchDiagnostics });
  if (
    request.id !== draftId ||
    request.refresh !== refresh ||
    request.fetchDiagnostics !== fetchDiagnostics
  ) {
    setRequest({ id: draftId, refresh, fetchDiagnostics });
    setData(null);
    setError(false);
    setLoading(true);
  }
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Load routing diagnostics for this request and ignore responses after its viewer retires.
  useEffect(() => {
    let active = true;
    void request
      .fetchDiagnostics(request.id)
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
  }, [request]);
  return (
    <section className="flex min-w-0 flex-col gap-2" aria-label="Routing diagnostics">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p>Routing jobs are temporary inference tasks. They do not have an interactive chat.</p>
        <Button
          variant="outline"
          size="compact"
          disabled={loading}
          onClick={() => setRefresh((value) => value + 1)}
        >
          Refresh diagnostics
        </Button>
      </div>
      {loading ? <p role="status">Loading routing diagnostics…</p> : null}
      {error ? (
        <p role="alert">Routing diagnostics unavailable. Refresh to try reading them again.</p>
      ) : null}
      {data ? (
        <>
          <p>
            Routing: {data.routing_state} · prompt revision {data.draft_revision}
          </p>
          {data.unavailable.length > 0 ? <p>Unavailable: {data.unavailable.join(", ")}</p> : null}
          {data.jobs.length === 0 ? <p>No routing jobs observed.</p> : null}
          {data.jobs.map((job) => (
            <article key={job.job_id} className="flex min-w-0 flex-col gap-2 rounded-md border p-3">
              <h4 className="font-medium wrap-anywhere">
                {job.job_id} · {job.state}
              </h4>
              <dl className="grid min-w-0 grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
                <dt>Requested model</dt>
                <dd className="wrap-anywhere">
                  {job.requested_model ?? "Unavailable"} ·{" "}
                  {job.requested_effort ?? "effort unavailable"}
                </dd>
                <dt>Observed model</dt>
                <dd className="wrap-anywhere">
                  {job.observed_model ?? "Unavailable"} ·{" "}
                  {job.observed_effort ?? "effort unavailable"}
                </dd>
                <dt>Validation</dt>
                <dd className="wrap-anywhere">{job.validation_outcome ?? "Not observed"}</dd>
                <dt>Void reason</dt>
                <dd className="wrap-anywhere">{job.void_reason ?? "None reported"}</dd>
                <dt>Input digest</dt>
                <dd className="break-all">{job.input_digest}</dd>
              </dl>
              <details>
                <summary className="cursor-pointer">Context coverage and provenance</summary>
                <pre className="whitespace-pre-wrap wrap-anywhere">
                  {JSON.stringify(job.manifest, null, 2)}
                </pre>
              </details>
              <details>
                <summary className="cursor-pointer">Reported usage</summary>
                <pre className="whitespace-pre-wrap wrap-anywhere">
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
