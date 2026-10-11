import type { WorkQueueMetadataResult } from "@t3tools/contracts";
import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../../components/ui/button";

export type WorkQueueMetadataLoader = (signal: AbortSignal) => Promise<WorkQueueMetadataResult>;
const timeFormatter = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "numeric",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
  timeZone: "America/New_York",
  timeZoneName: "short",
});
const formatTime = (time: number) => timeFormatter.format(time);
const unavailableMessages = {
  source_unavailable: "the source cannot be read",
  invalid_artifact: "the source data is invalid",
  source_mismatch: "the sample does not match the configured source",
  future_sample: "the sample timestamp is in the future",
  invalid_configuration: "the source configuration is invalid",
};
const bindingLabels: Record<string, string> = {
  owner_id: "Owner ID",
  server_generation: "Server generation",
  registry_version: "Registry version",
  membership_id: "Membership ID",
  native_reference_id: "Native reference ID",
  source_instance_id: "Source instance ID",
  native_thread_id: "Native thread ID",
  authority_namespace: "Authority namespace",
  store_generation: "Store generation",
  expires_at: "Binding expires at",
};

export function WorkQueueMetadataPanel({ load }: { load: WorkQueueMetadataLoader }) {
  const [result, setResult] = useState<WorkQueueMetadataResult | null>(null);
  const [failed, setFailed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [observedAt, setObservedAt] = useState(Date.now);
  const active = useRef<AbortController | null>(null);
  const refresh = useCallback(async () => {
    active.current?.abort();
    const controller = new AbortController();
    active.current = controller;
    setLoading(true);
    setFailed(false);
    setResult(null);
    try {
      const next = await load(controller.signal);
      if (!controller.signal.aborted) {
        setObservedAt(Date.now());
        setResult(next);
      }
    } catch {
      if (!controller.signal.aborted) setFailed(true);
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [load]);
  useEffect(() => {
    void refresh();
    return () => {
      active.current?.abort();
    };
  }, [refresh]);
  useEffect(() => {
    if (!result || !("snapshot" in result) || result.expires_at_ms <= observedAt) return;
    const remaining = result.expires_at_ms - Date.now();
    const timer = window.setTimeout(
      () => setObservedAt(Date.now()),
      Math.max(0, Math.min(remaining, 2_147_483_647)),
    );
    return () => window.clearTimeout(timer);
  }, [result, observedAt]);
  const snapshot = result && "snapshot" in result ? result.snapshot : null;
  const stale =
    result?.status === "stale" ||
    (result !== null && "snapshot" in result && result.expires_at_ms <= observedAt);
  return (
    <section aria-label="Queue metadata" className="space-y-4">
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="font-medium">Work queue</h2>
          <p className="text-sm text-muted-foreground">Read-only request status.</p>
        </div>
        <Button variant="outline" size="compact" disabled={loading} onClick={() => void refresh()}>
          Refresh metadata
        </Button>
      </div>
      <div role="status" className="text-sm text-muted-foreground">
        {loading && <p>Loading queue metadata…</p>}
        {failed && <p>Queue metadata unavailable. Refresh to try again.</p>}
        {result?.status === "unconfigured" && <p>Queue metadata is not configured.</p>}
        {result?.status === "unavailable" && (
          <p>Queue metadata unavailable: {unavailableMessages[result.reason]}.</p>
        )}
        {snapshot && (
          <p>
            {stale
              ? "Stale sample"
              : result?.status === "partial"
                ? "Partial sample"
                : "Ready sample"}
            {stale || snapshot.coverage === "partial" ? " · Current queue may differ." : null}
          </p>
        )}
      </div>
      {snapshot &&
        (snapshot.items.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            {snapshot.coverage === "partial" || result?.status === "partial" || stale
              ? "No rows in this sample; the current queue may contain work."
              : "No submitted work in this sample."}
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr>
                  {["Request / workstream", "Target", "Queue state", "Handoff"].map((heading) => (
                    <th key={heading} className="border-b p-3 font-medium">
                      {heading}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {snapshot.items.map((item) => (
                  <tr key={item.request_id} className="align-top">
                    <td className="max-w-64 break-words border-b p-3 [overflow-wrap:anywhere]">
                      <div className="font-medium">{item.workstream_id}</div>
                      <div className="text-xs text-muted-foreground">{item.request_id}</div>
                      <details className="mt-2 text-xs text-muted-foreground">
                        <summary className="cursor-pointer">Request details</summary>
                        <div className="mt-2 space-y-1">
                          <div>
                            Submitted:{" "}
                            {item.submitted_at_ms === null
                              ? "Not available"
                              : formatTime(item.submitted_at_ms)}
                          </div>
                          <div>
                            Lane: {item.lane} · {item.entry_kind} · {item.request_kind}
                          </div>
                          {item.canonical_binding ? (
                            <>
                              <div>Canonical binding verified at sample</div>
                              {Object.entries(item.canonical_binding).map(([key, value]) => (
                                <div key={key}>
                                  {bindingLabels[key]}: {value}
                                </div>
                              ))}
                            </>
                          ) : (
                            <>
                              <div>Legacy / unverified identity</div>
                              <div>No canonical binding available</div>
                            </>
                          )}
                          <div>Dispatch: {item.dispatch_status ?? "Not observed"}</div>
                          <div>Native command: {item.native_command_status ?? "Not observed"}</div>
                          <div>Completion: Not tracked</div>
                        </div>
                      </details>
                    </td>
                    <td className="max-w-64 break-words border-b p-3 [overflow-wrap:anywhere]">
                      {item.target ? (
                        <>
                          <div className="break-all">{item.target.thread_id}</div>
                          <div className="text-xs text-muted-foreground">
                            {item.target.host_id} · {item.target.environment_ref}
                          </div>
                        </>
                      ) : (
                        "Not available"
                      )}
                    </td>
                    <td className="max-w-64 break-words border-b p-3 [overflow-wrap:anywhere]">
                      {item.queue_state}
                    </td>
                    <td className="max-w-48 border-b p-3 text-muted-foreground">
                      Handoff: unconfirmed in this sample
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ))}
      <details className="text-xs text-muted-foreground">
        <summary className="cursor-pointer">About this queue sample</summary>
        <div className="mt-2 space-y-2 [overflow-wrap:anywhere]">
          <p>Requests from all supported producers. Prompt text and editing are unavailable.</p>
          <p>
            Accepted dispatch or native command status does not prove a handoff or completed work.
            Historical, held and uncertain states remain here until handoff evidence is available.
          </p>
          {snapshot ? (
            <>
              <p>
                Coverage: {snapshot.coverage} · Sampled {formatTime(snapshot.observed_at_ms)}
              </p>
              <p>
                Source: {snapshot.source.queue_id} · Host: {snapshot.source.host_id} · Environment:{" "}
                {snapshot.source.environment_ref} · Exporter: {snapshot.source.exporter_instance_id}
              </p>
            </>
          ) : null}
        </div>
      </details>
    </section>
  );
}
