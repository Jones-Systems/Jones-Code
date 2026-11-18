import type { WorkQueueMetadataResult } from "@t3tools/contracts";
import { useEffect, useEffectEvent, useRef, useState, type ReactNode } from "react";
import { Button } from "../../components/ui/button";
import { queueUnavailableMessage } from "./promptPresentation";
import { usePromptTime } from "./PromptRow";

export type WorkQueueMetadataLoader = (signal: AbortSignal) => Promise<WorkQueueMetadataResult>;

/**
 * Loads the read-only queue sample for one environment and states how current it is. A manual
 * refresh keeps the previous sample on screen, marked as from an earlier read, instead of
 * clearing it. Queue items render as Prompts rows supplied by the caller.
 */
export function WorkQueueMetadataPanel({
  load,
  onResult,
  onFailedChange,
  children,
}: {
  load: WorkQueueMetadataLoader;
  onResult?: (result: WorkQueueMetadataResult | null) => void;
  onFailedChange?: (failed: boolean) => void;
  children?: ReactNode;
}) {
  const formatTime = usePromptTime();
  const [result, setResult] = useState<WorkQueueMetadataResult | null>(null);
  const [failed, setFailed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [observedAt, setObservedAt] = useState(Date.now);
  const active = useRef<AbortController | null>(null);
  const refresh = async (read: WorkQueueMetadataLoader) => {
    active.current?.abort();
    const controller = new AbortController();
    active.current = controller;
    return await (async () => {
      try {
        const next = await read(controller.signal);
        if (!controller.signal.aborted) {
          const transient = next.status === "unavailable" && next.reason === "source_unavailable";
          setObservedAt(Date.now());
          setResult((previous) =>
            transient && previous && "snapshot" in previous ? previous : next,
          );
          setFailed(transient);
        }
      } catch {
        if (!controller.signal.aborted) setFailed(true);
      }
    })().finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
  };
  const refreshOnMount = useEffectEvent(refresh);
  const [previousLoad, setPreviousLoad] = useState(() => load);
  if (previousLoad !== load) {
    setPreviousLoad(() => load);
    setLoading(true);
    setFailed(false);
    setResult(null);
  }
  const requestRefresh = () => {
    setLoading(true);
    return refresh(load);
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Load queue metadata for the mounted environment and abort the active read on unmount.
  useEffect(() => {
    void refreshOnMount(load);
    return () => {
      active.current?.abort();
    };
  }, [load]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own the metadata-expiry timer so a formerly current snapshot is marked stale at its deadline.
  useEffect(() => {
    if (!result || !("snapshot" in result) || result.expires_at_ms <= observedAt) return;
    const remaining = result.expires_at_ms - Date.now();
    const timer = window.setTimeout(
      () => setObservedAt(Date.now()),
      Math.max(0, Math.min(remaining, 2_147_483_647)),
    );
    return () => window.clearTimeout(timer);
  }, [result, observedAt]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish committed asynchronous metadata results to the containing queue section.
  useEffect(() => {
    onResult?.(result);
  }, [result, onResult]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish a failed refresh so the containing section treats the retained sample as stale.
  useEffect(() => {
    onFailedChange?.(failed);
  }, [failed, onFailedChange]);
  const snapshot = result && "snapshot" in result ? result.snapshot : null;
  const stale =
    failed ||
    result?.status === "stale" ||
    (result !== null && "snapshot" in result && result.expires_at_ms <= observedAt);
  return (
    <section aria-label="Queue metadata" className="min-w-0 space-y-3">
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
        <div role="status" className="min-w-0 flex-1 space-y-1 text-sm text-muted-foreground">
          {loading && !result ? <p>Loading queue metadata…</p> : null}
          {loading && result ? <p>Refreshing the queue sample…</p> : null}
          {failed && !snapshot && result?.status !== "unavailable" ? (
            <p>Queue metadata unavailable. Refresh to try again.</p>
          ) : null}
          {failed && snapshot ? (
            <p>Refresh failed. The queue sample below is from an earlier read.</p>
          ) : null}
          {result?.status === "unconfigured" ? <p>Queue metadata is not configured.</p> : null}
          {result?.status === "unavailable" ? (
            <p>Queue metadata unavailable: {queueUnavailableMessage(result.reason)}.</p>
          ) : null}
          {snapshot ? (
            <p>
              {stale
                ? "Stale sample"
                : result?.status === "partial"
                  ? "Partial sample"
                  : "Ready sample"}
              {` · Sampled ${formatTime(snapshot.observed_at_ms)}`}
              {stale || snapshot.coverage === "partial" || result?.status === "partial"
                ? " · Current queue may differ."
                : null}
            </p>
          ) : null}
        </div>
        <Button
          variant="outline"
          size="compact"
          disabled={loading}
          onClick={() => void requestRefresh()}
        >
          Refresh queue
        </Button>
      </div>
      {children}
      <details className="text-xs text-muted-foreground">
        <summary className="cursor-pointer">About this queue sample</summary>
        <div className="mt-2 space-y-2 wrap-anywhere">
          <p>Requests from all supported producers. Prompt text and editing are unavailable.</p>
          <p>
            Accepted dispatch or native command status does not prove a handoff or completed work.
            Historical, held and uncertain states remain here until exact native handoff evidence is
            available.
          </p>
          {snapshot ? (
            <>
              <p>
                Coverage: {snapshot.coverage} · Sampled {formatTime(snapshot.observed_at_ms)} ·{" "}
                {snapshot.items.length} requests in this sample
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
