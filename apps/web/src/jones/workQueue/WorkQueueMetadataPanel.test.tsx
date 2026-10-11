// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import type { WorkQueueMetadata, WorkQueueMetadataResult } from "@t3tools/contracts";
import { WorkQueueMetadataPanel, type WorkQueueMetadataLoader } from "./WorkQueueMetadataPanel";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("submitted work metadata", () => {
  let root: Root;
  let container: HTMLDivElement;
  const sample = (): WorkQueueMetadata => ({
    schema: "codex.t3-work-queue-metadata/v1",
    source: {
      queue_id: "queue",
      host_id: "host",
      environment_ref: "environment",
      exporter_instance_id: "worker",
    },
    observed_at_ms: Date.now(),
    snapshot_token: "a".repeat(64),
    coverage: "complete",
    authority_effect: "none",
    items: [
      {
        request_id: "request-legacy",
        workstream_id: "raw-legacy-id",
        canonical_binding: null,
        entry_kind: "ordinary",
        request_kind: "initial",
        lane: "normal",
        queue_state: "unknown",
        submitted_at_ms: null,
        target: null,
        dispatch_status: "unknown",
        native_command_status: null,
        finish_line: "not_tracked",
      },
      {
        request_id: "request-canonical",
        workstream_id: "exact-canonical-id",
        canonical_binding: {
          owner_id: "owner",
          server_generation: 1,
          registry_version: 2,
          membership_id: "membership",
          native_reference_id: "reference",
          source_instance_id: "instance",
          native_thread_id: "thread",
          authority_namespace: "namespace",
          store_generation: 1,
          expires_at: "2099-01-01T00:00:00Z",
        },
        entry_kind: "flexible",
        request_kind: "owner_followup",
        lane: "high",
        queue_state: "observed_terminal",
        submitted_at_ms: null,
        target: { host_id: "host", environment_ref: "environment", thread_id: "thread" },
        dispatch_status: "accepted",
        native_command_status: "accepted",
        finish_line: "not_tracked",
      },
    ],
  });
  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });
  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });
  async function render(load: WorkQueueMetadataLoader, key = "environment-a") {
    await act(() => root.render(<WorkQueueMetadataPanel key={key} load={load} />));
  }
  const ready = (): WorkQueueMetadataResult => ({
    status: "ready",
    snapshot: sample(),
    expires_at_ms: Date.now() + 60_000,
  });
  it("shows raw and canonical identities, unknown outcomes and no mutation controls", async () => {
    await render(async () => ready());
    expect([...container.querySelectorAll("th")].map((heading) => heading.textContent)).toEqual([
      "Request / workstream",
      "Target",
      "Queue state",
      "Handoff",
    ]);
    const details = [...container.querySelectorAll("details")];
    expect(details).toHaveLength(3);
    for (const detail of details) {
      expect(detail.open).toBe(false);
      await act(() => detail.querySelector("summary")!.click());
      expect(detail.open).toBe(true);
    }
    const text = container.textContent;
    for (const value of [
      "raw-legacy-id",
      "exact-canonical-id",
      "Legacy / unverified",
      "Canonical binding verified at sample",
      "membership",
      "observed_terminal",
      "Dispatch: unknown",
      "Native command: Not observed",
      "Not tracked",
      "does not prove a handoff or completed work",
      "Handoff: unconfirmed in this sample",
      "Source: queue",
      "Sampled",
    ]) {
      expect(text).toContain(value);
    }
    expect(container.querySelector("textarea,input")).toBeNull();
    expect([...container.querySelectorAll("button")].map((button) => button.textContent)).toEqual([
      "Refresh metadata",
    ]);
  });
  it.each(["partial", "stale"] as const)(
    "keeps %s rows visible with their sample status",
    async (status) => {
      await render(async () => ({
        status,
        snapshot: { ...sample(), coverage: "partial" },
        expires_at_ms: Date.now() + 60_000,
      }));
      expect(container.textContent).toContain(
        status === "stale" ? "Stale sample" : "Partial sample",
      );
      expect(container.textContent).toContain("Coverage: partial");
      expect(container.textContent).toContain("request-legacy");
    },
  );
  it.each([
    { status: "unconfigured", reason: "not_configured" },
    { status: "unavailable", reason: "future_sample" },
    { status: "unavailable", reason: "source_unavailable" },
  ] satisfies WorkQueueMetadataResult[])(
    "does not turn $reason into an empty queue",
    async (result) => {
      await render(async () => result);
      expect(container.textContent).toContain(
        result.status === "unconfigured"
          ? "not configured"
          : result.reason === "future_sample"
            ? "the sample timestamp is in the future"
            : "the source cannot be read",
      );
      expect(container.textContent).not.toContain("No submitted work");
      expect(container.querySelector("table")).toBeNull();
    },
  );
  it("marks a cached sample stale when its validity expires without polling", async () => {
    vi.useFakeTimers();
    const load = vi.fn(async () => ready());
    await render(load);
    expect(container.textContent).toContain("Ready sample");
    await act(() => vi.advanceTimersByTime(60_000));
    expect(container.textContent).toContain("Stale sample");
    expect(load).toHaveBeenCalledTimes(1);
  });
  it("clears old data, aborts the old environment read and ignores its late result", async () => {
    const oldRead = deferred<WorkQueueMetadataResult>();
    let oldSignal: AbortSignal | undefined;
    await render((signal) => {
      oldSignal = signal;
      return oldRead.promise;
    });
    await render(
      async () => ({ status: "unconfigured", reason: "not_configured" }),
      "environment-b",
    );
    expect(oldSignal?.aborted).toBe(true);
    await act(() => oldRead.resolve(ready()));
    expect(container.textContent).toContain("not configured");
    expect(container.textContent).not.toContain("request-legacy");
  });
  it("refreshes explicitly and reports a failed read without displaying old rows as current", async () => {
    const load = vi
      .fn<WorkQueueMetadataLoader>()
      .mockResolvedValueOnce(ready())
      .mockRejectedValueOnce(new Error("offline"));
    await render(load);
    expect(container.textContent).toContain("request-legacy");
    await act(() => container.querySelector<HTMLButtonElement>("button")!.click());
    expect(container.textContent).toContain("Queue metadata unavailable");
    expect(container.textContent).not.toContain("request-legacy");
    expect(container.textContent).not.toContain("No submitted work");
  });
  it("distinguishes loading from an observed empty sample", async () => {
    const read = deferred<WorkQueueMetadataResult>();
    await render(() => read.promise);
    expect(container.textContent).toContain("Loading queue metadata");
    expect(container.textContent).not.toContain("No submitted work");
    expect(container.querySelector<HTMLButtonElement>("button")!.disabled).toBe(true);
    await act(() =>
      read.resolve({
        status: "ready",
        snapshot: { ...sample(), items: [] },
        expires_at_ms: Date.now() + 60_000,
      }),
    );
    expect(container.textContent).not.toContain("Loading queue metadata");
    expect(container.textContent).toContain("No submitted work in this sample");
    expect(container.querySelector<HTMLButtonElement>("button")!.disabled).toBe(false);
  });
  it.each(["partial", "stale"] as const)(
    "never describes an empty %s sample as an empty queue",
    async (status) => {
      await render(async () => ({
        status,
        snapshot: {
          ...sample(),
          items: [],
          coverage: status === "partial" ? "partial" : "complete",
        },
        expires_at_ms: Date.now() + 60_000,
      }));
      expect(container.textContent).toContain("the current queue may contain work");
      expect(container.textContent).not.toContain("No submitted work");
      expect(container.querySelector("[role='status']")!.textContent).toContain(
        status === "stale" ? "Stale sample" : "Partial sample",
      );
    },
  );
});
