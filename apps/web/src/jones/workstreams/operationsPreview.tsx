import { createRoot } from "react-dom/client";
import { useEffect, useRef, useState } from "react";
import "../../index.css";
import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  type WorkstreamOperationRequest,
  type WorkstreamOperationResult,
} from "@t3tools/contracts/jones/workstreamOperations";
import {
  WorkstreamOperationsClient,
  WorkstreamOperationError,
  operationSucceeded,
  type RetainedOperation,
} from "@t3tools/client-runtime/jones/workstreams/operations";
import type { WorkstreamListView } from "../../state/workstreams";
import { WorkstreamPicker } from "./WorkstreamPicker";
import { SetupStatusContent, sourceReasonLabels } from "./SetupStatusContent";
import { operationFixtureData, setupFixture } from "./operations.fixtures";
import { Button } from "../../components/ui/button";

// Development-only synthetic entry. No app bootstrap, authentication, provider or database access.
let sequence = 0;
const fixtureId = async () => `fixture-${Date.now()}-${++sequence}`;
const recoveryKey = "jones-workstreams-evidence-operations-v1";
const authorityKey = "jones-workstreams-evidence-authority-v1";
type Authority = {
  placement: string | null;
  names: { id: string; name: string }[];
  creates: number;
};
const initial: Authority = {
  placement: "alpha",
  names: [
    { id: "alpha", name: "Alpha" },
    { id: "beta", name: "Beta" },
  ],
  creates: 0,
};
const loadAuthority = (): Authority =>
  JSON.parse(sessionStorage.getItem(authorityKey) ?? JSON.stringify(initial));
function Scenario() {
  const authority = useRef(loadAuthority());
  const [snapshot, setSnapshot] = useState(authority.current);
  const [mode, setMode] = useState<"commit" | "pending" | "conflict" | "config" | "lost-reply">(
    "commit",
  );
  const modeRef = useRef(mode);
  modeRef.current = mode;
  const [open, setOpen] = useState(false);
  const [entries, setEntries] = useState<readonly RetainedOperation[]>([]);
  const [reason, setReason] = useState<keyof typeof sourceReasonLabels | "ready">("ready");
  const [client] = useState(() => {
    const results = new Map<string, WorkstreamOperationResult>();
    const requests = new Map<string, WorkstreamOperationRequest>();
    const submit = async ({
      request,
    }: {
      request: WorkstreamOperationRequest;
    }): Promise<WorkstreamOperationResult> => {
      requests.set(request.operation_id, request);
      const old = results.get(request.operation_id);
      if (old && operationSucceeded(old)) return old;
      const base = {
        contract: WORKSTREAM_OPERATIONS_CONTRACT,
        operation_id: request.operation_id,
        kind: request.kind,
        request_sha256: "a".repeat(64),
        origin: { kind: "app" as const },
        children: [],
        accepted_at: "2026-10-23T00:00:00Z",
        updated_at: "2026-10-23T00:00:00Z",
        observed: null,
        detail: null,
      };
      if (modeRef.current === "pending") {
        const result: WorkstreamOperationResult = {
          ...base,
          state: "reconciling",
          reason:
            request.kind === "create_workstream"
              ? "create_effect_unknown"
              : "membership_effect_unknown",
          result: null,
          poll_after_ms: 1000,
        };
        results.set(request.operation_id, result);
        return result;
      }
      if (modeRef.current === "config")
        return {
          ...base,
          state: "unavailable",
          reason: "not_configured",
          result: null,
          poll_after_ms: null,
        };
      if (modeRef.current === "conflict" && request.kind === "set_primary")
        return {
          ...base,
          kind: "set_primary",
          state: "conflict",
          reason: "precondition_failed",
          result: null,
          poll_after_ms: null,
          observed: { kind: "primary", workstream_id: "beta", name: "Beta" },
        };
      const next = { ...authority.current, names: [...authority.current.names] };
      let id: string | null;
      if (request.kind === "create_workstream") {
        id = `created-${request.operation_id}`;
        if (!next.names.some((item) => item.id === id)) {
          next.names.push({ id, name: request.workstream.name });
          next.creates += 1;
        }
      } else {
        id = request.target.kind === "workstream" ? request.target.workstream_id : null;
        next.placement = id;
      }
      authority.current = next;
      sessionStorage.setItem(authorityKey, JSON.stringify(next));
      const result: WorkstreamOperationResult = {
        ...base,
        state: "committed",
        reason: null,
        poll_after_ms: null,
        result: {
          workstream_id: id,
          membership_id: request.kind === "set_primary" ? "membership" : null,
          registry_version: 12,
        },
      };
      results.set(request.operation_id, result);
      if (modeRef.current === "lost-reply") {
        modeRef.current = "commit";
        throw new Error("Synthetic lost reply. Retry the retained operation.");
      }
      return result;
    };
    return new WorkstreamOperationsClient(
      {
        submit,
        read: async (payload) => {
          if (!("operation_id" in payload)) return { operations: [] };
          const result = results.get(payload.operation_id) ?? null;
          const retained = requests.get(payload.operation_id);
          return {
            operation:
              result?.state === "reconciling" && retained && modeRef.current !== "pending"
                ? await submit({ request: retained })
                : result,
          };
        },
        setup: async () => setupFixture,
      },
      {
        load: async () => sessionStorage.getItem(recoveryKey),
        save: async (value) => {
          sessionStorage.setItem(recoveryKey, value);
        },
      },
      fixtureId,
    );
  });
  useEffect(() => {
    const abort = new AbortController();
    const unsubscribe = client.subscribe(() => {
      setEntries(client.snapshot());
      setSnapshot(loadAuthority());
    });
    void client.watch(abort.signal).catch(() => undefined);
    return () => {
      abort.abort();
      unsubscribe();
    };
  }, [client]);
  const finish = async (
    key: string,
    make: (id: string) => WorkstreamOperationRequest,
    signal?: AbortSignal,
  ) => {
    const result = await client.run(key, make, { signal, retry: true, waitBudgetMs: 0 });
    if (!operationSucceeded(result)) throw new WorkstreamOperationError(result);
    setSnapshot(loadAuthority());
    return result;
  };
  const controller: WorkstreamListView = {
    data: {
      ...operationFixtureData,
      items: snapshot.names.map((item, index) => ({
        ...operationFixtureData.items[0]!,
        workstreamId: item.id,
        name: item.name,
        sortOrder: index,
      })),
    },
    operationEntries: entries,
    error: null,
    loading: false,
    placements: null,
    placementInventory: { coverage: "complete", identities: [], json: "[]", totalIdentities: 0 },
    refresh: () => setSnapshot(loadAuthority()),
    submit: async () => {
      throw new Error("Legacy command not available in this operations fixture.");
    },
    runBindingOperation: async () => {
      throw new Error("Not part of this fixture.");
    },
    loadDetail: async () => {
      throw new Error("Not part of this fixture.");
    },
    loadReference: async () => {
      throw new Error("Not part of this fixture.");
    },
    assignNativeThread: async (thread, destination, options) => {
      await finish(
        "thread:synthetic",
        (operation_id) => ({
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          origin: { kind: "app" },
          operation_id,
          kind: "set_primary",
          thread: { environment_id: thread.environmentId, thread_id: thread.id },
          target:
            destination === null
              ? { kind: "unassigned", registry_id: "registry-1", owner_id: "owner" }
              : {
                  kind: "workstream",
                  registry_id: "registry-1",
                  owner_id: "owner",
                  workstream_id: destination,
                },
          expected: { kind: "any" },
        }),
        options.signal,
      );
    },
    createWorkstream: async (name, options) => {
      const result = await finish(
        `create:${name}`,
        (operation_id) => ({
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          origin: { kind: "app" },
          operation_id,
          kind: "create_workstream",
          registry: { registry_id: "registry-1", owner_id: "owner" },
          workstream: { name, lifecycle: "planned", progress: { state: "unknown" }, sort_order: 0 },
        }),
        options.signal,
      );
      if (result.state !== "committed" || !result.result.workstream_id)
        throw new Error("Pending creation");
      return result.result.workstream_id;
    },
  };
  return (
    <main className="mx-auto max-w-2xl space-y-6 p-6 text-foreground">
      <h1 className="text-xl font-semibold">Workstreams synthetic evidence</h1>
      <p>Real picker and setup presentation; synthetic transport. No installed behavior claim.</p>
      <label className="block">
        Operation scenario{" "}
        <select
          aria-label="Operation scenario"
          value={mode}
          onChange={(event) => setMode(event.target.value as typeof mode)}
        >
          {["commit", "pending", "conflict", "config", "lost-reply"].map((item) => (
            <option key={item}>{item}</option>
          ))}
        </select>
      </label>
      <p role="status">
        Authoritative placement:{" "}
        {snapshot.names.find((item) => item.id === snapshot.placement)?.name ?? "Unassigned"} ·
        Created: {snapshot.creates}
      </p>
      <Button onClick={() => setOpen(true)}>Assign synthetic thread</Button>
      <Button
        variant="outline"
        onClick={() => {
          sessionStorage.removeItem(recoveryKey);
          sessionStorage.removeItem(authorityKey);
          location.reload();
        }}
      >
        Reset synthetic scenario
      </Button>
      {open ? (
        <WorkstreamPicker
          controller={controller}
          thread={{
            environmentId: "env:a",
            id: "synthetic",
            title:
              "Synthetic thread with a long descriptive title to check wrapping and narrow layouts",
          }}
          onClose={() => setOpen(false)}
          commandId={fixtureId}
        />
      ) : null}
      <section className="space-y-3">
        <h2 className="text-lg font-semibold">Workstreams setup</h2>
        <label>
          Source reason{" "}
          <select
            aria-label="Source reason"
            value={reason}
            onChange={(event) => setReason(event.target.value as typeof reason)}
          >
            {["ready", ...Object.keys(sourceReasonLabels)].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
        <SetupStatusContent
          status={{
            ...setupFixture,
            t3_source:
              reason === "ready"
                ? { state: "ready", reason: null }
                : { state: "unavailable", reason },
            next_steps:
              reason === "ready"
                ? []
                : [
                    {
                      code: "enroll_native_source",
                      doc: "docs/operations/workstreams-native-enrollment.md",
                    },
                  ],
          }}
        />
      </section>
    </main>
  );
}
createRoot(document.getElementById("root")!).render(<Scenario />);
