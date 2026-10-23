import { useAtomValue } from "@effect/atom-react";
import { useEffect, useRef, useState } from "react";
import { EnvironmentId, type T3WorkstreamListResult } from "@t3tools/contracts";
import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  type WorkstreamOperationRequest,
} from "@t3tools/contracts/jones/workstreamOperations";
import {
  WorkstreamOperationsClient,
  supportsWorkstreamOperations,
  operationSucceeded,
  operationRegistryIdentity,
  WorkstreamOperationError,
  type RetainedOperation,
} from "@t3tools/client-runtime/jones/workstreams/operations";
import { environmentCatalog } from "../../connection/catalog";
import { environmentSession } from "../../state/session";
import { enabledEnvironmentIds } from "@t3tools/client-runtime/state/connections";
import { Atom } from "effect/reactivity";
import {
  environmentOperationsTransport,
  requestEnvironmentWorkstreams,
} from "./environmentWorkstreamsRequest";
import { environmentWorkstreamsRoutes } from "./remoteThreadAssignment";

const configsAtom = Atom.make((get) =>
  [...enabledEnvironmentIds(get(environmentCatalog.catalogValueAtom))].map((environmentId) => ({
    environmentId,
    config: get(environmentSession.initialConfigValueAtom(environmentId)),
  })),
);
export function useWorkstreamOperations(input: {
  data: T3WorkstreamListResult | null;
  primaryEnvironmentId: string | null;
  refresh: () => void;
}) {
  const configs = useAtomValue(configsAtom);
  const clients = useRef(new Map<string, WorkstreamOperationsClient>());
  const idFactories = useRef(new Map<string, () => Promise<string>>());
  const [entries, setEntries] = useState<readonly RetainedOperation[]>([]);
  const [watchRevision, setWatchRevision] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const refreshRef = useRef(input.refresh);
  refreshRef.current = input.refresh;
  const authority =
    input.data &&
    JSON.stringify([
      input.data.binding.registryId,
      input.data.binding.ownerId,
      input.data.binding.principalId,
    ]);
  const capableIds = configs
    .filter((entry) => supportsWorkstreamOperations(entry.config?.environment.capabilities))
    .map((entry) => entry.environmentId);
  const capabilityKey = JSON.stringify(capableIds);
  const clientFor = (environmentId: string, newId?: () => Promise<string>) => {
    if (!authority) throw new Error("Workstream registry is unavailable.");
    const key = JSON.stringify([environmentId, authority]);
    if (newId) idFactories.current.set(key, newId);
    let client = clients.current.get(key);
    if (!client) {
      const storageKey = `jones.workstream-operations.v1:${key}`;
      client = new WorkstreamOperationsClient(
        environmentOperationsTransport(environmentId),
        {
          load: async () => window.localStorage.getItem(storageKey),
          save: async (value) => {
            window.localStorage.setItem(storageKey, value);
          },
        },
        () => {
          const factory = idFactories.current.get(key);
          if (!factory) throw new Error("A new Workstream operation requires an explicit action.");
          return factory();
        },
      );
      clients.current.set(key, client);
    }
    return client;
  };
  useEffect(() => {
    setEntries([]);
    setError(null);
    if (!authority) return;
    const controller = new AbortController();
    const active = capableIds.map((id) => clientFor(id));
    const completed = new Set<string>();
    const update = () => {
      if (controller.signal.aborted) return;
      const next = active.flatMap((client) => client.snapshot());
      setEntries(next);
      for (const entry of next) {
        if (entry.result && operationSucceeded(entry.result) && !completed.has(entry.operationId)) {
          completed.add(entry.operationId);
          refreshRef.current();
        }
      }
    };
    const unsubscribes = active.map((client) => client.subscribe(update));
    for (const client of active)
      void client.watch(controller.signal).catch(() => {
        if (!controller.signal.aborted)
          setError(
            "Workstream recovery is unavailable. Reopen or retry to check the retained operation.",
          );
      });
    update();
    return () => {
      controller.abort();
      for (const unsubscribe of unsubscribes) unsubscribe();
    };
    // Each binding and live capability set owns exactly one polling lifetime.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authority, capabilityKey, watchRevision]);
  const registryFor = async (environmentId: string, signal?: AbortSignal) => {
    const status = await environmentOperationsTransport(environmentId).setup(signal);
    if (!input.data) throw new Error("Workstreams unavailable.");
    return operationRegistryIdentity(status, input.data.binding.ownerId);
  };
  const run = async (
    environmentId: string,
    key: string,
    make: (id: string) => WorkstreamOperationRequest,
    options: {
      commandId: () => Promise<string>;
      signal?: AbortSignal;
      retry?: boolean;
    },
  ) => {
    setError(null);
    try {
      const result = await clientFor(environmentId, options.commandId).run(key, make, {
        ...options,
        waitBudgetMs: 3000,
      });
      if (!operationSucceeded(result)) throw new WorkstreamOperationError(result);
      refreshRef.current();
      return result;
    } finally {
      if (!options.signal?.aborted) setWatchRevision((value) => value + 1);
    }
  };
  return {
    entries,
    error,
    supports: (id: string) => capableIds.includes(EnvironmentId.make(id)),
    assign: async (
      thread: { environmentId: string; id: string },
      destination: string | null,
      expected: Extract<WorkstreamOperationRequest, { kind: "set_primary" }>["expected"],
      options: { commandId: () => Promise<string>; signal?: AbortSignal; retry?: boolean },
    ) => {
      const data = input.data;
      if (!data) throw new Error("Workstreams unavailable.");
      if (!input.primaryEnvironmentId) throw new Error("Primary Workstream registry unavailable.");
      let registryEnvironment = input.primaryEnvironmentId;
      if (!capableIds.includes(EnvironmentId.make(registryEnvironment))) {
        const remote = await environmentWorkstreamsRoutes(
          thread.environmentId,
          requestEnvironmentWorkstreams,
        ).list(undefined, options.signal);
        if (
          remote.binding.registryId !== data.binding.registryId ||
          remote.binding.ownerId !== data.binding.ownerId
        )
          throw new Error("Workstream change rejected: registry_mismatch.");
        registryEnvironment = thread.environmentId;
      }
      const registry = await registryFor(registryEnvironment, options.signal);
      return run(
        thread.environmentId,
        `thread:${thread.id}`,
        (operation_id) => ({
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          origin: { kind: "app" },
          operation_id,
          kind: "set_primary",
          thread: { environment_id: thread.environmentId, thread_id: thread.id },
          target:
            destination === null
              ? { kind: "unassigned", ...registry }
              : { kind: "workstream", ...registry, workstream_id: destination },
          expected,
        }),
        options,
      );
    },
    create: async (
      name: string,
      options: { commandId: () => Promise<string>; signal?: AbortSignal },
    ) => {
      const data = input.data;
      const environmentId = input.primaryEnvironmentId;
      if (!data || !environmentId) throw new Error("Workstreams unavailable.");
      const registry = await registryFor(environmentId, options.signal);
      const result = await run(
        environmentId,
        `create:${name.trim()}`,
        (operation_id) => ({
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          origin: { kind: "app" },
          operation_id,
          kind: "create_workstream",
          registry,
          workstream: {
            name: name.trim(),
            lifecycle: "planned",
            progress: { state: "unknown" },
            sort_order: 0,
          },
        }),
        options,
      );
      if (result.state !== "committed" || !result.result.workstream_id)
        throw new Error("Creation has no confirmed Workstream.");
      return result.result.workstream_id;
    },
  };
}
