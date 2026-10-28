import { useAtomValue } from "@effect/atom-react";
import { useEffect, useEffectEvent, useRef, useState } from "react";
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
  // oxlint-disable-next-line t3code/react-effect-budget -- Publish the committed refresh callback to retained workstream operation completion handlers.
  useEffect(() => {
    refreshRef.current = input.refresh;
  }, [input.refresh]);
  const refresh = useEffectEvent(() => input.refresh());
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
  const watchKey = JSON.stringify([authority, capableIds, watchRevision]);
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
  const startWatch = useEffectEvent((key: string) => {
    const [watchAuthority, watchCapableIds] = JSON.parse(key) as [string | null, string[], number];
    const controller = new AbortController();
    queueMicrotask(() => {
      if (controller.signal.aborted) return;
      setEntries([]);
      setError(null);
    });
    if (!watchAuthority) return () => controller.abort();
    const active = watchCapableIds.map((id) => clientFor(id));
    const completed = new Set<string>();
    const update = () =>
      queueMicrotask(() => {
        if (controller.signal.aborted) return;
        const next = active.flatMap((client) => client.snapshot());
        setEntries(next);
        for (const entry of next) {
          if (
            entry.result &&
            operationSucceeded(entry.result) &&
            !completed.has(entry.operationId)
          ) {
            completed.add(entry.operationId);
            refresh();
          }
        }
      });
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
  });
  // Each binding, live capability set and explicit retry owns one polling lifetime.
  // oxlint-disable-next-line t3code/react-effect-budget -- Own one operation watch lifetime per binding, capability set and explicit retry.
  useEffect(() => startWatch(watchKey), [watchKey]);
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
    return Promise.resolve()
      .then(() =>
        clientFor(environmentId, options.commandId).run(key, make, {
          ...options,
          waitBudgetMs: 3000,
        }),
      )
      .then((result) => {
        if (!operationSucceeded(result)) throw new WorkstreamOperationError(result);
        refreshRef.current();
        return result;
      })
      .finally(() => {
        if (!options.signal?.aborted) setWatchRevision((value) => value + 1);
      });
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
