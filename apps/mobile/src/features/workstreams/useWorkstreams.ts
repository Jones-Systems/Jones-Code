import { useAtomValue } from "@effect/atom-react";
import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import {
  validateAppearanceBinding,
  appendWorkstreamDtoPage,
  nativeWorkstreamThreadKey,
  planWorkstreamOwnerOrder,
  loadLiveT3Placements,
  workstreamBindingKey,
  type LiveT3Placements,
  type WorkstreamThreadLike,
  type WorkstreamDtoPage,
} from "@t3tools/client-runtime/state/workstreams";
import {
  EnvironmentId,
  T3PlacementIdentity,
  type WorkstreamAppearancePage,
  type WorkstreamAppearanceWrite,
  type WorkstreamCommand,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AsyncResult, Atom } from "effect/reactivity";
import { AppState, Pressable, Text } from "react-native";
import { createElement, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentCatalog } from "../../connection/catalog";
import { runtime } from "../../lib/runtime";
import { MobileSecureStorage } from "../../persistence/mobile-secure-storage";
import { environmentSession } from "../../state/session";
import { useEnvironments } from "../../state/environments";
import { workstreamRequest, type WorkstreamClient } from "./gateway";
import type { EnvironmentHttpAuthHeaders } from "@t3tools/client-runtime/authorization";
import { loadCompleteWorkstreamList } from "./loaders";
import { reconcileMobileWorkstreamCommand, waitForWorkstreamReceipt } from "./commands";
import { canEditWorkstreams } from "./actions";
import { MobileWorkstreamControls } from "./Controls";

import {
  sameMobileWorkstreamAuthority,
  mobilePlacementInventory,
  projectMobileWorkstreams,
  type MobileWorkstreamSnapshot,
  type MobileWorkstreamGroup,
} from "./projection";
import type { MobileWorkstreams } from "./types";
const COLLAPSE_KEY = "t3code.workstream-collapse.v1";
const refreshSubscribers = new Set<() => void>();
const refreshMountedWorkstreams = () => {
  for (const refresh of refreshSubscribers) refresh();
};

export function useMobileWorkstreams(threads: readonly WorkstreamThreadLike[]) {
  const { environments } = useEnvironments();
  const idsKey = JSON.stringify(environments.map((entry) => entry.environmentId).sort());
  const connectionsAtom = useMemo(
    () =>
      Atom.make((get) =>
        Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Array(EnvironmentId)))(
          idsKey,
        ).flatMap((id) => {
          const prepared = get(environmentSession.preparedConnectionValueAtom(id));
          const state = get(environmentCatalog.stateAtom(id));
          const session = get(environmentSession.sessionStateAtom(id));
          return AsyncResult.isSuccess(session) &&
            session.value.authenticated &&
            Option.isSome(prepared) &&
            AsyncResult.isSuccess(state) &&
            state.value.phase === "connected"
            ? [{ prepared: prepared.value, generation: state.value.generation }]
            : [];
        }),
      ),
    [idsKey],
  );
  const connections = useAtomValue(connectionsAtom);
  const context = useAtomValue(connectionAtomRuntime);
  const [loaded, setLoaded] = useState<{
    connections: typeof connections;
    snapshots: readonly MobileWorkstreamSnapshot[];
  }>({ connections, snapshots: [] });
  const [revision, setRevision] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [collapsedKeys, setCollapsedKeys] = useState<ReadonlySet<string>>(new Set());
  const [enabled, setEnabled] = useState(false);
  const [selection, setSelection] = useState<{
    thread?: WorkstreamThreadLike;
    groupKey?: string;
  } | null>(null);
  const pendingCommands = useRef(new Map<PreparedConnection, WorkstreamCommand>());
  const pendingConnections = useRef(connections);
  const controllers = useRef(new Set<AbortController>());
  const current = useRef(connections);
  current.current = connections;
  const inventory = JSON.stringify(mobilePlacementInventory(threads));
  const snapshots = loaded.connections === connections ? loaded.snapshots : [];
  const snapshotsRef = useRef(snapshots);
  snapshotsRef.current = snapshots;
  const request = useCallback(
    <A, E>(
      prepared: PreparedConnection,
      method: "GET" | "POST",
      path: string,
      run: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
      signal: AbortSignal,
    ) => {
      if (!AsyncResult.isSuccess(context))
        return Promise.reject(new Error("Connection is unavailable."));
      return runtime.runPromise(
        workstreamRequest(prepared, method, path, run).pipe(Effect.provide(context.value)),
        { signal },
      );
    },
    [context],
  );
  const refresh = useCallback(() => {
    setLoaded({ connections: current.current, snapshots: [] });
    setRevision((n) => n + 1);
  }, []);
  useEffect(() => {
    refreshSubscribers.add(refresh);
    return () => {
      refreshSubscribers.delete(refresh);
    };
  }, [refresh]);
  useEffect(() => {
    let active = true;
    void runtime
      .runPromise(MobileSecureStorage.pipe(Effect.flatMap((store) => store.getItem(COLLAPSE_KEY))))
      .then((value) => {
        if (active && value)
          setCollapsedKeys(
            new Set(
              Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Array(Schema.String)))(value),
            ),
          );
      })
      .catch(() => {});
    void runtime
      .runPromise(
        MobileSecureStorage.pipe(
          Effect.flatMap((store) => store.getItem(`${COLLAPSE_KEY}.enabled`)),
        ),
      )
      .then((value) => {
        if (active) setEnabled(value === "true");
      })
      .catch(() => {});
    return () => {
      active = false;
    };
  }, []);
  const toggleEnabled = () =>
    setEnabled((value) => {
      const next = !value;
      void runtime
        .runPromise(
          MobileSecureStorage.pipe(
            Effect.flatMap((store) => store.setItem(`${COLLAPSE_KEY}.enabled`, String(next))),
          ),
        )
        .catch(() => setError("Could not save grouping preference."));
      return next;
    });
  const toggleGroup = useCallback(
    (key: string) =>
      setCollapsedKeys((previous) => {
        const next = new Set(previous);
        if (next.has(key)) next.delete(key);
        else next.add(key);
        void runtime
          .runPromise(
            MobileSecureStorage.pipe(
              Effect.flatMap((store) => store.setItem(COLLAPSE_KEY, JSON.stringify([...next]))),
            ),
          )
          .catch(() => setError("Could not save collapsed Workstreams."));
        return next;
      }),
    [],
  );
  useEffect(() => {
    const controller = new AbortController();
    controllers.current.add(controller);
    setLoaded({ connections, snapshots: [] });
    setSelection(null);
    setError(null);
    if (pendingConnections.current !== connections) {
      pendingCommands.current.clear();
      pendingConnections.current = connections;
    }
    if (!AsyncResult.isSuccess(context)) return () => controller.abort();
    const identities = Schema.decodeUnknownSync(
      Schema.fromJsonString(Schema.Array(T3PlacementIdentity)),
    )(inventory);
    void Promise.all(
      connections.map(async (connection): Promise<MobileWorkstreamSnapshot | null> => {
        try {
          const data = await loadCompleteWorkstreamList(
            (cursor) =>
              request(
                connection.prepared,
                "GET",
                "",
                (client, headers) =>
                  client.list({ headers, payload: { limit: 50, ...(cursor ? { cursor } : {}) } }),
                controller.signal,
              ),
            { signal: controller.signal },
          );
          let appearance: WorkstreamAppearancePage | null = null;
          try {
            const items: WorkstreamAppearancePage["items"][number][] = [];
            const ids = data.items.map((item) => item.workstreamId);
            for (let start = 0; start < Math.max(1, ids.length); start += 100) {
              const batch = ids.slice(start, start + 100);
              const result = await request(
                connection.prepared,
                "POST",
                "/appearance/read",
                (client, headers) =>
                  client.appearanceRead({ headers, payload: { workstream_ids: batch } }),
                controller.signal,
              );
              if (!result.supported) {
                appearance = null;
                break;
              }
              validateAppearanceBinding(result.page, data.binding, batch);
              items.push(...result.page.items);
              appearance = { ...result.page, items };
            }
          } catch {
            appearance = null;
          }
          let placements: LiveT3Placements | null = null;
          try {
            placements = await loadLiveT3Placements(
              data,
              identities,
              () =>
                request(
                  connection.prepared,
                  "POST",
                  "/thread-placements",
                  (client, headers) =>
                    client.threadPlacements({ headers, payload: { identities } }),
                  controller.signal,
                ),
              Date.now,
              (bytes) =>
                runtime.runPromise(
                  Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.digest("SHA-256", bytes))),
                ),
            );
          } catch {
            /* Unverified placements leave native rows unassigned. */
          }
          return {
            ...connection,
            appearance,
            data,
            placements,
            identityKeys: new Set(
              identities.map((identity) =>
                nativeWorkstreamThreadKey(identity.source_instance_id, identity.native_thread_id),
              ),
            ),
          };
        } catch {
          return null;
        }
      }),
    ).then((values) => {
      if (!controller.signal.aborted && current.current === connections)
        setLoaded({
          connections,
          snapshots: values.filter((value): value is MobileWorkstreamSnapshot => value !== null),
        });
    });
    return () => {
      for (const pending of controllers.current) pending.abort();
      controllers.current.clear();
    };
  }, [connections, context, inventory, revision, request]);
  useEffect(() => {
    const expiries = snapshots.flatMap(
      (snapshot) => snapshot.placements?.items.map((item) => Date.parse(item.expires_at)) ?? [],
    );
    if (!expiries.length) return;
    const timer = setTimeout(
      refresh,
      Math.max(0, Math.min(2147483647, Math.min(...expiries) - Date.now())),
    );
    return () => clearTimeout(timer);
  }, [snapshots, refresh]);
  const bindingRevision = snapshots
    .map((snapshot) =>
      JSON.stringify([
        snapshot.prepared.environmentId,
        snapshot.generation,
        workstreamBindingKey(snapshot.data.binding),
      ]),
    )
    .join("|");
  useEffect(() => {
    const abort = new AbortController();
    let loading = false;
    const updateColors = async () => {
      if (loading || AppState.currentState !== "active") return;
      loading = true;
      try {
        for (const snapshot of snapshotsRef.current) {
          if (!snapshot.appearance) continue;
          const ids = snapshot.data.items.map((item) => item.workstreamId);
          let appearance = snapshot.appearance;
          const items: WorkstreamAppearancePage["items"][number][] = [];
          for (let start = 0; start < Math.max(1, ids.length); start += 100) {
            const batch = ids.slice(start, start + 100);
            const result = await request(
              snapshot.prepared,
              "POST",
              "/appearance/read",
              (client, headers) =>
                client.appearanceRead({ headers, payload: { workstream_ids: batch } }),
              abort.signal,
            );
            if (!result.supported) throw new Error("Appearance unavailable.");
            validateAppearanceBinding(result.page, snapshot.data.binding, batch);
            items.push(...result.page.items);
            appearance = { ...result.page, items };
          }
          if (!abort.signal.aborted)
            setLoaded((previous) =>
              previous.connections !== connections
                ? previous
                : {
                    ...previous,
                    snapshots: previous.snapshots.map((item) =>
                      item === snapshot ? { ...item, appearance } : item,
                    ),
                  },
            );
        }
      } catch {
        /* Retain the last observed display until an explicit refresh. */
      } finally {
        loading = false;
      }
    };
    const timer = setInterval(() => void updateColors(), 30_000);
    const subscription = AppState.addEventListener("change", (state) => {
      if (state === "active") void updateColors();
    });
    return () => {
      abort.abort();
      clearInterval(timer);
      subscription.remove();
    };
  }, [connections, request]);
  const assertCurrent = (snapshot: MobileWorkstreamSnapshot) => {
    if (
      !current.current.some(
        (entry) => entry.prepared === snapshot.prepared && entry.generation === snapshot.generation,
      ) ||
      !snapshotsRef.current.some((entry) => sameMobileWorkstreamAuthority(entry, snapshot))
    )
      throw new Error("Workstream access changed. Refresh before retrying.");
  };
  const read = async <A, E>(
    snapshot: MobileWorkstreamSnapshot,
    path: string,
    run: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
  ) => {
    assertCurrent(snapshot);
    const controller = new AbortController();
    controllers.current.add(controller);
    try {
      const result = await request(snapshot.prepared, "GET", path, run, controller.signal);
      assertCurrent(snapshot);
      return result;
    } catch (cause) {
      if (!controller.signal.aborted) refresh();
      throw cause;
    } finally {
      controllers.current.delete(controller);
    }
  };
  const pages = async <Item>(
    snapshot: MobileWorkstreamSnapshot,
    path: string,
    run: (
      client: WorkstreamClient,
      headers: EnvironmentHttpAuthHeaders,
      cursor?: string,
    ) => Effect.Effect<WorkstreamDtoPage<Item>, unknown>,
  ) => {
    let result = await read(snapshot, path, (client, headers) => run(client, headers));
    const seen = new Set<string>();
    while (result.next_cursor !== null) {
      const cursor = result.next_cursor;
      if (seen.has(cursor)) throw new Error("Repeated Workstream cursor.");
      seen.add(cursor);
      result = appendWorkstreamDtoPage(
        result,
        await read(snapshot, path, (client, headers) => run(client, headers, cursor)),
      );
    }
    if (
      result.context.owner_id !== snapshot.data.binding.ownerId ||
      result.context.server_generation !== snapshot.data.binding.serverGeneration ||
      result.context.registry_version !== snapshot.data.binding.registryVersion
    )
      throw new Error("Workstream revision changed.");
    return result;
  };
  const submitActions = async (
    snapshot: MobileWorkstreamSnapshot,
    actions: readonly WorkstreamCommand["action"][],
  ) => {
    assertCurrent(snapshot);
    if (!canEditWorkstreams(snapshot.data) || snapshot.placements?.readiness !== "ready")
      throw new Error("Workstreams are read-only.");
    const pending = pendingCommands.current.get(snapshot.prepared);
    if (pending) {
      const receipt = await reconcileMobileWorkstreamCommand({
        command: pending,
        ownerId: snapshot.data.binding.ownerId,
        principalId: snapshot.data.binding.principalId,
        resume: true,
        pollAttempts: 0,
        submit: () => Promise.reject(new Error("A pending command cannot be resubmitted.")),
        poll: (commandId) =>
          read(snapshot, `/commands/${encodeURIComponent(commandId)}`, (client, headers) =>
            client.command({ headers, params: { commandId } }),
          ),
        assertCurrent: () => assertCurrent(snapshot),
        wait: async () => {},
      });
      if (receipt.state === "pending" || receipt.state === "unresolved")
        throw new Error(`Command ${pending.command_id} remains ${receipt.state}.`);
      pendingCommands.current.delete(snapshot.prepared);
      refreshMountedWorkstreams();
      return receipt;
    }
    let registryVersion = snapshot.data.binding.registryVersion;
    let lastReceipt: WorkstreamReceipt | undefined;
    const controller = new AbortController();
    controllers.current.add(controller);
    try {
      for (const action of actions) {
        const command_id = await runtime.runPromise(
          Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.randomUUIDv4)),
        );
        assertCurrent(snapshot);
        const command: WorkstreamCommand = {
          command_id,
          expected_server_generation: snapshot.data.binding.serverGeneration,
          expected_registry_version: registryVersion,
          action,
        };
        pendingCommands.current.set(snapshot.prepared, command);
        const receipt = await reconcileMobileWorkstreamCommand({
          command,
          ownerId: snapshot.data.binding.ownerId,
          principalId: snapshot.data.binding.principalId,
          submit: () =>
            request(
              snapshot.prepared,
              "POST",
              "/commands",
              (client, headers) => client.submit({ headers, payload: { command } }),
              controller.signal,
            ),
          poll: (commandId) =>
            request(
              snapshot.prepared,
              "GET",
              `/commands/${encodeURIComponent(commandId)}`,
              (client, headers) => client.command({ headers, params: { commandId } }),
              controller.signal,
            ),
          assertCurrent: () => {
            controller.signal.throwIfAborted();
            assertCurrent(snapshot);
          },
          wait: (milliseconds) => waitForWorkstreamReceipt(milliseconds, controller.signal),
        });
        assertCurrent(snapshot);
        if (receipt.state !== "pending" && receipt.state !== "unresolved")
          pendingCommands.current.delete(snapshot.prepared);
        if (receipt.state !== "committed")
          throw new Error(
            `Workstream command ${command_id}: ${receipt.state}. Reconcile this command before retrying.`,
          );
        registryVersion = receipt.registry_version;
        lastReceipt = receipt;
      }
      if (!lastReceipt) throw new Error("No Workstream change was requested.");
      refreshMountedWorkstreams();
      return lastReceipt;
    } finally {
      controllers.current.delete(controller);
    }
  };
  const submit = (snapshot: MobileWorkstreamSnapshot, action: WorkstreamCommand["action"]) =>
    submitActions(snapshot, [action]);
  const reorderGroup = (group: MobileWorkstreamGroup, direction: -1 | 1) => {
    const items = group.snapshot.data.items;
    const ordered = [...items].sort(
      (a, b) => a.sortOrder - b.sortOrder || a.workstreamId.localeCompare(b.workstreamId),
    );
    const target =
      ordered.findIndex((item) => item.workstreamId === group.workstream.workstreamId) + direction;
    const actions = planWorkstreamOwnerOrder(items, group.workstream.workstreamId, target)
      .filter(({ item, sortOrder }) => item.sortOrder !== sortOrder)
      .map(({ item, sortOrder }): WorkstreamCommand["action"] => ({
        operation: "update_workstream",
        workstream_id: item.workstreamId,
        expected_version: item.version,
        name: item.name,
        lifecycle: item.lifecycle,
        progress: item.progress,
        sort_order: sortOrder,
      }));
    return submitActions(group.snapshot, actions);
  };
  const projection = useMemo(
    () => projectMobileWorkstreams(snapshots, threads, Date.now()),
    [snapshots, threads],
  );
  const saveAppearance = async (
    snapshot: MobileWorkstreamSnapshot,
    input: WorkstreamAppearanceWrite,
  ) => {
    assertCurrent(snapshot);
    if (!snapshot.appearance?.permissions.includes("workstreams:write"))
      throw new Error("Color editing unavailable.");
    const controller = new AbortController();
    controllers.current.add(controller);
    try {
      const result = await request(
        snapshot.prepared,
        "POST",
        "/appearance/write",
        (client, headers) => client.appearanceSave({ headers, payload: input }),
        controller.signal,
      );
      assertCurrent(snapshot);
      refreshMountedWorkstreams();
      return result;
    } finally {
      controllers.current.delete(controller);
    }
  };
  const api: MobileWorkstreams = {
    saveAppearance,
    ...projection,
    snapshots,
    enabled,
    toggleEnabled,
    collapsedKeys,
    toggleGroup,
    bindingRevision,
    readiness: snapshots.some((s) => s.placements?.readiness === "ready") ? "ready" : "unavailable",
    error,
    refresh,
    submit,
    reorderGroup,
    read,
    pages,
  };
  return {
    ...api,
    openThread: (thread: WorkstreamThreadLike) => setSelection({ thread }),
    openGroup: (groupKey: string) => setSelection({ groupKey }),
    controls: createElement(
      Pressable,
      { accessibilityRole: "button", onPress: () => setSelection({}), style: { padding: 12 } },
      createElement(Text, null, "Workstreams"),
    ),
    sheet: createElement(MobileWorkstreamControls, {
      api,
      selection: loaded.connections === connections ? selection : null,
      onSelect: setSelection,
      onClose: () => setSelection(null),
    }),
  };
}
