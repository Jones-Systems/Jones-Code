import { useAtomValue } from "@effect/atom-react";
import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import {
  validateAppearanceBinding,
  appendWorkstreamDtoPage,
  nativeWorkstreamThreadKey,
  planWorkstreamOwnerOrder,
  loadLiveT3Placements,
  workstreamBindingKey,
  type ConnectedEnvironmentTrustInput,
  type LiveT3Placements,
  type WorkstreamAssignmentIndexFetchers,
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
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentCatalog } from "../../connection/catalog";
import { runtime } from "../../lib/runtime";
import { MobileSecureStorage } from "../../persistence/mobile-secure-storage";
import { environmentSession } from "../../state/session";
import { useEnvironments } from "../../state/environments";
import {
  workstreamPageUrl,
  workstreamRequest,
  type WorkstreamClient,
  type WorkstreamUrls,
  type WorkstreamUrlSelector,
} from "./gateway";
import type { EnvironmentHttpAuthHeaders } from "@t3tools/client-runtime/authorization";
import {
  classifyMobileWorkstreamFailure,
  loadMobileWorkstreamRegistry,
  settleMobileWorkstreamSnapshots,
  type MobileWorkstreamLoadOutcome,
} from "./loaders";
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
const decodeEnvironmentIds = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Array(EnvironmentId)),
);
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
        decodeEnvironmentIds(idsKey).flatMap((id) => {
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
  // Session loss, unlike a dropped transport, removes an environment's loaded Workstreams.
  const signedOutAtom = useMemo(
    () =>
      Atom.make((get) =>
        decodeEnvironmentIds(idsKey)
          .filter((id) => {
            const session = get(environmentSession.sessionStateAtom(id));
            return AsyncResult.isSuccess(session) && !session.value.authenticated;
          })
          .join("\n"),
      ),
    [idsKey],
  );
  // Only the current session's bootstrap config is live; it adds display trust, never write trust.
  const liveEnvironmentsAtom = useMemo(
    () =>
      Atom.make((get): readonly ConnectedEnvironmentTrustInput[] =>
        decodeEnvironmentIds(idsKey).map((environmentId) => {
          const config = get(environmentSession.initialConfigValueAtom(environmentId));
          return { environmentId, config, live: config !== null };
        }),
      ),
    [idsKey],
  );
  const connections = useAtomValue(connectionsAtom);
  const liveEnvironments = useAtomValue(liveEnvironmentsAtom);
  const signedOutKey = useAtomValue(signedOutAtom);
  const context = useAtomValue(connectionAtomRuntime);
  const retained = useMemo(() => {
    const enrolled = new Set<string>(decodeEnvironmentIds(idsKey));
    const signedOut = new Set(signedOutKey.split("\n"));
    return (id: EnvironmentId) => enrolled.has(id) && !signedOut.has(id);
  }, [idsKey, signedOutKey]);
  const retainedRef = useRef(retained);
  // The last good load per environment. Refreshes and reconnects replace it only on success.
  const [loaded, setLoaded] = useState<ReadonlyMap<EnvironmentId, MobileWorkstreamSnapshot>>(
    () => new Map(),
  );
  const loadedRef = useRef(loaded);
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
  const inventory = JSON.stringify(mobilePlacementInventory(threads));
  const snapshots = useMemo(
    () =>
      [...loaded]
        .filter(([environmentId]) => retained(environmentId))
        .map(([, snapshot]) =>
          snapshot.stale ||
          connections.some(
            (entry) =>
              entry.prepared === snapshot.prepared && entry.generation === snapshot.generation,
          )
            ? snapshot
            : { ...snapshot, stale: true },
        ),
    [loaded, connections, retained],
  );
  const snapshotsRef = useRef(snapshots);
  useLayoutEffect(() => {
    retainedRef.current = retained;
    loadedRef.current = loaded;
    current.current = connections;
    snapshotsRef.current = snapshots;
  }, [retained, loaded, connections, snapshots]);
  const request = useCallback(
    <A, E>(
      prepared: PreparedConnection,
      method: "GET" | "POST",
      url: WorkstreamUrlSelector,
      run: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
      signal: AbortSignal,
    ) => {
      if (!AsyncResult.isSuccess(context))
        return Promise.reject(new Error("Connection is unavailable."));
      return runtime.runPromise(
        workstreamRequest(prepared, method, url, run).pipe(Effect.provide(context.value)),
        { signal },
      );
    },
    [context],
  );
  const refresh = useCallback(() => setRevision((n) => n + 1), []);
  const registryFetchers = useCallback(
    (prepared: PreparedConnection): WorkstreamAssignmentIndexFetchers => {
      const page = (cursor: string | undefined, limit: number) => ({
        limit,
        ...(cursor ? { cursor } : {}),
      });
      return {
        listWorkstreams: ({ cursor, signal }) =>
          request(
            prepared,
            "GET",
            (urls) => workstreamPageUrl(urls.list(), page(cursor, 50)),
            (client, headers) => client.list({ headers, payload: page(cursor, 50) }),
            signal,
          ),
        references: ({ cursor, signal }) =>
          request(
            prepared,
            "GET",
            (urls) => workstreamPageUrl(urls.references(), page(cursor, 100)),
            (client, headers) => client.references({ headers, payload: page(cursor, 100) }),
            signal,
          ),
        memberships: ({ workstreamId, cursor, signal }) =>
          request(
            prepared,
            "GET",
            (urls) =>
              workstreamPageUrl(urls.memberships({ params: { workstreamId } }), page(cursor, 100)),
            (client, headers) =>
              client.memberships({
                headers,
                params: { workstreamId },
                payload: page(cursor, 100),
              }),
            signal,
          ),
      };
    },
    [request],
  );
  useEffect(() => {
    setLoaded((previous) => {
      const next = settleMobileWorkstreamSnapshots(previous, new Map(), retained);
      return next.size === previous.size ? previous : next;
    });
  }, [retained]);
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
      connections.map(
        async (
          connection,
        ): Promise<
          readonly [EnvironmentId, MobileWorkstreamLoadOutcome<MobileWorkstreamSnapshot>]
        > => {
          const environmentId = connection.prepared.environmentId;
          return await (async () => {
            const previous = loadedRef.current.get(environmentId);
            const { data, index } = await loadMobileWorkstreamRegistry(
              registryFetchers(connection.prepared),
              { signal: controller.signal, listOnlyFallback: !previous?.index },
            );
            let appearance: WorkstreamAppearancePage | null = null;
            await (async () => {
              const items: WorkstreamAppearancePage["items"][number][] = [];
              const ids = data.items.map((item) => item.workstreamId);
              for (let start = 0; start < Math.max(1, ids.length); start += 100) {
                const batch = ids.slice(start, start + 100);
                const result = await request(
                  connection.prepared,
                  "POST",
                  (urls) => urls.appearanceRead(),
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
            })().catch((cause: unknown) => {
              // Lost authorization fails the whole load so the settled round purges it.
              if (classifyMobileWorkstreamFailure(cause) === "authorization") throw cause;
              appearance = null;
            });
            let placements: LiveT3Placements | null = null;
            await (async () => {
              placements = await loadLiveT3Placements(
                data,
                identities,
                () =>
                  request(
                    connection.prepared,
                    "POST",
                    (urls) => urls.threadPlacements(),
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
            })().catch((cause: unknown) => {
              if (classifyMobileWorkstreamFailure(cause) === "authorization") throw cause;
              /* Without current proof, stored assignments still group; changes stay blocked. */
            });
            return [
              environmentId,
              {
                kind: "loaded",
                snapshot: {
                  ...connection,
                  appearance,
                  data,
                  placements,
                  index,
                  stale: false,
                  identityKeys: new Set(
                    identities.map((identity) =>
                      nativeWorkstreamThreadKey(
                        identity.source_instance_id,
                        identity.native_thread_id,
                      ),
                    ),
                  ),
                },
              },
            ] as const;
          })().catch((cause: unknown) => {
            return [environmentId, { kind: "failed", cause }] as const;
          });
        },
      ),
    ).then((outcomes) => {
      // One coherent swap per refresh round; an aborted or superseded round changes nothing.
      if (!controller.signal.aborted && current.current === connections)
        setLoaded((previous) =>
          settleMobileWorkstreamSnapshots(previous, new Map(outcomes), retainedRef.current),
        );
    });
    return () => {
      for (const pending of controllers.current) pending.abort();
      controllers.current.clear();
    };
  }, [connections, context, inventory, revision, request, registryFetchers]);
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
      let reading: MobileWorkstreamSnapshot | undefined;
      await (async () => {
        for (const snapshot of snapshotsRef.current) {
          if (!snapshot.appearance || snapshot.stale) continue;
          reading = snapshot;
          const ids = snapshot.data.items.map((item) => item.workstreamId);
          let appearance = snapshot.appearance;
          const items: WorkstreamAppearancePage["items"][number][] = [];
          for (let start = 0; start < Math.max(1, ids.length); start += 100) {
            const batch = ids.slice(start, start + 100);
            const result = await request(
              snapshot.prepared,
              "POST",
              (urls) => urls.appearanceRead(),
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
            setLoaded(
              (previous) =>
                new Map(
                  [...previous].map(([id, item]) => [
                    id,
                    sameMobileWorkstreamAuthority(item, snapshot) ? { ...item, appearance } : item,
                  ]),
                ),
            );
        }
      })()
        .catch((cause: unknown) => {
          // Lost authorization drops that environment; anything else keeps the last display.
          if (
            reading !== undefined &&
            !abort.signal.aborted &&
            classifyMobileWorkstreamFailure(cause) === "authorization"
          ) {
            const environmentId = reading.prepared.environmentId;
            setLoaded((previous) =>
              settleMobileWorkstreamSnapshots(
                previous,
                new Map([[environmentId, { kind: "failed", cause }]]),
                retainedRef.current,
              ),
            );
          }
        })
        .finally(() => {
          loading = false;
        });
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
    url: WorkstreamUrlSelector,
    run: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
  ) => {
    assertCurrent(snapshot);
    const controller = new AbortController();
    controllers.current.add(controller);
    return await (async () => {
      const result = await request(snapshot.prepared, "GET", url, run, controller.signal);
      assertCurrent(snapshot);
      return result;
    })()
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          // Lost authorization drops this environment's Workstreams now, not after a reload.
          if (classifyMobileWorkstreamFailure(cause) === "authorization")
            setLoaded((previous) =>
              settleMobileWorkstreamSnapshots(
                previous,
                new Map([[snapshot.prepared.environmentId, { kind: "failed", cause }]]),
                retainedRef.current,
              ),
            );
          refresh();
        }
        throw cause;
      })
      .finally(() => {
        controllers.current.delete(controller);
      });
  };
  const pages = async <Item,>(
    snapshot: MobileWorkstreamSnapshot,
    url: (urls: WorkstreamUrls, cursor?: string) => string,
    run: (
      client: WorkstreamClient,
      headers: EnvironmentHttpAuthHeaders,
      cursor?: string,
    ) => Effect.Effect<WorkstreamDtoPage<Item>, unknown>,
  ) => {
    let result = await read(
      snapshot,
      (urls) => url(urls),
      (client, headers) => run(client, headers),
    );
    const seen = new Set<string>();
    while (result.next_cursor !== null) {
      const cursor = result.next_cursor;
      if (seen.has(cursor)) throw new Error("Repeated Workstream cursor.");
      seen.add(cursor);
      result = appendWorkstreamDtoPage(
        result,
        await read(
          snapshot,
          (urls) => url(urls, cursor),
          (client, headers) => run(client, headers, cursor),
        ),
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
    if (
      snapshot.stale ||
      !canEditWorkstreams(snapshot.data) ||
      snapshot.placements?.readiness !== "ready"
    )
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
          read(
            snapshot,
            (urls) => urls.command({ params: { commandId } }),
            (client, headers) => client.command({ headers, params: { commandId } }),
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
    return await (async () => {
      for (const action of actions) {
        const command_id = await runtime.runPromise(
          Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.randomUUIDv4())),
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
              (urls) => urls.submit(),
              (client, headers) => client.submit({ headers, payload: { command } }),
              controller.signal,
            ),
          poll: (commandId) =>
            request(
              snapshot.prepared,
              "GET",
              (urls) => urls.command({ params: { commandId } }),
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
    })().finally(() => {
      controllers.current.delete(controller);
    });
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
    () => projectMobileWorkstreams(snapshots, threads, Date.now(), liveEnvironments),
    [snapshots, threads, liveEnvironments],
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
    return await (async () => {
      const result = await request(
        snapshot.prepared,
        "POST",
        (urls) => urls.appearanceSave(),
        (client, headers) => client.appearanceSave({ headers, payload: input }),
        controller.signal,
      );
      assertCurrent(snapshot);
      refreshMountedWorkstreams();
      return result;
    })().finally(() => {
      controllers.current.delete(controller);
    });
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
    controls: (
      <Pressable
        accessibilityRole="button"
        onPress={() => setSelection({})}
        style={{ padding: 12 }}
      >
        <Text>Workstreams</Text>
      </Pressable>
    ),
    sheet: (
      <MobileWorkstreamControls
        api={api}
        selection={selection}
        onSelect={setSelection}
        onClose={() => setSelection(null)}
      />
    ),
  };
}
