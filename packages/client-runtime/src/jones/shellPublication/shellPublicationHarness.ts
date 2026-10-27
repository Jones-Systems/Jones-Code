import {
  EnvironmentId,
  ORCHESTRATION_V2_WS_METHODS,
  ThreadId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type NetworkStatus,
  type PreparedConnection,
} from "../../connection/model.ts";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import * as EnvironmentRegistry from "../../connection/registry.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import * as Persistence from "../../platform/persistence.ts";
import type { WsRpcProtocolClient } from "../../rpc/protocol.ts";
import type { RpcSession } from "../../rpc/session.ts";
import { createEnvironmentShellAtoms } from "../../state/shell.ts";
import { createEnvironmentSnapshotAtom } from "../../state/snapshots.ts";
import { createOptimisticThreadLifecycle } from "../../state/threadLifecycle.ts";
import * as ShellSnapshotLoader from "../../state/shellSnapshotHttp.ts";
import { createEnvironmentThreadShellAtoms } from "../../state/threadShell.ts";
import { v2ShellSnapshot, v2ThreadShell } from "../../state/orchestrationV2TestFixtures.ts";

export const makeShellPublicationHarness = Effect.fn("TestShellPublication.make")(
  function* (options: {
    readonly count: number;
    readonly environmentCount?: number;
    readonly instrument?: boolean;
    readonly initialAvailable?: boolean;
  }) {
    const ids = Array.from({ length: options.environmentCount ?? 1 }, (_, i) =>
      EnvironmentId.make(`shell-publication-${i}`),
    );
    const environments = new Map<
      EnvironmentId,
      {
        events: Queue.Queue<OrchestrationV2ShellStreamItem, unknown>;
        opened: Deferred.Deferred<void>;
        closed: Deferred.Deferred<void>;
        closedEvents: Queue.Queue<void>;
        target: PrimaryConnectionTarget;
        supervisor: ReturnType<typeof EnvironmentSupervisor.EnvironmentSupervisor.of>;
        snapshot: OrchestrationV2ShellSnapshot;
        httpSnapshot: Option.Option<OrchestrationV2ShellSnapshot>;
      }
    >();
    for (const [index, id] of ids.entries()) {
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem, unknown>();
      const opened = yield* Deferred.make<void>();
      const closed = yield* Deferred.make<void>();
      const closedEvents = yield* Queue.unbounded<void>();
      const target = new PrimaryConnectionTarget({
        environmentId: id,
        label: `Synthetic ${index}`,
        httpBaseUrl: "https://example.test",
        wsBaseUrl: "wss://example.test",
      });
      const prepared: PreparedConnection = {
        target,
        environmentId: id,
        label: target.label,
        httpBaseUrl: target.httpBaseUrl,
        socketUrl: target.wsBaseUrl,
        httpAuthorization: null,
      };
      const session: RpcSession = {
        client: {
          [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Effect.acquireRelease(Deferred.succeed(opened, undefined), () =>
                  Deferred.succeed(closed, undefined).pipe(
                    Effect.andThen(Queue.offer(closedEvents, undefined)),
                  ),
                );
                return Stream.fromQueue(events);
              }),
            ),
        } as unknown as WsRpcProtocolClient,
        initialConfig: Effect.sync(() => {
          return { shellResumeCompletionMarker: true } as never;
        }),
        subscribeServerConfig: () => Stream.empty,
        ready: Effect.void,
        probe: Effect.void,
        closed: Effect.never,
      };
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make(Option.some(session)),
        prepared: yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
          Option.some(prepared),
        ),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const snapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 1,
        threads: Array.from({ length: options.count }, (_, i) => ({
          ...v2ThreadShell,
          id: ThreadId.make(`thread-${index}-${i}`),
          title: `Thread ${index}-${i}`,
        })),
      };
      environments.set(id, {
        events,
        opened,
        closed,
        closedEvents,
        target,
        supervisor,
        snapshot,
        httpSnapshot: Option.none(),
      });
      if (options.initialAvailable)
        yield* Queue.offerAll(events, [{ kind: "snapshot", snapshot }, { kind: "synchronized" }]);
    }
    const env = environments.get(ids[0]!)!;
    const cache = Persistence.EnvironmentCacheStore.of({
      loadShell: () => Effect.succeedNone,
      saveShell: () => Effect.void,
      loadThread: () => Effect.succeedNone,
      saveThread: () => Effect.void,
      removeThread: () => Effect.void,
      loadServerConfig: () => Effect.succeedNone,
      saveServerConfig: () => Effect.void,
      loadVcsRefs: () => Effect.succeedNone,
      saveVcsRefs: () => Effect.void,
      removeVcsRefs: () => Effect.void,
      clearVcsRefs: () => Effect.void,
      clear: () => Effect.void,
    });
    const environmentRegistry = EnvironmentRegistry.EnvironmentRegistry.of({
      entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
        new Map(),
      ),
      networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
      start: Effect.void,
      register: () => Effect.die("Unexpected registration"),
      registerPlatform: () => Effect.die("Unexpected registration"),
      reconcilePlatform: () => Effect.die("Unexpected reconciliation"),
      remove: () => Effect.die("Unexpected removal"),
      removeRoute: () => Effect.die("Unexpected removal"),
      reorderRoutes: () => Effect.die("Unexpected reorder"),
      removeRelayEnvironments: () => Effect.die("Unexpected removal"),
      retryNow: () => Effect.void,
      setEnabled: () => Effect.die("Unexpected toggle"),
      setCompatibility: () => Effect.die("Unexpected compatibility"),
      recordUpdateCompatibility: () => Effect.die("Unexpected compatibility"),
      state: (id) => SubscriptionRef.get(environments.get(id)!.supervisor.state),
      stateChanges: (id) => SubscriptionRef.changes(environments.get(id)!.supervisor.state),
      run: (id, effect) =>
        Effect.provideService(
          effect,
          EnvironmentSupervisor.EnvironmentSupervisor,
          environments.get(id)!.supervisor,
        ),
      runStream: (id, stream) =>
        Stream.provideService(
          stream,
          EnvironmentSupervisor.EnvironmentSupervisor,
          environments.get(id)!.supervisor,
        ),
      followStream: (id, stream) =>
        Stream.provideService(
          stream,
          EnvironmentSupervisor.EnvironmentSupervisor,
          environments.get(id)!.supervisor,
        ),
    });
    const runtime = Atom.runtime(
      Layer.mergeAll(
        Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
        Layer.succeed(Persistence.EnvironmentCacheStore, cache),
        Layer.succeed(
          ShellSnapshotLoader.ShellSnapshotLoader,
          ShellSnapshotLoader.ShellSnapshotLoader.of({
            load: (prepared) =>
              Effect.sync(() => environments.get(prepared.environmentId)!.httpSnapshot),
          }),
        ),
      ),
    );
    const shell = createEnvironmentShellAtoms(runtime);
    const stateAtom = shell.stateAtom;
    const snapshots = createEnvironmentSnapshotAtom(stateAtom);
    const lifecycle = createOptimisticThreadLifecycle(snapshots);
    const catalog = Atom.make({
      isReady: true,
      entries: new Map(
        ids.map((id) => [
          id,
          { target: environments.get(id)!.target, enabled: true, profile: Option.none() },
        ]),
      ),
    });
    const threads = createEnvironmentThreadShellAtoms({
      catalogValueAtom: catalog,
      snapshotAtom: lifecycle.snapshotAtom,
    });
    const metrics = {
      evaluations: 0,
      memberReads: 0,
      trackedReads: 0,
      emissions: 0,
      publications: 0,
    };
    if (options.instrument) {
      const read = threads.quietThreadShellsAtom.read;
      Object.defineProperty(threads.quietThreadShellsAtom, "read", {
        value: (get: Atom.AtomContext) => {
          metrics.evaluations++;
          return read(
            new Proxy(get, {
              apply(target, thisArg, args) {
                metrics.trackedReads++;
                const candidate = args[0];
                if (
                  Atom.isAtom(candidate) &&
                  candidate.label?.[0].startsWith("environment-quiet-thread-shell:")
                )
                  metrics.memberReads++;
                return Reflect.apply(target, thisArg, args);
              },
            }),
          );
        },
      });
    }
    const registry = yield* Effect.acquireRelease(
      Effect.sync(() => AtomRegistry.make()),
      (value) => Effect.sync(() => value.dispose()),
    );
    const ref = { environmentId: ids[0]!, threadId: env.snapshot.threads[0]!.id };
    const disposers = [
      registry.mount(threads.quietThreadShellsAtom),
      registry.subscribe(threads.quietThreadShellsAtom, () => metrics.emissions++),
      registry.subscribe(threads.quietThreadShellsAtom, () => {}),
      registry.subscribe(threads.quietThreadShellsAtom, () => {}),
      registry.mount(threads.threadShellsAtom),
      registry.mount(threads.delegatedUnderParentAtom),
      registry.mount(threads.threadShellAtom(ref)),
      registry.mount(threads.quietThreadShellAtom(ref)),
      registry.mount(threads.childThreadInputsAtom(ref)),
      ...ids.flatMap((id) => [
        registry.subscribe(stateAtom(id), () => metrics.publications++),
        registry.mount(shell.statusAtom(id)),
        registry.mount(shell.hasSnapshotAtom(id)),
      ]),
    ];
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        for (let i = disposers.length - 1; i >= 0; i--) disposers[i]!();
      }),
    );
    const publish = Effect.fn("TestShellPublication.publish")(function* (
      id: EnvironmentId,
      items: ReadonlyArray<OrchestrationV2ShellStreamItem>,
      sequence: number,
    ) {
      const done = yield* Deferred.make<void>();
      const stop = registry.subscribe(stateAtom(id), (value) => {
        const state = value._tag === "Success" ? value.value : undefined;
        if (
          state !== undefined &&
          Option.isSome(state.snapshot) &&
          state.snapshot.value.snapshotSequence >= sequence
        )
          // Observe completion after the entire notification/batch stack drains.
          queueMicrotask(() => Deferred.doneUnsafe(done, Effect.void));
      });
      try {
        yield* Queue.offerAll(environments.get(id)!.events, items);
        yield* Deferred.await(done);
      } finally {
        stop();
      }
    });
    const replaceSession = Effect.fn("TestShellPublication.replaceSession")(function* (
      id: EnvironmentId,
      snapshot: OrchestrationV2ShellSnapshot,
    ) {
      const environment = environments.get(id)!;
      environment.httpSnapshot = Option.some(snapshot);
      const done = yield* Deferred.make<void>();
      const stop = registry.subscribe(stateAtom(id), (result) => {
        if (
          result._tag === "Success" &&
          Option.isSome(result.value.snapshot) &&
          result.value.snapshot.value.snapshotSequence >= snapshot.snapshotSequence
        )
          // Observe completion after the entire notification/batch stack drains.
          queueMicrotask(() => Deferred.doneUnsafe(done, Effect.void));
      });
      try {
        const session = Option.getOrThrow(
          yield* SubscriptionRef.get(environment.supervisor.session),
        );
        yield* SubscriptionRef.set(environment.supervisor.session, Option.some({ ...session }));
        yield* Deferred.await(done);
      } finally {
        stop();
      }
    });
    for (const id of ids) {
      yield* Deferred.await(environments.get(id)!.opened);
      if (options.initialAvailable) {
        // Await only the preloaded stream: a second offer would mask a lost cold publication.
        const ready = yield* Deferred.make<void>();
        const check = () => {
          const value = registry.get(stateAtom(id));
          if (
            value._tag === "Success" &&
            Option.isSome(value.value.snapshot) &&
            value.value.snapshot.value.snapshotSequence === 1 &&
            value.value.status === "live"
          )
            queueMicrotask(() => Deferred.doneUnsafe(ready, Effect.void));
        };
        const stop = registry.subscribe(stateAtom(id), check);
        try {
          check();
          yield* Deferred.await(ready);
        } finally {
          stop();
        }
      } else {
        yield* publish(
          id,
          [
            { kind: "snapshot", snapshot: environments.get(id)!.snapshot },
            { kind: "synchronized" },
          ],
          1,
        );
      }
    }
    return {
      registry,
      shell,
      stateAtom,
      snapshots,
      lifecycle,
      threads,
      catalog,
      ids,
      environments,
      ref,
      metrics,
      publish,
      replaceSession,
      resetMetrics: () => {
        metrics.evaluations =
          metrics.memberReads =
          metrics.trackedReads =
          metrics.emissions =
          metrics.publications =
            0;
      },
    };
  },
);
