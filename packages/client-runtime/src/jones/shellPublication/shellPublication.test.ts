import { expect, it } from "@effect/vitest";
import { RunId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { AsyncResult, AtomRegistry } from "effect/reactivity";
import type { AtomCommand } from "../../state/runtime.ts";
import { EnvironmentRpcUnavailableError } from "../../rpc/client.ts";
import { makeShellPublicationHarness } from "./shellPublicationHarness.ts";

it.effect("publishes a warmed shell snapshot without rebuilding the all-thread list per row", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* makeShellPublicationHarness({ count: 60 });
      const id = h.ids[0]!;
      const original = h.environments.get(id)!.snapshot;
      expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(60);
      for (const changed of [20, 60]) {
        h.resetMetrics();
        const sequence = changed === 20 ? 2 : 3;
        const next = {
          ...original,
          snapshotSequence: sequence,
          threads: original.threads.map((thread, index) => ({
            ...thread,
            title: index < changed ? `Renamed ${changed}-${index}` : thread.title,
          })),
        };
        yield* h.publish(id, [{ kind: "snapshot", snapshot: next }], sequence);
        const rows = h.registry.get(h.threads.quietThreadShellsAtom);
        expect(rows.map((row) => row.id)).toEqual(original.threads.map((row) => row.id));
        expect(rows.slice(0, changed).map((row) => row.title)).toEqual(
          next.threads.slice(0, changed).map((row) => row.title),
        );
        expect(h.metrics.emissions).toBe(1);
      }
    }),
  ),
);

it.effect("keeps streaming counts raw while publishing Stop and completion promptly", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* makeShellPublicationHarness({ count: 12, instrument: true });
      const id = h.ids[0]!;
      const snapshot = h.environments.get(id)!.snapshot;
      const first = snapshot.threads[0]!;
      const running = {
        ...first,
        activeRunId: RunId.make("synthetic-run"),
        latestRunId: RunId.make("synthetic-run"),
        status: "running" as const,
        itemCount: 1,
        visibleItemCount: 1,
      };
      yield* h.publish(
        id,
        [{ kind: "thread.updated", location: "active", sequence: 2, thread: running }],
        2,
      );
      const quiet = h.registry.get(h.threads.quietThreadShellAtom(h.ref));
      h.resetMetrics();
      const tick = {
        ...running,
        itemCount: 4,
        visibleItemCount: 4,
        updatedAt: DateTime.add(running.updatedAt, { seconds: 1 }),
      };
      yield* h.publish(
        id,
        [{ kind: "thread.updated", location: "active", sequence: 3, thread: tick }],
        3,
      );
      expect(h.registry.get(h.threads.threadShellAtom(h.ref))?.itemCount).toBe(4);
      expect(h.registry.get(h.threads.quietThreadShellAtom(h.ref))).toBe(quiet);
      expect(h.metrics.emissions).toBe(0);
      yield* h.publish(
        id,
        [
          {
            kind: "thread.updated",
            location: "active",
            sequence: 4,
            thread: { ...tick, activeRunId: null, status: "completed" },
          },
        ],
        4,
      );
      expect(h.registry.get(h.threads.quietThreadShellAtom(h.ref))?.runtime?.status).toBe(
        "completed",
      );
      expect(h.registry.get(h.threads.quietThreadShellAtom(h.ref))?.itemCount).toBe(4);
    }),
  ),
);

it.effect(
  "reconciles two confirmations while another optimistic update remains, then rolls it back",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const h = yield* makeShellPublicationHarness({ count: 6, instrument: true });
        const id = h.ids[0]!;
        const snapshot = h.environments.get(id)!.snapshot;
        type Input = {
          readonly environmentId: typeof id;
          readonly input: { readonly threadId: ThreadId; readonly title: string };
        };
        const accept: Array<
          (
            value:
              | AsyncResult.Success<{ sequence: number }, string>
              | AsyncResult.Failure<{ sequence: number }, string>,
          ) => void
        > = [];
        const base: AtomCommand<Input, { sequence: number }, string> = {
          label: "synthetic rename",
          run: () => new Promise((resolve) => accept.push(resolve)),
        };
        const command = h.lifecycle.wrap(base, (thread, input) => ({
          ...thread,
          title: input.title,
        }));
        const seenConfirmed = new Set<string>();
        const stopObserving = h.registry.subscribe(h.threads.quietThreadShellsAtom, (rows) => {
          for (let i = 0; i < 2; i++) {
            const row = rows.find((item) => item.id === snapshot.threads[i]!.id)!;
            if (row.title === `Confirmed ${i}`) seenConfirmed.add(row.id);
            if (seenConfirmed.has(row.id)) expect(row.title).toBe(`Confirmed ${i}`);
          }
        });
        yield* Effect.addFinalizer(() => Effect.sync(stopObserving));
        const pending = snapshot.threads.slice(0, 3).map((thread, i) =>
          command.run(h.registry, {
            environmentId: id,
            input: { threadId: thread.id, title: `Optimistic ${i}` },
          }),
        );
        expect(
          h.registry
            .get(h.threads.quietThreadShellsAtom)
            .slice(0, 3)
            .map((row) => row.title),
        ).toEqual(["Optimistic 0", "Optimistic 1", "Optimistic 2"]);
        accept[0]!(AsyncResult.success({ sequence: 2 }));
        accept[1]!(AsyncResult.success({ sequence: 2 }));
        yield* Effect.promise(() => Promise.all(pending.slice(0, 2)));
        const server = {
          ...snapshot,
          snapshotSequence: 2,
          threads: snapshot.threads.map((thread, i) =>
            i < 2 ? { ...thread, title: `Confirmed ${i}` } : thread,
          ),
        };
        yield* h.publish(id, [{ kind: "snapshot", snapshot: server }], 2);
        expect(
          h.registry
            .get(h.threads.quietThreadShellsAtom)
            .slice(0, 3)
            .map((row) => row.title),
        ).toEqual(["Confirmed 0", "Confirmed 1", "Optimistic 2"]);
        accept[2]!(AsyncResult.failure(Cause.fail("declined")));
        yield* Effect.promise(() => pending[2]!);
        expect(
          h.registry
            .get(h.threads.quietThreadShellsAtom)
            .slice(0, 3)
            .map((row) => row.title),
        ).toEqual(["Confirmed 0", "Confirmed 1", snapshot.threads[2]!.title]);
      }),
    ),
);

it.effect(
  "keeps cross-environment approval, parent removal and catalog visibility consistent",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const h = yield* makeShellPublicationHarness({
          count: 4,
          environmentCount: 2,
          instrument: true,
        });
        const [home, remote] = h.ids;
        const parent = h.environments.get(home!)!.snapshot.threads[0]!;
        const original = h.environments.get(remote!)!.snapshot;
        const task = {
          ...original.threads[0]!,
          status: "running" as const,
          delegatedFrom: { environmentId: home!, threadId: parent.id, title: parent.title },
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("synthetic-approval"),
            kind: "command" as const,
            createdAt: parent.createdAt,
          },
        };
        yield* h.publish(
          remote!,
          [{ kind: "thread.updated", sequence: 2, location: "active", thread: task }],
          2,
        );
        expect(h.registry.get(h.threads.quietThreadShellAtom(h.ref))?.pendingSubagentRequest).toBe(
          "approval",
        );
        expect(
          h.registry.get(h.threads.childThreadInputsAtom(h.ref)).map((row) => row.id),
        ).toContain(task.id);
        yield* h.publish(
          remote!,
          [
            {
              kind: "thread.updated",
              sequence: 3,
              location: "active",
              thread: { ...task, pendingRuntimeRequest: null },
            },
          ],
          3,
        );
        expect(
          h.registry.get(h.threads.quietThreadShellAtom(h.ref))?.pendingSubagentRequest,
        ).toBeNull();
        const catalog = h.registry.get(h.catalog);
        const entries = new Map(catalog.entries);
        entries.set(remote!, { ...entries.get(remote!)!, enabled: false });
        h.registry.set(h.catalog, { ...catalog, entries });
        expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(4);
        expect(h.registry.get(h.threads.delegatedUnderParentAtom).size).toBe(0);
        h.registry.set(h.catalog, catalog);
        expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(8);
        expect(h.registry.get(h.threads.delegatedUnderParentAtom).size).toBe(1);
        const homeSnapshot = h.environments.get(home!)!.snapshot;
        yield* h.publish(
          home!,
          [
            {
              kind: "snapshot",
              snapshot: {
                ...homeSnapshot,
                snapshotSequence: 2,
                threads: homeSnapshot.threads.slice(1),
              },
            },
          ],
          2,
        );
        expect(h.registry.get(h.threads.quietThreadShellAtom(h.ref))).toBeNull();
        expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(7);
        expect(h.registry.get(h.threads.delegatedUnderParentAtom).size).toBe(0);
      }),
    ),
);

it.effect("reuses unchanged HTTP rows while publishing twenty changed rows once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* makeShellPublicationHarness({ count: 60 });
      const id = h.ids[0]!;
      const snapshot = h.environments.get(id)!.snapshot;
      const before = h.registry.get(h.threads.quietThreadShellsAtom);
      h.resetMetrics();
      const incoming = {
        ...snapshot,
        snapshotSequence: 2,
        threads: snapshot.threads.map((row, i) => ({
          ...row,
          title: i < 20 ? `HTTP changed ${i}` : row.title,
        })),
      };
      yield* h.replaceSession(id, incoming);
      const after = h.registry.get(h.threads.quietThreadShellsAtom);
      expect(after.map((row) => row.title)).toEqual(incoming.threads.map((row) => row.title));
      for (let i = 20; i < 60; i++) expect(after[i]).toBe(before[i]);
      expect(h.metrics.emissions).toBe(1);
    }),
  ),
);

it.effect(
  "publishes data available during cold mount and cancels the actual subscription on disposal",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const h = yield* makeShellPublicationHarness({ count: 8, initialAvailable: true });
        const id = h.ids[0]!;
        const environment = h.environments.get(id)!;
        expect(h.registry.get(h.threads.quietThreadShellsAtom).map((row) => row.id)).toEqual(
          environment.snapshot.threads.map((row) => row.id),
        );
        const cold = h.registry.get(h.shell.stateValueAtom(id));
        expect(cold.status).toBe("live");
        expect(cold.snapshot._tag === "Some" && cold.snapshot.value.snapshotSequence).toBe(1);
        h.registry.dispose();
        yield* Queue.take(environment.closedEvents);
        const emissions = h.metrics.emissions;
        yield* Queue.offer(environment.events, {
          kind: "snapshot",
          snapshot: { ...environment.snapshot, snapshotSequence: 2 },
        });
        yield* Effect.yieldNow;
        expect(h.metrics.emissions).toBe(emissions);
        expect(h.registry.getNodes().size).toBe(0);
        const remounted = AtomRegistry.make();
        const observed = yield* Deferred.make<void>();
        const stopState = remounted.subscribe(h.stateAtom(id), (result) => {
          if (
            result._tag === "Success" &&
            result.value.snapshot._tag === "Some" &&
            result.value.snapshot.value.snapshotSequence === 2
          )
            Deferred.doneUnsafe(observed, Effect.void);
        });
        const stopList = remounted.mount(h.threads.quietThreadShellsAtom);
        yield* Deferred.await(observed);
        expect(remounted.get(h.threads.quietThreadShellsAtom)).toHaveLength(8);
        stopList();
        stopState();
        remounted.dispose();
        yield* Queue.take(environment.closedEvents);
      }),
    ),
);

it.effect("keeps cached rows and exposes an expected subscription failure as shell state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* makeShellPublicationHarness({ count: 5 });
      const id = h.ids[0]!;
      const noticed = yield* Deferred.make<void>();
      const stop = h.registry.subscribe(h.shell.stateValueAtom(id), (value) => {
        if (value.error._tag === "Some") Deferred.doneUnsafe(noticed, Effect.void);
      });
      yield* Queue.failCause(
        h.environments.get(id)!.events,
        Cause.fail(
          new EnvironmentRpcUnavailableError({
            environmentId: id,
            message: "Synthetic expected failure",
          }),
        ),
      );
      yield* Deferred.await(noticed);
      const state = h.registry.get(h.shell.stateValueAtom(id));
      expect(state.error._tag).toBe("Some");
      expect(state.status).toBe("cached");
      expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(5);
      stop();
    }),
  ),
);
