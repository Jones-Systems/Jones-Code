import { expect, it } from "@effect/vitest";
import {
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Console from "effect/Console";
import { AsyncResult } from "effect/reactivity";
import type { AtomCommand } from "../../state/runtime.ts";
import { makeShellPublicationHarness } from "./shellPublicationHarness.ts";

// This opt-in benchmark measures real cascade cost; normal CI only runs semantic tests.
const host = globalThis as typeof globalThis & {
  process: { env: Readonly<Record<string, string | undefined>>; version: string };
};

type Scenario =
  | "one"
  | "tick"
  | "equivalent-http"
  | "snapshot"
  | "http-10"
  | "http-100"
  | "http-all"
  | "chunk"
  | "separate-chunks";
const scenarios: ReadonlyArray<Scenario> = [
  "one",
  "tick",
  "equivalent-http",
  "snapshot",
  "http-10",
  "http-100",
  "http-all",
  "chunk",
  "separate-chunks",
];

// oxlint-disable-next-line t3code/no-test-timeout-or-unreasoned-skip -- Explicit opt-in CPU benchmark matrix, bounded externally too.
it.effect.skipIf(host.process.env.JONES_SHELL_PUBLICATION_SCALING !== "1")(
  "measures actual shell publication counts and uninstrumented cascade time",
  () =>
    Effect.gen(function* () {
      const baseline = host.process.env.JONES_SHELL_PUBLICATION_PHASE === "baseline";
      const report: Array<Record<string, unknown>> = [];
      for (const count of host.process.env.JONES_SHELL_PUBLICATION_TIMING_ONLY === "1" ||
      host.process.env.JONES_SHELL_PUBLICATION_S7_ONLY === "1"
        ? []
        : [250, 1000, 4000])
        for (const scenario of scenarios) {
          if (baseline && count === 4000 && !["one", "tick", "equivalent-http"].includes(scenario))
            continue;
          yield* Effect.scoped(
            Effect.gen(function* () {
              const h = yield* makeShellPublicationHarness({ count, instrument: true });
              const id = h.ids[0]!;
              let sequence = 1;
              const current = () => h.registry.get(h.snapshots(id))!;
              const event = (thread: OrchestrationV2ThreadShell, next: number) => ({
                kind: "thread.updated" as const,
                sequence: next,
                location: "active" as const,
                thread,
              });
              if (scenario === "tick") {
                const thread = {
                  ...current().threads[0]!,
                  status: "running" as const,
                  activeRunId: RunId.make("scaling-run"),
                  latestRunId: RunId.make("scaling-run"),
                  itemCount: 1,
                  visibleItemCount: 1,
                };
                yield* h.publish(id, [event(thread, ++sequence)], sequence);
              }
              const operation = Effect.fn("ShellPublication.measureOperation")(function* () {
                const before = current();
                if (scenario === "one" || scenario === "tick") {
                  const thread = before.threads[0]!;
                  yield* h.publish(
                    id,
                    [
                      event(
                        scenario === "tick"
                          ? { ...thread, itemCount: thread.itemCount + 1 }
                          : { ...thread, title: `Rename ${sequence}` },
                        ++sequence,
                      ),
                    ],
                    sequence,
                  );
                } else if (scenario === "snapshot") {
                  yield* h.publish(
                    id,
                    [
                      {
                        kind: "snapshot",
                        snapshot: {
                          ...before,
                          snapshotSequence: ++sequence,
                          threads: before.threads.map((thread) => ({
                            ...thread,
                            title: `Snapshot ${sequence}`,
                          })),
                        },
                      },
                    ],
                    sequence,
                  );
                } else if (scenario.startsWith("http") || scenario === "equivalent-http") {
                  const k =
                    scenario === "http-all"
                      ? count
                      : scenario === "http-100"
                        ? 100
                        : scenario === "http-10"
                          ? 10
                          : 0;
                  yield* h.replaceSession(id, {
                    ...before,
                    snapshotSequence: ++sequence,
                    threads: before.threads.map((thread, i) => ({
                      ...thread,
                      title: i < k ? `HTTP ${sequence}` : thread.title,
                    })),
                  });
                } else {
                  const chunks = scenario === "chunk" ? 1 : 10;
                  for (let chunk = 0; chunk < chunks; chunk++) {
                    const rows = current().threads;
                    const events = Array.from({ length: scenario === "chunk" ? 100 : 10 }, (_, i) =>
                      event(
                        { ...rows[(chunk * 10 + i) % count]!, title: `Chunk ${sequence}-${i}` },
                        ++sequence,
                      ),
                    );
                    yield* h.publish(id, events, sequence);
                  }
                }
              });
              h.resetMetrics();
              yield* operation();
              report.push({ count, scenario, kind: "counts", ...h.metrics });
              if (!baseline) {
                const l = h.metrics.publications;
                expect(h.metrics.evaluations).toBeLessThanOrEqual(2 * l);
                expect(h.metrics.memberReads).toBeLessThanOrEqual((2 * count + 16) * l);
                if (scenario === "tick" || scenario === "equivalent-http")
                  expect(h.metrics.emissions).toBe(0);
                else expect(h.metrics.emissions).toBe(scenario === "separate-chunks" ? 10 : 1);
              }
            }),
          );
        }
      // Reentrant roots have no once-only bound; retain matched before/after residual counts.
      for (const count of [250, 1000]) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const h = yield* makeShellPublicationHarness({
              count,
              environmentCount: 2,
              instrument: true,
            });
            const [home, remote] = h.ids;
            const snapshot = h.environments.get(home!)!.snapshot;
            type Input = {
              readonly environmentId: NonNullable<typeof home>;
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
            h.resetMetrics();
            const pending = snapshot.threads.slice(0, 3).map((thread, i) =>
              command.run(h.registry, {
                environmentId: home!,
                input: { threadId: thread.id, title: `Optimistic ${i}` },
              }),
            );
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
            yield* h.publish(home!, [{ kind: "snapshot", snapshot: server }], 2);
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
            report.push({
              count,
              environments: 2,
              scenario: "optimistic-mix",
              kind: "residual-counts",
              ...h.metrics,
            });
            const catalog = h.registry.get(h.catalog);
            const entries = new Map(catalog.entries);
            entries.set(remote!, { ...entries.get(remote!)!, enabled: false });
            h.resetMetrics();
            h.registry.set(h.catalog, { ...catalog, entries });
            expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(count);
            h.registry.set(h.catalog, catalog);
            expect(h.registry.get(h.threads.quietThreadShellsAtom)).toHaveLength(count * 2);
            report.push({
              count,
              environments: 2,
              scenario: "catalog-toggle",
              kind: "residual-counts",
              ...h.metrics,
            });
            const row = h.environments.get(remote!)!.snapshot.threads[0]!;
            const task = {
              ...row,
              status: "running" as const,
              delegatedFrom: {
                environmentId: home!,
                threadId: snapshot.threads[0]!.id,
                title: snapshot.threads[0]!.title,
              },
              pendingRuntimeRequest: {
                id: RuntimeRequestId.make("diagnostic-approval"),
                kind: "command" as const,
                createdAt: row.createdAt,
              },
            };
            h.resetMetrics();
            yield* h.publish(
              remote!,
              [{ kind: "thread.updated", sequence: 2, location: "active", thread: task }],
              2,
            );
            expect(
              h.registry.get(h.threads.quietThreadShellAtom(h.ref))?.pendingSubagentRequest,
            ).toBe("approval");
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
            report.push({
              count,
              environments: 2,
              scenario: "delegated-approval",
              kind: "residual-counts",
              ...h.metrics,
            });
          }),
        );
      }
      // Timing is deliberately separate from the counter-instrumented graph.
      for (const scenario of (host.process.env.JONES_SHELL_PUBLICATION_S7_ONLY === "1"
        ? []
        : baseline
          ? ["one", "tick", "equivalent-http"]
          : [
              "one",
              "tick",
              "equivalent-http",
              "snapshot",
              "http-all",
              "chunk",
              "separate-chunks",
            ]) as ReadonlyArray<Scenario>) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const h = yield* makeShellPublicationHarness({ count: 4000 });
            const id = h.ids[0]!;
            let sequence = 1;
            const current = () => h.registry.get(h.snapshots(id))!;
            if (scenario === "tick") {
              const row = current().threads[0]!;
              yield* h.publish(
                id,
                [
                  {
                    kind: "thread.updated",
                    sequence: ++sequence,
                    location: "active",
                    thread: {
                      ...row,
                      status: "running",
                      activeRunId: RunId.make("timing-run"),
                      latestRunId: RunId.make("timing-run"),
                      itemCount: 1,
                      visibleItemCount: 1,
                    },
                  },
                ],
                sequence,
              );
            }
            const samples: number[] = [];
            for (let repetition = 0; repetition < 4; repetition++) {
              const before = current();
              // oxlint-disable-next-line t3code/no-wall-clock-in-tests -- Opt-in CPU benchmark, not a correctness timeout.
              const start = performance.now();
              if (scenario === "one" || scenario === "tick") {
                const row = before.threads[0]!;
                yield* h.publish(
                  id,
                  [
                    {
                      kind: "thread.updated",
                      sequence: ++sequence,
                      location: "active",
                      thread:
                        scenario === "tick"
                          ? { ...row, itemCount: row.itemCount + 1 }
                          : { ...row, title: `Timing ${sequence}` },
                    },
                  ],
                  sequence,
                );
              } else if (scenario === "snapshot") {
                yield* h.publish(
                  id,
                  [
                    {
                      kind: "snapshot",
                      snapshot: {
                        ...before,
                        snapshotSequence: ++sequence,
                        threads: before.threads.map((row) => ({
                          ...row,
                          title: `Timing ${sequence}`,
                        })),
                      },
                    },
                  ],
                  sequence,
                );
              } else if (scenario === "chunk" || scenario === "separate-chunks") {
                const chunks = scenario === "chunk" ? 1 : 10;
                for (let chunk = 0; chunk < chunks; chunk++) {
                  const rows = current().threads;
                  const events = Array.from(
                    { length: scenario === "chunk" ? 100 : 10 },
                    (_, i) => ({
                      kind: "thread.updated" as const,
                      location: "active" as const,
                      sequence: ++sequence,
                      thread: { ...rows[chunk * 10 + i]!, title: `Burst ${sequence}` },
                    }),
                  );
                  yield* h.publish(id, events, sequence);
                }
              } else {
                yield* h.replaceSession(id, {
                  ...before,
                  snapshotSequence: ++sequence,
                  threads: before.threads.map((row) => ({
                    ...row,
                    ...(scenario === "http-all" ? { title: `Timing ${sequence}` } : {}),
                  })),
                });
              }
              // oxlint-disable-next-line t3code/no-wall-clock-in-tests -- Opt-in CPU benchmark, not a correctness timeout.
              if (repetition > 0) samples.push(performance.now() - start);
            }
            const median = [...samples].sort((a, b) => a - b)[1]!;
            report.push({
              count: 4000,
              scenario,
              kind: "timing",
              timingMeaning: "elapsed operation completion, not uninterrupted CPU occupancy",
              samplesMs: samples,
              medianMs: median,
              maxMs: Math.max(...samples),
            });
            if (!baseline && (scenario === "snapshot" || scenario === "http-all"))
              expect(median).toBeLessThanOrEqual(250);
            if (!baseline && scenario === "tick") expect(median).toBeLessThanOrEqual(16);
          }),
        );
      }
      // The coordinator captures stdout into its task-owned report outside the worktree.
      yield* Console.log(
        "JONES_SHELL_PUBLICATION_RESULT",
        JSON.stringify({
          phase: baseline ? "baseline" : "candidate",
          node: host.process.version,
          report,
        }),
      ).pipe(Effect.provideService(Console.Console, globalThis.console));
    }),
  180000,
);
