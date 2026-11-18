import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { DEFAULT_SERVER_SETTINGS, ServerSettingsError, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import * as OwnerAction from "./OwnerAction.ts";
import { TargetObservation } from "./TargetPresence.ts";

const evaluation = vi.hoisted(() => ({
  state: "retired" as "retired" | "held",
}));
vi.mock("./Runtime.ts", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./Runtime.ts")>()),
  evaluate: () =>
    Effect.succeed({
      state: evaluation.state,
      reasons: evaluation.state === "retired" ? [] : ["synthetic_hold"],
      outcome: {
        state: evaluation.state,
        reasonCodes: [],
        receiptId: null,
        fingerprint: null,
        paths: [],
        details: [],
        outputObligations: [],
      },
    }),
}));

const interrupted = vi.hoisted(() => new Set<string>());
vi.mock("./InterruptedRetirement.ts", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./InterruptedRetirement.ts")>()),
  holdsAbsentCheckout: (worktree: string) => Effect.succeed(interrupted.has(worktree)),
}));

const isSettingsError = Schema.is(ServerSettingsError);
const isProjectionReadError = Schema.is(Projections.ProjectionStoreReadError);
const threadId = ThreadId.make("synthetic-thread");
const worktreePath = "/synthetic/checkout";
const fixture = (failure?: ServerSettingsError | Projections.ProjectionStoreReadError) =>
  Effect.gen(function* () {
    let threadReads = 0;
    const action = yield* OwnerAction.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(Projects.ProjectStoreV2)({}),
          Layer.mock(Settings.ServerSettingsService)({
            getSettings: isSettingsError(failure)
              ? Effect.fail(failure)
              : Effect.succeed(DEFAULT_SERVER_SETTINGS),
          }),
          Layer.mock(Projections.ProjectionStoreV2)({
            getThreadWorktreePaths: () =>
              isProjectionReadError(failure)
                ? Effect.fail(failure)
                : Effect.succeed([{ threadId, worktreePath }]),
            getThread: () =>
              Effect.sync(() => {
                threadReads++;
              }).pipe(
                Effect.andThen(Effect.fail(new Projections.ProjectionStoreReadError({ threadId }))),
              ),
          }),
        ),
      ),
    );
    return { action, threadReads: () => threadReads };
  });

const testLayer = Layer.mergeAll(
  Config.layerTest(process.cwd(), { prefix: "retirement-owner-action-" }),
  NodeSqliteClient.layer({ filename: ":memory:" }),
).pipe(Layer.provide(NodeServices.layer));
const runTest = <A, E>(program: Effect.Effect<A, E, Config.ServerConfig | SqlClient.SqlClient>) =>
  program.pipe(Effect.provide(testLayer), Effect.scoped);

describe("owner retirement proposal failures", () => {
  it.effect(
    "reports unavailable thread metadata with the known target and an actionable hold",
    () =>
      runTest(
        Effect.gen(function* () {
          const f = yield* fixture();
          const report = yield* f.action({ action: "propose" });
          expect(report.operation).toBe("propose");
          expect(report.counts).toEqual({ removed: 0, kept: 0, failed: 1 });
          expect(report.entries).toMatchObject([
            {
              outcome: "failed",
              threadId,
              path: worktreePath,
              threadTitle: null,
              projectPath: null,
              retirement: {
                state: "held",
                fingerprint: null,
                reasonCodes: ["thread_metadata_unavailable"],
                details: [{ nextAction: expect.stringContaining("thread's metadata lookup") }],
              },
            },
          ]);
        }),
      ),
  );

  it.effect("preserves the settings error before candidate metadata or evaluation", () =>
    runTest(
      Effect.gen(function* () {
        const error = new ServerSettingsError({
          settingsPath: "/synthetic/settings.json",
          operation: "read-file",
          cause: "synthetic settings failure",
        });
        const f = yield* fixture(error);
        const result = yield* f.action({ action: "propose" }).pipe(Effect.result);
        expect(Result.isFailure(result) && result.failure).toBe(error);
        expect(f.threadReads()).toBe(0);
      }),
    ),
  );

  it.effect("keeps inventory failures global instead of returning an empty success", () =>
    runTest(
      Effect.gen(function* () {
        const error = new Projections.ProjectionStoreReadError({ threadId });
        const f = yield* fixture(error);
        const result = yield* f.action({ action: "propose" }).pipe(Effect.result);
        expect(Result.isFailure(result) && result.failure.cause).toBe(error);
        expect(f.threadReads()).toBe(0);
      }),
    ),
  );
});

const retireThread = (name: string) => ({
  threadId: ThreadId.make(`thread-${name}`),
  worktreePath: `/synthetic/${name}`,
});
const retireFixture = (
  measure: (path: string) => number | null,
  presence: "present" | "absent_unregistered" = "present",
) =>
  Effect.gen(function* () {
    const candidates = [retireThread("a"), retireThread("b")];
    const threads = new Map(candidates.map((c) => [c.threadId, c]));
    const measured: string[] = [];
    const action = yield* OwnerAction.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(Projects.ProjectStoreV2)({
            get: () => Effect.succeedSome({ workspaceRoot: "/synthetic/project" } as never),
          }),
          Layer.mock(Settings.ServerSettingsService)({
            getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
          }),
          Layer.mock(Projections.ProjectionStoreV2)({
            getThreadWorktreePaths: () => Effect.succeed(candidates),
            getThread: (id) =>
              Effect.succeed({
                title: `Title ${id}`,
                projectId: "synthetic-project",
                worktreePath: threads.get(id)?.worktreePath,
                branch: "synthetic",
                updatedAt: DateTime.nowUnsafe(),
              } as never),
          }),
          Layer.succeed(TargetObservation, async () => ({ status: presence })),
        ),
      ),
    );
    const call = (input: Parameters<typeof action>[0]) =>
      action(input, {
        measure: (path) =>
          Effect.sync(() => {
            measured.push(path);
            return measure(path);
          }),
      });
    return { call, measured };
  });

describe("owner retirement results", () => {
  it.effect("bytes are reported only for removed entries", () =>
    runTest(
      Effect.gen(function* () {
        const f = yield* retireFixture((path) => (path === "/synthetic/a" ? 4096 : 2048));
        const exact = (name: string) => ({
          action: "retire" as const,
          ...retireThread(name),
          expectedFingerprint: "synthetic-fingerprint",
        });

        evaluation.state = "retired";
        const removed = yield* f.call(exact("a"));
        expect(removed.operation).toBe("retire");
        expect(removed.entries).toMatchObject([
          { outcome: "removed", path: "/synthetic/a", bytes: 4096 },
        ]);
        expect(removed.bytesFreed).toBe(4096);

        evaluation.state = "held";
        const held = yield* f.call(exact("b"));
        expect(held.entries).toMatchObject([
          { outcome: "kept", path: "/synthetic/b", bytes: 2048 },
        ]);
        expect(held.bytesFreed).toBe(0);

        const proposed = yield* f.call({ action: "propose" });
        expect(proposed.entries.every((e) => e.bytes === null)).toBe(true);
        expect(f.measured).toEqual(["/synthetic/a", "/synthetic/b"]);
      }),
    ),
  );

  it.effect("an unmeasurable checkout is still retired with null bytes", () =>
    runTest(
      Effect.gen(function* () {
        const f = yield* retireFixture(() => null);
        evaluation.state = "retired";
        const report = yield* f.call({
          action: "retire",
          ...retireThread("a"),
          expectedFingerprint: "synthetic-fingerprint",
        });
        expect(report.entries).toMatchObject([{ outcome: "removed", bytes: null }]);
        expect(report.bytesFreed).toBe(0);
      }),
    ),
  );

  it.effect("an absent checkout is evaluated only while an interrupted retirement holds it", () =>
    runTest(
      Effect.gen(function* () {
        const f = yield* retireFixture(() => null, "absent_unregistered");
        interrupted.add("/synthetic/a");
        try {
          evaluation.state = "held";
          const proposed = yield* f.call({ action: "propose" });
          expect(proposed.entries).toMatchObject([
            { path: "/synthetic/a", retirement: { state: "held", reasonCodes: [] } },
            {
              path: "/synthetic/b",
              retirement: { state: "proposed", reasonCodes: ["worktree_already_absent"] },
            },
          ]);
          evaluation.state = "retired";
          const retired = yield* f.call({
            action: "retire",
            ...retireThread("a"),
            expectedFingerprint: "interrupted-receipt",
          });
          expect(retired.entries).toMatchObject([{ outcome: "removed", path: "/synthetic/a" }]);
        } finally {
          interrupted.clear();
        }
      }),
    ),
  );

  it.effect("stale binding returns a held entry naming binding_changed", () =>
    runTest(
      Effect.gen(function* () {
        const f = yield* retireFixture(() => 1);
        const report = yield* f.call({
          action: "retire",
          threadId: ThreadId.make("thread-a"),
          worktreePath: "/synthetic/moved",
          expectedFingerprint: "synthetic-fingerprint",
        });
        expect(report.counts).toEqual({ removed: 0, kept: 1, failed: 0 });
        expect(report.entries).toMatchObject([
          {
            path: "/synthetic/moved",
            threadId: "thread-a",
            retirement: { state: "held", reasonCodes: ["binding_changed"] },
          },
        ]);
        expect(f.measured).toEqual([]);

        const incomplete = yield* f.call({ action: "retire", threadId: ThreadId.make("thread-a") });
        expect(incomplete.entries).toMatchObject([
          { retirement: { state: "held", reasonCodes: ["exact_proposal_required"] } },
        ]);
      }),
    ),
  );
});
