import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ServerSettingsError, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import * as OwnerAction from "./OwnerAction.ts";

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
            getSettings:
              failure instanceof ServerSettingsError
                ? Effect.fail(failure)
                : Effect.succeed(DEFAULT_SERVER_SETTINGS),
          }),
          Layer.mock(Projections.ProjectionStoreV2)({
            getThreadWorktreePaths: () =>
              failure instanceof Projections.ProjectionStoreReadError
                ? Effect.fail(failure)
                : Effect.succeed([{ threadId, worktreePath }]),
            getThread: () =>
              Effect.sync(() => {
                threadReads++;
              }).pipe(
                Effect.andThen(
                  Effect.fail(new Projections.ProjectionStoreReadError({ threadId })),
                ),
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
  it.effect("reports unavailable thread metadata with the known target and an actionable hold", () =>
    runTest(
      Effect.gen(function* () {
        const f = yield* fixture();
        const report = yield* f.action({ action: "propose" });
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
