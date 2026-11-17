import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RunAttemptId,
  ThreadId,
  OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ProjectionStoreReadError } from "../../orchestration-v2/ProjectionStore.ts";
import { git } from "../workspaceRetirement/Filesystem.ts";
import { acquireNativeFixture, releaseNativeFixture } from "./Native.testkit.ts";
import { assertTerminalQueueReaction } from "./PreSessionQueue.testkit.ts";

interface Input {
  readonly text: string;
  readonly identity?: string;
  readonly driver?: "codex" | "claudeAgent";
  readonly nativeResume?: "same";
  readonly runtimeRecoveryRead?: () => Effect.Effect<void, ProjectionStoreReadError>;
  readonly fileSystem?: FileSystem.FileSystem;
  readonly worktreePath?: string;
  readonly worktreesDir?: string;
  readonly recreation?: {
    readonly projectRoot: string;
    readonly baseDir: string;
    readonly worktreesDir: string;
  };
}
interface Harness<E> {
  readonly start: Effect.Effect<void, E>;
  readonly startWithRetry: Effect.Effect<void, E>;
  readonly projection: () => OrchestrationV2ThreadProjection;
  readonly setProjection: (projection: OrchestrationV2ThreadProjection) => void;
  readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  readonly open: unknown;
  readonly recreateCheckout: unknown;
  readonly startRootRun: unknown;
  readonly writeIfRunCurrent: unknown;
  readonly settledRuns: ReadonlyArray<string>;
}

class NativeFixtureError extends Schema.TaggedError<NativeFixtureError>()(
  "PreSessionNativeFixtureError",
  {
    cause: Schema.Defect(),
  },
) {}

const serializeEvents = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(OrchestrationV2DomainEvent)),
);

const native = <A>(action: () => Promise<A>) =>
  Effect.tryPromise({
    try: action,
    catch: (cause) => new NativeFixtureError({ cause }),
  }).pipe(Effect.uninterruptible);

export function registerPreSessionFailureTests<E>(makeHarness: (input: Input) => Harness<E>) {
  it.effect.each(["codex", "claudeAgent"] as const)(
    "settles exhausted missing-source starts once with sanitized evidence (%s)",
    (driver) =>
      Effect.gen(function* () {
        const f = yield* Effect.acquireRelease(native(acquireNativeFixture), (fixture) =>
          native(() => releaseNativeFixture(fixture)).pipe(Effect.orDie),
        );
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const worktree = path.join(f.root, "project", "checkout");
        yield* fs.makeDirectory(path.dirname(worktree), { recursive: true });
        yield* native(() => git(f.repo, ["worktree", "move", f.checkout, worktree]));
        yield* fs.remove(worktree, { recursive: true });
        yield* native(() => git(f.repo, ["worktree", "prune"]));
        const harness = makeHarness({
          text: "Continue",
          driver,
          fileSystem: fs,
          worktreePath: worktree,
          worktreesDir: f.root,
          recreation: {
            projectRoot: f.repo,
            baseDir: path.join(f.root, "state"),
            worktreesDir: f.root,
          },
        });
        const original = harness.projection();
        const retry = yield* Effect.result(harness.startWithRetry);
        expect(retry).toMatchObject({
          _tag: "Failure",
          failure: { cause: { _tag: "RecreationHold", reason: "source_unavailable" } },
        });
        expect(harness.projection()).toEqual(original);
        expect(harness.events).toEqual([]);
        const exhausted = yield* Effect.result(harness.start);
        expect(exhausted).toMatchObject({
          _tag: "Failure",
          failure: { cause: { reason: "source_unavailable" } },
        });
        const after = harness.projection();
        expect(after.runs.at(-1)).toMatchObject({
          id: original.runs.at(-1)!.id,
          activeAttemptId: original.runs.at(-1)!.activeAttemptId,
          status: "failed",
          startedAt: null,
        });
        expect(after.attempts[0]).toMatchObject({
          id: original.attempts[0]!.id,
          status: "failed",
          startedAt: null,
        });
        expect(after.nodes[0]).toMatchObject({
          id: original.nodes[0]!.id,
          status: "failed",
          startedAt: null,
        });
        expect(after.providerThreads).toEqual(original.providerThreads);
        expect(after.providerTurns).toEqual(original.providerTurns);
        expect(after.turnItems).toHaveLength(1);
        expect(after.turnItems[0]).toMatchObject({
          type: "error",
          status: "failed",
          failure: {
            class: "provider_error",
            code: "checkout_source_unavailable",
            retryable: false,
            message:
              "The checkout is unavailable and its original identity could not be verified. This turn did not start.",
          },
        });
        expect(harness.events.map((e) => e.type)).toEqual([
          "turn-item.updated",
          "run.updated",
          "run-attempt.updated",
          "node.updated",
        ]);
        expect(serializeEvents(harness.events)).not.toContain(f.root);
        expect(harness.settledRuns).toEqual(["failed"]);
        expect(harness.writeIfRunCurrent).toHaveBeenCalledWith(
          expect.objectContaining({
            activeAttemptId: original.runs.at(-1)!.activeAttemptId,
            expectedStatus: "starting",
          }),
        );
        expect(harness.writeIfRunCurrent).not.toHaveBeenCalledWith(
          expect.objectContaining({ startRefusalReason: expect.anything() }),
        );
        yield* harness.start;
        expect(harness.events).toHaveLength(4);
        expect(harness.settledRuns).toEqual(["failed"]);
        expect(harness.recreateCheckout).not.toHaveBeenCalled();
        expect(harness.open).not.toHaveBeenCalled();
        expect(harness.startRootRun).not.toHaveBeenCalled();
        expect(yield* fs.exists(worktree)).toBe(false);
        if (driver === "codex") {
          const error = after.turnItems[0]!;
          expect(error.type).toBe("error");
          if (error.type === "error") yield* assertTerminalQueueReaction(error);
        }
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.effect(
    "retries an unrelated recovery read, then settles exhausted failure with fixed copy",
    () =>
      Effect.gen(function* () {
        const error = new ProjectionStoreReadError({
          threadId: ThreadId.make("synthetic"),
          cause: "PRIVATE_READ_DETAIL /private/workspace",
        });
        const h = makeHarness({ text: "Continue", runtimeRecoveryRead: () => Effect.fail(error) });
        const original = h.projection();
        expect(yield* Effect.result(h.startWithRetry)).toMatchObject({
          _tag: "Failure",
          failure: { cause: error },
        });
        expect(h.projection()).toEqual(original);
        expect(yield* Effect.result(h.start)).toMatchObject({
          _tag: "Failure",
          failure: { cause: error },
        });
        expect(h.projection().runs.at(-1)?.status).toBe("failed");
        expect(h.projection().turnItems[0]).toMatchObject({
          type: "error",
          failure: {
            class: "provider_error",
            code: "workspace_preflight_failed",
            message: "The workspace could not be prepared. This turn did not start.",
          },
        });
        expect(serializeEvents(h.events)).not.toContain("PRIVATE_READ_DETAIL");
        expect(h.open).not.toHaveBeenCalled();
        expect(h.startRootRun).not.toHaveBeenCalled();
      }),
  );

  it.effect("a transient pre-session read retries into exactly one ordinary start", () =>
    Effect.gen(function* () {
      let reads = 0;
      const h = makeHarness({
        text: "Continue",
        nativeResume: "same",
        runtimeRecoveryRead: () =>
          ++reads === 1
            ? Effect.fail(
                new ProjectionStoreReadError({
                  threadId: ThreadId.make("synthetic"),
                  cause: "read unavailable",
                }),
              )
            : Effect.void,
      });
      expect((yield* Effect.result(h.startWithRetry))._tag).toBe("Failure");
      expect(h.events).toEqual([]);
      yield* h.start;
      expect(h.projection().runs.at(-1)?.status).toBe("running");
      expect(h.open).toHaveBeenCalledOnce();
      expect(h.startRootRun).toHaveBeenCalledOnce();
      expect(h.projection().turnItems.filter((item) => item.type === "error")).toEqual([]);
    }),
  );

  it.effect.each(["successor", "cancelled", "completed", "running"] as const)(
    "pre-session settlement cannot overwrite a %s state captured during the failing read",
    (state) =>
      Effect.gen(function* () {
        const h = makeHarness({
          text: "Continue",
          runtimeRecoveryRead: () =>
            Effect.suspend(() => {
              const projection = h.projection();
              h.setProjection({
                ...projection,
                runs: projection.runs.map((r) => ({
                  ...r,
                  ...(state === "successor"
                    ? { activeAttemptId: RunAttemptId.make("successor") }
                    : { status: state }),
                })),
              });
              return Effect.fail(
                new ProjectionStoreReadError({
                  threadId: ThreadId.make("synthetic"),
                  cause: "read failed",
                }),
              );
            }),
        });
        expect((yield* Effect.result(h.start))._tag).toBe("Failure");
        expect(h.events).toEqual([]);
        expect(h.settledRuns).toEqual([]);
        expect(h.projection().runs.at(-1)).toMatchObject(
          state === "successor"
            ? { activeAttemptId: "successor", status: "starting" }
            : { status: state },
        );
        expect(h.projection().attempts[0]?.status).toBe("pending");
        expect(h.projection().nodes[0]?.status).toBe("pending");
        expect(h.open).not.toHaveBeenCalled();
        expect(h.startRootRun).not.toHaveBeenCalled();
      }),
  );
}
