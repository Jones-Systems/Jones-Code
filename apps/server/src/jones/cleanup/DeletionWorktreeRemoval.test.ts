import { assert, it } from "@effect/vitest";
import { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import type { ExecuteGitInput, ExecuteGitResult } from "../../vcs/GitVcsDriver.ts";
import * as Removal from "./DeletionWorktreeRemoval.ts";
import type { DeletionWorktreeRemovalStartV1 } from "./DeletionWorktreeRemovalTypes.ts";

const start: DeletionWorktreeRemovalStartV1 = {
  schema: "t3.deletion-worktree-removal-start/v1",
  effectId: "effect:deletion:worktree",
  bindingSha256: "a".repeat(64),
  workerId: "worker:captured",
  expectedAttempt: 2,
  target: {
    projectId: ProjectId.make("project:captured"),
    projectRoot: "/synthetic/repository",
    path: "/synthetic/target [worktree]",
    branch: "captured",
    force: false,
  },
  startedAt: "2026-10-03T00:00:00.000Z",
};
const output = (stdout: string, overrides: Partial<ExecuteGitResult> = {}): ExecuteGitResult => ({
  exitCode: 0 as ExecuteGitResult["exitCode"],
  stdout,
  stderr: "",
  stdoutTruncated: false,
  stderrTruncated: false,
  ...overrides,
});
const fixture = (
  options: {
    readonly unknown?: boolean;
    readonly truncated?: boolean;
    readonly wrongBranch?: boolean;
    readonly missingRegistration?: boolean;
    readonly unavailableFilesystem?: boolean;
    readonly initiallyAbsent?: boolean;
  } = {},
) => {
  const calls: ExecuteGitInput[] = [];
  let absent = options.initiallyAbsent ?? false;
  const git = Removal.DeletionWorktreeGit.of({
    execute: (input) =>
      Effect.sync(() => {
        calls.push(input);
        if (input.args[0] === "rev-parse") return output("/synthetic/repository/.git\n");
        if (input.args[1] === "list")
          return output(
            `worktree /synthetic/repository\0HEAD ${"b".repeat(40)}\0branch refs/heads/main\0\0` +
              (absent || options.missingRegistration
                ? ""
                : `worktree ${start.target.path}\0HEAD ${"c".repeat(40)}\0branch refs/heads/${options.wrongBranch ? "other" : "captured"}\0\0`),
            options.truncated ? { stdoutTruncated: true } : {},
          );
        absent = true;
        return output("", options.unknown ? { stdoutTruncated: true } : {});
      }),
  });
  const fs = Removal.DeletionWorktreeFilesystem.of({
    inspect: (path) =>
      Effect.succeed(
        options.unavailableFilesystem
          ? { status: "unavailable", path, reason: "fixture_unreadable" }
          : { status: absent ? "absent" : "present", path },
      ),
  });
  const layer = Removal.producerLayer.pipe(
    Layer.provide(Layer.succeed(Removal.DeletionWorktreeGit, git)),
    Layer.provide(Layer.succeed(Removal.DeletionWorktreeFilesystem, fs)),
  );
  return { calls, layer, removals: () => calls.filter((call) => call.args[1] === "remove") };
};

it.effect("invokes only the exact captured command after durable owner revalidation", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      let validated = false;
      const observation = yield* producer.executeStarted(start, 7, (captured, ordinal) =>
        Effect.sync(() => {
          assert.deepStrictEqual(captured, start);
          assert.strictEqual(ordinal, 7);
          assert.strictEqual(f.removals().length, 0);
          validated = true;
        }),
      );
      assert.isTrue(validated);
      assert.deepStrictEqual(f.removals()[0]?.args, [
        "worktree",
        "remove",
        "--",
        start.target.path,
      ]);
      assert.strictEqual(f.removals()[0]?.cwd, start.target.projectRoot);
      assert.strictEqual(observation.after.filesystem.status, "absent");
      assert.deepStrictEqual(observation.operation, {
        kind: "executed",
        exitCode: 0,
        completion: "exited",
      });
      assert.isTrue(Object.isFrozen(observation.start.target));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("unknown completion is observed without another invocation", () =>
  Effect.gen(function* () {
    const f = fixture({ unknown: true });
    yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      const observed = yield* producer.executeStarted(start, 1, () => Effect.void);
      assert.deepStrictEqual(observed.operation, {
        kind: "executed",
        exitCode: 0,
        completion: "unknown",
      });
      const repeated = yield* producer
        .executeStarted(start, 1, () => Effect.void)
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(repeated));
      const recovery = yield* producer.observeStarted(start, 1);
      assert.deepStrictEqual(recovery.operation, { kind: "reconciled", completion: "unknown" });
      assert.strictEqual(f.removals().length, 1);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("already absent requires both Git registration and lstat absence", () =>
  Effect.gen(function* () {
    const f = fixture({ initiallyAbsent: true });
    const observed = yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      return yield* producer.executeStarted(start, 1, () => Effect.void);
    }).pipe(Effect.provide(f.layer));
    assert.deepStrictEqual(observed.operation, {
      kind: "already_absent",
      completion: "not_invoked",
    });
    assert.strictEqual(f.removals().length, 0);
  }),
);

it.effect.each([
  { label: "wrong branch", wrongBranch: true },
  { label: "missing registration but present filesystem", missingRegistration: true },
  { label: "truncated Git readback", truncated: true },
  { label: "unreadable lstat", unavailableFilesystem: true },
])("refuses $label before durable revalidation or removal", (options) =>
  Effect.gen(function* () {
    const f = fixture(options);
    yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      const result = yield* producer
        .executeStarted(start, 1, () => Effect.die("invalid target must not revalidate"))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.strictEqual(f.removals().length, 0);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("stale owner start refuses execution", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      const result = yield* producer
        .executeStarted(start, 1, () =>
          Effect.fail(
            new EventSink.EventSinkWriteError({ eventCount: 0, cause: "stale-original-task" }),
          ),
        )
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.strictEqual(f.removals().length, 0);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("invalid start ordinal fails without filesystem or Git access", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      const result = yield* producer.executeStarted(start, -1, () => Effect.void).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.strictEqual(f.calls.length, 0);
    }).pipe(Effect.provide(f.layer));
  }),
);

// This service fixture only supplies the bounded deletion ports; unrelated methods
// throw if accidentally reached, so the test cannot silently expand its owner seam.
const deletionOwner = (
  ports: Pick<
    EventSink.EventSinkV2Shape,
    | "readDeletionWorktreeRemovalStart"
    | "revalidateDeletionWorktreeRemovalStart"
    | "qualifyDeletionWorktreeRemovalObservation"
  >,
): EventSink.EventSinkV2Shape =>
  new Proxy(ports as EventSink.EventSinkV2Shape, {
    get: (target, property, receiver) => {
      if (
        typeof property === "string" &&
        ![
          "readDeletionWorktreeRemovalStart",
          "revalidateDeletionWorktreeRemovalStart",
          "qualifyDeletionWorktreeRemovalObservation",
        ].includes(property)
      )
        throw new Error("Unrelated EventSink port reached by deletion fixture");
      return Reflect.get(target, property, receiver);
    },
  });

it.effect("unbound durable owner denies before any Git read or invocation", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* Effect.gen(function* () {
      const service = yield* Removal.QualifiedDeletionWorktreeRemoval;
      const error = yield* service.execute(start.effectId).pipe(Effect.flip);
      assert.strictEqual(error.reason, "native_deletion_owner_unavailable");
      assert.strictEqual(f.calls.length, 0);
    }).pipe(
      Effect.provide(
        Removal.layer.pipe(
          Layer.provide(f.layer),
          Layer.provide(Layer.succeed(EventSink.EventSinkV2, deletionOwner({}))),
        ),
      ),
    );
  }),
);

it.effect("same owner qualifies the original observation after one captured invocation", () =>
  Effect.gen(function* () {
    const f = fixture();
    let revalidated = false;
    let qualified = false;
    const owner = deletionOwner({
      readDeletionWorktreeRemovalStart: () => Effect.succeed({ start, ordinal: 3 }),
      revalidateDeletionWorktreeRemovalStart: (captured, ordinal) =>
        Effect.sync(() => {
          assert.deepStrictEqual(captured, start);
          assert.strictEqual(ordinal, 3);
          revalidated = true;
        }),
      qualifyDeletionWorktreeRemovalObservation: (observation) =>
        Effect.sync(() => {
          assert.isTrue(revalidated);
          assert.strictEqual(f.removals().length, 1);
          assert.deepStrictEqual(observation.start, start);
          assert.strictEqual(observation.startOrdinal, 3);
          qualified = true;
        }),
    });
    yield* Effect.gen(function* () {
      const service = yield* Removal.QualifiedDeletionWorktreeRemoval;
      yield* service.execute(start.effectId);
      assert.isTrue(qualified);
    }).pipe(
      Effect.provide(
        Removal.layer.pipe(
          Layer.provide(f.layer),
          Layer.provide(Layer.succeed(EventSink.EventSinkV2, owner)),
        ),
      ),
    );
  }),
);

it.effect("wrong original effect cannot substitute another owner start", () =>
  Effect.gen(function* () {
    const f = fixture();
    const owner = deletionOwner({
      readDeletionWorktreeRemovalStart: () => Effect.succeed({ start, ordinal: 3 }),
      revalidateDeletionWorktreeRemovalStart: () => Effect.void,
      qualifyDeletionWorktreeRemovalObservation: () => Effect.void,
    });
    yield* Effect.gen(function* () {
      const service = yield* Removal.QualifiedDeletionWorktreeRemoval;
      const error = yield* service.execute("effect:another-task").pipe(Effect.flip);
      assert.strictEqual(error.reason, "original_removal_start_unavailable");
      assert.strictEqual(f.calls.length, 0);
    }).pipe(
      Effect.provide(
        Removal.layer.pipe(
          Layer.provide(f.layer),
          Layer.provide(Layer.succeed(EventSink.EventSinkV2, owner)),
        ),
      ),
    );
  }),
);

it.effect.each([
  "2026-02-30T00:00:00.000Z",
  "2026-13-01T00:00:00.000Z",
  "2026-10-03T24:00:00.000Z",
])("rejects noncanonical original start time %s before Git or filesystem reads", (startedAt) =>
  Effect.gen(function* () {
    const f = fixture();
    yield* Effect.gen(function* () {
      const producer = yield* Removal.DeletionWorktreeRemoval;
      const original = { ...start, startedAt };
      const execution = yield* producer
        .executeStarted(original, 1, () => Effect.void)
        .pipe(Effect.flip);
      const observation = yield* producer.observeStarted(original, 1).pipe(Effect.flip);
      assert.strictEqual(execution.reason, "invalid_original_start_time");
      assert.strictEqual(observation.reason, "invalid_original_start_time");
      assert.strictEqual(f.calls.length, 0);
    }).pipe(Effect.provide(f.layer));
  }),
);
