import { it, expect } from "@effect/vitest";
import { CommandId, ThreadId, type OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Duty from "../workspaceRetirement/Duty.ts";
import * as ReceiptStore from "../workspaceRetirement/Receipts.ts";
import type { RetirementReceipt } from "../workspaceRetirement/Receipts.ts";
import { RetirementReceiptJson } from "../workspaceRetirement/DurableCodecs.ts";
import * as Boundary from "../workspaceLifecycle/NativeBoundary.ts";
import { LifecycleConfiguration } from "../workspaceLifecycle/NativeSource.ts";
import { WorkspaceLifecycle } from "../workspaceLifecycle/WorkspaceLifecycle.ts";
import { git, exists } from "../workspaceRetirement/Filesystem.ts";
import { recreate, RecreationHold, type RecreationInput } from "./Native.ts";
import { acquireNativeFixture, releaseNativeFixture, partialSource } from "./Native.testkit.ts";
import { deferRestore, prepareTurn, withTurnClaims } from "./Runtime.ts";

const nativeOperation = <A>(action: () => Promise<A>) =>
  Effect.tryPromise({
    try: action,
    catch: (cause) => new Error("Native J4 fixture operation failed", { cause }),
  }).pipe(Effect.uninterruptible);
const nativeFixture = Effect.acquireRelease(
  nativeOperation(acquireNativeFixture),
  (fixture) => nativeOperation(() => releaseNativeFixture(fixture)).pipe(Effect.orDie),
);

it.effect("a claimed next turn cannot use a present partial recreation or replay its native add", () =>
  Effect.gen(function* () {
    const f = yield* nativeFixture;
    const source = yield* nativeOperation(() => partialSource(f.repo, f.checkout));
    yield* nativeOperation(() =>
      expect(recreate({ ...f.input, source })).rejects.toMatchObject({ reason: "unknown" }),
    );
    const registrations = yield* nativeOperation(() =>
      git(f.repo, ["worktree", "list", "--porcelain", "-z"]),
    );
    let providerUses = 0;
    let nativeCalls = 0;
    yield* Effect.gen(function* () {
      const store = yield* ReceiptStore.Receipts.pipe(Effect.provide(ReceiptStore.layer));
      const result = yield* withTurnClaims(
        "partial-next-turn",
        Duty.withDuty(
          f.checkout,
          "turn",
          "partial-next-turn",
          prepareTurn({
            threadId: "partial",
            runId: "partial-next-turn",
            projectRoot: f.repo,
            worktree: f.checkout,
            managed: true,
            nativeGuarded: false,
          }).pipe(Effect.tap(() => Effect.sync(() => providerUses++))),
        ),
      ).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure).toMatchObject({ reason: "unknown" });
      expect(yield* store.claims(f.checkout)).toEqual([]);
    }).pipe(
      Effect.provide(
        Layer.mock(Git.GitVcsDriver)({
          recreateCheckout: (input: RecreationInput) =>
            Effect.sync(() => nativeCalls++).pipe(Effect.andThen(nativeRecreate(input))),
        }),
      ),
      Effect.provide(Sqlite.layerMemory),
    );
    expect(providerUses).toBe(0);
    expect(nativeCalls).toBe(0);
    expect(
      yield* nativeOperation(() => git(f.repo, ["worktree", "list", "--porcelain", "-z"])),
    ).toEqual(registrations);
    expect(yield* nativeOperation(() => exists(f.input.alternatePath))).toBe(false);
  }).pipe(Effect.scoped),
);

it.effect("real turn claims cover recreation and provider lifetime; retirement conflicts in either admission order", () =>
  Effect.gen(function* () {
    const f = yield* nativeFixture;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const store = yield* ReceiptStore.Receipts.pipe(Effect.provide(ReceiptStore.layer));
        const receipt: RetirementReceipt = {
          schema: "worktree-retirement/v1",
          id: "retirement",
          incarnation: f.source.incarnation,
          cause: "settlement",
          state: "intent",
          binding: f.source.binding,
          qualification: {
            schema: "workspace-qualification/v1",
            authority_effect: "none",
            outcome: "retirable",
            decision: "propose",
            level: "branch-retained",
            reasons: [],
            observationMs: 0,
            head: f.source.head,
            branch: f.source.branch,
          },
          coverage: {},
          preservationRefs: [],
          journal: "retirement",
          disposed: [],
          reasons: [],
          limits: [],
          bytes: { logical: null, allocated: null, statvfs_delta: null },
        };
        // The reservation-first ordering refuses the real native checkout claim atomically.
        yield* store.begin(receipt);
        const refused = yield* store
          .claim({
            id: "during-removal",
            worktree: f.checkout,
            kind: "turn",
            owner: "run",
            holder: "{}",
          })
          .pipe(Effect.result);
        expect(refused._tag).toBe("Failure");
        if (refused._tag === "Failure") expect(refused.failure.reason).toBe("retiring");
        yield* nativeOperation(() => git(f.repo, ["worktree", "remove", f.checkout]));
        yield* store.finish({ ...receipt, state: "retired" });
        const running = yield* Deferred.make<void, Error>(),
          complete = yield* Deferred.make<void>();
        const provider = yield* withTurnClaims(
          "turn-j4",
          Duty.withDuty(
            f.checkout,
            "turn",
            "turn-j4",
            Effect.gen(function* () {
              const result = yield* prepareTurn({
                threadId: "thread",
                runId: "turn-j4",
                projectRoot: f.repo,
                worktree: f.checkout,
                managed: true,
                nativeGuarded: false,
              });
              expect(result?.head).toBe(f.source.head);
              const live = yield* store.claims(f.checkout);
              expect(live.some((c) => c.incarnation_id !== null)).toBe(true);
              yield* Deferred.succeed(running, undefined);
              yield* Deferred.await(complete);
            }),
          ),
        ).pipe(
          Effect.onExit((exit) =>
            Exit.isFailure(exit)
              ? Deferred.fail(
                  running,
                  new Error("Claimed recreation provider failed before readiness", {
                    cause: exit.cause,
                  }),
                )
              : Effect.void,
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(running);
        // The start-first ordering retains actual turn claims through provider execution.
        const blocked = yield* store
          .begin({ ...receipt, id: "next-retirement", incarnation: "new-incarnation" })
          .pipe(Effect.result);
        expect(blocked._tag).toBe("Failure");
        if (blocked._tag === "Failure") expect(blocked.failure.reason).toBe("live_duty");
        expect((yield* store.claims(f.checkout)).length).toBeGreaterThan(0);
        yield* Deferred.succeed(complete, undefined);
        yield* Fiber.join(provider);
        expect(yield* store.claims(f.checkout)).toEqual([]);
        expect(
          (yield* sql`SELECT state FROM jones_workspace_retirement_receipts WHERE receipt_id='retirement'`)[0]
            ?.state,
        ).toBe("retired");
      }),
    ).pipe(Effect.provide(nativeDriver), Effect.provide(Sqlite.layerMemory));
  }).pipe(Effect.scoped),
);

it.effect("settlement restore commits placement without recreation; the next claimed turn recreates after the terminal receipt", () =>
  Effect.gen(function* () {
    const f = yield* nativeFixture;
    yield* nativeOperation(() => git(f.repo, ["worktree", "remove", f.checkout]));
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:restore-j4"),
        now = DateTime.nowUnsafe();
      const receipt: RetirementReceipt = {
        schema: "worktree-retirement/v1",
        id: "restore-receipt",
        incarnation: f.source.incarnation,
        cause: "settlement",
        state: "retired",
        binding: f.source.binding,
        qualification: {
          schema: "workspace-qualification/v1",
          authority_effect: "none",
          outcome: "retirable",
          decision: "propose",
          level: "branch-retained",
          reasons: [],
          observationMs: 0,
          head: f.source.head,
          branch: f.source.branch,
        },
        coverage: {},
        preservationRefs: [],
        journal: "restore-receipt",
        disposed: [],
        reasons: [],
        limits: [],
        bytes: { logical: null, allocated: null, statvfs_delta: null },
      };
      // Only the thread binding is consumed by the native restore boundary.
      yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
      VALUES(${threadId},'project','Restore fixture','codex','full-access','default','2026-11-06T00:00:00Z','2026-11-06T00:00:00Z',${JSON.stringify({ id: threadId, projectId: "project", worktreePath: f.checkout, branch: "task" })})`;
      yield* sql`INSERT INTO jones_workspace_retirement_receipts VALUES(${receipt.id},${receipt.incarnation},${f.checkout},'retired',${yield* Schema.encodeEffect(RetirementReceiptJson)(receipt)})`;
      yield* sql`INSERT INTO jones_workspace_retirement_state VALUES(${f.checkout},${receipt.incarnation},'retired',${receipt.id})`;
      expect(yield* deferRestore(threadId)).toBe(true);
      let committed = false;
      const command = {
        type: "thread.settlement.restore" as const,
        threadId,
        commandId: CommandId.make("restore"),
        settleCommandId: CommandId.make("settle"),
      };
      const event = {
        type: "thread.unsettled",
        threadId,
        occurredAt: now,
        payload: {},
      } as unknown as OrchestrationV2DomainEvent;
      yield* Boundary.commitCommand(
        command,
        [event],
        Boundary.validateCommand(command, [event]).pipe(
          Effect.andThen(
            Effect.sync(() => {
              committed = true;
            }),
          ),
        ),
      );
      expect(committed).toBe(true);
      expect(yield* Effect.promise(() => exists(f.checkout))).toBe(false);
      yield* withTurnClaims(
        "restore-next-turn",
        Duty.withDuty(
          f.checkout,
          "turn",
          "restore-next-turn",
          prepareTurn({
            threadId,
            runId: "restore-next-turn",
            projectRoot: f.repo,
            worktree: f.checkout,
            managed: true,
            nativeGuarded: false,
          }),
        ),
      );
      expect(yield* Effect.promise(() => exists(f.checkout))).toBe(true);
    }).pipe(
      Effect.provideService(LifecycleConfiguration, {
        environmentId: "fixture",
        projects: new Map(),
      }),
      Effect.provide(
        Layer.mock(WorkspaceLifecycle)({
          resume: () => Effect.die("restore must defer registry and checkout work"),
          validate: () => Effect.die("restore must defer registry work"),
        }),
      ),
      Effect.provide(nativeDriver),
      Effect.provide(Sqlite.layerMemory),
    );
  }).pipe(Effect.scoped),
);

it.effect("a completed manual recreation permits the next claimed turn after a normal commit", () =>
  Effect.gen(function* () {
    const f = yield* nativeFixture;
    const fs = yield* FileSystem.FileSystem;
    const store = yield* ReceiptStore.Receipts.pipe(Effect.provide(ReceiptStore.layer));
    let nativeCalls = 0;
    let providerUses = 0;
    const turn = (runId: string) =>
      withTurnClaims(
        runId,
        Duty.withDuty(
          f.checkout,
          "turn",
          runId,
          prepareTurn({
            threadId: "continued",
            runId,
            projectRoot: f.repo,
            worktree: f.checkout,
            managed: true,
            nativeGuarded: false,
          }).pipe(Effect.tap(() => Effect.sync(() => providerUses++))),
        ),
      );
    expect(yield* turn("ordinary-present")).toBeNull();
    yield* fs.remove(f.checkout, { recursive: true });
    const recreated = yield* turn("recreated").pipe(
      Effect.provide(
        Layer.mock(Git.GitVcsDriver)({
          recreateCheckout: (input: RecreationInput) =>
            Effect.sync(() => nativeCalls++).pipe(Effect.andThen(nativeRecreate(input))),
        }),
      ),
    );
    expect(recreated?.head).toBe(f.source.head);
    const registrations = yield* nativeOperation(() =>
      git(f.repo, ["worktree", "list", "--porcelain", "-z"]),
    );
    yield* nativeOperation(() =>
      git(f.checkout, [
        "-c",
        "user.name=J4 fixture",
        "-c",
        "user.email=j4@example.invalid",
        "commit",
        "--allow-empty",
        "-m",
        "next ordinary commit",
      ]),
    );
    const advanced = yield* nativeOperation(() => git(f.checkout, ["rev-parse", "HEAD"]));
    expect(advanced.toString().trim()).not.toBe(f.source.head);
    expect(yield* turn("continued")).toBeNull();
    expect(providerUses).toBe(3);
    expect(nativeCalls).toBe(1);
    expect(yield* nativeOperation(() => git(f.checkout, ["rev-parse", "HEAD"]))).toEqual(advanced);
    const current = yield* nativeOperation(() =>
      git(f.repo, ["worktree", "list", "--porcelain", "-z"]),
    );
    expect(current.toString().replace(advanced.toString().trim(), f.source.head)).toBe(
      registrations.toString(),
    );
    expect(yield* store.claims(f.checkout)).toEqual([]);
    expect(yield* nativeOperation(() => exists(f.input.alternatePath))).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.provide(Sqlite.layerMemory)),
);

function nativeRecreate(input: RecreationInput) {
  return Effect.tryPromise({
    try: () => recreate(input),
    catch: (cause) =>
      cause instanceof RecreationHold
        ? cause
        : new RecreationHold(
            "unknown",
            input.source.binding.path,
            input.owner,
            "fixture native observation unavailable",
            "Inspect the retained native error.",
            { cause },
          ),
  });
}
const nativeDriver = Layer.mock(Git.GitVcsDriver)({
  recreateCheckout: nativeRecreate,
});
