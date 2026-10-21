import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ScheduledTaskId,
  ServerSelfUpdateError,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import { userFacingDispatchErrorMessage } from "../../orchestration-v2/UserFacingErrors.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { findActivationWithdrawal } from "./followerPolicy.ts";
import { readHostWork } from "./hostWork.ts";
import * as Fence from "./UpdateAdmissionFence.ts";

type FenceService = Fence.UpdateAdmissionFence["Service"];

const failure = <A, E>(exit: Exit.Exit<A, E>) =>
  Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;

const start = [{ request: { type: "provider-turn.start" } }];
const detach = [{ request: { type: "provider-session.detach" } }];

/** The refusal phase for one new turn start, or undefined when it is admitted. */
const refusal = (fence: FenceService) =>
  fence
    .admit(start)
    .pipe(Effect.match({ onSuccess: () => undefined, onFailure: (error) => error.phase }));

const attachedFence = Effect.gen(function* () {
  const fence = yield* Fence.make;
  yield* fence.attach;
  return fence;
});

const launcherRefused = new ServerSelfUpdateError({ reason: "The launcher refused the update." });

/** Lets the fence's background settlement of a failed handoff run. */
const drain = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow, { discard: true });

const busy = (fence: FenceService) =>
  fence.raise("manual").pipe(
    Effect.match({
      onSuccess: () => false,
      onFailure: (error) => error._tag === "UpdateAdmissionBusy",
    }),
  );

/**
 * A qualified installation reduced to the order the fence depends on: preparation, the capture
 * passed to `onPrepared`, then the launcher, whose answer `launcher` scripts.
 */
const scriptedInstall =
  (
    log: Array<string>,
    script: {
      readonly beforePrepared?: Effect.Effect<void, ServerSelfUpdateError>;
      readonly capture?: Effect.Effect<void, ServerSelfUpdateError>;
      readonly launcher?: Effect.Effect<boolean>;
    } = {},
  ) =>
  (hooks: {
    readonly onPrepared: (
      capture: Effect.Effect<void, ServerSelfUpdateError>,
    ) => Effect.Effect<void, ServerSelfUpdateError>;
    readonly onAccepted: Effect.Effect<void>;
  }) =>
    Effect.gen(function* () {
      log.push("preflight");
      yield* script.beforePrepared ?? Effect.void;
      yield* hooks.onPrepared(
        (script.capture ?? Effect.void).pipe(
          Effect.tap(() => Effect.sync(() => log.push("capture"))),
        ),
      );
      log.push("launcher");
      if (!(yield* script.launcher ?? Effect.succeed(true))) return yield* launcherRefused;
      yield* hooks.onAccepted;
      return "accepted" as const;
    });

describe("UpdateAdmissionFence", () => {
  it.effect("an open fence admits all work without recording anything", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      for (let index = 0; index < 3; index++) assert.isUndefined(yield* refusal(fence));
      yield* fence.admit(detach);
      // Nothing was held: a later installation raises it and hands off normally.
      const ticket = yield* fence.raise("automatic");
      assert.isTrue(yield* ticket.handOff);
    }),
  );

  it.effect("only turn starts, steers and approval replies are fenced", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const ticket = yield* fence.raise("manual");
      assert.isTrue(yield* ticket.handOff);
      yield* fence.admit(detach);
      yield* fence.admit([]);
      for (const type of EffectOutbox.PROCESS_BOUND_EFFECT_TYPES) {
        const refused = yield* fence
          .admit([{ request: { type } }])
          .pipe(Effect.match({ onSuccess: () => false, onFailure: () => true }));
        assert.isTrue(refused, type);
      }
    }),
  );

  it.effect("an automatic fence trips instead of refusing until it hands off", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const ticket = yield* fence.raise("automatic");
      assert.isUndefined(yield* refusal(fence));
      assert.isFalse(yield* ticket.handOff);
      // A tripped fence stays raised; releasing it reopens admission and reports no refusals.
      assert.isFalse(yield* ticket.release);
      assert.isUndefined(yield* refusal(fence));
    }),
  );

  it.effect("acceptance keeps refusing and release cannot reopen it", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const ticket = yield* fence.raise("automatic");
      assert.isTrue(yield* ticket.handOff);
      assert.strictEqual(yield* refusal(fence), "handing-off");
      yield* ticket.accept;
      assert.strictEqual(yield* refusal(fence), "accepted");
      assert.isFalse(yield* ticket.release);
      assert.strictEqual(yield* refusal(fence), "accepted");
      const busy = yield* Effect.flip(fence.raise("manual"));
      assert.strictEqual(busy._tag, "UpdateAdmissionBusy");
    }),
  );

  it.effect("a stale settlement cannot reopen or accept a later installation's fence", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const first = yield* fence.raise("manual");
      assert.isTrue(yield* first.handOff);
      yield* first.settle(first.release.pipe(Effect.asVoid));
      yield* drain;
      assert.isUndefined(yield* refusal(fence));
      const second = yield* fence.raise("manual");
      assert.isTrue(yield* second.handOff);
      // A late or duplicate outcome for the first handoff arrives while the second holds the fence.
      yield* first.settle(first.release.pipe(Effect.asVoid));
      yield* first.settle(first.accept);
      yield* drain;
      assert.strictEqual(yield* refusal(fence), "handing-off");
      assert.isTrue(yield* second.release);
      assert.isUndefined(yield* refusal(fence));
    }),
  );

  it.effect("a released ticket cannot change a later installation's fence", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const first = yield* fence.raise("manual");
      assert.isFalse(yield* first.release);
      const second = yield* fence.raise("manual");
      assert.isFalse(yield* first.release);
      yield* first.accept;
      assert.isFalse(yield* first.handOff);
      assert.strictEqual(yield* refusal(fence), "installing");
      assert.isTrue(yield* second.handOff);
      assert.strictEqual(yield* refusal(fence), "handing-off");
      assert.isTrue(yield* second.release);
      assert.isUndefined(yield* refusal(fence));
    }),
  );

  it.effect("refusal messages tell the sender to send again", () =>
    Effect.sync(() => {
      assert.match(
        new Fence.UpdateAdmissionRefused({ phase: "handing-off" }).message,
        /not started\. Send it again in a moment\./,
      );
      assert.match(
        new Fence.UpdateAdmissionRefused({ phase: "accepted" }).message,
        /restarting to install an update.*Send it again once the host reconnects\./,
      );
      const wrapped = { _tag: "EventSinkWriteError", cause: { _tag: "UpdateAdmissionRefused" } };
      assert.isTrue(Fence.isUpdateAdmissionRefusal({ cause: wrapped }));
      assert.isFalse(Fence.isUpdateAdmissionRefusal({ cause: { _tag: "Other" } }));
    }),
  );
});

describe("fencedInstall", () => {
  const automatic = (
    fence: FenceService,
    install: ReturnType<typeof scriptedInstall>,
    options: {
      readonly finalCheck?: Effect.Effect<string | undefined>;
      readonly handoffOutcome?: () => Effect.Effect<"accepted" | "rejected">;
      readonly reopened?: Effect.Effect<void>;
      readonly mode?: Fence.AdmissionFenceMode;
    } = {},
  ) =>
    Fence.fencedInstall({
      fence,
      mode: options.mode ?? "automatic",
      finalCheck: options.finalCheck ?? Effect.succeed(undefined),
      handoffOutcome: options.handoffOutcome ?? (() => Effect.succeed("rejected" as const)),
      ...(options.reopened === undefined ? {} : { reopened: options.reopened }),
      install,
    });

  it.effect(
    "late check race: work started after the final check withdraws before the launcher is asked",
    () =>
      Effect.gen(function* () {
        const fence = yield* attachedFence;
        const log: Array<string> = [];
        // Before the fence, a Send that landed during the capture started a turn in the process
        // that was about to stop. Now it is recorded, trips the fence and withdraws the update.
        const exit = yield* automatic(
          fence,
          scriptedInstall(log, {
            capture: fence.admit(start).pipe(Effect.orDie),
          }),
        ).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
        const error = failure(exit);
        assert.include(
          findActivationWithdrawal(error)?.message ?? "",
          "New work started on this host after its final check.",
        );
        assert.deepStrictEqual(log, ["preflight", "capture"]);
        assert.isUndefined(yield* refusal(fence));
        const next = yield* fence.raise("automatic");
        assert.isTrue(yield* next.handOff);
      }),
  );

  it.effect("work recorded during the final check withdraws the update", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const log: Array<string> = [];
      const exit = yield* automatic(fence, scriptedInstall(log), {
        finalCheck: fence.admit(start).pipe(Effect.orDie, Effect.as(undefined)),
      }).pipe(Effect.exit);
      assert.isDefined(findActivationWithdrawal(failure(exit)));
      assert.deepStrictEqual(log, ["preflight", "capture"]);
      assert.isUndefined(yield* refusal(fence));
    }),
  );

  it.effect("a final check that finds work withdraws before capturing and reopens", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const log: Array<string> = [];
      let checkedWhileRaised: "installing" | "handing-off" | "accepted" | undefined | "unset" =
        "unset";
      const exit = yield* automatic(fence, scriptedInstall(log), {
        finalCheck: Effect.gen(function* () {
          checkedWhileRaised = yield* refusal(fence);
          return "A delegated task is running on this host.";
        }),
      }).pipe(Effect.exit);
      // The check ran with the fence raised: the automatic fence tripped rather than refused.
      assert.isUndefined(checkedWhileRaised);
      assert.include(
        findActivationWithdrawal(failure(exit))?.message ?? "",
        "A delegated task is running",
      );
      assert.deepStrictEqual(log, ["preflight"]);
      assert.isUndefined(yield* refusal(fence));
    }),
  );

  it.effect("new work during the handoff is refused and stays refused once accepted", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const log: Array<string> = [];
      let duringHandoff: string | undefined;
      const result = yield* automatic(
        fence,
        scriptedInstall(log, {
          launcher: Effect.gen(function* () {
            duringHandoff = yield* refusal(fence);
            return true;
          }),
        }),
      );
      assert.strictEqual(result, "accepted");
      assert.strictEqual(duringHandoff, "handing-off");
      assert.strictEqual(yield* refusal(fence), "accepted");
      assert.deepStrictEqual(log, ["preflight", "capture", "launcher"]);
    }),
  );

  it.effect("a refused handoff reopens admission and restarts the work it refused", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      let restarts = 0;
      const exit = yield* automatic(
        fence,
        scriptedInstall([], {
          launcher: refusal(fence).pipe(Effect.as(false)),
        }),
        { reopened: Effect.sync(() => restarts++) },
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      assert.isUndefined(findActivationWithdrawal(failure(exit)));
      yield* drain;
      assert.isUndefined(yield* refusal(fence));
      assert.strictEqual(restarts, 1);
    }),
  );

  it.effect("a refused handoff that refused nothing restarts nothing", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      let restarts = 0;
      yield* automatic(fence, scriptedInstall([], { launcher: Effect.succeed(false) }), {
        reopened: Effect.sync(() => restarts++),
      }).pipe(Effect.exit);
      yield* drain;
      assert.isUndefined(yield* refusal(fence));
      assert.strictEqual(restarts, 0);
    }),
  );

  it.effect("an uncertain handoff the launcher kept leaves admission closed", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      let restarts = 0;
      yield* automatic(
        fence,
        scriptedInstall([], { launcher: refusal(fence).pipe(Effect.as(false)) }),
        {
          handoffOutcome: () => Effect.succeed("accepted"),
          reopened: Effect.sync(() => restarts++),
        },
      ).pipe(Effect.exit);
      yield* drain;
      assert.strictEqual(yield* refusal(fence), "accepted");
      assert.strictEqual(restarts, 0);
    }),
  );

  // HFR-440-F1: an unreadable or missing launcher answer once reopened admission here.
  it.effect("an unreadable launcher outcome after a failed handoff keeps admission closed", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      let restarts = 0;
      yield* automatic(fence, scriptedInstall([], { launcher: Effect.succeed(false) }), {
        handoffOutcome: () => Effect.die("unreadable"),
        reopened: Effect.sync(() => restarts++),
      }).pipe(Effect.exit);
      yield* drain;
      assert.strictEqual(yield* refusal(fence), "handing-off");
      assert.isTrue(yield* busy(fence));
      assert.strictEqual(restarts, 0);
    }),
  );

  it.effect("an unproven handoff keeps admission closed however long it waits", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      let restarts = 0;
      yield* automatic(fence, scriptedInstall([], { launcher: Effect.succeed(false) }), {
        handoffOutcome: () => Effect.never,
        reopened: Effect.sync(() => restarts++),
      }).pipe(Effect.exit);
      yield* TestClock.adjust("1 day");
      yield* drain;
      assert.strictEqual(yield* refusal(fence), "handing-off");
      assert.isTrue(yield* busy(fence));
      assert.strictEqual(restarts, 0);
    }),
  );

  it.effect(
    "a rejection proven after the handoff failed reopens once and restarts refused work",
    () =>
      Effect.gen(function* () {
        const fence = yield* attachedFence;
        const proof = yield* Deferred.make<"accepted" | "rejected">();
        const restarted = yield* Deferred.make<void>();
        let restarts = 0;
        yield* automatic(fence, scriptedInstall([], { launcher: Effect.succeed(false) }), {
          handoffOutcome: () => Deferred.await(proof),
          reopened: Effect.sync(() => restarts++).pipe(
            Effect.andThen(Deferred.succeed(restarted, undefined)),
          ),
        }).pipe(Effect.exit);
        yield* drain;
        // Work arriving while the outcome is unknown is refused, and restarted on reopening.
        assert.strictEqual(yield* refusal(fence), "handing-off");
        assert.strictEqual(restarts, 0);
        yield* Deferred.succeed(proof, "rejected");
        yield* fence.awaitAdmission;
        yield* Deferred.await(restarted);
        yield* drain;
        assert.isUndefined(yield* refusal(fence));
        assert.strictEqual(restarts, 1);
        const next = yield* fence.raise("manual");
        assert.isTrue(yield* next.handOff);
      }),
  );

  it.effect("an acceptance proven after the handoff failed stays closed until exit", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const proof = yield* Deferred.make<"accepted" | "rejected">();
      let restarts = 0;
      yield* automatic(fence, scriptedInstall([], { launcher: Effect.succeed(false) }), {
        handoffOutcome: () => Deferred.await(proof),
        reopened: Effect.sync(() => restarts++),
      }).pipe(Effect.exit);
      yield* drain;
      assert.strictEqual(yield* refusal(fence), "handing-off");
      yield* Deferred.succeed(proof, "accepted");
      yield* drain;
      assert.strictEqual(yield* refusal(fence), "accepted");
      assert.isTrue(yield* busy(fence));
      assert.strictEqual(restarts, 0);
    }),
  );

  it.effect("cancelling during the handoff waits for the launcher's proof before reopening", () =>
    Effect.gen(function* () {
      for (const outcome of ["rejected", "accepted"] as const) {
        const fence = yield* attachedFence;
        const waiting = yield* Deferred.make<void>();
        const proof = yield* Deferred.make<"accepted" | "rejected">();
        const fiber = yield* automatic(
          fence,
          scriptedInstall([], {
            launcher: Deferred.succeed(waiting, undefined).pipe(Effect.andThen(Effect.never)),
          }),
          { handoffOutcome: () => Deferred.await(proof) },
        ).pipe(Effect.forkChild);
        yield* Deferred.await(waiting);
        assert.strictEqual(yield* refusal(fence), "handing-off");
        yield* Fiber.interrupt(fiber);
        yield* drain;
        assert.strictEqual(yield* refusal(fence), "handing-off");
        yield* Deferred.succeed(proof, outcome);
        yield* drain;
        assert.strictEqual(yield* refusal(fence), outcome === "accepted" ? "accepted" : undefined);
      }
    }),
  );

  it.effect("cancelling during the capture reopens admission", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const capturing = yield* Deferred.make<void>();
      const fiber = yield* automatic(
        fence,
        scriptedInstall([], {
          capture: Deferred.succeed(capturing, undefined).pipe(Effect.andThen(Effect.never)),
        }),
        { handoffOutcome: () => Effect.die("must not be asked before the handoff") },
      ).pipe(Effect.forkChild);
      yield* Deferred.await(capturing);
      yield* Fiber.interrupt(fiber);
      assert.isUndefined(yield* refusal(fence));
    }),
  );

  it.effect("preparation and capture failures reopen admission", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      const failed = new ServerSelfUpdateError({ reason: "Candidate preflight failed." });
      yield* automatic(fence, scriptedInstall([], { beforePrepared: Effect.fail(failed) })).pipe(
        Effect.exit,
      );
      assert.isUndefined(yield* refusal(fence));
      yield* automatic(fence, scriptedInstall([], { capture: Effect.fail(failed) })).pipe(
        Effect.exit,
      );
      assert.isUndefined(yield* refusal(fence));
      const next = yield* fence.raise("manual");
      assert.isFalse(yield* next.release);
    }),
  );

  it.effect("a manual install refuses new work and proceeds", () =>
    Effect.gen(function* () {
      const fence = yield* attachedFence;
      let duringCapture: string | undefined;
      const result = yield* automatic(
        fence,
        scriptedInstall([], {
          capture: Effect.gen(function* () {
            duringCapture = yield* refusal(fence);
          }),
        }),
        { mode: "manual" },
      );
      assert.strictEqual(result, "accepted");
      assert.strictEqual(duringCapture, "installing");
      assert.strictEqual(yield* refusal(fence), "accepted");
    }),
  );

  it.effect("an automatic install holds when no event sink consults the fence", () =>
    Effect.gen(function* () {
      const fence = yield* Fence.make;
      const log: Array<string> = [];
      const exit = yield* automatic(fence, scriptedInstall(log)).pipe(Effect.exit);
      assert.include(
        findActivationWithdrawal(failure(exit))?.message ?? "",
        "work admission is not fenced",
      );
      assert.deepStrictEqual(log, ["preflight"]);
      const ticket = yield* fence.raise("manual");
      assert.isTrue(yield* ticket.handOff);
    }),
  );
});

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Admission tests inspect durable start intent without a provider"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const orchestration = (fence: Layer.Layer<Fence.UpdateAdmissionFence>) =>
  Layer.mergeAll(
    database,
    EffectOutbox.layer.pipe(Layer.provide(database)),
    ProjectionStore.layer.pipe(Layer.provide(database)),
    ProviderReplayHarness.layerWithRegistry(
      { name: "update-admission" },
      ProviderAdapterRegistry.layerFromAdapters([adapter]),
      { databaseLayer: database, runEffectWorker: false },
    ),
  ).pipe(Layer.provideMerge(fence));

const threadId = ThreadId.make("thread:update-admission");
const fixture = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const fence = yield* Fence.UpdateAdmissionFence;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project:update-admission"),
    title: "Update admission",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const send = (text: string, type: "start_immediately" | "queue_after_active" | "defer_start") =>
    orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${text}`),
      threadId,
      messageId: MessageId.make(`message:${text}`),
      text,
      attachments: [],
      dispatchMode: { type },
      createdBy: "user",
      creationSource: "web",
    });
  const runs = () => store.getThreadProjection(threadId).pipe(Effect.map((view) => view.runs));
  const starts = () =>
    outbox
      .listByThreadId(threadId)
      .pipe(
        Effect.map((effects) =>
          effects.flatMap((effect) =>
            effect.request.type === "provider-turn.start" ? [effect.request.runId] : [],
          ),
        ),
      );
  const finish = (run: OrchestrationV2Run) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      yield* store.apply({
        id: EventId.make(`fixture:${run.id}:completed`),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "completed", completedAt: now },
      });
    });
  return { orchestrator, fence, send, runs, starts, finish };
});

describe("update admission through the event sink", () => {
  it.effect("a Send refused during the handoff records nothing and can be sent again", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const ticket = yield* f.fence.raise("automatic");
      assert.isTrue(yield* ticket.handOff);
      const error = yield* Effect.flip(f.send("during", "start_immediately"));
      assert.isTrue(Fence.isUpdateAdmissionRefusal(error));
      assert.strictEqual(
        userFacingDispatchErrorMessage(error),
        new Fence.UpdateAdmissionRefused({ phase: "handing-off" }).message,
      );
      assert.lengthOf(yield* f.runs(), 0);
      assert.lengthOf(yield* f.starts(), 0);
      // Releasing reports the refusal, so held queued runs can be started again.
      assert.isTrue(yield* ticket.release);
      // No rejected receipt was recorded, so the same command is accepted once admission reopens.
      yield* f.send("during", "start_immediately");
      const [run] = yield* f.runs();
      assert.deepStrictEqual(yield* f.starts(), [run!.id]);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );

  it.effect("a Send after the fence rises withdraws an automatic update", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const ticket = yield* f.fence.raise("automatic");
      yield* f.send("raised", "start_immediately");
      assert.lengthOf(yield* f.starts(), 1);
      assert.isFalse(yield* ticket.handOff);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );

  it.effect("the final check waits for a start admitted just before the fence rose", () => {
    const admitted = Deferred.makeUnsafe<void>();
    const commit = Deferred.makeUnsafe<void>();
    // The real fence, paused inside the start's write transaction once it has been admitted.
    const paused = Layer.effect(
      Fence.UpdateAdmissionFence,
      Effect.gen(function* () {
        const fence = yield* Fence.make;
        return Fence.UpdateAdmissionFence.of({
          ...fence,
          admit: (effects) =>
            fence
              .admit(effects)
              .pipe(
                Effect.andThen(
                  effects.some((effect) => effect.request.type === "provider-turn.start")
                    ? Deferred.succeed(admitted, undefined).pipe(
                        Effect.andThen(Deferred.await(commit)),
                      )
                    : Effect.void,
                ),
              ),
        });
      }),
    );
    return Effect.gen(function* () {
      const f = yield* fixture;
      const order: Array<string> = [];
      const send = yield* f.send("racing", "start_immediately").pipe(
        Effect.tap(() => Effect.sync(() => order.push("committed"))),
        Effect.forkChild,
      );
      yield* Deferred.await(admitted);
      const ticket = yield* f.fence.raise("automatic");
      const check = yield* readHostWork.pipe(
        Effect.tap(() => Effect.sync(() => order.push("checked"))),
        Effect.forkChild,
      );
      for (let turn = 0; turn < 50; turn++) yield* Effect.yieldNow;
      // The admitted transaction holds the SQLite writer, so the check cannot read past it.
      assert.isUndefined(check.pollUnsafe());
      yield* Deferred.succeed(commit, undefined);
      yield* Fiber.join(send);
      const work = yield* Fiber.join(check);
      assert.deepStrictEqual(order, ["committed", "checked"]);
      assert.strictEqual(work.processEffects, 1);
      // The start was admitted before the fence rose, so only the check can see it.
      assert.isTrue(yield* ticket.handOff);
    }).pipe(Effect.provide(orchestration(paused)));
  });

  it.effect("a queued run refused during the handoff stays queued and starts on reopening", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("active", "start_immediately");
      yield* f.send("queued", "queue_after_active");
      const [active, queued] = yield* f.runs();
      yield* f.finish(active!);
      let duringHandoff: string | undefined;
      const resumed = yield* Deferred.make<void>();
      const exit = yield* Fence.fencedInstall({
        fence: f.fence,
        mode: "automatic",
        handoffOutcome: () => Effect.succeed("rejected"),
        reopened: f.orchestrator.resumeQueuedRuns.pipe(
          Effect.orDie,
          Effect.andThen(Deferred.succeed(resumed, undefined)),
          Effect.asVoid,
        ),
        install: scriptedInstall([], {
          launcher: Effect.gen(function* () {
            yield* f.orchestrator.resumeQueuedRuns.pipe(Effect.orDie);
            const held = (yield* f.runs()).find((run) => run.id === queued!.id);
            duringHandoff = held?.status;
            return false;
          }).pipe(Effect.orDie),
        }),
      }).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      assert.strictEqual(duringHandoff, "queued");
      yield* Deferred.await(resumed);
      assert.notStrictEqual(
        (yield* f.runs()).find((run) => run.id === queued!.id)?.status,
        "queued",
      );
      assert.sameMembers([...(yield* f.starts())], [active!.id, queued!.id]);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );
});

describe("work that is not a turn start yet", () => {
  it.effect("a launch's preparing run trips an automatic update and is refused at handoff", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const ticket = yield* f.fence.raise("automatic");
      yield* f.send("prepare", "defer_start");
      const [preparing] = yield* f.runs();
      assert.strictEqual(preparing?.status, "preparing");
      assert.isFalse(yield* ticket.handOff);
      assert.isFalse(yield* ticket.release);
      // Behind a preparing run another launch message would only queue, which is durable.
      yield* f.finish(preparing!);
      const next = yield* f.fence.raise("automatic");
      assert.isTrue(yield* next.handOff);
      const held = yield* f.send("prepare later", "defer_start").pipe(Effect.exit);
      assert.isTrue(Fence.isUpdateAdmissionRefusal(failure(held)));
      assert.lengthOf(yield* f.runs(), 1);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );

  it.effect("a queued message behind a running turn is durable and is not fenced", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("active", "start_immediately");
      const ticket = yield* f.fence.raise("automatic");
      assert.isTrue(yield* ticket.handOff);
      yield* f.send("behind", "queue_after_active");
      assert.strictEqual((yield* f.runs())[1]?.status, "queued");
      assert.isFalse(yield* ticket.release);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );
});

describe("a scheduled or webhook dispatch held for an update", () => {
  const threadManagement = ThreadManagementService.ThreadManagementService.pipe(
    Effect.provide(
      ThreadManagementService.layer.pipe(
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(database))),
      ),
    ),
  );
  // The scheduler's call for one webhook delivery; its ids derive from the delivery.
  const delivery = {
    projectId: ProjectId.make("project:update-admission"),
    commandId: CommandId.make("scheduled-task:task-1:webhook:delivery-1"),
    threadId,
    messageId: MessageId.make("scheduled-task-message:task-1:webhook:delivery-1"),
    scheduledTaskId: ScheduledTaskId.make("task-1"),
    text: "webhook prompt",
    attachments: [],
    modelSelection,
    mode: "queue" as const,
    createdBy: "agent" as const,
    creationSource: "mcp" as const,
  };
  const settle = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow, {
    discard: true,
  });

  it.effect("waits instead of failing and runs under its command id when the fence lifts", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const threads = yield* threadManagement;
      const ticket = yield* f.fence.raise("automatic");
      assert.isTrue(yield* ticket.handOff);
      const sending = yield* threads.sendToThread(delivery).pipe(Effect.forkChild);
      yield* settle;
      assert.isUndefined(sending.pollUnsafe());
      assert.lengthOf(yield* f.runs(), 0);
      assert.isFalse(yield* threads.hasReceipt(delivery.commandId));
      // A refused handoff, a cancel or a failure reopens admission without a restart.
      assert.isTrue(yield* ticket.release);
      const sent = yield* Fiber.join(sending);
      assert.strictEqual(sent.delivery, "started");
      assert.isTrue(yield* threads.hasReceipt(delivery.commandId));
      assert.deepStrictEqual(yield* f.starts(), [sent.run.id]);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );

  it.effect("stays held once the launcher accepts, until the restart interrupts it", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const threads = yield* threadManagement;
      const ticket = yield* f.fence.raise("manual");
      assert.isTrue(yield* ticket.handOff);
      yield* ticket.accept;
      // The scheduler's run: its dispatch result decides whether the delivery is dropped.
      let recorded: Exit.Exit<unknown, unknown> | undefined;
      const run = yield* Effect.exit(threads.sendToThread(delivery)).pipe(
        Effect.tap((exit) => Effect.sync(() => (recorded = exit))),
        Effect.forkChild,
      );
      yield* settle;
      assert.isUndefined(run.pollUnsafe());
      // The restart interrupts the run from outside, so its result is never recorded.
      yield* Fiber.interrupt(run);
      assert.isUndefined(recorded);
      assert.lengthOf(yield* f.runs(), 0);
      assert.isFalse(yield* threads.hasReceipt(delivery.commandId));
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );

  it.effect("holds a scheduled launch's first message and prepares its run on reopening", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const threads = yield* threadManagement;
      const ticket = yield* f.fence.raise("manual");
      // The launch service dispatches a launched thread's first message like this.
      const launching = yield* threads
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("scheduled-task:task-1:webhook:delivery-2:initial-message"),
          threadId,
          messageId: MessageId.make("scheduled-task-message:task-1:webhook:delivery-2"),
          scheduledTaskId: ScheduledTaskId.make("task-1"),
          text: "webhook prompt",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "defer_start", workspaceStrategy: { type: "root" } },
          createdBy: "agent",
          creationSource: "mcp",
        })
        .pipe(Effect.forkChild);
      yield* settle;
      assert.isUndefined(launching.pollUnsafe());
      assert.lengthOf(yield* f.runs(), 0);
      assert.isTrue(yield* ticket.release);
      yield* Fiber.join(launching);
      assert.deepStrictEqual(
        (yield* f.runs()).map((run) => run.status),
        ["preparing"],
      );
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );

  it.effect("a person's send is still refused at once, not held", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const threads = yield* threadManagement;
      const ticket = yield* f.fence.raise("manual");
      const { scheduledTaskId: _scheduled, ...send } = delivery;
      const refused = yield* threads
        .sendToThread({ ...send, commandId: CommandId.make("send:person"), mode: "auto" })
        .pipe(Effect.exit);
      assert.isTrue(Fence.isUpdateAdmissionRefusal(failure(refused)));
      assert.lengthOf(yield* f.runs(), 0);
      assert.isTrue(yield* ticket.release);
    }).pipe(Effect.provide(orchestration(Fence.layer))),
  );
});
