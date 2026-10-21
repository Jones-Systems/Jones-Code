import { ServerSelfUpdateError } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { PROCESS_BOUND_EFFECT_TYPES } from "../../orchestration-v2/EffectOutbox.ts";
import { AutomaticActivationWithdrawn } from "./followerPolicy.ts";

/**
 * An automatic installation withdraws when work arrives after its final check; a manual one is
 * explicit intent, so it refuses new work instead.
 */
export type AdmissionFenceMode = "automatic" | "manual";

/**
 * New provider work refused while this process hands off to an update. Nothing was recorded: a
 * client's command can be sent again, here if the handoff fails or to the next server once it
 * starts, and work the server starts itself, such as a queued run or a delegated result, stays
 * pending for the next server.
 */
export class UpdateAdmissionRefused extends Schema.TaggedError<UpdateAdmissionRefused>()(
  "UpdateAdmissionRefused",
  { phase: Schema.Literals(["installing", "handing-off", "accepted"]) },
) {
  override get message(): string {
    return this.phase === "accepted"
      ? "This host is restarting to install an update, so this was not started. Send it again once the host reconnects."
      : "This host is starting an update, so this was not started. Send it again in a moment.";
  }
}

export class UpdateAdmissionBusy extends Schema.TaggedError<UpdateAdmissionBusy>()(
  "UpdateAdmissionBusy",
  {},
) {
  override get message(): string {
    return "Another installation is already handing off on this host.";
  }
}

/** One installation's hold on the fence. Every method ignores a ticket that no longer holds it. */
export interface AdmissionTicket {
  /**
   * Closes admission until acceptance or release. False when new work tripped an automatic fence,
   * which then stays raised until release.
   */
  readonly handOff: Effect.Effect<boolean>;
  /** The launcher accepted: new work stays refused until this process exits. */
  readonly accept: Effect.Effect<void>;
  /**
   * Reopens admission unless the launcher accepted, and tells whether work was refused while the
   * ticket held it, so held queued runs can be started again.
   */
  readonly release: Effect.Effect<boolean>;
  /**
   * Runs `decide` in the fence's own scope, outliving the installation that handed off, while
   * admission stays closed. It ends with this process if no proof arrives.
   */
  readonly settle: (decide: Effect.Effect<void>) => Effect.Effect<void>;
}

type FenceState =
  | { readonly phase: "open" }
  | {
      readonly phase: "raised";
      readonly ticket: number;
      readonly mode: AdmissionFenceMode;
      readonly tripped: boolean;
      readonly refused: boolean;
    }
  | { readonly phase: "handing-off"; readonly ticket: number; readonly refused: boolean }
  | { readonly phase: "accepted"; readonly ticket: number };

type EffectRequests = ReadonlyArray<{ readonly request: { readonly type: string } }>;
type RecordedEvents = ReadonlyArray<{ readonly type: string; readonly payload?: unknown }>;

const processBound = new Set<string>(PROCESS_BOUND_EFFECT_TYPES);
/**
 * A process-bound effect, or a run created to prepare its workspace first. That run has no
 * effect yet: this process provisions its worktree and then starts it.
 */
const startsProviderWork = (effects: EffectRequests, events: RecordedEvents) =>
  effects.some((effect) => processBound.has(effect.request.type)) ||
  events.some(
    (event) =>
      event.type === "run.created" &&
      (event.payload as { readonly status?: unknown } | undefined)?.status === "preparing",
  );

/**
 * Keeps new provider work out of a process that is handing off to an update. The event sink
 * checks it inside the transaction that records a process-bound effect (a turn start, steer or
 * approval reply) or creates a run that prepares a workspace, for commands and for the
 * orchestrator's own writes, so every origin passes it: sends, launches, queued runs, delegated
 * tasks and their results, scheduled and webhook runs and agents' tools. A queued message is
 * durable, and writes that settle a running attempt record no new provider work; neither is
 * checked. While the fence is open a check reads one reference and never waits, so it
 * serializes nothing. A scheduled task's message is held until admission reopens rather than
 * refused; see `holdScheduledDispatch`.
 *
 * The final check of an automatic installation runs on the SQLite writer after the fence is
 * raised. A transaction admitted just before the fence went up holds the writer, so that check
 * waits for it and sees its work; one that starts later trips the fence.
 */
export class UpdateAdmissionFence extends Context.Service<
  UpdateAdmissionFence,
  {
    /**
     * Admits a write recording `effects` and `events`. New provider work is refused while an
     * installation holds the fence, and withdraws an automatic installation that has not yet
     * handed off.
     */
    readonly admit: (
      effects: EffectRequests,
      events?: RecordedEvents,
    ) => Effect.Effect<void, UpdateAdmissionRefused>;
    /**
     * Waits until new provider work would be admitted again: the installation released the fence
     * or is still in its final check. Once the launcher accepts, only this process's exit ends it.
     */
    readonly awaitAdmission: Effect.Effect<void>;
    /** Called by each event sink that consults the fence. */
    readonly attach: Effect.Effect<void>;
    /** Whether any event sink consults the fence; automatic installations hold without one. */
    readonly attached: Effect.Effect<boolean>;
    readonly raise: (
      mode: AdmissionFenceMode,
    ) => Effect.Effect<AdmissionTicket, UpdateAdmissionBusy>;
  }
>()("t3/jones/updates/UpdateAdmissionFence") {}

export const make = Effect.gen(function* () {
  const scope = yield* Effect.scope;
  const state = yield* Ref.make<FenceState>({ phase: "open" });
  /** Completed, and replaced, each time a release reopens admission. */
  const reopenedSignal = yield* Ref.make(Deferred.makeUnsafe<void>());
  const sinks = yield* Ref.make(0);
  let tickets = 0;
  const refused = (current: FenceState): Option.Option<UpdateAdmissionRefused["phase"]> =>
    current.phase === "accepted"
      ? Option.some("accepted" as const)
      : current.phase === "handing-off"
        ? Option.some("handing-off" as const)
        : current.phase === "raised" && current.mode === "manual"
          ? Option.some("installing" as const)
          : Option.none();
  const note = (current: FenceState): FenceState =>
    current.phase === "raised"
      ? current.mode === "automatic"
        ? { ...current, tripped: true }
        : { ...current, refused: true }
      : current.phase === "handing-off"
        ? { ...current, refused: true }
        : current;
  const admit = (effects: EffectRequests, events: RecordedEvents = []) =>
    startsProviderWork(effects, events)
      ? Ref.modify(state, (current) => [refused(current), note(current)] as const).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.void,
              onSome: (phase) => Effect.fail(new UpdateAdmissionRefused({ phase })),
            }),
          ),
        )
      : Effect.void;
  const signalReopened = Ref.getAndSet(reopenedSignal, Deferred.makeUnsafe<void>()).pipe(
    Effect.flatMap((signal) => Deferred.succeed(signal, undefined)),
  );
  const awaitAdmission: Effect.Effect<void> = Effect.gen(function* () {
    for (;;) {
      // The signal is read first, so a release between the two reads has already completed it.
      const signal = yield* Ref.get(reopenedSignal);
      if (Option.isNone(refused(yield* Ref.get(state)))) return;
      yield* Deferred.await(signal);
    }
  });
  const raise = (mode: AdmissionFenceMode) =>
    Effect.gen(function* () {
      const ticket = ++tickets;
      const raised = yield* Ref.modify(state, (current) =>
        current.phase === "open"
          ? ([true, { phase: "raised", ticket, mode, tripped: false, refused: false }] as const)
          : ([false, current] as const),
      );
      if (!raised) return yield* new UpdateAdmissionBusy();
      const holds = (current: FenceState) => current.phase !== "open" && current.ticket === ticket;
      return {
        handOff: Ref.modify(state, (current): readonly [boolean, FenceState] =>
          !holds(current) || current.phase !== "raised"
            ? [holds(current), current]
            : current.tripped
              ? [false, current]
              : [true, { phase: "handing-off", ticket, refused: current.refused }],
        ),
        accept: Ref.update(state, (current): FenceState =>
          holds(current) ? { phase: "accepted", ticket } : current,
        ),
        release: Ref.modify(state, (current): readonly [Option.Option<boolean>, FenceState] =>
          holds(current) && (current.phase === "raised" || current.phase === "handing-off")
            ? [Option.some(current.refused), { phase: "open" }]
            : [Option.none(), current],
        ).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.succeed(false),
              onSome: (refusedWork) => signalReopened.pipe(Effect.as(refusedWork)),
            }),
          ),
        ),
        settle: (decide) => Effect.forkIn(decide, scope).pipe(Effect.asVoid),
      } satisfies AdmissionTicket;
    });
  return UpdateAdmissionFence.of({
    admit,
    awaitAdmission,
    attach: Ref.update(sinks, (count) => count + 1),
    attached: Ref.get(sinks).pipe(Effect.map((count) => count > 0)),
    raise,
  });
});

export const layer = Layer.effect(UpdateAdmissionFence, make);

/** Whether a dispatch failed only because the fence refused it, so nothing was recorded. */
export function isUpdateAdmissionRefusal(error: unknown): boolean {
  let current = error;
  for (let depth = 0; depth < 8 && current !== null && typeof current === "object"; depth++) {
    if ((current as { readonly _tag?: unknown })._tag === "UpdateAdmissionRefused") return true;
    current = (current as { readonly cause?: unknown }).cause;
  }
  return false;
}

/**
 * Runs a scheduled task's dispatch, holding it while the fence refuses new provider work instead
 * of failing it. The scheduler would record a refusal as a failed run and drop an accepted webhook
 * delivery with it. A held dispatch records nothing: it runs under the same command id once
 * admission reopens here, and the restart that follows an accepted handoff interrupts it from
 * outside, which leaves the delivery for the next server to run.
 */
export const holdScheduledDispatch = <A, E, R>(
  fence: Option.Option<UpdateAdmissionFence["Service"]>,
  dispatch: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => {
  if (Option.isNone(fence)) return dispatch;
  const attempt: Effect.Effect<A, E, R> = dispatch.pipe(
    Effect.catchIf(isUpdateAdmissionRefusal, () =>
      Effect.logInfo("Scheduled task dispatch held while this host hands off to an update").pipe(
        Effect.andThen(fence.value.awaitAdmission),
        Effect.andThen(Effect.suspend(() => attempt)),
      ),
    ),
  );
  return attempt;
};

const withdrawn = (reason: string) =>
  new ServerSelfUpdateError({
    reason: `The automatic update was withdrawn: ${reason}`,
    cause: new AutomaticActivationWithdrawn(reason),
  });

/**
 * Runs one qualified installation with admission fenced from its final check until the launcher
 * accepts it. `install` must call `onPrepared` last before the launcher handoff, passing its
 * continuity capture, and `onAccepted` once the launcher accepts. A failure before the handoff
 * reopens admission. After a failed handoff admission stays closed while `handoffOutcome` waits,
 * in the background, for the launcher's proof: accepted keeps it closed until this process exits,
 * rejected reopens it. No proof, a timeout or an unreadable receipt never reopens it. When
 * admission reopens after refusing work, `reopened` restarts what the refusal held.
 */
export const fencedInstall = <A>(options: {
  readonly fence: UpdateAdmissionFence["Service"];
  readonly mode: AdmissionFenceMode;
  /** The final quiet check, made with the fence raised; a reason withdraws the installation. */
  readonly finalCheck?: Effect.Effect<string | undefined>;
  readonly handoffOutcome: (
    cause: Cause.Cause<ServerSelfUpdateError>,
  ) => Effect.Effect<"accepted" | "rejected">;
  readonly reopened?: Effect.Effect<void>;
  readonly install: (hooks: {
    readonly onPrepared: (
      capture: Effect.Effect<void, ServerSelfUpdateError>,
    ) => Effect.Effect<void, ServerSelfUpdateError>;
    readonly onAccepted: Effect.Effect<void>;
  }) => Effect.Effect<A, ServerSelfUpdateError>;
}): Effect.Effect<A, ServerSelfUpdateError> =>
  Effect.suspend(() => {
    let ticket: AdmissionTicket | undefined;
    let handedOff = false;
    let accepted = false;
    const onPrepared = (capture: Effect.Effect<void, ServerSelfUpdateError>) =>
      Effect.gen(function* () {
        if (options.mode === "automatic" && !(yield* options.fence.attached))
          return yield* withdrawn("work admission is not fenced on this host.");
        ticket = yield* options.fence
          .raise(options.mode)
          .pipe(
            Effect.mapError((cause) => new ServerSelfUpdateError({ reason: cause.message, cause })),
          );
        const reason = options.finalCheck === undefined ? undefined : yield* options.finalCheck;
        if (reason !== undefined) return yield* withdrawn(reason);
        yield* capture;
        if (!(yield* ticket.handOff))
          return yield* withdrawn("New work started on this host after its final check.");
        handedOff = true;
      });
    const onAccepted = Effect.suspend(() => {
      accepted = true;
      return ticket?.accept ?? Effect.void;
    });
    return options.install({ onPrepared, onAccepted }).pipe(
      Effect.onExit((exit) =>
        Effect.suspend(() => {
          const held = ticket;
          if (held === undefined || accepted) return Effect.void;
          // The launcher returns only after accepting the operation.
          if (Exit.isSuccess(exit)) return held.accept;
          const release = held.release.pipe(
            Effect.flatMap((refused) =>
              refused && options.reopened !== undefined ? options.reopened : Effect.void,
            ),
          );
          if (!handedOff) return release;
          return held.settle(
            options.handoffOutcome(exit.cause).pipe(
              Effect.flatMap((outcome) => (outcome === "accepted" ? held.accept : release)),
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.void
                  : Effect.logWarning(
                      "Update handoff outcome is unproven; admission stays closed",
                      {
                        cause,
                      },
                    ),
              ),
            ),
          );
        }),
      ),
    );
  });
