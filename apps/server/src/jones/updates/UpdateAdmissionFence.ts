import { ServerSelfUpdateError } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
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
 * serializes nothing.
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
  const state = yield* Ref.make<FenceState>({ phase: "open" });
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
        release: Ref.modify(state, (current): readonly [boolean, FenceState] =>
          holds(current) && (current.phase === "raised" || current.phase === "handing-off")
            ? [current.refused, { phase: "open" }]
            : [false, current],
        ),
      } satisfies AdmissionTicket;
    });
  return UpdateAdmissionFence.of({
    admit,
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
 * Whether a failed dispatch was held for an update. The work was not recorded under its command
 * id, so a caller that keeps it, such as an accepted webhook delivery, dispatches it again under
 * the same command id once admission reopens here or on the next server. It is not a failure.
 */
export const isUpdateAdmissionHold = (cause: Cause.Cause<unknown>): boolean =>
  cause.reasons.some(
    (reason) => Cause.isFailReason(reason) && isUpdateAdmissionRefusal(reason.error),
  );

const withdrawn = (reason: string) =>
  new ServerSelfUpdateError({
    reason: `The automatic update was withdrawn: ${reason}`,
    cause: new AutomaticActivationWithdrawn(reason),
  });

/**
 * Runs one qualified installation with admission fenced from its final check until the launcher
 * accepts it. `install` must call `onPrepared` last before the launcher handoff, passing its
 * continuity capture, and `onAccepted` once the launcher accepts. Any failure before acceptance
 * reopens admission; after a failed handoff `launcherHolds` decides whether the launcher took the
 * operation anyway, in which case admission stays closed until this process exits. When admission
 * reopens after refusing work, `reopened` restarts what the refusal held.
 */
export const fencedInstall = <A>(options: {
  readonly fence: UpdateAdmissionFence["Service"];
  readonly mode: AdmissionFenceMode;
  /** The final quiet check, made with the fence raised; a reason withdraws the installation. */
  readonly finalCheck?: Effect.Effect<string | undefined>;
  readonly launcherHolds: Effect.Effect<boolean>;
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
          return options.launcherHolds.pipe(
            Effect.catchCause(() => Effect.succeed(false)),
            Effect.flatMap((holds) => (holds ? held.accept : release)),
          );
        }),
      ),
    );
  });
