import type {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderRuntimeBinding,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { ProviderAdapterV2Event } from "../../orchestration-v2/ProviderAdapter.ts";

export class ProviderEventOriginStaleError extends Schema.TaggedError<ProviderEventOriginStaleError>()(
  "ProviderEventOriginStaleError",
  {},
) {
  override get message(): string {
    return "The captured provider event source is no longer current.";
  }
}

export interface ProviderEventTurnOrigin {
  readonly binding: ProviderRuntimeBinding;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly providerTurnId: ProviderTurnId;
}

export interface ProviderEventProducerOrigin {
  readonly token: object;
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly runtimeGeneration?: string;
  readonly revalidateCurrent: Effect.Effect<void, ProviderEventOriginStaleError>;
}

export interface ProviderEventOrigin {
  readonly producer: ProviderEventProducerOrigin;
  readonly turn?: ProviderEventTurnOrigin;
}

export interface ProviderEventProducer {
  readonly origin: ProviderEventProducerOrigin;
  readonly accepting: boolean;
  readonly drain: () => void;
  readonly retire: () => void;
}

export function makeProviderEventProducer(
  identity: Omit<ProviderEventProducerOrigin, "token" | "revalidateCurrent">,
  isCurrent: Effect.Effect<boolean> = Effect.succeed(true),
): ProviderEventProducer {
  let state: "accepting" | "draining" | "retired" = "accepting";
  const retired = () => state === "retired";
  const origin: ProviderEventProducerOrigin = {
    ...identity,
    token: {},
    revalidateCurrent: Effect.gen(function* () {
      if (retired() || !(yield* isCurrent) || retired()) {
        return yield* new ProviderEventOriginStaleError({});
      }
    }),
  };
  return {
    origin,
    get accepting() {
      return state === "accepting";
    },
    drain: () => {
      if (state === "accepting") state = "draining";
    },
    retire: () => {
      state = "retired";
    },
  };
}

export function revalidateProviderEventOrigin(
  event: ProviderAdapterV2Event,
  runtime: {
    readonly driver: ProviderDriverKind;
    readonly instanceId: ProviderInstanceId;
    readonly providerSessionId: ProviderSessionId;
    readonly eventOriginMode?: "captured";
  },
): Effect.Effect<void, ProviderEventOriginStaleError> {
  return Effect.gen(function* () {
    const origin = readProviderEventOrigin(event);
    if (origin === undefined) {
      if (runtime.eventOriginMode === "captured") return yield* new ProviderEventOriginStaleError({});
      return;
    }
    const { producer, turn } = origin;
    if (
      producer.driver !== event.driver ||
      producer.driver !== runtime.driver ||
      producer.instanceId !== runtime.instanceId ||
      producer.providerSessionId !== runtime.providerSessionId ||
      (event.type === "runtime_identity.observed" &&
        (event.binding.driver !== producer.driver ||
          event.binding.providerInstanceId !== producer.instanceId ||
          event.binding.providerSessionId !== producer.providerSessionId ||
          (producer.runtimeGeneration !== undefined &&
            event.binding.runtimeGeneration !== producer.runtimeGeneration))) ||
      (event.type !== "runtime_identity.observed" && event.runtimeEvidence !== undefined &&
        (event.runtimeEvidence.driver !== producer.driver ||
          event.runtimeEvidence.providerInstanceId !== producer.instanceId ||
          event.runtimeEvidence.providerSessionId !== producer.providerSessionId ||
          (producer.runtimeGeneration !== undefined &&
            event.runtimeEvidence.runtimeGeneration !== producer.runtimeGeneration))) ||
      (turn !== undefined &&
        (turn.binding.driver !== producer.driver ||
          turn.binding.providerInstanceId !== producer.instanceId ||
          turn.binding.providerSessionId !== producer.providerSessionId ||
          (producer.runtimeGeneration !== undefined &&
            turn.binding.runtimeGeneration !== producer.runtimeGeneration) ||
          (event.type === "turn.terminal" &&
            (event.providerThreadId !== turn.binding.providerThreadId ||
              event.providerTurnId !== turn.providerTurnId)) ||
          (event.type === "provider_turn.updated" &&
            (event.providerTurn.providerThreadId !== turn.binding.providerThreadId ||
              event.providerTurn.id !== turn.providerTurnId ||
              event.providerTurn.runAttemptId !== turn.attemptId))))
    ) {
      return yield* new ProviderEventOriginStaleError({});
    }
    yield* producer.revalidateCurrent;
  });
}

export class ProviderEventOriginConflictError extends Error {
  override readonly name = "ProviderEventOriginConflictError";

  constructor() {
    super("A provider event cannot be reassociated with a different origin.");
  }
}

const origins = new WeakMap<object, ProviderEventOrigin>();

function snapshotOrigin(origin: ProviderEventOrigin): ProviderEventOrigin {
  const producer: ProviderEventProducerOrigin = Object.freeze({
    token: origin.producer.token,
    driver: origin.producer.driver,
    instanceId: origin.producer.instanceId,
    providerSessionId: origin.producer.providerSessionId,
    ...(origin.producer.runtimeGeneration === undefined
      ? {}
      : { runtimeGeneration: origin.producer.runtimeGeneration }),
    revalidateCurrent: origin.producer.revalidateCurrent,
  });
  // SDK tokens and Effect internals remain owned by their producers. Only the
  // captured identity and turn binding copies are immutable here.
  return Object.freeze({
    producer,
    ...(origin.turn === undefined
      ? {}
      : {
          turn: Object.freeze({
            ...origin.turn,
            binding: Object.freeze({ ...origin.turn.binding }),
          }),
        }),
  });
}

function sameTurn(
  left: ProviderEventTurnOrigin | undefined,
  right: ProviderEventTurnOrigin | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  return (
    left.runId === right.runId &&
    left.attemptId === right.attemptId &&
    left.providerTurnId === right.providerTurnId &&
    left.binding.threadId === right.binding.threadId &&
    left.binding.providerThreadId === right.binding.providerThreadId &&
    left.binding.providerSessionId === right.binding.providerSessionId &&
    left.binding.providerInstanceId === right.binding.providerInstanceId &&
    left.binding.driver === right.binding.driver &&
    left.binding.runtimeGeneration === right.binding.runtimeGeneration &&
    left.binding.nativeThreadId === right.binding.nativeThreadId
  );
}

function sameOrigin(left: ProviderEventOrigin, right: ProviderEventOrigin): boolean {
  return (
    left.producer.token === right.producer.token &&
    left.producer.driver === right.producer.driver &&
    left.producer.instanceId === right.producer.instanceId &&
    left.producer.providerSessionId === right.producer.providerSessionId &&
    left.producer.runtimeGeneration === right.producer.runtimeGeneration &&
    left.producer.revalidateCurrent === right.producer.revalidateCurrent &&
    sameTurn(left.turn, right.turn)
  );
}

/** Capture before queue insertion. The event itself and its serialized representation remain unchanged. */
export function stampProviderEvent<Event extends object>(
  event: Event,
  origin: ProviderEventOrigin,
): Event {
  const prior = origins.get(event);
  if (prior !== undefined) {
    if (!sameOrigin(prior, origin)) throw new ProviderEventOriginConflictError();
    return event;
  }
  origins.set(event, snapshotOrigin(origin));
  return event;
}

/** Currentness is the captured producer's check, including terminal drain; reading an origin does not run it. */
export function readProviderEventOrigin(event: object): ProviderEventOrigin | undefined {
  return origins.get(event);
}

/** Filtering may clone an event; copy its captured source instead of deriving one from the current runtime. */
export function copyProviderEventOrigin<Event extends object>(original: object, derived: Event): Event {
  const origin = origins.get(original);
  return origin === undefined ? derived : stampProviderEvent(derived, origin);
}
