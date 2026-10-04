import type { ProviderDriverKind, ProviderInstanceId, ProviderSessionId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";

import type { ProviderAssistantOutputOwner } from "./ProviderEventIngestor.ts";

export interface ProviderEventProducerOrigin {
  readonly token: object;
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly runtimeGeneration?: string;
  readonly revalidateCurrent: Effect.Effect<void, unknown>;
}

export interface ProviderEventOrigin {
  readonly producer: ProviderEventProducerOrigin;
  readonly turn?: ProviderAssistantOutputOwner;
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
    ...(origin.producer.runtimeGeneration === undefined ? {} : { runtimeGeneration: origin.producer.runtimeGeneration }),
    revalidateCurrent: origin.producer.revalidateCurrent,
  });
  // SDK tokens and Effect internals remain owned by their producers. Only the
  // captured identity and turn binding copies are immutable here.
  return Object.freeze({
    producer,
    ...(origin.turn === undefined ? {} : {
      turn: Object.freeze({ ...origin.turn, binding: Object.freeze({ ...origin.turn.binding }) }),
    }),
  });
}

function sameTurn(left: ProviderAssistantOutputOwner | undefined, right: ProviderAssistantOutputOwner | undefined): boolean {
  if (left === undefined || right === undefined) return left === right;
  return left.runId === right.runId && left.attemptId === right.attemptId && left.providerTurnId === right.providerTurnId &&
    left.binding.threadId === right.binding.threadId && left.binding.providerThreadId === right.binding.providerThreadId &&
    left.binding.providerSessionId === right.binding.providerSessionId && left.binding.instanceId === right.binding.instanceId &&
    left.binding.runtimeGeneration === right.binding.runtimeGeneration && left.binding.nativeThreadId === right.binding.nativeThreadId;
}

function sameOrigin(left: ProviderEventOrigin, right: ProviderEventOrigin): boolean {
  return left.producer.token === right.producer.token && left.producer.driver === right.producer.driver &&
    left.producer.instanceId === right.producer.instanceId && left.producer.providerSessionId === right.producer.providerSessionId &&
    left.producer.runtimeGeneration === right.producer.runtimeGeneration &&
    left.producer.revalidateCurrent === right.producer.revalidateCurrent && sameTurn(left.turn, right.turn);
}

/** Capture before queue insertion. The event itself and its serialized representation remain unchanged. */
export function stampProviderEvent<Event extends object>(event: Event, origin: ProviderEventOrigin): Event {
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
