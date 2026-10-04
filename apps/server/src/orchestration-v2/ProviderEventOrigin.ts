import type {
  MessageId,
  NodeId,
  OrchestrationV2ProviderRef,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type * as Effect from "effect/Effect";

import type { ProviderAdapterProtocolError } from "./ProviderAdapter.ts";
import type { ProviderAssistantOutputOwner } from "./ProviderEventIngestor.ts";

export interface ProviderEventProducerOrigin {
  readonly token: object;
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly runtimeGeneration?: string;
  readonly revalidateCurrent: Effect.Effect<void, ProviderAdapterProtocolError | string>;
}

export interface ProviderEventOrigin {
  readonly producer: ProviderEventProducerOrigin;
  readonly turn?: ProviderAssistantOutputOwner;
  readonly derivation?: ClaudeBufferedSubagentCompletionDerivationV1;
}

export interface ClaudeBufferedSubagentCompletionDerivationV1 {
  readonly kind: "claude_buffered_subagent_completion";
  readonly executor: ProviderAssistantOutputOwner;
  readonly result: {
    readonly token: object;
    readonly producer: ProviderEventProducerOrigin;
    readonly nativeThreadId: string;
  };
  readonly notification: {
    readonly token: object;
    readonly producer: ProviderEventProducerOrigin;
    readonly nativeThreadId: string;
    readonly nativeTaskId: string;
    readonly toolUseId: string | null;
    readonly summary: string;
    readonly status: "completed" | "failed" | "cancelled";
  };
  readonly subject: {
    readonly subagentId: NodeId;
    readonly parentThreadId: ThreadId;
    readonly runId: RunId | null;
    readonly parentNodeId: NodeId;
    readonly providerThreadId: ProviderThreadId | null;
    readonly childThreadId: ThreadId;
    readonly childRootNodeId: NodeId;
    readonly nativeTaskRef: OrchestrationV2ProviderRef;
    readonly startedAt: DateTime.Utc | null;
    readonly expectedUpdatedAt: DateTime.Utc;
  };
  readonly childResult?: {
    readonly messageId: MessageId;
    readonly turnItemId: TurnItemId;
    readonly nativeItemRef: OrchestrationV2ProviderRef;
  };
  readonly revalidateDerivation: Effect.Effect<void, ProviderAdapterProtocolError | string>;
}

export class ProviderEventOriginConflictError extends Error {
  override readonly name = "ProviderEventOriginConflictError";

  constructor() {
    super("A provider event cannot be reassociated with a different origin.");
  }
}

const origins = new WeakMap<object, ProviderEventOrigin>();

function snapshotProducer(producer: ProviderEventProducerOrigin): ProviderEventProducerOrigin {
  return Object.freeze({
    token: producer.token,
    driver: producer.driver,
    instanceId: producer.instanceId,
    providerSessionId: producer.providerSessionId,
    ...(producer.runtimeGeneration === undefined
      ? {}
      : { runtimeGeneration: producer.runtimeGeneration }),
    revalidateCurrent: producer.revalidateCurrent,
  });
}

function snapshotTurn(turn: ProviderAssistantOutputOwner): ProviderAssistantOutputOwner {
  return Object.freeze({ ...turn, binding: Object.freeze({ ...turn.binding }) });
}

function snapshotTimestamp(value: DateTime.Utc): DateTime.Utc {
  const captured = DateTime.makeUnsafe(DateTime.toEpochMillis(value));
  // DateTime lazily caches UTC parts; prime the private copy before freezing it.
  Object.freeze(DateTime.toPartsUtc(captured));
  return Object.freeze(captured);
}

function snapshotDerivation(
  derivation: ClaudeBufferedSubagentCompletionDerivationV1,
): ClaudeBufferedSubagentCompletionDerivationV1 {
  return Object.freeze({
    kind: derivation.kind,
    executor: snapshotTurn(derivation.executor),
    result: Object.freeze({
      ...derivation.result,
      producer: snapshotProducer(derivation.result.producer),
    }),
    notification: Object.freeze({
      ...derivation.notification,
      producer: snapshotProducer(derivation.notification.producer),
    }),
    subject: Object.freeze({
      ...derivation.subject,
      nativeTaskRef: Object.freeze({ ...derivation.subject.nativeTaskRef }),
      startedAt:
        derivation.subject.startedAt === null
          ? null
          : snapshotTimestamp(derivation.subject.startedAt),
      expectedUpdatedAt: snapshotTimestamp(derivation.subject.expectedUpdatedAt),
    }),
    ...(derivation.childResult === undefined
      ? {}
      : {
          childResult: Object.freeze({
            ...derivation.childResult,
            nativeItemRef: Object.freeze({ ...derivation.childResult.nativeItemRef }),
          }),
        }),
    revalidateDerivation: derivation.revalidateDerivation,
  });
}

function snapshotOrigin(origin: ProviderEventOrigin): ProviderEventOrigin {
  // SDK tokens and Effect internals remain owned by their producers. Captured
  // identity, turn bindings and derivation facts are immutable copies.
  return Object.freeze({
    producer: snapshotProducer(origin.producer),
    ...(origin.turn === undefined ? {} : { turn: snapshotTurn(origin.turn) }),
    ...(origin.derivation === undefined
      ? {}
      : { derivation: snapshotDerivation(origin.derivation) }),
  });
}

function sameTurn(
  left: ProviderAssistantOutputOwner | undefined,
  right: ProviderAssistantOutputOwner | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  return (
    left.runId === right.runId &&
    left.attemptId === right.attemptId &&
    left.providerTurnId === right.providerTurnId &&
    left.binding.threadId === right.binding.threadId &&
    left.binding.providerThreadId === right.binding.providerThreadId &&
    left.binding.providerSessionId === right.binding.providerSessionId &&
    left.binding.instanceId === right.binding.instanceId &&
    left.binding.runtimeGeneration === right.binding.runtimeGeneration &&
    left.binding.nativeThreadId === right.binding.nativeThreadId
  );
}

function sameProducer(
  left: ProviderEventProducerOrigin,
  right: ProviderEventProducerOrigin,
): boolean {
  return (
    left.token === right.token &&
    left.driver === right.driver &&
    left.instanceId === right.instanceId &&
    left.providerSessionId === right.providerSessionId &&
    left.runtimeGeneration === right.runtimeGeneration &&
    left.revalidateCurrent === right.revalidateCurrent
  );
}

function sameNativeRef(
  left: OrchestrationV2ProviderRef,
  right: OrchestrationV2ProviderRef,
): boolean {
  return (
    left.driver === right.driver &&
    left.nativeId === right.nativeId &&
    left.strength === right.strength &&
    left.fingerprint === right.fingerprint &&
    left.ordinal === right.ordinal
  );
}

function sameTimestamp(left: DateTime.Utc | null, right: DateTime.Utc | null): boolean {
  return left === null || right === null
    ? left === right
    : DateTime.toEpochMillis(left) === DateTime.toEpochMillis(right);
}

function sameDerivation(
  left: ClaudeBufferedSubagentCompletionDerivationV1 | undefined,
  right: ClaudeBufferedSubagentCompletionDerivationV1 | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  const l = left.subject,
    r = right.subject;
  const child =
    left.childResult === undefined || right.childResult === undefined
      ? left.childResult === right.childResult
      : left.childResult.messageId === right.childResult.messageId &&
        left.childResult.turnItemId === right.childResult.turnItemId &&
        sameNativeRef(left.childResult.nativeItemRef, right.childResult.nativeItemRef);
  return (
    left.kind === right.kind &&
    sameTurn(left.executor, right.executor) &&
    left.result.token === right.result.token &&
    sameProducer(left.result.producer, right.result.producer) &&
    left.result.nativeThreadId === right.result.nativeThreadId &&
    left.notification.token === right.notification.token &&
    sameProducer(left.notification.producer, right.notification.producer) &&
    left.notification.nativeThreadId === right.notification.nativeThreadId &&
    left.notification.nativeTaskId === right.notification.nativeTaskId &&
    left.notification.toolUseId === right.notification.toolUseId &&
    left.notification.summary === right.notification.summary &&
    left.notification.status === right.notification.status &&
    l.subagentId === r.subagentId &&
    l.parentThreadId === r.parentThreadId &&
    l.runId === r.runId &&
    l.parentNodeId === r.parentNodeId &&
    l.providerThreadId === r.providerThreadId &&
    l.childThreadId === r.childThreadId &&
    l.childRootNodeId === r.childRootNodeId &&
    sameNativeRef(l.nativeTaskRef, r.nativeTaskRef) &&
    sameTimestamp(l.startedAt, r.startedAt) &&
    sameTimestamp(l.expectedUpdatedAt, r.expectedUpdatedAt) &&
    child &&
    left.revalidateDerivation === right.revalidateDerivation
  );
}

function sameOrigin(left: ProviderEventOrigin, right: ProviderEventOrigin): boolean {
  return (
    sameProducer(left.producer, right.producer) &&
    sameTurn(left.turn, right.turn) &&
    sameDerivation(left.derivation, right.derivation)
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
export function copyProviderEventOrigin<Event extends object>(
  original: object,
  derived: Event,
): Event {
  const origin = origins.get(original);
  return origin === undefined ? derived : stampProviderEvent(derived, origin);
}
