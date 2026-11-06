import { ProviderRuntimeBinding, ProviderSessionId, type ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class WriterRetirementError extends Schema.TaggedError<WriterRetirementError>()(
  "WriterRetirementError",
  { providerSessionId: ProviderSessionId, cause: Schema.Defect() },
) {
  override get message(): string {
    return "The previous native writer could not be retired.";
  }
}

export interface Retirement {
  readonly source: object;
  readonly scope: Scope.Closeable;
  readonly providerSessionId: ProviderSessionId;
  readonly threadIds: ReadonlySet<ThreadId>;
  readonly kind: "session" | "opening" | "transfer";
  readonly sessionWide: boolean;
  readonly conversationKey?: string;
  readonly bindings: ReadonlyArray<ProviderRuntimeBinding>;
  readonly completion: Deferred.Deferred<void, WriterRetirementError>;
}

const equivalentBinding = Schema.toEquivalence(ProviderRuntimeBinding);

export function receiptMatches(
  captured: ProviderRuntimeBinding,
  received: ProviderRuntimeBinding,
): boolean {
  return equivalentBinding(captured, received);
}

export function makeRetirementLedger() {
  const pending = new Set<Retirement>();
  // Completion releases pending proof, but a transferred producer/thread incarnation stays fenced.
  const retiredWriters = new WeakMap<object, Set<ThreadId>>();
  const retain = (input: Omit<Retirement, "completion">): Retirement => {
    if (input.kind === "transfer") {
      const threads = retiredWriters.get(input.source) ?? new Set<ThreadId>();
      for (const threadId of input.threadIds) threads.add(threadId);
      retiredWriters.set(input.source, threads);
    }
    const existing = [...pending].find(
      (record) =>
        record.source === input.source &&
        record.kind === input.kind &&
        record.scope === input.scope &&
        record.bindings.length === input.bindings.length &&
        record.bindings.every((binding, index) =>
          equivalentBinding(binding, input.bindings[index]!),
        ) &&
        record.threadIds.size === input.threadIds.size &&
        [...input.threadIds].every((threadId) => record.threadIds.has(threadId)),
    );
    if (existing !== undefined) return existing;
    const record = { ...input, completion: Deferred.makeUnsafe<void, WriterRetirementError>() };
    pending.add(record);
    return record;
  };
  const complete = (record: Retirement) =>
    Effect.sync(() => pending.delete(record)).pipe(
      Effect.andThen(Deferred.succeed(record.completion, undefined)),
      Effect.asVoid,
    );
  return {
    retain,
    complete,
    retiredWriter: (source: object, threadId: ThreadId) =>
      retiredWriters.get(source)?.has(threadId) === true,
    pendingConversation: (threadId: ThreadId, destinationKey: string | undefined) =>
      [...pending].filter(
        (record) =>
          record.conversationKey !== undefined &&
          record.threadIds.has(threadId) &&
          (destinationKey === undefined || destinationKey === record.conversationKey),
      ),
    completeScope: (scope: Scope.Closeable) =>
      Effect.forEach(
        [...pending].filter((record) => record.scope === scope),
        complete,
        { discard: true },
      ),
    find: (providerSessionId: ProviderSessionId, threadId?: ThreadId, kind?: Retirement["kind"]) =>
      [...pending].find(
        (record) =>
          record.providerSessionId === providerSessionId &&
          (kind === undefined || record.kind === kind) &&
          (threadId === undefined || record.threadIds.has(threadId)),
      ),
    blocks: (input: {
      readonly providerSessionId: ProviderSessionId;
      readonly threadId: ThreadId;
      readonly source?: object | undefined;
      readonly destinationKey?: string | undefined;
    }) =>
      [...pending].some(
        (record) =>
          (record.threadIds.has(input.threadId) &&
            (record.conversationKey === undefined ||
              input.destinationKey === undefined ||
              input.destinationKey === record.conversationKey)) ||
          (record.providerSessionId === input.providerSessionId &&
            (record.sessionWide || record.source !== input.source)),
      ),
  };
}
