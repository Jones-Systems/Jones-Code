import { ProviderRuntimeBinding, type ProviderSessionId, type ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export interface Retirement {
  readonly source: object;
  readonly scope: Scope.Closeable;
  readonly providerSessionId: ProviderSessionId;
  readonly threadIds: ReadonlySet<ThreadId>;
  readonly kind: "session" | "opening" | "transfer";
  readonly sessionWide: boolean;
  readonly bindings: ReadonlyArray<ProviderRuntimeBinding>;
  readonly completion: Deferred.Deferred<void, unknown>;
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
  const retain = (input: Omit<Retirement, "completion">): Retirement => {
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
    const record = { ...input, completion: Deferred.makeUnsafe<void, unknown>() };
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
      readonly source?: object;
    }) =>
      [...pending].some(
        (record) =>
          record.threadIds.has(input.threadId) ||
          (record.providerSessionId === input.providerSessionId &&
            (record.sessionWide || record.source !== input.source)),
      ),
  };
}
