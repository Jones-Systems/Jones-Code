import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type ProviderRuntimeBinding,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import { makeRetirementLedger, receiptMatches } from "./RetirementLedger.ts";

const threadId = ThreadId.make("retiring-app-thread");
const sessionId = ProviderSessionId.make("retiring-session");
const binding: ProviderRuntimeBinding = {
  threadId,
  providerThreadId: ProviderThreadId.make("retiring-provider-thread"),
  providerSessionId: sessionId,
  providerInstanceId: ProviderInstanceId.make("source-account"),
  driver: ProviderDriverKind.make("codex"),
  nativeThreadId: "saved-native-conversation",
  runtimeGeneration: "source-generation",
};

it.effect(
  "a retained shared writer fences its app thread across session changes while preserving sibling admission",
  () =>
    Effect.gen(function* () {
      const ledger = makeRetirementLedger();
      const scope = yield* Scope.make();
      const source = {};
      const input = {
        source,
        scope,
        providerSessionId: sessionId,
        threadIds: new Set([threadId]),
        kind: "transfer" as const,
        sessionWide: false,
        bindings: [binding],
      };
      const record = ledger.retain(input);
      assert.strictEqual(ledger.retain(input), record);
      assert.isTrue(
        ledger.blocks({ providerSessionId: ProviderSessionId.make("replacement-session"), threadId }),
      );
      assert.isTrue(ledger.blocks({ providerSessionId: sessionId, threadId, source }));
      assert.isFalse(
        ledger.blocks({ providerSessionId: sessionId, threadId: ThreadId.make("sibling"), source }),
      );
      assert.isTrue(
        ledger.blocks({
          providerSessionId: sessionId,
          threadId: ThreadId.make("sibling"),
          source: {},
        }),
      );
      yield* ledger.complete(record);
      yield* Deferred.await(record.completion);
      assert.isFalse(ledger.blocks({ providerSessionId: sessionId, threadId, source }));
    }),
);

it.effect("late completion clears only its captured generation and scope", () =>
  Effect.gen(function* () {
    const ledger = makeRetirementLedger();
    const oldScope = yield* Scope.make();
    const newScope = yield* Scope.make();
    const old = ledger.retain({
      source: {},
      scope: oldScope,
      providerSessionId: sessionId,
      threadIds: new Set([threadId]),
      kind: "session",
      sessionWide: true,
      bindings: [binding],
    });
    const current = ledger.retain({
      source: {},
      scope: newScope,
      providerSessionId: sessionId,
      threadIds: new Set([threadId]),
      kind: "transfer",
      sessionWide: true,
      bindings: [{ ...binding, runtimeGeneration: "replacement-generation" }],
    });
    yield* ledger.completeScope(oldScope);
    yield* ledger.complete(old);
    assert.strictEqual(ledger.find(sessionId, threadId), current);
    assert.isTrue(
      ledger.blocks({
        providerSessionId: sessionId,
        threadId: ThreadId.make("new-attachment"),
        source: current.source,
      }),
    );
    yield* ledger.complete(current);
  }),
);

it.effect(
  "an unsuccessful retirement result leaves admission fenced until its captured scope finishes",
  () =>
    Effect.gen(function* () {
      const ledger = makeRetirementLedger();
      const scope = yield* Scope.make();
      const record = ledger.retain({
        source: {},
        scope,
        providerSessionId: sessionId,
        threadIds: new Set([threadId]),
        kind: "transfer",
        sessionWide: false,
        bindings: [binding],
      });
      yield* Deferred.fail(record.completion, "retirement unavailable");
      assert.isTrue(ledger.blocks({ providerSessionId: sessionId, threadId }));
      yield* ledger.completeScope(scope);
      assert.isUndefined(ledger.find(sessionId, threadId));
    }),
);

it("accepts only the exact captured source binding", () => {
  assert.isTrue(receiptMatches(binding, { ...binding }));
  for (const changed of [
    { threadId: ThreadId.make("another-app-thread") },
    { providerThreadId: ProviderThreadId.make("another-provider-thread") },
    { providerSessionId: ProviderSessionId.make("another-session") },
    { providerInstanceId: ProviderInstanceId.make("target-account") },
    { driver: ProviderDriverKind.make("claudeAgent") },
    { nativeThreadId: "another-native-conversation" },
    { runtimeGeneration: "another-generation" },
  ])
    assert.isFalse(receiptMatches(binding, { ...binding, ...changed }));
});
