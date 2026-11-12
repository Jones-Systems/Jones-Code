import { assert, it } from "@effect/vitest";
import { ProviderThreadId, ProviderTurnId, RunAttemptId, RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Trace from "./SupersededTerminalTrace.ts";

const key: Trace.Key = {
  threadId: ThreadId.make("thread:trace"), runId: RunId.make("run:trace"),
  providerThreadId: ProviderThreadId.make("provider-thread:trace"),
  providerTurnId: ProviderTurnId.make("turn:trace"),
  interruptedAttemptId: RunAttemptId.make("attempt:trace"),
};
it.effect("keeps only allowlisted reasons for the exact old provider-turn identity", () => Effect.sync(() => {
  const trace = Trace.make();
  for (const reason of Trace.reasons) {
    trace.record(key, reason);
    assert.equal(trace.read(key), reason);
  }
  for (const field of Object.keys(key) as Array<keyof Trace.Key>) {
    assert.isUndefined(trace.read({ ...key, [field]: "different" }));
  }
  trace.record(key, "private error text" as Trace.Reason);
  assert.equal(trace.read(key), "write_error");
  trace.clear(key);
  assert.isUndefined(trace.read(key));
}));
it.effect("evicts the least recently recorded diagnostic at the cap", () => Effect.sync(() => {
  const trace = Trace.make(2);
  const next = { ...key, providerTurnId: ProviderTurnId.make("turn:next") };
  const last = { ...key, providerTurnId: ProviderTurnId.make("turn:last") };
  trace.record(key, "origin");
  trace.record(next, "effect");
  trace.record(key, "successor");
  trace.record(last, "locator");
  assert.isUndefined(trace.read(next));
  assert.equal(trace.read(key), "successor");
  assert.equal(trace.read(last), "locator");
  assert.isUndefined(Trace.make().read(key));
}));
