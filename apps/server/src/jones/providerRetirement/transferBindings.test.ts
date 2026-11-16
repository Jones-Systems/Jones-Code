import { assert, it } from "@effect/vitest";
import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";
import { selectTransferBindings } from "./transferBindings.ts";

const parent = {
  id: "parent-provider",
  appThreadId: "parent",
  providerSessionId: "source-session",
  providerInstanceId: "source-account",
  driver: "codex",
  nativeThreadRef: { driver: "codex", nativeId: "native-parent", strength: "strong" },
  runtimeIdentity: { runtimeGeneration: "source-generation" },
} as unknown as OrchestrationV2ProviderThread;
const child = {
  ...parent,
  id: "child-provider" as typeof parent.id,
  appThreadId: "child" as typeof parent.appThreadId,
  nativeThreadRef: { ...parent.nativeThreadRef!, nativeId: "native-child" },
};
const input = { threadId: parent.appThreadId!, providerSessionId: parent.providerSessionId! };
const source = { driver: parent.driver, instanceId: parent.providerInstanceId };

it("captures only the parent's binding from a projection containing native children", () => {
  const result = selectTransferBindings(input, source, [parent, child]);
  assert.isTrue(result.complete);
  assert.deepEqual(result.providerThreads, [parent]);
  assert.deepEqual(
    result.bindings.map((binding) => binding.nativeThreadId),
    ["native-parent"],
  );
  assert.isFalse(selectTransferBindings(input, source, [child]).complete);
});

it("retains unknown owners and incomplete owned rows as missing retirement evidence", () => {
  for (const invalid of [
    { ...child, appThreadId: null },
    { ...parent, nativeThreadRef: null },
    { ...parent, providerInstanceId: "foreign-account" as typeof parent.providerInstanceId },
  ]) {
    assert.isFalse(selectTransferBindings(input, source, [parent, invalid]).complete);
  }
  assert.isFalse(
    selectTransferBindings({ ...input, providerThreadId: child.id }, source, [parent, child])
      .complete,
  );
});

it("accepts a committed replacement session only with its exact provider-thread selector", () => {
  const replacement = {
    ...parent,
    providerSessionId: "replacement-session" as typeof parent.providerSessionId,
  };
  const selected = { ...input, replacementProviderSessionId: replacement.providerSessionId! };
  assert.isFalse(selectTransferBindings(selected, source, [replacement, child]).complete);
  const result = selectTransferBindings({ ...selected, providerThreadId: parent.id }, source, [
    replacement,
    child,
  ]);
  assert.isTrue(result.complete);
  assert.equal(result.bindings[0]!.providerSessionId, parent.providerSessionId);
});
