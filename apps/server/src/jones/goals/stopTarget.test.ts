import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import { hasRetainedGoalStopTarget } from "./stopTarget.ts";

const id = ProviderThreadId.make("goal-owner");
const run = { providerThreadId: id, status: "completed" } as OrchestrationV2Run;
const owner = {
  id,
  driver: ProviderDriverKind.make("codex"),
  providerSessionId: ProviderSessionId.make("goal-session"),
  nativeThreadRef: { nativeId: "native-goal", driver: ProviderDriverKind.make("codex") },
} as OrchestrationV2ProviderThread;

it("retains Stop delivery to the current native goal owner after turn completion", () => {
  assert.isTrue(hasRetainedGoalStopTarget(run, [owner], id));
});
it("does not infer a goal owner from a sibling, unloaded binding, or another provider", () => {
  assert.isFalse(hasRetainedGoalStopTarget(undefined, [owner], id));
  assert.isFalse(hasRetainedGoalStopTarget(run, [owner], ProviderThreadId.make("sibling")));
  assert.isFalse(hasRetainedGoalStopTarget(run, [{ ...owner, providerSessionId: null }], id));
  assert.isFalse(hasRetainedGoalStopTarget(run, [{ ...owner, nativeThreadRef: null }], id));
  assert.isFalse(
    hasRetainedGoalStopTarget(run, [{ ...owner, driver: ProviderDriverKind.make("claude") }], id),
  );
});
