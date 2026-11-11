import { assert, it } from "@effect/vitest";

import { PROVIDER_INTERRUPT_TIMEOUT_MS } from "../../orchestration-v2/ProviderSessionManager.ts";
import {
  rootStartWaitMs,
  STOP_CHILD_BUDGET_MS,
  STOP_RECOVERY_BUDGET_MS,
  STOP_REQUEST_BUDGET_MS,
} from "./stopBudgets.ts";

it("keeps the Jones stop budgets at their retained values", () => {
  assert.deepEqual(
    [STOP_CHILD_BUDGET_MS, STOP_REQUEST_BUDGET_MS, STOP_RECOVERY_BUDGET_MS],
    [10_000, 3_000, 14_000],
  );
});

it("ends every stop budget before the session manager's interrupt timeout", () => {
  assert.isBelow(STOP_RECOVERY_BUDGET_MS, PROVIDER_INTERRUPT_TIMEOUT_MS);
  assert.isBelow(STOP_CHILD_BUDGET_MS + STOP_REQUEST_BUDGET_MS, STOP_RECOVERY_BUDGET_MS);
});

it("waits for a queued root start only while the root request still fits", () => {
  assert.equal(rootStartWaitMs(0), STOP_CHILD_BUDGET_MS);
  assert.equal(rootStartWaitMs(STOP_CHILD_BUDGET_MS), 1_000);
  assert.equal(rootStartWaitMs(STOP_RECOVERY_BUDGET_MS), 0);
  for (let elapsed = 0; elapsed <= STOP_RECOVERY_BUDGET_MS; elapsed += 250) {
    const wait = rootStartWaitMs(elapsed);
    assert.isAtMost(wait, STOP_CHILD_BUDGET_MS);
    assert.isAtLeast(wait, 0);
    if (elapsed <= STOP_RECOVERY_BUDGET_MS - STOP_REQUEST_BUDGET_MS) {
      assert.isAtMost(elapsed + wait + STOP_REQUEST_BUDGET_MS, STOP_RECOVERY_BUDGET_MS);
    }
  }
});
