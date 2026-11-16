import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId, type ModelSelection } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { selectionAheadOfQueuedPicks } from "./executionSelection.ts";

const inEffect: ModelSelection = { instanceId: ProviderInstanceId.make("claude"), model: "opus" };
const picked: ModelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.5" };
const queuedAt = DateTime.makeUnsafe("2026-11-10T10:00:00Z");
const later = DateTime.makeUnsafe("2026-11-10T10:00:01Z");

describe("selectionAheadOfQueuedPicks", () => {
  it("takes the selection in effect when the saved default is a queued pick", () => {
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: queuedAt }, inEffect, [
        { modelSelection: picked, requestedAt: queuedAt },
      ]),
      inEffect,
    );
  });

  it("keeps a default saved after the pick was queued", () => {
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: later }, inEffect, [
        { modelSelection: picked, requestedAt: queuedAt },
      ]),
      picked,
    );
  });

  it("keeps the saved default when no queued pick carries it", () => {
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: queuedAt }, inEffect, [
        { modelSelection: inEffect, requestedAt: queuedAt },
      ]),
      picked,
    );
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: queuedAt }, inEffect, []),
      picked,
    );
  });
});
