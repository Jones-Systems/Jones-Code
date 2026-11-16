import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  captureExplicitComposerModelPick,
  captureFollowUpModelSelection,
  consumeSubmittedModelPick,
  queuedModelSelectionForDraft,
  resolveQueuedModelSelection,
} from "./modelSelection";

const saved = {
  instanceId: ProviderInstanceId.make("codex-owner-account"),
  model: "gpt-6.1-sol",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const failover = { ...saved, instanceId: ProviderInstanceId.make("codex-next-account") };

describe("mobile selection intent across queued sends", () => {
  it("retains an explicit choice equal to the captured default after an account switch and replay", () => {
    const queued = captureFollowUpModelSelection(captureExplicitComposerModelPick(saved), saved);
    const replayed = JSON.parse(JSON.stringify(queued)) as typeof queued;
    expect(replayed.modelSelectionExplicit).toBe(true);
    expect(resolveQueuedModelSelection(replayed, failover)).toEqual(saved);
    expect(queuedModelSelectionForDraft(replayed)).toEqual(saved);
  });

  it("follows the saved failover only for recorded inherited intent without restoring a picker choice", () => {
    const queued = captureFollowUpModelSelection(undefined, saved);
    expect(queued.modelSelection).toBe(saved);
    expect(queued.modelSelectionExplicit).toBe(false);
    expect(resolveQueuedModelSelection(queued, failover)).toBe(failover);
    expect(queuedModelSelectionForDraft(queued)).toBeUndefined();
    expect(resolveQueuedModelSelection({ modelSelection: saved }, failover)).toBe(saved);
  });

  it("consumes the submitted pick while preserving a later equal re-selection", () => {
    const submitted = captureExplicitComposerModelPick(saved);
    const draft = { text: "Next message", modelSelection: submitted };
    expect(consumeSubmittedModelPick(draft, submitted).modelSelection).toBeUndefined();
    const newer = { ...draft, modelSelection: captureExplicitComposerModelPick(saved) };
    expect(consumeSubmittedModelPick(newer, submitted)).toBe(newer);
    expect(newer.modelSelection).toEqual(saved);
  });
});
