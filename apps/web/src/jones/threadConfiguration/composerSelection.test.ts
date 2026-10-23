import { afterEach, describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ProviderInstanceId, OrchestrationV2Command } from "@t3tools/contracts";
import { useComposerDraftStore, DraftId } from "../../composerDraftStore";
import { acknowledgeComposerSelection, composerSelectionForDispatch } from "./composerSelection";

const decodeCommand = Schema.decodeUnknownSync(OrchestrationV2Command);

describe("web composer selection dispatch contract", () => {
  const initialState = useComposerDraftStore.getState();
  afterEach(() => useComposerDraftStore.setState(initialState, true));
  const selection = {
    instanceId: ProviderInstanceId.make("compatible-b"),
    model: "gpt-6-astra",
    options: [{ id: "reasoningEffort", value: "high" }],
  };
  it.each([undefined, false, true])(
    "carries selection values separately from picker intent %s",
    (explicit) => {
      const fields = composerSelectionForDispatch(selection, {
        activeProvider: selection.instanceId,
        modelSelectionByProvider: { [selection.instanceId]: selection },
        ...(explicit === undefined ? {} : { modelSelectionExplicit: explicit }),
      });
      const wire = decodeCommand({
        type: "message.dispatch",
        commandId: "composer",
        threadId: "thread",
        messageId: "follow-up",
        text: "continue",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        ...fields,
        deliveryIntent: "auto",
        dispatchMode: { type: "start_immediately" },
      });
      expect(wire).toMatchObject({
        modelSelection: selection,
        modelSelectionExplicit: explicit === true,
        deliveryIntent: "auto",
      });
    },
  );
  it("consumes an accepted picker choice so the next follow-up inherits defaults", () => {
    const target = DraftId.make("self-config-composer");
    const store = useComposerDraftStore.getState();
    store.setModelSelection(target, selection, { explicit: true });
    const submitted = store.getComposerDraft(target);
    expect(composerSelectionForDispatch(selection, submitted).modelSelectionExplicit).toBe(true);
    store.clearComposerContent(target);
    acknowledgeComposerSelection(target, submitted);
    expect(
      composerSelectionForDispatch(selection, store.getComposerDraft(target))
        .modelSelectionExplicit,
    ).toBe(false);
    store.setModelSelection(target, selection, { explicit: true });
    expect(
      composerSelectionForDispatch(selection, store.getComposerDraft(target))
        .modelSelectionExplicit,
    ).toBe(true);
  });
  it("preserves a newer picker choice made while the accepted send is in flight", () => {
    const target = DraftId.make("self-config-newer-composer");
    const store = useComposerDraftStore.getState();
    store.setModelSelection(target, selection, { explicit: true });
    const submitted = store.getComposerDraft(target);
    const next = { ...selection, model: "different-model" };
    store.setModelSelection(target, next, { explicit: true });
    acknowledgeComposerSelection(target, submitted);
    expect(composerSelectionForDispatch(next, store.getComposerDraft(target))).toEqual({
      modelSelection: next,
      modelSelectionExplicit: true,
    });
  });
  it("does not turn an inherited saved selection into a picker change when no draft exists", () => {
    expect(composerSelectionForDispatch(selection, undefined)).toEqual({
      modelSelection: selection,
      modelSelectionExplicit: false,
    });
  });
});
