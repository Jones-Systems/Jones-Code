import { describe, expect, it, vi } from "vite-plus/test";
import { VoiceReviewDraft, type VoiceReviewMutationResult } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  remainingSeconds,
  VoiceReviewActions,
  RegistryCorrectionActions,
  voiceReviewError,
  type VoiceReviewTransport,
} from "./voiceReviewActions";

const decodeDraft = Schema.decodeUnknownSync(VoiceReviewDraft);

function draft(overrides: Partial<VoiceReviewDraft> = {}): VoiceReviewDraft {
  return decodeDraft({
    id: "capture-1",
    source_id: "mic-1",
    state: "held",
    revision: 1,
    text: "<script>literal prompt</script>",
    transcript_provider: "local",
    language: "en",
    edited: false,
    created_at: "2026-10-02T12:00:00Z",
    updated_at: "2026-10-02T12:00:00Z",
    due_at: "2026-10-02T12:03:00Z",
    remaining_ms: 180000,
    expires_at: "2026-10-03T12:00:00Z",
    command_id: null,
    command_status: null,
    reason: null,
    server_now: "2026-10-02T12:00:00Z",
    ...overrides,
  });
}
function deferred<A>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function idle(actions: VoiceReviewActions) {
  if (!actions.busy) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const unsubscribe = actions.subscribe(() => {
      if (actions.busy) return;
      unsubscribe();
      resolve();
    });
  });
}

const held = draft();
const editing = draft({ revision: 2, state: "editing" });
const lease = { draft: editing, edit_handle: "opaque" };

function saved(text: string) {
  return {
    draft: draft({ revision: 3, state: "paused", text, edited: true }),
    edit_handle: null,
  };
}

const released = {
  draft: draft({ revision: 4, state: "released", text: null }),
  edit_handle: null,
} as const;

describe("voice review composition and actions", () => {
  it("leaves the release countdown running and sends the captured text for empty inputs", async () => {
    const mutate = vi.fn<VoiceReviewTransport["mutate"]>().mockResolvedValueOnce(released);
    const actions = new VoiceReviewActions(held, { mutate, get: vi.fn() });
    actions.setText("");
    actions.setInstructions(" \n ");
    expect(actions.composition).toEqual({ text: held.text });
    expect(mutate).not.toHaveBeenCalled();
    expect(remainingSeconds(actions.draft, 1500)).toBe(179);

    await actions.act("send-now");
    expect(mutate).toHaveBeenCalledExactlyOnceWith("capture-1", "send-now", {
      expected_revision: 1,
    });
  });

  it("keeps accepting input while the lease is acquired and saves then sends the current fields", async () => {
    const begin = deferred<VoiceReviewMutationResult>();
    const text = "<b>edited</b>\n\nAdditional instructions:\nUse plain text.";
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockImplementationOnce(() => begin.promise)
      .mockResolvedValueOnce(saved(text))
      .mockResolvedValueOnce(released);
    const actions = new VoiceReviewActions(held, { mutate, get: vi.fn() });
    actions.setText("first keystrokes");
    expect(actions.editHandle).toBeNull();
    expect(actions.canType).toBe(true);
    actions.setText("<b>edited</b>");
    actions.setInstructions("  Use plain text.  ");
    expect(mutate).toHaveBeenCalledExactlyOnceWith("capture-1", "edit-begin", {
      expected_revision: 1,
    });
    const acquired = idle(actions);
    begin.resolve(lease);
    await acquired;
    await actions.act("send-now");

    expect(mutate.mock.calls.slice(1)).toEqual([
      ["capture-1", "edit-save", { expected_revision: 2, text, edit_handle: "opaque" }],
      ["capture-1", "send-now", { expected_revision: 3 }],
    ]);
    expect(actions.draft.state).toBe("released");
    expect(actions.hasLocalWork).toBe(false);
  });

  it("keeps the captured prompt for instructions-only edits and resumes the confirmed revision", async () => {
    const text = `${held.text}\n\nAdditional instructions:\nUse examples.`;
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce(lease)
      .mockResolvedValueOnce(saved(text))
      .mockResolvedValueOnce({ draft: draft({ revision: 4, text }), edit_handle: null });
    const actions = new VoiceReviewActions(held, { mutate, get: vi.fn() });
    actions.setText("");
    actions.setInstructions("Use examples.");
    await idle(actions);
    await actions.act("play");
    expect(mutate).toHaveBeenCalledTimes(1);
    await actions.act("edit-save");

    expect(actions.text).toBe(held.text);
    expect(actions.instructions).toBe("Use examples.");
    expect(actions.hasLocalWork).toBe(false);
    await actions.act("play");
    expect(mutate.mock.calls.slice(1)).toEqual([
      ["capture-1", "edit-save", { expected_revision: 2, text, edit_handle: "opaque" }],
      ["capture-1", "play", { expected_revision: 3 }],
    ]);
  });

  it("updates clean fields from refresh and keeps both dirty fields across a lost lease", async () => {
    const remote = draft({ revision: 3, state: "paused", text: "remote update" });
    const mutate = vi.fn<VoiceReviewTransport["mutate"]>().mockResolvedValueOnce(lease);
    const actions = new VoiceReviewActions(held, {
      mutate,
      get: vi.fn().mockResolvedValue(remote),
    });
    actions.setText("local prompt");
    actions.setInstructions("local instructions");
    await idle(actions);
    actions.receive(editing);
    expect(actions.editHandle).toBe("opaque");
    actions.receive(remote);
    actions.receive(editing);

    expect(actions.editHandle).toBeNull();
    expect(actions.reviewRequired).toBe(true);
    expect(actions.composition).toEqual({
      text: "local prompt",
      instructions: "local instructions",
    });
    expect(actions.draft.revision).toBe(3);
    await actions.act("send-now");
    expect(mutate).toHaveBeenCalledTimes(1);

    await actions.reconcile();
    expect(actions.reviewRequired).toBe(false);
    expect(mutate).toHaveBeenCalledTimes(1);
    await actions.act("edit-cancel");
    expect(actions.text).toBe("remote update");
    expect(actions.instructions).toBe("");
    expect(actions.hasLocalWork).toBe(false);
  });

  it("never sends after a save conflict and keeps the local composition", async () => {
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce(lease)
      .mockRejectedValueOnce({ _tag: "VoiceReviewConflictError" });
    const get = vi
      .fn()
      .mockResolvedValue(draft({ revision: 3, state: "editing", text: "remote text" }));
    const actions = new VoiceReviewActions(held, { mutate, get });
    actions.setText("local prompt");
    actions.setInstructions("local instructions");
    await idle(actions);
    await actions.act("send-now");

    expect(actions.composition).toEqual({
      text: "local prompt",
      instructions: "local instructions",
    });
    expect(actions.draft.revision).toBe(3);
    expect(actions.reviewRequired).toBe(true);
    expect(mutate.mock.calls.map(([, action]) => action)).toEqual(["edit-begin", "edit-save"]);
    expect(get).toHaveBeenCalledWith("capture-1");
  });

  it("does not advance from an acknowledged save superseded during the request", async () => {
    const saving = deferred<VoiceReviewMutationResult>();
    const newer = draft({ revision: 4, state: "editing", text: "newer text" });
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce(lease)
      .mockImplementationOnce(() => saving.promise);
    const actions = new VoiceReviewActions(held, { mutate, get: vi.fn().mockResolvedValue(newer) });
    actions.setText("my text");
    await idle(actions);
    const pending = actions.act("send-now");
    actions.receive(newer);
    saving.resolve(saved("my text"));
    await pending;

    expect(actions.draft).toBe(newer);
    expect(actions.text).toBe("my text");
    expect(actions.reviewRequired).toBe(true);
    expect(mutate).toHaveBeenCalledTimes(2);
  });

  it("requires an explicit send after reconciling a lost save reply and never appends twice", async () => {
    const text = `${held.text}\n\nAdditional instructions:\nKeep it short.`;
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce(lease)
      .mockRejectedValueOnce(new Error("lost save reply"))
      .mockResolvedValueOnce(released);
    const get = vi.fn().mockResolvedValue(saved(text).draft);
    const actions = new VoiceReviewActions(held, { mutate, get });
    actions.setInstructions("Keep it short.");
    await idle(actions);
    await actions.act("send-now");

    expect(actions.uncertain).toBe(true);
    expect(actions.instructions).toBe("Keep it short.");
    await actions.reconcile();
    expect(actions.uncertain).toBe(false);
    expect(actions.composition).toEqual({ text: held.text, instructions: "Keep it short." });
    expect(mutate).toHaveBeenCalledTimes(2);

    await actions.act("send-now");
    expect(mutate.mock.calls[2]).toEqual(["capture-1", "send-now", { expected_revision: 3 }]);
  });

  it("does not replay an unconfirmed send even when readback initially shows the same revision", async () => {
    const mutate = vi.fn().mockRejectedValue(new Error("private upstream exception"));
    const get = vi.fn().mockResolvedValue(held);
    const actions = new VoiceReviewActions(held, { mutate, get });
    await actions.act("send-now");
    expect(actions.uncertain).toBe(true);
    expect(actions.error).not.toContain("private");
    await actions.reconcile();
    expect(actions.uncertain).toBe(true);
    await actions.act("send-now");
    expect(mutate).toHaveBeenCalledTimes(1);

    get.mockResolvedValue(released.draft);
    await actions.reconcile();
    expect(actions.uncertain).toBe(false);
    await actions.act("send-now");
    expect(mutate).toHaveBeenCalledTimes(1);
  });

  it("keeps saved fields available while the send after save is unconfirmed", async () => {
    const text = "New prompt\n\nAdditional instructions:\nExplain the result.";
    const paused = saved(text);
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce(lease)
      .mockResolvedValueOnce(paused)
      .mockRejectedValueOnce(new Error("lost send reply"));
    const get = vi.fn().mockResolvedValue(paused.draft);
    const actions = new VoiceReviewActions(held, { mutate, get });
    actions.setText("New prompt");
    actions.setInstructions("Explain the result.");
    await idle(actions);
    await actions.act("send-now");

    expect(actions.draft.revision).toBe(3);
    expect(actions.composition).toEqual({
      text: "New prompt",
      instructions: "Explain the result.",
    });
    await actions.reconcile();
    await actions.act("send-now");
    expect(actions.uncertain).toBe(true);
    expect(mutate.mock.calls.map(([, action]) => action)).toEqual([
      "edit-begin",
      "edit-save",
      "send-now",
    ]);
  });

  it("can pause a remote edit, then send the locally composed text with a new lease", async () => {
    const paused = draft({ revision: 3, state: "paused" });
    const nextEdit = { draft: draft({ revision: 4, state: "editing" }), edit_handle: "owned" };
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce({ draft: paused, edit_handle: null })
      .mockResolvedValueOnce(nextEdit)
      .mockResolvedValueOnce({
        draft: draft({ revision: 5, state: "paused", text: "mine" }),
        edit_handle: null,
      })
      .mockResolvedValueOnce({ ...released, draft: { ...released.draft, revision: 6 } });
    const actions = new VoiceReviewActions(editing, { mutate, get: vi.fn() });
    actions.setText("mine");
    await actions.act("send-now");
    await actions.act("play");
    expect(mutate).not.toHaveBeenCalled();
    await actions.act("pause");
    await actions.act("send-now");
    expect(mutate.mock.calls).toEqual([
      ["capture-1", "pause", { expected_revision: 2 }],
      ["capture-1", "edit-begin", { expected_revision: 3 }],
      ["capture-1", "edit-save", { expected_revision: 4, text: "mine", edit_handle: "owned" }],
      ["capture-1", "send-now", { expected_revision: 5 }],
    ]);
  });

  it("keeps another prompt actionable while the first response is delayed", async () => {
    const slow = deferred<VoiceReviewMutationResult>();
    const first = new VoiceReviewActions(held, { mutate: () => slow.promise, get: vi.fn() });
    const secondMutation = vi.fn().mockResolvedValue({
      draft: draft({ id: "capture-2", revision: 2, state: "deleted", text: null }),
      edit_handle: null,
    });
    const second = new VoiceReviewActions(draft({ id: "capture-2" }), {
      mutate: secondMutation,
      get: vi.fn(),
    });
    const pending = first.act("pause");
    await second.act("delete");
    expect(second.draft.state).toBe("deleted");
    expect(first.busy).toBe(true);
    slow.resolve({ draft: draft({ revision: 2, state: "paused" }), edit_handle: null });
    await pending;
  });

  it("discards local fields with the lease and keeps the prompt paused", async () => {
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce(lease)
      .mockResolvedValueOnce(saved(held.text!));
    const actions = new VoiceReviewActions(held, { mutate, get: vi.fn() });
    actions.setInstructions("discard this");
    await idle(actions);
    await actions.act("edit-cancel");
    expect(mutate.mock.calls[1]).toEqual([
      "capture-1",
      "edit-cancel",
      { expected_revision: 2, edit_handle: "opaque" },
    ]);
    expect(actions.draft.state).toBe("paused");
    expect(actions.instructions).toBe("");
    expect(actions.hasLocalWork).toBe(false);
  });

  it("projects held countdowns without a mutation and freezes paused/editing projections", () => {
    expect(remainingSeconds(held, 1500)).toBe(179);
    expect(remainingSeconds(held, 300000)).toBe(0);
    expect(remainingSeconds(draft({ state: "paused" }), 300000)).toBe(180);
    expect(remainingSeconds(editing, 300000)).toBe(180);
    expect(voiceReviewError({ _tag: "VoiceReviewNotConfiguredError" })).toContain("not configured");
    expect(voiceReviewError({ _tag: "VoiceReviewNotFoundError" })).toContain("unavailable");
  });
});

describe("workstream metadata corrections", () => {
  const payload = {
    schema: "voice.association-mutation/v1" as const,
    subject: "prompt:command-1",
    workstream_ref: "inferred:voice-review",
    state: "suppressed" as const,
    expected_revision: 4,
    request_id: "correction-1",
    command_id: "command-1",
  };
  it("preserves CAS and command evidence without using a delivery transport", async () => {
    const receipt = {
      schema: "voice.registry-receipt/v1" as const,
      request_id: "correction-1",
      revision: 5,
      event_sequence: 7,
      record: {
        subject: payload.subject,
        workstream_ref: payload.workstream_ref,
        state: payload.state,
        revision: 5,
        origin: "owner" as const,
        job_id: null,
      },
    };
    const correctAssociation = vi.fn().mockResolvedValue(receipt);
    const actions = new RegistryCorrectionActions({ correctAssociation });
    expect(await actions.correct(payload)).toEqual(receipt);
    expect(correctAssociation).toHaveBeenCalledExactlyOnceWith(payload);
    expect(actions.uncertain).toBe(false);
    await actions.correct({ ...payload, workstream_ref: "native:readonly" });
    expect(correctAssociation).toHaveBeenCalledTimes(1);
  });
  it("never retries an unconfirmed correction until explicit metadata reconciliation", async () => {
    const correctAssociation = vi.fn().mockRejectedValue(new Error("lost response"));
    const actions = new RegistryCorrectionActions({ correctAssociation });
    await actions.correct(payload);
    await actions.correct(payload);
    expect(correctAssociation).toHaveBeenCalledTimes(1);
    expect(actions.uncertain).toBe(true);
    actions.reconciled();
    await actions.correct({ ...payload, request_id: "correction-2", expected_revision: 5 });
    expect(correctAssociation).toHaveBeenCalledTimes(2);
  });
  it("treats an unrelated receipt as unconfirmed", async () => {
    const correctAssociation = vi.fn().mockResolvedValue({ request_id: "someone-else" });
    const actions = new RegistryCorrectionActions({ correctAssociation });
    expect(await actions.correct(payload)).toBeNull();
    expect(actions.uncertain).toBe(true);
  });
});
