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

describe("voice review acknowledged actions", () => {
  it("does not enable editing before the acknowledgement; save and send uses the saved revision and literal text", async () => {
    const begin = deferred<VoiceReviewMutationResult>();
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockImplementationOnce(() => begin.promise)
      .mockResolvedValueOnce({
        draft: draft({ revision: 3, state: "paused", text: "<b>edited</b>", edited: true }),
        edit_handle: null,
      })
      .mockResolvedValueOnce({
        draft: draft({ revision: 4, state: "released", text: null }),
        edit_handle: null,
      });
    const actions = new VoiceReviewActions(draft(), { mutate, get: vi.fn() });
    const pending = actions.act("edit-begin");
    expect(actions.editHandle).toBeNull();
    expect(actions.busy).toBe(true);
    begin.resolve({ draft: draft({ revision: 2, state: "editing" }), edit_handle: "opaque" });
    await pending;
    actions.setText("<b>edited</b>");
    await actions.act("send-now");
    expect(mutate).toHaveBeenCalledTimes(1);
    await actions.act("edit-save", true);
    expect(mutate.mock.calls[1]).toEqual([
      "capture-1",
      "edit-save",
      { expected_revision: 2, text: "<b>edited</b>", edit_handle: "opaque" },
    ]);
    expect(mutate.mock.calls[2]).toEqual(["capture-1", "send-now", { expected_revision: 3 }]);
    expect(actions.draft.state).toBe("released");
    expect(actions.editHandle).toBeNull();
  });

  it("preserves unsaved text on conflict and never sends when save was not acknowledged", async () => {
    const mutate = vi
      .fn<VoiceReviewTransport["mutate"]>()
      .mockResolvedValueOnce({
        draft: draft({ revision: 2, state: "editing" }),
        edit_handle: "opaque",
      })
      .mockRejectedValueOnce({ _tag: "VoiceReviewConflictError" });
    const get = vi
      .fn()
      .mockResolvedValue(draft({ revision: 3, state: "editing", text: "server text" }));
    const actions = new VoiceReviewActions(draft(), { mutate, get });
    await actions.act("edit-begin");
    actions.setText("unsaved local text");
    await actions.act("edit-save", true);
    actions.receive(draft({ revision: 3, state: "editing", text: "server text" }));
    expect(actions.text).toBe("unsaved local text");
    expect(actions.draft.revision).toBe(3);
    expect(mutate).toHaveBeenCalledTimes(2);
    expect(get).toHaveBeenCalledWith("capture-1");
  });

  it("observes ambiguous mutations once and gates new mutations until explicit reconciliation", async () => {
    const mutate = vi.fn().mockRejectedValue(new Error("private upstream exception"));
    const get = vi.fn().mockResolvedValue(draft({ state: "released", revision: 2, text: null }));
    const actions = new VoiceReviewActions(draft(), { mutate, get });
    await actions.act("send-now");
    expect(actions.uncertain).toBe(true);
    expect(actions.error).not.toContain("private");
    await actions.act("send-now");
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(get).toHaveBeenCalledTimes(1);
    await actions.reconcile();
    expect(actions.uncertain).toBe(false);
  });

  it("keeps another prompt actionable while the first response is delayed", async () => {
    const slow = deferred<VoiceReviewMutationResult>();
    const first = new VoiceReviewActions(draft(), { mutate: () => slow.promise, get: vi.fn() });
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

  it("save and cancel finish paused; a fresh component does not restore an edit handle or play", async () => {
    for (const action of ["edit-save", "edit-cancel"] as const) {
      const mutate = vi
        .fn<VoiceReviewTransport["mutate"]>()
        .mockResolvedValueOnce({
          draft: draft({ revision: 2, state: "editing" }),
          edit_handle: "holder",
        })
        .mockResolvedValueOnce({
          draft: draft({ revision: 3, state: "paused" }),
          edit_handle: null,
        });
      const actions = new VoiceReviewActions(draft(), { mutate, get: vi.fn() });
      await actions.act("edit-begin");
      await actions.act(action);
      expect(actions.draft.state).toBe("paused");
      expect(actions.editHandle).toBeNull();
    }
    const mutate = vi.fn();
    const remount = new VoiceReviewActions(draft({ state: "editing", revision: 2 }), {
      mutate,
      get: vi.fn(),
    });
    expect(remount.editHandle).toBeNull();
    await remount.act("play");
    expect(mutate).not.toHaveBeenCalled();
  });

  it("projects held countdowns without a mutation and freezes paused/editing projections", () => {
    expect(remainingSeconds(draft(), 1500)).toBe(179);
    expect(remainingSeconds(draft(), 300000)).toBe(0);
    expect(remainingSeconds(draft({ state: "paused" }), 300000)).toBe(180);
    expect(remainingSeconds(draft({ state: "editing" }), 300000)).toBe(180);
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
