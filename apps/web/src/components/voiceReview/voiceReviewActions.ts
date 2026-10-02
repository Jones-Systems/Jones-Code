import type {
  VoiceReviewAction,
  VoiceReviewDraft,
  VoiceReviewMutationPayload,
  VoiceReviewMutationResult,
} from "@t3tools/contracts";

export interface VoiceReviewTransport {
  mutate(
    id: string,
    action: VoiceReviewAction,
    payload: VoiceReviewMutationPayload,
  ): Promise<VoiceReviewMutationResult>;
  get(id: string): Promise<VoiceReviewDraft>;
}

export function voiceReviewError(error: unknown): string {
  const tag = typeof error === "object" && error !== null && "_tag" in error ? error._tag : null;
  switch (tag) {
    case "VoiceReviewNotConfiguredError":
      return "Voice review is not configured for this environment.";
    case "VoiceReviewForbiddenError":
      return "This session cannot review voice prompts.";
    case "VoiceReviewNotFoundError":
      return "Voice review is unavailable on this environment, or this prompt no longer exists.";
    case "VoiceReviewConflictError":
      return "This prompt changed. Your unsaved text is preserved; review its current state before acting again.";
    default:
      return "The result is uncertain. Check the current prompt state before taking another action.";
  }
}

export class VoiceReviewActions {
  draft: VoiceReviewDraft;
  text: string;
  editHandle: string | null = null;
  busy = false;
  error: string | null = null;
  uncertain = false;
  observedAt = performance.now();
  private listeners = new Set<() => void>();
  private version = 0;
  constructor(
    draft: VoiceReviewDraft,
    private transport: VoiceReviewTransport,
  ) {
    this.draft = draft;
    this.observedAt = performance.now();
    this.text = draft.text ?? "";
  }
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  snapshot = () => this.version;
  private notify() {
    this.version++;
    this.listeners.forEach((listener) => listener());
  }
  receive(draft: VoiceReviewDraft) {
    if (draft.revision < this.draft.revision) return;
    this.draft = draft;
    this.observedAt = performance.now();
    if (this.editHandle === null) this.text = draft.text ?? "";
    this.notify();
  }
  setText(text: string) {
    this.text = text;
    this.notify();
  }
  async act(action: VoiceReviewAction, sendAfterSave = false) {
    if (this.busy || this.uncertain) return;
    if (["released", "deleted", "expired"].includes(this.draft.state)) return;
    if (this.draft.state === "editing" && (action === "send-now" || action === "play")) return;
    if ((action === "edit-save" || action === "edit-cancel") && this.editHandle === null) return;
    this.busy = true;
    this.error = null;
    this.notify();
    try {
      const payload: VoiceReviewMutationPayload = {
        expected_revision: this.draft.revision,
        ...(action === "edit-save" ? { text: this.text, edit_handle: this.editHandle! } : {}),
        ...(action === "edit-cancel" ? { edit_handle: this.editHandle! } : {}),
      };
      const result = await this.transport.mutate(this.draft.id, action, payload);
      this.draft = result.draft;
      this.observedAt = performance.now();
      this.editHandle = result.edit_handle;
      if (action === "edit-save" || action === "edit-cancel") this.text = result.draft.text ?? "";
      if (sendAfterSave && action === "edit-save") {
        const sent = await this.transport.mutate(this.draft.id, "send-now", {
          expected_revision: result.draft.revision,
        });
        this.draft = sent.draft;
        this.observedAt = performance.now();
      }
    } catch (error) {
      this.error = voiceReviewError(error);
      const tag =
        typeof error === "object" && error !== null && "_tag" in error ? error._tag : null;
      this.uncertain =
        tag !== "VoiceReviewConflictError" &&
        tag !== "VoiceReviewForbiddenError" &&
        tag !== "VoiceReviewNotConfiguredError" &&
        tag !== "VoiceReviewNotFoundError";
      // A possibly effective mutation is observed by the same ID, never retried.
      try {
        this.draft = await this.transport.get(this.draft.id);
        this.observedAt = performance.now();
      } catch {
        /* Keep the last acknowledged state until observation succeeds. */
      }
    } finally {
      this.busy = false;
      this.notify();
    }
  }
  async reconcile() {
    try {
      this.receive(await this.transport.get(this.draft.id));
      this.uncertain = false;
      this.error = null;
    } catch (error) {
      this.error = voiceReviewError(error);
    }
    this.notify();
  }
}

export function remainingSeconds(draft: VoiceReviewDraft, elapsedMs: number) {
  if (draft.remaining_ms === null) return null;
  return Math.ceil(
    Math.max(0, draft.remaining_ms - (draft.state === "held" ? elapsedMs : 0)) / 1000,
  );
}
