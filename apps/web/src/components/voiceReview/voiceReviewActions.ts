import type {
  VoiceReviewAction,
  VoiceReviewDraft,
  VoiceReviewMutationPayload,
  VoiceReviewMutationResult,
  ThreadRegistryAssociationPayload,
  ThreadRegistryMutationReceipt,
} from "@t3tools/contracts";

export interface RegistryCorrectionTransport {
  correctAssociation(
    payload: ThreadRegistryAssociationPayload,
  ): Promise<ThreadRegistryMutationReceipt>;
}

export class RegistryCorrectionActions {
  busy = false;
  uncertain = false;
  error: string | null = null;
  constructor(private transport: RegistryCorrectionTransport) {}
  async correct(payload: ThreadRegistryAssociationPayload) {
    if (this.busy || this.uncertain || !payload.workstream_ref.startsWith("inferred:")) return null;
    this.busy = true;
    this.error = null;
    try {
      const receipt = await this.transport.correctAssociation(payload);
      if (
        receipt.request_id !== payload.request_id ||
        !("subject" in receipt.record) ||
        receipt.record.subject !== payload.subject ||
        receipt.record.workstream_ref !== payload.workstream_ref ||
        receipt.record.state !== payload.state ||
        receipt.record.revision <= payload.expected_revision
      )
        throw new Error("Unmatched correction receipt");
      return receipt;
    } catch {
      this.uncertain = true;
      this.error =
        "Correction is unconfirmed. Refresh the workstreams to review current metadata before another change.";
      return null;
    } finally {
      this.busy = false;
    }
  }
  reconciled() {
    this.uncertain = false;
    this.error = null;
  }
}

export interface VoiceReviewTransport {
  mutate(
    id: string,
    action: VoiceReviewAction,
    payload: VoiceReviewMutationPayload,
  ): Promise<VoiceReviewMutationResult>;
  get(id: string): Promise<VoiceReviewDraft>;
}

export function voiceReviewError(error: unknown): string {
  const tag = errorTag(error);
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

export const VOICE_REVIEW_TEXT_LIMIT = 100_000;

interface PendingComposition {
  readonly text: string;
  readonly instructions?: string;
}

interface LegacySave {
  readonly revision: number;
  readonly composition: PendingComposition;
  readonly text: string;
}

export type PendingReviewAction = Exclude<VoiceReviewAction, "edit-begin">;

const ended = new Set<VoiceReviewDraft["state"]>(["released", "deleted", "expired"]);

function errorTag(error: unknown) {
  return typeof error === "object" && error !== null && "_tag" in error ? error._tag : null;
}

function legacyPromptText({ text, instructions }: PendingComposition) {
  return instructions === undefined ? text : `${text}\n\nAdditional instructions:\n${instructions}`;
}

export class VoiceReviewActions {
  draft: VoiceReviewDraft;
  editHandle: string | null = null;
  busy = false;
  acquiringEdit = false;
  error: string | null = null;
  reviewRequired = false;
  observedAt = performance.now();
  private localText: string | null = null;
  private localInstructions: string | null = null;
  private savedComposition: LegacySave | null = null;
  private unconfirmedSave: LegacySave | null = null;
  private unconfirmedRevision: number | null = null;
  private listeners = new Set<() => void>();

  constructor(
    draft: VoiceReviewDraft,
    private transport: VoiceReviewTransport,
  ) {
    this.draft = draft;
  }

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  private get observedComposition(): PendingComposition {
    return this.savedComposition?.text === this.draft.text
      ? this.savedComposition.composition
      : { text: this.draft.text ?? "" };
  }

  get text() {
    return this.localText ?? this.observedComposition.text;
  }

  get instructions() {
    return this.localInstructions ?? this.observedComposition.instructions ?? "";
  }

  get composition(): PendingComposition {
    const instructions = this.instructions.trim();
    return {
      text: this.text.trim() ? this.text : this.observedComposition.text,
      ...(instructions ? { instructions } : {}),
    };
  }

  get compositionError() {
    const composition = this.composition;
    if (!composition.text.trim()) return "Enter prompt text to continue.";
    if (legacyPromptText(composition).length > VOICE_REVIEW_TEXT_LIMIT)
      return "Prompt text and instructions together exceed the length limit.";
    return null;
  }

  get hasChanges() {
    return this.localText !== null || this.localInstructions !== null;
  }

  get needsSave() {
    return legacyPromptText(this.composition) !== (this.draft.text ?? "");
  }

  get uncertain() {
    return this.unconfirmedRevision !== null;
  }

  get hasLocalWork() {
    return (
      this.hasChanges ||
      this.editHandle !== null ||
      this.busy ||
      this.uncertain ||
      this.reviewRequired
    );
  }

  private notify() {
    this.listeners.forEach((listener) => listener());
  }

  receive(draft: VoiceReviewDraft) {
    if (draft.id !== this.draft.id || draft.revision < this.draft.revision) return;
    const lostEdit =
      this.editHandle !== null &&
      (draft.revision !== this.draft.revision || draft.state !== "editing");
    const changedText = this.hasChanges && draft.text !== this.draft.text;
    if (lostEdit || changedText) {
      this.editHandle = null;
      this.reviewRequired = true;
      if (!this.uncertain) this.error = voiceReviewError({ _tag: "VoiceReviewConflictError" });
    }
    this.draft = draft;
    this.observedAt = performance.now();
    this.notify();
  }

  setText(text: string) {
    if (!this.canType) return;
    this.localText = text === this.observedComposition.text ? null : text;
    this.onInput();
  }

  setInstructions(instructions: string) {
    if (!this.canType) return;
    const observed = this.observedComposition.instructions ?? "";
    this.localInstructions = instructions.trim() === observed ? null : instructions;
    this.onInput();
  }

  get canType() {
    return !ended.has(this.draft.state) && !this.uncertain && (!this.busy || this.acquiringEdit);
  }

  private onInput() {
    this.notify();
    // Focus and empty fields leave the default release countdown running.
    if (this.needsSave && !this.busy && !this.reviewRequired && this.draft.state !== "editing") {
      this.acquiringEdit = true;
      void this.perform(async () => {
        try {
          await this.beginEdit();
        } finally {
          this.acquiringEdit = false;
        }
      });
    }
  }

  private async mutate(
    action: VoiceReviewAction,
    payload: VoiceReviewMutationPayload = { expected_revision: this.draft.revision },
  ) {
    const revision = payload.expected_revision;
    const { id, source_id, text } = this.draft;
    try {
      const result = await this.transport.mutate(id, action, payload);
      if (
        result.draft.id !== id ||
        result.draft.source_id !== source_id ||
        result.draft.revision < revision ||
        (action === "edit-begin" &&
          (result.edit_handle === null ||
            result.draft.state !== "editing" ||
            result.draft.revision <= revision ||
            result.draft.text !== text)) ||
        (action !== "edit-begin" && result.edit_handle !== null)
      ) {
        throw new Error("Unmatched voice review acknowledgement");
      }
      if (result.draft.revision < this.draft.revision) {
        throw Object.assign(new Error("Voice review acknowledgement was superseded"), {
          _tag: "VoiceReviewConflictError",
        });
      }
      this.draft = result.draft;
      this.observedAt = performance.now();
      this.editHandle = result.edit_handle;
      this.reviewRequired = false;
      this.error = null;
      return result;
    } catch (error) {
      const tag = errorTag(error);
      this.reviewRequired = tag === "VoiceReviewConflictError";
      if (this.reviewRequired) this.editHandle = null;
      if (
        !this.reviewRequired &&
        tag !== "VoiceReviewForbiddenError" &&
        tag !== "VoiceReviewNotConfiguredError" &&
        tag !== "VoiceReviewNotFoundError"
      ) {
        this.unconfirmedRevision = revision;
      }
      throw error;
    }
  }

  private async beginEdit() {
    await this.mutate("edit-begin");
  }

  private clearFields() {
    this.localText = null;
    this.localInstructions = null;
  }

  private recordSave(save: LegacySave) {
    this.savedComposition = save;
    this.unconfirmedSave = null;
  }

  private async save(composition: PendingComposition) {
    if (this.editHandle === null) await this.beginEdit();
    const save = {
      revision: this.draft.revision,
      composition,
      text: legacyPromptText(composition),
    };
    const { draft } = await this.mutate("edit-save", {
      expected_revision: save.revision,
      text: save.text,
      edit_handle: this.editHandle!,
    }).catch((error: unknown) => {
      if (this.uncertain) this.unconfirmedSave = save;
      throw error;
    });
    if (draft.revision <= save.revision || draft.state !== "paused" || draft.text !== save.text) {
      this.unconfirmedSave = save;
      this.unconfirmedRevision = save.revision;
      throw new Error("Unmatched voice review save acknowledgement");
    }
    this.recordSave(save);
    return draft.revision;
  }

  async act(action: PendingReviewAction) {
    if (this.busy || this.uncertain) return;
    if (action === "edit-cancel") {
      if (this.editHandle !== null) {
        await this.perform(async () => {
          await this.mutate("edit-cancel", {
            expected_revision: this.draft.revision,
            edit_handle: this.editHandle!,
          });
          this.clearFields();
        });
      } else {
        this.clearFields();
        if (ended.has(this.draft.state)) {
          this.reviewRequired = false;
          this.error = null;
        }
        this.notify();
      }
      return;
    }
    if (ended.has(this.draft.state) || this.reviewRequired) return;
    const editing = this.draft.state === "editing";
    if (action === "play" && (editing || this.needsSave)) return;
    if (action === "edit-save" || action === "send-now") {
      if ((editing && this.editHandle === null) || this.compositionError) return;
      const composition = this.composition;
      await this.perform(async () => {
        if (this.editHandle !== null || this.needsSave) {
          const revision = await this.save(composition);
          if (action === "send-now") await this.mutate("send-now", { expected_revision: revision });
        } else if (action === "send-now") {
          await this.mutate("send-now");
        }
        this.clearFields();
      });
      return;
    }
    await this.perform(async () => {
      await this.mutate(action);
      if (action === "delete" && this.draft.state === "deleted") this.clearFields();
    });
  }

  private async perform(run: () => Promise<void>) {
    this.busy = true;
    this.error = null;
    this.notify();
    try {
      await run();
    } catch (error) {
      this.error = voiceReviewError(error);
      // Observe the same ID. A failed or unconfirmed save never advances to send.
      try {
        this.receive(await this.transport.get(this.draft.id));
      } catch {
        // Retain the last acknowledged state when readback is unavailable.
      }
    } finally {
      this.busy = false;
      this.notify();
    }
  }

  async reconcile() {
    if (this.busy) return;
    this.busy = true;
    this.notify();
    try {
      const observation = await this.transport.get(this.draft.id);
      this.receive(observation);
      if (observation.revision < this.draft.revision) return;
      if (this.unconfirmedRevision !== null && observation.revision <= this.unconfirmedRevision)
        return;
      const save = this.unconfirmedSave;
      if (save && observation.revision > save.revision && observation.text === save.text) {
        this.recordSave(save);
        this.clearFields();
      }
      this.unconfirmedSave = null;
      this.unconfirmedRevision = null;
      this.reviewRequired = false;
      this.error = null;
    } catch (error) {
      this.error = voiceReviewError(error);
    } finally {
      this.busy = false;
      this.notify();
    }
  }
}

export function remainingSeconds(draft: VoiceReviewDraft, elapsedMs: number) {
  if (draft.remaining_ms === null) return null;
  return Math.ceil(
    Math.max(0, draft.remaining_ms - (draft.state === "held" ? elapsedMs : 0)) / 1000,
  );
}
