import type {
  VoiceReviewAction,
  VoiceReviewDraft,
  VoiceReviewMutationPayload,
  VoiceReviewMutationResult,
  VoiceReviewSendOptions,
  VoiceReviewSendReviewedPayload,
  VoiceReviewSendReviewedResult,
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
  sendOptions(id: string): Promise<VoiceReviewSendOptions | null>;
  sendReviewed(
    id: string,
    payload: VoiceReviewSendReviewedPayload,
  ): Promise<VoiceReviewSendReviewedResult>;
}

export type PendingSendOptions =
  | { readonly kind: "checking" | "failed" | "unsupported" }
  | { readonly kind: "available"; readonly options: VoiceReviewSendOptions };

export function reviewedSendBlocker(
  sendOptions: PendingSendOptions,
  workstreamId: string | null,
  availableWorkstreamIds: ReadonlySet<string>,
) {
  if (sendOptions.kind === "checking") return "Checking where this prompt can be sent…";
  if (sendOptions.kind === "failed") return "Could not check this prompt’s destinations.";
  if (workstreamId !== null) {
    if (sendOptions.kind !== "available" || !availableWorkstreamIds.has(workstreamId))
      return "The selected Workstream is unavailable on this source. Choose another option.";
    return null;
  }
  if (sendOptions.kind !== "available" || sendOptions.options.can_send_without_workstream)
    return null;
  return availableWorkstreamIds.size > 0
    ? "Choose a Workstream inbox to send this prompt."
    : "No receiving thread is available. Check the receiving thread or inbox configuration for this voice source.";
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
const checkingSendOptions: PendingSendOptions = { kind: "checking" };
const noWorkstreamIds: ReadonlySet<string> = new Set();

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
  workstreamId: string | null = null;
  sendOptions: PendingSendOptions = checkingSendOptions;
  private sendOptionsRequest: object | null = null;
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
    return this.localText !== null || this.localInstructions !== null || this.workstreamId !== null;
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
    if (
      draft.id !== this.draft.id ||
      draft.source_id !== this.draft.source_id ||
      draft.revision < this.draft.revision
    )
      return false;
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
    return true;
  }

  async checkSendOptions() {
    const request = {};
    this.sendOptionsRequest = request;
    this.sendOptions = checkingSendOptions;
    this.notify();
    try {
      const options = await this.transport.sendOptions(this.draft.id);
      if (this.sendOptionsRequest !== request) return;
      this.sendOptions =
        options === null ? { kind: "unsupported" } : { kind: "available", options };
    } catch {
      if (this.sendOptionsRequest !== request) return;
      this.sendOptions = { kind: "failed" };
    }
    this.sendOptionsRequest = null;
    this.notify();
  }

  invalidateSendOptions() {
    this.sendOptionsRequest = null;
    if (this.sendOptions === checkingSendOptions) return;
    this.sendOptions = checkingSendOptions;
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

  setWorkstream(workstreamId: string | null) {
    if (!this.canType || workstreamId === this.workstreamId) return;
    if (
      workstreamId !== null &&
      (this.sendOptions.kind !== "available" ||
        !this.sendOptions.options.workstream_ids.includes(workstreamId))
    )
      return;
    this.workstreamId = workstreamId;
    this.onInput();
  }

  get canType() {
    return !ended.has(this.draft.state) && !this.uncertain && (!this.busy || this.acquiringEdit);
  }

  private onInput() {
    this.notify();
    // Focus and empty fields leave the default release countdown running.
    if (
      (this.needsSave || this.workstreamId !== null) &&
      !this.busy &&
      !this.reviewRequired &&
      this.draft.state !== "editing"
    ) {
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
    const { text } = this.draft;
    return this.commitMutation(
      payload.expected_revision,
      () => this.transport.mutate(this.draft.id, action, payload),
      (result) =>
        action === "edit-begin"
          ? result.edit_handle !== null &&
            result.draft.state === "editing" &&
            result.draft.revision > payload.expected_revision &&
            result.draft.text === text
          : result.edit_handle === null,
    );
  }

  private async commitMutation(
    revision: number,
    run: () => Promise<VoiceReviewMutationResult>,
    accepts: (result: VoiceReviewMutationResult) => boolean,
  ) {
    const { id, source_id } = this.draft;
    try {
      const result = await run();
      if (
        result.draft.id !== id ||
        result.draft.source_id !== source_id ||
        result.draft.revision < revision ||
        !accepts(result)
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

  private discardComposition() {
    this.clearFields();
    this.workstreamId = null;
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

  async act(action: PendingReviewAction, availableWorkstreamIds = noWorkstreamIds) {
    if (this.busy || this.uncertain) return;
    if (action === "edit-cancel") {
      if (this.editHandle !== null) {
        await this.perform(async () => {
          await this.mutate("edit-cancel", {
            expected_revision: this.draft.revision,
            edit_handle: this.editHandle!,
          });
          this.discardComposition();
        });
      } else {
        this.discardComposition();
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
    if (action === "play" && (editing || this.needsSave || this.workstreamId !== null)) return;
    if (action === "edit-save" || action === "send-now") {
      if ((editing && this.editHandle === null) || this.compositionError) return;
      const composition = this.composition;
      if (action === "send-now") {
        const { sendOptions, workstreamId } = this;
        if (reviewedSendBlocker(sendOptions, workstreamId, availableWorkstreamIds)) return;
        if (sendOptions.kind === "available") {
          if (workstreamId !== null && !sendOptions.options.workstream_ids.includes(workstreamId))
            return;
          await this.sendReviewed(composition, workstreamId);
          return;
        }
      }
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
      if (action === "delete" && this.draft.state === "deleted") this.discardComposition();
    });
  }

  private async sendReviewed(composition: PendingComposition, workstreamId: string | null) {
    const { id, revision, text } = this.draft;
    const payload: VoiceReviewSendReviewedPayload = {
      expected_revision: revision,
      workstream_id: workstreamId,
      ...(composition.text !== (text ?? "") ? { text: composition.text } : {}),
      ...(composition.instructions ? { additional_instructions: composition.instructions } : {}),
      ...(this.editHandle === null ? {} : { edit_handle: this.editHandle }),
    };
    await this.perform(
      async () => {
        await this.commitMutation(
          revision,
          () => this.transport.sendReviewed(id, payload),
          (result) =>
            result.edit_handle === null &&
            result.draft.revision > revision &&
            result.draft.state === "released" &&
            !!result.draft.command_id,
        );
        this.discardComposition();
      },
      (error) =>
        errorTag(error) === "VoiceReviewConflictError"
          ? "The prompt or its destination changed. Check the current state and available destinations before sending again."
          : voiceReviewError(error),
    );
  }

  private async perform(run: () => Promise<void>, describeError = voiceReviewError) {
    this.busy = true;
    this.error = null;
    this.notify();
    try {
      await run();
    } catch (error) {
      // Read the same draft after an unsuccessful mutation without replaying the attempt.
      try {
        this.receive(await this.transport.get(this.draft.id));
      } catch {
        // Retain the last acknowledged state when readback is unavailable.
      }
      this.error = describeError(error);
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
      if (!this.receive(observation)) return;
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
