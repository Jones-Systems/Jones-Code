export interface CompanionActionEvidence {
  readonly controlled: boolean;
  readonly dispatched: boolean;
  readonly attachmentGeneration: number | null;
  readonly currentAttachmentGeneration: number | null;
}
const validationErrors = new Set([
  "PreviewAutomationInvalidSelectorError",
  "PreviewAutomationTargetNotEditableError",
  "PreviewAutomationUnsupportedOperationError",
]);
const lostConnection = (cause: unknown) =>
  cause instanceof Error &&
  /(?:target|page|context|browser|session|connection).*(?:closed|disconnected)|(?:closed|disconnected).*(?:target|page|context|browser|session|connection)/i.test(
    cause.message,
  );

export const companionOutcome = (
  evidence: CompanionActionEvidence | undefined,
  cause?: unknown,
  errorTag?: string,
): "not_started" | "unknown" | undefined => {
  if (!evidence?.controlled) return undefined;
  if (!evidence.dispatched) return cause === undefined ? undefined : "not_started";
  if (errorTag !== undefined && validationErrors.has(errorTag)) return "not_started";
  if (
    evidence.attachmentGeneration === null ||
    evidence.currentAttachmentGeneration !== evidence.attachmentGeneration ||
    lostConnection(cause)
  )
    return "unknown";
  return undefined;
};
