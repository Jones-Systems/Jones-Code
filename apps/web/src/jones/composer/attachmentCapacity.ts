import { PROVIDER_SEND_TURN_MAX_ATTACHMENTS } from "@t3tools/contracts";

export function remainingComposerAttachmentSlots(attached: number, pending: number): number {
  return Math.max(0, PROVIDER_SEND_TURN_MAX_ATTACHMENTS - attached - pending);
}
