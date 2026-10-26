import type { ProviderApprovalOption } from "@t3tools/contracts";
import type { CommandExecutionRequestApprovalResponse } from "effect-codex-app-server/schema";

// The current command approval protocol supports these decisions independently
// of optional policy amendments. Publish them at the producer for all clients.
export const codexCommandApprovalOptions = [
  { decision: "cancel", label: "Cancel" },
  { decision: "decline", label: "Deny" },
  { decision: "acceptForSession", label: "Always allow this session" },
  { decision: "accept", label: "Approve once" },
] satisfies ReadonlyArray<
  ProviderApprovalOption & {
    readonly decision: CommandExecutionRequestApprovalResponse["decision"];
  }
>;
