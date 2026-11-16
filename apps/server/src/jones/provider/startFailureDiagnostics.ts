import * as NodeUtil from "node:util";
import type { OrchestrationV2ProviderFailure } from "@t3tools/contracts";

const reasons = new Set([
  "runtime_use_app_thread_unbound",
  "runtime_use_attachment_physical_check_inside_transaction",
  "runtime_use_attachment_publication_outside_transaction",
  "runtime_use_attachment_publication_owner_changed",
  "runtime_use_conflicting_deletion",
  "runtime_use_conversation_changed",
  "runtime_use_conversation_missing",
  "runtime_use_conversation_unavailable",
  "runtime_use_default_workspace_mismatch",
  "runtime_use_expected_outbox_claim_changed",
  "runtime_use_identity_changed",
  "runtime_use_initial_producer_thread_mismatch",
  "runtime_use_inventory_changed",
  "runtime_use_open_outside_workspace_exclusion",
  "runtime_use_opened_session_mismatch",
  "runtime_use_original_coverage_changed",
  "runtime_use_original_outbox_reference_changed",
  "runtime_use_original_scope_changed",
  "runtime_use_original_session_replaced",
  "runtime_use_owner_without_actual_database",
  "runtime_use_prior_outcome_unknown_no_replay",
  "runtime_use_producer_session_mismatch",
  "runtime_use_provider_thread_missing",
  "runtime_use_provider_thread_replaced",
  "runtime_use_provider_thread_unbound",
  "runtime_use_requested_workspace_mismatch",
  "runtime_use_response_caller_changed",
  "runtime_use_response_changed_before_dispatch",
  "runtime_use_response_original_intent_changed",
  "runtime_use_response_request_changed",
  "runtime_use_response_request_missing",
  "runtime_use_scope_changed",
  "runtime_use_scope_missing",
  "runtime_use_terminal_cwd_outside_workspace",
  "runtime_use_terminal_scope_changed",
  "runtime_use_terminal_thread_missing",
  "runtime_use_unavailable",
  "runtime_use_uncovered_runtime",
  "runtime_use_workspace_alias",
  "runtime_use_workspace_changed",
  "runtime_use_workspace_not_canonical",
  "runtime_use_workspace_replaced",
]);

function dataProperty(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && "value" in descriptor ? descriptor.value : undefined;
}

// Only the direct cause chain is needed for session-open failures. Opaque or
// exhausted chains carry no attribution; a recognized error has a fixed fallback.
export function startFailureDiagnosticCode(input: unknown): string | undefined {
  const seen = new Set<object>();
  let current = input;
  try {
    for (let count = 0; count < 16; count += 1) {
      if (
        typeof current !== "object" ||
        current === null ||
        NodeUtil.types.isProxy(current) ||
        seen.has(current)
      ) {
        return undefined;
      }
      seen.add(current);
      if (dataProperty(current, "_tag") === "RuntimeUseError") {
        const reason = dataProperty(current, "reason");
        return typeof reason === "string" && reasons.has(reason)
          ? reason
          : "runtime_use_unclassified";
      }
      current = dataProperty(current, "cause");
    }
  } catch {
    return undefined;
  }
  return undefined;
}

export function withStartFailureDiagnostic(
  failure: OrchestrationV2ProviderFailure,
  cause: unknown,
): OrchestrationV2ProviderFailure {
  if (failure.code !== null) return failure;
  const code = startFailureDiagnosticCode(cause);
  return code === undefined ? failure : { ...failure, code };
}
