import type { OrchestrationV2RuntimeRequest } from "@t3tools/contracts";
import type { ComposerDispatchMode } from "@t3tools/client-runtime/state/composer-dispatch";

export function resolveQuestionAwareDispatchMode(input: {
  readonly busy: boolean;
  readonly setupRunning: boolean;
  readonly followUpAction: ComposerDispatchMode;
  readonly requests: ReadonlyArray<
    Pick<OrchestrationV2RuntimeRequest, "kind" | "status" | "responseCapability">
  >;
}): "queue" | "auto" | null {
  // Steering can cancel the provider call that owns a live question. Queue the
  // ordinary message without answering or dismissing that request.
  if (
    input.busy &&
    input.requests.some(
      (request) =>
        request.kind === "user_input" &&
        request.status === "pending" &&
        request.responseCapability.type === "live",
    )
  ) {
    return "queue";
  }
  return input.setupRunning
    ? "queue"
    : input.followUpAction === "auto"
      ? null
      : input.followUpAction === "queue"
        ? "queue"
        : "auto";
}
