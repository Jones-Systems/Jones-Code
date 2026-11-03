import { RegistryContext, useAtomValue } from "@effect/atom-react";
import type {
  DescendantApprovalModel,
  DescendantApprovalState,
} from "@t3tools/client-runtime/jones/descendant-approval";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ProviderApprovalDecision,
  RuntimeRequestId,
  ScopedThreadRef,
  ThreadId,
} from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { ShieldQuestionIcon } from "lucide-react";
import { useContext, useId } from "react";
import { ComposerPendingApprovalActions } from "../../components/chat/ComposerPendingApprovalActions";
import { ComposerPendingApprovalPanel } from "../../components/chat/ComposerPendingApprovalPanel";
import type { ComposerBannerStackItem } from "../../components/chat/ComposerBannerStack";
import { Button } from "../../components/ui/button";
import { toastManager } from "../../components/ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { descendantApproval } from "./atoms";

const emptyStateAtom = Atom.make<DescendantApprovalState | null>(null);
export type DescendantApprovalDispatch = (input: {
  readonly environmentId: EnvironmentId;
  readonly input: {
    readonly threadId: ThreadId;
    readonly requestId: RuntimeRequestId;
    readonly decision: ProviderApprovalDecision;
  };
}) => Promise<AtomCommandResult<unknown, unknown>>;

export function useDescendantApprovalBanner({
  parentRef,
  onOpenThread,
  model = descendantApproval,
  dispatch,
}: {
  readonly parentRef: ScopedThreadRef | null;
  readonly onOpenThread: (ref: ScopedThreadRef) => void;
  readonly model?: DescendantApprovalModel;
  readonly dispatch?: DescendantApprovalDispatch;
}): ComposerBannerStackItem | null {
  const registry = useContext(RegistryContext);
  const state = useAtomValue(parentRef === null ? emptyStateAtom : model.stateAtom(parentRef));
  const respondToApproval = useOrchestrationCommand(threadEnvironment.respondToApproval, {
    reportFailure: false,
  });
  const reasonId = useId();
  if (state === null || parentRef === null) return null;
  const { selected, view } = state;
  const approval = view.approval;
  const disabled = view.kind !== "actionable" || state.responding;
  const reason =
    view.kind === "unavailable"
      ? view.message
      : state.responding
        ? "Sending approval response."
        : undefined;
  const respond = async (requestId: RuntimeRequestId, decision: ProviderApprovalDecision) => {
    try {
      const response = await model.respond(registry, {
        parent: parentRef,
        child: selected.ref,
        requestId,
        dispatch: () =>
          (dispatch ?? respondToApproval)({
            environmentId: selected.ref.environmentId,
            input: { threadId: selected.ref.threadId, requestId, decision },
          }),
      });
      if (response.kind === "not-current") {
        toastManager.add({ type: "info", title: "This approval is no longer current." });
      } else if (
        response.kind === "sent" &&
        response.result._tag === "Failure" &&
        !isAtomCommandInterrupted(response.result)
      ) {
        const error = squashAtomCommandFailure(response.result);
        toastManager.add({
          type: "error",
          title: error instanceof Error ? error.message : "Failed to submit approval decision.",
        });
      }
    } catch (error) {
      toastManager.add({
        type: "error",
        title: error instanceof Error ? error.message : "Failed to submit approval decision.",
      });
    }
  };
  return {
    id: `descendant-approval:${selected.key}`,
    variant: "warning",
    priority: "urgent",
    icon: <ShieldQuestionIcon />,
    title: `${selected.title} needs approval`,
    description:
      state.candidates.length > 1
        ? `${state.position} of ${state.candidates.length} subagents waiting`
        : undefined,
    children: (
      <div className="flex min-w-0 flex-col gap-2">
        {approval !== null ? (
          <ComposerPendingApprovalPanel
            approval={approval}
            pendingCount={view.kind === "actionable" ? view.pendingCount : 1}
          />
        ) : null}
        {reason ? (
          <p id={reasonId} className="text-xs text-muted-foreground" role="status">
            {reason}
          </p>
        ) : null}
        {approval !== null ? (
          <Tooltip disabled={reason === undefined}>
            <TooltipTrigger
              render={
                <div
                  className="flex flex-wrap items-center gap-1.5 rounded-sm focus-visible:outline-2 focus-visible:outline-ring"
                  role="group"
                  aria-label="Subagent approval decisions"
                  aria-describedby={reason ? reasonId : undefined}
                  tabIndex={disabled ? 0 : undefined}
                />
              }
            >
              <ComposerPendingApprovalActions
                requestId={approval.requestId}
                options={approval.options}
                canRespond={view.kind === "actionable"}
                isResponding={state.responding}
                disabled={disabled}
                onRespondToApproval={respond}
              />
            </TooltipTrigger>
            <TooltipPopup>{reason}</TooltipPopup>
          </Tooltip>
        ) : null}
      </div>
    ),
    actions: (
      <>
        {state.candidates.length > 1 ? (
          <Button size="xs" variant="ghost" onClick={() => model.selectNext(registry, parentRef)}>
            Next
          </Button>
        ) : null}
        <Button size="xs" variant="ghost" onClick={() => onOpenThread(selected.ref)}>
          Open
        </Button>
      </>
    ),
  };
}
