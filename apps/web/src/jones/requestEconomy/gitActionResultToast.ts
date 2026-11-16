import type {
  GitRunStackedActionResult,
  GitStackedAction,
  ScopedThreadRef,
} from "@t3tools/contracts";
import { stackedThreadToast, toastManager } from "../../components/ui/toast";
import { resolveGitActionResultToastTiming } from "../../components/GitActionsControl.logic";

/** Actionable results survive navigation with the environment and command closure that produced them. */
export function showScopedGitActionResult(input: {
  readonly result: GitRunStackedActionResult;
  readonly threadRef: ScopedThreadRef | null;
  readonly run: (action: GitStackedAction) => Promise<void>;
  readonly openPullRequest: (url: string) => Promise<unknown>;
}): void {
  const cta = input.result.toast.cta;
  if (cta.kind !== "run_action" && cta.kind !== "open_pr") return;
  let id: ReturnType<typeof toastManager.add> | null = null;
  const timing = resolveGitActionResultToastTiming("success");
  id = toastManager.add(
    stackedThreadToast({
      type: "success",
      title: input.result.toast.title,
      description: input.result.toast.description,
      timeout: timing.timeout,
      data: {
        threadRef: input.threadRef,
        ...(timing.dismissAfterVisibleMs === null
          ? {}
          : { dismissAfterVisibleMs: timing.dismissAfterVisibleMs }),
      },
      actionProps: {
        children: cta.label,
        onClick: () => {
          if (id !== null) toastManager.close(id);
          if (cta.kind === "run_action") void input.run(cta.action.kind);
          else void input.openPullRequest(cta.url).catch(() => undefined);
        },
      },
    }),
  );
}
