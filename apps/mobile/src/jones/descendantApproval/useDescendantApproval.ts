import { RegistryContext, useAtomValue } from "@effect/atom-react";
import type { DescendantApprovalState } from "@t3tools/client-runtime/jones/descendant-approval";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type {
  ProviderApprovalDecision,
  RuntimeRequestId,
  ScopedThreadRef,
} from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { useContext } from "react";

import * as Dialog from "../../components/ui/Dialog";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { descendantApprovals } from "./atoms";

const EMPTY_STATE = Atom.make<DescendantApprovalState | null>(null);

export function useDescendantApproval(parent: ScopedThreadRef | null) {
  const registry = useContext(RegistryContext);
  const state = useAtomValue(parent === null ? EMPTY_STATE : descendantApprovals.stateAtom(parent));
  const dispatch = useAtomCommand(threadEnvironment.respondToApproval, {
    label: "subagent approval response",
    reportFailure: false,
  });

  const onRespond = async (requestId: RuntimeRequestId, decision: ProviderApprovalDecision) => {
    if (parent === null || state === null) return;
    const response = await descendantApprovals
      .respond(registry, {
        parent,
        child: state.selected.ref,
        requestId,
        dispatch: () =>
          dispatch({
            environmentId: state.selected.ref.environmentId,
            input: { threadId: state.selected.ref.threadId, requestId, decision },
          }),
      })
      .catch((error: unknown) => {
        Dialog.alert(
          "Could not respond to approval",
          error instanceof Error ? error.message : "Try again.",
        );
        return null;
      });
    if (response === null) return;
    if (response.kind === "not-current") {
      Dialog.alert(
        "Approval changed",
        "This approval is no longer available. Check the current subagent request.",
      );
    } else if (
      response.kind === "sent" &&
      response.result._tag === "Failure" &&
      !isAtomCommandInterrupted(response.result)
    ) {
      const error = squashAtomCommandFailure(response.result);
      Dialog.alert(
        "Could not respond to approval",
        error instanceof Error ? error.message : "Try again.",
      );
    }
    return response;
  };

  return {
    state,
    onRespond,
    onNext: () => {
      if (parent !== null) descendantApprovals.selectNext(registry, parent);
    },
    onSelect: (childKey: string) => {
      if (parent !== null) descendantApprovals.select(registry, parent, childKey);
    },
  };
}
