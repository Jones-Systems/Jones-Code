import { createDescendantApprovalAtoms } from "@t3tools/client-runtime/jones/descendant-approval";
import { environmentPresentations } from "../../state/presentation";
import { environmentSession } from "../../state/session";
import { environmentShell } from "../../state/shell";
import { environmentThreadDetails, environmentThreadShells } from "../../state/threads";

export const descendantApproval = createDescendantApprovalAtoms({
  childThreadInputsAtom: environmentThreadShells.childThreadInputsAtom,
  presentationAtom: environmentPresentations.presentationAtom,
  shellStateAtom: environmentShell.stateValueAtom,
  sessionStateAtom: environmentSession.sessionStateValueAtom,
  threadStatusAtom: environmentThreadDetails.statusAtom,
  threadErrorAtom: environmentThreadDetails.errorAtom,
  pendingRequestsAtom: environmentThreadDetails.pendingRequestsAtom,
});
