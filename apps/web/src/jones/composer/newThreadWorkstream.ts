import {
  isClosedPersonalWorkstream,
  isPersonalWorkstreamSnoozed,
} from "@t3tools/contracts/jones/workstreamsPersonal";
import type {
  PersonalThreadRef,
  PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { personalWorkstreamsController } from "../../state/workstreamsPersonal";
import { toastManager } from "../../components/ui/toast";
import { stackedThreadToast } from "../../components/ui/toastHelpers";
import { personalFailureMessage } from "../workstreams/personalNotifications";

export function availableComposerWorkstreams(
  workstreams: readonly PersonalWorkstreamView[],
  now: number,
) {
  return workstreams.filter(
    (workstream) =>
      !isClosedPersonalWorkstream(workstream.lifecycle) &&
      !isPersonalWorkstreamSnoozed(workstream, now),
  );
}

export async function assignNewThreadWorkstream(
  thread: PersonalThreadRef,
  workstreamId: string,
  openThread: () => void,
) {
  const showFailure = (description: string) => {
    toastManager.add(
      stackedThreadToast({
        type: "error",
        title: "Thread started, but Workstream assignment failed",
        description: `${description} Use Move to Workstream on the thread to recover.`,
        timeout: 0,
        actionProps: { children: "Open thread", onClick: openThread },
      }),
    );
  };

  try {
    const { workstreams } = appAtomRegistry.get(personalWorkstreamsController.state);
    const available = availableComposerWorkstreams(workstreams, Date.now());
    if (!available.some(({ id }) => id === workstreamId)) {
      showFailure("The selected Workstream is no longer available for new threads.");
      return;
    }
    const result = await personalWorkstreamsController.assign(thread, workstreamId);
    if (!result.ok) showFailure(personalFailureMessage(result.failure, "This environment"));
  } catch {
    showFailure(
      "Couldn't confirm the Workstream assignment. Check the thread's current Workstream.",
    );
  }
}
