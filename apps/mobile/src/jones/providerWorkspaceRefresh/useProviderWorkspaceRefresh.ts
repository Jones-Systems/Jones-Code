import { useAtomMount, useAtomRefresh } from "@effect/atom-react";
import { useEffect } from "react";
import { Atom } from "effect/reactivity";
import type { EnvironmentId, ServerProvider } from "@t3tools/contracts";
import { mobileProviderWorkspaceSnapshotRefresh } from "./snapshotRefresh";

const idle = Atom.make<void>(undefined);

export function useProviderWorkspaceRefresh(input: {
  readonly environmentId: EnvironmentId | null;
  readonly projectCwd: string | null;
  readonly provider: ServerProvider | null;
  readonly draftMessage: string;
}) {
  const scan =
    input.environmentId && input.projectCwd && input.provider
      ? mobileProviderWorkspaceSnapshotRefresh({
          environmentId: input.environmentId,
          instanceId: input.provider.instanceId,
          cwd: input.projectCwd,
        })
      : idle;
  useAtomMount(scan);
  const refresh = useAtomRefresh(scan);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Draft edits recheck snapshot expiry through the same keyed budget as the picker.
  useEffect(() => {
    refresh();
    // oxlint-disable-next-line react/exhaustive-effect-dependencies -- Every draft edit triggers an expiry check, including clearing the text.
  }, [input.draftMessage, refresh]);
}
