import type { DesignRequestRegistryStatus } from "@t3tools/client-runtime/jones/design-requests";
import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { personalWorkstreamsController } from "../../state/workstreamsPersonal";
export interface DesignRequestRegistrySnapshot {
  readonly status: DesignRequestRegistryStatus;
  readonly data: {
    readonly items: readonly { readonly workstreamId: string; readonly name: string }[];
    readonly confirmed: boolean;
  } | null;
  readonly placements: {
    readonly items: readonly {
      readonly workstreamId: string | null;
      readonly threadId: string;
      readonly environmentId: string;
      readonly revision: number;
    }[];
  } | null;
}
export async function loadDesignRequestRegistry(
  threads: readonly { readonly environmentId: string; readonly id: string }[],
  signal?: AbortSignal,
  _workstreamId?: string | null,
): Promise<DesignRequestRegistrySnapshot> {
  signal?.throwIfAborted();
  const state = appAtomRegistry.get(personalWorkstreamsController.state);
  const confirmed = new Set(
    state.environments
      .filter(
        (entry) => entry.confirmed && (entry.status === "ready" || entry.status === "read_only"),
      )
      .map((entry) => String(entry.environmentId)),
  );
  return {
    status: confirmed.size ? "ready" : "error",
    data: {
      items: state.workstreams.map((item) => ({ workstreamId: item.id, name: item.name })),
      confirmed: confirmed.size > 0,
    },
    placements: {
      items: threads.flatMap((thread) => {
        const assignment = state.assignmentsByThreadKey.get(
          personalThreadKey(thread as Parameters<typeof personalThreadKey>[0]),
        );
        return confirmed.has(thread.environmentId) &&
          assignment &&
          !state.pendingThreadKeys.has(
            personalThreadKey(thread as Parameters<typeof personalThreadKey>[0]),
          )
          ? [{ ...assignment, environmentId: thread.environmentId }]
          : [];
      }),
    },
  };
}
