import type {
  DesktopCompanionState,
  EnvironmentId,
  PreviewSessionSnapshot,
  ScopedThreadRef,
  PreviewCompanionThreadSelectionResponse,
} from "@t3tools/contracts";
import type { CompanionQuery } from "./state.ts";

function isAssignedHere(
  state: DesktopCompanionState | null,
  environmentId: EnvironmentId,
  threadId: string,
  tabId: string,
): boolean {
  return (
    state?.config.enabled === true &&
    state.config.environmentId === environmentId &&
    state.connectionGeneration !== null &&
    state.status === "online" &&
    state.assignments.some((key) => key.threadId === threadId && key.tabId === tabId)
  );
}

export function nativeCompanionRendering(input: {
  readonly environmentId: EnvironmentId;
  readonly primaryEnvironmentId: EnvironmentId | null;
  readonly snapshot:
    | Pick<PreviewSessionSnapshot, "runtime" | "threadId" | "tabId">
    | null
    | undefined;
  readonly binding: CompanionQuery<PreviewCompanionThreadSelectionResponse> | null;
  readonly companion: DesktopCompanionState | null;
}): boolean {
  const { snapshot, binding, companion, environmentId, primaryEnvironmentId } = input;
  if (snapshot?.runtime !== "server") return false;
  if (binding?.status === "unsupported") return environmentId === primaryEnvironmentId;
  if (binding?.status !== "ready") return false;
  const tab = binding.value.tabs.find((tab) => tab.tabId === snapshot.tabId);
  if (!tab) return false;
  if (tab.hostId === null) return environmentId === primaryEnvironmentId;
  return (
    tab.hostId === companion?.config.hostId &&
    isAssignedHere(companion, environmentId, snapshot.threadId, snapshot.tabId)
  );
}

export interface CompanionMountedSession {
  readonly threadRef: ScopedThreadRef;
  readonly snapshot: PreviewSessionSnapshot;
  readonly runtimeTabId: string;
  readonly pictureInPicture: boolean;
  readonly zoomFactor: number;
  readonly companion?: boolean;
}

export function mergeCompanionSessions(
  ordinary: ReadonlyArray<CompanionMountedSession>,
  assigned: ReadonlyArray<CompanionMountedSession>,
): ReadonlyArray<CompanionMountedSession> {
  const byId = new Map(ordinary.map((session) => [session.runtimeTabId, session]));
  for (const session of assigned)
    byId.set(session.runtimeTabId, {
      ...byId.get(session.runtimeTabId),
      ...session,
      companion: true,
    });
  return [...byId.values()];
}
