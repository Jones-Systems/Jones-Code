import type { PreviewAutomationRuntimeIdentity, PreviewTabId } from "@t3tools/contracts";

export interface RuntimeTabPin {
  readonly runtimeIdentity: PreviewAutomationRuntimeIdentity;
  readonly holdUntil?: number;
}

/** Only the targeted tab's expiry or confirmed logical retirement can release its protection. */
export const selectTabAffinity = (input: {
  readonly runtimeTabs: ReadonlyMap<PreviewTabId, RuntimeTabPin>;
  readonly tabId: PreviewTabId;
  readonly runtimeIdentity: PreviewAutomationRuntimeIdentity | null;
  readonly retired: ReadonlySet<PreviewTabId>;
  readonly now: number;
}): {
  readonly runtimeTabs: ReadonlyMap<PreviewTabId, RuntimeTabPin>;
  readonly decision: "dispatch" | "held" | "capacity";
} => {
  const runtimeTabs = new Map(input.runtimeTabs);
  for (const tabId of input.retired) runtimeTabs.delete(tabId);
  if (input.retired.has(input.tabId)) return { runtimeTabs, decision: "dispatch" };
  let pin = runtimeTabs.get(input.tabId);
  if (pin?.holdUntil !== undefined && pin.holdUntil <= input.now) {
    runtimeTabs.delete(input.tabId);
    pin = undefined;
  }
  if (pin !== undefined) {
    const identity = input.runtimeIdentity;
    const previous = pin.runtimeIdentity;
    if (
      identity !== null &&
      previous.schemaVersion === identity.schemaVersion &&
      previous.runtimeKind === identity.runtimeKind &&
      previous.runtimeInstanceId === identity.runtimeInstanceId &&
      previous.appVersion === identity.appVersion &&
      previous.buildCommit === identity.buildCommit
    ) {
      runtimeTabs.set(input.tabId, { runtimeIdentity: identity });
      return { runtimeTabs, decision: "dispatch" };
    }
    runtimeTabs.set(input.tabId, { ...pin, holdUntil: pin.holdUntil ?? input.now + 30_000 });
    return { runtimeTabs, decision: "held" };
  }
  if (input.runtimeIdentity !== null) {
    if (runtimeTabs.size >= 32) return { runtimeTabs, decision: "capacity" };
    runtimeTabs.set(input.tabId, { runtimeIdentity: input.runtimeIdentity });
  }
  return { runtimeTabs, decision: "dispatch" };
};

interface TabAssignment {
  readonly clientId: string;
  readonly connectionId: string;
  readonly tabId?: PreviewTabId;
  readonly tabSequence?: number;
  readonly runtimeTabs?: ReadonlyMap<PreviewTabId, RuntimeTabPin>;
}

/** The next global sequence excludes every reply already routed, including accepted continuations. */
export const retireClosedTab = (
  assignment: TabAssignment,
  close: { readonly clientId: string; readonly connectionId: string; readonly tabId: PreviewTabId },
  nextRequestSequence: number,
): TabAssignment => {
  if (assignment.clientId !== close.clientId || assignment.connectionId !== close.connectionId)
    return assignment;
  const runtimeTabs = new Map(assignment.runtimeTabs);
  runtimeTabs.delete(close.tabId);
  const { tabId, ...withoutTabId } = assignment;
  return {
    ...withoutTabId,
    ...(tabId === undefined || tabId === close.tabId ? {} : { tabId }),
    runtimeTabs,
    tabSequence: Math.max(assignment.tabSequence ?? -1, nextRequestSequence),
  };
};
