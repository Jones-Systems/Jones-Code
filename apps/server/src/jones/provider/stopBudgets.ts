// Each budget ends before ProviderSessionManager's interrupt timeout, so a stop that can settle
// does so before the provider is reported unresponsive: children first, then the root request,
// one margin between steps. stopBudgets.test.ts holds the ordering against that timeout.
const STOP_MARGIN_MS = 1_000;

/** Longest a restart waits for the interrupt of the turn it replaces. */
export const STOP_RECOVERY_BUDGET_MS = 14_000;

/** How long one native stop request has to be acknowledged, for a child or for the root. */
export const STOP_REQUEST_BUDGET_MS = 3_000;

/** Everything a stop does before it submits the root request. Also the adapters' retirement bound. */
export const STOP_CHILD_BUDGET_MS =
  STOP_RECOVERY_BUDGET_MS - STOP_REQUEST_BUDGET_MS - STOP_MARGIN_MS;

/** A queued root turn cannot be stopped until it starts; wait for that only while the root request still fits. */
export const rootStartWaitMs = (elapsedMs: number): number =>
  Math.min(
    STOP_CHILD_BUDGET_MS,
    Math.max(0, STOP_RECOVERY_BUDGET_MS - STOP_REQUEST_BUDGET_MS - elapsedMs),
  );
