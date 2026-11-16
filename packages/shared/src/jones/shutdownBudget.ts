/** Per-session close is bounded independently; server shutdown releases sessions concurrently. */
export const PROVIDER_SCOPE_CLOSE_TIMEOUT_MS = 30_000;

// Allow the captured-session and parent-scope close windows, plus reconciliation.
// Expiry permits escalation, never a claim that database writers are quiescent.
export const SERVER_CHILD_SHUTDOWN_GRACE_MS = 2 * PROVIDER_SCOPE_CLOSE_TIMEOUT_MS + 15_000;
export const SERVICE_MANAGER_STOP_TIMEOUT_SECONDS = 90;
