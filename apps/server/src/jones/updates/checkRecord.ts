// @effect-diagnostics globalDate:off
// The updater's check runs on the Promise boundary.
import type { JonesUpdateLastCheck } from "@t3tools/contracts/jones/jonesUpdates";

const MAX_REASON = 240;

/**
 * A check failure safe to log and publish: credentials, authorization headers and URL query
 * strings are removed, whitespace is collapsed, and the text is bounded.
 */
export function sanitizeCheckFailure(error: unknown): string {
  const raw = error instanceof Error ? error.message : "The update check failed.";
  const cleaned = raw
    .replace(/\b(gh[pousr]_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,})/g, "[redacted]")
    .replace(/\b(bearer|token|basic)\s+[A-Za-z0-9._~+/=-]{8,}/gi, "$1 [redacted]")
    .replace(/(https?:\/\/)[^/\s@]+@/gi, "$1")
    .replace(/(https?:\/\/[^\s?#]+)\?[^\s#]*/gi, "$1")
    .replace(/\s+/g, " ")
    .trim();
  return (cleaned.length === 0 ? "The update check failed." : cleaned).slice(0, MAX_REASON);
}

export function lastCheckRecord(
  outcome: JonesUpdateLastCheck["outcome"],
  at: string,
  reason?: string,
): JonesUpdateLastCheck {
  return { at, outcome, ...(reason === undefined ? {} : { reason: reason.slice(0, MAX_REASON) }) };
}
