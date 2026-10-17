import { EnvironmentHttpConflictError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const isHttpConflict = Schema.is(EnvironmentHttpConflictError);

/** A server without control-plane activation answers reads with this stable conflict. */
export const isWorkstreamsNotConfigured = (cause: unknown): boolean =>
  isHttpConflict(cause) && cause.message === "workstreams_not_configured";

/**
 * Only `unauthenticated` is a definite loss of access, so only it hides previously loaded
 * Workstreams. `unsupported` (a missing or unconfigured API) and every other failure,
 * including an opaque control-plane permission denial that shares the outage response, keep
 * the last good data as stale and read-only.
 */
export type WorkstreamReadFailure = "unsupported" | "unauthenticated" | "error";

const UNAUTHENTICATED = new Set([
  "EnvironmentAuthInvalidError",
  "EnvironmentAuthorizationError",
  "EnvironmentHttpUnauthorizedError",
  "EnvironmentHttpForbiddenError",
  "EnvironmentScopeRequiredError",
]);

/**
 * A server without the workstreams API answers 404, and one without control-plane activation
 * answers a not-configured conflict; authentication failures are unauthenticated.
 */
export function classifyWorkstreamReadFailure(cause: unknown): WorkstreamReadFailure {
  if (isWorkstreamsNotConfigured(cause)) return "unsupported";
  const value = cause && typeof cause === "object" ? (cause as Record<string, unknown>) : {};
  const tag = typeof value._tag === "string" ? value._tag : "";
  if (UNAUTHENTICATED.has(tag)) return "unauthenticated";
  const response = value.response as { status?: unknown } | undefined;
  const status = typeof value.status === "number" ? value.status : response?.status;
  if (status === 401 || status === 403) return "unauthenticated";
  if (status === 404 || tag === "EnvironmentResourceNotFoundError") return "unsupported";
  return "error";
}
