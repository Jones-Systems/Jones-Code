import type { ServerNotification__CodexErrorInfo } from "effect-codex-app-server/schema";

export type CodexFailoverFailure = "usage_limit" | "unavailable";

// This classifies native evidence only. A caller still needs an attributed failed
// terminal after native/capacity retries finish before admitting a continuation.
export function classifyCodexFailoverFailure(
  info: ServerNotification__CodexErrorInfo | null | undefined,
): CodexFailoverFailure | null {
  if (info === "usageLimitExceeded" || info === "rateLimitExceeded") return "usage_limit";
  if (info === "serverOverloaded" || info === "internalServerError" || info === "flexUnavailable")
    return "unavailable";
  if (info === null || typeof info !== "object") return null;

  const connection =
    "httpConnectionFailed" in info
      ? info.httpConnectionFailed
      : "responseStreamConnectionFailed" in info
        ? info.responseStreamConnectionFailed
        : "responseStreamDisconnected" in info
          ? info.responseStreamDisconnected
          : null;
  const exhaustion =
    "responseTooManyFailedAttempts" in info ? info.responseTooManyFailedAttempts : null;
  const status = (connection ?? exhaustion)?.httpStatusCode;
  if (status === 429) return "usage_limit";
  if (status === 408 || status === 500 || status === 502 || status === 503 || status === 504)
    return "unavailable";
  // A connection failure without an HTTP response is positive transport evidence.
  // An exhausted-attempt counter alone does not identify the underlying failure.
  return connection !== null && status == null ? "unavailable" : null;
}
