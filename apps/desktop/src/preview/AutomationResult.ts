export function unwrapPreviewAutomationResult<A>(value: A, deadlineMs?: number): A {
  if (
    deadlineMs === undefined ||
    typeof value !== "object" ||
    value === null ||
    !("type" in value) ||
    value.type !== "previewAutomationResult" ||
    !("ok" in value)
  )
    return value;
  if (value.ok === true && "result" in value) return value.result as A;
  if (
    value.ok === false &&
    "error" in value &&
    typeof value.error === "object" &&
    value.error !== null &&
    "outcome" in value.error &&
    value.error.outcome === "not_started" &&
    "_tag" in value.error &&
    value.error._tag === "PreviewAutomationNotStartedError"
  ) {
    throw value.error;
  }
  return value;
}
