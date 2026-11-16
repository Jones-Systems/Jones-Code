/**
 * Tagged errors without a `message` field have an empty message; a blank cause
 * must fall back to the start error's own text instead of the generic default.
 */
export function startFailureCauseMessage(error: Error, nestedCause: unknown): string {
  if (nestedCause instanceof Error && nestedCause.message.trim() !== "") {
    return nestedCause.message;
  }
  if (typeof nestedCause === "string" && nestedCause.trim() !== "") return nestedCause;
  return error.message;
}
