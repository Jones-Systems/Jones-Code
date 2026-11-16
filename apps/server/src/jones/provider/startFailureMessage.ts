/**
 * Chooses the visible text for a provider start failure from the start error
 * and its immediate cause.
 *
 * Effect tagged errors declared without a `message` field carry an empty
 * message. Treating that empty text as present would replace the start
 * error's own app-authored text (for example the provider instance and
 * session that failed to open) with the generic provider default. A blank
 * cause therefore falls back to the start error, exactly like a cause that is
 * neither an `Error` nor a string. No additional cause text is exposed.
 */
export function startFailureCauseMessage(error: Error, nestedCause: unknown): string {
  if (nestedCause instanceof Error && nestedCause.message.trim() !== "") {
    return nestedCause.message;
  }
  if (typeof nestedCause === "string" && nestedCause.trim() !== "") return nestedCause;
  return error.message;
}
