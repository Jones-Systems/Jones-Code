import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";

const AdmissionReason = Schema.Literals([
  "queue-full",
  "queue-timeout",
  "reader-unavailable",
  "checkpoint-held",
  "generation-retired",
]);

export class SqliteReaderAdmissionError extends Schema.TaggedError<SqliteReaderAdmissionError>()(
  "SqliteReaderAdmissionError",
  { reason: AdmissionReason },
) {
  override get message(): string {
    return this.reason;
  }
}

const isAdmissionReason = Schema.is(AdmissionReason);

/** Only the finite tagged reason is authority; SQL and arbitrary error text are not. */
export function findSqliteReaderAdmissionFailure(
  value: unknown,
): Pick<SqliteReaderAdmissionError, "_tag" | "reason"> | undefined {
  const pending = [{ value, depth: 0 }];
  const seen = new Set<unknown>();
  for (let index = 0; index < pending.length && index < 128; index++) {
    const entry = pending[index]!;
    if (
      entry.depth > 16 ||
      typeof entry.value !== "object" ||
      entry.value === null ||
      seen.has(entry.value)
    )
      continue;
    seen.add(entry.value);
    const next = (nested: unknown) => {
      if (pending.length < 128) pending.push({ value: nested, depth: entry.depth + 1 });
    };
    try {
      const current = entry.value as Record<string, unknown>;
      const reason = current.reason;
      if (current._tag === "SqliteReaderAdmissionError" && isAdmissionReason(reason))
        return { _tag: "SqliteReaderAdmissionError", reason };
      if (Cause.isCause(current)) {
        for (const reason of current.reasons) {
          if (pending.length >= 128) break;
          next(reason);
        }
      } else if (Cause.isReason(current)) {
        if (Cause.isFailReason(current)) next(current.error);
        else if (Cause.isDieReason(current)) next(current.defect);
      } else {
        next(current.cause);
        next(current.reason);
      }
    } catch {
      // An opaque wrapper cannot supply a trusted admission reason.
    }
  }
  return undefined;
}
