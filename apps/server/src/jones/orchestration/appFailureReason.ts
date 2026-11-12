import { findSqliteReaderAdmissionFailure } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";

export function appFailureCodeFromCause(cause: unknown): string | undefined {
  const failure = findSqliteReaderAdmissionFailure(cause);
  return failure === undefined ? undefined : `reader_admission_${failure.reason}`;
}
