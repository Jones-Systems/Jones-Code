import { expect, it } from "vite-plus/test";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import { userFacingDispatchErrorMessage } from "../../orchestration-v2/UserFacingErrors.ts";

it.each([
  "queue-full",
  "queue-timeout",
  "reader-unavailable",
  "checkpoint-held",
  "generation-retired",
] as const)(
  "explains %s through the production dispatch and SQL wrappers without promising replay",
  (reason) => {
    const sql = new SqlError({
      reason: classifySqliteError(new SqliteReaderAdmissionError({ reason })),
    });
    const message = userFacingDispatchErrorMessage({
      message: "Failed to dispatch orchestration command message.dispatch (synthetic-command).",
      cause: toPersistenceSqlError("private-operation")(sql),
    });
    expect(message).toBe(
      "The server was too busy to read its database. Check the server logs if this continues.",
    );
    expect(message).not.toMatch(
      /retry|SQLITE|private-operation|synthetic-command|No provider work/i,
    );
  },
);

it.each([
  [5, "busy (SQLITE_BUSY)"],
  [13, "full (SQLITE_FULL)"],
  [10, "database (SQLITE_IOERR)"],
] as const)("preserves native guidance for code %s", (errcode, guidance) => {
  const native = Object.assign(new Error("private-query"), { errcode, errno: errcode });
  const message = userFacingDispatchErrorMessage(
    new SqlError({ reason: classifySqliteError(native) }),
  );
  expect(message).toContain(guidance);
  expect(message).not.toContain("private-query");
});

it("keeps the private unknown-SQL fallback unchanged", () => {
  const error = new SqlError({ reason: classifySqliteError(new Error("SELECT private-secret")) });
  expect(userFacingDispatchErrorMessage({ cause: error })).toBe(
    "The T3 Code server could not complete this database request. Retry once; if it still fails, check the server logs for the database error.",
  );
});
