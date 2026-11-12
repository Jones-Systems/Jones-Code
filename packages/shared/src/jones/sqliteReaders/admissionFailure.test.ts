import { describe, expect, it } from "vite-plus/test";
import * as Cause from "effect/Cause";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";
import { findSqliteReaderAdmissionFailure, SqliteReaderAdmissionError } from "./admissionFailure.ts";

const reasons = [
  "queue-full", "queue-timeout", "reader-unavailable", "checkpoint-held", "generation-retired",
] as const;

describe("reader admission authority", () => {
  it.each(reasons)("retains %s through SQL, dispatch and Effect cause wrappers", (reason) => {
    const admission = new SqliteReaderAdmissionError({ reason });
    expect(admission.message).toBe(reason);
    const sql = new SqlError({ reason: classifySqliteError(admission, { operation: "acquire" }) });
    for (const value of [admission, sql, { cause: { reason: sql } }, Cause.fail({ cause: sql }), Cause.die(sql)]) {
      expect(findSqliteReaderAdmissionFailure(value)?.reason).toBe(reason);
    }
  });

  it.each([5, 13, 10])("does not mistake native SQLite code %s for admission", (errcode) => {
    const native = Object.assign(new Error("queue-timeout"), { errcode, errno: errcode });
    expect(findSqliteReaderAdmissionFailure(new SqlError({ reason: classifySqliteError(native) }))).toBeUndefined();
  });

  it("rejects message-only authority, invalid reasons and hostile wrappers", () => {
    for (const value of [
      new Error("checkpoint-held"),
      { message: "queue-timeout", reason: "queue-timeout" },
      { _tag: "SqliteReaderAdmissionError", reason: "private-query-text" },
      { get cause() { throw new Error("private-secret"); } },
    ]) expect(findSqliteReaderAdmissionFailure(value)).toBeUndefined();
  });

  it("bounds cycles, depth and broad Effect causes", () => {
    const cycle: { cause?: unknown; reason?: unknown } = {};
    cycle.cause = cycle;
    expect(findSqliteReaderAdmissionFailure(cycle)).toBeUndefined();
    cycle.reason = new SqliteReaderAdmissionError({ reason: "reader-unavailable" });
    expect(findSqliteReaderAdmissionFailure(cycle)?.reason).toBe("reader-unavailable");
    let deep: unknown = new SqliteReaderAdmissionError({ reason: "queue-full" });
    for (let index = 0; index < 16; index++) deep = { cause: deep };
    expect(findSqliteReaderAdmissionFailure(deep)?.reason).toBe("queue-full");
    expect(findSqliteReaderAdmissionFailure({ cause: deep })).toBeUndefined();
    const broad = Cause.fromReasons(Array.from({ length: 200 }, (_, index) => Cause.fail(
      index === 199 ? new SqliteReaderAdmissionError({ reason: "queue-full" }) : new Error(`unrelated-${index}`),
    ).reasons[0]!));
    expect(findSqliteReaderAdmissionFailure(broad)).toBeUndefined();
  });
});
