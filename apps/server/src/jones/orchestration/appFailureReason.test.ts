import { expect, it } from "vite-plus/test";
import * as Cause from "effect/Cause";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { appFailureCodeFromCause } from "./appFailureReason.ts";

it.each(["queue-full", "queue-timeout", "reader-unavailable", "checkpoint-held", "generation-retired"] as const)(
  "persists only the allowlisted reason %s without changing the generic local failure message", (reason) => {
    const cause = Cause.fail({ cause: new SqliteReaderAdmissionError({ reason }) });
    const failure = makeProviderFailure({ class: "unknown", code: appFailureCodeFromCause(cause) });
    expect(failure.code).toBe(`reader_admission_${reason}`);
    expect(failure.message).toBe("Provider turn failed.");
  },
);

it("cannot persist arbitrary code, SQL or private error text", () => {
  for (const cause of [
    Object.assign(new Error("private query"), { code: "private-token" }),
    { _tag: "SqliteReaderAdmissionError", reason: "private-token" },
    new Error("queue-timeout"),
  ]) expect(appFailureCodeFromCause(cause)).toBeUndefined();
  let reads = 0;
  expect(appFailureCodeFromCause({
    _tag: "SqliteReaderAdmissionError",
    get reason() { return ++reads === 1 ? "queue-timeout" : "private-token"; },
  })).toBe("reader_admission_queue-timeout");
  expect(reads).toBe(1);
});
