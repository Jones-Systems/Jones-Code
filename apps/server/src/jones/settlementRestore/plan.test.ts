import { assert, it } from "@effect/vitest";
import { SettlementRestoreRejectionCode } from "@t3tools/contracts";
import { userFacingDispatchErrorMessage } from "../../orchestration-v2/UserFacingErrors.ts";
import {
  readSettlementRestoreRejection,
  settlementRestoreRejection,
  SettlementRestoreRejectedError,
} from "./plan.ts";

// Clients match the rejection wording anywhere in the text they receive.
// This is the pattern in packages/client-runtime/src/jones/settlementRestore/model.ts.
const clientPattern = /Settlement restore rejected: ([a-z_]+):/;

it("a rejection round-trips its code through the typed cause for every code", () => {
  for (const code of SettlementRestoreRejectionCode.literals) {
    const rejection = settlementRestoreRejection(code);
    assert.instanceOf(rejection, SettlementRestoreRejectedError);
    assert.strictEqual(rejection.code, code);
    assert.strictEqual(readSettlementRestoreRejection(rejection), code);
  }
});

it("only the typed rejection is read back; the retired string protocol and look-alikes are not", () => {
  const code = "none";
  const text = settlementRestoreRejection(code).message;
  assert.isNull(readSettlementRestoreRejection(text));
  assert.isNull(
    readSettlementRestoreRejection({ _tag: "SettlementRestoreRejectedError", code, message: text }),
  );
  assert.isNull(readSettlementRestoreRejection(new Error(text)));
  assert.isNull(readSettlementRestoreRejection(undefined));
  assert.isNull(readSettlementRestoreRejection(null));
});

it("clients keep seeing the same wording and can still parse the code", () => {
  for (const code of SettlementRestoreRejectionCode.literals) {
    const rejection = settlementRestoreRejection(code);
    assert.match(rejection.message, clientPattern);
    assert.strictEqual(clientPattern.exec(rejection.message)?.[1], code);
    // The dispatch error nests the rejection as its cause; the user-facing text is the cause's.
    const dispatchError = Object.assign(new Error("Failed to dispatch orchestration command"), {
      cause: rejection,
    });
    assert.strictEqual(userFacingDispatchErrorMessage(dispatchError), rejection.message);
  }
  assert.strictEqual(
    settlementRestoreRejection("none").message,
    "Settlement restore rejected: none: this thread has no settlement to undo.",
  );
});
