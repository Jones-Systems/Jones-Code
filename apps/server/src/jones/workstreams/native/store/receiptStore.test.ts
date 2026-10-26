import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as ReceiptStore from "./receiptStore.ts";
import { scope, seed, testLayer, pending, terminal } from "./sql.testkit.ts";

it.effect(
  "exact request bytes and terminal receipt survive replay and conflicting bytes never replace them",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const receipts = yield* ReceiptStore.ReceiptStore;
      const request = pending("command:test-000000", '{ "sentinel": "receipt-request" }');
      yield* receipts.save(scope, request, null);
      const final = terminal(request);
      yield* receipts.save(scope, final, 0);
      expect(yield* receipts.save(scope, request, null)).toEqual(final);
      expect(yield* receipts.lookup(scope, request.receipt.command_id, 1)).toEqual({
        kind: "present",
        source: "native",
        receipt: final.receipt,
      });
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            receipts.save(scope, pending("command:test-000000", '{"different":true}'), null),
          ),
        ),
      ).toBe(true);
      expect(yield* receipts.lookup(scope, "missing", 1)).toEqual({
        kind: "proven_absent",
        witness: { server_generation: 1, registry_version: 0, archive: "complete" },
      });
      expect(yield* receipts.lookup(scope, "missing", 2)).toEqual({
        kind: "unknown",
        reason: "generation_mismatch",
      });
    }).pipe(Effect.provide(testLayer())),
);
