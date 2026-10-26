import { WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT } from "@t3tools/contracts/jones/workstreamsLegacyReceipts";
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
it.effect("the additive lookup preserves native absence, generation and principal rules", () =>
  Effect.gen(function* () {
    yield* seed;
    const receipts = yield* ReceiptStore.ReceiptStore;
    const request = {
      contract: WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT,
      command_id: "missing",
      expected_server_generation: 1,
    } as const;
    expect((yield* receipts.lookupLegacy(scope, "principal:test", request)).result.kind).toBe(
      "proven_absent",
    );
    expect(
      (yield* receipts.lookupLegacy(scope, "principal:test", {
        ...request,
        expected_server_generation: 2,
      })).result,
    ).toEqual({ kind: "unknown", reason: "generation_mismatch" });
    const record = pending("command:strict-0000");
    yield* receipts.save(scope, record, null);
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          receipts.lookupLegacy(scope, "other.principal", {
            ...request,
            command_id: record.receipt.command_id,
          }),
        ),
      ),
    ).toBe(true);
    expect(
      Exit.isFailure(
        yield* Effect.exit(
          receipts.lookupLegacy(scope, "principal:test", {
            ...request,
            command_id: record.receipt.command_id,
            request_sha256: "b".repeat(64),
          }),
        ),
      ),
    ).toBe(true);
    const short = { ...record, receipt: { ...record.receipt, command_id: "command:0" } };
    expect(Exit.isFailure(yield* Effect.exit(receipts.save(scope, short, null)))).toBe(true);
  }).pipe(Effect.provide(testLayer())),
);
