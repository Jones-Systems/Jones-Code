import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT as contract,
  WorkstreamsLegacyReceiptCapability,
  WorkstreamsLegacyReceiptLookupRequest,
  WorkstreamsLegacyReceiptLookupResult,
} from "./workstreamsLegacyReceipts.ts";
import {
  WorkstreamsNativeCommandId,
  WorkstreamsNativeHomeReceiptRequest,
  WorkstreamsNativeReceiptLookup,
} from "./workstreamsNative.ts";

const requestCodec = Schema.fromJsonString(WorkstreamsLegacyReceiptLookupRequest);
const decodeRequestText = Schema.decodeUnknownSync(requestCodec);
const encodeRequestText = Schema.encodeSync(requestCodec);
const decodeRequest = Schema.decodeUnknownSync(WorkstreamsLegacyReceiptLookupRequest);
const isRequest = Schema.is(WorkstreamsLegacyReceiptLookupRequest);
const isCommandId = Schema.is(WorkstreamsNativeCommandId);
const isNativeRequest = Schema.is(WorkstreamsNativeHomeReceiptRequest);
const isCapability = Schema.is(WorkstreamsLegacyReceiptCapability);
const resultCodec = Schema.fromJsonString(WorkstreamsLegacyReceiptLookupResult);
const decodeResultText = Schema.decodeUnknownSync(resultCodec);
const encodeResultText = Schema.encodeSync(resultCodec);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsLegacyReceiptLookupResult);
const isNativeResult = Schema.is(WorkstreamsNativeReceiptLookup);

it("keeps the legacy version separate from command admission and the closed v1 lookup", () => {
  const request = { contract, command_id: "command:0", expected_server_generation: 7 } as const;
  expect(decodeRequestText(encodeRequestText(request))).toEqual(request);
  expect(isCommandId("command:0")).toBe(false);
  expect(isNativeRequest({ command_ids: ["command:0"] })).toBe(false);
  expect(isRequest({ ...request, contract: "workstreams-legacy-receipts/2.0.0" })).toBe(false);
  expect(() => decodeRequest({ ...request, principal_id: "spoof" })).toThrow();
  expect(isCapability({ contract, lookup: true })).toBe(true);
  const result = { contract, result: { kind: "unknown", reason: "import_incomplete" } };
  expect(decodeResultText(encodeResultText(decodeResult(result)))).toEqual(result);
  expect(isNativeResult({ kind: "unknown", reason: "legacy_upgrade_required" })).toBe(false);
  expect(isNativeResult({ kind: "legacy_present", proof: {} })).toBe(false);
});
