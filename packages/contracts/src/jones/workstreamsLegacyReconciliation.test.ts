import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { WorkstreamCommand } from "../workstreams.ts";
import {
  LegacyCommand,
  LegacyReconciliationRequest,
  WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT as contract,
} from "./workstreamsLegacyReconciliation.ts";
const isLegacyCommand = Schema.is(LegacyCommand);
const isFreshCommand = Schema.is(WorkstreamCommand);
const decodeLegacy = Schema.decodeUnknownSync(LegacyCommand);
const isRequest = Schema.is(LegacyReconciliationRequest);
const decodeRequest = Schema.decodeUnknownSync(LegacyReconciliationRequest);
it("keeps historical evidence addresses separate from strict fresh command IDs", () => {
  const command = {
    command_id: "cp:1",
    expected_server_generation: 7,
    expected_registry_version: 9,
    action: {
      operation: "verify_reference",
      native_reference_id: "reference:1",
      expected_attestation_version: 0,
    },
  };
  expect(isLegacyCommand(command)).toBe(true);
  expect(isFreshCommand(command)).toBe(false);
  expect(decodeLegacy(command).command_id).toBe("cp:1");
});
it("closes the versioned request against caller-supplied authority, outcome or a verified boolean", () => {
  const request = {
    contract,
    command_id: "cp:1",
    request_sha256: "a".repeat(64),
    reservation_sha256: "b".repeat(64),
    expected_server_generation: 7,
    home_environment_id: "home:1",
  };
  expect(isRequest(request)).toBe(true);
  for (const extra of [
    { verified: true },
    { principal_id: "someone" },
    { result: { native_outcome: "committed" } },
    { contract: "workstreams-native/1.0.0" },
  ])
    expect(() => decodeRequest({ ...request, ...extra })).toThrow();
});
