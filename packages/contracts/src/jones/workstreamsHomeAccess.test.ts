import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WORKSTREAM_CONTRACT_MANIFEST_SHA256 } from "../workstreams.ts";
import {
  WORKSTREAMS_HOME_ACCESS_CONTRACT as contract,
  WorkstreamsHomeAccessRequest,
  WorkstreamsHomeAccessResult,
} from "./workstreamsHomeAccess.ts";
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner",
  home_environment_id: "home",
  server_generation: 2,
};
const result = {
  contract,
  home: { ...home, canonical_origin: "https://home.invalid", registry_version: 7 },
  principal_id: "principal",
  authorization_revision: 3,
  observed_at: "2026-08-02T12:00:00.000Z",
  capabilities: {
    contract_family: "workstreams",
    contract_version: "1.0.0",
    manifest_sha256: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
    context: { owner_id: home.owner_id, server_generation: 2, registry_version: 7 },
    permissions: ["workstreams:read", "workstreams:write"],
    max_request_bytes: 32768,
    max_response_bytes: 1048576,
    max_json_depth: 10,
    max_page_items: 100,
    max_pr_response_bytes: 262144,
    max_pr_request_seconds: 15,
    cursor_ttl_seconds: 900,
  },
};
const decodeRequest = Schema.decodeUnknownSync(WorkstreamsHomeAccessRequest);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsHomeAccessResult);
it("negotiates a separate closed access observation without accepting old pages or new authority fields", () => {
  expect(decodeRequest({ contract, expected_home: home })).toEqual({
    contract,
    expected_home: home,
  });
  expect(decodeResult(result)).toEqual(result);
  for (const bad of [
    { contract, expected_home: { ...home, redirect: "https://other.invalid" } },
    { contract, expected_home: home, permissions: ["workstreams:write"] },
    { contract: "workstreams-native-home-read-result/1.0.0", expected_home: home },
  ])
    expect(() => decodeRequest(bad)).toThrow();
  for (const bad of [
    { ...result, caller: {} },
    { ...result, authorization_revision: 0 },
    { ...result, home: { ...result.home, redirect: "https://other.invalid" } },
    { ...result, capabilities: { ...result.capabilities, forged_grant: true } },
    {
      ...result,
      capabilities: {
        ...result.capabilities,
        context: { ...result.capabilities.context, extra: true },
      },
    },
  ])
    expect(() => decodeResult(bad)).toThrow();
});
it("requires matching current home context, supported manifest and read permission", () => {
  for (const capabilities of [
    { ...result.capabilities, context: { ...result.capabilities.context, owner_id: "other" } },
    { ...result.capabilities, context: { ...result.capabilities.context, server_generation: 3 } },
    { ...result.capabilities, context: { ...result.capabilities.context, registry_version: 8 } },
    { ...result.capabilities, manifest_sha256: "a".repeat(64) },
    { ...result.capabilities, permissions: ["workstreams:write"] },
    { ...result.capabilities, permissions: ["workstreams:read", "workstreams:read"] },
  ])
    expect(() => decodeResult({ ...result, capabilities })).toThrow();
  expect(
    decodeResult({
      ...result,
      capabilities: { ...result.capabilities, permissions: ["workstreams:read"] },
    }).capabilities.permissions,
  ).toEqual(["workstreams:read"]);
});
