import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WORKSTREAM_CONTRACT_MANIFEST_SHA256 } from "../workstreams.ts";
import { WORKSTREAMS_HOME_ACCESS_CONTRACT as contract } from "./workstreamsHomeAccess.ts";
import * as Registration from "./workstreamsHomeRegistration.ts";
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

const decodeRequest = Schema.decodeUnknownSync(Registration.WorkstreamsHomeRegistrationRequest);
const decodeResult = Schema.decodeUnknownSync(Registration.WorkstreamsHomeRegistrationResult);
const request = {
  contract: Registration.WORKSTREAMS_HOME_REGISTRATION_CONTRACT,
  expected_home: home,
};
const response = { contract: request.contract, access: result, grant_id: "actual-grant" };
it("separately negotiates the closed current-grant observation and preserves access-v1", () => {
  expect(decodeRequest(request)).toEqual(request);
  expect(decodeResult(response)).toEqual(response);
  for (const value of [
    result,
    { ...response, contract },
    { ...response, grant_id: "" },
    { ...response, home: home },
    { ...response, caller: {} },
    { ...response, access: { ...result, grant_id: "forged" } },
    { ...response, access: { ...result, authorization_revision: 0 } },
  ])
    expect(() => decodeResult(value)).toThrow();
  for (const value of [
    { ...request, caller: {} },
    { ...request, expected_home: { ...home, origin: "other" } },
    { ...request, contract },
    { ...request, grant_id: "selected" },
  ])
    expect(() => decodeRequest(value)).toThrow();
});
it("inherits current capability/home coherence validation", () => {
  expect(() =>
    decodeResult({
      ...response,
      access: {
        ...result,
        capabilities: {
          ...result.capabilities,
          context: { ...result.capabilities.context, registry_version: 99 },
        },
      },
    }),
  ).toThrow();
});
