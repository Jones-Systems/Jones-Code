import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT as contract,
  WorkstreamsHomeReadResultRequest,
  WorkstreamsHomeReadResult,
} from "./workstreamsHomeReadResult.ts";
const context = { owner_id: "owner", server_generation: 1, registry_version: 0 };
const envelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    ...context,
    registry_instance_id: "12345678-1234-1234-1234-123456789abc",
    canonical_origin: "https://home.invalid",
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "home",
    served_by_environment_id: "home",
    source: "live",
    observed_at: "2026-10-26T12:00:00Z",
    stale: false,
  },
  coverage: { registry: "complete", sources: [] },
};
const base = { contract, envelope, authorization_revision: 1 };
it("keeps legacy bare pages separate and makes placement opt-in request-local", () => {
  const decode = decodeRequest;
  expect(decode({ contract, request: { route: "workstreams", params: {} } })).toEqual({
    contract,
    request: { route: "workstreams", params: {} },
  });
  const request = {
    contract,
    request: {
      route: "placements",
      params: { identities: [{ source_instance_id: "source", native_thread_id: "thread" }] },
    },
    placement_contract: "workstreams-t3-placement/2.0.0",
  };
  expect(decode(request)).toEqual(request);
  for (const r of [
    {
      contract,
      request: { route: "workstreams", params: {} },
      placement_contract: request.placement_contract,
    },
    { ...request, placement_contract: "workstreams-t3-placement/3.0.0" },
    { ...request, extra: "not-a-closed-request" },
  ])
    expect(() => decode(r)).toThrow();
  expect(() => decodeResult({ context, items: [], next_cursor: null })).toThrow();
});
it("decodes route-specific projection/lifecycle pages and rejects mixed placement versions and export pages", () => {
  const decode = decodeResult;
  for (const response of [
    { ...base, route: "workstreams", value: { context, items: [], next_cursor: null } },
    { ...base, route: "lifecycle-declarations", value: { context, items: [], next_cursor: null } },
    { ...base, route: "read-projection", value: { items: [], next_cursor: null } },
  ])
    expect(decode(response)).toEqual(response);
  for (const response of [
    { ...base, route: "counts", value: { items: [], next_cursor: null } },
    { ...base, route: "read-projection", value: { context, items: [], next_cursor: null } },
    { ...base, route: "export", value: "snapshot-jsonl" },
    {
      ...base,
      route: "placements",
      placement_contract: "workstreams-t3-placement/2.0.0",
      value: { context, inventory_sha256: "a".repeat(64), items: [], next_cursor: null },
    },
    {
      ...base,
      route: "workstreams",
      value: { context, items: [], next_cursor: null },
      extra: true,
    },
    {
      ...base,
      authorization_revision: 0,
      route: "workstreams",
      value: { context, items: [], next_cursor: null },
    },
  ])
    expect(() => decode(response)).toThrow();
});

const decodeRequest = Schema.decodeUnknownSync(WorkstreamsHomeReadResultRequest);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsHomeReadResult);

it("round trips each explicit placement version without coercing native provenance into frozen v1", () => {
  const page = {
    inventory_sha256: "a".repeat(64),
    context: { ...context, principal_id: "principal", authorization_revision: 1 },
    items: [],
    next_cursor: null,
  };
  const legacy = {
    ...base,
    route: "placements",
    placement_contract: "workstreams-t3-placement/1.0.0",
    value: page,
  };
  const native = {
    ...base,
    route: "placements",
    placement_contract: "workstreams-t3-placement/2.0.0",
    value: {
      ...page,
      provenance: {
        authority: "native-home",
        registry_instance_id: envelope.registry.registry_instance_id,
        home_environment_id: "home",
        home_generation: 1,
        served_by_environment_id: "home",
        observed_at: envelope.provenance.observed_at,
      },
    },
  };
  expect(decodeResult(legacy)).toEqual(legacy);
  expect(decodeResult(native)).toEqual(native);
  expect(() => decodeResult({ ...legacy, value: native.value })).toThrow();
  expect(() => decodeResult({ ...native, value: legacy.value })).toThrow();
});
