import {
  WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
  WorkstreamsHomeReadExportRequest,
  WorkstreamsHomeReadExportResult,
} from "./workstreamsNativeExport.ts";
import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WorkstreamsNativeReadExportQuery } from "./workstreamsNativeExport.ts";

const decode = Schema.decodeUnknownSync(WorkstreamsNativeReadExportQuery);

it("decodes an absent or bounded export page size without accepting caller or home authority", () => {
  expect(decode({})).toEqual({});
  expect(decode({ page_size: "1" })).toEqual({ page_size: 1 });
  expect(decode({ page_size: "100" })).toEqual({ page_size: 100 });
  for (const page_size of ["0", "101", "-1", "1.5", "NaN", "Infinity", "", "abc", ["1", "2"]])
    expect(() => decode({ page_size })).toThrow();
  for (const input of [
    { caller: { principal_id: "forged" } },
    { home_environment_id: "forged" },
    { page_size: "1", extra: "forged" },
  ])
    expect(() => decode(input)).toThrow();
});

const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner",
  home_environment_id: "home",
  server_generation: 1,
};
const decodePeer = Schema.decodeUnknownSync(WorkstreamsHomeReadExportRequest);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsHomeReadExportResult);
it("negotiates export independently with exact home and page bounds, without caller or routing selectors", () => {
  const request = {
    contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
    expected_home: home,
    page_size: 1,
  };
  expect(decodePeer(request)).toEqual(request);
  for (const change of [
    { contract: "workstreams-native-home-read-result/1.0.0" },
    { page_size: 0 },
    { page_size: 101 },
    { page_size: 1.5 },
    { caller: {} },
    { credential: "forged" },
    { url: "https://another.invalid" },
    { tool: "arbitrary" },
    { expected_home: { ...home, extra: true } },
  ])
    expect(() => decodePeer({ ...request, ...change })).toThrow();
  const result = {
    contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
    home,
    principal_id: "principal",
    authorization_revision: 1,
    jsonl: "original\n",
  };
  expect(decodeResult(result)).toEqual(result);
  expect(decodeResult({ ...result, principal_id: "x".repeat(1024) }).principal_id).toHaveLength(
    1024,
  );
  for (const change of [
    { authorization_revision: 0 },
    { principal_id: "" },
    { jsonl: "" },
    { complete: true },
    { home: { ...home, server_generation: 0 } },
  ])
    expect(() => decodeResult({ ...result, ...change })).toThrow();
});
