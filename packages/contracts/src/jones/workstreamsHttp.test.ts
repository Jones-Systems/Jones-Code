import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  WORKSTREAMS_HTTP_RESPONSE_CONTRACT_HEADER as header,
  WORKSTREAMS_HTTP_RESPONSE_CONTRACT as contract,
  WorkstreamsHttpResponseContractHeaders,
  workstreamsHttpResponseHeaders,
} from "./workstreamsHttp.ts";

describe("Workstreams response opt-in", () => {
  const decode = Schema.decodeUnknownSync(WorkstreamsHttpResponseContractHeaders);
  it("preserves absent legacy negotiation and the explicit modern opt-in", () => {
    expect(decode({})).toEqual({});
    expect(decode(workstreamsHttpResponseHeaders)).toEqual({ [header]: contract });
  });
  it.each([
    "",
    "workstreams-http-response/2.0.0",
    contract + ", " + contract,
    "x".repeat(257),
    null,
    1,
  ])("rejects an unsupported or malformed contract %s", (value) => {
    expect(() => decode({ [header]: value })).toThrow();
  });
});
