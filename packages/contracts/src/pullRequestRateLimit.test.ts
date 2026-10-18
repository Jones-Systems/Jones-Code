import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { PullRequestOperationError } from "./pullRequest.ts";

const decode = Schema.decodeUnknownSync(PullRequestOperationError);
const encode = Schema.encodeSync(PullRequestOperationError);
const legacy = { _tag: "PullRequestOperationError", operation: "detail", detail: "Read failed" };

describe("pull request pause metadata", () => {
  it("round-trips legacy and typed pause payloads", () => {
    for (const payload of [
      legacy,
      { ...legacy, reason: "not-found" },
      { ...legacy, reason: "rate-limited", retryAt: 123_000 },
    ]) {
      expect(encode(decode(payload))).toEqual(payload);
    }
  });

  it("rejects nonfinite retry times and unknown reasons", () => {
    for (const retryAt of [NaN, Infinity, -Infinity]) {
      expect(() => decode({ ...legacy, reason: "rate-limited", retryAt })).toThrow();
    }
    expect(() => decode({ ...legacy, reason: "transport" })).toThrow();
  });
});
