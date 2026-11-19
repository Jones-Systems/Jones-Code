import { describe, expect, it, vi } from "vite-plus/test";

import { syntheticDiscoveryLog } from "./syntheticDiscoveryLog";

const origin = "http://localhost:23446";
const prefix = "[10:31:08.042] WARN (#12):";
const message = "Could not discover the primary environment.";
const error = {
  _tag: "ConnectionTransientError",
  reason: "remote-unavailable",
  detail: `Remote environment endpoint returned an invalid response from ${origin}/.well-known/t3/environment.`,
  hop: "server",
  via: "direct",
};

describe("syntheticDiscoveryLog", () => {
  it("reports the first synthetic descriptor failure on each test page", () => {
    const write = vi.fn();
    const log = syntheticDiscoveryLog(write, origin);
    log(prefix, message, { error });
    log("[10:31:11.086] WARN (#21):", message, { error: { ...error } });
    syntheticDiscoveryLog(write, origin)(prefix, message, { error });

    expect(write.mock.calls).toEqual([
      [prefix, message, { error }],
      [prefix, message, { error }],
    ]);
  });

  it("forwards other discovery outcomes and unrelated logs even after a retry", () => {
    const write = vi.fn();
    const log = syntheticDiscoveryLog(write, origin);
    log(prefix, message, { error });
    log(prefix, message, { error });
    write.mockClear();

    const calls = [
      [prefix, message, { error: { ...error, detail: error.detail.replace(":23446", ":23447") } }],
      [
        prefix,
        message,
        { error: { ...error, detail: error.detail.replace(origin, "https://example.test") } },
      ],
      [prefix, message, { error: { ...error, _tag: "ConnectionAuthError" } }],
      [prefix, message, { error: { ...error, detail: "The discovery request timed out." } }],
      [prefix, message, { error, attempt: "different diagnostic" }],
      [prefix, "A different warning.", { error }],
      ["[10:31:08.042] ERROR (#12):", message, { error }],
    ];
    for (const args of calls) log(...args);

    expect(write.mock.calls).toEqual(calls);
  });

  it("uses the original logger on non-fixture origins", () => {
    const write = vi.fn();
    expect(syntheticDiscoveryLog(write, "https://example.test")).toBe(write);
    expect(syntheticDiscoveryLog(write, "https://localhost:23446")).toBe(write);
  });
});
