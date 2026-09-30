import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ResourceTelemetryProcessOwner } from "./resourceTelemetry.ts";

const decodeOwner = Schema.decodeUnknownSync(ResourceTelemetryProcessOwner);

describe("ResourceTelemetryProcessOwner", () => {
  it("accepts a provider and an opaque thread identifier", () => {
    expect(decodeOwner({ kind: "provider", provider: "codex", threadId: "thread-1" })).toEqual({
      kind: "provider",
      provider: "codex",
      threadId: "thread-1",
    });
  });

  it("rejects empty ownership fields and unknown owner kinds", () => {
    expect(() => decodeOwner({ kind: "provider", provider: " ", threadId: "thread-1" })).toThrow();
    expect(() => decodeOwner({ kind: "provider", provider: "codex", threadId: " " })).toThrow();
    expect(() =>
      decodeOwner({ kind: "terminal", provider: "codex", threadId: "thread-1" }),
    ).toThrow();
  });
});
