import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { ExecutionEnvironmentDescriptor } from "../environment.ts";
import {
  SETTLEMENT_ORIGINS_CAPABILITY,
  SettlementOriginsCapability,
  SettlementOriginsRequest,
  SettlementOriginsResult,
} from "./settlementOrigins.ts";

const item = { threadId: "thread:origin", settledAtMs: 1_700_000_000_123 };

describe("settlement origins contracts", () => {
  it("negotiates an optional v1 capability independently of thread settlement", () => {
    const descriptor = {
      environmentId: "test",
      label: "Test",
      platform: { os: "linux", arch: "x64" },
      serverVersion: "0.1.0",
      capabilities: { repositoryIdentity: true, threadSettlement: true },
    };
    const decode = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
    expect(decode(descriptor).capabilities.settlementOrigins).toBeUndefined();
    expect(
      decode({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          settlementOrigins: SETTLEMENT_ORIGINS_CAPABILITY,
        },
      }).capabilities.settlementOrigins,
    ).toEqual({ schemaVersion: "jones.settlement-origins/v1" });
    expect(() =>
      Schema.decodeUnknownSync(SettlementOriginsCapability)({
        schemaVersion: "jones.settlement-origins/v2",
      }),
    ).toThrow();
  });

  it("bounds batches and accepts only nonnegative integer epochs", () => {
    const decode = Schema.decodeUnknownSync(SettlementOriginsRequest);
    expect(decode({ items: Array.from({ length: 100 }, () => item) }).items).toHaveLength(100);
    expect(decode({ items: [] }).items).toEqual([]);
    expect(() => decode({ items: Array.from({ length: 101 }, () => item) })).toThrow();
    for (const settledAtMs of [-1, 0.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => decode({ items: [{ ...item, settledAtMs }] })).toThrow();
    }
    expect(() => decode({ items: [{ ...item, threadId: "" }] })).toThrow();
  });

  it("preserves positional query identity and the conservative unknown result", () => {
    const items = ["client", "other", "unknown"].map((origin) => ({ ...item, origin }));
    expect(Schema.decodeUnknownSync(SettlementOriginsResult)({ items })).toEqual({ items });
    expect(() =>
      Schema.decodeUnknownSync(SettlementOriginsResult)({ items: [{ ...item, origin: "manual" }] }),
    ).toThrow();
  });
});
