import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { ExecutionEnvironmentDescriptor } from "../environment.ts";
import { OrchestrationV2Command } from "../orchestrationV2.ts";
import {
  SETTLEMENT_RESTORE_CAPABILITY,
  SettlementRestoreInspectRequest,
  SettlementRestoreMcpInput,
  settlementRestoreMcpCommandId,
} from "./settlementRestore.ts";

describe("settlement restore contracts", () => {
  it("negotiates an optional v1 capability", () => {
    const descriptor = {
      environmentId: "test",
      label: "Test",
      platform: { os: "linux", arch: "x64" },
      serverVersion: "0.1.0",
      capabilities: { repositoryIdentity: true, threadSettlement: true },
    };
    const decode = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
    expect(decode(descriptor).capabilities.settlementRestore).toBeUndefined();
    expect(
      decode({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          settlementRestore: SETTLEMENT_RESTORE_CAPABILITY,
        },
      }).capabilities.settlementRestore,
    ).toEqual({ schemaVersion: "jones.settlement-restore/v1" });
  });

  it("restores only an exact observed settle", () => {
    const decode = Schema.decodeUnknownSync(OrchestrationV2Command);
    const command = {
      type: "thread.settlement.restore",
      commandId: "restore-1",
      threadId: "thread-1",
      settleCommandId: "settle-1",
    };
    expect(decode(command)).toEqual(command);
    expect(() => decode({ ...command, settleCommandId: undefined })).toThrow();
  });

  it("bounds inspect batches to between 1 and 100 exact IDs", () => {
    const decode = Schema.decodeUnknownSync(SettlementRestoreInspectRequest);
    const item = { threadId: "thread-1" };
    expect(decode({ items: Array.from({ length: 100 }, () => item) }).items).toHaveLength(100);
    expect(() => decode({ items: [] })).toThrow();
    expect(() => decode({ items: Array.from({ length: 101 }, () => item) })).toThrow();
  });

  it("requires the observed settle and a stable request ID for MCP restore", () => {
    const decode = Schema.decodeUnknownSync(SettlementRestoreMcpInput);
    expect(decode({ action: "inspect", threadId: "thread-1" }).action).toBe("inspect");
    expect(() =>
      decode({ action: "restore", threadId: "thread-1", clientRequestId: "r" }),
    ).toThrow();
    expect(() =>
      decode({ action: "restore", threadId: "thread-1", settleCommandId: "settle-1" }),
    ).toThrow();
    expect(
      decode({
        action: "restore",
        threadId: "thread-1",
        settleCommandId: "settle-1",
        clientRequestId: "r",
      }).clientRequestId,
    ).toBe("r");
    expect(settlementRestoreMcpCommandId("r")).toBe("mcp:settlement-restore:r");
  });
});
