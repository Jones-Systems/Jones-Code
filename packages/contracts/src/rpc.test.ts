import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { ORCHESTRATION_V2_WS_METHODS } from "./orchestrationV2.ts";
import { WS_METHODS, WsRpcGroup } from "./rpc.ts";

describe("WebSocket RPC contracts", () => {
  it("rejects server-internal commands sent to dispatchCommand", () => {
    const dispatchCommand = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
    if (dispatchCommand === undefined) throw new Error("dispatchCommand is not registered");
    const decode = Schema.decodeUnknownExit(dispatchCommand.payloadSchema);

    expect(
      Exit.isFailure(
        decode({
          type: "checkpoint.rollback.fail",
          commandId: "forged-rollback-failure",
          threadId: "thread-1",
          requestId: "rollback-1",
          message: "Forged failure.",
        }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        decode({
          type: "checkpoint.rollback",
          commandId: "rollback-1",
          threadId: "thread-1",
          scopeId: "scope-1",
          checkpointId: "checkpoint-1",
        }),
      ),
    ).toBe(true);
  });

  it("lets the server pick a new worktree's folder", () => {
    const createWorktree = WsRpcGroup.requests.get(WS_METHODS.vcsCreateWorktree);
    if (createWorktree === undefined) throw new Error("vcs.createWorktree is not registered");
    const decode = Schema.decodeUnknownExit(createWorktree.payloadSchema);
    const request = { cwd: "/repo", refName: "main", newRefName: "feature/x" };

    expect(Exit.isSuccess(decode({ ...request, path: null }))).toBe(true);
    expect(Exit.isFailure(decode({ ...request, path: "/repo/.git/hooks" }))).toBe(true);
  });
});

describe("legacy queue compatibility on the existing dispatch RPC", () => {
  const rpc = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand)!;
  const payload = {
    type: "thread.turn.start",
    commandId: "legacy:C",
    threadId: "legacy:T",
    createdAt: "2026-10-05T00:00:00.000Z",
    message: { messageId: "legacy:M", role: "user", text: "Prompt", attachments: [] },
    runtimeMode: "full-access",
    interactionMode: "default",
    bootstrap: { runSetupScript: false },
    dispatchGuard: {
      observedSnapshotSequence: 3,
      expectedModelSelection: { instanceId: "codex", model: "gpt-6" },
      expectedSessionStatus: null,
      expectedActiveTurnId: null,
      expectedLatestTurnId: null,
      requireIdle: true,
    },
  };
  it("preserves the complete closed legacy command and final guard without changing method identity", () => {
    expect(Schema.decodeUnknownSync(rpc.payloadSchema)(payload)).toEqual(payload);
  });
  it("rejects undeclared bootstrap fields while preserving native V2 excess-field behavior", () => {
    expect(
      Exit.isFailure(
        Schema.decodeUnknownExit(rpc.payloadSchema)({
          ...payload,
          bootstrap: { ...payload.bootstrap, transport: "legacy_websocket" },
        }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        Schema.decodeUnknownExit(rpc.payloadSchema)({
          type: "checkpoint.rollback",
          commandId: "native:rollback",
          threadId: "native:T",
          scopeId: "scope",
          checkpointId: "checkpoint",
          futureNativeField: true,
        }),
      ),
    ).toBe(true);
  });
  it.each(["transport", "unsupported", "legacyBootstrap"])(
    "rejects a caller-controlled %s field instead of discarding it",
    (key) => {
      expect(
        Exit.isFailure(
          Schema.decodeUnknownExit(rpc.payloadSchema)({ ...payload, [key]: "forged" }),
        ),
      ).toBe(true);
    },
  );
});
