import { EnvironmentId, ThreadId, ProviderInstanceId, PreviewTabId } from "../index.ts";
import {
  PreviewAutomationConnectionId,
  PreviewAutomationTimeoutError,
} from "../previewAutomation.ts";
import * as Schema from "effect/Schema";
import { expect, it } from "vite-plus/test";
import { PreviewRecordingState } from "./previewRecordingState.ts";

it("decodes the server recorder states and only projects timestamps for active/stopping", () => {
  const decode = Schema.decodeUnknownSync(PreviewRecordingState);
  for (const state of [
    "idle",
    "starting",
    "active",
    "stopping",
    "abandoning",
    "unknown",
    "unsupported",
  ]) {
    const result = decode({ state, startedAt: "now" });
    expect(result).toEqual({
      state,
      ...(state === "active" || state === "stopping" ? { startedAt: "now" } : {}),
    });
  }
});

it("describes recording unknowns and refusals without offering snapshots as reconciliation", () => {
  const base = {
    operation: "recordingStart" as const,
    timeoutMs: 100,
    requestId: "request",
    clientId: "client",
    connectionId: PreviewAutomationConnectionId.make("connection"),
    environmentId: EnvironmentId.make("env"),
    threadId: ThreadId.make("thread"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "session",
    tabId: PreviewTabId.make("tab"),
  };
  const unknown = new PreviewAutomationTimeoutError({ ...base, outcome: "unknown" });
  expect(unknown.message).toContain("snapshot cannot reconcile");
  expect(unknown.message).toContain("exact late reply");
  expect(unknown.message).toContain("reuseExistingTab: false");
  const refused = new PreviewAutomationTimeoutError({
    ...base,
    outcome: "not_started",
    unreconciled: true,
  });
  expect(refused.message).toContain("This action was not started");
  expect(refused.message).toContain("only earlier click, type, press, scroll, or evaluate");
  expect(refused.message).toContain("when reported");
  expect(refused.message).toContain("if available");
  expect(refused.message).toContain("clear no fence");
});
