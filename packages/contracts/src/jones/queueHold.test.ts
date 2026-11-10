import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import { OrchestrationV2Run, OrchestrationV2RunJson } from "../orchestrationV2.ts";

const decodeStored = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2RunJson));
const encodeStored = Schema.encodeSync(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeWire = Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2Run));

const legacyHeldRun = {
  id: "run:legacy",
  threadId: "thread:legacy",
  ordinal: 2,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-5.1-codex" },
  providerThreadId: null,
  userMessageId: "message:legacy",
  rootNodeId: null,
  activeAttemptId: null,
  status: "queued",
  queuePosition: 1,
  queueHeld: true,
  requestedAt: "2026-10-20T00:00:00.000Z",
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};

describe("queue hold stamps", () => {
  it("decodes a stored run held before reasons were recorded", () => {
    const run = decodeStored(JSON.stringify(legacyHeldRun));
    expect(run.queueHeld).toBe(true);
    expect(run.queueHeldAt).toBeUndefined();
    expect(run.queueHeldReason).toBeUndefined();
    expect(decodeWire(legacyHeldRun).queueHeldReason).toBeUndefined();
  });

  it("round-trips a recorded hold time and reason", () => {
    const stamped = {
      ...legacyHeldRun,
      queueHeldAt: "2026-11-09T12:00:00.000Z",
      queueHeldReason: "restart",
    };
    const run = decodeStored(JSON.stringify(stamped));
    expect(run.queueHeldReason).toBe("restart");
    expect(DateTime.formatIso(run.queueHeldAt!)).toBe("2026-11-09T12:00:00.000Z");
    expect(JSON.parse(encodeStored(run))).toMatchObject({
      queueHeldAt: "2026-11-09T12:00:00.000Z",
      queueHeldReason: "restart",
    });
  });
});
