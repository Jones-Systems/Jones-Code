import { OrchestrationV2DomainEventJson } from "../orchestrationV2.ts";
import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  StopCurrentThreadRuntimeInput,
  ReadCurrentRuntimeStopTargetResult,
} from "./runtimeStop.ts";
import { ExecutionEnvironmentCapabilities } from "../environment.ts";
const input = {
  commandId: "command:stop",
  threadId: "thread:stop",
  target: {
    binding: {
      threadId: "thread:stop",
      providerThreadId: "provider-thread:stop",
      providerSessionId: "session:stop",
      providerInstanceId: "codex",
      driver: "codex",
      nativeThreadId: "native:stop",
      runtimeGeneration: "physical:stop",
    },
    evidenceRevision: 3,
  },
};
describe("captured runtime stop contract", () => {
  it("requires complete capture identity rather than only a thread name", () => {
    expect(Schema.decodeUnknownSync(StopCurrentThreadRuntimeInput)(input)).toEqual(input);
    expect(() =>
      Schema.decodeUnknownSync(StopCurrentThreadRuntimeInput)({ ...input, target: undefined }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(StopCurrentThreadRuntimeInput)({
        ...input,
        target: { ...input.target, evidenceRevision: -1 },
      }),
    ).toThrow();
  });
  it("does not accept payload authority as part of a stop command", () => {
    expect(() =>
      Schema.decodeUnknownSync(StopCurrentThreadRuntimeInput)(
        { ...input, actorSessionId: "fabricated", scopes: ["orchestration:operate"] },
        { onExcessProperty: "error" },
      ),
    ).toThrow();
  });
  it("reports unsupported physical capture without manufacturing a target", () => {
    expect(
      Schema.decodeUnknownSync(ReadCurrentRuntimeStopTargetResult)({
        status: "unavailable",
        reason: "physical_capture_unavailable",
        backgroundCoverage: "partial",
      }).status,
    ).toBe("unavailable");
    expect(() =>
      Schema.decodeUnknownSync(ReadCurrentRuntimeStopTargetResult)({
        status: "available",
        backgroundCoverage: "partial",
      }),
    ).toThrow();
  });
  it("keeps capability absent on old environments and driver support explicit", () => {
    const decode = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);
    expect(decode({ repositoryIdentity: false }).currentRuntimeStop).toBeUndefined();
    expect(
      decode({
        repositoryIdentity: false,
        currentRuntimeStop: {
          targetRequired: true,
          supportedDrivers: ["codex", "claudeAgent"],
          backgroundCoverage: "partial",
        },
      }).currentRuntimeStop?.supportedDrivers,
    ).toEqual(["codex", "claudeAgent"]);
    expect(() =>
      decode({
        repositoryIdentity: false,
        currentRuntimeStop: {
          targetRequired: true,
          supportedDrivers: ["openCode"],
          backgroundCoverage: "partial",
        },
      }),
    ).toThrow();
  });
});

it("replays requested stop without decoding it as successful detachment", () => {
  const wire = {
    id: "event:stop:requested",
    type: "provider-session.detach-requested",
    threadId: "thread:stop",
    occurredAt: "2026-10-07T12:00:00.000Z",
    driver: "codex",
    providerInstanceId: "codex",
    payload: { providerSessionId: "session:stop" },
  };
  const event = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson)(wire);
  expect(event.type).toBe("provider-session.detach-requested");
  expect(Schema.encodeSync(OrchestrationV2DomainEventJson)(event)).toEqual(wire);
  expect(event.payload).toEqual({ providerSessionId: "session:stop" });
  expect(() =>
    Schema.decodeUnknownSync(OrchestrationV2DomainEventJson)({
      ...wire,
      type: "provider-session.detached",
    }),
  ).toThrow();
});
