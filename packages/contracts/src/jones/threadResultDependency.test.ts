import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { OrchestratorMcpThreadSendInput, OrchestratorMcpThreadSendResult } from "../orchestratorMcp.ts";
import { OrchestrationV2ShellStreamItem, OrchestrationV2SubscribeShellInput } from "../orchestrationV2.ts";
import { ThreadRequestFulfillInput, ThreadRequestObserveFields, ThreadResultRequestDescriptor } from "./threadResultDependency.ts";

it("keeps ordinary send contracts decodable and adds an optional result dependency", () => {
  const input = Schema.decodeUnknownSync(OrchestratorMcpThreadSendInput);
  expect(input({ threadId: "receiver", message: "INFO" }).awaitResult).toBeUndefined();
  expect(input({ threadId: "receiver", message: "Need a result", awaitResult: true }).awaitResult).toBe(true);
  const response = { threadId: "receiver", messageId: "message", runId: "run", status: "running", delivery: "steered" };
  const output = Schema.decodeUnknownSync(OrchestratorMcpThreadSendResult);
  expect(output(response).dependency).toBeUndefined();
  expect(output({ ...response, dependency: { dependencyId: "dependency", requestId: null, state: "sending" } }).dependency?.state).toBe("sending");
});

it("bounds answers, tokens and observation holds and rejects unsupported protocol versions", () => {
  const fulfill = Schema.decodeUnknownSync(ThreadRequestFulfillInput);
  expect(fulfill({ requestId: "request", outcome: "declined", result: "Cannot answer" }).outcome).toBe("declined");
  expect(() => fulfill({ requestId: "request", outcome: "fulfilled", result: "" })).toThrow();
  expect(() => fulfill({ requestId: "request", outcome: "fulfilled", result: "x".repeat(16_001) })).toThrow();
  const observe = Schema.decodeUnknownSync(Schema.Struct(ThreadRequestObserveFields));
  expect(observe({ operationId: "operation", timeoutMs: 50_000 }).timeoutMs).toBe(50_000);
  for (const timeoutMs of [-1, 50_001, Infinity, NaN]) expect(() => observe({ operationId: "operation", timeoutMs })).toThrow();
  expect(() => observe({ operationId: "operation", afterToken: "x".repeat(4097) })).toThrow();
  expect(() => Schema.decodeUnknownSync(ThreadResultRequestDescriptor)({ operationId: "operation", schemaVersion: "v2" })).toThrow();
});

it("negotiates shell refresh without requiring older subscribers to send the flag", () => {
  const subscribe = Schema.decodeUnknownSync(OrchestrationV2SubscribeShellInput);
  expect(subscribe({}).acceptThreadRefresh).toBeUndefined();
  expect(subscribe({ afterSequence: 12, acceptThreadRefresh: true }).acceptThreadRefresh).toBe(true);
  const item = { kind: "thread.omitted", sequence: 12, location: "active", threadId: "child" };
  expect(Schema.decodeUnknownSync(OrchestrationV2ShellStreamItem)(item)).toEqual(item);
});
