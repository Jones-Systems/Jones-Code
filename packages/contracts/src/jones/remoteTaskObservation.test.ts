import { expect, it } from "vitest";
import * as Schema from "effect/Schema";
import {
  OrchestratorMcpThreadWaitInput,
  OrchestratorMcpThreadWaitResult,
} from "../orchestratorMcp.ts";
import { RemoteChildProgress } from "./remoteTaskObservation.ts";

it("keeps legacy waits decodable and negotiates exact versioned observations", () => {
  const decode = Schema.decodeUnknownSync(OrchestratorMcpThreadWaitResult);
  const legacy = {
    threadId: "thread:remote",
    runId: "run:remote",
    status: "running",
    timedOut: true,
  };
  expect(decode(legacy).remoteObservation).toBeUndefined();
  const remoteObservation = {
    schemaVersion: "t3.remote-task-observation/v1",
    environmentId: "environment:remote",
    threadId: "thread:remote",
    originalRunId: "run:remote",
    blockingQuestions: {
      resumeToken: "resume",
      requests: [{ requestId: "request:remote", runId: "run:remote" }],
    },
    childProgress: {
      hasPendingChildRuns: true,
      latestTerminalRunId: null,
      latestTerminalStatus: null,
    },
  };
  expect(decode({ ...legacy, remoteObservation }).remoteObservation).toEqual(remoteObservation);
  expect(() =>
    decode({ ...legacy, remoteObservation: { ...remoteObservation, schemaVersion: "v2" } }),
  ).toThrow();
  expect(
    Schema.decodeUnknownSync(OrchestratorMcpThreadWaitInput)({
      threadId: "thread:remote",
      observeBlockingQuestions: true,
      questionResumeToken: "resume",
      childProgressForRunId: "run:remote",
    }),
  ).toMatchObject({ observeBlockingQuestions: true, questionResumeToken: "resume" });
});

it("rejects progress that invents a terminal status without a matching run", () => {
  const decode = Schema.decodeUnknownSync(RemoteChildProgress);
  expect(() =>
    decode({
      hasPendingChildRuns: true,
      latestTerminalRunId: null,
      latestTerminalStatus: "completed",
    }),
  ).toThrow();
  expect(() =>
    decode({
      hasPendingChildRuns: false,
      latestTerminalRunId: "run:child",
      latestTerminalStatus: null,
    }),
  ).toThrow();
});
