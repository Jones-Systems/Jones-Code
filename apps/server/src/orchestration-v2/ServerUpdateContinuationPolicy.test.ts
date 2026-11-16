import { assert, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { restartContinuationRun } from "./RestartContinuationPolicy.ts";
import {
  serverUpdateContinuationEffect,
  serverUpdateContinuationRun,
} from "./ServerUpdateContinuationPolicy.ts";

const threadId = ThreadId.make("thread:update-policy");
const projectId = ProjectId.make("project:update-policy");
const runId = RunId.make("run:update-policy");
const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const providerThreadId = ProviderThreadId.make("provider-thread:update-policy");
const sessionId = ProviderSessionId.make("session:update-policy");
const attemptId = RunAttemptId.make("attempt:update-policy");
const now = DateTime.makeUnsafe("2026-10-16T00:00:00.000Z");

function makeProjection() {
  return {
    thread: {
      id: threadId,
      projectId,
      providerInstanceId: instanceId,
      archivedAt: null,
      deletedAt: null,
    },
    runs: [
      {
        id: runId,
        ordinal: 1,
        providerInstanceId: instanceId,
        providerThreadId,
        activeAttemptId: attemptId,
        status: "running",
        completedAt: null,
      },
    ],
    providerThreads: [
      {
        id: providerThreadId,
        appThreadId: threadId,
        ownerNodeId: null,
        driver,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
        status: "active",
      },
    ],
    providerSessions: [{ id: sessionId, driver, providerInstanceId: instanceId, status: "ready" }],
    providerTurns: [
      {
        id: ProviderTurnId.make("turn:update-policy"),
        providerThreadId,
        runAttemptId: attemptId,
        status: "running",
      },
    ],
    runtimeRequests: [],
    turnItems: [],
  } as unknown as Parameters<typeof restartContinuationRun>[0];
}

it("follows the environment default and project overrides in both directions", () => {
  const projection = makeProjection();
  const optedOut = { ...DEFAULT_SERVER_SETTINGS, continueThreadsAfterServerUpdate: false };
  assert.strictEqual(
    serverUpdateContinuationRun(projection, DEFAULT_SERVER_SETTINGS, now),
    projection.runs[0],
  );
  assert.isUndefined(serverUpdateContinuationRun(projection, optedOut, now));
  assert.isUndefined(
    serverUpdateContinuationRun(
      projection,
      {
        ...DEFAULT_SERVER_SETTINGS,
        projectSettingsOverrides: { [projectId]: { continueThreadsAfterServerUpdate: false } },
      },
      now,
    ),
  );
  assert.strictEqual(
    serverUpdateContinuationRun(
      projection,
      {
        ...optedOut,
        projectSettingsOverrides: { [projectId]: { continueThreadsAfterServerUpdate: true } },
      },
      now,
    ),
    projection.runs[0],
  );
});

it("allows explicit preparation without preferences but retains runtime eligibility", () => {
  const projection = makeProjection();
  assert.strictEqual(serverUpdateContinuationRun(projection, null, now), projection.runs[0]);
  assert.isUndefined(serverUpdateContinuationRun({ ...projection, providerTurns: [] }, null, now));
});

it("recovers a prepared starting run without preparing it again", () => {
  const projection = makeProjection();
  const prepared = {
    ...projection,
    runs: [
      {
        ...projection.runs[0]!,
        status: "starting" as const,
        restartContinuationOfRunId: RunId.make("run:before-update"),
      },
    ],
    providerSessions: [{ ...projection.providerSessions[0]!, status: "stopped" as const }],
    providerTurns: [],
  };
  assert.strictEqual(restartContinuationRun(prepared, now), prepared.runs[0]);
  assert.isUndefined(serverUpdateContinuationRun(prepared, null, now));
  assert.isUndefined(
    restartContinuationRun(
      {
        ...prepared,
        runs: [{ ...projection.runs[0]!, status: "starting" }],
      },
      now,
    ),
  );
});

it("ignores queued runs and orders candidates by runRanAfter, including completion time", () => {
  const projection = makeProjection();
  const completedAt = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
  const withLaterOrdinals = {
    ...projection,
    runs: [
      projection.runs[0]!,
      {
        ...projection.runs[0]!,
        id: RunId.make("run:completed"),
        ordinal: 2,
        status: "completed" as const,
        completedAt,
      },
      {
        ...projection.runs[0]!,
        id: RunId.make("run:queued"),
        ordinal: 3,
        status: "queued" as const,
      },
    ],
  };
  assert.strictEqual(serverUpdateContinuationRun(withLaterOrdinals, null, now), projection.runs[0]);
  const latest = { ...projection.runs[0]!, id: RunId.make("run:latest"), ordinal: 4 };
  assert.strictEqual(
    serverUpdateContinuationRun(
      { ...withLaterOrdinals, runs: [...withLaterOrdinals.runs, latest] },
      null,
      now,
    ),
    latest,
  );
});

it("does not reinterpret cancelled or completed runs as interrupted running work", () => {
  const projection = makeProjection();
  for (const status of ["cancelled", "completed"] as const) {
    const settled = { ...projection, runs: [{ ...projection.runs[0]!, status }] };
    assert.isUndefined(restartContinuationRun(settled, now));
    assert.isUndefined(serverUpdateContinuationRun(settled, null, now));
  }
});

it("retains native identity, provider session, and active turn fences", () => {
  const projection = makeProjection();
  const providerThread = projection.providerThreads[0]!;
  const session = projection.providerSessions[0]!;
  const turn = projection.providerTurns[0]!;
  for (const invalid of [
    {
      ...projection,
      thread: { ...projection.thread, providerInstanceId: ProviderInstanceId.make("other") },
    },
    { ...projection, providerThreads: [{ ...providerThread, nativeThreadRef: null }] },
    {
      ...projection,
      providerThreads: [
        {
          ...providerThread,
          nativeThreadRef: { driver, nativeId: "native-thread", strength: "weak" as const },
        },
      ],
    },
    {
      ...projection,
      providerThreads: [{ ...providerThread, appThreadId: ThreadId.make("thread:other") }],
    },
    { ...projection, providerSessions: [] },
    { ...projection, providerSessions: [{ ...session, status: "stopped" as const }] },
    {
      ...projection,
      providerSessions: [{ ...session, driver: ProviderDriverKind.make("other") }],
    },
    {
      ...projection,
      providerTurns: [{ ...turn, runAttemptId: RunAttemptId.make("attempt:other") }],
    },
    {
      ...projection,
      providerTurns: [
        { ...turn, providerThreadId: ProviderThreadId.make("provider-thread:other") },
      ],
    },
  ]) {
    assert.isUndefined(restartContinuationRun(invalid, now));
    assert.isUndefined(serverUpdateContinuationRun(invalid, null, now));
  }
});

it("keeps passive effect identity and preference override payloads unchanged", () => {
  for (const optedInOnly of [true, false]) {
    assert.deepEqual(
      serverUpdateContinuationEffect({
        preparationId: "preparation-id",
        threadId,
        sourceRunId: runId,
        optedInOnly,
      }),
      {
        id: "effect:server-update-continuation:preparation-id",
        commandId: CommandId.make("command:server-update-prepare:preparation-id"),
        threadId,
        request: {
          type: "provider-runtime.continue",
          sourceRunId: runId,
          preparedForRestart: true,
          continueWithoutPreference: !optedInOnly,
        },
      },
    );
  }
});
