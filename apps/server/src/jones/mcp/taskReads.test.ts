import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  NodeId,
  OrchestrationV2DomainEvent,
  type RemoteTaskObservation,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2Subagent,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ThreadProjection,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as OrchestratorMcpService from "../../mcp/OrchestratorMcpService.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProjectionRecordField } from "../../orchestration-v2/ProjectionStore.ts";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import { fixture as continuationFixture } from "../taskContinuation/testkit.ts";

const parentId = ThreadId.make("thread:task-reads-parent");
const taskId = NodeId.make("opaque-selected-task");
const providerInstanceId = ProviderInstanceId.make("codex");
const now = DateTime.makeUnsafe("2026-10-01T00:00:00Z");
const scope: McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("environment:task-reads-local"),
  requestNamespace: "task-reads",
  thread: { threadId: parentId, providerSessionId: "task-reads", providerInstanceId },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};
const remoteChild = {
  environmentId: EnvironmentId.make("environment:task-reads-remote"),
  threadId: ThreadId.make("thread:task-reads-remote-child"),
  runId: RunId.make("run:task-reads-remote-original"),
  label: "Remote child",
};
const task: OrchestrationV2Subagent = {
  id: taskId,
  threadId: parentId,
  runId: null,
  parentNodeId: NodeId.make("node:task-reads-parent"),
  origin: "app_owned",
  createdBy: "agent",
  driver: ProviderDriverKind.make("codex"),
  providerInstanceId,
  providerThreadId: null,
  childThreadId: null,
  remoteChild,
  nativeTaskRef: null,
  prompt: "Synthetic task",
  title: null,
  model: "test-model",
  status: "running",
  result: null,
  startedAt: now,
  completedAt: null,
  updatedAt: now,
};
const storedUpdate = (id: NodeId, sequence = 42): OrchestrationV2StoredEvent => ({
  sequence,
  commandId: null,
  event: {
    id: EventId.make(`event:task-reads-${sequence}`),
    threadId: parentId,
    nodeId: taskId,
    occurredAt: now,
    type: "subagent.updated",
    payload: { ...task, id },
  },
});
const serviceLayer = (
  management: Partial<ThreadManagementService.ThreadManagementService["Service"]>,
) =>
  OrchestratorMcpService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mergeAll(NodeServices.layer, NodeCrypto.layer),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          readRemoteTaskObservation: () => Effect.sync(() => undefined),
          ...management,
        }),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({
          delegate: () => Effect.succeed({ taskId }),
          observe: (_scope, observedTask) =>
            Effect.succeed({
              observation: {
                schemaVersion: "t3.remote-task-observation/v1",
                environmentId: remoteChild.environmentId,
                threadId: remoteChild.threadId,
                originalRunId: remoteChild.runId,
                blockingQuestions: { resumeToken: "task-reads", requests: [] },
                childProgress: {
                  hasPendingChildRuns: false,
                  latestTerminalRunId: remoteChild.runId,
                  latestTerminalStatus: "completed",
                },
              } satisfies RemoteTaskObservation,
              latestTerminalSummary: observedTask.result,
            }),
        }),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      ),
    ),
  );

const parent = idleThreadProjection(liveThreadShell(parentId));

const isOrchestrationV2DomainEvent = Schema.is(OrchestrationV2DomainEvent);

type ReadCall = {
  readonly threadId: ThreadId;
  readonly fields: ReadonlyArray<ProjectionRecordField>;
  readonly options: Parameters<
    ThreadManagementService.ThreadManagementService["Service"]["getThreadRecords"]
  >[2];
};
const expectedTaskRead: ReadCall = {
  threadId: parentId,
  fields: ["subagents"],
  options: { subagentIds: [taskId] },
};
const expectedParentRead: ReadCall = {
  threadId: parentId,
  fields: ["runs", "contextTransfers"],
  options: undefined,
};
const expectedAckRead: ReadCall = { threadId: parentId, fields: ["runs"], options: undefined };

describe("MCP task read selection", () => {
  it.effect("exposes a completed Claude task's captured native ID without manual notes", () =>
    Effect.gen(function* () {
      const f = continuationFixture();
      const reads: Array<ReadonlyArray<string>> = [];
      const task = {
        ...f.task,
        completionDelivery: { state: "acknowledged" as const, observedByRunId: null },
      };
      const layer = serviceLayer({
        getThreadRecords: (threadId, fields) =>
          Effect.sync(() => {
            if (threadId === f.parent.thread.id) return { ...f.parent, subagents: [task] };
            assert.equal(threadId, f.child.thread.id);
            reads.push(fields);
            return f.child;
          }),
      });
      const result = yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        return yield* service.taskStatus(f.scope, f.task.id);
      }).pipe(Effect.provide(layer));
      assert.deepEqual(result.nativeSession, {
        nativeSessionId: "original-native",
        sourceRunId: f.run.id,
        sourceAttemptId: f.attempt.id,
        evidence: "attempt",
      });
      assert.deepEqual(reads.at(-1), ["attempts", "providerTurns"]);
    }),
  );
  it.effect.each(["siblings", "completion", "question", "undecodable"] as const)(
    "filters parent wake events: %s",
    (scenario) =>
      Effect.gen(function* () {
        let reads = 0;
        const parentCalls: ReadCall[] = [];
        let cursorCaptured = false;
        let streamClosed = false;
        const sibling = storedUpdate(NodeId.make("unrelated-task"));
        assert.isTrue(isOrchestrationV2DomainEvent(sibling.event));
        const events =
          scenario === "siblings"
            ? Array.from({ length: 128 }, (_, index) => ({ ...sibling, sequence: 42 + index }))
            : [
                scenario === "undecodable"
                  ? ({
                      ...sibling,
                      event: { ...sibling.event, payload: null },
                    } as unknown as OrchestrationV2StoredEvent)
                  : storedUpdate(taskId),
              ];
        const layer = serviceLayer({
          getThreadEventSequence: () =>
            Effect.sync(() => {
              assert.equal(reads, 0);
              cursorCaptured = true;
              return 41;
            }),
          getThreadRecords: (threadId, fields, options) =>
            Effect.sync(() => {
              assert.equal(threadId, parentId);
              assert.isTrue(cursorCaptured);
              parentCalls.push({ threadId, fields, options });
              reads += 1;
              const selected: OrchestrationV2Subagent =
                reads === 1 || scenario === "siblings"
                  ? task
                  : scenario === "question"
                    ? { ...task, status: "waiting", progress: "Which option?" }
                    : {
                        ...task,
                        status: "completed",
                        result: "Done",
                        completionDelivery: { state: "acknowledged", observedByRunId: null },
                      };
              return {
                ...parent,
                subagents: [
                  selected,
                  ...Array.from({ length: 128 }, (_, index) => ({
                    ...task,
                    id: NodeId.make(`sibling-${index}`),
                  })),
                ],
              };
            }),
          streamStoredEventsFrom: (input) => {
            assert.deepEqual(input, {
              threadId: parentId,
              afterSequence: 41,
              eventType: "subagent.updated",
            });
            return Stream.fromIterable(events).pipe(
              Stream.ensuring(
                Effect.sync(() => {
                  streamClosed = true;
                }),
              ),
            );
          },
          dispatch: () => Effect.succeed({} as never),
        });
        const result = yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          return yield* service.delegateTask(scope, {
            task: "Synthetic task",
            mode: "wait",
            timeoutMs: 1_000,
            target: { environmentId: remoteChild.environmentId },
          });
        }).pipe(Effect.provide(layer));
        assert.equal(reads, 2);
        assert.deepEqual(parentCalls, [expectedTaskRead, expectedTaskRead]);
        assert.isTrue(streamClosed);
        assert.equal(result.waitTimedOut, scenario === "siblings");
        assert.equal(result.hasPendingQuestion, scenario === "question");
        assert.equal(
          result.status,
          scenario === "completion" || scenario === "undecodable" ? "completed" : "running",
        );
      }),
  );

  it.effect.each([
    "run.updated",
    "subagent.updated",
    "provider-thread.updated",
    "runtime-request.updated",
    "approval",
  ] as const)("keeps child activity wakeups: %s", (scenario) =>
    Effect.gen(function* () {
      const eventType = scenario === "approval" ? "runtime-request.updated" : scenario;
      const pendingQuestion = scenario === "runtime-request.updated";
      const pendingApproval = scenario === "approval";
      const childId = ThreadId.make("thread:task-reads-local-child");
      const childRunId = RunId.make("run:task-reads-local");
      const approvalNodeId = NodeId.make("node:task-reads-approval");
      const approvalRequestId = RuntimeRequestId.make("request:task-reads-approval");
      const child = idleThreadProjection(liveThreadShell(childId));
      let parentReads = 0;
      const calls: ReadCall[] = [];
      let controlReads = 0;
      let resultReads = 0;
      const layer = serviceLayer({
        getThreadEventSequence: () => Effect.succeed(41),
        getThreadRecords: (threadId, fields, options) =>
          Effect.sync(() => {
            calls.push({ threadId, fields, options });
            if (threadId === parentId) {
              parentReads += 1;
              return {
                ...parent,
                subagents: [
                  {
                    ...task,
                    childThreadId: childId,
                    completionDelivery: {
                      state: pendingApproval ? ("acknowledged" as const) : ("disposed" as const),
                      observedByRunId: null,
                    },
                  },
                ],
              };
            }
            assert.equal(threadId, childId);
            if (fields.some((field) => field === "runs")) {
              assert.deepEqual<ReadonlyArray<ProjectionRecordField>>(fields, [
                "runs",
                "messages",
                "contextTransfers",
                "subagents",
                "providerThreads",
                "providerTurns",
                "runtimeRequests",
                "nodes",
              ]);
              assert.deepEqual(options, {
                messageRoles: ["user"],
                omitSettledLeafNodes: true,
                omitSettledSubagents: true,
              });
              controlReads += 1;
            } else {
              assert.deepEqual<ReadonlyArray<ProjectionRecordField>>(fields, [
                "messages",
                "turnItems",
              ]);
              resultReads += 1;
            }
            const finished = controlReads > 1;
            return {
              ...child,
              runs: [
                {
                  id: childRunId,
                  delegatedTaskId: taskId,
                  ordinal: 1,
                  status:
                    finished && eventType !== "runtime-request.updated" ? "completed" : "running",
                  startedAt: now,
                  completedAt: finished && eventType !== "runtime-request.updated" ? now : null,
                } as OrchestrationV2Run,
              ],
              nodes:
                finished && pendingApproval
                  ? [
                      {
                        id: approvalNodeId,
                        runId: childRunId,
                      } as OrchestrationV2ThreadProjection["nodes"][number],
                    ]
                  : [],
              runtimeRequests:
                finished && (pendingQuestion || pendingApproval)
                  ? [
                      {
                        id: approvalRequestId,
                        nodeId: approvalNodeId,
                        kind: pendingApproval ? "command" : "user_input",
                        status: "pending",
                        responseCapability: { type: "live" },
                        createdAt: now,
                      } as OrchestrationV2RuntimeRequest,
                    ]
                  : [],
            };
          }),
        delegatedTaskResultPending: () => Effect.succeed(false),
        streamStoredEventsFrom: (input) => {
          assert.equal(input?.afterSequence, 41);
          if (input?.threadId !== childId || input.eventType !== eventType) return Stream.empty;
          const event = storedUpdate(NodeId.make("nested-child-task"));
          return Stream.succeed({
            ...event,
            event: { ...event.event, threadId: childId, type: eventType },
          } as OrchestrationV2StoredEvent);
        },
      });
      const result = yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        return yield* service.delegateTask(scope, {
          task: "Synthetic task",
          mode: "wait",
          timeoutMs: 1_000,
          target: { environmentId: remoteChild.environmentId },
        });
      }).pipe(Effect.provide(layer));
      assert.equal(
        result.status,
        eventType === "runtime-request.updated" ? "running" : "completed",
      );
      assert.equal(result.hasPendingQuestion, pendingQuestion);
      assert.deepEqual(
        result.pendingApproval,
        pendingApproval
          ? { requestId: approvalRequestId, kind: "command", runId: childRunId }
          : null,
      );
      assert.isFalse(result.waitTimedOut);
      assert.equal(parentReads, 4);
      const control: ReadCall = {
        threadId: childId,
        fields: [
          "runs",
          "messages",
          "contextTransfers",
          "subagents",
          "providerThreads",
          "providerTurns",
          "runtimeRequests",
          "nodes",
        ],
        options: { messageRoles: ["user"], omitSettledLeafNodes: true, omitSettledSubagents: true },
      };
      const resultRead: ReadCall = {
        threadId: childId,
        fields: ["messages", "turnItems"],
        options: {
          messageRoles: ["assistant"],
          messageRunIds: [childRunId],
          turnItemRunIds: [childRunId],
          turnItemTypes: ["assistant_message", "error"],
        },
      };
      assert.deepEqual(calls, [
        expectedTaskRead,
        expectedParentRead,
        control,
        resultRead,
        expectedTaskRead,
        expectedParentRead,
        control,
        resultRead,
      ]);
      assert.equal(controlReads, 2);
      assert.equal(resultReads, 2);
    }),
  );

  it.effect.each(["running", "completed"] as const)(
    "reads local original and followup results: %s",
    (followupStatus) =>
      Effect.gen(function* () {
        const childId = ThreadId.make("thread:task-reads-followup");
        const originalId = RunId.make("run:task-reads-original");
        const followupId = RunId.make("run:task-reads-followup");
        const child = idleThreadProjection(liveThreadShell(childId));
        const calls: ReadCall[] = [];
        const childCalls: ReadCall[] = [];
        const layer = serviceLayer({
          getThreadRecords: (threadId, fields, options) =>
            Effect.sync(() => {
              if (threadId === parentId) {
                calls.push({ threadId, fields, options });
                return {
                  ...parent,
                  subagents: [
                    {
                      ...task,
                      childThreadId: childId,
                      completionDelivery: { state: "disposed" as const, observedByRunId: null },
                    },
                  ],
                };
              }
              assert.equal(threadId, childId);
              childCalls.push({ threadId, fields, options });
              return {
                ...child,
                runs: [
                  {
                    id: originalId,
                    delegatedTaskId: taskId,
                    ordinal: 1,
                    status: "completed",
                    startedAt: now,
                    completedAt: now,
                  },
                  {
                    id: followupId,
                    delegatedTaskId: taskId,
                    ordinal: 2,
                    status: followupStatus,
                    startedAt: now,
                    completedAt: followupStatus === "completed" ? now : null,
                  },
                ] as Array<OrchestrationV2Run>,
              };
            }),
          delegatedTaskResultPending: () => Effect.succeed(false),
        });
        const result = yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          return yield* service.taskStatus(scope, taskId);
        }).pipe(Effect.provide(layer));
        assert.deepEqual(calls, [expectedTaskRead, expectedParentRead]);
        const resultRunIds = followupStatus === "running" ? [followupId, originalId] : [followupId];
        assert.deepEqual(childCalls, [
          {
            threadId: childId,
            fields: [
              "runs",
              "messages",
              "contextTransfers",
              "subagents",
              "providerThreads",
              "providerTurns",
              "runtimeRequests",
              "nodes",
            ],
            options: {
              messageRoles: ["user"],
              omitSettledLeafNodes: true,
              omitSettledSubagents: true,
            },
          },
          {
            threadId: childId,
            fields: ["messages", "turnItems"],
            options: {
              messageRoles: ["assistant"],
              messageRunIds: resultRunIds,
              turnItemRunIds: resultRunIds,
              turnItemTypes: ["assistant_message", "error"],
            },
          },
        ]);
        assert.equal(result.childRunId, originalId);
        assert.equal(result.status, followupStatus);
        assert.equal(
          result.latestTerminalRunId,
          followupStatus === "completed" ? followupId : originalId,
        );
        assert.equal(result.hasPendingChildRuns, followupStatus === "running");
      }),
  );

  it.effect(
    "remote acknowledgement reads fresh runs only and retries with a fresh command id",
    () =>
      Effect.gen(function* () {
        const calls: ReadCall[] = [];
        const commands: Array<string> = [];
        const observingRunId = RunId.make("run:fresh-observer");
        const layer = serviceLayer({
          getThreadRecords: (threadId, fields, options) =>
            Effect.sync(() => {
              assert.equal(threadId, parentId);
              calls.push({ threadId, fields, options });
              return {
                ...parent,
                subagents: [{ ...task, status: "completed" as const, result: "Remote result" }],
                runs:
                  fields.length === 1 && fields[0] === "runs"
                    ? [
                        {
                          id: observingRunId,
                          status: "running",
                          ordinal: 2,
                          providerInstanceId,
                        } as OrchestrationV2Run,
                      ]
                    : [],
              };
            }),
          dispatch: (command) =>
            Effect.gen(function* () {
              assert.equal(command.type, "delegated_task.completion-delivery.acknowledge");
              if (command.type !== "delegated_task.completion-delivery.acknowledge")
                return {} as never;
              assert.equal(command.observedByRunId, observingRunId);
              commands.push(command.commandId);
              if (commands.length === 1)
                return yield* Effect.fail(new Error("Synthetic failure") as never);
              return {} as never;
            }),
        });
        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const error = yield* service.taskStatus(scope, taskId).pipe(Effect.flip);
          assert.equal(error.code, "orchestration_error");
          const result = yield* service.taskStatus(scope, taskId);
          assert.equal(result.summary, "Remote result");
          assert.equal(result.status, "completed");
        }).pipe(Effect.provide(layer));
        assert.deepEqual(calls, [
          expectedTaskRead,
          expectedAckRead,
          expectedTaskRead,
          expectedAckRead,
        ]);
        assert.lengthOf(commands, 2);
        assert.notEqual(commands[0], commands[1]);
      }),
  );
});
