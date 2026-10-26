import { CommandId, EventId, MessageId, OrchestrationV2AppThread, OrchestrationV2Run, OrchestrationV2ConversationMessage, ThreadId, type OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { makeDependencyStore } from "./DependencyStore.ts";
import * as Changes from "./DependencyChanges.ts";
import type { OpenRequest } from "./DependencyCommit.ts";

const database = Sqlite.layerMemory;
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(Layer.provideMerge(database));
export const layer = Layer.merge(stores, EventSink.layer.pipe(Layer.provide(stores))).pipe(Layer.provideMerge(Changes.layer));
export const time = DateTime.makeUnsafe("2026-10-26T16:00:00.000Z");
const decodeThread = Schema.decodeUnknownSync(OrchestrationV2AppThread);
const decodeRun = Schema.decodeUnknownSync(OrchestrationV2Run);
const decodeMessage = Schema.decodeUnknownSync(OrchestrationV2ConversationMessage);
export const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = makeDependencyStore(sql, makeJonesTablePresence(sql));
  const sink = yield* EventSink.EventSinkV2;
  const projection = yield* ProjectionStore.ProjectionStoreV2;
  const thread = (id: string) => decodeThread({
    createdBy: "user", creationSource: "web", id, projectId: "result-test", title: id,
    providerInstanceId: "codex", modelSelection: { instanceId: "codex", model: "test" },
    runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id }, forkedFrom: null,
    createdAt: time, updatedAt: time, archivedAt: null, settledOverride: null, settledAt: null, lastVisitedAt: null, deletedAt: null,
  });
  const waiter = thread("waiter");
  const receiver = thread("receiver");
  const run = (id: string, threadId: ThreadId, status: OrchestrationV2Run["status"], ordinal = 1) => decodeRun({
    id, threadId, ordinal, providerInstanceId: "codex", modelSelection: { instanceId: "codex", model: "test" },
    providerThreadId: null, userMessageId: `message:${id}`, rootNodeId: null, activeAttemptId: null,
    status, requestedAt: time, startedAt: time, completedAt: status === "completed" ? time : null, checkpointId: null, contextHandoffId: null,
  });
  const waiterRun = run("waiter-run", waiter.id, "completed");
  const receiverRun = run("receiver-run", receiver.id, "running");
  const message = (id: string, target = receiverRun, role: OrchestrationV2ConversationMessage["role"] = "user") => decodeMessage({
    id, threadId: target.threadId, runId: target.id, nodeId: null, role, text: "Request body", attachments: [], streaming: false,
    createdBy: "agent", creationSource: "mcp", createdAt: time, updatedAt: time,
  });
  let ordinal = 0;
  const event = (value: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">) => ({ ...value, id: EventId.make(`fixture:${++ordinal}`), occurredAt: time }) as OrchestrationV2DomainEvent;
  yield* sink.write({ events: [
    event({ type: "thread.created", threadId: waiter.id, payload: waiter }),
    event({ type: "thread.created", threadId: receiver.id, payload: receiver }),
    event({ type: "run.created", threadId: waiter.id, runId: waiterRun.id, payload: waiterRun }),
    event({ type: "run.created", threadId: receiver.id, runId: receiverRun.id, payload: receiverRun }),
  ] });
  const requestInput = (key: string): OpenRequest => ({
    requestId: `request:${key}`, operationId: `operation:${key}`, targetThreadId: receiver.id,
    messageId: MessageId.make(`request-message:${key}`), commandId: `send:${key}`,
    originKind: "local", originEnvironmentId: "local", originThreadId: waiter.id, originLabel: waiter.title,
    localDependency: { dependencyId: `dependency:${key}`, waiterRunId: waiterRun.id, waiterGeneration: 0, targetLabel: receiver.title },
  });
  const send = (key: string, targetRun = receiverRun, input = requestInput(key)) => sink.commitCommand({
    commandId: CommandId.make(input.commandId), threadId: receiver.id, commandType: "message.dispatch", acceptedAt: time,
    resultRequest: input,
    events: [event({ type: "message.updated", threadId: receiver.id, runId: targetRun.id, payload: message(input.messageId, targetRun) })], effects: [],
  }).pipe(Effect.as(input));
  return { sql, store, sink, projection, waiter, receiver, waiterRun, receiverRun, run, message, event, requestInput, send };
});
