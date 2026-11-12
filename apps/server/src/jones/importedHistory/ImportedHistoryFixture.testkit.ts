import {
  AuthSessionId,
  AuthOrchestrationOperateScope,
  ThreadId,
  RunId,
  MessageId,
  NodeId,
  RunAttemptId,
  ProviderThreadId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProjectId,
  EventId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
export const principal = {
  sessionId: AuthSessionId.make("session:atomic-import"),
  subject: "fixture",
  method: "browser-session-cookie" as const,
  scopes: new Set([AuthOrchestrationOperateScope]),
};
export const threadId = ThreadId.make("thread:atomic-import");
export const runId = RunId.make("run:atomic-import");
const messageId = MessageId.make("message:atomic-import");
export const providerThreadId = ProviderThreadId.make("provider-thread:atomic-import");
export const providerInstanceId = ProviderInstanceId.make("codex");
export const now = DateTime.makeUnsafe("2026-10-07T00:00:00Z");
export const capabilities = {
  context: { canConsumeHandoffSummaries: true, supportsFullThreadHandoff: true },
};
export const delivery = { type: "queued_run" as const, runId, messageId };
export const run = {
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId,
  modelSelection: { instanceId: providerInstanceId, model: "fixture" },
  providerThreadId,
  userMessageId: messageId,
  rootNodeId: NodeId.make("node:atomic-import"),
  activeAttemptId: RunAttemptId.make("attempt:atomic-import"),
  status: "queued" as const,
  queueHeld: true,
  requestedAt: now,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};
export const setup = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO auth_sessions(session_id,subject,scopes,method,issued_at,expires_at) VALUES (${principal.sessionId}, 'fixture', ${yield* encode([...principal.scopes])}, 'browser-session-cookie', '2026-10-07T00:00:00Z', '2099-01-01T00:00:00Z')`;
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('project:atomic-import','Fixture','/fixture','[]','2026-10-07T00:00:00Z','2026-10-07T00:00:00Z')`;
  const fixtureEvents = [
    {
      id: EventId.make(`migration:v1:thread:${threadId}:created`),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: {
        id: threadId,
        projectId: ProjectId.make("project:atomic-import"),
        title: "Imported fixture",
        providerInstanceId,
        modelSelection: run.modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdBy: "user",
        creationSource: "web",
        historyOrigin: "v1_import",
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    },
    {
      id: EventId.make("event:import:provider"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: providerThreadId,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      id: EventId.make("event:import:run"),
      type: "run.created",
      threadId,
      runId,
      occurredAt: now,
      payload: run,
    },
    {
      id: EventId.make("event:import:message"),
      type: "message.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: {
        id: messageId,
        threadId,
        runId,
        nodeId: run.rootNodeId,
        role: "user",
        text: "Reviewed queued prompt",
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      id: EventId.make("event:import:source"),
      type: "turn-item.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: TurnItemId.make("item:import:source"),
        threadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "user_message",
        messageId: MessageId.make("message:import:source"),
        inputIntent: "turn_start",
        text: "Imported transcript",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
    },
  ] satisfies Parameters<typeof sink.write>[0]["events"];
  for (const event of fixtureEvents) yield* sink.write({ events: [event] });
});
