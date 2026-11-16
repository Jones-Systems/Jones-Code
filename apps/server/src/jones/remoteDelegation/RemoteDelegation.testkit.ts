import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
  CommandId,
  EnvironmentId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Layer from "effect/Layer";
import migration from "../persistence/Migrations/117_JonesRemoteDelegationIntents.ts";
import type * as Store from "./IntentStore.ts";
export const commandId = CommandId.make("remote:intent:test");
export const envelope: Store.IntentEnvelope = {
  scope: {
    environmentId: EnvironmentId.make("source"),
    requestNamespace: "provider:source:session-1",
    thread: {
      threadId: ThreadId.make("parent"),
      providerSessionId: "session-1",
      providerInstanceId: ProviderInstanceId.make("codex"),
    },
  },
  environmentId: EnvironmentId.make("destination"),
  linkedAt: "2026-10-21T00:00:00.000Z",
  label: "Destination",
  launch: {
    title: "Remote fixture",
    projectId: ProjectId.make("project:destination"),
    message: "Inspect fixture",
    clientRequestId: "delegate:parent:exact-key",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "fixture" },
    workspaceStrategy: { type: "worktree" },
  },
  request: {
    type: "delegated_task.remote.request",
    commandId,
    parentThreadId: ThreadId.make("parent"),
    parentRunId: RunId.make("parent:run"),
    parentNodeId: NodeId.make("parent:node"),
    task: "Inspect fixture",
    driver: ProviderDriverKind.make("remote"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "fixture" },
  },
};
export const layer = Layer.effectDiscard(migration).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
