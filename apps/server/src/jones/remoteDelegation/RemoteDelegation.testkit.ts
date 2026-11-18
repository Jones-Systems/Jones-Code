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
  type OrchestrationV2RunStatus,
  type OrchestratorMcpThreadSendResult,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as RemoteSendReceipts from "../orchestration/RemoteSendReceiptStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
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

/** The delivery a receiving stand-in reports for a send it accepts. */
export interface AcceptedSend {
  readonly runId: RunId;
  readonly status: OrchestrationV2RunStatus;
  readonly delivery: OrchestratorMcpThreadSendResult["delivery"];
}

/** Linked fixture sends record the actual receipt in the same SQL transaction. */
export const acceptingSendReceipts = (sql: SqlClient.SqlClient) => {
  const store = RemoteSendReceipts.makeRemoteSendReceiptStore(sql, makeJonesTablePresence(sql));
  const remoteSendReceipts: RemoteSendReceipts.RemoteSendReceiptPorts = {
    available: store.available,
    lookup: store.lookup,
    withSendCommit: store.withSendCommit,
    pruneBefore: store.pruneBefore,
    // Stand-in threads prepare no workspace, so there is no target lock to take.
    guardPreparation: <A, E, R>(id: CommandId, preparation: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const capture = yield* RemoteSendReceipts.SendCommit;
        if (capture?.commandId !== id)
          return yield* new RemoteSendReceipts.RemoteSendReceiptError({
            reason: "preparation_capture_missing",
          });
        return yield* preparation;
      }),
  };
  const accept = <A, R>(
    targetCreatedAt: DateTime.Utc,
    accepted: AcceptedSend,
    send: Effect.Effect<A, never, R>,
  ) =>
    Effect.gen(function* () {
      const capture = yield* RemoteSendReceipts.SendCommit;
      // A send made here, not through a link, records no receipt.
      if (capture === undefined) return yield* send;
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* RemoteSendReceipts.assertTargetBirth(capture, targetCreatedAt);
            const result = yield* send;
            yield* store.record(capture, {
              threadId: capture.threadId,
              messageId: capture.messageId,
              runId: accepted.runId,
              status: accepted.status,
              delivery: accepted.delivery,
              ...(capture.link === undefined ? {} : { link: capture.link }),
            });
            return result;
          }),
        )
        .pipe(
          Effect.mapError(
            (cause) =>
              new Orchestrator.OrchestratorDispatchError({
                commandId: capture.commandId,
                commandType: "message.dispatch",
                cause,
              }),
          ),
        );
    });
  return { remoteSendReceipts, accept };
};
