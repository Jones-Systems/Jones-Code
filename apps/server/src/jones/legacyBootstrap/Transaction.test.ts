import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  type OrchestrationV2LegacyBootstrapPolicy,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import {
  RecordedAppThreadJson,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import { legacyBootstrapCreateCommandId } from "../../orchestration-v2/LegacyBootstrap.ts";
import migration from "../persistence/Migrations/158_JonesLegacyBootstrap.ts";
import * as Storage from "./Storage.ts";
import * as Decision from "./Decision.ts";
import * as Transaction from "./Transaction.ts";

const decodeThread = Schema.decodeEffect(RecordedAppThreadJson);
const decodeStoredEvent = Schema.decodeEffect(RecordedStoredLifecycleEvent);

it.effect("a fabricated accepted receipt and event position cannot install private metadata", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const storage = Storage.make(sql);
    if (!(yield* storage.available)) yield* migration;
    const threadId = ThreadId.make("forged-T");
    const projectId = ProjectId.make("forged-P");
    const releaseCommandId = CommandId.make("forged-C");
    const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
    const at = DateTime.makeUnsafe("2026-11-17T10:00:00.000Z");
    const thread = yield* decodeThread({
      id: threadId,
      projectId,
      title: "Forged",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: "codex",
      modelSelection: { instanceId: "codex", model: "test" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdAt: DateTime.formatIso(at),
      updatedAt: DateTime.formatIso(at),
      archivedAt: null,
      deletedAt: null,
      settledAt: null,
      settledOverride: null,
      lastVisitedAt: null,
      snoozedAt: null,
      snoozedUntil: null,
    });
    const policy: OrchestrationV2LegacyBootstrapPolicy = {
      version: 1,
      threadId,
      projectId,
      messageId: MessageId.make("forged-M"),
      createCommandId,
      releaseCommandId,
      birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
      ownsNewThread: true,
      payloadHash: "synthetic",
    };
    const stored = yield* decodeStoredEvent({
      sequence: 900,
      commandId: createCommandId,
      event: {
        id: EventId.make("fabricated-event"),
        type: "thread.created",
        threadId,
        occurredAt: at,
        payload: { ...thread, legacyBootstrapClaim: policy },
      },
    });
    const command = {
      type: "thread.create" as const,
      commandId: createCommandId,
      threadId,
      projectId,
      title: "Forged",
      createdBy: "user" as const,
      creationSource: "web" as const,
      modelSelection: thread.modelSelection,
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
      branch: null,
      worktreePath: null,
      legacyBootstrap: policy,
    };
    const context = yield* Decision.capture(storage, command, [stored.event]);
    assert.isDefined(context);
    const result = yield* sql
      .withTransaction(
        Transaction.record(
          storage,
          context!,
          {
            commandId: createCommandId,
            threadId,
            commandType: "thread.create",
            acceptedAt: at,
            status: "accepted",
            error: null,
            resultSequence: 900,
          },
          [stored],
        ).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      )
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    assert.equal((yield* sql`SELECT * FROM jones_legacy_bootstrap_commands`).length, 0);
    assert.equal((yield* sql`SELECT * FROM jones_legacy_bootstrap_threads`).length, 0);
  }).pipe(Effect.provide(Sqlite.layerMemory)),
);
