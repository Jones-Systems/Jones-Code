import {
  AuthAdministrativeScopes,
  AuthSessionId,
  EnvironmentId,
  ProviderInstanceId,
  type EnvironmentSessionPrincipalShape,
  OrchestrationV2LinkOrigin,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import { encodeJson } from "./json.ts";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import events from "../../persistence/Migrations/001_OrchestrationEvents.ts";
import receipts from "../../persistence/Migrations/002_OrchestrationCommandReceipts.ts";
import core from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import integration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import review from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Providers from "../../provider/ProviderInstanceRegistry.ts";
import * as Settings from "../../serverSettings.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { makeKeyedLaunchResultReader, makeLaunchEvidenceReader } from "./EvidenceReader.ts";
import { ReceiverRecoveryEvidence } from "./RecoveryStore.ts";
import { originalReturnBinding } from "./returnBinding.ts";
import { makeReviewService } from "./ReviewService.ts";
import { makePeerProtocol } from "./PeerProtocol.ts";
import { syntheticReceiverAuth } from "./receiver.testkit.ts";

export const p1Owner: EnvironmentSessionPrincipalShape = {
  sessionId: AuthSessionId.make("owner"),
  subject: "desktop-bootstrap",
  method: "browser-session-cookie",
  scopes: new Set(AuthAdministrativeScopes),
};
export const p1Scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiver"),
  requestNamespace: "client:remote",
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
  thread: undefined,
  client: {
    sessionId: "remote",
    label: "Synthetic peer",
    access: "full-access",
    linked: true,
    remoteOrigin: {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: "sender",
      linkSessionId: "remote",
      thread: { threadId: "origin" },
    },
  },
};
const database = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* core;
    yield* integration;
    yield* review;
    yield* events;
    yield* receipts;
    yield* sql`ALTER TABLE orchestration_command_receipts ADD COLUMN command_type TEXT NOT NULL DEFAULT 'legacy'`;
    yield* syntheticReceiverAuth;
    // Only receiver evidence columns used by this fixture; all data is synthetic.
    yield* sql`CREATE TABLE jones_thread_links(launch_command_id TEXT,kind TEXT,from_environment_id TEXT,
    from_thread_id TEXT,to_environment_id TEXT,to_thread_id TEXT,asserted_by TEXT)`;
    yield* sql`CREATE TABLE synthetic_receiver_shells(id TEXT PRIMARY KEY,shell_json TEXT)`;
    yield* sql`INSERT INTO jones_remote_admission_policy VALUES(1,${encodeJson({
      enabled: true,
      revision: "policy",
      modelSelection: {
        instanceId: ProviderInstanceId.make("synthetic"),
        model: "synthetic",
      },
    })})`;
  }),
).pipe(Layer.provideMerge(Sqlite.layer({ filename: ":memory:" })));
const StoredShell = Schema.Struct({
  id: ThreadId,
  projectId: ProjectId,
  deletedAt: Schema.NullOr(Schema.DateTimeUtcFromString),
  linkOrigin: OrchestrationV2LinkOrigin,
});
const projections = Layer.unwrap(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const getThreadShell = Effect.fn("receiverP1.getThreadShell")(
      function* (id: ThreadId) {
        const row = (yield* sql<{ shell_json: string }>`
          SELECT shell_json FROM synthetic_receiver_shells WHERE id=${id}`)[0];
        if (!row) return null;
        const stored = yield* Schema.decodeEffect(Schema.fromJsonString(StoredShell))(row.shell_json);
        return { ...liveThreadShell(stored.id), ...stored };
      },
      (effect, id) =>
        effect.pipe(
          Effect.mapError((cause) => new Projections.ProjectionStoreReadError({ threadId: id, cause })),
        ),
    );
    return Layer.mock(Projections.ProjectionStoreV2)({ getThreadShell });
  }),
);
const stores = Layer.merge(Receipts.layer, projections);
const evidenceLayer = Layer.effect(
  ReceiverRecoveryEvidence,
  Effect.gen(function* () {
    return {
      read: makeLaunchEvidenceReader(
        yield* SqlClient.SqlClient,
        yield* Receipts.CommandReceiptStoreV2,
        yield* Projections.ProjectionStoreV2,
        () => false,
      ),
      authorize: () => Effect.succeed(true),
      root: (record) => Effect.succeed(originalReturnBinding(record)),
    } satisfies ReceiverRecoveryEvidence["Service"];
  }),
).pipe(Layer.provideMerge(stores));
export const receiverP1Layer = Intake.layer.pipe(
  Layer.provideMerge(
    Admission.layerWithEvaluator(() =>
      Effect.succeed({
        verdict: "hold",
        category: "risky",
        reason: "Synthetic owner review",
      }),
    ),
  ),
  Layer.provideMerge(evidenceLayer),
  Layer.provideMerge(Settings.ServerSettingsService.layerTest()),
  Layer.provide(Layer.mock(Providers.ProviderInstanceRegistry)({ getCatalogSource: () => undefined })),
  Layer.provideMerge(database),
);
export const p1Services = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    admissions = yield* Admission.RemoteAdmission,
    intake = yield* Intake.RemoteAdmissionIntake,
    evidence = yield* ReceiverRecoveryEvidence;
  return {
    sql,
    admissions,
    intake,
    evidence,
    review: makeReviewService(
      sql,
      admissions,
      intake,
      yield* Settings.ServerSettingsService,
      evidence,
      {
        receiverEnvironmentId: "receiver",
        liveDestinations: (ids) => Effect.succeed(ids),
        validLeaseIds: () => Effect.succeed([]),
      },
    ),
    protocol: makePeerProtocol(
      sql,
      admissions,
      makeKeyedLaunchResultReader(
        sql,
        yield* Receipts.CommandReceiptStoreV2,
        yield* Projections.ProjectionStoreV2,
      ),
    ),
  };
});
export const writeP1Creation = Effect.fn("receiverP1.writeCreation")(
  (sql: SqlClient.SqlClient, commandId: string, projectId: string) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,
      event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json)
      VALUES(${commandId},'thread',${commandId},1,'thread.created','2026-11-16T00:00:00.000Z',
      ${commandId},'system',${encodeJson({ id: commandId, projectId })},'{}')`;
        yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,
      result_sequence,status,error,command_type) SELECT ${commandId},'thread',${commandId},
      '2026-11-16T00:00:00.000Z',sequence,'accepted',NULL,'thread.create' FROM orchestration_events WHERE command_id=${commandId}`;
        yield* sql`INSERT INTO synthetic_receiver_shells VALUES(${commandId},${encodeJson({
          id: commandId,
          projectId,
          deletedAt: null,
          linkOrigin: { sessionId: "remote", label: "Synthetic peer" },
        })})`;
        yield* sql`INSERT INTO jones_thread_links VALUES(${commandId},'launch','sender','origin','receiver',${commandId},'link:remote')`;
      }),
    ).pipe(Effect.mapError((cause) => new Intake.RemoteAdmissionIntakeError({ cause }))),
);
