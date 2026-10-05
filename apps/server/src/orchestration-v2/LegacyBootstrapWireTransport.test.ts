import { expect, it } from "vite-plus/test";
import {
  AuthSessionId,
  type AuthEnvironmentScope,
  CommandId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentOrchestrationHttpApi,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ApplicationEvents from "../persistence/Layers/OrchestrationEventStore.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as Threads from "./ThreadManagementService.ts";
import { OrchestratorProjectionError } from "./Orchestrator.ts";
import { LegacyReleaseDecision, RecordedAppThreadJson, RecordedRunJson } from "./RecordedTypes.ts";
import { orchestrationHttpApiLayer } from "./http.ts";

const decodeThread = Schema.decodeUnknownSync(RecordedAppThreadJson);
const decodeRun = Schema.decodeUnknownSync(RecordedRunJson);

it("serves authenticated full, bounded and shell HTTP snapshots from recorded SQL without private correlation", async () => {
  const threadId = ThreadId.make("recorded-http:T");
  const runId = RunId.make("recorded-http:R");
  const projectId = ProjectId.make("recorded-http:P");
  const timestamp = "2026-10-05T00:00:00.000Z";
  const policy = {
    version: 1 as const,
    createCommandId: CommandId.make("recorded-http:B"),
    birthCommandId: CommandId.make("recorded-http:B:initial-message"),
    releaseCommandId: CommandId.make("recorded-http:C"),
    projectId,
    threadId,
    messageId: MessageId.make("recorded-http:M"),
    payloadHash: "private-http-payload-hash",
    ownsNewThread: false,
    runId,
  };
  const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6" };
  const thread = decodeThread({
    id: threadId,
    projectId,
    title: "Ordinary recorded thread",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: timestamp,
    updatedAt: timestamp,
    archivedAt: null,
    deletedAt: null,
    settledAt: null,
    settledOverride: null,
    lastVisitedAt: null,
    legacyBootstrapClaim: policy,
  });
  const run = decodeRun({
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    providerThreadId: null,
    userMessageId: policy.messageId,
    rootNodeId: null,
    activeAttemptId: null,
    status: "preparing",
    requestedAt: timestamp,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    workspaceRunSetupScript: false,
    legacyBootstrap: policy,
    legacyPreparationFailureKnown: false,
  });
  let snapshotReads = 0;
  const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer, Receipts.layer).pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
  );
  const persistence = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));
  const readers = Layer.unwrap(
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      // Codec storage fixture, not an authenticated bootstrap or a fabricated V1 trace.
      yield* sink.write({
        events: [
          {
            id: EventId.make("recorded-http:thread"),
            type: "thread.created",
            threadId,
            occurredAt: thread.createdAt,
            payload: thread,
          },
          {
            id: EventId.make("recorded-http:run"),
            type: "run.created",
            threadId,
            runId,
            occurredAt: run.requestedAt,
            payload: run,
          },
        ],
      });
      const correlation = {
        policy,
        threadId,
        runId,
        claimEventId: EventId.make("recorded-http:claim"),
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: EventId.make("recorded-http:birth"),
        birthSequence: 2,
        birthReceiptSequence: 2,
        preparationGeneration: "private-http-generation",
        workspacePath: "/private-http-workspace",
        projectWorkspaceRoot: "/private-http-workspace",
      };
      const decision = yield* Schema.decodeUnknownEffect(LegacyReleaseDecision)({
        version: 1,
        status: "rejected",
        policy,
        claimEventId: correlation.claimEventId,
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: correlation.birthEventId,
        birthSequence: 2,
        birthReceiptSequence: 2,
        evidenceEventId: EventId.make("recorded-http:C:event"),
        reason: "Private HTTP codec rejection",
        observed: { snapshotSequence: 2, lastEventSequence: 2, target: null },
        guard: {
          observedSnapshotSequence: 0,
          expectedModelSelection: modelSelection,
          expectedSessionStatus: null,
          expectedActiveTurnId: null,
          expectedLatestTurnId: null,
          requireIdle: true,
        },
        deletion: {
          ...correlation,
          version: 1,
          type: "no_control",
          commandId: CommandId.make("recorded-http:D"),
          evidenceEventId: EventId.make("recorded-http:D:event"),
          control: { ...correlation, version: 1, type: "no_control" },
        },
      });
      // This direct recorded DTO fixture tests authenticated serving, not D authority or a V1 trace.
      const payload = yield* Schema.encodeEffect(Schema.fromJsonString(RecordedRunJson))({
        ...run,
        legacyReleaseDecision: decision,
      });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${payload} WHERE run_id = ${runId}`;
      const mapError = (cause: unknown) => new OrchestratorProjectionError({ threadId, cause });
      return Layer.mock(Threads.ThreadManagementService)({
        getThreadSnapshot: (id) =>
          projections.getThreadSnapshot(id).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                snapshotReads++;
              }),
            ),
            Effect.mapError(mapError),
          ),
        getThreadSnapshotWindow: (id, options) =>
          projections.getThreadSnapshotWindow(id, options).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                snapshotReads++;
              }),
            ),
            Effect.mapError(mapError),
          ),
        getShellSnapshot: (options) =>
          projections.getShellSnapshot(options).pipe(Effect.mapError(mapError)),
      });
    }),
  ).pipe(Layer.provideMerge(persistence));
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.headers.authorization !== "Bearer fixture")
        return yield* failEnvironmentAuthInvalid("missing_credential");
      return yield* effect.pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, {
          sessionId: AuthSessionId.make("recorded-http-auth"),
          subject: "fixture",
          method: "bearer-access-token",
          scopes: new Set<AuthEnvironmentScope>(["orchestration:read"]),
        }),
      );
    }),
  );
  const dependencies = Layer.mergeAll(
    readers,
    Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
    Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
    ApplicationEvents.OrchestrationEventStoreLive.pipe(Layer.provide(SqlitePersistenceMemory)),
  );
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentOrchestrationHttpApi),
  ).pipe(
    Layer.provide(orchestrationHttpApiLayer.pipe(Layer.provide(dependencies))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  const http = HttpRouter.toWebHandler(routes, { disableLogger: true });
  try {
    const fullUrl = `http://test/api/orchestration/threads/${threadId}`;
    expect((await http.handler(new Request(fullUrl), Context.empty())).status).toBe(401);
    expect(snapshotReads).toBe(0);
    for (const url of [fullUrl, `${fullUrl}/bounded`, "http://test/api/orchestration/shell"]) {
      const response = await http.handler(
        new Request(url, {
          headers: {
            authorization: "Bearer fixture",
            [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
          },
        }),
        Context.empty(),
      );
      expect(response.status).toBe(200);
      const serialized = await response.text();
      expect(serialized).toContain(threadId);
      expect(serialized).not.toContain("legacyBootstrap");
      expect(serialized).not.toContain("legacyPreparation");
      expect(serialized).not.toContain("legacyReleaseDecision");
      expect(serialized).not.toContain("workspaceRunSetupScript");
      expect(serialized).not.toContain(policy.payloadHash);
      expect(serialized).not.toContain("private-http-workspace");
      expect(serialized).not.toContain("private-http-generation");
      expect(serialized).toContain("Ordinary recorded thread");
    }
    expect(snapshotReads).toBe(2);
  } finally {
    await http.dispose();
  }
});
