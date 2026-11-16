import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  EventId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  OrchestrationV2AppThread,
  OrchestrationV2ShellSnapshot,
  OrchestrationV2Subagent,
  ThreadId,
} from "@t3tools/contracts";
import { expect } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import { EventSinkV2 } from "../../orchestration-v2/EventSink.ts";
import * as OrchestrationHttp from "../../orchestration-v2/http.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import {
  ProjectEnrichmentService,
  type ProjectEnrichmentChange,
} from "../../project/ProjectEnrichmentService.ts";
import * as ServerSettings from "../../serverSettings.ts";

import * as ProjectService from "../../project/ProjectService.ts";
import * as ShellMaterialization from "../orchestration/shellMaterialization.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";

class RetentionTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.orchestration,
) {}

export const now = DateTime.makeUnsafe("2026-10-27T04:00:00.000Z");
export const environmentId = EnvironmentId.make("retention-origin");
export const parent = Schema.decodeSync(OrchestrationV2AppThread)({
  id: "older-remote-owner",
  projectId: "retention-project",
  title: "Older settled parent of remote work",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-6.1-sol" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: {
    rootThreadId: "older-remote-owner",
    parentThreadId: null,
    relationshipToParent: null,
  },
  forkedFrom: null,
  createdBy: "user",
  creationSource: "web",
  createdAt: DateTime.makeUnsafe("2026-09-30T00:00:00.000Z"),
  updatedAt: now,
  archivedAt: null,
  settledOverride: "settled",
  settledAt: DateTime.makeUnsafe("2026-10-01T00:00:00.000Z"),
  lastVisitedAt: null,
  deletedAt: null,
});
export const remoteTask = Schema.decodeSync(OrchestrationV2Subagent)({
  id: "completed-remote-task",
  threadId: parent.id,
  runId: null,
  parentNodeId: "remote-owner-node",
  origin: "app_owned",
  createdBy: "agent",
  driver: "remote",
  providerInstanceId: "codex",
  providerThreadId: null,
  childThreadId: null,
  nativeTaskRef: null,
  prompt: "Synthetic completed delegated work",
  title: "Remote task",
  model: "gpt-6.1-sol",
  modelSelection: { instanceId: "codex", model: "gpt-6.1-sol" },
  status: "completed",
  result: "Done",
  remoteChild: {
    environmentId: "retention-destination",
    threadId: "remote-task-thread",
    label: "Other synthetic machine",
  },
  startedAt: parent.createdAt,
  completedAt: parent.settledAt,
  updatedAt: now,
});
const decodeSnapshot = Schema.decodeUnknownEffect(Schema.toCodecJson(OrchestrationV2ShellSnapshot));

// Real events, projection stores, and HTTP snapshot reduction. No hand-inserted
// parent shell or mocked retention lookup can make this regression pass.
export const createRetentionHarness = (
  options: {
    readonly nestedOwner?: boolean;
    readonly bind?: boolean;
  } = {},
) =>
  Effect.gen(function* () {
    const nestedOwner = options.nestedOwner === true;
    const runtime = ProviderReplayHarness.layerWithRegistry(
      { name: "remote-owner-shell-retention" },
      ProviderAdapterRegistry.layerFromAdapters([]),
      { runEffectWorker: false },
    );
    const services = yield* Layer.build(
      Layer.mergeAll(
        runtime,
        ThreadManagementService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(runtime, CommandReceiptStore.layer, ServerSettings.layerTest({})),
          ),
        ),
        OrchestrationEventStore.layer,
        ProjectStore.layer,
        ProjectionStore.layer,
      ).pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
    );
    const eventSink = Context.get(services, EventSinkV2);
    const middle = {
      ...parent,
      id: ThreadId.make("local-middle"),
      lineage: {
        rootThreadId: parent.id,
        parentThreadId: parent.id,
        relationshipToParent: "subagent" as const,
      },
      settledOverride: null,
      settledAt: null,
    };
    const leaf = {
      ...middle,
      id: ThreadId.make("local-remote-owner"),
      lineage: { ...middle.lineage, parentThreadId: middle.id },
    };
    const sibling = { ...leaf, id: ThreadId.make("unrelated-idle-sibling") };
    yield* eventSink.write({
      events: [
        parent,
        ...Array.from({ length: 26 }, (_, index) => {
          const id = ThreadId.make(`recent-settled-${index}`);
          return {
            ...parent,
            id,
            lineage: { ...parent.lineage, rootThreadId: id },
            settledAt: DateTime.makeUnsafe(
              `2026-10-${String(2 + index).padStart(2, "0")}T00:00:00.000Z`,
            ),
          };
        }),
        ...(nestedOwner ? [middle, leaf, sibling] : []),
      ].map((thread) => ({
        id: EventId.make(`create-${thread.id}`),
        type: "thread.created" as const,
        threadId: thread.id,
        occurredAt: now,
        payload: thread,
      })),
    });
    if (options.bind !== false)
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("complete-remote-task"),
            type: "subagent.updated",
            threadId: nestedOwner ? leaf.id : parent.id,
            occurredAt: now,
            payload: { ...remoteTask, threadId: nestedOwner ? leaf.id : parent.id },
          },
        ],
      });
    const routes = HttpApiBuilder.layer(RetentionTestApi).pipe(
      Layer.provide(OrchestrationHttp.layer),
      Layer.provide(
        Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(environmentId),
        }),
      ),
      Layer.provide(
        Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
          effect.pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              sessionId: AuthSessionId.make("retention-test-session"),
              subject: "synthetic-test",
              method: "bearer-access-token",
              scopes: new Set(["orchestration:read"] as const),
            }),
          ),
        ),
      ),
      Layer.provide(Layer.succeedContext(services)),
      Layer.provide(Layer.mock(ProjectEnrichmentService)({})),
      Layer.provide(HttpPlatform.layer),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    );
    const { handler, dispose } = HttpRouter.toWebHandler(routes, { disableLogger: true });
    yield* Effect.addFinalizer(() => Effect.promise(dispose));
    const httpResponse = (query = "") =>
      Effect.promise(() =>
        handler(
          new Request(`http://localhost/api/orchestration/shell${query}`, {
            headers: { [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT },
          }),
        ),
      );
    const http = (query = "") =>
      Effect.gen(function* () {
        const response = yield* httpResponse(query);
        expect(response.status).toBe(200);
        return yield* decodeSnapshot(yield* Effect.promise(() => response.json()));
      });
    const enrichmentBus = yield* PubSub.unbounded<ProjectEnrichmentChange>();
    const enrichment = Layer.mock(ProjectEnrichmentService)({
      subscribeChanges: PubSub.subscribe(enrichmentBus),
    });
    const wsServices = yield* Layer.build(
      Layer.mergeAll(
        ShellMaterialization.layer,
        enrichment,
        Layer.mock(ProjectService.ProjectService)({}),
      ).pipe(Layer.provideMerge(Layer.succeedContext(services))),
    );
    const withWs = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(Effect.provide(wsServices));
    const ws = (input: Parameters<typeof subscribeOrchestrationV2Shell>[0] = {}) =>
      withWs(
        Effect.scoped(
          Effect.gen(function* () {
            const stream = yield* subscribeOrchestrationV2Shell(input);
            const items = yield* Stream.runCollect(stream.pipe(Stream.take(1)));
            const item = Array.from(items)[0];
            if (item?.kind !== "snapshot")
              return yield* Effect.die("expected first shell snapshot");
            return item.snapshot;
          }),
        ),
      );
    return {
      http,
      httpResponse,
      ws,
      withWs,
      services,
      eventSink,
      sql: Context.get(services, SqlClient.SqlClient),
      projections: Context.get(services, ProjectionStore.ProjectionStoreV2),
      owner: nestedOwner ? leaf : parent,
      middle,
      leaf,
      sibling,
    };
  });
