import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as Managed from "../../policy/ManagedPolicy.ts";
import * as Settings from "../../serverSettings.ts";
import * as Generation from "../../textGeneration/TextGeneration.ts";
import * as Config from "../../config.ts";
import * as Providers from "../../provider/ProviderInstanceRegistry.ts";
import core from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import integration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import review from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import * as Changes from "../remoteAdmission/ReviewChanges.ts";
import { RemoteAdmissionIntake } from "./RemoteAdmissionIntake.ts";
import { RemoteAdmissionReviewService } from "./ReviewService.ts";
import { ReceiverPeerProtocol } from "./protocolTools.ts";
import { ReceiverClaimTracker } from "./ClaimTracker.ts";
import { ReceiverRecoveryEvidence } from "./RecoveryStore.ts";
import { runtimeLayer } from "./registration.ts";
import { syntheticReceiverAuth } from "./receiver.testkit.ts";
const memory = Layer.effectDiscard(
  core.pipe(
    Effect.andThen(integration),
    Effect.andThen(review),
    Effect.andThen(syntheticReceiverAuth),
  ),
).pipe(Layer.provideMerge(Sqlite.layer({ filename: ":memory:" })));
const shell = {
  id: "target",
  projectId: "project",
  runtimeMode: "auto",
  interactionMode: "default",
  deletedAt: null,
  linkOrigin: { sessionId: "remote" },
} as unknown as OrchestrationV2ThreadShell;
const dependencies = Layer.mergeAll(
  Settings.ServerSettingsService.layerTest(),
  Layer.mock(Projections.ProjectionStoreV2)({ getThreadShell: () => Effect.succeed(shell) }),
  Layer.mock(Projects.ProjectStoreV2)({
    get: () => Effect.succeedSome({ projectId: "project", deletedAt: null } as Projects.ProjectRow),
  }),
  Layer.mock(Receipts.CommandReceiptStoreV2)({
    getByCommandId: () => Effect.succeedNone,
  }),
  Layer.mock(Threads.ThreadManagementService)({}),
  Layer.mock(Environment.ServerEnvironment)({
    getEnvironmentId: Effect.succeed(EnvironmentId.make("receiver")),
  }),
  Managed.layerTest(),
  Config.layerTest("/synthetic/receiver", { prefix: "admission-runtime-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
  Layer.mock(Providers.ProviderInstanceRegistry)({ getCatalogSource: () => undefined }),
  Layer.mock(Generation.TextGeneration)({
    evaluateRemoteRequest: () =>
      Effect.succeed({ verdict: "pass", category: "none", reason: "Ordinary request" }),
  }),
);
const layer = runtimeLayer.pipe(Layer.provide(dependencies), Layer.provideMerge(memory));
it.effect(
  "composes real receiver adapters once and dispatches with current same-database authority",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient,
        intake = yield* RemoteAdmissionIntake;
      yield* Admission.RemoteAdmission;
      yield* Changes.ReviewChanges;
      yield* ReceiverClaimTracker;
      yield* ReceiverRecoveryEvidence;
      yield* RemoteAdmissionReviewService;
      yield* ReceiverPeerProtocol;
      const policy = {
        enabled: true,
        revision: "policy",
        modelSelection: { instanceId: ProviderInstanceId.make("synthetic"), model: "synthetic" },
      };
      yield* sql`INSERT INTO jones_remote_admission_policy VALUES(1,${JSON.stringify(policy)})`;
      let calls = 0;
      intake.register(
        "t3_thread_send",
        () =>
          Effect.sync(() => {
            calls++;
            return { accepted: true };
          }),
        () => Effect.succeed({ id: "target" }),
      );
      const scope = {
        environmentId: EnvironmentId.make("receiver"),
        capabilities: new Set(["orchestration"] as const),
        issuedAt: 0,
        requestNamespace: "client:remote",
        thread: undefined,
        client: {
          sessionId: "remote",
          label: "Synthetic peer",
          access: "full-access" as const,
          linked: true,
        },
      };
      const input = {
        clientRequestId: "first",
        threadId: "target",
        projectId: "project",
        message: "Synthetic",
      };
      expect(yield* intake.invoke("t3_thread_send", input, scope)).toEqual({ accepted: true });
      expect(calls).toBe(1);
      yield* sql`UPDATE auth_sessions SET revoked_at='2026-11-16' WHERE session_id='remote'`;
      expect(
        (yield* intake
          .invoke("t3_thread_send", { ...input, clientRequestId: "second" }, scope)
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(calls).toBe(1);
    }).pipe(Effect.provide(layer)),
);
