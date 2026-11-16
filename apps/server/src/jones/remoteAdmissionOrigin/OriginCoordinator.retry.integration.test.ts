import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Review from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import { DependencyWakeCommit } from "../threadResultDependencies/DependencyCommit.ts";
import migration from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import * as Coordinator from "./OriginCoordinator.ts";
import * as Bindings from "./OriginPeerBindings.ts";
import * as Delivery from "./OriginDelivery.ts";
import { admitOriginWake, intent, makeOriginStore, type ObservationRow } from "./OriginStore.ts";

const environmentId = EnvironmentId.make("receiver");
const timestamp = "2026-11-16T00:00:00.000Z";
const callerLimits: PeerMcpClient.PeerCallLimits = {
  runtimeMode: "auto-accept-edits",
  interactionMode: "default",
};
const peer: Bindings.OriginPeerBinding = {
  environmentId,
  generation: "generation-1",
  receiverSessionId: "session-1",
  peerLongPoll: false,
  limits: Bindings.protocolLimits,
};
const attempt: Review.RemoteAdmissionRetryAttempt = {
  attemptId: "attempt",
  rootAdmissionId: "root-admission",
  predecessorAdmissionId: "root-admission",
  childAdmissionId: "retry-admission",
  receiverEnvironmentId: environmentId,
  locator: "locator",
  state: "prepared",
  createdAt: timestamp,
  expiresAt: "2026-11-17T00:00:00.000Z",
  grantIntent: null,
};
const launchResult: Review.RemoteAdmissionLaunchResult = {
  outcome: "created",
  stages: {
    creation: "confirmed",
    availability: "available",
    initialMessage: "none",
    preparation: "ready",
    providerProgress: "unknown",
  },
  threadId: "created-thread",
  recordedAt: timestamp,
  reasonCode: "created",
};
const memory = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* migration;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_thread_result_fences (origin_environment_id TEXT, operation_id TEXT, generation INTEGER)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads (thread_id TEXT, archived_at TEXT, deleted_at TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES ('waiter',NULL,NULL)`;
    yield* sql`CREATE TABLE synthetic_wake_receipts (command_id TEXT PRIMARY KEY, message_id TEXT UNIQUE)`;
    const store = makeOriginStore(sql);
    const root: ObservationRow = {
      observation_key: "root",
      peer_environment_id: environmentId,
      link_generation: peer.generation,
      session_id: peer.receiverSessionId,
      client_request_id: "original-key",
      receiver_namespace: "client:session-1",
      kind: "launch",
      root_observation_key: "root",
      predecessor_observation_key: null,
      admission_id: "root-admission",
      attempt_id: null,
      recovered_revision: null,
      return_binding_json: JSON.stringify({
        rootObservationKey: "root",
        originThreadId: "waiter",
        returnRouteKey: "root",
        waiterGeneration: 0,
        waiterRunId: null,
      }),
      intent_json: JSON.stringify({
        phase: "prepared",
        fingerprint: "root-digest",
        registered: true,
        acknowledged: false,
        limits: callerLimits,
      }),
      result_json: null,
      state: "intent",
      created_at: timestamp,
      updated_at: timestamp,
      terminal_at: null,
    };
    yield* sql.withTransaction(store.prepareInTransaction(root));
    yield* store.release("root", timestamp);
    yield* sql.withTransaction(store.prepareInTransaction({
      ...root,
      observation_key: "retry",
      client_request_id: attempt.attemptId,
      receiver_namespace: "admission-retry:root-admission",
      kind: "retry",
      predecessor_observation_key: "root",
      admission_id: attempt.childAdmissionId,
      attempt_id: attempt.attemptId,
      intent_json: JSON.stringify({
        phase: "prepared",
        fingerprint: attempt.attemptId,
        registered: false,
        acknowledged: false,
        limits: callerLimits,
      }),
    }));
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));

const regression = (advanced: "claimed" | "completed", replaceOnReply = false) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOriginStore(sql);
    const lostReply = yield* Deferred.make<void>();
    let statusRead = yield* Deferred.make<void>();
    let remoteState: Review.RemoteAdmissionRetryAttempt["state"] = "prepared";
    let registeredKey: string | null = null;
    let generation = peer.generation;
    let launches = 0;
    let acknowledgments = 0;
    let observations = 0;
    const registrations: Array<{
      arguments: Record<string, unknown>;
      expectedGeneration?: string;
      limits: PeerMcpClient.PeerCallLimits;
    }> = [];
    const peers = PeerMcpClient.PeerMcpClient.of({
      call: (input) => Effect.gen(function* () {
        let response: unknown;
        switch (input.tool) {
          case "orchestrator_capabilities":
            response = {
              parentThreadId: null,
              inheritedProviderInstanceId: null,
              inheritedModel: null,
              runtimeMode: "full-access",
              interactionMode: "default",
              providers: [],
              features: {
                appOwnedSubagents: true,
                asyncPolling: true,
                cancellation: true,
                batchThreadCreation: true,
                threadManagement: true,
                incrementalThreadRead: true,
                scheduledTasks: true,
                maxBatchThreads: 10,
                remoteAdmissionPeerStatusVersion: Review.REMOTE_ADMISSION_PEER_STATUS_VERSION,
                remoteAdmissionRetryObservationVersion: Review.REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION,
                remoteAdmissionCapabilitiesRegisterVersion: Review.REMOTE_ADMISSION_CAPABILITIES_REGISTER_VERSION,
              },
            };
            break;
          case "t3_remote_admission_capabilities_register":
            response = input.arguments;
            break;
          case "t3_remote_admission_attempt_register":
            registrations.push(input);
            expect(input.arguments).toEqual({ attemptId: "attempt", observationKey: "retry" });
            if (registeredKey === null) {
              registeredKey = "retry";
              // External receiver fixture: acceptance permits one claim, then loses its reply.
              remoteState = advanced;
              launches += 1;
              yield* Deferred.succeed(lostReply, undefined);
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Accepted registration reply lost.",
              });
            }
            expect(registeredKey).toBe(input.arguments.observationKey);
            if (replaceOnReply) generation = "replacement";
            response = { attempt: { ...attempt, state: remoteState } };
            break;
          case "t3_remote_admission_peer_status":
            // Claimed/completed retries are absent from the discovery projection.
            response = {
              after: "peer:session-1:1",
              receiverEnvironmentId: environmentId,
              sessionId: "session-1",
              peerLongPoll: false,
              serverNow: timestamp,
              grants: [],
              attempts: remoteState === "prepared" ? [attempt] : [],
              notices: [],
            };
            yield* Deferred.succeed(statusRead, undefined);
            break;
          default:
            return yield* Effect.die(`Unexpected tool: ${input.tool}`);
        }
        return yield* Schema.decodeUnknownEffect(input.success)(response).pipe(
          Effect.mapError(() => new OrchestratorMcpFailure({ code: "orchestration_error", message: "Invalid synthetic peer response." })),
        );
      }),
    });
    const bindings = Layer.succeed(Bindings.OriginPeerBindings, {
      current: () => Effect.sync(() => ({ ...peer, generation })),
      relevant: () => Effect.succeed([peer]),
      observeRetry: (_binding, attemptId, observationKey) => Effect.sync(() => {
        expect(attemptId).toBe("attempt");
        expect(observationKey).toBe(registeredKey);
        remoteState = "completed";
        observations += 1;
        return { admission: null, result: launchResult };
      }),
      ackRetry: (_binding, attemptId, observationKey, disposition) => Effect.sync(() => {
        expect(attemptId).toBe("attempt");
        expect(observationKey).toBe(registeredKey);
        expect(disposition).toBe("delivered");
        acknowledgments += 1;
      }),
    });
    // The external dispatch fixture commits the production wake hook and receipt
    // in one SQL transaction; it does not mock origin delivery state transitions.
    const orchestrator = Layer.mock(Orchestrator.OrchestratorV2)({
      dispatch: (command) => Effect.gen(function* () {
        const commit = yield* DependencyWakeCommit;
        if (!commit || !("kind" in commit) || commit.kind !== "remote_admission_origin")
          return yield* Effect.die("Missing origin wake commit");
        yield* sql.withTransaction(Effect.gen(function* () {
          yield* sql`INSERT INTO synthetic_wake_receipts VALUES (${command.commandId},${commit.messageId})`;
          yield* admitOriginWake(sql, commit);
        })).pipe(Effect.orDie);
        return { sequence: 1, storedEvents: [] };
      }),
    });
    const dependencies = Layer.mergeAll(
      bindings,
      Layer.succeed(PeerMcpClient.PeerMcpClient, peers),
      orchestrator,
      Layer.succeed(Delivery.OriginDelivery, {
        isSending: () => false,
        prepare: () => Effect.die("Unexpected launch preparation"),
        pending: () => Effect.die("Unexpected launch transmission"),
        release: () => Effect.die("Unexpected launch release"),
      }),
      Layer.succeed(SecureRandom.SecureRandom, SecureRandom.make((size) => new Uint8Array(size).fill(7))),
    );
    const round = (completed: Effect.Effect<void>) =>
      Effect.scoped(Effect.gen(function* () {
        const coordinator = yield* Coordinator.OriginCoordinator;
        yield* coordinator.reconcile();
        yield* completed;
      })).pipe(
        Effect.provide(Layer.fresh(Coordinator.layer).pipe(Layer.provide(dependencies))),
      );

    yield* round(Deferred.await(lostReply));
    expect(intent((yield* store.get("retry"))!).registered).toBe(false);
    expect(launches).toBe(1);
    // Rebuild all coordinator-local maps/fibers over the SAME retained SQL rows.
    statusRead = yield* Deferred.make<void>();
    yield* round(Deferred.await(statusRead));
    expect(registrations.length).toBe(2);
    for (const registration of registrations) {
      expect(registration.expectedGeneration).toBe("generation-1");
      expect(registration.limits).toEqual(callerLimits);
    }
    if (replaceOnReply) {
      expect(intent((yield* store.get("retry"))!).registered).toBe(false);
      expect(observations).toBe(0);
      expect(acknowledgments).toBe(0);
      expect((yield* sql`SELECT * FROM synthetic_wake_receipts`).length).toBe(0);
      return;
    }
    const delivered = (yield* store.get("retry"))!;
    expect(delivered.state).toBe("delivered");
    expect(intent(delivered).registered).toBe(true);
    expect(intent(delivered).acknowledged).toBe(true);
    expect(launches).toBe(1);
    expect(observations).toBe(1);
    expect(acknowledgments).toBe(1);
    expect((yield* sql`SELECT * FROM synthetic_wake_receipts`).length).toBe(1);
    // A further reload uses the committed delivery/ack facts, without another wake.
    statusRead = yield* Deferred.make<void>();
    yield* round(Deferred.await(statusRead));
    expect(registrations.length).toBe(2);
    expect(launches).toBe(1);
    expect(acknowledgments).toBe(1);
    expect((yield* sql`SELECT * FROM synthetic_wake_receipts`).length).toBe(1);
  }).pipe(Effect.provide(memory));

for (const state of ["claimed", "completed"] as const) {
  it.effect(
    `reconciles a lost accepted registration after ${state} and origin restart without discovery`,
    () => regression(state),
  );
}
it.effect(
  "replacement during retained retry registration fences local confirmation, wake and ack",
  () => regression("completed", true),
);
