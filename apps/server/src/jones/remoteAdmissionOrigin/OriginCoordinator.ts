import {
  CommandId,
  MessageId,
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpFailure,
  ThreadId,
} from "@t3tools/contracts";
import * as Review from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as PeerMcpClient from "../../peer/PeerMcpClient.ts";
import { DependencyWakeCommit } from "../threadResultDependencies/DependencyCommit.ts";
import * as Bindings from "./OriginPeerBindings.ts";
import * as Delivery from "./OriginDelivery.ts";
import {
  decodeReturnBinding,
  encodeIntent,
  encodeLaunchResult,
  intent,
  makeOriginStore,
  returnBinding,
  type ObservationRow,
} from "./OriginStore.ts";

const encodeGrant = Schema.encodeSync(Schema.fromJsonString(Review.RemoteAdmissionGrant));
const decodeLaunchResult = Schema.decodeEffect(
  Schema.fromJsonString(Review.RemoteAdmissionLaunchResult),
);
const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);

export const FALLBACK_MS = 600_000;
export const STALE_MS = 900_000;
export const cacheIsStale = (observedAt: string, now: number) =>
  now - Date.parse(observedAt) >= STALE_MS;
export class OriginCoordinator extends Context.Service<
  OriginCoordinator,
  {
    /** Parent calls on startup, intent change and reconnect; repeated calls coalesce. */
    readonly reconcile: () => Effect.Effect<
      void,
      OrchestratorMcpFailure,
      import("effect/Scope").Scope
    >;
  }
>()("t3/jones/remoteAdmissionOrigin/OriginCoordinator") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const bindings = yield* Bindings.OriginPeerBindings;
  const delivery = yield* Delivery.OriginDelivery;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const random = yield* SecureRandom.SecureRandom;
  const store = makeOriginStore(sql);
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  // A paused generation stays in this map until replacement, including reconnects.
  const running = new Map<
    string,
    { generation: string; wakeup: Queue.Queue<void>; fiber?: Fiber.Fiber<void> }
  >();
  const current = Effect.fn("OriginCoordinator.current")(function* (
    binding: Bindings.OriginPeerBinding,
  ) {
    const latest = yield* bindings.current(binding.environmentId);
    return (
      latest?.generation === binding.generation &&
      latest.receiverSessionId === binding.receiverSessionId
    );
  });
  const call = <S extends Schema.Decoder<unknown>>(
    binding: Bindings.OriginPeerBinding,
    tool: string,
    args: Record<string, unknown>,
    success: S,
  ) =>
    peers.call({
      environmentId: binding.environmentId,
      expectedGeneration: binding.generation,
      limits: binding.limits,
      tool,
      arguments: args,
      success,
    });
  const wake = Effect.fn("OriginCoordinator.wake")(function* (row: ObservationRow) {
    if (row.state !== "resolved" || !intent(row).registered || row.result_json === null) return;
    if (yield* sql.withTransaction(store.suppressIfStoppedInTransaction(row, yield* now))) return;
    const result = yield* decodeLaunchResult(row.result_json);
    const binding = returnBinding(row);
    const messageId = MessageId.make(yield* random.uuidV4());
    const summary = `Linked launch result: ${result.outcome}`;
    const detail = `Data from linked environment ${row.peer_environment_id}. Creation: ${result.stages.creation}; availability: ${result.stages.availability}; initial message: ${result.stages.initialMessage}; provider: ${result.stages.providerProgress}. Admission: ${row.admission_id ?? "unavailable"}. Confirmed created thread: ${result.stages.creation === "confirmed" ? (result.threadId ?? "unavailable") : "unconfirmed"}.`;
    yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(yield* random.uuidV4()),
        messageId,
        threadId: ThreadId.make(binding.originThreadId),
        text: `${summary}\n\n${detail}`,
        notification: {
          source: { kind: "background_task" },
          outcome:
            result.outcome === "created"
              ? "completed"
              : result.outcome === "unknown"
                ? "unknown"
                : "failed",
          summary,
          detail,
        },
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      })
      .pipe(
        Effect.provideService(DependencyWakeCommit, {
          kind: "remote_admission_origin",
          dependencyId: row.observation_key,
          observationKey: row.observation_key,
          generation: binding.waiterGeneration,
          waiterThreadId: ThreadId.make(binding.originThreadId),
          messageId,
          linkGeneration: row.link_generation,
          sessionId: row.session_id,
          committedAt: yield* now,
        }),
      );
  });
  const registerRetry = Effect.fn("OriginCoordinator.registerRetry")(function* (
    peer: Bindings.OriginPeerBinding,
    row: ObservationRow,
  ) {
    if (intent(row).registered) return true;
    if (
      row.kind !== "retry" ||
      row.attempt_id === null ||
      row.peer_environment_id !== peer.environmentId ||
      row.link_generation !== peer.generation ||
      row.session_id !== peer.receiverSessionId ||
      !(yield* current(peer))
    )
      return false;
    // A retained observation is sufficient to reconcile this idempotent call.
    // Claimed/completed attempts no longer appear in the discovery projection.
    yield* call(
      { ...peer, limits: intent(row).limits ?? Bindings.protocolLimits },
      "t3_remote_admission_attempt_register",
      { attemptId: row.attempt_id, observationKey: row.observation_key },
      Review.RemoteAdmissionAttemptRegisterResult,
    );
    if (!(yield* current(peer))) return false;
    yield* sql.withTransaction(
      store.updateIntent(row.observation_key, { registered: true }, yield* now),
    );
    return true;
  });
  const follow = Effect.fn("OriginCoordinator.follow")(function* (
    peer: Bindings.OriginPeerBinding,
    row: ObservationRow,
  ) {
    if (delivery.isSending(row.observation_key)) return;
    if (row.kind === "retry" && !(yield* registerRetry(peer, row))) return;
    if (row.state === "intent" || row.state === "pending") {
      const observed =
        row.kind === "retry"
          ? yield* bindings.observeRetry(peer, row.attempt_id!, row.observation_key)
          : yield* call(
              peer,
              "t3_remote_admission_observe",
              { clientRequestId: row.client_request_id },
              Review.RemoteAdmissionObservation,
            );
      if (!(yield* current(peer))) return;
      if (observed.admission)
        yield* sql`UPDATE jones_remote_origin_observations SET admission_id = COALESCE(admission_id,${observed.admission.id}) WHERE observation_key = ${row.observation_key}`;
      if (observed.result)
        yield* store.resolve(
          row.observation_key,
          observed.result,
          observed.admission?.id ?? null,
          yield* now,
        );
    }
    const fresh = yield* store.get(row.observation_key);
    if (!fresh) return;
    yield* wake(fresh);
    const committed = yield* store.get(row.observation_key);
    if (
      !committed ||
      intent(committed).acknowledged ||
      !["delivered", "suppressed"].includes(committed.state)
    )
      return;
    const disposition = committed.state === "delivered" ? "delivered" : "suppressed";
    if (committed.kind === "recovered_notice")
      yield* call(
        peer,
        "t3_remote_admission_notice_ack",
        {
          receiverEnvironmentId: peer.environmentId,
          admissionId: committed.admission_id,
          recoveredRevision: committed.recovered_revision,
          disposition,
        },
        Review.RemoteAdmissionAckResult,
      );
    else if (committed.kind === "retry")
      yield* bindings.ackRetry(peer, committed.attempt_id!, committed.observation_key, disposition);
    else
      yield* call(
        peer,
        "t3_remote_admission_ack",
        { clientRequestId: committed.client_request_id, disposition },
        Review.RemoteAdmissionAckResult,
      );
    if (yield* current(peer))
      yield* sql.withTransaction(
        store.updateIntent(committed.observation_key, { acknowledged: true }, yield* now),
      );
  });
  const discover = Effect.fn("OriginCoordinator.discover")(function* (
    peer: Bindings.OriginPeerBinding,
    status: Review.RemoteAdmissionPeerStatus,
    rows: ReadonlyArray<ObservationRow>,
  ) {
    // Inline success/refusal released its waiter without a wake. It may not
    // have learned the receiver admission ID. Retry discovery fills only that
    // missing correlation via the original pure lookup; it never replays launch.
    if (
      status.attempts.some(
        (attempt) =>
          !rows.some(
            (row) => row.kind === "launch" && row.admission_id === attempt.rootAdmissionId,
          ),
      )
    ) {
      for (const root of rows.filter(
        (row) =>
          row.kind === "launch" &&
          row.admission_id === null &&
          !delivery.isSending(row.observation_key),
      )) {
        const observed = yield* call(
          peer,
          "t3_remote_admission_observe",
          { clientRequestId: root.client_request_id },
          Review.RemoteAdmissionObservation,
        );
        if (!(yield* current(peer))) return;
        if (observed.admission) {
          yield* sql`UPDATE jones_remote_origin_observations SET admission_id = ${observed.admission.id} WHERE observation_key = ${root.observation_key} AND admission_id IS NULL`;
          root.admission_id = observed.admission.id;
        }
      }
    }
    for (const attempt of status.attempts) {
      if (!(yield* current(peer))) return;
      if (attempt.receiverEnvironmentId !== peer.environmentId) continue;
      const key = `retry:${peer.environmentId}:${attempt.attemptId}`;
      const existing = yield* store.get(key);
      // The register reply can be lost after the receiver advanced. Reconcile
      // our existing durable identity instead of waiting for "prepared" again.
      if (attempt.state !== "prepared" && (!existing || intent(existing).registered)) continue;
      const root = rows.find(
        (row) => row.kind === "launch" && row.admission_id === attempt.rootAdmissionId,
      );
      const predecessor = rows.find((row) => row.admission_id === attempt.predecessorAdmissionId);
      if (!root || !predecessor || !(yield* store.anchor(root.root_observation_key))) continue;
      const timestamp = yield* now;
      const row = yield* sql.withTransaction(
        store.prepareInTransaction({
          ...root,
          observation_key: key,
          kind: "retry",
          root_observation_key: root.root_observation_key,
          predecessor_observation_key: predecessor.observation_key,
          client_request_id: attempt.attemptId,
          receiver_namespace: `admission-retry:${attempt.predecessorAdmissionId}`,
          admission_id: attempt.childAdmissionId,
          attempt_id: attempt.attemptId,
          result_json: null,
          state: "intent",
          created_at: timestamp,
          updated_at: timestamp,
          terminal_at: null,
          intent_json: encodeIntent({
            phase: "prepared",
            fingerprint: attempt.attemptId,
            registered: false,
            acknowledged: false,
            limits: intent(root).limits ?? Bindings.protocolLimits,
          }),
        }),
      );
      yield* registerRetry(peer, row);
    }
    for (const notice of status.notices) {
      if (!(yield* current(peer))) return;
      if (notice.receiverEnvironmentId !== peer.environmentId) continue;
      const anchor = yield* store.anchor(notice.returnBinding.rootObservationKey);
      if (
        !anchor ||
        anchor.session_id !== peer.receiverSessionId ||
        anchor.peer_environment_id !== peer.environmentId
      )
        continue;
      const binding = decodeReturnBinding(anchor.return_binding_json);
      if (
        binding.originThreadId !== notice.returnBinding.originThreadId ||
        binding.returnRouteKey !== notice.returnBinding.returnRouteKey
      )
        continue;
      const key = `notice:${peer.environmentId}:${notice.admissionId}:${notice.recoveredRevision}`;
      const timestamp = yield* now;
      const row = yield* sql.withTransaction(
        store.prepareInTransaction({
          observation_key: key,
          peer_environment_id: peer.environmentId,
          link_generation: peer.generation,
          session_id: peer.receiverSessionId,
          client_request_id: key,
          receiver_namespace: `client:${peer.receiverSessionId}`,
          kind: "recovered_notice",
          root_observation_key: binding.rootObservationKey,
          predecessor_observation_key: null,
          admission_id: notice.admissionId,
          attempt_id: null,
          recovered_revision: notice.recoveredRevision,
          return_binding_json: anchor.return_binding_json,
          result_json: encodeLaunchResult(notice.result),
          state: "resolved",
          intent_json: encodeIntent({
            phase: "prepared",
            fingerprint: notice.evidenceDigest,
            registered: false,
            acknowledged: false,
          }),
          created_at: timestamp,
          updated_at: timestamp,
          terminal_at: null,
        }),
      );
      if (!intent(row).registered) {
        yield* call(
          peer,
          "t3_remote_admission_notice_register",
          {
            receiverEnvironmentId: peer.environmentId,
            admissionId: notice.admissionId,
            recoveredRevision: notice.recoveredRevision,
            observationKey: key,
          },
          Review.RemoteAdmissionNoticeRegisterResult,
        );
        if (yield* current(peer))
          yield* sql.withTransaction(store.updateIntent(key, { registered: true }, timestamp));
      }
    }
  });
  const run = Effect.fn("OriginCoordinator.run")(function* (
    peer: Bindings.OriginPeerBinding,
    wakeup: Queue.Queue<void>,
  ) {
    const capabilities = yield* call(
      peer,
      "orchestrator_capabilities",
      {},
      OrchestratorMcpCapabilitiesResult,
    );
    const features = capabilities.features;
    const projection =
      features.remoteAdmissionPeerStatusVersion === Review.REMOTE_ADMISSION_PEER_STATUS_VERSION;
    const registration =
      features.remoteAdmissionCapabilitiesRegisterVersion ===
      Review.REMOTE_ADMISSION_CAPABILITIES_REGISTER_VERSION;
    const retry =
      features.remoteAdmissionRetryObservationVersion ===
      Review.REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION;
    const recovered =
      features.remoteAdmissionRecoveredResultObservationVersion ===
      Review.REMOTE_ADMISSION_RECOVERED_RESULT_VERSION;
    // Long-poll support is independent: the parent negotiates it through this binding.
    const longPoll = projection && peer.peerLongPoll === true;
    if (registration)
      yield* call(
        peer,
        "t3_remote_admission_capabilities_register",
        {
          coordinatorGeneration: peer.generation,
          versions: {
            ...(projection ? { peerProjection: Review.REMOTE_ADMISSION_PEER_STATUS_VERSION } : {}),
            ...(longPoll ? { peerLongPoll: Review.REMOTE_ADMISSION_PEER_STATUS_VERSION } : {}),
            ...(retry
              ? { retryObservation: Review.REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION }
              : {}),
            ...(recovered
              ? { recoveredResultObservation: Review.REMOTE_ADMISSION_RECOVERED_RESULT_VERSION }
              : {}),
            ...(features.remoteAdmissionRecoveryStatusVersion ===
            Review.REMOTE_ADMISSION_RECOVERY_STATUS_VERSION
              ? { recoveryInspection: Review.REMOTE_ADMISSION_RECOVERY_STATUS_VERSION }
              : {}),
          },
        },
        Review.RemoteAdmissionCapabilitiesRegisterResult,
      );
    let after: string | undefined;
    let ordinal = (yield* sql<{
      ordinal: number;
    }>`SELECT COALESCE(MAX(ordinal),0) AS ordinal FROM jones_remote_grant_status_cache WHERE peer_environment_id = ${peer.environmentId} AND link_generation = ${peer.generation}`)[0]!
      .ordinal;
    while (yield* current(peer)) {
      const rows = (yield* store.list()).filter(
        (row) =>
          row.peer_environment_id === peer.environmentId &&
          row.session_id === peer.receiverSessionId &&
          row.link_generation === peer.generation,
      );
      const clock = Date.parse(yield* now);
      let relevant = rows.some(
        (row) =>
          ["intent", "pending", "resolved"].includes(row.state) ||
          (["delivered", "suppressed"].includes(row.state) && !intent(row).acknowledged) ||
          (row.result_json !== null && Date.parse(row.updated_at) > clock - 30 * 86400_000),
      );
      // Resume registration from local durable intent before discovery (including
      // a long poll). A lost accepted reply must not depend on re-listing a child.
      for (const row of rows) {
        if (row.kind === "retry" && !intent(row).registered) yield* follow(peer, row);
      }
      if (projection) {
        const status = yield* call(
          peer,
          "t3_remote_admission_peer_status",
          { ...(after ? { after } : {}), timeoutMs: longPoll ? 50_000 : 0 },
          Review.RemoteAdmissionPeerStatus,
        );
        if (
          !(yield* current(peer)) ||
          status.receiverEnvironmentId !== peer.environmentId ||
          status.sessionId !== peer.receiverSessionId
        )
          return;
        after = status.after;
        ordinal += 1;
        const timestamp = yield* now;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            for (const grant of status.grants) {
              if (
                grant.binding.receiverEnvironmentId !== peer.environmentId ||
                grant.binding.sessionId !== peer.receiverSessionId
              )
                continue;
              relevant ||=
                grant.state !== "revoked" ||
                Date.parse(grant.revokedAt ?? grant.expiresAt) >
                  Date.parse(timestamp) - 30 * 86400_000;
              yield* sql`INSERT INTO jones_remote_grant_status_cache (peer_environment_id,link_generation,grant_id,ordinal,status_json,observed_at)
              VALUES (${peer.environmentId},${peer.generation},${grant.grantId},${ordinal},${encodeGrant(grant)},${timestamp})
              ON CONFLICT(peer_environment_id,link_generation,grant_id) DO UPDATE SET ordinal = excluded.ordinal, status_json = excluded.status_json, observed_at = excluded.observed_at`;
            }
          }),
        );
        yield* discover(
          peer,
          {
            ...status,
            attempts: retry && registration ? status.attempts : [],
            notices: recovered && registration ? status.notices : [],
          },
          rows,
        );
      }
      const pendingRows = (yield* store.list()).filter(
        (row) =>
          row.peer_environment_id === peer.environmentId &&
          row.session_id === peer.receiverSessionId &&
          row.link_generation === peer.generation,
      );
      relevant ||= pendingRows.some((row) => ["intent", "pending", "resolved"].includes(row.state));
      for (const row of pendingRows) {
        if (!(yield* current(peer))) return;
        yield* follow(peer, row);
      }
      if (!relevant) return;
      yield* Effect.race(Effect.sleep(longPoll ? 1_000 : FALLBACK_MS), Queue.take(wakeup));
    }
  });
  const reconcile = Effect.fn("OriginCoordinator.reconcile")(
    function* () {
      if (!(yield* store.available)) return;
      for (const peer of yield* bindings.relevant()) {
        const retained = yield* store.list();
        for (const row of retained) {
          if (
            row.peer_environment_id !== peer.environmentId ||
            row.link_generation === peer.generation ||
            !["intent", "pending"].includes(row.state) ||
            delivery.isSending(row.observation_key)
          )
            continue;
          const timestamp = yield* now;
          yield* store.resolve(
            row.observation_key,
            {
              outcome: "unknown",
              stages: {
                creation: "unknown",
                availability: "unknown",
                initialMessage: "unconfirmed",
                preparation: "unknown",
                providerProgress: "unknown",
              },
              threadId: null,
              recordedAt: timestamp,
              reasonCode: "link_replaced",
            },
            row.admission_id,
            timestamp,
          );
          const resolved = yield* store.get(row.observation_key);
          if (resolved) yield* wake(resolved);
        }
        const old = running.get(peer.environmentId);
        if (old?.generation === peer.generation) {
          yield* Queue.offer(old.wakeup, undefined);
          continue;
        }
        if (old?.fiber) yield* Fiber.interrupt(old.fiber);
        const entry: { generation: string; wakeup: Queue.Queue<void>; fiber?: Fiber.Fiber<void> } =
          { generation: peer.generation, wakeup: yield* Queue.sliding<void>(1) };
        running.set(peer.environmentId, entry);
        entry.fiber = yield* run(peer, entry.wakeup).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              if (running.get(peer.environmentId) === entry) running.delete(peer.environmentId);
            }),
          ),
          Effect.catch((error) =>
            Effect.gen(function* () {
              yield* Effect.logWarning(
                "Origin launch follower paused or deferred; durable intent retained.",
                { environmentId: peer.environmentId },
              );
              // Auth failures pause this exact generation. No refresh/re-pair/admin path.
              if (isOrchestratorMcpFailure(error) && ["capability_denied"].includes(error.code))
                return;
              if (running.get(peer.environmentId) === entry) running.delete(peer.environmentId);
            }),
          ),
          Effect.forkScoped,
        );
      }
    },
    Effect.mapError(
      () =>
        new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "Launch observation coordination is unavailable.",
        }),
    ),
  );
  return OriginCoordinator.of({ reconcile });
});
export const layer = Layer.effect(OriginCoordinator, make);
