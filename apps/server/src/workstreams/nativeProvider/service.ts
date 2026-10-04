// @effect-diagnostics nodeBuiltinImport:off -- SHA-256 binds content-free protocol evidence and immutable request bytes.
import * as NodeCrypto from "node:crypto";
import packageJson from "../../../package.json" with { type: "json" };
import {
  WORKSTREAMS_T3_PROVIDER_PROTOCOL,
  WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS,
  WorkstreamsNativeBuild,
  WorkstreamsNativeContext,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeAttestationRequest,
  type WorkstreamsNativeContextResponse,
  type WorkstreamsNativeAttestationResponse,
  type WorkstreamsNativeSettlementResponse,
  type WorkstreamsNativeSettlementEvent,
} from "@t3tools/contracts";
import {
  AuthSessionId,
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  NativeCommandIdentityV2,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { NativeStoreAuthority } from "../../environment/NativeStoreAuthority.ts";
import { NativeStoreAuthorityPersistenceError } from "../../environment/nativeStoreAuthorityPersistence.ts";
import type { EventSinkV2Shape, NativeCommandFactsV2 } from "../../orchestration-v2/EventSink.ts";
import type { OrchestratorV2Shape } from "../../orchestration-v2/Orchestrator.ts";
import type { ProjectionStoreV2Error } from "../../orchestration-v2/ProjectionStore.ts";
import { type NativeProviderAttempts, type NativeProviderAttempt } from "./attemptRepository.ts";
import {
  NativeProviderBuild,
  NATIVE_PROVIDER_SCOPES,
  type NativeProviderEnrollmentBinding,
} from "./enrollment.ts";

export const sha256Bytes = (bytes: string | Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");

export const readNativeProviderBuild = Schema.decodeUnknownEffect(WorkstreamsNativeBuild)(
  (packageJson as { readonly jonesSource?: unknown }).jonesSource,
).pipe(Effect.option);

export const NativeProviderBuildLive = Layer.succeed(NativeProviderBuild, {
  readCurrent: readNativeProviderBuild,
});

export interface NativeProviderPorts {
  readonly authority: Pick<NativeStoreAuthority["Service"], "readCurrent">;
  readonly threadExists: (threadId: ThreadId) => Effect.Effect<boolean, ProjectionStoreV2Error>;
  readonly orchestrator: Pick<OrchestratorV2Shape, "dispatchNativeWorkstreamSettlement"> &
    Partial<Pick<OrchestratorV2Shape, "observeNativeWorkstreamSettlementBinding">>;
  readonly eventSink: Pick<EventSinkV2Shape, "readNativeCommandFacts" | "getThreadIncarnation">;
  readonly attempts: NativeProviderAttempts["Service"];
  readonly build?: Effect.Effect<Option.Option<WorkstreamsNativeBuild>>;
}

type Rejection = Extract<WorkstreamsNativeSettlementResponse, { state: "rejected" }>;
const reject = (reason: Rejection["reason"]): Rejection => ({
  protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
  state: "rejected",
  reason,
});
const unknownSettlement = (
  request: WorkstreamsNativeSettlementRequest,
  reason: Extract<WorkstreamsNativeSettlementResponse, { state: "unknown" }>["reason"],
): WorkstreamsNativeSettlementResponse => ({
  protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
  state: "unknown",
  request,
  reason,
});
const sameAuthority = (
  binding: NativeProviderEnrollmentBinding,
  tuple: NativeStoreAuthority["Service"]["readCurrent"] extends Effect.Effect<infer A, infer _E>
    ? A
    : never,
) =>
  tuple.environmentId === binding.source_instance_id &&
  tuple.authorityNamespace === binding.authority_namespace &&
  tuple.storeGeneration === binding.store_generation;

const association = (request: WorkstreamsNativeSettlementRequest) => ({
  owner_id: request.owner_id,
  principal_id: request.principal_id,
  command_id: request.command_id,
  native_reference_id: request.native_reference_id,
  native_action: request.native_action,
  request_sha256: request.request_sha256,
  reservation_sha256: request.reservation_sha256,
});

const CanonicalResultDigest = Schema.fromJsonString(
  Schema.Struct({
    command_id: Schema.String,
    native_action: Schema.String,
    native_outcome: Schema.String,
    native_reference_id: Schema.String,
    owner_id: Schema.String,
    principal_id: Schema.String,
    request_sha256: Schema.String,
    reservation_sha256: Schema.String,
  }),
);

// All association values are scalars; sorted schema fields match the registry's canonical JSON.
export const nativeProviderResultEvidence = Effect.fn("NativeProvider.resultEvidence")(function* (
  request: WorkstreamsNativeSettlementRequest,
  outcome: "committed" | "denied",
) {
  const value = association(request);
  const encoded = yield* Schema.encodeEffect(CanonicalResultDigest)({
    command_id: value.command_id,
    native_action: value.native_action,
    native_outcome: outcome,
    native_reference_id: value.native_reference_id,
    owner_id: value.owner_id,
    principal_id: value.principal_id,
    request_sha256: value.request_sha256,
    reservation_sha256: value.reservation_sha256,
  });
  return { ...value, result_sha256: sha256Bytes(encoded) };
}, Effect.orDie);

export const nativeProviderSettlementFromFacts = Effect.fn("NativeProvider.settlementFromFacts")(
  function* (
    attempt: NativeProviderAttempt,
    facts: NativeCommandFactsV2,
    expectedIdentity: NativeCommandIdentityV2 | null,
  ): Effect.fn.Return<WorkstreamsNativeSettlementResponse> {
    const request = attempt.request;
    if (facts.receipt === null) return unknownSettlement(request, "receipt_missing");
    const receipt = facts.receipt;
    const events = facts.eventMetadata;
    const commandType = request.native_action === "settle" ? "thread.settle" : "thread.unsettle";
    const identity = facts.identity;
    if (
      facts.commandId !== attempt.nativeCommandId ||
      facts.threadId !== request.identity.native_id ||
      facts.incarnation === null ||
      facts.creationProvenance !== "native_created" ||
      !Number.isSafeInteger(receipt.resultSequence) ||
      receipt.resultSequence < facts.incarnation.sequence ||
      receipt.resultSequence > facts.snapshotSequence ||
      receipt.commandId !== attempt.nativeCommandId ||
      receipt.commandType !== commandType ||
      receipt.threadId !== request.identity.native_id ||
      identity === null ||
      !Schema.is(NativeCommandIdentityV2)(identity) ||
      identity.kind !== "workstream_settlement" ||
      identity.commandId !== attempt.nativeCommandId ||
      identity.commandType !== commandType ||
      identity.aggregateId !== request.identity.native_id ||
      facts.eventMetadataOverflow ||
      events.length > 256 ||
      events.some(
        (event) =>
          event.applicationEventVersion !== 2 ||
          event.commandId !== attempt.nativeCommandId ||
          event.aggregateKind !== "thread" ||
          event.aggregateId !== request.identity.native_id,
      )
    )
      return unknownSettlement(request, "evidence_conflict");
    if (
      expectedIdentity !== null &&
      (!Schema.is(NativeCommandIdentityV2)(expectedIdentity) ||
        !Schema.toEquivalence(NativeCommandIdentityV2)(identity, expectedIdentity))
    )
      return unknownSettlement(request, "evidence_conflict");
    const identityFailure =
      expectedIdentity === null ? unknownSettlement(request, "authority_unavailable") : null;
    const acceptedAt = DateTime.formatIso(receipt.acceptedAt);
    const nativeReceipt = {
      commandId: receipt.commandId,
      aggregateKind: "thread" as const,
      aggregateId: receipt.threadId,
      acceptedAt,
      resultSequence: receipt.resultSequence,
    };
    if (receipt.status === "rejected") {
      if (events.length !== 0) return unknownSettlement(request, "evidence_conflict");
      if (identityFailure !== null) return identityFailure;
      return {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "terminal",
        request,
        result: {
          native_outcome: "denied",
          native_evidence: yield* nativeProviderResultEvidence(request, "denied"),
        },
        native_receipt: { ...nativeReceipt, status: "rejected" },
        settlement_event: null,
      };
    }
    const settlementEvents = events.filter(
      (event) => event.type === "thread.settled" || event.type === "thread.unsettled",
    );
    const settlement = settlementEvents[0];
    const last = events.at(-1);
    if (
      receipt.status !== "accepted" ||
      settlementEvents.length !== 1 ||
      settlement === undefined ||
      last === undefined ||
      settlement.type !==
        (request.native_action === "settle" ? "thread.settled" : "thread.unsettled") ||
      last.sequence !== receipt.resultSequence ||
      last.occurredAt !== acceptedAt ||
      events.some(
        (event, index) =>
          !Number.isSafeInteger(event.sequence) ||
          event.sequence <= 0 ||
          event.sequence > receipt.resultSequence ||
          (index > 0 && event.sequence !== events[index - 1]!.sequence + 1),
      )
    )
      return unknownSettlement(request, "evidence_conflict");
    if (identityFailure !== null) return identityFailure;
    const settlementEvent: WorkstreamsNativeSettlementEvent = {
      eventId: settlement.eventId,
      commandId: attempt.nativeCommandId,
      aggregateKind: "thread",
      aggregateId: settlement.aggregateId,
      sequence: settlement.sequence,
      type: request.native_action === "settle" ? "thread.settled" : "thread.unsettled",
      occurredAt: settlement.occurredAt,
    };
    return {
      protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
      state: "terminal",
      request,
      result: {
        native_outcome: "committed",
        native_evidence: yield* nativeProviderResultEvidence(request, "committed"),
      },
      native_receipt: { ...nativeReceipt, status: "accepted" },
      settlement_event: settlementEvent,
    };
  },
);

export const makeWorkstreamsNativeProvider = (ports: NativeProviderPorts) => {
  const build = ports.build ?? readNativeProviderBuild;
  const validateBinding = Effect.fn("NativeProvider.validateBinding")(function* (
    binding: NativeProviderEnrollmentBinding,
    scope: (typeof NATIVE_PROVIDER_SCOPES)[keyof typeof NATIVE_PROVIDER_SCOPES],
  ) {
    if (!binding.scopes.includes(scope)) return reject("forbidden");
    const actualBuild = yield* build;
    if (Option.isNone(actualBuild)) return "build_unavailable" as const;
    if (
      binding.protocol !== WORKSTREAMS_T3_PROVIDER_PROTOCOL ||
      binding.build.repository !== actualBuild.value.repository ||
      binding.build.sha !== actualBuild.value.sha ||
      binding.build.tree !== actualBuild.value.tree
    )
      return reject("contract_mismatch");
    return null;
  });
  const checkAuthority = ports.authority.readCurrent.pipe(
    Effect.map((tuple) => ({ state: "read" as const, tuple })),
    Effect.catch((error) =>
      Effect.succeed({
        state: "failed" as const,
        fenced: error instanceof NativeStoreAuthorityPersistenceError && error.code === "fenced",
      }),
    ),
  );
  const validateRequest = (
    binding: NativeProviderEnrollmentBinding,
    request: WorkstreamsNativeAttestationRequest,
  ): Rejection | null => {
    if (binding.owner_id !== request.owner_id || binding.principal_id !== request.principal_id)
      return reject("caller_mismatch");
    if (
      request.identity.provider !== "t3" ||
      request.identity.resource_kind !== "thread" ||
      request.identity.id_kind !== "internal" ||
      request.identity.account_provenance.kind !== "not_account_scoped"
    )
      return reject("unsupported_identity");
    if (request.identity.source_instance_id !== binding.source_instance_id)
      return reject("source_mismatch");
    if (
      request.expected_authority_namespace !== binding.authority_namespace ||
      request.expected_store_generation !== binding.store_generation
    )
      return reject("generation_conflict");
    return null;
  };

  const context = Effect.fn("NativeProvider.context")(function* (
    binding: NativeProviderEnrollmentBinding,
  ): Effect.fn.Return<WorkstreamsNativeContextResponse> {
    const invalid = yield* validateBinding(binding, NATIVE_PROVIDER_SCOPES.context);
    if (invalid === "build_unavailable")
      return { protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL, state: "unavailable", reason: invalid };
    if (invalid !== null) return invalid;
    const current = yield* checkAuthority;
    if (current.state === "failed")
      return current.fenced
        ? reject("authority_fenced")
        : {
            protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
            state: "unavailable",
            reason: "authority_unavailable",
          };
    if (!sameAuthority(binding, current.tuple)) return reject("generation_conflict");
    const contextValue = {
      owner_id: binding.owner_id,
      principal_id: binding.principal_id,
      source_instance_id: binding.source_instance_id,
      authority_namespace: binding.authority_namespace,
      store_generation: binding.store_generation,
      enrollment_id: binding.enrollment_id,
      protocol: binding.protocol,
      build: binding.build,
    };
    return { protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL, state: "ready", context: contextValue };
  });

  const attest = Effect.fn("NativeProvider.attest")(function* (
    binding: NativeProviderEnrollmentBinding,
    request: WorkstreamsNativeAttestationRequest,
  ): Effect.fn.Return<WorkstreamsNativeAttestationResponse> {
    const invalid = yield* validateBinding(binding, NATIVE_PROVIDER_SCOPES.context);
    if (invalid === "build_unavailable") return reject("contract_mismatch");
    if (invalid !== null) return invalid;
    const wrongRequest = validateRequest(binding, request);
    if (wrongRequest !== null) return wrongRequest;
    const before = yield* checkAuthority;
    if (before.state === "failed")
      return before.fenced
        ? reject("authority_fenced")
        : {
            protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
            state: "unknown",
            reason: "authority_unavailable",
          };
    if (!sameAuthority(binding, before.tuple)) return reject("generation_conflict");
    const exists = yield* ports
      .threadExists(ThreadId.make(request.identity.native_id))
      .pipe(Effect.option);
    const birth = yield* ports.eventSink
      .getThreadIncarnation(ThreadId.make(request.identity.native_id))
      .pipe(Effect.option);
    const after = yield* checkAuthority;
    if (after.state === "failed")
      return after.fenced
        ? reject("authority_fenced")
        : {
            protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
            state: "unknown",
            reason: "authority_unavailable",
          };
    if (!sameAuthority(binding, after.tuple))
      return {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "unknown",
        reason: "authority_changed",
      };
    if (Option.isNone(exists))
      return {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "unknown",
        reason: "observation_unavailable",
      };
    if (!exists.value) return reject("thread_not_found");
    if (Option.isNone(birth) || birth.value === null)
      return {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "unknown",
        reason: "observation_unavailable",
      };
    const now = yield* DateTime.now;
    const attestedAt = DateTime.formatIso(now);
    const expiresAt = DateTime.formatIso(
      DateTime.addDuration(now, WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS),
    );
    const requestJson = yield* Schema.encodeEffect(
      Schema.fromJsonString(WorkstreamsNativeAttestationRequest),
    )(request).pipe(Effect.option);
    if (Option.isNone(requestJson)) return reject("invalid_request");
    return {
      protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
      state: "attested",
      request,
      attestation: {
        provider: "t3",
        source_instance_id: binding.source_instance_id,
        authority_namespace: binding.authority_namespace,
        native_id: request.identity.native_id,
        store_generation: binding.store_generation,
        evidence_sha256: sha256Bytes(`${requestJson.value}\n${attestedAt}\n${expiresAt}`),
      },
      attested_at: attestedAt,
      expires_at: expiresAt,
    };
  });

  const observe = Effect.fn("NativeProvider.observe")(function* (
    binding: NativeProviderEnrollmentBinding,
    attempt: NativeProviderAttempt,
  ): Effect.fn.Return<
    WorkstreamsNativeSettlementResponse,
    never,
    EnvironmentAuthenticatedPrincipal
  > {
    const request = attempt.request;
    if (attempt.dispatchStartedAt === null)
      return unknownSettlement(request, "dispatch_in_progress");
    const before = yield* checkAuthority;
    if (before.state === "failed") return unknownSettlement(request, "authority_unavailable");
    if (!sameAuthority(binding, before.tuple))
      return unknownSettlement(request, "authority_changed");
    const readBinding = ports.orchestrator.observeNativeWorkstreamSettlementBinding;
    const observed = yield* readBinding === undefined
      ? ports.eventSink
          .readNativeCommandFacts({
            threadId: ThreadId.make(request.identity.native_id),
            commandId: CommandId.make(attempt.nativeCommandId),
            authority: { actorSessionId: AuthSessionId.make(binding.session_id) },
          })
          .pipe(
            Effect.map((facts) => ({ facts, expectedIdentity: null })),
            Effect.result,
          )
      : readBinding({ enrollment: binding, request, attempt }).pipe(Effect.result);
    const after = yield* checkAuthority;
    if (after.state === "failed") return unknownSettlement(request, "authority_unavailable");
    if (!sameAuthority(binding, after.tuple))
      return unknownSettlement(request, "authority_changed");
    if (observed._tag === "Failure") {
      if (observed.failure._tag === "NativeWorkstreamSettlementAuthorityError")
        return unknownSettlement(
          request,
          observed.failure.code === "authority_unavailable"
            ? "authority_unavailable"
            : "evidence_conflict",
        );
      return unknownSettlement(request, "provider_unavailable");
    }
    // AUTH derives the expected identity from the original immutable server witness.
    return yield* nativeProviderSettlementFromFacts(
      attempt,
      observed.success.facts,
      observed.success.expectedIdentity,
    );
  });

  const settlement = (lookupOnly: boolean) =>
    Effect.fn("NativeProvider.settlement")(function* (
      binding: NativeProviderEnrollmentBinding,
      request: WorkstreamsNativeSettlementRequest,
      requestBytesSha256: string,
    ): Effect.fn.Return<
      WorkstreamsNativeSettlementResponse,
      never,
      EnvironmentAuthenticatedPrincipal
    > {
      const invalid = yield* validateBinding(
        binding,
        lookupOnly ? NATIVE_PROVIDER_SCOPES.reconciliation : NATIVE_PROVIDER_SCOPES.settlement,
      );
      if (invalid === "build_unavailable") return reject("contract_mismatch");
      if (invalid !== null) return invalid;
      const wrongRequest = validateRequest(binding, request);
      if (wrongRequest !== null) return wrongRequest;
      const bindingJson = yield* Schema.encodeEffect(
        Schema.fromJsonString(WorkstreamsNativeContext),
      )({
        owner_id: binding.owner_id,
        principal_id: binding.principal_id,
        source_instance_id: binding.source_instance_id,
        authority_namespace: binding.authority_namespace,
        store_generation: binding.store_generation,
        enrollment_id: binding.enrollment_id,
        protocol: binding.protocol,
        build: binding.build,
      }).pipe(Effect.option);
      const requestJson = yield* Schema.encodeEffect(
        Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
      )(request).pipe(Effect.option);
      if (
        Option.isNone(bindingJson) ||
        Option.isNone(requestJson) ||
        !/^[a-f0-9]{64}$/.test(requestBytesSha256)
      )
        return reject("invalid_request");
      const enrollmentSha256 = sha256Bytes(
        `${binding.registry_origin}\n${binding.session_id}\n${bindingJson.value}`,
      );
      const existing = yield* ports.attempts.get(request).pipe(Effect.option);
      if (Option.isNone(existing)) return unknownSettlement(request, "provider_unavailable");
      let attempt: NativeProviderAttempt;
      if (Option.isSome(existing.value)) {
        attempt = existing.value.value;
      } else {
        if (lookupOnly) return unknownSettlement(request, "attempt_not_found");
        const before = yield* checkAuthority;
        if (before.state === "failed")
          return before.fenced
            ? reject("authority_fenced")
            : unknownSettlement(request, "authority_unavailable");
        if (!sameAuthority(binding, before.tuple)) return reject("generation_conflict");
        const exists = yield* ports
          .threadExists(ThreadId.make(request.identity.native_id))
          .pipe(Effect.option);
        const birth = yield* ports.eventSink
          .getThreadIncarnation(ThreadId.make(request.identity.native_id))
          .pipe(Effect.option);
        const after = yield* checkAuthority;
        if (after.state === "failed")
          return after.fenced
            ? reject("authority_fenced")
            : unknownSettlement(request, "authority_unavailable");
        if (!sameAuthority(binding, after.tuple))
          return unknownSettlement(request, "authority_changed");
        if (Option.isNone(exists)) return unknownSettlement(request, "provider_unavailable");
        if (!exists.value) return reject("thread_not_found");
        if (Option.isNone(birth) || birth.value === null)
          return unknownSettlement(request, "evidence_conflict");
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const reserved = yield* ports.attempts
          .reserve({
            request,
            requestBytesSha256,
            enrollmentSha256,
            enrollment: binding,
            nativeCommandId: `workstreams:${sha256Bytes(`${enrollmentSha256}\n${requestJson.value}`)}`,
            createdAt,
            dispatchStartedAt: null,
          })
          .pipe(Effect.option);
        if (Option.isNone(reserved)) return unknownSettlement(request, "provider_unavailable");
        attempt = reserved.value;
      }
      const persistedJson = yield* Schema.encodeEffect(
        Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
      )(attempt.request).pipe(Effect.option);
      if (
        attempt.enrollmentSha256 !== enrollmentSha256 ||
        attempt.nativeCommandId !==
          `workstreams:${sha256Bytes(`${enrollmentSha256}\n${requestJson.value}`)}` ||
        Option.isNone(persistedJson) ||
        persistedJson.value !== requestJson.value ||
        attempt.requestBytesSha256 !== requestBytesSha256
      )
        return reject("idempotency_conflict");
      if (lookupOnly || attempt.dispatchStartedAt !== null) return yield* observe(binding, attempt);
      const startedAt = DateTime.formatIso(yield* DateTime.now);
      const claimed = yield* ports.attempts.startDispatch(request, startedAt).pipe(Effect.option);
      if (Option.isNone(claimed)) return unknownSettlement(request, "provider_unavailable");
      if (!claimed.value) {
        const currentAttempt = yield* ports.attempts.get(request).pipe(Effect.option);
        return Option.isSome(currentAttempt) && Option.isSome(currentAttempt.value)
          ? yield* observe(binding, currentAttempt.value.value)
          : unknownSettlement(request, "dispatch_in_progress");
      }
      const current = yield* checkAuthority;
      if (current.state === "failed") return unknownSettlement(request, "authority_unavailable");
      if (!sameAuthority(binding, current.tuple))
        return unknownSettlement(request, "authority_changed");
      const startedAttempt = yield* ports.attempts.get(request).pipe(Effect.option);
      if (Option.isNone(startedAttempt) || Option.isNone(startedAttempt.value))
        return unknownSettlement(request, "provider_unavailable");
      const durableAttempt = startedAttempt.value.value;
      if (
        durableAttempt.dispatchStartedAt === null ||
        durableAttempt.nativeCommandId !== attempt.nativeCommandId ||
        durableAttempt.enrollmentSha256 !== enrollmentSha256 ||
        durableAttempt.requestBytesSha256 !== requestBytesSha256
      )
        return unknownSettlement(request, "evidence_conflict");
      // Persisted dispatch-start survives timeout and lost replies; a started attempt is observation-only.
      const dispatched = yield* ports.orchestrator
        .dispatchNativeWorkstreamSettlement({
          enrollment: binding,
          request,
          attempt: durableAttempt,
        })
        .pipe(Effect.result);
      if (
        dispatched._tag === "Failure" &&
        dispatched.failure._tag === "NativeWorkstreamSettlementAuthorityError"
      ) {
        const code = dispatched.failure.code;
        if (code === "authority_unavailable")
          return unknownSettlement(request, "authority_unavailable");
        if (code === "idempotency_conflict") return reject("idempotency_conflict");
        return reject(code === "binding_mismatch" ? "caller_mismatch" : "invalid_request");
      }
      return yield* observe(binding, durableAttempt);
    });

  return { context, attest, settle: settlement(false), lookup: settlement(true) };
};

export type WorkstreamsNativeProvider = ReturnType<typeof makeWorkstreamsNativeProvider>;
