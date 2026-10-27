// @effect-diagnostics-next-line nodeBuiltinImport:off -- Mint opaque receipt identities synchronously inside the SQL transaction.
import * as NodeCrypto from "node:crypto";
import * as C from "@t3tools/contracts";
import type {
  ContinuityRequirement,
  GoalContinuityContext,
  RecoveryBoundary,
} from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import { assertSameLink, callerLinkOrigin } from "../../../mcp/linkOrigin.ts";
import {
  assertLiveCaller,
  assertTargetWithinLimits,
  readCaller,
} from "../../../mcp/threadAccess.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Sessions from "../../../orchestration-v2/ProviderSessionManager.ts";
import * as Store from "./GoalContinuityStore.ts";
import {
  abandonContinuity,
  canonicalJson,
  checkpointContinuity,
  closeContinuity,
  continuityDigest,
  detachContinuity,
  handoffContinuity,
  incrementCounter,
  initializeContinuity,
  refused,
  sameContinuityOwner,
  sameContinuityWitness,
  snapshotRef,
  utf8Bytes,
  CONTINUITY_LIMITS,
} from "./model.ts";
import { buildRecoveryCapsule } from "./capsule.ts";
import { selectContinuity } from "./selection.ts";

const callerBrand: unique symbol = Symbol("continuity caller");
/** Minted per authenticated invocation; possession of serialized fields is insufficient. */
export interface ContinuityCaller {
  readonly [callerBrand]: true;
  readonly owner: C.ContinuityOwner;
  readonly witness: C.ContinuityWitness | null;
  readonly actorNamespace: string;
  readonly actorId: string;
  readonly kind: "agent" | "owner";
  readonly writable: boolean;
  readonly limits: Store.ContinuityCommitLimits;
  readonly revalidate: (
    write: boolean,
    target?: C.ThreadId,
  ) => Effect.Effect<void, C.GoalContinuityError>;
}

export class GoalContinuity extends Context.Service<
  GoalContinuity,
  {
    readonly authorize: (
      threadId?: C.ThreadId,
    ) => Effect.Effect<
      ContinuityCaller,
      C.GoalContinuityError,
      Invocation.McpInvocationContext | Threads.ThreadManagementService
    >;
    readonly execute: (
      caller: ContinuityCaller,
      request: C.ContinuityRequest,
    ) => Effect.Effect<C.ContinuityResult, C.GoalContinuityError>;
    readonly requirementFor: (
      environmentId: C.EnvironmentId,
      threadId: C.ThreadId,
    ) => Effect.Effect<ContinuityRequirement, C.GoalContinuityError>;
    readonly context: (capability: C.ContinuityRecoveryCapability) => GoalContinuityContext;
    readonly recordRecoveryTransition: GoalContinuityContext["recordRecoveryTransition"];
    readonly prepareRecovery: GoalContinuityContext["prepareRecovery"];
    readonly beginDelivery: GoalContinuityContext["beginDelivery"];
    readonly markDispatching: GoalContinuityContext["markDispatching"];
    readonly settleDelivery: GoalContinuityContext["settleDelivery"];
    readonly checkContinuation: GoalContinuityContext["checkContinuation"];
  }
>()("t3/jones/goals/continuity/GoalContinuity") {}

const fail = (reason: C.ContinuityRefusalReason) => new C.GoalContinuityError({ reason });
const authError = () => fail("forbidden");
const counter = (value: string) => {
  const next = incrementCounter(value);
  return typeof next === "string" ? Effect.succeed(next) : Effect.fail(fail(next.reason));
};
const readonlyActions = new Set(["read", "selection", "receipt", "delivery_receipt"]);
const capability: C.ContinuityRecoveryCapability = {
  strongIdentity: true,
  turnStart: "fresh_context",
  compaction: "fresh_context_injection",
  strictPreResumptionBarrier: "unsupported",
};
const serialBoundary = (boundary: RecoveryBoundary): C.ContinuityRecoveryBoundary => ({
  witness: boundary.witness,
  producer: boundary.producer,
  point: boundary.point,
});
const ownerOf = (witness: C.ContinuityWitness): C.ContinuityOwner => ({
  environmentId: witness.environmentId,
  threadId: witness.threadId,
  providerInstanceId: witness.providerInstanceId,
  providerThreadId: witness.providerThreadId,
  driver: witness.driver,
  nativeThreadId: witness.nativeThreadId,
});
const expectedOf = (snapshot: C.ContinuitySnapshot): C.ExpectedContinuity => ({
  ...snapshotRef(snapshot),
  owner: snapshot.owner,
});
const rowKey = (
  owner: Pick<C.ContinuityOwner, "environmentId" | "threadId">,
  kind: Store.ContinuityRowKey["kind"],
  key: string,
  namespace = "server",
): Store.ContinuityRowKey => ({
  environmentId: owner.environmentId,
  threadId: owner.threadId,
  kind,
  key,
  namespace,
});
const decode = <A>(schema: Schema.Codec<A, unknown>, row: Store.ContinuityStoredRow) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(row.payload).pipe(
    Effect.mapError(() => fail("corrupt_snapshot")),
    Effect.tap((value) =>
      continuityDigest(value) === row.bodyDigest
        ? Effect.void
        : Effect.fail(fail("corrupt_snapshot")),
    ),
  );
const fenceSchema = Schema.Struct({ revision: C.ContinuityCounter });
function immutableJson<A>(value: A): A {
  const copy: A = JSON.parse(canonicalJson(value));
  const freeze = (item: unknown): void => {
    if (item !== null && typeof item === "object") {
      Object.values(item).forEach(freeze);
      Object.freeze(item);
    }
  };
  freeze(copy);
  return copy;
}

const make = Effect.gen(function* () {
  const store = yield* Store.GoalContinuityStore;
  const callers = new WeakSet<ContinuityCaller>();
  // These maps retain the actual captured producer, not a serialized authority token.
  const producers = new Map<string, RecoveryBoundary["producerOrigin"]>();
  const permits = new WeakMap<
    C.ContinuityDispatchPermit,
    { readonly boundary: RecoveryBoundary; settled: boolean }
  >();
  const liveDeliveries = new Set<string>();

  const authorize: GoalContinuity["Service"]["authorize"] = Effect.fn("GoalContinuity.authorize")(
    function* (target) {
      const initial = yield* readCaller().pipe(Effect.mapError(authError));
      const scope = initial.scope;
      const manager = yield* Effect.serviceOption(Sessions.ProviderSessionManagerV2);
      const threadId = scope.thread?.threadId ?? target;
      if (
        !threadId ||
        (scope.thread !== undefined && target !== undefined && target !== scope.thread.threadId) ||
        (scope.thread === undefined && scope.client === undefined)
      )
        return yield* authError();
      const threads = initial.threads;
      const records = yield* threads
        .getThreadRecords(threadId, ["providerThreads", "runs", "attempts"])
        .pipe(Effect.mapError(authError));
      const provider = records.providerThreads.find(
        (item) => item.id === records.thread.activeProviderThreadId,
      );
      const persisted =
        scope.thread === undefined
          ? yield* store.readOnly(
              Effect.gen(function* () {
                const record = yield* store.current(scope.environmentId, threadId);
                if (record !== null) return record.snapshot.owner;
                const open = yield* store.openDeliveries(scope.environmentId, threadId);
                return open[0] === undefined
                  ? null
                  : (yield* decode(C.ContinuityDeliveryReceipt, open[0])).identity.originalOwner;
              }),
            )
          : null;
      let owner: C.ContinuityOwner;
      if (persisted !== null) owner = persisted;
      else {
        if (
          !provider ||
          provider.nativeThreadRef?.strength !== "strong" ||
          !provider.nativeThreadRef.nativeId ||
          provider.nativeThreadRef.driver !== provider.driver
        )
          return yield* fail("unavailable");
        owner = {
          environmentId: scope.environmentId,
          threadId,
          providerInstanceId: provider.providerInstanceId,
          providerThreadId: provider.id,
          driver: provider.driver,
          nativeThreadId: provider.nativeThreadRef.nativeId,
        };
      }
      let witness: C.ContinuityWitness | null = null;
      if (scope.thread !== undefined) {
        const run = records.runs.find((item) => item.id === initial.caller?.activeRunId);
        const attempt = records.attempts.find((item) => item.id === run?.activeAttemptId);
        if (
          !provider ||
          !run ||
          !attempt ||
          provider.providerSessionId === null ||
          !provider.runtimeIdentity?.runtimeGeneration ||
          scope.thread.providerInstanceId !== provider.providerInstanceId ||
          Option.isNone(manager)
        )
          return yield* authError();
        witness = {
          ...owner,
          providerSessionId: provider.providerSessionId,
          runtimeGeneration: provider.runtimeIdentity.runtimeGeneration,
          runId: run.id,
          attemptId: attempt.id,
        };
      }
      const revalidate: ContinuityCaller["revalidate"] = Effect.fn(
        "GoalContinuity.revalidateCaller",
      )(function* (write, requested = threadId) {
        const current = yield* readCaller().pipe(
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.provideService(Threads.ThreadManagementService, threads),
          Effect.mapError(authError),
        );
        if (scope.thread !== undefined) {
          if (requested !== threadId || witness === null || Option.isNone(manager))
            return yield* authError();
          yield* assertLiveCaller(current).pipe(
            Effect.provideService(Sessions.ProviderSessionManagerV2, manager.value),
            Effect.mapError(authError),
          );
          if (
            !(yield* manager.value.isMcpCallerAttached({
              threadId,
              providerSessionId: witness.providerSessionId,
              providerInstanceId: witness.providerInstanceId,
              mcpCredentialId: scope.thread.providerSessionId,
            }))
          )
            return yield* authError();
        } else if (scope.client === undefined || (write && scope.client.access === "read-only"))
          return yield* authError();
        const shell = yield* threads.getThreadShell(requested).pipe(Effect.mapError(authError));
        if (shell === null || shell.deletedAt !== null) return yield* fail("unavailable");
        if (write) {
          yield* assertTargetWithinLimits(current.limits, shell).pipe(Effect.mapError(authError));
          yield* Effect.suspend<void, C.OrchestratorMcpFailure, never>(() =>
            assertSameLink(current, shell),
          ).pipe(Effect.mapError(authError));
        }
      });
      const origin = callerLinkOrigin(initial);
      const caller: ContinuityCaller = Object.freeze({
        [callerBrand]: true as const,
        owner: Object.freeze(owner),
        witness: witness === null ? null : Object.freeze(witness),
        kind: scope.thread === undefined ? ("owner" as const) : ("agent" as const),
        actorNamespace: continuityDigest({
          namespace: scope.requestNamespace,
          actor: scope.thread?.threadId ?? scope.client!.sessionId,
        }),
        actorId: continuityDigest(
          scope.thread === undefined
            ? { clientSession: scope.client!.sessionId }
            : { thread: scope.thread.threadId, credential: scope.thread.providerSessionId },
        ),
        writable: scope.client?.access !== "read-only",
        revalidate,
        limits: Object.freeze({
          ...initial.limits,
          ...(origin !== undefined && C.linkOriginIsFenced(origin)
            ? { linkSessionId: origin.sessionId }
            : {}),
        }),
      });
      yield* revalidate(false);
      yield* store.readOnly(store.validateBinding(owner, witness));
      callers.add(caller);
      return caller;
    },
  );

  const fence = Effect.fn("GoalContinuity.fence")(function* (
    owner: Pick<C.ContinuityOwner, "environmentId" | "threadId">,
  ) {
    const row = yield* store.row(rowKey(owner, "fence", "current"));
    return { row, revision: row === null ? "0" : (yield* decode(fenceSchema, row)).revision };
  });
  const advanceFence = Effect.fn("GoalContinuity.advanceFence")(function* (
    owner: C.ContinuityOwner,
  ) {
    const previous = yield* fence(owner);
    const revision = yield* counter(previous.revision);
    yield* store.writeRow(rowKey(owner, "fence", "current"), previous.row, { revision });
    return revision;
  });
  const delivery = Effect.fn("GoalContinuity.delivery")(function* (
    owner: Pick<C.ContinuityOwner, "environmentId" | "threadId">,
    id: string,
  ) {
    const row = yield* store.row(rowKey(owner, "delivery", id));
    if (row === null) return null;
    const receipt = yield* decode(C.ContinuityDeliveryReceipt, row);
    if (
      receipt.identity.deliveryId !== id ||
      receipt.identity.originalOwner.environmentId !== owner.environmentId ||
      receipt.identity.originalOwner.threadId !== owner.threadId
    )
      return yield* fail("corrupt_snapshot");
    return { row, receipt };
  });
  const gateFor = Effect.fn("GoalContinuity.gateFor")(function* (
    owner: Pick<C.ContinuityOwner, "environmentId" | "threadId">,
    committingDispatch?: string,
  ) {
    const { revision } = yield* fence(owner);
    const rows = yield* store.openDeliveries(owner.environmentId, owner.threadId);
    const receipts: C.ContinuityDeliveryReceipt[] = [];
    for (const row of rows) {
      const receipt = yield* decode(C.ContinuityDeliveryReceipt, row);
      if (
        receipt.identity.originalOwner.environmentId !== owner.environmentId ||
        receipt.identity.originalOwner.threadId !== owner.threadId
      )
        return yield* fail("corrupt_snapshot");
      receipts.push(receipt);
    }
    if (receipts.length === 0) return { outcome: "clear", fenceRevision: revision } as const;
    // A persisted dispatch with no original in-memory permit is unfinished work
    // from reconstruction. It is uncertain even if its stored phase says dispatching.
    const uncertain = receipts.some(
      (item) =>
        item.phase === "unknown" ||
        (item.phase === "dispatching" &&
          item.identity.deliveryId !== committingDispatch &&
          !liveDeliveries.has(item.identity.deliveryId)),
    );
    return {
      outcome: "hold",
      fenceRevision: revision,
      reason: uncertain
        ? "delivery_unknown"
        : receipts.some(
              (item) =>
                item.phase === "pending" ||
                item.phase === "prepared" ||
                item.phase === "dispatching",
            )
          ? "delivery_pending"
          : "recovery_required",
      deliveryIds: receipts.map((item) => item.identity.deliveryId).sort(),
    } satisfies C.DeliveryGate;
  });
  const appendPhase = Effect.fn("GoalContinuity.appendPhase")(function* (
    previous: { row: Store.ContinuityStoredRow; receipt: C.ContinuityDeliveryReceipt } | null,
    receipt: C.ContinuityDeliveryReceipt,
    event: C.ContinuityDeliveryPhaseEvent,
    open: boolean,
  ) {
    const next = {
      ...receipt,
      phase: event.phase,
      deliveryRevision: event.deliveryRevision,
      fenceRevision: event.fenceRevision,
      events: [...receipt.events, event],
    };
    yield* store.writeRow(
      rowKey(
        receipt.identity.originalOwner,
        "phase",
        `${receipt.identity.deliveryId}:${event.deliveryRevision}`,
      ),
      null,
      event,
    );
    yield* store.writeRow(
      rowKey(receipt.identity.originalOwner, "delivery", receipt.identity.deliveryId),
      previous?.row ?? null,
      next,
      open,
    );
    return next;
  });
  const validateBoundary = Effect.fn("GoalContinuity.validateBoundary")(function* (
    boundary: RecoveryBoundary,
  ) {
    const { witness, producer, producerOrigin } = boundary;
    if (
      !Schema.is(C.ContinuityRecoveryBoundary)(serialBoundary(boundary)) ||
      producerOrigin.driver !== witness.driver ||
      producerOrigin.instanceId !== witness.providerInstanceId ||
      producerOrigin.providerSessionId !== witness.providerSessionId ||
      producerOrigin.runtimeGeneration !== witness.runtimeGeneration ||
      producer.driver !== witness.driver ||
      producer.providerInstanceId !== witness.providerInstanceId ||
      producer.providerSessionId !== witness.providerSessionId ||
      producer.runtimeGeneration !== witness.runtimeGeneration
    )
      return yield* fail("binding_mismatch");
    const captured = producers.get(producer.producerId);
    if (
      captured !== undefined &&
      (captured.token !== producerOrigin.token ||
        captured.revalidateCurrent !== producerOrigin.revalidateCurrent)
    )
      return yield* fail("binding_mismatch");
    yield* producerOrigin.revalidateCurrent.pipe(Effect.mapError(() => fail("binding_mismatch")));
    yield* store.validateBinding(ownerOf(witness), witness);
    producers.set(producer.producerId, producerOrigin);
  });
  const reconstructUnfinished = Effect.fn("GoalContinuity.reconstructUnfinished")(function* (
    environmentId: C.EnvironmentId,
    threadId: C.ThreadId,
  ) {
    const candidates = yield* store.readOnly(store.openDeliveries(environmentId, threadId));
    for (const candidate of candidates) {
      const observed = yield* decode(C.ContinuityDeliveryReceipt, candidate);
      if (observed.phase !== "dispatching" || liveDeliveries.has(observed.identity.deliveryId))
        continue;
      // Indexed, lazy reconstruction for the requested thread only. The phase
      // and fence commit before admission reads the gate; no provider is loaded.
      yield* store
        .transaction(
          Effect.gen(function* () {
            const found = yield* delivery(
              observed.identity.originalOwner,
              observed.identity.deliveryId,
            );
            if (
              found === null ||
              found.receipt.phase !== "dispatching" ||
              liveDeliveries.has(observed.identity.deliveryId)
            )
              return;
            const dispatch = found.receipt.events.findLast(
              (event) => event.phase === "dispatching",
            );
            if (!dispatch?.permit) return yield* fail("corrupt_snapshot");
            const deliveryRevision = yield* counter(found.receipt.deliveryRevision);
            const fenceRevision = yield* advanceFence(found.receipt.identity.originalOwner);
            yield* appendPhase(
              found,
              found.receipt,
              {
                eventId: NodeCrypto.randomUUID(),
                deliveryRevision,
                fenceRevision,
                phase: "unknown",
                transition: null,
                permit: dispatch.permit,
                evidence: { outcome: "unknown", cause: "reconstruction" },
              },
              true,
            );
          }),
        )
        .pipe(Effect.uninterruptible);
    }
  });
  const requirementFor: GoalContinuity["Service"]["requirementFor"] = Effect.fn(
    "GoalContinuity.requirementFor",
  )(function* (environmentId, threadId) {
    yield* reconstructUnfinished(environmentId, threadId);
    return yield* store.readOnly(
      Effect.gen(function* () {
        const gate = yield* gateFor({ environmentId, threadId });
        const record = yield* store.current(environmentId, threadId);
        const current =
          record === null
            ? null
            : { owner: record.snapshot.owner, target: snapshotRef(record.snapshot) };
        // History wins even when the current pointer is detached, replaced or absent.
        if (gate.outcome === "hold")
          return { outcome: "hold", reason: gate.reason, gate, current } as const;
        if (record === null) return { outcome: "absent", gate } as const;
        yield* store.validateBinding(record.snapshot.owner);
        if (record.snapshot.attachment.state === "detached")
          return { outcome: "hold", reason: "detached", gate, current } as const;
        if (["complete", "abandoned"].includes(record.snapshot.status))
          return { outcome: "hold", reason: "terminal", gate, current } as const;
        if (record.snapshot.unresolvedEffect !== null) return yield* fail("unresolved_effect");
        return {
          outcome: "required",
          owner: record.snapshot.owner,
          target: snapshotRef(record.snapshot),
          gate,
        } as const;
      }),
    );
  });

  const execute: GoalContinuity["Service"]["execute"] = Effect.fn("GoalContinuity.execute")(
    function* (caller, request) {
      if (!callers.has(caller) || !Schema.is(C.ContinuityRequest)(request))
        return yield* authError();
      const write = !readonlyActions.has(request.action);
      if (write && !caller.writable) return yield* authError();
      yield* caller.revalidate(write);
      if (request.action === "handoff") {
        if (
          caller.kind !== "owner" ||
          request.destination.environmentId !== caller.owner.environmentId
        )
          return yield* authError();
        yield* caller.revalidate(true, request.destination.threadId);
      }
      let previousGeneration: C.ContinuityCounter | null = null;
      let attemptedGeneration: C.ContinuityCounter | null = null;
      const operation = Effect.gen(function* () {
        // Recheck after transaction admission; these checks inspect server-owned
        // attachment/projection state and never issue a provider RPC.
        yield* caller.revalidate(write);
        // Current authority is always checked, even when reading a historical receipt.
        yield* store.validateBinding(
          caller.owner,
          caller.witness,
          write ? caller.limits : undefined,
        );
        const record = yield* store.current(caller.owner.environmentId, caller.owner.threadId);
        const snapshot = record?.snapshot ?? null;
        previousGeneration = snapshot?.generation ?? null;
        if (request.action === "read")
          return snapshot === null
            ? ({ outcome: "absent" } as const)
            : ({ outcome: "snapshot", snapshot, snapshotDigest: record!.digest } as const);
        if (request.action === "selection")
          return snapshot === null
            ? refused("unavailable")
            : selectContinuity(snapshot, request.selection);
        if (request.action === "delivery_receipt") {
          const found = yield* delivery(caller.owner, request.deliveryId);
          return found === null
            ? refused("unavailable")
            : ({
                outcome: "delivery_receipt",
                receipt: found.receipt,
                gate: yield* gateFor(caller.owner),
              } as const);
        }
        // Request identity survives credential replacement and owner takeover.
        // Actor provenance stays in the receipt; current authority is checked above.
        const receiptKey = rowKey(caller.owner, "mutation", request.clientRequestId);
        const prior = yield* store.row(receiptKey);
        if (request.action === "receipt") {
          if (prior === null)
            return {
              outcome: "unknown",
              clientRequestId: request.clientRequestId,
              requestDigest: request.requestDigest,
              previousGeneration: null,
              attemptedGeneration: null,
            } as const;
          const receipt = yield* decode(C.ContinuityReceipt, prior);
          return receipt.requestDigest !== request.requestDigest
            ? refused("request_key_conflict")
            : ({ outcome: "receipt", receipt, replayed: true } as const);
        }
        const requestDigest = continuityDigest(request);
        if (prior !== null) {
          const receipt = yield* decode(C.ContinuityReceipt, prior);
          return receipt.requestDigest === requestDigest
            ? ({ outcome: "receipt", receipt, replayed: true } as const)
            : refused("request_key_conflict");
        }
        const result = yield* Effect.gen(function* () {
          let result: C.ContinuityMutationOutcome = refused("invalid_request");
          const gate = yield* gateFor(caller.owner);
          if (request.action === "reconcile_delivery") {
            if (caller.kind !== "owner") return yield* authError();
            const found = yield* delivery(caller.owner, request.deliveryId);
            if (found === null) return refused("unavailable");
            const original = found.receipt;
            if (
              !sameContinuityOwner(
                original.identity.originalOwner,
                request.expectedOriginalOwner,
              ) ||
              original.deliveryRevision !== request.expectedDeliveryRevision ||
              gate.fenceRevision !== request.expectedFenceRevision ||
              canonicalJson(snapshot === null ? null : expectedOf(snapshot)) !==
                canonicalJson(request.expectedCurrentRecord)
            )
              result = refused("stale");
            else {
              const deliveryRevision = yield* counter(original.deliveryRevision);
              const fenceRevision = yield* advanceFence(original.identity.originalOwner);
              // This domain has no server-observed termination/pause-settlement proof
              // source. Owner assertions record a decision, never dispatch authority.
              const disposition: C.ContinuityDeliveryDisposition = {
                dispositionId: NodeCrypto.randomUUID(),
                request,
                actorNamespace: caller.actorNamespace,
                actorId: caller.actorId,
                deliveryRevision,
                fenceRevision,
                quiescence: { outcome: "unproven" },
                effect: { type: "held", reason: "quiescence_required" },
              };
              yield* store.writeRow(
                rowKey(caller.owner, "disposition", disposition.dispositionId),
                null,
                disposition,
              );
              yield* store.writeRow(
                rowKey(caller.owner, "delivery", request.deliveryId),
                found.row,
                {
                  ...original,
                  deliveryRevision,
                  fenceRevision,
                  dispositions: [...original.dispositions, disposition],
                },
                found.row.open,
              );
              result = {
                outcome: "delivery_reconciled",
                disposition,
                gate: yield* gateFor(caller.owner),
              };
            }
          } else if (request.action === "initialize") {
            if (!sameContinuityOwner(request.expectedOwner, caller.owner))
              result = refused("binding_mismatch");
            else if (
              canonicalJson(request.expectedCurrent) !==
              canonicalJson(snapshot === null ? null : snapshotRef(snapshot))
            )
              result = refused("stale");
            else if (gate.outcome === "hold") result = refused(gate.reason);
            else if (request.initialization.type === "propose") {
              const proposal: C.ContinuityProposal = {
                proposalId: request.initialization.proposalId,
                intentDigest: continuityDigest(request.intent),
                intent: request.intent,
                owner: caller.owner,
              };
              const key = rowKey(caller.owner, "proposal", proposal.proposalId);
              const existing = yield* store.row(key);
              if (utf8Bytes(canonicalJson(proposal)) > CONTINUITY_LIMITS.snapshotBytes)
                result = refused("snapshot_too_large");
              else if (
                request.intent.initializationMode !== "confirm_first" ||
                (existing !== null &&
                  canonicalJson(yield* decode(C.ContinuityProposal, existing)) !==
                    canonicalJson(proposal))
              )
                result = refused("proposal_mismatch");
              else {
                if (existing === null) yield* store.writeRow(key, null, proposal);
                result = { outcome: "proposed", proposal };
              }
            } else {
              let confirmation:
                | { proposal: C.ContinuityProposal; confirmation: C.ContinuityOwnerConfirmation }
                | undefined;
              if (request.initialization.type === "confirm") {
                if (caller.kind !== "owner") return yield* authError();
                const proposed = yield* store.row(
                  rowKey(caller.owner, "proposal", request.initialization.proposalId),
                );
                if (proposed === null) return refused("confirmation_required");
                const proposal = yield* decode(C.ContinuityProposal, proposed);
                const proof: C.ContinuityOwnerConfirmation = {
                  confirmationId: request.initialization.confirmationId,
                  proposalId: proposal.proposalId,
                  intentDigest: proposal.intentDigest,
                  owner: caller.owner,
                  ownerResponseId: continuityDigest({
                    actor: caller.actorId,
                    clientRequestId: request.clientRequestId,
                    requestDigest,
                  }),
                  authenticatedOwnerId: caller.actorId,
                };
                const key = rowKey(caller.owner, "confirmation", proof.confirmationId);
                const old = yield* store.row(key);
                if (
                  old !== null &&
                  canonicalJson(yield* decode(C.ContinuityOwnerConfirmation, old)) !==
                    canonicalJson(proof)
                )
                  return refused("confirmation_mismatch");
                if (old === null) yield* store.writeRow(key, null, proof);
                confirmation = { proposal, confirmation: proof };
              }
              result = initializeContinuity({
                intent: request.intent,
                owner: caller.owner,
                current: snapshot,
                ...(confirmation ? { confirmation } : {}),
              });
            }
          } else if (snapshot === null) result = refused("unavailable");
          else if (!sameContinuityOwner(snapshot.owner, caller.owner))
            result = refused("binding_mismatch");
          else if (request.action === "handoff" && gate.outcome === "hold")
            result = refused(gate.reason);
          else {
            switch (request.action) {
              case "checkpoint":
                result = checkpointContinuity(snapshot, request.expected, request.patch);
                break;
              case "close":
                result = closeContinuity(snapshot, request.expected, request.completionEvidenceIds);
                break;
              case "abandon":
                result = abandonContinuity(
                  snapshot,
                  request.expected,
                  request.reason,
                  request.evidenceIds,
                );
                break;
              case "detach":
                result = detachContinuity(snapshot, request.expected, request.reason);
                break;
              case "handoff": {
                yield* caller.revalidate(true, request.destination.threadId);
                yield* store.validateBinding(request.destination, null, caller.limits, true);
                const destinationGate = yield* gateFor(request.destination);
                const destination = yield* store.current(
                  request.destination.environmentId,
                  request.destination.threadId,
                );
                result =
                  destinationGate.outcome === "hold"
                    ? refused(destinationGate.reason)
                    : destination !== null && destination.recordId !== record!.recordId
                      ? refused("binding_mismatch")
                      : handoffContinuity(
                          snapshot,
                          request.expected,
                          request.destination,
                          request.evidenceIds,
                        );
                break;
              }
            }
          }
          return result;
        });
        attemptedGeneration =
          result.outcome === "applied"
            ? result.snapshot.generation
            : (snapshot?.generation ?? null);
        if (result.outcome === "applied") yield* store.save(record, result.snapshot);
        const receipt: C.ContinuityReceipt = {
          clientRequestId: request.clientRequestId,
          requestDigest,
          environmentId: caller.owner.environmentId,
          threadId: caller.owner.threadId,
          actorNamespace: caller.actorNamespace,
          actorId: caller.actorId,
          targetOwner: caller.owner,
          witness: caller.witness,
          previousGeneration: snapshot?.generation ?? null,
          attemptedGeneration:
            result.outcome === "applied"
              ? result.snapshot.generation
              : (snapshot?.generation ?? null),
          result,
        };
        yield* store.writeRow(receiptKey, null, receipt);
        return { outcome: "receipt", receipt, replayed: false } as const;
      });
      // Durable receipt and snapshot commit are cancellation-atomic. Unconfirmed
      // outcomes require the exact receipt, never a retry with a new request key.
      return yield* (
        write
          ? store.transaction(operation).pipe(Effect.uninterruptible)
          : store.readOnly(operation)
      ).pipe(
        Effect.catchTags({
          GoalContinuityError: (error) =>
            error.reason === "unavailable" && typeof request.clientRequestId === "string"
              ? Effect.succeed({
                  outcome: "unknown",
                  clientRequestId: request.clientRequestId,
                  requestDigest:
                    request.action === "receipt"
                      ? request.requestDigest
                      : continuityDigest(request),
                  previousGeneration,
                  attemptedGeneration,
                } as const)
              : Effect.fail(error),
        }),
      );
    },
  );

  const recordRecoveryTransition: GoalContinuityContext["recordRecoveryTransition"] = Effect.fn(
    "GoalContinuity.recordRecoveryTransition",
  )(function* (boundary, transition) {
    return yield* store
      .transaction(
        Effect.gen(function* () {
          const owner = ownerOf(boundary.witness);
          const record = yield* store.current(owner.environmentId, owner.threadId);
          if (record === null) {
            const gate = yield* gateFor(owner);
            return gate.outcome === "hold"
              ? refused(gate.reason)
              : ({ outcome: "absent", gate } as const);
          }
          yield* validateBoundary(boundary);
          if (!Schema.is(C.ContinuityRecoveryTransition)(transition))
            return refused("invalid_transition");
          const projected = buildRecoveryCapsule(record.snapshot, {
            owner,
            capability,
            point: boundary.point,
          });
          if (projected.outcome !== "context")
            return refused(projected.outcome === "absent" ? "unavailable" : projected.reason);
          const id = continuityDigest(serialBoundary(boundary));
          const found = yield* delivery(owner, id);
          if (found !== null) {
            const duplicate = found.receipt.events.find(
              (event) => event.transition?.eventId === transition.eventId,
            );
            if (duplicate) {
              if (duplicate.transition?.type !== transition.type)
                return refused("invalid_transition");
              const complete = found.receipt.events.some(
                (event) => event.transition?.type === "completed",
              );
              return {
                outcome: "recorded",
                receipt: found.receipt,
                pendingTicket:
                  found.receipt.phase === "pending" && complete
                    ? {
                        type: "pending_ticket" as const,
                        identity: found.receipt.identity,
                        deliveryRevision: found.receipt.deliveryRevision,
                        fenceRevision: found.receipt.fenceRevision,
                      }
                    : null,
                gate: yield* gateFor(owner),
              } as const;
            }
            if (
              transition.type !== "completed" ||
              found.receipt.phase !== "pending" ||
              found.receipt.events.some((event) => event.transition?.type === "completed")
            )
              return refused("invalid_transition");
            const deliveryRevision = yield* counter(found.receipt.deliveryRevision);
            const fenceRevision = yield* advanceFence(owner);
            const receipt = yield* appendPhase(
              found,
              found.receipt,
              {
                eventId: transition.eventId,
                deliveryRevision,
                fenceRevision,
                phase: "pending",
                transition,
                permit: null,
                evidence: null,
              },
              true,
            );
            return {
              outcome: "recorded",
              receipt,
              pendingTicket: {
                type: "pending_ticket" as const,
                identity: receipt.identity,
                deliveryRevision,
                fenceRevision,
              },
              gate: yield* gateFor(owner),
            } as const;
          }
          const gate = yield* gateFor(owner);
          if (gate.outcome === "hold") return refused(gate.reason);
          if (
            (boundary.point.type === "turn_start" && transition.type !== "completed") ||
            (boundary.point.type === "compaction" && transition.type !== "started")
          )
            return refused("invalid_transition");
          const fenceRevision = yield* advanceFence(owner);
          const identity: C.ContinuityDeliveryIdentity = {
            deliveryId: id,
            originalOwner: owner,
            goalId: record.snapshot.intent.goalId,
            revision: record.snapshot.intent.revision,
            generation: record.snapshot.generation,
            point: boundary.point,
            witness: boundary.witness,
            producer: boundary.producer,
          };
          const base: C.ContinuityDeliveryReceipt = {
            identity,
            deliveryRevision: "0",
            fenceRevision,
            phase: "pending",
            preparation: null,
            events: [],
            dispositions: [],
          };
          const receipt = yield* appendPhase(
            null,
            base,
            {
              eventId: transition.eventId,
              deliveryRevision: "0",
              fenceRevision,
              phase: "pending",
              transition,
              permit: null,
              evidence: null,
            },
            true,
          );
          return {
            outcome: "recorded",
            receipt,
            pendingTicket:
              transition.type === "completed"
                ? {
                    type: "pending_ticket" as const,
                    identity,
                    deliveryRevision: "0",
                    fenceRevision,
                  }
                : null,
            gate: yield* gateFor(owner),
          } as const;
        }),
      )
      .pipe(Effect.uninterruptible);
  });

  const prepareRecovery: GoalContinuityContext["prepareRecovery"] = Effect.fn(
    "GoalContinuity.prepareRecovery",
  )(function* (boundary) {
    return yield* store.readOnly(
      Effect.gen(function* () {
        const owner = ownerOf(boundary.witness);
        const record = yield* store.current(owner.environmentId, owner.threadId);
        const gate = yield* gateFor(owner);
        if (record === null)
          return gate.outcome === "hold"
            ? ({ outcome: "hold", reason: gate.reason } as const)
            : ({ outcome: "absent" } as const);
        yield* validateBoundary(boundary);
        if (gate.outcome === "hold") {
          const id = continuityDigest(serialBoundary(boundary));
          const found = yield* delivery(owner, id);
          if (
            gate.deliveryIds.length !== 1 ||
            gate.deliveryIds[0] !== id ||
            found?.receipt.phase !== "pending" ||
            !found.receipt.events.some((event) => event.transition?.type === "completed")
          )
            return { outcome: "hold", reason: gate.reason } as const;
        }
        return buildRecoveryCapsule(record.snapshot, { owner, capability, point: boundary.point });
      }),
    );
  });

  const beginDelivery: GoalContinuityContext["beginDelivery"] = Effect.fn(
    "GoalContinuity.beginDelivery",
  )(function* (input) {
    return yield* store
      .transaction(
        Effect.gen(function* () {
          const ticket = input.pendingTicket;
          const owner = ticket.identity.originalOwner;
          const found = yield* delivery(owner, ticket.identity.deliveryId);
          const gate = yield* gateFor(owner);
          if (
            found === null ||
            found.receipt.phase !== "pending" ||
            found.receipt.deliveryRevision !== ticket.deliveryRevision ||
            found.receipt.fenceRevision !== ticket.fenceRevision ||
            canonicalJson(found.receipt.identity) !== canonicalJson(ticket.identity) ||
            gate.fenceRevision !== ticket.fenceRevision ||
            !found.receipt.events.some((event) => event.transition?.type === "completed")
          )
            return refused("stale");
          // No serialized recovery grant is accepted: this lane cannot prove quiescence.
          if (
            input.recoveryAuthorization !== undefined ||
            gate.outcome !== "hold" ||
            gate.deliveryIds.length !== 1 ||
            gate.deliveryIds[0] !== ticket.identity.deliveryId
          )
            return refused("recovery_required");
          const origin = producers.get(ticket.identity.producer.producerId);
          if (!origin) return refused("binding_mismatch");
          const boundary: RecoveryBoundary = {
            witness: ticket.identity.witness,
            point: ticket.identity.point,
            producer: ticket.identity.producer,
            producerOrigin: origin,
          };
          yield* validateBoundary(boundary);
          const record = yield* store.current(owner.environmentId, owner.threadId);
          const fresh = buildRecoveryCapsule(record?.snapshot ?? null, {
            owner,
            capability,
            point: ticket.identity.point,
          });
          if (
            fresh.outcome !== "context" ||
            canonicalJson(fresh) !== canonicalJson(input.projection) ||
            record === null
          )
            return refused("stale");
          const preparation: C.ContinuityDeliveryPreparation = {
            target: expectedOf(record.snapshot),
            capsuleDigest: continuityDigest(fresh.text),
          };
          const deliveryRevision = yield* counter(found.receipt.deliveryRevision);
          const fenceRevision = yield* advanceFence(owner);
          const receipt = yield* appendPhase(
            found,
            { ...found.receipt, preparation },
            {
              eventId: NodeCrypto.randomUUID(),
              deliveryRevision,
              fenceRevision,
              phase: "prepared",
              transition: null,
              permit: null,
              evidence: null,
            },
            true,
          );
          return {
            outcome: "prepared",
            ticket: {
              type: "prepared_ticket",
              identity: receipt.identity,
              deliveryRevision,
              fenceRevision,
              preparation,
            },
            gate: yield* gateFor(owner),
          } as const;
        }),
      )
      .pipe(Effect.uninterruptible);
  });

  const markDispatching: GoalContinuityContext["markDispatching"] = Effect.fn(
    "GoalContinuity.markDispatching",
  )(function* (ticket) {
    const origin = producers.get(ticket.identity.producer.producerId);
    if (!origin) return refused("binding_mismatch");
    const boundary: RecoveryBoundary = {
      witness: ticket.identity.witness,
      point: ticket.identity.point,
      producer: ticket.identity.producer,
      producerOrigin: origin,
    };
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const result = yield* store.transaction(
          Effect.gen(function* () {
            yield* validateBoundary(boundary);
            const found = yield* delivery(
              ticket.identity.originalOwner,
              ticket.identity.deliveryId,
            );
            const gate = yield* gateFor(ticket.identity.originalOwner);
            if (
              found === null ||
              found.receipt.phase !== "prepared" ||
              found.receipt.deliveryRevision !== ticket.deliveryRevision ||
              found.receipt.fenceRevision !== ticket.fenceRevision ||
              gate.fenceRevision !== ticket.fenceRevision ||
              canonicalJson(found.receipt.identity) !== canonicalJson(ticket.identity) ||
              canonicalJson(found.receipt.preparation) !== canonicalJson(ticket.preparation)
            )
              return refused("permit_consumed");
            const record = yield* store.current(
              ticket.identity.originalOwner.environmentId,
              ticket.identity.originalOwner.threadId,
            );
            if (
              record === null ||
              canonicalJson(expectedOf(record.snapshot)) !==
                canonicalJson(ticket.preparation.target)
            )
              return refused("stale");
            const fresh = buildRecoveryCapsule(record.snapshot, {
              owner: ticket.identity.originalOwner,
              capability,
              point: ticket.identity.point,
            });
            if (
              fresh.outcome !== "context" ||
              continuityDigest(fresh.text) !== ticket.preparation.capsuleDigest
            )
              return refused("stale");
            const deliveryRevision = yield* counter(found.receipt.deliveryRevision);
            const fenceRevision = yield* advanceFence(ticket.identity.originalOwner);
            const permit = immutableJson<C.ContinuityDispatchPermit>({
              ...ticket,
              type: "dispatch_permit",
              deliveryRevision,
              fenceRevision,
              permitId: NodeCrypto.randomUUID(),
              dispatchAttemptId: NodeCrypto.randomUUID(),
              rpcRequestId: NodeCrypto.randomUUID(),
            });
            yield* appendPhase(
              found,
              found.receipt,
              {
                eventId: NodeCrypto.randomUUID(),
                deliveryRevision,
                fenceRevision,
                phase: "dispatching",
                transition: null,
                permit,
                evidence: null,
              },
              true,
            );
            return {
              outcome: "dispatching",
              permit,
              gate: yield* gateFor(ticket.identity.originalOwner, ticket.identity.deliveryId),
            } as const;
          }),
        );
        // Issuance happens only after a confirmed commit. Replaying a prepared ticket
        // never reconstructs this object or grants a second send.
        if (result.outcome === "dispatching") {
          permits.set(result.permit, {
            boundary: {
              witness: result.permit.identity.witness,
              producer: result.permit.identity.producer,
              point: result.permit.identity.point,
              producerOrigin: origin,
            },
            settled: false,
          });
          liveDeliveries.add(result.permit.identity.deliveryId);
        }
        return result;
      }),
    );
  });

  const settleDelivery: GoalContinuityContext["settleDelivery"] = Effect.fn(
    "GoalContinuity.settleDelivery",
  )(function* ({ permit, evidence }) {
    const association = permits.get(permit);
    if (!association || !Schema.is(C.ContinuityDeliveryEvidence)(evidence))
      return refused("permit_revoked");
    // Currentness is deliberately not required for authentic late acknowledgement:
    // its original permit and producer association remain the only authority.
    const boundary = association.boundary;
    if (
      evidence.outcome === "acknowledged" &&
      (evidence.rpcRequestId !== permit.rpcRequestId ||
        !sameContinuityWitness(evidence.witness, boundary.witness) ||
        canonicalJson(evidence.producer) !== canonicalJson(boundary.producer))
    )
      return refused("binding_mismatch");
    // Only the original issued object reaches this trusted executor callback.
    // The captured adapter reports dispatch_precluded before invoking its client;
    // this is not an inference from missing runtime state or an owner assertion.
    if (
      evidence.outcome === "not_sent" &&
      (evidence.proof.type === "dispatch_precluded"
        ? association.settled || evidence.proof.revocationRef !== permit.dispatchAttemptId
        : evidence.proof.rpcRequestId !== permit.rpcRequestId)
    )
      return refused("no_effect_proof_required");
    return yield* store
      .transaction(
        Effect.gen(function* () {
          const found = yield* delivery(permit.identity.originalOwner, permit.identity.deliveryId);
          if (found === null) return refused("unavailable");
          const dispatch = found.receipt.events.find(
            (event) => event.phase === "dispatching" && event.permit?.permitId === permit.permitId,
          );
          if (!dispatch || canonicalJson(dispatch.permit) !== canonicalJson(permit))
            return refused("permit_revoked");
          const prior = found.receipt.events.find(
            (event) =>
              event.evidence !== null && canonicalJson(event.evidence) === canonicalJson(evidence),
          );
          if (prior || found.receipt.phase === "acknowledged")
            return {
              outcome: "settled",
              receipt: found.receipt,
              gate: yield* gateFor(permit.identity.originalOwner),
            } as const;
          if (
            !["dispatching", "unknown"].includes(found.receipt.phase) ||
            (evidence.outcome !== "acknowledged" && association.settled)
          )
            return refused("permit_consumed");
          const deliveryRevision = yield* counter(found.receipt.deliveryRevision);
          const fenceRevision = yield* advanceFence(permit.identity.originalOwner);
          const receipt = yield* appendPhase(
            found,
            found.receipt,
            {
              eventId: NodeCrypto.randomUUID(),
              deliveryRevision,
              fenceRevision,
              phase: evidence.outcome,
              transition: null,
              permit,
              evidence,
            },
            evidence.outcome !== "acknowledged",
          );
          return {
            outcome: "settled",
            receipt,
            gate: yield* gateFor(permit.identity.originalOwner),
          } as const;
        }),
      )
      .pipe(
        Effect.tap((result) =>
          Effect.sync(() => {
            if (result.outcome === "settled") {
              association.settled = true;
              liveDeliveries.delete(permit.identity.deliveryId);
            }
          }),
        ),
        Effect.uninterruptible,
      );
  });

  const checkContinuation: GoalContinuityContext["checkContinuation"] = Effect.fn(
    "GoalContinuity.checkContinuation",
  )(function* (boundary) {
    const owner = ownerOf(boundary.witness);
    const requirement = yield* requirementFor(owner.environmentId, owner.threadId);
    if (requirement.outcome === "absent") return { requirement, currentOwner: null };
    yield* validateBoundary(boundary);
    if (requirement.outcome === "required" && !sameContinuityOwner(requirement.owner, owner))
      return yield* fail("binding_mismatch");
    return {
      requirement,
      currentOwner:
        requirement.outcome === "required"
          ? requirement.owner
          : (requirement.current?.owner ?? null),
    };
  });
  return GoalContinuity.of({
    authorize,
    execute,
    requirementFor,
    recordRecoveryTransition,
    prepareRecovery,
    beginDelivery,
    markDispatching,
    settleDelivery,
    checkContinuation,
    context: (requested) => ({
      capability: requested,
      recordRecoveryTransition,
      prepareRecovery: (boundary) =>
        (boundary.point.type === "compaction"
          ? requested.compaction === "unsupported"
          : requested.turnStart === "unsupported") || !requested.strongIdentity
          ? Effect.succeed({ outcome: "unsupported", reason: "unsupported" } as const)
          : prepareRecovery(boundary),
      beginDelivery,
      markDispatching,
      settleDelivery,
      checkContinuation,
    }),
  });
});
export const layer = Layer.effect(GoalContinuity, make);
