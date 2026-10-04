import * as NodeCrypto from "node:crypto";
import {
  CheckpointScopeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";
import {
  OrdinaryCheckoutAdmissionV1,
  OrdinaryCheckoutExecutionRefV1,
  ordinaryCheckoutAdmissionMatchesV1,
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutLeaseIdentityV1,
  type OrdinaryCheckoutExecutionExecutorV1,
} from "./OrdinaryCheckoutOwnership.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

export type ProviderManagedActorExecutorV1 = Extract<
  OrdinaryCheckoutExecutionExecutorV1,
  { readonly kind: "captured_managed_run" }
>;
export interface ProviderManagedActorAdmissionV1 {
  readonly startExecution: OrdinaryCheckoutExecutionRefV1;
  readonly admission: OrdinaryCheckoutAdmissionV1;
  readonly checkpointScopeId: CheckpointScopeId;
  readonly providerThreadId: ProviderThreadId;
}

export const ProviderManagedActorSourceV1 = Schema.Struct({
  sourceId: TrimmedNonEmptyString,
  driver: ProviderDriverKind,
  instanceId: ProviderInstanceId,
  providerSessionId: ProviderSessionId,
  providerThreadId: ProviderThreadId,
  threadId: ThreadId,
  runtimeGeneration: Schema.optionalKey(TrimmedNonEmptyString),
  nativeThreadId: Schema.optionalKey(TrimmedNonEmptyString),
  nativeTurnId: Schema.optionalKey(TrimmedNonEmptyString),
  nativeTaskId: Schema.optionalKey(TrimmedNonEmptyString),
  nativeRequestId: Schema.optionalKey(TrimmedNonEmptyString),
});
export type ProviderManagedActorSourceV1 = typeof ProviderManagedActorSourceV1.Type;
const outcome = Schema.Literals(["completed", "failed", "interrupted"]);
const nativeIdentityKeys = [
  "nativeThreadId",
  "nativeTurnId",
  "nativeTaskId",
  "nativeRequestId",
] as const;
const observedAt = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value) &&
      !Number.isNaN(Date.parse(value)),
  ),
);
const NativeEndpoint = Schema.Struct({
  kind: Schema.Literal("native_endpoint"),
  endpoint: TrimmedNonEmptyString,
  outcome,
  observedAt,
  nativeThreadId: Schema.optionalKey(TrimmedNonEmptyString),
  nativeTurnId: Schema.optionalKey(TrimmedNonEmptyString),
  nativeTaskId: Schema.optionalKey(TrimmedNonEmptyString),
  nativeRequestId: Schema.optionalKey(TrimmedNonEmptyString),
}).check(Schema.makeFilter((value) => nativeIdentityKeys.some((key) => value[key] !== undefined)));
const QualifiedIdle = Schema.Struct({
  kind: Schema.Literal("qualified_idle"),
  endpoint: TrimmedNonEmptyString,
  qualification: TrimmedNonEmptyString,
  nativeThreadId: TrimmedNonEmptyString,
  outcome,
  observedAt,
});
const KnownNoEntry = Schema.Struct({
  kind: Schema.Literal("known_no_entry"),
  outcome: Schema.Literal("no_effect"),
  observedAt,
});
const CapturedTargetExit = Schema.Struct({
  kind: Schema.Literal("captured_target_exit"),
  sourceId: TrimmedNonEmptyString,
  targetId: TrimmedNonEmptyString,
  exitCode: Schema.Int,
  outcome,
  observedAt,
});
const PrimaryEvidence = Schema.Union([
  NativeEndpoint,
  QualifiedIdle,
  KnownNoEntry,
  CapturedTargetExit,
]);
export type ProviderManagedActorPrimaryEndEvidenceV1 = typeof PrimaryEvidence.Type;
const TaskJoin = Schema.Struct({
  kind: Schema.Literal("task_join"),
  taskId: TrimmedNonEmptyString,
  outcome,
  observedAt,
});
export type ProviderManagedActorTaskJoinEvidenceV1 = typeof TaskJoin.Type;
export const ProviderManagedActorEndEvidenceV1 = Schema.Union([
  PrimaryEvidence,
  Schema.Struct({
    kind: Schema.Literal("task_joins"),
    tasks: Schema.Array(TaskJoin).check(Schema.makeFilter((items) => items.length > 0)),
  }),
  Schema.Struct({
    kind: Schema.Literal("endpoint_and_task_joins"),
    endpoint: PrimaryEvidence,
    tasks: Schema.Array(TaskJoin).check(Schema.makeFilter((items) => items.length > 0)),
  }),
]);
const actorKind = Schema.Literals([
  "foreground",
  "native_request",
  "retry",
  "background_command",
  "tool",
  "subagent",
  "continuation",
  "client_terminal",
]);
export type ProviderManagedActorKindV1 = typeof actorKind.Type;
const ActorRow = Schema.Struct({
  actorId: TrimmedNonEmptyString,
  parentActorId: Schema.NullOr(TrimmedNonEmptyString),
  kind: actorKind,
  source: ProviderManagedActorSourceV1,
  endEvidence: ProviderManagedActorEndEvidenceV1,
});
const managedExecution = OrdinaryCheckoutExecutionRefV1.check(
  Schema.makeFilter((ref) => ref.executor.kind === "captured_managed_run"),
);
export const ProviderManagedActorClosureDescriptorV1 = Schema.Struct({
  version: Schema.Literal(1),
  schema: Schema.Literal("t3.provider-managed-actor-closure/v1"),
  ticketId: TrimmedNonEmptyString,
  managedExecution,
  membershipOrdinal: Schema.Int.check(Schema.makeFilter((value) => value > 0)),
  actorSetSha256: Schema.String.check(Schema.makeFilter((value) => /^[a-f0-9]{64}$/.test(value))),
  closedAt: observedAt,
  actors: Schema.Array(ActorRow).check(Schema.makeFilter((items) => items.length > 0)),
}).check(
  Schema.makeFilter((value) => {
    const executor = value.managedExecution.executor;
    if (
      executor.kind !== "captured_managed_run" ||
      value.membershipOrdinal < value.actors.length ||
      value.actorSetSha256 !== nativeCreationSha256(nativeCreationCanonicalJson(value.actors))
    )
      return false;
    const byId = new Map(value.actors.map((actor) => [actor.actorId, actor]));
    if (byId.size !== value.actors.length) return false;
    return value.actors.every((actor) => {
      if (
        actor.source.driver !== executor.driver ||
        actor.source.instanceId !== executor.binding.instanceId ||
        actor.source.providerSessionId !== executor.binding.providerSessionId ||
        actor.source.threadId !== executor.binding.threadId ||
        actor.source.providerThreadId !== executor.binding.providerThreadId
      )
        return false;
      const ancestors = new Set([actor.actorId]);
      let parent = actor.parentActorId;
      while (parent !== null) {
        const ancestor = byId.get(parent);
        if (ancestor === undefined || ancestors.has(parent)) return false;
        ancestors.add(parent);
        parent = ancestor.parentActorId;
      }
      const endpoint =
        actor.endEvidence.kind === "endpoint_and_task_joins"
          ? actor.endEvidence.endpoint
          : actor.endEvidence.kind === "task_joins"
            ? undefined
            : actor.endEvidence;
      if (
        endpoint?.kind === "native_endpoint" &&
        nativeIdentityKeys.some(
          (key) => endpoint[key] !== undefined && endpoint[key] !== actor.source[key],
        )
      )
        return false;
      if (
        endpoint?.kind === "qualified_idle" &&
        endpoint.nativeThreadId !== actor.source.nativeThreadId
      )
        return false;
      if (endpoint?.kind === "captured_target_exit" && endpoint.sourceId !== actor.source.sourceId)
        return false;
      const tasks =
        actor.endEvidence.kind === "task_joins" ||
        actor.endEvidence.kind === "endpoint_and_task_joins"
          ? actor.endEvidence.tasks
          : [];
      return new Set(tasks.map((task) => task.taskId)).size === tasks.length;
    });
  }),
);
export type ProviderManagedActorClosureDescriptorV1 =
  typeof ProviderManagedActorClosureDescriptorV1.Type;
export const ProviderManagedActorClosureV1 = ProviderManagedActorClosureDescriptorV1;
export type ProviderManagedActorClosureV1 = ProviderManagedActorClosureDescriptorV1;
declare const observationBrand: unique symbol;
export interface ProviderManagedActorClosureObservationV1 {
  readonly descriptor: ProviderManagedActorClosureDescriptorV1;
  readonly [observationBrand]: true;
}
declare const actorBrand: unique symbol;
export interface ProviderManagedActorV1 {
  readonly actorId: string;
  readonly [actorBrand]: true;
}
export type ProviderManagedActorCompletionModeV1 =
  | "native_endpoint"
  | "task_join"
  | "native_endpoint_and_task_join"
  | "qualified_idle"
  | "known_no_entry"
  | "captured_target_exit";
export type ProviderManagedActorClosureReadV1 =
  | { readonly status: "pending"; readonly ticketId: string }
  | { readonly status: "closed"; readonly observation: ProviderManagedActorClosureObservationV1 }
  | { readonly status: "unknown"; readonly reason: string };

export class ProviderManagedActorCompletionError extends Error {
  override readonly name = "ProviderManagedActorCompletionError";
  readonly reason: string;
  constructor(reason: string, options?: ErrorOptions) {
    super(`Managed actor completion rejected: ${reason}.`, options);
    this.reason = reason;
  }
}
export interface ProviderManagedActorProducerGuardsV1<E = never> {
  /** Bounded captured-memory checks; completion may survive deliberate stopping of that same source. */
  readonly revalidateMutation: Effect.Effect<void, E>;
  readonly revalidateCompletion: Effect.Effect<void, E>;
}
export interface ProviderManagedActorIssuerV1 {
  readonly ticketId: string;
  readonly readManagedExecution: Effect.Effect<OrdinaryCheckoutExecutionRefV1 | undefined>;
  readonly admitActor: (input: {
    readonly parent?: ProviderManagedActorV1;
    readonly kind: ProviderManagedActorKindV1;
    readonly actualSource: ProviderManagedActorSourceV1;
    readonly completionMode: ProviderManagedActorCompletionModeV1;
  }) => Effect.Effect<ProviderManagedActorV1, ProviderManagedActorCompletionError>;
  readonly markActorEntered: (
    actor: ProviderManagedActorV1,
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly bindActorNativeIdentity: (
    actor: ProviderManagedActorV1,
    identity: Partial<
      Pick<
        ProviderManagedActorSourceV1,
        "nativeThreadId" | "nativeTurnId" | "nativeTaskId" | "nativeRequestId"
      >
    >,
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly requireTaskJoin: (
    actor: ProviderManagedActorV1,
    input: { readonly taskId: string; readonly fiber: Fiber.Fiber<unknown, unknown> },
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly joinTask: (
    actor: ProviderManagedActorV1,
    taskId: string,
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly recordNativeEndpoint: (
    actor: ProviderManagedActorV1,
    evidence: ProviderManagedActorPrimaryEndEvidenceV1,
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly retainUnknown: (
    actor: ProviderManagedActorV1,
    reason: string,
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly seal: Effect.Effect<void, ProviderManagedActorCompletionError>;
}
export interface ProviderManagedActorRunReaderV1 {
  readonly ticketId: string;
  readonly readClosure: Effect.Effect<ProviderManagedActorClosureReadV1>;
  readonly awaitNativeClosure: Effect.Effect<ProviderManagedActorClosureReadV1>;
  /** Completed original actors may qualify their captured binding before activation commits. */
  readonly revalidateCompletionBinding: Effect.Effect<void, ProviderManagedActorCompletionError>;
  readonly bindManagedExecution: (
    actualCommittedRef: OrdinaryCheckoutExecutionRefV1,
  ) => Effect.Effect<void, ProviderManagedActorCompletionError>;
  /** Owning scope cleanup after committed handoff; release never supplies completion evidence. */
  readonly release: Effect.Effect<void>;
}
export type ProviderManagedActorProducerFactoryV1<E = never, GuardE = never> = (input: {
  readonly admission: ProviderManagedActorAdmissionV1;
  readonly issuer: ProviderManagedActorIssuerV1;
}) => Effect.Effect<ProviderManagedActorProducerGuardsV1<GuardE>, E>;

const producers = new WeakMap<
  object,
  (
    admission: ProviderManagedActorAdmissionV1,
  ) => Effect.Effect<ProviderManagedActorRunReaderV1, ProviderManagedActorCompletionError>
>();
const ticketsByRuntime = new WeakMap<
  object,
  Map<string, Effect.Effect<ProviderManagedActorRunReaderV1, ProviderManagedActorCompletionError>>
>();
const readersByRef = new Map<
  string,
  { readonly bytes: string; readonly reader: ProviderManagedActorRunReaderV1 }
>();
const issued = new WeakMap<
  object,
  {
    readonly descriptor: ProviderManagedActorClosureDescriptorV1;
    readonly bytes: string;
    readonly revalidateIssued: Effect.Effect<void, ProviderManagedActorCompletionError>;
  }
>();
const executionContext = Context.Reference<OrdinaryCheckoutExecutionRefV1 | undefined>(
  "t3/provider-managed-actor-execution",
  { defaultValue: () => undefined },
);
const reject = (reason: string) => new ProviderManagedActorCompletionError(reason);
const encodeRef = Schema.encodeSync(OrdinaryCheckoutExecutionRefV1);
const decodeRef = Schema.decodeUnknownSync(OrdinaryCheckoutExecutionRefV1, {
  onExcessProperty: "error",
});
const encodeAdmission = Schema.encodeSync(OrdinaryCheckoutAdmissionV1);
const decodeAdmission = Schema.decodeUnknownSync(OrdinaryCheckoutAdmissionV1, {
  onExcessProperty: "error",
});
const encodeDescriptor = Schema.encodeSync(ProviderManagedActorClosureDescriptorV1);
const bytes = (value: unknown) => nativeCreationCanonicalJson(value);
const refBytes = (ref: OrdinaryCheckoutExecutionRefV1) => bytes(encodeRef(ref));
function freeze<A>(value: A): A {
  if (typeof value === "object" && value !== null) {
    if (DateTime.isDateTime(value)) Object.freeze(DateTime.toPartsUtc(value));
    for (const child of Object.values(value)) if (typeof child !== "function") freeze(child);
    Object.freeze(value);
  }
  return value;
}

export function registerProviderManagedActorProducer<E, GuardE>(
  actualRawRuntime: ProviderAdapterV2SessionRuntime,
  producerFactory: ProviderManagedActorProducerFactoryV1<E, GuardE>,
): void {
  if (producers.has(actualRawRuntime)) throw reject("producer_already_registered");
  producers.set(actualRawRuntime, (admission) =>
    makeTicket(actualRawRuntime, admission, producerFactory),
  );
}

function snapshotAdmission(
  input: ProviderManagedActorAdmissionV1,
): ProviderManagedActorAdmissionV1 {
  const startExecution = decodeRef(encodeRef(input.startExecution));
  const admission = decodeAdmission(encodeAdmission(input.admission));
  const checkpointScopeId = Schema.decodeUnknownSync(CheckpointScopeId)(input.checkpointScopeId);
  const providerThreadId = Schema.decodeUnknownSync(ProviderThreadId)(input.providerThreadId);
  if (
    startExecution.executor.kind !== "actual_outbox_claim" ||
    startExecution.originalUse.source.kind !== "outbox" ||
    admission.run === null ||
    !ordinaryCheckoutAdmissionMatchesV1(admission) ||
    bytes(startExecution.originalUse.admission) !==
      bytes(ordinaryCheckoutAdmissionRefV1(admission)) ||
    bytes(ordinaryCheckoutLeaseIdentityV1(startExecution.originalUse.lease)) !==
      bytes(ordinaryCheckoutLeaseIdentityV1(admission.capture.lease)) ||
    bytes(startExecution.executor.source) !== bytes(startExecution.originalUse.source) ||
    startExecution.executor.source.link.threadId !== admission.capture.threadId ||
    startExecution.executor.source.link.commandId !== admission.capture.commandId ||
    bytes(startExecution.executor.source.link.admission) !==
      bytes(startExecution.originalUse.admission)
  )
    throw reject("start_admission_mismatch");
  return freeze({ startExecution, admission, checkpointScopeId, providerThreadId });
}

export function prepareProviderManagedActorRun(
  rawRuntime: ProviderAdapterV2SessionRuntime,
  admission: ProviderManagedActorAdmissionV1,
): Effect.Effect<ProviderManagedActorRunReaderV1, ProviderManagedActorCompletionError> {
  return Effect.suspend(() => {
    const factory = producers.get(rawRuntime);
    if (factory === undefined) return Effect.fail(reject("producer_missing"));
    let captured: ProviderManagedActorAdmissionV1;
    try {
      captured = snapshotAdmission(admission);
    } catch {
      return Effect.fail(reject("invalid_admission"));
    }
    const key = bytes([
      captured.startExecution.originalUse.operationId,
      captured.admission.run!.runAttemptId,
      captured.providerThreadId,
    ]);
    let tickets = ticketsByRuntime.get(rawRuntime);
    if (tickets === undefined) ticketsByRuntime.set(rawRuntime, (tickets = new Map()));
    const reuse = (
      existing: Effect.Effect<ProviderManagedActorRunReaderV1, ProviderManagedActorCompletionError>,
    ) =>
      existing.pipe(
        Effect.flatMap((reader) => {
          const expected = preparedAdmissions.get(reader);
          return expected ===
            bytes(encodeAdmission(captured.admission)) +
              refBytes(captured.startExecution) +
              captured.checkpointScopeId +
              captured.providerThreadId
            ? Effect.succeed(reader)
            : Effect.fail(reject("ticket_admission_conflict"));
        }),
      );
    const existing = tickets.get(key);
    if (existing !== undefined) return reuse(existing);
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        // Publish the shared preparation before factory work can yield or native dispatch can start.
        const gate = yield* Deferred.make<
          ProviderManagedActorRunReaderV1,
          ProviderManagedActorCompletionError
        >();
        const pending = Deferred.await(gate);
        if (tickets!.has(key)) return yield* reuse(tickets!.get(key)!);
        tickets!.set(key, pending);
        const result = yield* restore(factory(captured)).pipe(Effect.exit);
        yield* Deferred.done(gate, result);
        return yield* pending;
      }),
    );
  });
}
const preparedAdmissions = new WeakMap<object, string>();

function makeTicket<E, GuardE>(
  runtime: ProviderAdapterV2SessionRuntime,
  admission: ProviderManagedActorAdmissionV1,
  factory: ProviderManagedActorProducerFactoryV1<E, GuardE>,
): Effect.Effect<ProviderManagedActorRunReaderV1, ProviderManagedActorCompletionError> {
  return Effect.gen(function* () {
    const permit = yield* Semaphore.make(1);
    let changed = yield* Deferred.make<void>();
    const ticketId = NodeCrypto.randomUUID();
    type Actor = {
      readonly handle: ProviderManagedActorV1;
      readonly parentActorId: string | null;
      readonly kind: ProviderManagedActorKindV1;
      source: ProviderManagedActorSourceV1;
      readonly mode: ProviderManagedActorCompletionModeV1;
      entered: boolean;
      sealed: boolean;
      endpoint?: ProviderManagedActorPrimaryEndEvidenceV1;
      readonly tasks: Map<
        string,
        {
          readonly fiber: Fiber.Fiber<unknown, unknown>;
          evidence?: ProviderManagedActorTaskJoinEvidenceV1;
        }
      >;
    };
    const actors = new Map<ProviderManagedActorV1, Actor>();
    let membershipOrdinal = 0;
    let sealed = false;
    let unknown: string | undefined;
    let managedRef: OrdinaryCheckoutExecutionRefV1 | undefined;
    let observation: ProviderManagedActorClosureObservationV1 | undefined;
    let released = false;
    let guards: ProviderManagedActorProducerGuardsV1<GuardE> | undefined;
    const signal = Effect.gen(function* () {
      const prior = changed;
      changed = yield* Deferred.make<void>();
      yield* Deferred.succeed(prior, undefined);
    });
    const actorFor = (handle: ProviderManagedActorV1) => {
      const actor = actors.get(handle);
      if (actor === undefined) throw reject("actor_not_issued_for_ticket");
      return actor;
    };
    const synchronized = <A, E>(
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, ProviderManagedActorCompletionError> =>
      effect.pipe(
        permit.withPermits(1),
        Effect.mapError((error) =>
          error instanceof ProviderManagedActorCompletionError
            ? error
            : reject("producer_check_failed"),
        ),
        Effect.catchDefect(() => Effect.fail(reject("invalid_producer_fact"))),
      );
    const mutation = <A, E>(effect: Effect.Effect<A, E>) =>
      synchronized(
        Effect.gen(function* () {
          if (unknown !== undefined || sealed || released)
            return yield* Effect.fail(reject("admission_closed"));
          if (guards !== undefined) yield* guards.revalidateMutation;
          return yield* effect;
        }),
      );
    const issuer = Object.freeze<ProviderManagedActorIssuerV1>({
      ticketId,
      readManagedExecution: Effect.sync(() => managedRef).pipe(permit.withPermits(1)),
      admitActor: (input) =>
        mutation(
          Effect.gen(function* () {
            const parent = input.parent === undefined ? undefined : actorFor(input.parent);
            if (parent?.sealed) return yield* Effect.fail(reject("parent_admission_closed"));
            const source = freeze(
              yield* Schema.decodeUnknownEffect(ProviderManagedActorSourceV1, {
                onExcessProperty: "error",
              })(input.actualSource).pipe(Effect.mapError(() => reject("invalid_producer_fact"))),
            );
            if (
              source.driver !== runtime.driver ||
              source.instanceId !== runtime.instanceId ||
              source.providerSessionId !== runtime.providerSessionId ||
              source.threadId !== admission.admission.capture.threadId ||
              source.providerThreadId !== admission.providerThreadId
            )
              return yield* Effect.fail(reject("actor_source_mismatch"));
            const handle = Object.freeze({
              actorId: NodeCrypto.randomUUID(),
            }) as ProviderManagedActorV1;
            actors.set(handle, {
              handle,
              parentActorId: parent?.handle.actorId ?? null,
              kind: input.kind,
              source,
              mode: input.completionMode,
              entered: false,
              sealed: false,
              tasks: new Map(),
            });
            membershipOrdinal++;
            yield* signal;
            return handle;
          }),
        ),
      markActorEntered: (handle) =>
        mutation(
          Effect.gen(function* () {
            const actor = actorFor(handle);
            if (guards === undefined) return yield* Effect.fail(reject("producer_not_prepared"));
            if (actor.sealed || actor.entered)
              return yield* Effect.fail(reject("actor_entry_closed"));
            actor.entered = true;
            yield* signal;
          }),
        ),
      bindActorNativeIdentity: (handle, identity) =>
        synchronized(
          Effect.gen(function* () {
            const actor = actorFor(handle);
            if (unknown !== undefined || released)
              return yield* Effect.fail(reject("ticket_closed"));
            const keys = Object.keys(identity);
            if (
              keys.length === 0 ||
              keys.some(
                (key) => !nativeIdentityKeys.includes(key as (typeof nativeIdentityKeys)[number]),
              )
            )
              return yield* Effect.fail(reject("native_identity_invalid"));
            for (const key of nativeIdentityKeys)
              if (
                identity[key] !== undefined &&
                actor.source[key] !== undefined &&
                identity[key] !== actor.source[key]
              )
                return yield* Effect.fail(reject("native_identity_conflict"));
            if (
              nativeIdentityKeys.every(
                (key) => identity[key] === undefined || identity[key] === actor.source[key],
              )
            )
              return;
            if (observation !== undefined) return yield* Effect.fail(reject("ticket_closed"));
            const source = freeze(
              yield* Schema.decodeUnknownEffect(ProviderManagedActorSourceV1, {
                onExcessProperty: "error",
              })({ ...actor.source, ...identity }).pipe(
                Effect.mapError(() => reject("invalid_producer_fact")),
              ),
            );
            if (
              actor.endpoint?.kind === "native_endpoint" &&
              nativeIdentityKeys.some(
                (key) =>
                  actor.endpoint?.kind === "native_endpoint" &&
                  actor.endpoint[key] !== undefined &&
                  source[key] !== undefined &&
                  actor.endpoint[key] !== source[key],
              )
            ) {
              unknown = "endpoint_source_mismatch";
              yield* signal;
              return yield* Effect.fail(reject(unknown));
            }
            actor.source = source;
            membershipOrdinal++;
            yield* signal;
          }),
        ),
      requireTaskJoin: (handle, input) =>
        mutation(
          Effect.gen(function* () {
            const actor = actorFor(handle);
            if (actor.sealed || actor.tasks.has(input.taskId) || input.taskId.trim().length === 0)
              return yield* Effect.fail(reject("task_registration_closed"));
            actor.tasks.set(input.taskId, { fiber: input.fiber });
            membershipOrdinal++;
            yield* signal;
          }),
        ),
      joinTask: (handle, taskId) =>
        Effect.gen(function* () {
          const task = yield* synchronized(Effect.sync(() => actorFor(handle).tasks.get(taskId)));
          if (task === undefined) return yield* Effect.fail(reject("task_not_registered"));
          const exit = yield* Fiber.await(task.fiber);
          const at = DateTime.formatIso(yield* DateTime.now);
          yield* synchronized(
            Effect.gen(function* () {
              const actor = actorFor(handle);
              if (actor.tasks.get(taskId) !== task)
                return yield* Effect.fail(reject("task_changed"));
              task.evidence ??= freeze({
                kind: "task_join",
                taskId,
                outcome: Exit.isSuccess(exit)
                  ? "completed"
                  : Cause.hasInterruptsOnly(exit.cause)
                    ? "interrupted"
                    : "failed",
                observedAt: at,
              });
              if (actor.mode === "task_join") actor.sealed = true;
              yield* signal;
            }),
          );
        }),
      recordNativeEndpoint: (handle, evidence) =>
        synchronized(
          Effect.gen(function* () {
            const actor = actorFor(handle);
            if (unknown !== undefined || released)
              return yield* Effect.fail(reject("endpoint_already_closed"));
            const captured = freeze(
              yield* Schema.decodeUnknownEffect(PrimaryEvidence, { onExcessProperty: "error" })(
                evidence,
              ).pipe(Effect.mapError(() => reject("invalid_producer_fact"))),
            );
            if (actor.endpoint !== undefined) {
              if (bytes({ ...actor.endpoint, observedAt: captured.observedAt }) === bytes(captured))
                return;
              unknown = "endpoint_conflict";
              yield* signal;
              return yield* Effect.fail(reject(unknown));
            }
            const expected =
              actor.mode === "native_endpoint_and_task_join" ? "native_endpoint" : actor.mode;
            if (
              captured.kind !== expected ||
              (captured.kind === "known_no_entry" ? actor.entered : !actor.entered)
            )
              return yield* Effect.fail(reject("endpoint_mode_mismatch"));
            if (captured.kind === "native_endpoint") {
              for (const key of nativeIdentityKeys)
                if (
                  captured[key] !== undefined &&
                  actor.source[key] !== undefined &&
                  captured[key] !== actor.source[key]
                )
                  return yield* Effect.fail(reject("endpoint_source_mismatch"));
            } else if (
              captured.kind === "qualified_idle" &&
              captured.nativeThreadId !== actor.source.nativeThreadId
            )
              return yield* Effect.fail(reject("endpoint_source_mismatch"));
            else if (
              captured.kind === "captured_target_exit" &&
              captured.sourceId !== actor.source.sourceId
            )
              return yield* Effect.fail(reject("endpoint_source_mismatch"));
            actor.endpoint = captured;
            actor.sealed = true;
            yield* signal;
          }),
        ),
      retainUnknown: (handle, reason) =>
        synchronized(
          Effect.gen(function* () {
            actorFor(handle);
            unknown ??= reason.trim().length === 0 ? "actor_outcome_unknown" : reason;
            yield* signal;
          }),
        ),
      seal: synchronized(
        Effect.gen(function* () {
          if (guards !== undefined) yield* guards.revalidateMutation;
          sealed = true;
          yield* signal;
        }),
      ),
    });
    guards = yield* Effect.suspend(() => factory({ admission, issuer })).pipe(
      Effect.mapError(() => reject("producer_preparation_failed")),
      Effect.catchDefect(() => Effect.fail(reject("producer_preparation_failed"))),
    );
    if (
      typeof guards !== "object" ||
      guards === null ||
      !Effect.isEffect(guards.revalidateMutation) ||
      !Effect.isEffect(guards.revalidateCompletion)
    )
      return yield* Effect.fail(reject("invalid_producer_guards"));
    const actorsClosed = () =>
      sealed &&
      actors.size > 0 &&
      [...actors.values()].every(
        (actor) =>
          actor.sealed &&
          [...actor.tasks.values()].every((task) => task.evidence !== undefined) &&
          (actor.mode === "task_join"
            ? actor.entered && actor.tasks.size > 0
            : actor.endpoint !== undefined) &&
          (actor.endpoint?.kind !== "native_endpoint" ||
            nativeIdentityKeys.every(
              (key) =>
                actor.endpoint?.kind === "native_endpoint" &&
                (actor.endpoint[key] === undefined || actor.endpoint[key] === actor.source[key]),
            )) &&
          (actor.mode !== "native_endpoint_and_task_join" || actor.tasks.size > 0),
      );
    const ready = () => managedRef !== undefined && actorsClosed();
    const revalidateCompletionBinding = synchronized(
      Effect.gen(function* () {
        if (released || unknown !== undefined || !actorsClosed())
          return yield* Effect.fail(reject("completion_binding_not_ready"));
        const current = yield* guards!.revalidateCompletion.pipe(Effect.result);
        if (current._tag === "Failure") {
          unknown = "completion_capture_changed";
          yield* signal;
          return yield* Effect.fail(reject(unknown));
        }
      }),
    );
    const readClosure: Effect.Effect<ProviderManagedActorClosureReadV1> = synchronized(
      Effect.gen(function* () {
        if (unknown !== undefined) return { status: "unknown" as const, reason: unknown };
        if (released) return { status: "unknown" as const, reason: "ticket_released" };
        if (sealed && actors.size === 0)
          return { status: "unknown" as const, reason: "actor_set_missing" };
        if (!ready()) return { status: "pending" as const, ticketId };
        const current = yield* guards!.revalidateCompletion.pipe(Effect.result);
        if (current._tag === "Failure") {
          unknown = "completion_capture_changed";
          yield* signal;
          return { status: "unknown" as const, reason: unknown };
        }
        if (observation === undefined) {
          const rows = [...actors.values()].map((actor) => {
            const tasks = [...actor.tasks.values()].map((task) => task.evidence!);
            const endEvidence =
              actor.endpoint === undefined
                ? { kind: "task_joins" as const, tasks }
                : tasks.length === 0
                  ? actor.endpoint
                  : { kind: "endpoint_and_task_joins" as const, endpoint: actor.endpoint, tasks };
            return {
              actorId: actor.handle.actorId,
              parentActorId: actor.parentActorId,
              kind: actor.kind,
              source: actor.source,
              endEvidence,
            };
          });
          const descriptor = freeze(
            yield* Schema.decodeUnknownEffect(ProviderManagedActorClosureDescriptorV1, {
              onExcessProperty: "error",
            })({
              version: 1,
              schema: "t3.provider-managed-actor-closure/v1",
              ticketId,
              managedExecution: encodeRef(managedRef!),
              membershipOrdinal,
              actorSetSha256: nativeCreationSha256(bytes(rows)),
              closedAt: DateTime.formatIso(yield* DateTime.now),
              actors: rows,
            }).pipe(Effect.mapError(() => reject("invalid_producer_fact"))),
          );
          observation = Object.freeze({ descriptor }) as ProviderManagedActorClosureObservationV1;
          const original = observation;
          issued.set(original, {
            descriptor,
            bytes: bytes(encodeDescriptor(descriptor)),
            revalidateIssued: Effect.gen(function* () {
              yield* guards!.revalidateCompletion.pipe(
                Effect.mapError((error) =>
                  error instanceof ProviderManagedActorCompletionError
                    ? error
                    : new ProviderManagedActorCompletionError("producer_check_failed", {
                        cause: error,
                      }),
                ),
              );
              if (unknown !== undefined || observation !== original || !ready())
                return yield* Effect.fail(reject("issued_closure_changed"));
            }),
          });
        }
        return { status: "closed" as const, observation };
      }),
    ).pipe(
      Effect.catch(() =>
        Effect.succeed({ status: "unknown" as const, reason: "completion_observation_failed" }),
      ),
    );
    const reader = Object.freeze<ProviderManagedActorRunReaderV1>({
      ticketId,
      readClosure,
      revalidateCompletionBinding,
      awaitNativeClosure: Effect.gen(function* () {
        while (true) {
          const wait = changed;
          const value = yield* readClosure;
          if (value.status !== "pending") return value;
          yield* Deferred.await(wait);
        }
      }),
      bindManagedExecution: (ref) =>
        synchronized(
          Effect.gen(function* () {
            if (released) return yield* Effect.fail(reject("ticket_released"));
            const captured = freeze(decodeRef(encodeRef(ref)));
            const executor = captured.executor;
            if (
              executor.kind !== "captured_managed_run" ||
              bytes(encodeRef(captured).originalUse) !==
                bytes(encodeRef(admission.startExecution).originalUse) ||
              executor.checkpointScopeId !== admission.checkpointScopeId ||
              bytes(executor.run) !== bytes(admission.admission.run) ||
              executor.driver !== runtime.driver ||
              executor.binding.instanceId !== runtime.instanceId ||
              executor.binding.providerSessionId !== runtime.providerSessionId ||
              executor.binding.threadId !== admission.admission.capture.threadId ||
              executor.binding.providerThreadId !== admission.providerThreadId ||
              [...actors.values()].some(
                (actor) => actor.source.providerThreadId !== executor.binding.providerThreadId,
              )
            )
              return yield* Effect.fail(reject("managed_execution_mismatch"));
            if (managedRef !== undefined && refBytes(managedRef) !== refBytes(captured))
              return yield* Effect.fail(reject("managed_execution_conflict"));
            const prior = readersByRef.get(captured.associationId);
            if (prior !== undefined && prior.reader !== reader)
              return yield* Effect.fail(reject("managed_execution_already_bound"));
            managedRef = captured;
            readersByRef.set(captured.associationId, { bytes: refBytes(captured), reader });
            yield* signal;
          }),
        ),
      release: synchronized(
        Effect.gen(function* () {
          released = true;
          if (
            managedRef !== undefined &&
            readersByRef.get(managedRef.associationId)?.reader === reader
          )
            readersByRef.delete(managedRef.associationId);
          const key = bytes([
            admission.startExecution.originalUse.operationId,
            admission.admission.run!.runAttemptId,
            admission.providerThreadId,
          ]);
          ticketsByRuntime.get(runtime)?.set(key, Effect.fail(reject("ticket_released")));
          managedRef = undefined;
          actors.clear();
          yield* signal;
        }),
      ).pipe(Effect.orDie),
    });
    preparedAdmissions.set(
      reader,
      bytes(encodeAdmission(admission.admission)) +
        refBytes(admission.startExecution) +
        admission.checkpointScopeId +
        admission.providerThreadId,
    );
    return reader;
  });
}

export function readProviderManagedActorClosure(
  managedRef: OrdinaryCheckoutExecutionRefV1,
): Effect.Effect<ProviderManagedActorClosureReadV1> {
  return Effect.suspend(() => {
    const bound = readersByRef.get(managedRef.associationId);
    try {
      return bound !== undefined && bound.bytes === refBytes(managedRef)
        ? bound.reader.readClosure
        : Effect.succeed({ status: "unknown", reason: "managed_execution_not_bound" });
    } catch {
      return Effect.succeed({ status: "unknown", reason: "invalid_managed_execution" });
    }
  });
}

// Validate the original issued object before any SQL decoder erases its private provenance.
export function validateIssuedProviderManagedActorClosure(
  observation: unknown,
  managedRef: OrdinaryCheckoutExecutionRefV1,
): {
  readonly descriptor: ProviderManagedActorClosureDescriptorV1;
  readonly revalidateIssued: Effect.Effect<void, ProviderManagedActorCompletionError>;
} | null {
  if (typeof observation !== "object" || observation === null) return null;
  const proof = issued.get(observation);
  if (proof === undefined) return null;
  try {
    if (
      (observation as ProviderManagedActorClosureObservationV1).descriptor !== proof.descriptor ||
      bytes(encodeDescriptor(proof.descriptor)) !== proof.bytes ||
      refBytes(proof.descriptor.managedExecution) !== refBytes(managedRef)
    )
      return null;
    return { descriptor: proof.descriptor, revalidateIssued: proof.revalidateIssued };
  } catch {
    return null;
  }
}

export const readProviderManagedActorExecution = Effect.gen(function* () {
  return yield* executionContext;
});
export function withProviderManagedActorExecution<A, E, R>(
  actualRef: OrdinaryCheckoutExecutionRefV1,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> {
  return effect.pipe(
    Effect.provideService(executionContext, freeze(decodeRef(encodeRef(actualRef)))),
  );
}
