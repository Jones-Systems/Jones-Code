import {
  readRuntimeUseTarget,
  revalidateRuntimeUseConversation,
} from "./RuntimeUseConversation.ts";
import { withRuntimeUseAttachmentPublication } from "./RuntimeUseAttachment.ts";
import * as Context from "effect/Context";
import {
  OrchestrationV2ProviderSession,
  OrchestrationV2ProviderThreadJson,
  type OrchestrationV2ProviderThread,
  OrchestrationV2RuntimeRequestJson,
} from "@t3tools/contracts";
import type { ProviderAdapterV2RuntimeRequestResponseInput } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import { withWorkspaceLease } from "../../workspace/workspaceLease.ts";
import { overlaps } from "../ordinaryCheckout/CheckoutTarget.ts";
import {
  assertCheckedDatabaseOwner,
  bindHeldDatabaseOwner,
  HeldRuntimeOwner,
  type CheckedDatabaseOwnerToken,
} from "./CheckedRuntimeOwner.ts";
import { currentRuntimeUseClaim, validateRuntimeUseClaim } from "./RuntimeUseClaim.ts";
import { decodeOrchestrationEffectPayloadV2 } from "../../orchestration-v2/EffectOutbox.ts";
import { prepareRuntimeUseInvocation } from "./RuntimeUseInvocation.ts";
import { makeRuntimeUseLifetime, RuntimeUseLifetime } from "./RuntimeUseLifetime.ts";
import {
  canonical,
  digest,
  RuntimeUseError,
  runtimeUseError,
  type RuntimeUseBinding,
} from "./RuntimeUseProtocol.ts";
import { captureRuntimeUseSession } from "./RuntimeUseSession.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import type { CodexCapturedProducerIdentity } from "../provider/codex/CodexCapturedLifetime.ts";
import {
  captureRuntimeUseWorkspace,
  assertRuntimeUseDefaultWorkspace,
} from "./RuntimeUseWorkspace.ts";

interface OpenInput {
  readonly threadId: string;
  readonly providerSessionId: string;
  readonly instanceId: string;
  readonly cwd: string | null;
}
const scopeSchema = Schema.Struct({
  id: Schema.String,
  projectId: Schema.String,
  createdAt: Schema.String,
  worktreePath: Schema.NullOr(Schema.String),
  deletedAt: Schema.NullOr(Schema.String),
});
const decodeScope = Schema.decodeUnknownEffect(Schema.fromJsonString(scopeSchema));
const decodeRuntime = Schema.decodeUnknownEffect(
  Schema.Struct({
    providerSession: OrchestrationV2ProviderSession,
  }),
);
const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RuntimeRequestJson),
);
const decodeProviderThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);
const threadBinding = (thread: OrchestrationV2ProviderThread) => ({
  id: thread.id,
  appThreadId: thread.appThreadId,
  providerSessionId: thread.providerSessionId,
  providerInstanceId: thread.providerInstanceId,
  driver: thread.driver,
  nativeThreadRef: thread.nativeThreadRef,
  generation: thread.runtimeIdentity?.runtimeGeneration ?? null,
});
const responseBody = (input: {
  readonly [K in "decision" | "feedback" | "answers"]?:
    | ProviderAdapterV2RuntimeRequestResponseInput[K]
    | undefined;
}) => ({
  ...(input.decision === undefined ? {} : { decision: input.decision }),
  ...(input.feedback === undefined ? {} : { feedback: input.feedback }),
  ...(input.answers === undefined ? {} : { answers: input.answers }),
});
type ScopeSnapshot = {
  readonly thread: typeof scopeSchema.Type;
  readonly projectRoot: string;
  readonly target: Effect.Success<ReturnType<typeof readRuntimeUseTarget>>;
};
class RuntimeUseWorkspaceContext extends Context.Service<
  RuntimeUseWorkspaceContext,
  {
    readonly input: OpenInput;
    readonly owner: CheckedDatabaseOwnerToken;
    readonly prepare: (
      driver: string,
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof prepareRuntimeUseInvocation>>,
      RuntimeUseError
    >;
  }
>()("t3/jones/cleanup/RuntimeUseOpenGuard/RuntimeUseWorkspaceContext") {}

// Absence supplies no cleanup capability. Legacy/raw test graphs keep their
// existing runtime behavior, but can never be counted as covered journal uses.
export const make = Effect.gen(function* () {
  const observationScope = yield* Scope.Scope;
  const held = yield* Effect.serviceOption(HeldRuntimeOwner);
  const storage = yield* Effect.serviceOption(SqlClient.SqlClient);
  const runtimes = new WeakMap<
    object,
    {
      readonly lifetime: Effect.Success<ReturnType<typeof makeRuntimeUseLifetime>>;
      readonly owner: CheckedDatabaseOwnerToken;
      readonly binding: RuntimeUseBinding;
      readonly sessionCreatedAt: string;
      readonly sessionCwd: string;
      readonly covered: Map<string, RuntimeUseBinding>;
    }
  >();
  const readScope = Effect.fn("RuntimeUseOpenGuard.readScope")(function* (
    sql: SqlClient.SqlClient,
    input: OpenInput,
  ) {
    const rows = yield* sql<{
      readonly thread_id: string;
      readonly project_id: string;
      readonly payload_json: string;
      readonly workspace_root: string;
      readonly project_deleted_at: string | null;
    }>`SELECT t.thread_id,t.project_id,t.payload_json,p.workspace_root,p.deleted_at AS project_deleted_at
      FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id=t.project_id
      WHERE t.thread_id=${input.threadId}`;
    const row = rows[0];
    if (row === undefined || row.project_deleted_at !== null)
      return yield* new RuntimeUseError({ reason: "runtime_use_scope_missing" });
    const thread = yield* decodeScope(row.payload_json);
    if (
      thread.id !== input.threadId ||
      thread.id !== row.thread_id ||
      thread.projectId !== row.project_id ||
      thread.deletedAt !== null
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_scope_changed" });
    const target = yield* readRuntimeUseTarget({
      threadId: input.threadId,
      projectId: thread.projectId,
      worktreePath: thread.worktreePath,
      projectRoot: row.workspace_root,
      cwd: input.cwd,
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    return { thread, projectRoot: row.workspace_root, target } satisfies ScopeSnapshot;
  }, Effect.mapError(runtimeUseError));

  const withWorkspace = Effect.fn("RuntimeUseOpenGuard.withWorkspace")(function* <A, E, R>(
    input: OpenInput,
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | RuntimeUseError, R> {
    if (Option.isNone(held)) return yield* effect;
    if (Option.isNone(storage))
      return yield* new RuntimeUseError({ reason: "runtime_use_owner_without_actual_database" });
    const sql = storage.value;
    const owner = yield* bindHeldDatabaseOwner(held.value.token).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError(runtimeUseError),
    );
    const snapshot = yield* readScope(sql, input);
    const workspace = yield* captureRuntimeUseWorkspace(snapshot.target.path);
    const revalidateWorkspace = workspace.revalidate.pipe(
      Effect.andThen(revalidateRuntimeUseConversation(snapshot.target.conversation)),
      Effect.andThen(
        input.cwd === null ? assertRuntimeUseDefaultWorkspace(workspace) : Effect.void,
      ),
    );
    const validate = Effect.gen(function* () {
      if (canonical(yield* readScope(sql, input)) !== canonical(snapshot))
        return yield* new RuntimeUseError({ reason: "runtime_use_original_scope_changed" });
      const deletions = yield* sql<{ readonly worktree_path: string }>`SELECT worktree_path
          FROM jones_deletion_worktree_admissions WHERE state!='completed' ORDER BY worktree_path`;
      if (deletions.some((row) => overlaps(workspace.canonicalPath, row.worktree_path, "/")))
        return yield* new RuntimeUseError({ reason: "runtime_use_conflicting_deletion" });
    }).pipe(Effect.mapError(runtimeUseError));
    const prepare = Effect.fn("RuntimeUseOpenGuard.prepare")(function* (driver: string) {
      const claim = Option.getOrNull(yield* currentRuntimeUseClaim);
      const provider = { sessionId: input.providerSessionId, instanceId: input.instanceId, driver };
      const sessionHash = digest({ providerSessionId: input.providerSessionId });
      const legacyId = `provider-open:v1:${sessionHash}`;
      const claimedPrefix = `provider-open:v2:${sessionHash}:`;
      // The logical session survives process restarts. A durable effect identifies
      // a particular open, including all subsequent worker claims of that effect.
      const useId = claim === null ? legacyId : claimedPrefix + digest(claim.effectId);
      const store = yield* makeRuntimeUseStore;
      const checkPriorOpen = Effect.gen(function* () {
        if (claim === null) {
          // An unclaimed call cannot establish a new intent for this same thread.
          const prior = yield* sql`SELECT 1 FROM jones_workspace_runtime_uses
            WHERE use_id GLOB ${claimedPrefix + "*"} AND thread_id=${input.threadId} LIMIT 1`;
          if (prior.length > 0)
            return yield* new RuntimeUseError({
              reason: "runtime_use_prior_outcome_unknown_no_replay",
            });
          return;
        }
        // Old releases keyed opens only by session. Preserve their original
        // evidence and refuse a replay across the key format change.
        const prior = yield* store.read(legacyId);
        if (prior === null) return;
        const { actor, original, threadId } = prior.binding;
        if (actor.kind !== "provider_open" || canonical(actor.provider) !== canonical(provider))
          return yield* new RuntimeUseError({ reason: "runtime_use_identity_changed" });
        if (
          original.claim?.effectId === claim.effectId ||
          (original.claim === null && threadId === input.threadId)
        )
          return yield* new RuntimeUseError({
            reason: "runtime_use_prior_outcome_unknown_no_replay",
          });
      }).pipe(Effect.mapError(runtimeUseError));
      return yield* prepareRuntimeUseInvocation({
        owner,
        revalidateWorkspace,
        validate,
        validateReservation: checkPriorOpen,
        binding: {
          schema: "jones.workspace-runtime-use/v1",
          useId,
          canonicalPath: workspace.canonicalPath,
          directoryIdentity: workspace.directoryIdentity,
          threadId: input.threadId,
          actor: {
            kind: "provider_open",
            provider,
          },
          original: {
            commandId: claim?.commandId ?? null,
            runId: claim?.runId ?? null,
            attemptId: claim?.attemptId ?? null,
            outboxReference: claim?.effectId ?? null,
            nativeReference: claim?.nativeReference?.claimId ?? null,
            claim,
          },
        },
      }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    });
    return yield* withWorkspaceLease(
      workspace.canonicalPath,
      Effect.gen(function* () {
        yield* Effect.try({ try: () => assertCheckedDatabaseOwner(owner), catch: runtimeUseError });
        yield* revalidateWorkspace;
        yield* sql.withTransaction(validate).pipe(Effect.mapError(runtimeUseError));
        return yield* effect.pipe(
          Effect.provideService(RuntimeUseWorkspaceContext, { input, prepare, owner }),
        );
      }),
    );
  });
  const guardOpen = Effect.fn("RuntimeUseOpenGuard.guardOpen")(function* <A, E, R>(
    input: OpenInput,
    driver: string,
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | RuntimeUseError, R | Scope.Scope> {
    if (Option.isNone(held)) return yield* effect;
    if (Option.isNone(storage))
      return yield* new RuntimeUseError({ reason: "runtime_use_owner_without_actual_database" });
    const context = yield* Effect.serviceOption(RuntimeUseWorkspaceContext);
    if (Option.isNone(context) || canonical(context.value.input) !== canonical(input))
      return yield* new RuntimeUseError({ reason: "runtime_use_open_outside_workspace_exclusion" });
    const use = yield* context.value.prepare(driver);
    const sql = storage.value;
    let openedSession: { readonly createdAt: string; readonly cwd: string } | undefined;
    const lifetime = yield* makeRuntimeUseLifetime({
      binding: use.binding,
      observationScope,
      prepareProducer: Effect.fn("RuntimeUseOpenGuard.prepareProducer")(
        function* (identity: CodexCapturedProducerIdentity) {
          if (
            identity.providerSessionId !== input.providerSessionId ||
            identity.instanceId !== input.instanceId ||
            identity.driver !== driver
          )
            return yield* new RuntimeUseError({ reason: "runtime_use_producer_session_mismatch" });
          // Initial opening has no published session. Each later producer instead
          // freezes its own caller, session and physical target before any write.
          if (openedSession === undefined) {
            if (identity.threadId !== input.threadId)
              return yield* new RuntimeUseError({
                reason: "runtime_use_initial_producer_thread_mismatch",
              });
            return use.producerValidation;
          }
          const current = yield* captureRuntimeUseSession({
            owner: context.value.owner,
            threadId: identity.threadId,
            providerSessionId: input.providerSessionId,
            instanceId: input.instanceId,
            driver,
          });
          if (
            current.snapshot.session.createdAt !== openedSession.createdAt ||
            current.snapshot.session.cwd !== openedSession.cwd
          )
            return yield* new RuntimeUseError({ reason: "runtime_use_original_session_replaced" });
          const revalidate = Effect.gen(function* () {
            yield* current.checkOwner;
            yield* current.workspace.revalidate;
            yield* sql
              .withTransaction(current.validateAttachment)
              .pipe(Effect.mapError(runtimeUseError));
            yield* current.checkOwner;
            yield* current.workspace.revalidate;
          });
          return { validate: current.validateAttachment, revalidate };
        },
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    const seal = lifetime.seal.pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("jones.runtime-use-seal-unavailable", {
          useId: use.binding.useId,
          cause,
        }),
      ),
    );
    return yield* use.invoke(
      lifetime.withOperation(
        effect.pipe(
          Effect.provideService(RuntimeUseLifetime, lifetime),
          Effect.onError(() => seal),
          // Registered after adapter resources, so admission seals before their
          // teardown. Successful finalizers alone still never retire the journal.
          Effect.tap((runtime) =>
            Effect.gen(function* () {
              const captured = yield* decodeRuntime(runtime).pipe(Effect.mapError(runtimeUseError));
              const session = captured.providerSession;
              if (
                session.id !== input.providerSessionId ||
                session.providerInstanceId !== input.instanceId ||
                session.driver !== driver
              )
                return yield* new RuntimeUseError({
                  reason: "runtime_use_opened_session_mismatch",
                });
              openedSession = {
                createdAt: DateTime.formatIso(session.createdAt),
                cwd: session.cwd,
              };
              if (typeof runtime === "object" && runtime !== null)
                runtimes.set(runtime, {
                  lifetime,
                  owner: context.value.owner,
                  binding: use.binding,
                  sessionCreatedAt: DateTime.formatIso(session.createdAt),
                  sessionCwd: session.cwd,
                  covered: new Map([
                    [
                      digest({
                        path: use.binding.canonicalPath,
                        identity: use.binding.directoryIdentity,
                      }),
                      use.binding,
                    ],
                  ]),
                });
            }),
          ),
          Effect.tap(() => Effect.addFinalizer(() => seal)),
        ),
      ),
    );
  });
  const sealRuntime = (runtime: unknown) =>
    Effect.suspend(() => {
      const lifetime =
        typeof runtime === "object" && runtime !== null ? runtimes.get(runtime) : undefined;
      return (
        lifetime?.lifetime.seal.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("jones.runtime-use-seal-unavailable", { cause }),
          ),
        ) ?? Effect.void
      );
    });
  const withRuntimeOperation = <A, E, R>(runtime: unknown, effect: Effect.Effect<A, E, R>) =>
    Effect.suspend((): Effect.Effect<A, E | RuntimeUseError, R> => {
      if (Option.isNone(held)) return effect;
      const lifetime =
        typeof runtime === "object" && runtime !== null ? runtimes.get(runtime) : undefined;
      return lifetime === undefined
        ? Effect.fail(new RuntimeUseError({ reason: "runtime_use_uncovered_runtime" }))
        : lifetime.lifetime.withOperation(effect);
    });
  const prepareUse = Effect.fn("RuntimeUseOpenGuard.prepareUse")(function* (
    runtime: unknown,
    threadId: string | null,
    requestedCwd: string | null | undefined,
    kind: "invoke" | "attachment",
  ) {
    if (Option.isNone(held)) return;
    if (threadId === null)
      return yield* new RuntimeUseError({ reason: "runtime_use_app_thread_unbound" });
    if (Option.isNone(storage))
      return yield* new RuntimeUseError({ reason: "runtime_use_owner_without_actual_database" });
    const entry =
      typeof runtime === "object" && runtime !== null ? runtimes.get(runtime) : undefined;
    if (entry === undefined || entry.binding.actor.kind === "terminal_spawn")
      return yield* new RuntimeUseError({ reason: "runtime_use_uncovered_runtime" });
    const sql = storage.value;
    const provider = entry.binding.actor.provider;
    yield* entry.lifetime.assertActive;
    const captured = yield* captureRuntimeUseSession({
      owner: entry.owner,
      threadId,
      providerSessionId: provider.sessionId,
      instanceId: provider.instanceId,
      driver: provider.driver,
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    if (
      requestedCwd !== undefined &&
      requestedCwd !== null &&
      requestedCwd !== captured.workspace.canonicalPath
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_requested_workspace_mismatch" });
    const revalidateWorkspace = captured.workspace.revalidate.pipe(
      Effect.andThen(
        requestedCwd === null ? assertRuntimeUseDefaultWorkspace(captured.workspace) : Effect.void,
      ),
    );
    if (
      captured.snapshot.session.createdAt !== entry.sessionCreatedAt ||
      captured.snapshot.session.cwd !== entry.sessionCwd
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_original_session_replaced" });
    return yield* withWorkspaceLease(
      captured.workspace.canonicalPath,
      Effect.gen(function* () {
        yield* captured.checkOwner;
        yield* revalidateWorkspace;
        const validate = kind === "attachment" ? captured.validate : captured.validateAttachment;
        yield* sql.withTransaction(validate);
        const workspaceKey = digest({
          path: captured.workspace.canonicalPath,
          identity: captured.workspace.directoryIdentity,
        });
        const key = kind === "attachment" ? digest({ kind, threadId, workspaceKey }) : workspaceKey;
        const prior = entry.covered.get(key);
        const store = yield* makeRuntimeUseStore;
        if (prior !== undefined) {
          const record = yield* store.read(prior.useId);
          if (record?.state !== "pending" || canonical(record.binding) !== canonical(prior))
            return yield* new RuntimeUseError({ reason: "runtime_use_original_coverage_changed" });
        } else {
          const claim = captured.claim;
          // All coverage belongs to this admitted open; sibling and successor
          // runtimes cannot inherit an earlier lifetime's attachment evidence.
          const operation = kind === "attachment" ? "provider-attach" : "provider-invoke";
          const useId =
            entry.binding.original.claim === null
              ? `${operation}:v1:${digest({ sessionId: provider.sessionId, threadId, key })}`
              : `${operation}:v2:${digest({ openUseId: entry.binding.useId, threadId, key })}`;
          const binding: RuntimeUseBinding = {
            schema: "jones.workspace-runtime-use/v1",
            useId,
            canonicalPath: captured.workspace.canonicalPath,
            directoryIdentity: captured.workspace.directoryIdentity,
            threadId,
            actor: { kind: "provider_invoke", provider },
            original: {
              commandId: claim?.commandId ?? null,
              runId: claim?.runId ?? null,
              attemptId: claim?.attemptId ?? null,
              outboxReference: claim?.effectId ?? null,
              nativeReference: claim?.nativeReference?.claimId ?? null,
              claim,
            },
          };
          const use = yield* prepareRuntimeUseInvocation({
            binding,
            owner: entry.owner,
            revalidateWorkspace,
            validate,
          });
          // New workspace attachment gets independent durable uncertainty before
          // dispatch. It cannot borrow retirement from the initial workspace.
          yield* use.invoke(
            entry.lifetime.adoptUse(binding).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  entry.covered.set(key, binding);
                  if (kind === "attachment" && !entry.covered.has(workspaceKey))
                    entry.covered.set(workspaceKey, binding);
                }),
              ),
            ),
          );
        }
        yield* captured.checkOwner;
        yield* revalidateWorkspace;
        yield* entry.lifetime.assertActive;
        return {
          threadId,
          providerSessionId: provider.sessionId,
          validate: captured.validate,
          revalidate: captured.checkOwner.pipe(
            Effect.andThen(revalidateWorkspace),
            Effect.andThen(entry.lifetime.assertActive),
          ),
        };
      }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    );
  }, Effect.mapError(runtimeUseError));
  const beforeUse = Effect.fn("RuntimeUseOpenGuard.beforeUse")(function* (
    runtime: unknown,
    threadId: string | null,
    requestedCwd?: string | null,
  ) {
    yield* prepareUse(runtime, threadId, requestedCwd, "invoke");
  });
  // Attachment validation precedes publication of its SQL binding. It still
  // validates the original live physical session, target and expected caller.
  const withAttachment = Effect.fn("RuntimeUseOpenGuard.withAttachment")(function* <A, E, R>(
    runtime: unknown,
    threadId: string,
    requestedCwd: string | null,
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | RuntimeUseError, R> {
    const admitted = yield* prepareUse(runtime, threadId, requestedCwd, "attachment");
    return yield* admitted === undefined
      ? effect
      : withRuntimeUseAttachmentPublication(admitted, effect);
  });
  const beforeResponse = Effect.fn("RuntimeUseOpenGuard.beforeResponse")(function* (
    runtime: unknown,
    input: ProviderAdapterV2RuntimeRequestResponseInput,
  ) {
    if (Option.isNone(held)) return;
    if (Option.isNone(storage))
      return yield* new RuntimeUseError({ reason: "runtime_use_owner_without_actual_database" });
    const entry =
      typeof runtime === "object" && runtime !== null ? runtimes.get(runtime) : undefined;
    if (entry === undefined || entry.binding.actor.kind === "terminal_spawn")
      return yield* new RuntimeUseError({ reason: "runtime_use_uncovered_runtime" });
    const sql = storage.value;
    const provider = entry.binding.actor.provider;
    const claim = Option.getOrNull(yield* currentRuntimeUseClaim);
    const read = Effect.gen(function* () {
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly node_id: string;
        readonly status: string;
        readonly payload_json: string;
      }>`
        SELECT thread_id,node_id,status,payload_json FROM orchestration_v2_projection_runtime_requests
        WHERE runtime_request_id=${input.requestId}`;
      const row = rows[0];
      if (row === undefined)
        return yield* new RuntimeUseError({ reason: "runtime_use_response_request_missing" });
      const request = yield* decodeRequest(row.payload_json);
      if (
        request.id !== input.requestId ||
        request.nodeId !== row.node_id ||
        request.status !== "resolved" ||
        row.status !== request.status ||
        request.responseCapability.type !== "live" ||
        request.responseCapability.providerSessionId !== provider.sessionId ||
        canonical(responseBody(request)) !== canonical(responseBody(input))
      )
        return yield* new RuntimeUseError({ reason: "runtime_use_response_request_changed" });
      if (claim !== null) {
        yield* validateRuntimeUseClaim(claim).pipe(Effect.provideService(SqlClient.SqlClient, sql));
        const effects = yield* sql<{
          readonly payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_effect_outbox
          WHERE effect_id=${claim.effectId}`;
        const effectRow = effects[0];
        if (effectRow === undefined || claim.threadId !== row.thread_id)
          return yield* new RuntimeUseError({ reason: "runtime_use_response_caller_changed" });
        const payload = yield* decodeOrchestrationEffectPayloadV2(effectRow.payload_json);
        if (
          payload.request.type !== "runtime-request.respond" ||
          payload.request.requestId !== input.requestId ||
          payload.request.providerSessionId !== provider.sessionId ||
          canonical(responseBody(payload.request)) !== canonical(responseBody(input))
        )
          return yield* new RuntimeUseError({
            reason: "runtime_use_response_original_intent_changed",
          });
      }
      return { threadId: row.thread_id, payload: row.payload_json };
    });
    const original = yield* sql.withTransaction(read);
    yield* beforeUse(runtime, original.threadId);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        if (canonical(yield* read) !== canonical(original))
          return yield* new RuntimeUseError({
            reason: "runtime_use_response_changed_before_dispatch",
          });
      }),
    );
  }, Effect.mapError(runtimeUseError));
  const assertBoundThread = Effect.fn("RuntimeUseOpenGuard.assertBoundThread")(function* (
    runtime: unknown,
    thread: OrchestrationV2ProviderThread,
  ) {
    if (Option.isNone(held)) return;
    if (Option.isNone(storage))
      return yield* new RuntimeUseError({ reason: "runtime_use_owner_without_actual_database" });
    const entry =
      typeof runtime === "object" && runtime !== null ? runtimes.get(runtime) : undefined;
    if (entry === undefined || entry.binding.actor.kind === "terminal_spawn")
      return yield* new RuntimeUseError({ reason: "runtime_use_uncovered_runtime" });
    const provider = entry.binding.actor.provider;
    if (
      thread.appThreadId === null ||
      thread.providerSessionId !== provider.sessionId ||
      thread.providerInstanceId !== provider.instanceId ||
      thread.driver !== provider.driver ||
      thread.nativeThreadRef === null ||
      thread.nativeThreadRef.nativeId === null ||
      thread.nativeThreadRef.driver !== provider.driver
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_provider_thread_unbound" });
    const rows = yield* storage.value<{
      readonly thread_id: string;
      readonly payload_json: string;
    }>`
      SELECT thread_id,payload_json FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id=${thread.id}`;
    const row = rows[0];
    if (row === undefined || row.thread_id !== thread.appThreadId)
      return yield* new RuntimeUseError({ reason: "runtime_use_provider_thread_missing" });
    const actual = yield* decodeProviderThread(row.payload_json);
    if (canonical(threadBinding(actual)) !== canonical(threadBinding(thread)))
      return yield* new RuntimeUseError({ reason: "runtime_use_provider_thread_replaced" });
    yield* entry.lifetime.assertActive;
  }, Effect.mapError(runtimeUseError));
  return {
    withWorkspace,
    guardOpen,
    sealRuntime,
    withRuntimeOperation,
    beforeUse,
    withAttachment,
    beforeResponse,
    assertBoundThread,
  };
});
