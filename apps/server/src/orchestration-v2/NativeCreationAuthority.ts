import {
  AuthSessionId,
  CommandId,
  type NativeCreationGuard,
  type NativeCreationEffect,
  NativeCreationHistoricalBinding,
  NativeCreationRejectionCode,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AuthSessionRepository } from "../persistence/AuthSessions.ts";
import {
  NativeCreationRepository,
  type NativeCreationRepositoryError,
} from "../persistence/Services/NativeCreationRepository.ts";
import {
  nativeCreationCanonicalJson,
  validateNativeCreationPreparation,
  type ValidatedNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

export class NativeCreationAuthorityError extends Schema.TaggedError<NativeCreationAuthorityError>()(
  "NativeCreationAuthorityError",
  { code: NativeCreationRejectionCode, message: Schema.String },
) {}
const decodeHistoricalBinding = Schema.decodeUnknownEffect(NativeCreationHistoricalBinding);

export interface NativeCreationResources {
  readonly projectCwd: string;
  readonly branch: string;
  readonly worktreePath: string;
}

export type NativeCreationStage =
  | "claim"
  | "normalization"
  | "tracker_registration"
  | "bootstrap_detachment"
  | "fetch"
  | "worktree"
  | "worktree_ownership"
  | "native_command"
  | "setup"
  | "setup_detachment"
  | "setup_completion_detachment"
  | "cleanup"
  | "deletion_drain"
  | "git_status_refresh";

const executionReferenceFields = {
  version: Schema.Literal(2),
  claimId: Schema.String.check(Schema.isNonEmpty()),
  stageCommandId: CommandId,
  effectId: Schema.String.check(Schema.isNonEmpty()),
  stage: Schema.Literals([
    "claim",
    "normalization",
    "tracker_registration",
    "bootstrap_detachment",
    "fetch",
    "worktree",
    "worktree_ownership",
    "native_command",
    "setup",
    "setup_detachment",
    "setup_completion_detachment",
    "cleanup",
    "deletion_drain",
    "git_status_refresh",
  ]),
};
const executionReference = Schema.Struct(executionReferenceFields);

// Persisted references identify ledger work; they never establish current authority.
export const NativeCreationExecutionReferenceV2 = Schema.flip(
  Schema.flip(executionReference).check(
    Schema.makeFilter((value) =>
      Reflect.ownKeys(value).every((key) => Object.hasOwn(executionReferenceFields, key)),
    ),
  ),
);
export type NativeCreationExecutionReferenceV2 = typeof NativeCreationExecutionReferenceV2.Type;

export interface NativeCreationAuthorityInput {
  readonly actorSessionId: AuthSessionId;
  readonly preparation: ValidatedNativeCreationPreparation;
  readonly guard: NativeCreationGuard;
  readonly resources: NativeCreationResources;
  readonly stage: NativeCreationStage;
  readonly recoveryScopeId?: string;
  readonly recoveryResource?: Extract<NativeCreationEffect, { kind: "cleanup" }>["resource"];
}

declare const issuedExecutionContext: unique symbol;

export interface NativeCreationExecutionContextV2 {
  readonly [issuedExecutionContext]: true;
}

export interface NativeCreationExecutionStageInput {
  readonly resources: NativeCreationResources;
  readonly stage: NativeCreationStage;
  readonly recoveryScopeId?: string;
  readonly recoveryResource?: NativeCreationAuthorityInput["recoveryResource"];
}

const issuedExecutions = new WeakMap<
  NativeCreationExecutionContextV2,
  {
    readonly reference: NativeCreationExecutionReferenceV2;
    readonly authorize: (input: NativeCreationExecutionStageInput) => Effect.Effect<
      NativeCreationHistoricalBinding,
      NativeCreationAuthorityError
    >;
  }
>();

// Historical correlation carries no current permission or execution outcome.
export const getNativeCreationExecutionReference = (
  context: NativeCreationExecutionContextV2,
): NativeCreationExecutionReferenceV2 | null => issuedExecutions.get(context)?.reference ?? null;

// A recheck does not replace the separate durable start required by each actual effect.
export const authorizeNativeCreationExecution = Effect.fn("authorizeNativeCreationExecution")(
  function* (context: NativeCreationExecutionContextV2, input: NativeCreationExecutionStageInput) {
    const execution = issuedExecutions.get(context);
    if (execution === undefined) {
      return yield* new NativeCreationAuthorityError({
        code: "unsupported_authority",
        message: "Native execution context was not issued by an authority",
      });
    }
    return yield* execution.authorize(input);
  },
);

function freezePreparation<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freezePreparation(child);
    Object.freeze(value);
  }
  return value;
}

export interface NativeCreationGrant {
  readonly grantId: string;
  readonly revision: number;
  readonly actorSessionId: AuthSessionId;
  readonly issuerId: string;
  readonly expiresAt: DateTime.Utc;
  readonly revoked: boolean;
  readonly operationId: string;
  readonly preparationId: string;
  readonly preparationSha256: string;
  readonly bindingDigest: string;
  readonly binding: NativeCreationHistoricalBinding;
  readonly resources: NativeCreationResources;
  readonly allowedStages: ReadonlyArray<NativeCreationStage>;
  readonly recoveryScopes: ReadonlyArray<{
    readonly scopeId: string;
    readonly resource: Extract<NativeCreationEffect, { kind: "cleanup" }>["resource"];
  }>;
}

export class NativeCreationGrantResolver extends Context.Service<
  NativeCreationGrantResolver,
  {
    readonly resolveCurrent: (input: {
      readonly actorSessionId: AuthSessionId;
      readonly guard: NativeCreationGuard;
    }) => Effect.Effect<
      {
        readonly enrolledSessionId: AuthSessionId;
        readonly trustedIssuerId: string;
        readonly grant: NativeCreationGrant;
      },
      NativeCreationAuthorityError
    >;
  }
>()("t3/orchestration/NativeCreationAuthority/NativeCreationGrantResolver") {}

// This port must read current native environment, project, enabled provider and qualified account mapping.
export class NativeCreationBindingResolver extends Context.Service<
  NativeCreationBindingResolver,
  {
    readonly resolveCurrent: (
      preparation: ValidatedNativeCreationPreparation,
    ) => Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>;
  }
>()("t3/orchestration/NativeCreationAuthority/NativeCreationBindingResolver") {}

const unavailable = () =>
  new NativeCreationAuthorityError({
    code: "unsupported_authority",
    message: "Native creation authority has not been qualified",
  });
export const NativeCreationGrantResolverUnavailable = Layer.succeed(NativeCreationGrantResolver, {
  resolveCurrent: () => Effect.fail(unavailable()),
});
export const NativeCreationBindingResolverUnavailable = Layer.succeed(
  NativeCreationBindingResolver,
  {
    resolveCurrent: () => Effect.fail(unavailable()),
  },
);

export class NativeCreationAuthority extends Context.Service<
  NativeCreationAuthority,
  {
    readonly authorize: (
      input: NativeCreationAuthorityInput,
    ) => Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>;
    readonly isAutomationEnrolled: (
      actorSessionId: AuthSessionId,
    ) => Effect.Effect<boolean, NativeCreationAuthorityError>;
    readonly issueExecution: (input: {
      readonly reference: NativeCreationExecutionReferenceV2;
      readonly timestamp: string;
    }) => Effect.Effect<
      NativeCreationExecutionContextV2,
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
    readonly authorizeExecution: (
      context: NativeCreationExecutionContextV2,
      input: NativeCreationExecutionStageInput,
    ) => Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>;
  }
>()("t3/orchestration/NativeCreationAuthority") {}

export const makeNativeCreationAuthority = Effect.gen(function* () {
  const sessions = yield* AuthSessionRepository;
  const grants = yield* NativeCreationGrantResolver;
  const bindings = yield* NativeCreationBindingResolver;
  const repository = yield* NativeCreationRepository;
  const isAutomationEnrolled = (actorSessionId: AuthSessionId) =>
    repository.hasAutomationEnrollment(actorSessionId).pipe(Effect.mapError(() => unavailable()));

  const authorize = Effect.fn("NativeCreationAuthority.authorize")(function* (
    input: NativeCreationAuthorityInput,
  ) {
    const now = yield* DateTime.now;
    if (!(yield* isAutomationEnrolled(input.actorSessionId))) {
      return yield* new NativeCreationAuthorityError({
        code: "stale_grant",
        message: "Native session has no permanent automation enrollment",
      });
    }
    const session = yield* sessions
      .getById({ sessionId: input.actorSessionId })
      .pipe(Effect.mapError(() => unavailable()));
    if (
      Option.isNone(session) ||
      session.value.revokedAt !== null ||
      DateTime.toEpochMillis(session.value.expiresAt) <= DateTime.toEpochMillis(now) ||
      !session.value.scopes.includes("orchestration:operate")
    ) {
      return yield* new NativeCreationAuthorityError({
        code: "stale_grant",
        message: "Current native session cannot operate orchestration",
      });
    }
    const resolved = yield* grants.resolveCurrent({
      actorSessionId: input.actorSessionId,
      guard: input.guard,
    });
    const grant = resolved.grant;
    if (
      resolved.enrolledSessionId !== input.actorSessionId ||
      grant.actorSessionId !== input.actorSessionId ||
      !resolved.trustedIssuerId ||
      grant.issuerId !== resolved.trustedIssuerId ||
      grant.revoked ||
      grant.grantId !== input.guard.grantId ||
      grant.revision !== input.guard.grantRevision ||
      DateTime.toEpochMillis(grant.expiresAt) <= DateTime.toEpochMillis(now)
    ) {
      return yield* new NativeCreationAuthorityError({
        code: "stale_grant",
        message: "Native creation enrollment or grant is stale",
      });
    }
    const preparation = input.preparation;
    const currentBinding = yield* bindings.resolveCurrent(preparation);
    const binding = yield* decodeHistoricalBinding(currentBinding).pipe(
      Effect.mapError(
        () =>
          new NativeCreationAuthorityError({
            code: "binding_mismatch",
            message: "Current qualified native binding is invalid",
          }),
      ),
    );
    const expected = {
      backendInstance: preparation.binding.backend_instance,
      environmentId: preparation.binding.environment_id,
      projectId: preparation.binding.project_id,
      projectCwd: preparation.binding.project_cwd,
      accountRef: preparation.binding.account_ref,
      accountBindingId: binding.accountBindingId,
      accountBindingRevision: binding.accountBindingRevision,
      providerModelSelection: preparation.binding.provider_model_selection,
      runtimeMode: preparation.binding.runtime_mode,
      interactionMode: preparation.binding.interaction_mode,
      baseBranch: preparation.binding.base_branch,
      startFromOrigin: preparation.binding.start_from_origin,
      runSetupScript: preparation.binding.run_setup_script,
      requestedBranch: preparation.command.bootstrap.prepareWorktree.branch,
    };
    if (
      grant.operationId !== preparation.operationId ||
      grant.preparationId !== preparation.preparationId ||
      grant.preparationSha256 !== preparation.preparationSha256 ||
      grant.bindingDigest !== preparation.bindingDigest ||
      nativeCreationCanonicalJson(binding) !== nativeCreationCanonicalJson(expected) ||
      nativeCreationCanonicalJson(grant.binding) !== nativeCreationCanonicalJson(binding) ||
      nativeCreationCanonicalJson(grant.resources) !==
        nativeCreationCanonicalJson(input.resources) ||
      input.resources.projectCwd !== binding.projectCwd ||
      input.resources.branch !== binding.requestedBranch ||
      !input.resources.worktreePath.startsWith("/") ||
      !grant.allowedStages.includes(input.stage) ||
      (input.stage === "cleanup" &&
        !grant.recoveryScopes.some(
          (scope) =>
            scope.scopeId === input.recoveryScopeId &&
            input.recoveryResource !== undefined &&
            nativeCreationCanonicalJson(scope.resource) ===
              nativeCreationCanonicalJson(input.recoveryResource),
        ))
    ) {
      return yield* new NativeCreationAuthorityError({
        code: "binding_mismatch",
        message: "Native creation intent, binding or resource scope disagrees",
      });
    }
    return binding;
  });

  const issueExecution: NativeCreationAuthority["Service"]["issueExecution"] = Effect.fn(
    "NativeCreationAuthority.issueExecution",
  )(function* (input: Parameters<NativeCreationAuthority["Service"]["issueExecution"]>[0]) {
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)(
      input.reference,
    ).pipe(
      Effect.mapError(
        () =>
          new NativeCreationAuthorityError({
            code: "unresolved_claim",
            message: "Native execution reference is invalid",
          }),
      ),
    );
    if (reference.stage !== "native_command") {
      return yield* new NativeCreationAuthorityError({
        code: "unresolved_claim",
        message: "Native execution issuance requires a native command stage",
      });
    }
    const resolved = yield* repository.readExecutionReference(reference);
    const intent = resolved.history.intent;
    const preparation = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(intent.canonicalPreparation),
    ).pipe(
      Effect.mapError(
        () =>
          new NativeCreationAuthorityError({
            code: "unresolved_claim",
            message: "Native execution preparation cannot be recovered from its immutable claim",
          }),
      ),
    );
    if (
      preparation.canonicalText !== resolved.preparation.canonicalText ||
      preparation.preparationSha256 !== resolved.preparation.preparationSha256
    ) {
      return yield* new NativeCreationAuthorityError({
        code: "unresolved_claim",
        message: "Native execution preparation differs from its immutable claim",
      });
    }
    const actorSessionId = yield* Schema.decodeUnknownEffect(Schema.toType(AuthSessionId))(
      intent.actorSessionId,
    ).pipe(
      Effect.mapError(
        () =>
          new NativeCreationAuthorityError({
            code: "unresolved_claim",
            message: "Native execution claim has an invalid actor session",
          }),
      ),
    );
    const authorityInput: NativeCreationAuthorityInput = Object.freeze({
      actorSessionId,
      preparation: freezePreparation(preparation),
      guard: Object.freeze({
        schema: "t3.native-creation-guard/v1" as const,
        grantId: intent.grantId,
        grantRevision: intent.grantRevision,
      }),
      resources: Object.freeze({ ...intent.resources }),
      stage: reference.stage,
    });
    const started = yield* repository.startEffectV2(
      reference,
      input.timestamp,
      authorize(authorityInput),
    );
    if (
      started.status !== "started" ||
      started.fact.version !== 2 ||
      started.fact.kind !== "native_command" ||
      started.fact.phase !== "started" ||
      started.fact.effectId !== reference.effectId ||
      started.fact.commandId !== reference.stageCommandId ||
      started.fact.threadId !== intent.threadId ||
      started.fact.commandType !== resolved.command.type ||
      started.fact.commandDigest !== resolved.nativeIdentity.normalizedCommandDigest
    ) {
      return yield* new NativeCreationAuthorityError({
        code: "unresolved_claim",
        message: "Native execution start disagrees with its immutable reference",
      });
    }
    // Only this invocation's new committed start can mint an in-memory execution context.
    const context = Object.freeze({}) as NativeCreationExecutionContextV2;
    issuedExecutions.set(context, {
      reference: Object.freeze({ ...reference }),
      authorize: Effect.fn("NativeCreationAuthority.authorizeExecution")(function* (
        actual: NativeCreationExecutionStageInput,
      ) {
        if (
          nativeCreationCanonicalJson(actual.resources) !==
          nativeCreationCanonicalJson(authorityInput.resources)
        ) {
          return yield* new NativeCreationAuthorityError({
            code: "binding_mismatch",
            message: "Native execution resources differ from the immutable claim",
          });
        }
        return yield* authorize({
          ...authorityInput,
          stage: actual.stage,
          resources: actual.resources,
          ...(actual.recoveryScopeId === undefined
            ? {}
            : { recoveryScopeId: actual.recoveryScopeId }),
          ...(actual.recoveryResource === undefined
            ? {}
            : { recoveryResource: actual.recoveryResource }),
        });
      }),
    });
    return context;
  });

  return NativeCreationAuthority.of({
    authorize,
    isAutomationEnrolled,
    issueExecution,
    authorizeExecution: authorizeNativeCreationExecution,
  });
});

export const NativeCreationAuthorityLive = Layer.effect(
  NativeCreationAuthority,
  makeNativeCreationAuthority,
);
export const NativeCreationAuthorityUnavailable = NativeCreationAuthorityLive.pipe(
  Layer.provide(NativeCreationGrantResolverUnavailable),
  Layer.provide(NativeCreationBindingResolverUnavailable),
);
