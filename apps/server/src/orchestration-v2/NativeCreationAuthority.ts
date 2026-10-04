import {
  AuthSessionId,
  CommandId,
  type ThreadId,
  type NativeThreadIncarnationV2,
  OrchestrationV2Command,
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
  nativeCreationV2CommandDigest,
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
    readonly authorize: (
      input: NativeCreationExecutionStageInput,
    ) => Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>;
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

declare const issuedThreadRecoveryContext: unique symbol;

export interface NativeCreationThreadRecoveryContextV2 {
  readonly [issuedThreadRecoveryContext]: true;
}

export interface NativeCreationThreadRecoveryReferenceV2 {
  readonly version: 2;
  readonly claimId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly commandDigest: string;
  readonly commandStartEffectId: string;
  readonly cleanupStartEffectId: string;
  readonly recoveryScopeId: string;
  readonly incarnation: NativeThreadIncarnationV2;
}

export interface NativeCreationThreadRecoveryInputV2 {
  readonly claimId: string;
  readonly commandStartEffectId: string;
  readonly cleanupStartEffectId: string;
  readonly authorization: NativeCreationAuthorityInput;
}

const issuedThreadRecoveries = new WeakMap<
  NativeCreationThreadRecoveryContextV2,
  {
    readonly reference: NativeCreationThreadRecoveryReferenceV2;
    readonly authorize: Effect.Effect<
      NativeCreationHistoricalBinding,
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
  }
>();

// Recovery correlation grants no external action; acceptance must prove the current birth atomically.
export const getNativeCreationThreadRecoveryReference = (
  context: NativeCreationThreadRecoveryContextV2,
): NativeCreationThreadRecoveryReferenceV2 | null =>
  issuedThreadRecoveries.get(context)?.reference ?? null;

export const authorizeNativeCreationThreadRecovery = Effect.fn(
  "authorizeNativeCreationThreadRecovery",
)(function* (context: NativeCreationThreadRecoveryContextV2) {
  const recovery = issuedThreadRecoveries.get(context);
  if (recovery === undefined) {
    return yield* new NativeCreationAuthorityError({
      code: "unsupported_authority",
      message: "Native thread recovery context was not issued by an authority",
    });
  }
  return yield* recovery.authorize;
});

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
>()("t3/orchestration-v2/NativeCreationAuthority/NativeCreationGrantResolver") {}

// This port must read current native environment, project, enabled provider and qualified account mapping.
export class NativeCreationBindingResolver extends Context.Service<
  NativeCreationBindingResolver,
  {
    readonly resolveCurrent: (
      preparation: ValidatedNativeCreationPreparation,
    ) => Effect.Effect<NativeCreationHistoricalBinding, NativeCreationAuthorityError>;
  }
>()("t3/orchestration-v2/NativeCreationAuthority/NativeCreationBindingResolver") {}

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
    readonly issueThreadRecovery: (
      input: NativeCreationThreadRecoveryInputV2,
    ) => Effect.Effect<
      NativeCreationThreadRecoveryContextV2,
      NativeCreationRepositoryError | NativeCreationAuthorityError
    >;
  }
>()("t3/orchestration-v2/NativeCreationAuthority") {}

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

  const issueThreadRecovery: NativeCreationAuthority["Service"]["issueThreadRecovery"] = Effect.fn(
    "NativeCreationAuthority.issueThreadRecovery",
  )(function* (input: NativeCreationThreadRecoveryInputV2) {
    const invalid = (message: string) =>
      new NativeCreationAuthorityError({ code: "unresolved_claim", message });
    const claimId = input.claimId;
    const commandStartEffectId = input.commandStartEffectId;
    const cleanupStartEffectId = input.cleanupStartEffectId;
    const requested = input.authorization;
    if (
      claimId.length === 0 ||
      commandStartEffectId.length === 0 ||
      cleanupStartEffectId.length === 0 ||
      commandStartEffectId === cleanupStartEffectId ||
      requested.stage !== "cleanup" ||
      requested.recoveryResource?.kind !== "thread" ||
      requested.recoveryScopeId === undefined
    ) {
      return yield* invalid("Native thread recovery requires its exact cleanup and command starts");
    }
    const actorSessionId = requested.actorSessionId;
    const guard = Object.freeze({
      schema: requested.guard.schema,
      grantId: requested.guard.grantId,
      grantRevision: requested.guard.grantRevision,
    });
    const resources = Object.freeze({
      projectCwd: requested.resources.projectCwd,
      branch: requested.resources.branch,
      worktreePath: requested.resources.worktreePath,
    });
    const expectedPreparation = nativeCreationCanonicalJson(requested.preparation);
    const expectedResource = nativeCreationCanonicalJson(requested.recoveryResource);
    const recoveryScopeId = requested.recoveryScopeId;
    const initialHistory = yield* repository.readHistoryByClaim(claimId);
    const intent = initialHistory.intent;
    const immutableIntent = nativeCreationCanonicalJson(intent);
    const preparation = freezePreparation(
      yield* validateNativeCreationPreparation(
        new TextEncoder().encode(intent.canonicalPreparation),
      ).pipe(
        Effect.mapError(() =>
          invalid("Native recovery preparation cannot be recovered from its immutable claim"),
        ),
      ),
    );
    if (
      intent.claimId !== claimId ||
      intent.actorSessionId !== actorSessionId ||
      intent.grantId !== guard.grantId ||
      intent.grantRevision !== guard.grantRevision ||
      guard.schema !== "t3.native-creation-guard/v1" ||
      expectedPreparation !== nativeCreationCanonicalJson(preparation) ||
      nativeCreationCanonicalJson(resources) !== nativeCreationCanonicalJson(intent.resources) ||
      preparation.preparationId !== intent.preparationId ||
      preparation.operationId !== intent.operationId ||
      preparation.preparationSha256 !== intent.preparationSha256 ||
      preparation.bindingDigest !== intent.bindingDigest ||
      preparation.promptDigest !== intent.promptDigest ||
      preparation.commandDigest !== intent.commandDigest ||
      preparation.command.commandId !== intent.commandId ||
      preparation.command.threadId !== intent.threadId ||
      preparation.command.message.messageId !== intent.messageId
    ) {
      return yield* invalid("Native thread recovery differs from its immutable claim");
    }
    const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)(
      {
        type: "thread.delete",
        commandId: `${intent.commandId}:bootstrap-thread-delete`,
        threadId: intent.threadId,
      },
      { onExcessProperty: "error" },
    ).pipe(Effect.mapError(() => invalid("Native recovery command is invalid")));
    if (
      command.type !== "thread.delete" ||
      command.commandId !== `${intent.commandId}:bootstrap-thread-delete` ||
      command.threadId !== intent.threadId
    )
      return yield* invalid("Native recovery command IDs cannot be normalized");
    const canonicalCommand = nativeCreationCanonicalJson(command);
    const commandDigest = nativeCreationV2CommandDigest(command);
    const verify = Effect.gen(function* () {
      const history = yield* repository.readHistoryByClaim(claimId);
      if (nativeCreationCanonicalJson(history.intent) !== immutableIntent)
        return yield* invalid("Native recovery claim changed");
      const reserved = yield* repository.readThreadRecoveryCommand(command.commandId);
      if (
        reserved === null ||
        reserved.version !== 2 ||
        reserved.claimId !== claimId ||
        reserved.commandId !== command.commandId ||
        reserved.threadId !== command.threadId ||
        reserved.commandType !== command.type ||
        reserved.commandDigest !== commandDigest ||
        reserved.commandStartEffectId !== commandStartEffectId ||
        reserved.cleanupStartEffectId !== cleanupStartEffectId ||
        reserved.recoveryScopeId !== recoveryScopeId ||
        nativeCreationCanonicalJson(reserved.resource) !== expectedResource ||
        nativeCreationCanonicalJson(reserved.canonicalCommand) !== canonicalCommand
      ) {
        return yield* invalid("Native recovery command is not exactly reserved");
      }
      const starts = history.effects.filter(
        (fact) => fact.effectId === commandStartEffectId && fact.phase === "started",
      );
      const start = starts[0];
      if (
        starts.length !== 1 ||
        start?.kind !== "native_command" ||
        start.phase !== "started" ||
        start.commandId !== command.commandId ||
        start.threadId !== command.threadId ||
        start.commandType !== command.type ||
        start.commandDigest !== commandDigest ||
        history.effects.filter(
          (fact) =>
            fact.kind === "native_command" &&
            fact.phase === "started" &&
            fact.commandId === command.commandId,
        ).length !== 1 ||
        history.effects.some(
          (fact) => fact.effectId === commandStartEffectId && fact.phase === "completed",
        )
      ) {
        return yield* invalid("Native recovery command has no unique unresolved matching start");
      }
      const cleanup = history.effects
        .filter(
          (fact) =>
            fact.kind === "cleanup" &&
            fact.phase === "started" &&
            fact.resource.kind === "thread" &&
            fact.resource.threadId === command.threadId &&
            fact.ordinal < start.ordinal &&
            !history.effects.some(
              (completion) =>
                completion.effectId === fact.effectId && completion.phase === "completed",
            ),
        )
        .at(-1);
      if (
        cleanup?.kind !== "cleanup" ||
        cleanup.phase !== "started" ||
        cleanup.resource.kind !== "thread" ||
        cleanup.effectId !== cleanupStartEffectId ||
        cleanup.recoveryScopeId !== recoveryScopeId ||
        cleanup.ordinal !== reserved.cleanupStartOrdinal ||
        nativeCreationCanonicalJson(cleanup.resource) !== expectedResource ||
        history.effects.filter(
          (fact) => fact.effectId === cleanupStartEffectId && fact.phase === "started",
        ).length !== 1
      ) {
        return yield* invalid("Native recovery cleanup scope or incarnation changed");
      }
      return {
        resource: cleanup.resource,
        canonicalCompanion: nativeCreationCanonicalJson(reserved),
      };
    });
    const verified = yield* verify;
    const verifiedResource = verified.resource;
    const resource = freezePreparation({
      ...verifiedResource,
      incarnation: { ...verifiedResource.incarnation },
    });
    const originalInput: NativeCreationAuthorityInput = Object.freeze({
      actorSessionId,
      preparation,
      guard,
      resources,
      stage: "native_command",
    });
    const immutableBinding = nativeCreationCanonicalJson(intent.binding);
    const revalidate = verify.pipe(
      Effect.flatMap((current) =>
        Effect.gen(function* () {
          if (current.canonicalCompanion !== verified.canonicalCompanion)
            return yield* invalid("Native recovery reservation changed");
          const nativeBinding = yield* authorize(originalInput);
          const cleanupBinding = yield* authorize({
            ...originalInput,
            stage: "cleanup",
            recoveryScopeId,
            recoveryResource: resource,
          });
          if (
            nativeCreationCanonicalJson(nativeBinding) !== immutableBinding ||
            nativeCreationCanonicalJson(cleanupBinding) !== immutableBinding
          )
            return yield* invalid("Current recovery authority differs from its claimed binding");
          return cleanupBinding;
        }),
      ),
    );
    yield* revalidate;
    // This context guards one logical SQL command; it never substitutes an external-effect execution context.
    const context = Object.freeze({}) as NativeCreationThreadRecoveryContextV2;
    issuedThreadRecoveries.set(context, {
      reference: freezePreparation({
        version: 2 as const,
        claimId,
        commandId: command.commandId,
        threadId: command.threadId,
        commandDigest,
        commandStartEffectId,
        cleanupStartEffectId,
        recoveryScopeId,
        incarnation: { ...resource.incarnation },
      }),
      authorize: revalidate,
    });
    return context;
  });

  return NativeCreationAuthority.of({
    authorize,
    isAutomationEnrolled,
    issueExecution,
    authorizeExecution: authorizeNativeCreationExecution,
    issueThreadRecovery,
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
