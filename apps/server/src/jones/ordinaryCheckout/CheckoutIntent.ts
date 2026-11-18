import {
  ThreadId,
  OrchestrationV2RunJson,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { PendingOrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";
import type { CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import { readApplicationBirthRecord } from "../importedHistory/ApplicationBirth.ts";
import {
  CheckoutError,
  digest,
  Origin,
  type Admission,
  type Birth,
  type Relation,
  type Target,
} from "./CheckoutProtocol.ts";
import * as Conflicts from "./CheckoutConflictIntegration.ts";
import * as CheckoutTarget from "./CheckoutTarget.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckout.ts";
import * as Preparation from "./CheckoutPreparation.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as Shared from "../cleanup/sharedAdmission/Protocol.ts";
import { decodedCommand, planDigests } from "./CheckoutPreparationSchema.ts";
import {
  canonical,
  preparationOriginId,
  preparedAdmissionId,
  type PreparationOrigin,
  type PreparationBinding,
  type PreparationRelease,
  type TargetSnapshot,
} from "./CheckoutProtocol.ts";

const decodeRetainedCertificate = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      commandDigest: Schema.String,
      receiptThreadId: Schema.String,
      receiptSequence: Schema.Number,
    }),
  ),
);
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson));
// Digest persisted encoded strings without applying decode-time trimming.
const decodeStrategyDigest = Schema.decodeEffect(
  Schema.fromJsonString(Schema.toEncoded(OrchestrationV2ThreadLaunchWorkspaceStrategy)),
  { onExcessProperty: "error" },
);
const encodeWorkspaceStrategy = Schema.encodeEffect(OrchestrationV2ThreadLaunchWorkspaceStrategy);

export interface IntentCommand {
  readonly commandId: string;
  readonly type: string;
  readonly scheduledTaskId?: string | null | undefined;
}
export interface IntentPlan {
  readonly command: IntentCommand;
  readonly preparation?: Preparation.PreparationHandle;
  readonly localAcceptance?: Preparation.LocalAcceptance;
  readonly threadId: string;
  readonly events: readonly OrchestrationV2DomainEvent[];
  readonly effects: readonly PendingOrchestrationEffectV2[];
}
interface Candidate {
  readonly subjectThreadId: string;
  readonly target: Target;
  readonly before: CheckoutTarget.TargetInput | null;
  readonly birth: Birth | null;
  readonly birthEventId: string;
  readonly relation: Relation;
  readonly origin: OrdinaryCheckout.CommandIdentity["origin"];
  readonly expectedOwner: OrdinaryCheckout.Accept["expectedOwner"];
}
interface OriginCandidate {
  readonly subjectThreadId: string;
  readonly before: TargetSnapshot | null;
  readonly initial: TargetSnapshot;
  readonly birth: Birth | null;
  readonly birthEventId: string;
  readonly run: {
    readonly id: string;
    readonly activeAttemptId: string;
    readonly rootNodeId: string | null;
    readonly userMessageId: string;
    readonly providerThreadId: string;
  };
  readonly basisEventId: string;
  readonly commandDigest: string;
  readonly strategyJson: string;
  readonly classification: OrdinaryCheckout.CommandIdentity["origin"];
}
export interface PreparedIntent {
  readonly plan: IntentPlan;
  readonly origins: readonly OriginCandidate[];
  readonly preparation: Preparation.PreparedSwitch | Preparation.PreparedRelease | null;
  readonly candidates: readonly Candidate[];
  readonly preflight: readonly Conflicts.OrdinaryPreflight[];
  readonly commit: Conflicts.CommitOrdinary | null;
}
export interface CommittedEffects {
  readonly storedEvents: readonly OrchestrationV2StoredEvent[];
  readonly effects: readonly PendingOrchestrationEffectV2[];
}
export class CheckoutIntent extends Context.Service<
  CheckoutIntent,
  {
    readonly replay: (
      command: IntentCommand,
      threadId: string,
    ) => Effect.Effect<boolean, CheckoutError>;
    readonly prepare: (plan: IntentPlan) => Effect.Effect<PreparedIntent, CheckoutError>;
    readonly validate: (intent: PreparedIntent) => Effect.Effect<void, CheckoutError>;
    readonly accept: (
      intent: PreparedIntent,
      committed: CommittedEffects & { readonly receipt: CommandReceiptV2 },
    ) => Effect.Effect<void, CheckoutError, OrdinaryCheckout.CheckoutCommitAuthority>;
    readonly validateSwitch: (intent: PreparedIntent) => Effect.Effect<void, CheckoutError>;
    readonly validateRelease: (intent: PreparedIntent) => Effect.Effect<void, CheckoutError>;
    readonly acceptSwitch: (
      intent: PreparedIntent,
      committed: CommittedEffects & { readonly receipt: CommandReceiptV2 },
    ) => Effect.Effect<void, CheckoutError, OrdinaryCheckout.CheckoutCommitAuthority>;
    readonly acceptRelease: (
      intent: PreparedIntent,
      committed: CommittedEffects & { readonly receipt: CommandReceiptV2 },
    ) => Effect.Effect<void, CheckoutError, OrdinaryCheckout.CheckoutCommitAuthority>;
    readonly linkEffects: (
      committed: CommittedEffects,
    ) => Effect.Effect<void, CheckoutError, OrdinaryCheckout.CheckoutCommitAuthority>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutIntent") {}

const role = (effect: PendingOrchestrationEffectV2): OrdinaryCheckout.Link["role"] | null => {
  if (effect.nativeCreationExecutionReference !== undefined) return null;
  switch (effect.request.type) {
    case "provider-turn.start":
      return "launch";
    case "provider-turn.interrupt":
    case "provider-turn.steer":
    case "provider-turn.steer-batch":
    case "provider-turn.restart":
    case "runtime-request.respond":
      return "control";
    case "provider-thread.rollback":
      return "rollback";
    case "provider-runtime.continue":
      return "continuation";
    case "checkpoint.capture":
      return "checkpoint";
    default:
      return null;
  }
};
const threadTarget = Schema.fromJsonString(
  Schema.Struct({
    projectId: Schema.String,
    worktreePath: Schema.NullOr(Schema.String),
    branch: Schema.NullOr(Schema.String),
    updatedAt: Schema.String,
    deletedAt: Schema.NullOr(Schema.String),
  }),
);
const decodeThreadTarget = Schema.decodeUnknownEffect(threadTarget);
const isCheckoutError = Schema.is(CheckoutError);
const decodeOrigin = Schema.decodeUnknownEffect(Origin);
const mapError = (cause: unknown) =>
  isCheckoutError(cause) ? cause : new CheckoutError({ reason: "storage", cause });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const checkout = yield* OrdinaryCheckout.OrdinaryCheckout;
  const target = yield* CheckoutTarget.CheckoutTarget;
  const conflicts = yield* Conflicts.CheckoutConflictIntegration;
  const preparation = yield* Preparation.CheckoutPreparation;
  const hasTable = makeJonesTablePresence(sql);
  const captures = new WeakMap<
    PreparedIntent,
    {
      readonly fiber: number;
      readonly identity: string;
      readonly commit: Conflicts.CommitOrdinary | null;
      readonly preparation: PreparedIntent["preparation"];
      readonly local: Preparation.LocalAcceptance | undefined;
      validatedTransaction?: unknown;
      spent: boolean;
    }
  >();
  const fingerprint = (intent: PreparedIntent) =>
    digest({
      command: intent.plan.command,
      threadId: intent.plan.threadId,
      plan: planDigests(intent.plan).authority,
      candidates: intent.candidates,
      origins: intent.origins,
      preflight: intent.preflight,
      local: intent.plan.localAcceptance?.expected ?? null,
    });
  const retain = Effect.fn("CheckoutIntent.retain")(function* (intent: PreparedIntent) {
    captures.set(intent, {
      fiber: yield* Effect.fiberId,
      identity: fingerprint(intent),
      commit: intent.commit,
      preparation: intent.preparation,
      local: intent.plan.localAcceptance,
      spent: false,
    });
    return intent;
  });
  const captured = Effect.fn("CheckoutIntent.captured")(function* (intent: PreparedIntent) {
    const state = captures.get(intent);
    if (
      !state ||
      state.spent ||
      state.fiber !== (yield* Effect.fiberId) ||
      state.identity !== fingerprint(intent) ||
      state.commit !== intent.commit ||
      state.preparation !== intent.preparation ||
      state.local !== intent.plan.localAcceptance
    )
      return yield* new CheckoutError({ reason: "identity" });
    return state;
  });
  const transaction = Effect.gen(function* () {
    if (
      Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
      Option.isNone(yield* Effect.serviceOption(sql.transactionService))
    )
      return yield* new CheckoutError({ reason: "unavailable" });
  });
  const readTarget = Effect.fn("CheckoutIntent.readTarget")(function* (threadId: string) {
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${threadId}`;
    if (!rows[0]) return yield* new CheckoutError({ reason: "unavailable" });
    const thread = yield* decodeThreadTarget(rows[0].payload_json);
    const projects = yield* sql<{
      workspace_root: string;
      updated_at: string;
      deleted_at: string | null;
    }>`SELECT workspace_root,updated_at,deleted_at FROM projection_projects WHERE project_id=${thread.projectId}`;
    const project = projects[0];
    if (!project || project.deleted_at !== null || thread.deletedAt !== null)
      return yield* new CheckoutError({ reason: "unavailable" });
    return {
      threadId,
      projectId: thread.projectId,
      projectRoot: project.workspace_root,
      projectRevision: project.updated_at,
      threadRevision: thread.updatedAt,
      worktreePath: thread.worktreePath,
      threadBranch: thread.branch,
    };
  });
  const effectRunId = Effect.fn("CheckoutIntent.effectRunId")(function* (
    effect: PendingOrchestrationEffectV2,
  ) {
    const request = effect.request;
    if ("runId" in request) return request.runId;
    if (request.type === "provider-runtime.continue") return request.sourceRunId;
    if (request.type === "runtime-request.respond") {
      const rows = yield* sql<{
        run_id: string | null;
      }>`SELECT run_id FROM orchestration_v2_projection_runtime_requests
        WHERE request_id=${request.requestId} AND thread_id=${effect.threadId}`;
      if (rows.length !== 1 || rows[0]!.run_id === null)
        return yield* new CheckoutError({ reason: "identity" });
      return rows[0]!.run_id;
    }
    if ("providerTurnId" in request && "providerThreadId" in request) {
      const rows = yield* sql<{ run_id: string; providerSessionId: string | null }>`SELECT a.run_id,
        json_extract(p.payload_json,'$.providerSessionId') AS providerSessionId
        FROM orchestration_v2_projection_provider_turns t JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id=t.run_attempt_id
        JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id=t.provider_thread_id
        WHERE t.provider_turn_id=${request.providerTurnId} AND t.provider_thread_id=${request.providerThreadId}
          AND t.thread_id=${effect.threadId} AND a.thread_id=${effect.threadId}`;
      if (
        rows.length !== 1 ||
        ("providerSessionId" in request && rows[0]!.providerSessionId !== request.providerSessionId)
      )
        return yield* new CheckoutError({ reason: "identity" });
      return rows[0]!.run_id;
    }
    return null;
  });
  const replay = Effect.fn("CheckoutIntent.replay")(function* (
    command: IntentCommand,
    threadId: string,
  ) {
    const decoded = yield* decodedCommand(command);
    const origins = yield* checkout.readPreparationOrigins(command.commandId);
    if (origins.length > 0) {
      for (const origin of origins) {
        if (
          origin.commandDigest !== decoded.digest ||
          origin.commandType !== command.type ||
          origin.receiptThreadId !== threadId
        )
          return yield* new CheckoutError({ reason: "replay" });
        const receipts =
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${command.commandId}
          AND aggregate_id=${threadId} AND aggregate_kind='thread' AND status='accepted' AND result_sequence=${origin.receiptSequence}`;
        if (receipts.length !== 1) return yield* new CheckoutError({ reason: "unknown" });
      }
      return true;
    }
    // Transition/release replay checks exact command content before any observation.
    if (yield* hasTable("jones_ordinary_checkout_preparing_bindings")) {
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM jones_ordinary_checkout_preparing_bindings WHERE command_id=${command.commandId}
        UNION ALL SELECT record_json FROM jones_ordinary_checkout_preparing_releases WHERE command_id=${command.commandId}`;
      if (rows.length > 0) {
        for (const row of rows) {
          const retained = yield* decodeRetainedCertificate(row.record_json);
          if (retained.commandDigest !== decoded.digest || retained.receiptThreadId !== threadId)
            return yield* new CheckoutError({ reason: "replay" });
          const receipts =
            yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${command.commandId}
            AND aggregate_id=${threadId} AND status='accepted' AND result_sequence=${retained.receiptSequence}`;
          if (receipts.length !== 1) return yield* new CheckoutError({ reason: "unknown" });
        }
        return true;
      }
    }
    const rows =
      yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions WHERE command_id=${command.commandId}`;
    const identities =
      rows.length === 0
        ? [
            {
              subjectThreadId: threadId,
              origin: { kind: "command" as const, parentThreadId: null, requestId: null },
            },
          ]
        : rows.map((row) => ({
            subjectThreadId: row.subject_thread_id,
            origin: {
              kind: row.origin_kind,
              parentThreadId: row.origin_parent_thread_id,
              requestId: row.request_id,
            },
          }));
    let found = false;
    for (const identity of identities) {
      const result = yield* checkout.replay({
        ...identity,
        origin: yield* decodeOrigin(identity.origin),
        command,
        commandId: command.commandId,
        commandType: command.type,
        receiptThreadId: threadId,
      });
      found = result.kind !== "absent";
    }
    return found;
  }, Effect.mapError(mapError));
  const prepare = Effect.fn("CheckoutIntent.prepare")(
    function* (plan: IntentPlan) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new CheckoutError({ reason: "unavailable" });
      if (yield* replay(plan.command, plan.threadId))
        return yield* retain({
          plan,
          candidates: [],
          preflight: [],
          commit: null,
          origins: [],
          preparation: null,
        });
      yield* decodedCommand(plan.command);
      if (plan.preparation !== undefined) {
        const prepared =
          plan.command.type === "prepared-run.release"
            ? yield* preparation.prepareRelease(plan.preparation, {
                command: plan.command,
                finalPlan: plan,
              })
            : yield* preparation.prepareSwitch(plan.preparation, {
                command: plan.command,
                finalPlan: plan,
              });
        if (plan.localAcceptance === undefined)
          return yield* new CheckoutError({ reason: "unavailable" });
        return yield* retain({
          plan,
          candidates: [],
          preflight: [],
          commit: null,
          origins: [],
          preparation: prepared,
        });
      }
      if (plan.command.type === "prepared-run.release") {
        const decoded = yield* decodedCommand(plan.command);
        if (decoded.command.type !== "prepared-run.release")
          return yield* new CheckoutError({ reason: "identity" });
        const rows = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_runs
          WHERE run_id=${decoded.command.runId} AND thread_id=${decoded.command.threadId}`;
        const run = rows[0] ? yield* decodeRun(rows[0].payload_json) : null;
        if (run?.workspacePreparation !== undefined)
          return yield* new CheckoutError({ reason: "unavailable" });
      }
      for (const event of plan.events) {
        if (event.type !== "thread.metadata-updated") continue;
        if (
          plan.events.some(
            (candidate) =>
              candidate.type === "thread.created" && candidate.threadId === event.threadId,
          )
        )
          continue;
        const before = yield* readTarget(event.threadId);
        if (
          (yield* hasTable("jones_ordinary_checkout_preparing_origins")) &&
          (before.projectId !== event.payload.projectId ||
            before.worktreePath !== event.payload.worktreePath ||
            before.threadBranch !== event.payload.branch)
        ) {
          const rows =
            yield* sql`SELECT o.origin_id FROM jones_ordinary_checkout_preparing_origins o
            WHERE o.subject_thread_id=${event.threadId} AND NOT EXISTS
            (SELECT 1 FROM jones_ordinary_checkout_preparing_releases r WHERE r.origin_id=o.origin_id)`;
          if (rows.length > 0) return yield* new CheckoutError({ reason: "unavailable" });
        }
      }
      const subjects = new Set(plan.effects.filter((e) => role(e) !== null).map((e) => e.threadId));
      for (const event of plan.events)
        if (event.type === "run.created") subjects.add(event.threadId);
      const candidates: Candidate[] = [];
      const origins: OriginCandidate[] = [];
      for (const subject of subjects) {
        if (
          plan.effects.some(
            (e) => e.threadId === subject && e.nativeCreationExecutionReference !== undefined,
          )
        ) {
          if (plan.effects.some((e) => e.threadId === subject && role(e) !== null))
            return yield* new CheckoutError({ reason: "identity" });
          continue;
        }
        const creation = plan.events.find(
          (event) => event.threadId === subject && event.type === "thread.created",
        );
        const before = creation?.type === "thread.created" ? null : yield* readTarget(subject);
        const targetInput =
          before ??
          (yield* Effect.gen(function* () {
            if (creation?.type !== "thread.created")
              return yield* new CheckoutError({ reason: "identity" });
            const projects = yield* sql<{
              workspace_root: string;
              updated_at: string;
              deleted_at: string | null;
            }>`SELECT workspace_root,updated_at,deleted_at FROM projection_projects WHERE project_id=${creation.payload.projectId}`;
            const project = projects[0];
            if (!project || project.deleted_at !== null)
              return yield* new CheckoutError({ reason: "unavailable" });
            return {
              threadId: subject,
              projectId: creation.payload.projectId,
              projectRoot: project.workspace_root,
              projectRevision: project.updated_at,
              threadRevision: DateTime.formatIso(creation.payload.updatedAt),
              worktreePath: creation.payload.worktreePath,
              threadBranch: creation.payload.branch,
            };
          }));
        const createdRun = plan.events.find(
          (event) => event.threadId === subject && event.type === "run.created",
        );
        if (
          createdRun?.type === "run.created" &&
          createdRun.payload.workspacePreparation !== undefined
        ) {
          const decoded = yield* decodedCommand(plan.command);
          const command = decoded.command;
          const strategy =
            command.type === "thread.create"
              ? command.initialMessage?.workspaceStrategy
              : command.type === "message.dispatch" && command.dispatchMode.type === "defer_start"
                ? command.dispatchMode.workspaceStrategy
                : undefined;
          const run = createdRun.payload;
          if (
            strategy === undefined ||
            canonical(strategy) !== canonical(run.workspacePreparation) ||
            run.status !== "preparing" ||
            run.activeAttemptId === null ||
            run.providerThreadId === null ||
            plan.localAcceptance === undefined ||
            plan.effects.some(
              (effect) =>
                effect.threadId === subject &&
                (role(effect) !== null || effect.nativeCreationExecutionReference !== undefined),
            )
          )
            return yield* new CheckoutError({ reason: "identity" });
          const birth =
            before === null ? null : yield* readApplicationBirthRecord(ThreadId.make(subject));
          if (before !== null && birth === null)
            return yield* new CheckoutError({ reason: "identity" });
          const metadata = plan.events.filter(
            (event) => event.threadId === subject && event.type === "thread.metadata-updated",
          );
          for (const event of metadata) {
            if (
              event.type === "thread.metadata-updated" &&
              (event.payload.projectId !== targetInput.projectId ||
                event.payload.worktreePath !== targetInput.worktreePath ||
                event.payload.branch !== targetInput.threadBranch)
            )
              return yield* new CheckoutError({ reason: "identity" });
          }
          const lastMetadata = metadata.at(-1);
          const initial = {
            ...targetInput,
            threadRevision:
              lastMetadata?.type === "thread.metadata-updated"
                ? DateTime.formatIso(lastMetadata.payload.updatedAt)
                : targetInput.threadRevision,
          };
          origins.push({
            subjectThreadId: subject,
            before,
            initial,
            birth: birth === null ? null : { eventId: birth.eventId, sequence: birth.sequence },
            birthEventId: birth?.eventId ?? creation!.id,
            run: {
              id: run.id,
              activeAttemptId: run.activeAttemptId,
              rootNodeId: run.rootNodeId,
              userMessageId: run.userMessageId,
              providerThreadId: run.providerThreadId,
            },
            basisEventId: createdRun.id,
            commandDigest: decoded.digest,
            strategyJson: canonical(yield* encodeWorkspaceStrategy(strategy)),
            classification: {
              kind:
                subject !== plan.threadId
                  ? "delegated"
                  : plan.command.scheduledTaskId != null
                    ? "scheduled"
                    : "prepared",
              parentThreadId: subject !== plan.threadId ? plan.threadId : null,
              requestId: plan.command.scheduledTaskId ?? null,
            },
          });
          continue;
        }
        const captured = yield* target.capture(targetInput);
        const birth =
          before === null ? null : yield* readApplicationBirthRecord(ThreadId.make(subject));
        if (before !== null && birth === null)
          return yield* new CheckoutError({ reason: "unavailable" });
        const runs = plan.events.filter(
          (e): e is Extract<OrchestrationV2DomainEvent, { type: "run.created" | "run.updated" }> =>
            e.threadId === subject && (e.type === "run.created" || e.type === "run.updated"),
        );
        const effectRuns = new Set<string>();
        for (const effect of plan.effects.filter(
          (effect) => effect.threadId === subject && role(effect) !== null,
        )) {
          const id = yield* effectRunId(effect);
          if (id !== null) effectRuns.add(id);
        }
        if (effectRuns.size > 1) return yield* new CheckoutError({ reason: "identity" });
        const effectRun = [...effectRuns][0];
        const runEvent =
          effectRun === undefined
            ? runs.at(-1)
            : runs.filter((event) => event.payload.id === effectRun).at(-1);
        let run = runEvent?.payload;
        if (run === undefined && effectRun !== undefined) {
          const rows = yield* sql<{
            payload_json: string;
          }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id=${effectRun} AND thread_id=${subject}`;
          if (rows[0]) run = yield* decodeRun(rows[0].payload_json);
        }
        if (run === undefined) return yield* new CheckoutError({ reason: "unavailable" });
        for (const effect of plan.effects.filter(
          (e) => e.threadId === subject && role(e) !== null,
        )) {
          const request = effect.request;
          const effectRun = yield* effectRunId(effect);
          if (
            (effectRun !== null && effectRun !== run.id) ||
            ("providerThreadId" in request && request.providerThreadId !== run.providerThreadId)
          )
            return yield* new CheckoutError({ reason: "identity" });
          if (request.type === "runtime-request.respond") {
            const requests = yield* sql<{
              run_id: string | null;
            }>`SELECT run_id FROM orchestration_v2_projection_runtime_requests WHERE request_id=${request.requestId} AND thread_id=${subject}`;
            if (requests.length !== 1 || requests[0]!.run_id !== run.id)
              return yield* new CheckoutError({ reason: "identity" });
          }
        }
        const threadEvents = plan.events.filter(
          (e) => e.threadId === subject && e.type === "thread.metadata-updated",
        );
        const lastThread = threadEvents.at(-1);
        if (
          threadEvents.some(
            (event) =>
              event.type === "thread.metadata-updated" &&
              (event.payload.projectId !== targetInput.projectId ||
                event.payload.worktreePath !== targetInput.worktreePath ||
                event.payload.branch !== targetInput.threadBranch),
          )
        )
          return yield* new CheckoutError({ reason: "stale" });
        const owner = yield* checkout.readOwner(captured.checkoutKey);
        candidates.push({
          subjectThreadId: subject,
          before,
          target: {
            ...captured,
            threadRevision:
              lastThread?.type === "thread.metadata-updated"
                ? DateTime.formatIso(lastThread.payload.updatedAt)
                : captured.threadRevision,
          },
          birth: birth === null ? null : { eventId: birth.eventId, sequence: birth.sequence },
          birthEventId: birth?.eventId ?? creation!.id,
          relation: {
            runId: run?.id ?? null,
            attemptId: run?.activeAttemptId ?? null,
            nodeId: run?.rootNodeId ?? null,
            messageId: run?.userMessageId ?? null,
            checkpointScopeId: null,
          },
          origin: {
            kind:
              subject !== plan.threadId
                ? "delegated"
                : plan.command.scheduledTaskId != null
                  ? "scheduled"
                  : plan.command.type.startsWith("prepared-run.") || run?.status === "preparing"
                    ? "prepared"
                    : run?.status === "queued"
                      ? "queue"
                      : "command",
            parentThreadId: subject !== plan.threadId ? plan.threadId : null,
            requestId: plan.command.scheduledTaskId ?? null,
          },
          expectedOwner:
            owner === null ? null : { epoch: owner.owner_epoch, revision: owner.revision },
        });
      }
      const preflight: Conflicts.OrdinaryPreflight[] = candidates.map((candidate) => ({
        command: plan.command,
        commandId: plan.command.commandId,
        commandType: plan.command.type,
        receiptThreadId: plan.threadId,
        subjectThreadId: candidate.subjectThreadId,
        origin: candidate.origin,
        admissionId: `ordinary-admission:${digest({ commandId: plan.command.commandId, subject: candidate.subjectThreadId, checkout: candidate.target.checkoutKey })}`,
        target: candidate.target,
        relation: candidate.relation,
        expectedOwner: candidate.expectedOwner,
        birth: candidate.birth,
        birthEventId: candidate.birthEventId,
        effects: plan.effects
          .filter((e) => e.threadId === candidate.subjectThreadId && role(e) !== null)
          .map((e) => ({ effectId: e.id, request: e.request, role: role(e)! })),
      }));
      const commit = preflight.length === 0 ? null : yield* conflicts.prepareOrdinary(preflight);
      return yield* retain({ plan, candidates, preflight, commit, origins, preparation: null });
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError(mapError),
  );
  const validate = Effect.fn("CheckoutIntent.validate")(function* (intent: PreparedIntent) {
    yield* transaction;
    const state = yield* captured(intent);
    state.validatedTransaction = undefined;
    if (intent.preparation !== null) {
      yield* preparation.validate(intent.preparation, "before");
      state.validatedTransaction = Option.getOrUndefined(
        yield* Effect.serviceOption(sql.transactionService),
      );
      return;
    }
    for (const candidate of [...intent.candidates, ...intent.origins]) {
      if (candidate.before === null) {
        const existing =
          yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id=${candidate.subjectThreadId}`;
        if (existing.length !== 0) return yield* new CheckoutError({ reason: "stale" });
      } else if (digest(yield* readTarget(candidate.subjectThreadId)) !== digest(candidate.before))
        return yield* new CheckoutError({ reason: "stale" });
    }
    state.validatedTransaction = Option.getOrUndefined(
      yield* Effect.serviceOption(sql.transactionService),
    );
  }, Effect.mapError(mapError));
  const accept = Effect.fn("CheckoutIntent.accept")(
    function* (
      intent: PreparedIntent,
      committed: CommittedEffects & { readonly receipt: CommandReceiptV2 },
    ) {
      yield* transaction;
      const { plan } = intent;
      if (
        committed.receipt.commandId !== plan.command.commandId ||
        committed.receipt.commandType !== plan.command.type ||
        committed.receipt.threadId !== plan.threadId ||
        committed.receipt.status !== "accepted" ||
        planDigests({ events: [], effects: committed.effects }).actual !==
          planDigests({ events: [], effects: plan.effects }).actual
      )
        return yield* new CheckoutError({ reason: "identity" });
      const state = yield* captured(intent);
      if (
        state.validatedTransaction === undefined ||
        state.validatedTransaction !==
          Option.getOrUndefined(yield* Effect.serviceOption(sql.transactionService))
      )
        return yield* new CheckoutError({ reason: "stale" });
      if (
        new Set(committed.storedEvents.map((stored) => stored.event.id)).size !==
          committed.storedEvents.length ||
        committed.storedEvents.some(
          (stored, index) =>
            stored.sequence > committed.receipt.resultSequence ||
            (index > 0 && stored.sequence <= committed.storedEvents[index - 1]!.sequence),
        )
      )
        return yield* new CheckoutError({ reason: "identity" });
      const actualPlan = {
        events: committed.storedEvents.map((stored) => stored.event),
        effects: committed.effects,
      };
      if (planDigests(actualPlan).authority !== planDigests(plan).authority)
        return yield* new CheckoutError({ reason: "identity" });
      const operation: OrdinaryCheckout.CommittedCheckoutOperation = {
        prepared: intent,
        command: plan.command,
        threadId: plan.threadId,
        ...committed,
      };
      const witness = yield* OrdinaryCheckout.CheckoutCommitAuthority;
      if (intent.preparation !== null) {
        const prepared = intent.preparation;
        if (prepared.kind === "switch")
          yield* witness.assertSwitch({ ...operation, origin: prepared.origin });
        else yield* witness.assertRelease({ ...operation, origin: prepared.origin });
        yield* Shared.requireLocalAdmission(
          plan.localAcceptance!.live,
          plan.localAcceptance!.expected,
        ).pipe(Effect.provideService(SqlClient.SqlClient, sql), Effect.mapError(mapError));
        const now = DateTime.formatIso(committed.receipt.acceptedAt);
        const digests = planDigests(actualPlan);
        return yield* preparation.consume(
          prepared,
          Effect.gen(function* () {
            const fiber = yield* Effect.fiberId;
            let open = true;
            const scopeCheck = Effect.gen(function* () {
              if (
                !open ||
                fiber !== (yield* Effect.fiberId) ||
                Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql
              )
                return yield* new CheckoutError({ reason: "identity" });
              yield* transaction;
            });
            if (prepared.kind === "switch") {
              const metadata = committed.storedEvents.filter(
                (stored) =>
                  stored.event.threadId === prepared.origin.subjectThreadId &&
                  stored.event.type === "thread.metadata-updated",
              );
              if (metadata.length !== 1) return yield* new CheckoutError({ reason: "identity" });
              const binding: PreparationBinding = {
                version: 1,
                originId: prepared.origin.originId,
                revision: (prepared.predecessor?.revision ?? 0) + 1,
                predecessorDigest: digest(prepared.predecessor ?? prepared.origin),
                before: prepared.before,
                target: prepared.target,
                commandId: plan.command.commandId,
                commandType: plan.command.type,
                commandDigest: prepared.commandDigest,
                receiptThreadId: plan.threadId,
                receiptSequence: committed.receipt.resultSequence,
                authorityPlanDigest: prepared.planDigest,
                acceptedPlanDigest: digests.actual,
                metadataEventId: metadata[0]!.event.id,
                metadataSequence: metadata[0]!.sequence,
                operationId: prepared.reservation.operationId,
                generation: prepared.reservation.generation,
                resultDigest: digest(prepared.result),
                journalOperationKey: plan.localAcceptance!.expected.operationKey,
                journalBindingDigest: plan.localAcceptance!.expected.bindingSha256,
                recordedAt: now,
              };
              return yield* checkout.recordPreparationBinding(binding).pipe(
                Effect.provideService(OrdinaryCheckout.CheckoutCertificateAuthority, {
                  assertOrigin: () => Effect.fail(new CheckoutError({ reason: "identity" })),
                  assertBinding: (input) =>
                    input === binding
                      ? scopeCheck
                      : Effect.fail(new CheckoutError({ reason: "identity" })),
                  assertAdmission: () => Effect.fail(new CheckoutError({ reason: "identity" })),
                  assertLink: () => Effect.fail(new CheckoutError({ reason: "identity" })),
                }),
                Effect.ensuring(
                  Effect.sync(() => {
                    open = false;
                    state.spent = true;
                  }),
                ),
              );
            }
            const starts = committed.effects.filter((effect) => role(effect) !== null);
            const start = starts[0];
            const basis = committed.storedEvents
              .filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.threadId === prepared.origin.subjectThreadId &&
                  stored.event.payload.id === prepared.origin.runId,
              )
              .at(-1);
            if (
              committed.effects.some(
                (effect) => effect.nativeCreationExecutionReference !== undefined,
              ) ||
              starts.length !== 1 ||
              !start ||
              start.request.type !== "provider-turn.start" ||
              start.commandId !== plan.command.commandId ||
              start.threadId !== prepared.origin.subjectThreadId ||
              start.request.runId !== prepared.origin.runId ||
              start.nativeCreationExecutionReference !== undefined ||
              !basis
            )
              return yield* new CheckoutError({ reason: "identity" });
            const release: PreparationRelease = {
              version: 1,
              originId: prepared.origin.originId,
              bindingRevision: prepared.binding.revision,
              bindingDigest: digest(prepared.binding),
              admissionId: preparedAdmissionId(
                prepared.origin.originId,
                prepared.binding.revision,
                prepared.target.checkoutKey,
              ),
              commandId: plan.command.commandId,
              commandType: "prepared-run.release",
              commandDigest: prepared.commandDigest,
              receiptThreadId: plan.threadId,
              receiptSequence: committed.receipt.resultSequence,
              authorityPlanDigest: prepared.planDigest,
              acceptedPlanDigest: digests.actual,
              basisEventId: basis.event.id,
              basisSequence: basis.sequence,
              effectId: start.id,
              requestDigest: digest(start.request),
              target: prepared.target,
              journalOperationKey: plan.localAcceptance!.expected.operationKey,
              journalBindingDigest: plan.localAcceptance!.expected.bindingSha256,
              recordedAt: now,
            };
            const input: OrdinaryCheckout.PreparedAccept = {
              origin: prepared.origin,
              binding: prepared.binding,
              release,
              expectedOwner: prepared.expectedOwner,
              leaseId: `ordinary-prepared-lease:${digest({ origin: prepared.origin.originId, binding: prepared.binding.revision })}`,
            };
            const link: OrdinaryCheckout.Link = {
              effectId: start.id,
              request: start.request,
              role: "launch",
            };
            return yield* Effect.gen(function* () {
              yield* checkout.acceptPrepared(input);
              yield* checkout.linkPreparedStart(input, link);
            }).pipe(
              Effect.provideService(OrdinaryCheckout.CheckoutCertificateAuthority, {
                assertOrigin: () => Effect.fail(new CheckoutError({ reason: "identity" })),
                assertBinding: () => Effect.fail(new CheckoutError({ reason: "identity" })),
                assertAdmission: (value) =>
                  value === input
                    ? scopeCheck
                    : Effect.fail(new CheckoutError({ reason: "identity" })),
                assertLink: (value, effect) =>
                  value === input && effect === link
                    ? scopeCheck
                    : Effect.fail(new CheckoutError({ reason: "identity" })),
              }),
              Effect.ensuring(
                Effect.sync(() => {
                  open = false;
                  state.spent = true;
                }),
              ),
            );
          }),
        );
      }
      yield* witness.assertOrigin(operation);
      for (const candidate of intent.origins) {
        if (plan.localAcceptance === undefined)
          return yield* new CheckoutError({ reason: "unavailable" });
        yield* Shared.requireLocalAdmission(
          plan.localAcceptance.live,
          plan.localAcceptance.expected,
        ).pipe(Effect.provideService(SqlClient.SqlClient, sql), Effect.mapError(mapError));
        const basis = committed.storedEvents.find(
          (stored) =>
            stored.event.id === candidate.basisEventId && stored.event.type === "run.created",
        );
        const creation = committed.storedEvents.find(
          (stored) =>
            stored.event.id === candidate.birthEventId && stored.event.type === "thread.created",
        );
        const birth =
          candidate.birth ??
          (creation ? { eventId: creation.event.id, sequence: creation.sequence } : null);
        if (!basis || birth === null) return yield* new CheckoutError({ reason: "identity" });
        const origin: PreparationOrigin = {
          version: 1,
          originId: preparationOriginId({
            commandId: plan.command.commandId,
            subjectThreadId: candidate.subjectThreadId,
            birth,
            runId: candidate.run.id,
            attemptId: candidate.run.activeAttemptId,
          }),
          commandId: plan.command.commandId,
          commandType: plan.command.type,
          commandDigest: candidate.commandDigest,
          receiptThreadId: plan.threadId,
          receiptSequence: committed.receipt.resultSequence,
          subjectThreadId: candidate.subjectThreadId,
          birth,
          runId: candidate.run.id,
          attemptId: candidate.run.activeAttemptId,
          nodeId: candidate.run.rootNodeId,
          messageId: candidate.run.userMessageId,
          providerThreadId: candidate.run.providerThreadId,
          basisEventId: basis.event.id,
          basisSequence: basis.sequence,
          authorityPlanDigest: planDigests(plan).authority,
          acceptedPlanDigest: planDigests(actualPlan).actual,
          strategyJson: candidate.strategyJson,
          strategyDigest: digest(yield* decodeStrategyDigest(candidate.strategyJson)),
          before: candidate.before,
          initial: candidate.initial,
          classification: candidate.classification,
          journalOperationKey: plan.localAcceptance.expected.operationKey,
          journalBindingDigest: plan.localAcceptance.expected.bindingSha256,
          recordedAt: DateTime.formatIso(committed.receipt.acceptedAt),
        };
        const fiber = yield* Effect.fiberId;
        let open = true;
        yield* checkout.recordPreparationOrigin(origin).pipe(
          Effect.provideService(OrdinaryCheckout.CheckoutCertificateAuthority, {
            assertOrigin: Effect.fn("CheckoutIntent.assertOrigin")(function* (
              input: PreparationOrigin,
            ) {
              if (
                !open ||
                input !== origin ||
                fiber !== (yield* Effect.fiberId) ||
                Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql
              )
                return yield* new CheckoutError({ reason: "identity" });
              yield* transaction;
            }),
            assertBinding: () => Effect.fail(new CheckoutError({ reason: "identity" })),
            assertAdmission: () => Effect.fail(new CheckoutError({ reason: "identity" })),
            assertLink: () => Effect.fail(new CheckoutError({ reason: "identity" })),
          }),
          Effect.ensuring(
            Effect.sync(() => {
              open = false;
            }),
          ),
        );
      }
      const inputs: OrdinaryCheckout.Accept[] = [];
      for (const candidate of intent.preflight) {
        const basis = committed.storedEvents
          .filter((e) => e.event.threadId === candidate.subjectThreadId)
          .at(-1);
        if (!basis || !plan.events.some((e) => e.id === basis.event.id))
          return yield* new CheckoutError({ reason: "identity" });
        const created = committed.storedEvents.find(
          (e) =>
            e.event.id === candidate.birthEventId &&
            e.event.type === "thread.created" &&
            e.event.threadId === candidate.subjectThreadId,
        );
        const birth =
          candidate.birth ??
          (created === undefined
            ? null
            : { eventId: created.event.id, sequence: created.sequence });
        if (birth === null) return yield* new CheckoutError({ reason: "identity" });
        const now = DateTime.formatIso(committed.receipt.acceptedAt);
        const identity = {
          commandId: plan.command.commandId,
          subject: candidate.subjectThreadId,
          checkout: candidate.target.checkoutKey,
        };
        inputs.push({
          ...candidate,
          birth,
          leaseId: `ordinary-lease:${digest({ ...identity, birth })}`,
          receiptSequence: committed.receipt.resultSequence,
          basisEventId: basis.event.id,
          basisSequence: basis.sequence,
          now,
          expiresAt: now,
        });
      }
      if (inputs.length === 0) {
        state.spent = true;
        return;
      }
      if (intent.commit === null) return yield* new CheckoutError({ reason: "identity" });
      const acceptingFiber = yield* Effect.fiberId;
      let accepting = true;
      yield* intent.commit(inputs).pipe(
        Effect.provideService(OrdinaryCheckout.CheckoutAcceptanceAuthority, {
          assertNewAcceptance: Effect.fn("CheckoutIntent.assertNewAcceptance")(function* (
            input: OrdinaryCheckout.Accept,
          ) {
            if (
              !accepting ||
              !inputs.includes(input) ||
              acceptingFiber !== (yield* Effect.fiberId) ||
              Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql
            )
              return yield* new CheckoutError({ reason: "identity" });
            yield* transaction;
          }),
          assertDerivedLink: () => Effect.fail(new CheckoutError({ reason: "unavailable" })),
        }),
        Effect.ensuring(
          Effect.sync(() => {
            accepting = false;
            state.spent = true;
          }),
        ),
      );
    },
    (effect, intent) =>
      effect.pipe(
        Effect.mapError(mapError),
        Effect.ensuring(
          Effect.sync(() => {
            const state = captures.get(intent);
            if (state) state.spent = true;
          }),
        ),
      ),
  );
  const linkEffects = Effect.fn("CheckoutIntent.linkEffects")(function* (
    committed: CommittedEffects,
  ) {
    yield* transaction;
    yield* (yield* OrdinaryCheckout.CheckoutCommitAuthority).assertDerived(committed);
    for (const event of committed.storedEvents) {
      if (
        event.event.type !== "thread.metadata-updated" ||
        !(yield* hasTable("jones_ordinary_checkout_preparing_origins"))
      )
        continue;
      const rows =
        yield* sql`SELECT o.origin_id FROM jones_ordinary_checkout_preparing_origins o WHERE o.subject_thread_id=${event.event.threadId}
        AND NOT EXISTS (SELECT 1 FROM jones_ordinary_checkout_preparing_releases r WHERE r.origin_id=o.origin_id)`;
      if (rows.length > 0) return yield* new CheckoutError({ reason: "unavailable" });
    }
    for (const effect of committed.effects) {
      const effectRole = role(effect);
      if (effectRole === null) continue;
      const request = effect.request;
      const runId = yield* effectRunId(effect);
      if (runId === null) return yield* new CheckoutError({ reason: "unavailable" });
      const rows =
        yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions WHERE subject_thread_id=${effect.threadId} AND run_id=${runId}`;
      if (rows.length !== 1) return yield* new CheckoutError({ reason: "unavailable" });
      const admission = rows[0]!;
      const uses = yield* sql<{
        operation_id: string;
        revision: number;
      }>`SELECT operation_id,revision FROM jones_ordinary_checkout_all_uses WHERE admission_id=${admission.admission_id} AND state IN ('reserved','started','draining')`;
      const runRows = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id=${runId} AND thread_id=${effect.threadId}`;
      const currentRun = runRows[0] ? yield* decodeRun(runRows[0].payload_json) : null;
      if (
        !currentRun ||
        currentRun.activeAttemptId !== admission.attempt_id ||
        ("providerThreadId" in request && currentRun.providerThreadId !== request.providerThreadId)
      )
        return yield* new CheckoutError({ reason: "stale" });
      const basis = committed.storedEvents
        .filter((stored) => stored.event.threadId === effect.threadId)
        .at(-1);
      if (
        uses.length !== 1 ||
        !basis ||
        ((basis.event.type === "run.updated" || basis.event.type === "run.created") &&
          basis.event.payload.activeAttemptId !== admission.attempt_id)
      )
        return yield* new CheckoutError({ reason: "unavailable" });
      const link: OrdinaryCheckout.DerivedLink = {
        operationId: uses[0]!.operation_id,
        expectedRevision: uses[0]!.revision,
        admissionId: admission.admission_id,
        basisEventId: basis.event.id,
        basisSequence: basis.sequence,
        link: { effectId: effect.id, request, role: effectRole },
        now: DateTime.formatIso(basis.event.occurredAt),
      };
      const fiber = yield* Effect.fiberId;
      let open = true;
      yield* checkout.linkDerived(link).pipe(
        Effect.provideService(OrdinaryCheckout.CheckoutAcceptanceAuthority, {
          assertNewAcceptance: () => Effect.fail(new CheckoutError({ reason: "unavailable" })),
          assertDerivedLink: Effect.fn("CheckoutIntent.assertDerivedLink")(function* (
            input: OrdinaryCheckout.DerivedLink,
            current: Admission,
          ) {
            if (
              !open ||
              fiber !== (yield* Effect.fiberId) ||
              Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
              input !== link ||
              current.admission_id !== admission.admission_id
            )
              return yield* new CheckoutError({ reason: "identity" });
            yield* transaction;
          }),
        }),
        Effect.ensuring(
          Effect.sync(() => {
            open = false;
          }),
        ),
      );
    }
  }, Effect.mapError(mapError));
  return CheckoutIntent.of({
    replay,
    prepare,
    validate,
    accept,
    linkEffects,
    validateSwitch: Effect.fn("CheckoutIntent.validateSwitch")((intent) =>
      intent.preparation?.kind === "switch"
        ? validate(intent)
        : Effect.fail(new CheckoutError({ reason: "identity" })),
    ),
    validateRelease: Effect.fn("CheckoutIntent.validateRelease")((intent) =>
      intent.preparation?.kind === "release"
        ? validate(intent)
        : Effect.fail(new CheckoutError({ reason: "identity" })),
    ),
    acceptSwitch: Effect.fn("CheckoutIntent.acceptSwitch")((intent, committed) =>
      intent.preparation?.kind === "switch"
        ? accept(intent, committed)
        : Effect.fail(new CheckoutError({ reason: "identity" })),
    ),
    acceptRelease: Effect.fn("CheckoutIntent.acceptRelease")((intent, committed) =>
      intent.preparation?.kind === "release"
        ? accept(intent, committed)
        : Effect.fail(new CheckoutError({ reason: "identity" })),
    ),
  });
});

// Inactive source composition: the application must not install this until DC-C is qualified.
export const layer: Layer.Layer<
  CheckoutIntent,
  never,
  | SqlClient.SqlClient
  | OrdinaryCheckout.OrdinaryCheckout
  | CheckoutTarget.CheckoutTarget
  | Conflicts.CheckoutConflictIntegration
  | Preparation.CheckoutPreparation
> = Layer.effect(CheckoutIntent, make);
