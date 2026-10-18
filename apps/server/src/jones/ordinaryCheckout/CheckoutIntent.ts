import {
  ThreadId,
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

export interface IntentCommand {
  readonly commandId: string;
  readonly type: string;
  readonly scheduledTaskId?: string | null | undefined;
}
export interface IntentPlan {
  readonly command: IntentCommand;
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
export interface PreparedIntent {
  readonly plan: IntentPlan;
  readonly candidates: readonly Candidate[];
  readonly preflight: readonly Conflicts.OrdinaryPreflight[];
  readonly commit: Conflicts.CommitOrdinary | null;
}
interface CommittedEffects {
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
    ) => Effect.Effect<void, CheckoutError>;
    readonly linkEffects: (committed: CommittedEffects) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutIntent") {}

const role = (effect: PendingOrchestrationEffectV2): OrdinaryCheckout.Link["role"] | null => {
  if (effect.nativeCreationExecutionReference !== undefined) return null;
  switch (effect.request.type) {
    case "provider-turn.start":
      return "launch";
    case "provider-turn.interrupt":
    case "provider-turn.steer":
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
  const transaction = Effect.gen(function* () {
    if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new CheckoutError({ reason: "unavailable" });
  });
  const readTarget = (threadId: string) =>
    Effect.gen(function* () {
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
  const replay = (command: IntentCommand, threadId: string) =>
    Effect.gen(function* () {
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
    }).pipe(Effect.mapError(mapError));
  const prepare = (plan: IntentPlan) =>
    Effect.gen(function* () {
      if (yield* replay(plan.command, plan.threadId))
        return { plan, candidates: [], preflight: [], commit: null };
      const subjects = new Set(plan.effects.filter((e) => role(e) !== null).map((e) => e.threadId));
      for (const event of plan.events)
        if (event.type === "run.created") subjects.add(event.threadId);
      const candidates: Candidate[] = [];
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
        const captured = yield* target.capture(targetInput);
        const birth =
          before === null ? null : yield* readApplicationBirthRecord(ThreadId.make(subject));
        if (before !== null && birth === null)
          return yield* new CheckoutError({ reason: "unavailable" });
        const runs = plan.events.filter(
          (e): e is Extract<OrchestrationV2DomainEvent, { type: "run.created" | "run.updated" }> =>
            e.threadId === subject && (e.type === "run.created" || e.type === "run.updated"),
        );
        if (runs.length > 1 && new Set(runs.map((e) => e.payload.id)).size > 1)
          return yield* new CheckoutError({ reason: "unavailable" });
        const runEvent = runs.at(-1);
        const run =
          runEvent?.type === "run.created" || runEvent?.type === "run.updated"
            ? runEvent.payload
            : undefined;
        if (run === undefined) return yield* new CheckoutError({ reason: "unavailable" });
        for (const effect of plan.effects.filter(
          (e) => e.threadId === subject && role(e) !== null,
        )) {
          const request = effect.request;
          const effectRun =
            "runId" in request
              ? request.runId
              : request.type === "provider-runtime.continue"
                ? request.sourceRunId
                : null;
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
          lastThread?.type === "thread.metadata-updated" &&
          (lastThread.payload.projectId !== targetInput.projectId ||
            lastThread.payload.worktreePath !== targetInput.worktreePath ||
            lastThread.payload.branch !== targetInput.threadBranch)
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
      return { plan, candidates, preflight, commit };
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql), Effect.mapError(mapError));
  const validate = (intent: PreparedIntent) =>
    Effect.gen(function* () {
      yield* transaction;
      for (const candidate of intent.candidates) {
        if (candidate.before === null) {
          const existing =
            yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id=${candidate.subjectThreadId}`;
          if (existing.length !== 0) return yield* new CheckoutError({ reason: "stale" });
        } else if (
          digest(yield* readTarget(candidate.subjectThreadId)) !== digest(candidate.before)
        )
          return yield* new CheckoutError({ reason: "stale" });
      }
    }).pipe(Effect.mapError(mapError));
  const accept = (
    intent: PreparedIntent,
    committed: CommittedEffects & { readonly receipt: CommandReceiptV2 },
  ) =>
    Effect.gen(function* () {
      yield* transaction;
      const { plan } = intent;
      if (
        committed.receipt.commandId !== plan.command.commandId ||
        committed.receipt.commandType !== plan.command.type ||
        committed.receipt.threadId !== plan.threadId ||
        committed.receipt.status !== "accepted" ||
        digest(committed.effects) !== digest(plan.effects)
      )
        return yield* new CheckoutError({ reason: "identity" });
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
      if (inputs.length === 0) return;
      if (intent.commit === null) return yield* new CheckoutError({ reason: "identity" });
      yield* intent.commit(inputs).pipe(
        Effect.provideService(OrdinaryCheckout.CheckoutAcceptanceAuthority, {
          assertNewAcceptance: (input) =>
            inputs.includes(input)
              ? transaction
              : Effect.fail(new CheckoutError({ reason: "identity" })),
          assertDerivedLink: () => Effect.fail(new CheckoutError({ reason: "unavailable" })),
        }),
      );
    }).pipe(Effect.mapError(mapError));
  const linkEffects = (committed: CommittedEffects) =>
    Effect.gen(function* () {
      yield* transaction;
      for (const effect of committed.effects) {
        const effectRole = role(effect);
        if (effectRole === null) continue;
        const request = effect.request;
        const runId =
          "runId" in request
            ? request.runId
            : request.type === "provider-runtime.continue"
              ? request.sourceRunId
              : null;
        if (runId === null) return yield* new CheckoutError({ reason: "unavailable" });
        const rows =
          yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions WHERE subject_thread_id=${effect.threadId} AND run_id=${runId}`;
        if (rows.length !== 1) return yield* new CheckoutError({ reason: "unavailable" });
        const admission = rows[0]!;
        const uses = yield* sql<{
          operation_id: string;
          revision: number;
        }>`SELECT operation_id,revision FROM jones_ordinary_checkout_uses WHERE admission_id=${admission.admission_id} AND state IN ('reserved','started','draining')`;
        const basis = committed.storedEvents
          .filter(
            (e) =>
              e.event.threadId === effect.threadId &&
              (e.event.type === "run.updated" || e.event.type === "run.created") &&
              e.event.payload.id === runId,
          )
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
        yield* checkout.linkDerived(link).pipe(
          Effect.provideService(OrdinaryCheckout.CheckoutAcceptanceAuthority, {
            assertNewAcceptance: () => Effect.fail(new CheckoutError({ reason: "unavailable" })),
            assertDerivedLink: (input, current) =>
              input === link && current.admission_id === admission.admission_id
                ? transaction
                : Effect.fail(new CheckoutError({ reason: "identity" })),
          }),
        );
      }
    }).pipe(Effect.mapError(mapError));
  return CheckoutIntent.of({ replay, prepare, validate, accept, linkEffects });
});

// Inactive source composition: the application must not install this until DC-C is qualified.
export const layer: Layer.Layer<
  CheckoutIntent,
  never,
  | SqlClient.SqlClient
  | OrdinaryCheckout.OrdinaryCheckout
  | CheckoutTarget.CheckoutTarget
  | Conflicts.CheckoutConflictIntegration
> = Layer.effect(CheckoutIntent, make);
