import * as EffectOutbox from "./EffectOutbox.ts";
import { awaitThreadCreationCleanup } from "./ThreadDeletion.ts";
import {
  CommandId,
  EventId,
  GitCommandError,
  QueueDispatchCommand,
  type OrchestrationV2LegacyPreflightBinding,
  type OrchestrationV2PrivateEvent,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as Projects from "../project/ProjectService.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as Threads from "./ThreadManagementService.ts";
import { canonicalLegacyPayload } from "./LegacyBootstrap.ts";

export type LegacyPreflightOutcome = Extract<
  OrchestrationV2PrivateEvent,
  { type: "legacy-bootstrap.preflight-outcome" }
>["payload"];

class PreflightValidationFailure extends Schema.TaggedError<PreflightValidationFailure>()(
  "PreflightValidationFailure",
  { detail: Schema.String },
) {}

export const makeLegacyPreflight = Effect.gen(function* () {
  const git = yield* GitWorkflow.GitWorkflowService;
  const projects = yield* Projects.ProjectService;
  const threads = yield* Threads.ThreadManagementService;
  const sink = yield* EventSink.EventSinkV2;
  const store = yield* EventStore.EventStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const lane = yield* Semaphore.make(1);
  return Effect.fn("LegacyBootstrap.preflight")(function* (
    binding: OrchestrationV2LegacyPreflightBinding,
  ) {
    const policy = binding.policy;
    const project = yield* projects.getById(policy.projectId);
    if (Option.isNone(project) || project.value.workspaceRoot !== binding.fetch.cwd)
      return yield* new PreflightValidationFailure({
        detail: "Bootstrap project checkout binding changed.",
      });
    const shell = yield* threads.getThreadShell(policy.threadId);
    if (shell !== null && shell.projectId !== policy.projectId)
      return yield* new PreflightValidationFailure({
        detail: "Bootstrap target belongs to another project.",
      });
    yield* awaitThreadCreationCleanup(outbox, policy.threadId);
    const intentId = CommandId.make(`${policy.createCommandId}:preflight-intent`);
    const outcomeId = CommandId.make(`${policy.createCommandId}:preflight-outcome`);
    const intent = yield* sink.commitLegacyPreflight({
      commandId: intentId,
      event: {
        id: EventId.make(`${intentId}:event`),
        type: "legacy-bootstrap.preflight-intent",
        threadId: policy.threadId,
        occurredAt: yield* DateTime.now,
        payload: binding,
      },
    });
    const prior = Array.from(
      yield* store.readByCommandId({ commandId: outcomeId }).pipe(Stream.runCollect),
    );
    if (prior.length > 0) {
      const recorded = prior[0];
      if (
        prior.length !== 1 ||
        recorded?.event.type !== "legacy-bootstrap.preflight-outcome" ||
        canonicalLegacyPayload(recorded.event.payload.binding) !== canonicalLegacyPayload(binding)
      )
        return yield* new PreflightValidationFailure({
          detail: "Preflight outcome binding changed.",
        });
      yield* sink.commitLegacyPreflight({ commandId: outcomeId, event: recorded.event });
      return recorded.event.payload;
    }
    if (!intent.committed)
      return {
        binding,
        intentCommandId: intentId,
        intentSequence: intent.receipt.resultSequence,
        status: "unknown" as const,
        detail:
          "Accepted preflight intent has no durable outcome; fetch may already have executed.",
      } satisfies LegacyPreflightOutcome;

    const payload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(QueueDispatchCommand))(
      binding.canonicalPayload,
    );
    const work = Effect.gen(function* () {
      if (payload.type !== "thread.turn.start" || payload.bootstrap?.prepareWorktree === undefined)
        return yield* new PreflightValidationFailure({
          detail: "Missing closed bootstrap preparation input.",
        });
      const prepare = payload.bootstrap.prepareWorktree;
      const cwd = binding.fetch.cwd;
      let canPrepare = yield* git.isRepository(cwd);
      let baseRef = binding.fetch.baseRef;
      if (canPrepare) {
        if (
          binding.fetch.startFromOrigin &&
          (yield* git.remoteExists({ cwd, remoteName: "origin" }))
        ) {
          yield* git.fetchRemote({ cwd, remoteName: "origin", refName: baseRef });
          if (yield* git.remoteBranchExists({ cwd, refName: baseRef, remoteName: "origin" })) {
            baseRef = (yield* git.resolveRemoteTrackingCommit({
              cwd,
              refName: baseRef,
              fallbackRemoteName: "origin",
            })).commitSha;
          }
        }
        canPrepare = yield* git.hasCommit({ cwd, refName: baseRef });
      }
      if (!canPrepare && binding.fetch.requireWorktree)
        return yield* new PreflightValidationFailure({
          detail: "A separate worktree requires a Git repository and a base branch with a commit.",
        });
      return canPrepare
        ? {
            type: "worktree" as const,
            baseRef,
            startFromOrigin: false,
            ...(prepare.branch === undefined ? {} : { branch: prepare.branch }),
          }
        : {
            type: "root" as const,
            ...(payload.bootstrap.createThread?.branch == null
              ? {}
              : { branch: payload.bootstrap.createThread.branch }),
          };
    });
    const result = yield* work.pipe(Effect.exit);
    let outcome: LegacyPreflightOutcome;
    if (result._tag === "Success")
      outcome = {
        binding,
        intentCommandId: intentId,
        intentSequence: intent.receipt.resultSequence,
        status: "ready",
        workspaceStrategy: result.value,
      };
    else {
      const cause = Cause.squash(result.cause);
      const known =
        Schema.is(PreflightValidationFailure)(cause) ||
        (Schema.is(GitCommandError)(cause) && cause.exitCode !== undefined);
      outcome = {
        binding,
        intentCommandId: intentId,
        intentSequence: intent.receipt.resultSequence,
        status: known ? "known_failed" : "unknown",
        detail: Schema.is(PreflightValidationFailure)(cause)
          ? cause.detail
          : cause instanceof Error
            ? cause.message
            : String(cause),
      };
    }
    yield* sink
      .commitLegacyPreflight({
        commandId: outcomeId,
        event: {
          id: EventId.make(`${outcomeId}:event`),
          type: "legacy-bootstrap.preflight-outcome",
          threadId: policy.threadId,
          occurredAt: yield* DateTime.now,
          payload: outcome,
        },
      })
      .pipe(Effect.uninterruptible);
    return outcome;
  }, lane.withPermits(1));
});
