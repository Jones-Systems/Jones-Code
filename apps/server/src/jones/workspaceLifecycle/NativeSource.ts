import { EventId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { readApplicationBirthRecord } from "../importedHistory/ApplicationBirth.ts";
import { canonical, digest, Target } from "../ordinaryCheckout/CheckoutProtocol.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Targets from "../ordinaryCheckout/CheckoutTarget.ts";
import * as Store from "./TransitionStore.ts";
import * as Sources from "./CheckoutSource.ts";
import { LifecycleError } from "./Protocol.ts";
import {
  NativeLifecycleSource,
  type NativeSource,
  type NativeTransition,
} from "./WorkspaceLifecycle.ts";

export interface NativeOwnerEvidence {
  readonly id: string;
  readonly generation: string;
}
export class NativeCheckoutOwner extends Context.Service<
  NativeCheckoutOwner,
  {
    readonly verify: (
      input: NativeTransition,
      target: Target,
      birth: { readonly eventId: string; readonly sequence: number },
    ) => Effect.Effect<NativeOwnerEvidence, LifecycleError>;
    /** Native SQL validation only; EventSink invokes this within its transaction. */
    readonly validate: (
      input: NativeTransition,
      evidence: NativeOwnerEvidence,
    ) => Effect.Effect<void, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/NativeSource/NativeCheckoutOwner") {}

export class LifecycleConfiguration extends Context.Service<
  LifecycleConfiguration,
  {
    readonly environmentId: string;
    readonly projects: ReadonlyMap<
      string,
      { readonly repository: string; readonly projectRoot: string }
    >;
  }
>()("t3/jones/workspaceLifecycle/NativeSource/LifecycleConfiguration") {}

const threadSchema = Schema.fromJsonString(
  Schema.Struct({
    id: Schema.String,
    projectId: Schema.String,
    createdAt: Schema.String,
    updatedAt: Schema.String,
    worktreePath: Schema.NullOr(Schema.String),
    branch: Schema.NullOr(Schema.String),
    deletedAt: Schema.NullOr(Schema.String),
  }),
);
const isLifecycleError = Schema.is(LifecycleError);
const decodeThread = Schema.decodeEffect(threadSchema);
const decodeBirthIdentity = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.String,
      projectId: Schema.String,
      createdAt: Schema.String,
    }),
  ),
);
const decodePriorSource = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      captured: Schema.Struct({ ownership: Schema.String }),
    }),
  ),
);
const decodePriorOwnership = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      birth: Schema.Unknown,
      target: Target,
    }),
  ),
);
const decodeOwnership = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      birth: Schema.Unknown,
      target: Target,
      owner: Schema.Struct({ id: Schema.String, generation: Schema.String }),
    }),
  ),
);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const configuration = yield* LifecycleConfiguration;
  const observations = yield* Sources.CheckoutSourceObservation;
  const directories = yield* Targets.CheckoutTargetObservation;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = yield* Git.GitVcsDriver;
  const store = yield* Store.TransitionStore;
  const nativeOwner = yield* Effect.serviceOption(NativeCheckoutOwner);
  const capture = Effect.fn("NativeLifecycleSource.capture")(
    function* (input: NativeTransition) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new LifecycleError({ reason: "transaction" });
      // A projection assignment is not executor authority. No decoder, configuration or default issuer can fill this port.
      if (Option.isNone(nativeOwner)) return yield* new LifecycleError({ reason: "unavailable" });
      const rows = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${input.threadId}`;
      if (!rows[0]) return yield* new LifecycleError({ reason: "identity" });
      const thread = yield* decodeThread(rows[0].payload_json);
      const selected = configuration.projects.get(thread.projectId);
      if (
        !selected ||
        thread.id !== input.threadId ||
        (thread.deletedAt !== null && input.boundary !== "cleanup") ||
        thread.worktreePath === null
      )
        return yield* new LifecycleError({ reason: "identity" });
      const projects = yield* sql<{
        workspace_root: string;
        updated_at: string;
        deleted_at: string | null;
      }>`SELECT workspace_root,updated_at,deleted_at FROM projection_projects WHERE project_id=${thread.projectId}`;
      const project = projects[0];
      let birth = yield* readApplicationBirthRecord(ThreadId.make(input.threadId)).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      if (birth === null && thread.deletedAt !== null && input.boundary === "cleanup") {
        const births = yield* sql<{
          event_id: string;
          sequence: number;
          payload_json: string;
        }>`SELECT event_id,sequence,payload_json FROM orchestration_events WHERE stream_id=${input.threadId} AND application_event_version=2 AND aggregate_kind='thread' AND event_type='thread.created' ORDER BY sequence DESC LIMIT 1`;
        if (births[0]) {
          const identity = yield* decodeBirthIdentity(births[0].payload_json);
          if (
            identity.id === thread.id &&
            identity.projectId === thread.projectId &&
            identity.createdAt === thread.createdAt
          )
            birth = {
              kind: "application_v2_thread_birth",
              threadId: ThreadId.make(thread.id),
              eventId: EventId.make(births[0].event_id),
              sequence: births[0].sequence,
            };
        }
      }
      if (
        !project ||
        project.deleted_at !== null ||
        project.workspace_root !== selected.projectRoot ||
        birth === null
      )
        return yield* new LifecycleError({ reason: "identity" });
      const targetInput = {
        projectId: thread.projectId,
        threadId: thread.id,
        projectRoot: project.workspace_root,
        worktreePath: thread.worktreePath,
        threadBranch: thread.branch,
        projectRevision: project.updated_at,
        threadRevision: thread.updatedAt,
      };
      const missing = !(yield* fs.exists(thread.worktreePath));
      const observed = yield* missing
        ? Effect.gen(function* () {
            if (input.boundary !== "recreate")
              return yield* new LifecycleError({ reason: "unavailable" });
            const prior = yield* store.prior(path.normalize(thread.worktreePath!));
            if (
              prior === null ||
              prior.binding.thread_id !== thread.id ||
              prior.binding.branch !==
                (thread.branch?.startsWith("refs/") ? thread.branch : `refs/heads/${thread.branch}`)
            )
              return yield* new LifecycleError({ reason: "identity" });
            const recorded = yield* decodePriorSource(prior.source);
            const ownership = yield* decodePriorOwnership(recorded.captured.ownership);
            if (canonical(ownership.birth) !== canonical(birth))
              return yield* new LifecycleError({ reason: "stale" });
            const projectGit = yield* directories.git(project.workspace_root);
            if (
              !projectGit ||
              projectGit.commonDirectory !== prior.binding.git_common_dir ||
              canonical(projectGit.commonIdentity) !== canonical(ownership.target.gitCommonIdentity)
            )
              return yield* new LifecycleError({ reason: "stale" });
            const revision = yield* git.execute({
              operation: "WorkspaceLifecycle.recreationSource",
              cwd: project.workspace_root,
              args: ["rev-parse", "--verify", `${prior.binding.branch}^{commit}`],
              maxOutputBytes: 4096,
              env: { GIT_OPTIONAL_LOCKS: "0", LC_ALL: "C" },
            });
            if (
              revision.stdoutTruncated ||
              revision.stderrTruncated ||
              revision.exitCode !== 0 ||
              revision.stdout.trim() !== prior.binding.head
            )
              return yield* new LifecycleError({ reason: "stale" });
            if (yield* fs.exists(thread.worktreePath!))
              return yield* new LifecycleError({ reason: "stale" });
            return { target: { ...ownership.target, ...targetInput }, head: prior.binding.head };
          })
        : observations.capture(targetInput);
      const target = observed.target;
      if (
        target.gitCommonDirectory === null ||
        target.gitDirectory === null ||
        target.branch === null ||
        target.gitCommonDirectory === target.gitDirectory ||
        target.cwd !== target.checkoutKey
      )
        return yield* new LifecycleError({ reason: "identity" });
      const physical = {
        directory: target.directoryIdentity,
        gitCommon: target.gitCommonIdentity,
        gitDirectory: target.gitDirectoryIdentity,
      };
      const owner = yield* nativeOwner.value.verify(input, target, birth);
      if (
        !owner.id.trim() ||
        !owner.generation.trim() ||
        owner.id.length > 1024 ||
        owner.generation.length > 1024
      )
        return yield* new LifecycleError({ reason: "identity" });
      return {
        binding: {
          environment_id: configuration.environmentId,
          thread_id: thread.id,
          incarnation_id: `jones-checkout:${digest({ birth, physical })}`,
          repository: selected.repository,
          worktree: target.checkoutKey,
          git_common_dir: target.gitCommonDirectory,
          git_dir: target.gitDirectory,
          branch: target.branch,
          head: observed.head,
          source_id: `jones-git:${digest({ physical, head: observed.head, owner })}`,
        },
        ownership: canonical({ birth, target, missing, owner }),
      } satisfies NativeSource;
    },
    Effect.mapError((cause) =>
      isLifecycleError(cause) ? cause : new LifecycleError({ reason: "unavailable", cause }),
    ),
  );
  return NativeLifecycleSource.of({
    capture,
    validateOwnership: Effect.fn("NativeLifecycleSource.validateOwnership")(
      function* (input, source) {
        if (Option.isNone(nativeOwner)) return yield* new LifecycleError({ reason: "unavailable" });
        const recorded = yield* decodeOwnership(source.ownership);
        const current = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${input.threadId}`;
        if (!current[0]) return yield* new LifecycleError({ reason: "identity" });
        const thread = yield* decodeThread(current[0].payload_json);
        const projects = yield* sql<{
          workspace_root: string;
          updated_at: string;
          deleted_at: string | null;
        }>`SELECT workspace_root,updated_at,deleted_at FROM projection_projects WHERE project_id=${thread.projectId}`;
        const project = projects[0];
        const birth = yield* readApplicationBirthRecord(ThreadId.make(input.threadId)).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const target = recorded.target;
        if (
          !project ||
          project.deleted_at !== null ||
          thread.deletedAt !== null ||
          thread.id !== input.threadId ||
          thread.projectId !== target.projectId ||
          project.workspace_root !== target.projectRoot ||
          project.updated_at !== target.projectRevision ||
          thread.updatedAt !== target.threadRevision ||
          thread.worktreePath !== target.worktreePath ||
          thread.branch !== target.threadBranch ||
          canonical(birth) !== canonical(recorded.birth)
        )
          return yield* new LifecycleError({ reason: "stale" });
        yield* nativeOwner.value.validate(input, recorded.owner);
      },
      Effect.mapError((cause) =>
        isLifecycleError(cause) ? cause : new LifecycleError({ reason: "storage", cause }),
      ),
    ),
    revalidate: Effect.fn("NativeLifecycleSource.revalidate")(function* (input, source) {
      if (canonical(yield* capture(input)) !== canonical(source))
        return yield* new LifecycleError({ reason: "stale" });
    }),
  });
});
export const layer = Layer.effect(NativeLifecycleSource, make);
