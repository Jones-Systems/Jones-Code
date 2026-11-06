// @effect-diagnostics nodeBuiltinImport:off -- The sidecar adapter uses create-only hard links and no-follow bounded reads in the observed Git admin directory.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { readBreadcrumb } from "../workspaceRecreation/Native.ts";
import type { WorktreeProvenanceEntry } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { RecordedEvent } from "../../orchestration-v2/RecordedTypes.ts";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import { canonical, digest } from "../ordinaryCheckout/CheckoutProtocol.ts";
import * as Targets from "../ordinaryCheckout/CheckoutTarget.ts";
import * as TargetNode from "../ordinaryCheckout/CheckoutTargetNode.ts";
import * as Sources from "./CheckoutSource.ts";
import {
  LifecycleError,
  Provenance as ProvenanceRecord,
  type Provenance as Record,
  type ProvenanceEntry,
  type ProvenanceFact,
  type ProvenanceReason,
} from "./Protocol.ts";
import * as Store from "./TransitionStore.ts";

interface Capture {
  readonly threadId: string;
  readonly reason: ProvenanceReason;
  readonly createdBy: string;
  readonly baseRef?: string | null;
  readonly baseHead?: string | null;
  readonly baseIsCurrentHead?: boolean;
  readonly predecessor?: string | null;
  readonly taskId?: string | null;
  readonly runId?: string;
  readonly expected?: Sources.CheckoutSource;
  readonly requireExistingPhysical?: boolean;
}
export class Provenance extends Context.Service<
  Provenance,
  {
    readonly capture: (input: Capture) => Effect.Effect<Record, LifecycleError>;
    readonly record: (
      source: Sources.CheckoutSource,
      fact: ProvenanceFact,
    ) => Effect.Effect<void, LifecycleError>;
    readonly lookup: Store.ProvenanceStore["Service"]["lookup"];
    readonly production?: (
      threadId: string,
      reference: string,
      worktreePath: string,
    ) => Effect.Effect<void, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/Provenance") {}

export class ProvenanceSidecar extends Context.Service<
  ProvenanceSidecar,
  {
    readonly publish: (record: Record) => Effect.Effect<void, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/Provenance/ProvenanceSidecar") {}

const publish = Effect.fn("ProvenanceSidecar.publish")((record: Record) =>
  Effect.tryPromise({
    try: async () => {
      if (!Number.isInteger(NodeFS.constants.O_NOFOLLOW))
        throw new Error("No no-follow sidecar capability");
      const directory = record.target.gitDirectory;
      if (directory === null) throw new Error("No observed Git admin directory");
      const destination = NodePath.join(directory, "workspace-provenance.json");
      const scratch = await NodeFSP.mkdtemp(NodePath.join(directory, ".workspace-provenance-"));
      let primaryFailure: unknown;
      try {
        const temporary = NodePath.join(scratch, "record.json");
        const output = await NodeFSP.open(temporary, "wx", 0o600);
        try {
          await output.writeFile(`${canonical(record)}\n`);
          await output.sync();
        } finally {
          await output.close();
        }
        try {
          await NodeFSP.link(temporary, destination);
        } catch (cause) {
          if (!(cause instanceof Error) || !("code" in cause) || cause.code !== "EEXIST")
            throw cause;
          const input = await NodeFSP.open(
            destination,
            NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
          );
          try {
            const stat = await input.stat();
            if (!stat.isFile() || stat.size > 65536) throw new Error("Invalid provenance sidecar");
            const bytes = new Uint8Array(65537);
            const read = await input.read(bytes, 0, bytes.length, 0);
            if (read.bytesRead !== stat.size || read.bytesRead > 65536)
              throw new Error("Changed provenance sidecar");
            const existing = await Schema.decodePromise(Schema.fromJsonString(ProvenanceRecord), {
              onExcessProperty: "error",
            })(new TextDecoder().decode(bytes.subarray(0, read.bytesRead)));
            if (canonical(existing) !== canonical(record))
              throw new Error("Conflicting provenance sidecar");
          } finally {
            await input.close();
          }
        }
      } catch (cause) {
        primaryFailure = cause;
        throw cause;
      } finally {
        try {
          await NodeFSP.rm(scratch, { recursive: true, force: true });
        } catch (cleanup) {
          throw new AggregateError(
            primaryFailure === undefined ? [cleanup] : [primaryFailure, cleanup],
            "Provenance scratch cleanup failed",
          );
        }
      }
    },
    catch: (cause) => new LifecycleError({ reason: "storage", cause }),
  }).pipe(Effect.uninterruptible),
);
export const sidecarLayer = Layer.succeed(ProvenanceSidecar, { publish });

const threadSchema = Schema.fromJsonString(
  Schema.Struct({
    createdBy: Schema.optional(Schema.String),
    lineage: Schema.optional(Schema.Struct({ relationshipToParent: Schema.NullOr(Schema.String) })),
    pullRequests: Schema.optional(
      Schema.Array(
        Schema.Struct({
          host: Schema.optional(Schema.String),
          repository: Schema.String,
          number: Schema.Number,
          linkedAt: Schema.String,
          source: Schema.String,
          snapshot: Schema.NullOr(
            Schema.Struct({
              state: Schema.String,
              mergedAt: Schema.optional(Schema.NullOr(Schema.String)),
            }),
          ),
        }),
      ),
    ),
    delegatedFrom: Schema.optional(Schema.Struct({ threadId: Schema.String })),
    id: Schema.String,
    projectId: Schema.String,
    updatedAt: Schema.String,
    worktreePath: Schema.NullOr(Schema.String),
    branch: Schema.NullOr(Schema.String),
  }),
);
const incarnation = (environment: string, source: Sources.CheckoutSource, recreation: string | null) =>
  `workspace:${digest({
    environment,
    project: source.target.projectId,
    path: source.target.checkoutKey,
    birth: source.target.directoryIdentity,
    common: source.target.gitCommonIdentity,
    admin: source.target.gitDirectoryIdentity,
    ...(recreation === null ? {} : { recreation }),
  })}`;
const error = (cause: unknown) =>
  Schema.is(LifecycleError)(cause) ? cause : new LifecycleError({ reason: "unavailable", cause });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const identity = yield* ServerEnvironmentIdentity;
  const sources = yield* Sources.CheckoutSourceObservation;
  const store = yield* Store.ProvenanceStore;
  const sidecar = yield* Effect.serviceOption(ProvenanceSidecar);
  const environment = yield* identity.getEnvironmentId;
  const capture = Effect.fn("Provenance.capture")(function* (input: Capture) {
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${input.threadId}`;
    if (!rows[0]) return yield* new LifecycleError({ reason: "identity" });
    // Decode metadata only. Conversation items and source files are never read.
    const thread = yield* Schema.decodeEffect(threadSchema)(rows[0].payload_json);
    if (thread.id !== input.threadId) return yield* new LifecycleError({ reason: "identity" });
    const projects = yield* sql<{
      workspace_root: string;
      updated_at: string;
    }>`SELECT workspace_root,updated_at FROM projection_projects WHERE project_id=${thread.projectId} AND deleted_at IS NULL`;
    if (!projects[0]) return yield* new LifecycleError({ reason: "identity" });
    const source = yield* sources.capture({
      projectId: thread.projectId,
      threadId: thread.id,
      projectRoot: projects[0].workspace_root,
      projectRevision: projects[0].updated_at,
      worktreePath: thread.worktreePath,
      threadBranch: thread.branch,
      threadRevision: thread.updatedAt,
    });
    if (input.reason === "backfill" && input.expected === undefined)
      return yield* new LifecycleError({ reason: "prior-lineage" });
    if (input.expected !== undefined && canonical(source) !== canonical(input.expected))
      return yield* new LifecycleError({ reason: "stale" });
    const target = source.target;
    if (
      target.gitCommonDirectory === null ||
      target.gitDirectory === null ||
      target.gitCommonIdentity === null ||
      target.gitDirectoryIdentity === null
    )
      return yield* new LifecycleError({ reason: "identity" });
    const tasks = yield* sql<{
      subagent_id: string;
    }>`SELECT subagent_id FROM orchestration_v2_projection_subagents WHERE child_thread_id=${input.threadId}`;
    const reason =
      input.reason === "launch" &&
      (tasks.length > 0 ||
        thread.lineage?.relationshipToParent === "subagent" ||
        thread.delegatedFrom !== undefined)
        ? "delegate"
        : input.reason;
    const recreated = yield* Effect.tryPromise({
      try: () => readBreadcrumb(target.gitDirectory!),
      catch: (cause) => cause,
    });
    const id = incarnation(environment, source, recreated?.breadcrumb ?? null);
    const predecessors = yield* store.lookup({ threadId: input.threadId });
    const physicalHistory = predecessors.findLast(
      (p) =>
        canonical(p.provenance.birth) === canonical(target.directoryIdentity) &&
        canonical(p.provenance.admin_id) === canonical(target.gitDirectoryIdentity) &&
        canonical(p.provenance.target.gitCommonIdentity) === canonical(target.gitCommonIdentity),
    )?.provenance;
    if (input.requireExistingPhysical === true && physicalHistory === undefined)
      return yield* new LifecycleError({ reason: "prior-lineage" });
    const relocatedFrom =
      physicalHistory?.target.checkoutKey !== target.checkoutKey ? physicalHistory : undefined;
    const predecessor =
      input.predecessor ??
      relocatedFrom?.incarnation_id ??
      (input.reason === "handoff" || input.reason === "recreate"
        ? (predecessors.filter((p) => p.provenance.incarnation_id !== id).at(-1)?.provenance
            .incarnation_id ?? null)
        : null);
    if (
      predecessor !== null &&
      !predecessors.some((p) => p.provenance.incarnation_id === predecessor)
    )
      return yield* new LifecycleError({ reason: "prior-lineage" });
    yield* sources.revalidate(source);
    const record = yield* store.capture(
      {
        schema: "workspace-provenance/v1",
        environment_id: environment,
        project_id: thread.projectId,
        incarnation_id: id,
        reason,
        created_at: DateTime.formatIso(yield* DateTime.now),
        created_by:
          input.reason === "launch" ? (thread.createdBy ?? input.createdBy) : input.createdBy,
        base_ref: input.baseRef ?? null,
        base_head: input.baseHead ?? (input.baseIsCurrentHead === true ? source.head : null),
        birth: target.directoryIdentity,
        admin_id: target.gitDirectoryIdentity,
        predecessor,
        target,
        head: source.head,
        authority_effect: "none",
      },
      input.threadId,
      input.taskId ?? tasks[0]?.subagent_id ?? null,
      input.runId,
    );
    for (const task of tasks.slice(1))
      yield* store.capture(
        { ...record, target, head: source.head },
        input.threadId,
        task.subagent_id,
      );
    for (const link of thread.pullRequests ?? []) {
      if (link.source === "stack-dismissed") continue;
      const reference = `${link.host ?? "github.com"}/${link.repository}#${link.number}`;
      yield* store.fact({
        id: `pr-link:${digest({ incarnation: id, thread: input.threadId, reference, at: link.linkedAt })}`,
        incarnation_id: id,
        thread_id: input.threadId,
        kind: "pr_link",
        recorded_at: link.linkedAt,
        reference,
      });
      if (link.snapshot?.state === "merged" && link.snapshot.mergedAt != null)
        yield* store.fact({
          id: `pr-merge:${digest({ incarnation: id, thread: input.threadId, reference, at: link.snapshot.mergedAt })}`,
          incarnation_id: id,
          thread_id: input.threadId,
          kind: "pr_merge",
          recorded_at: link.snapshot.mergedAt,
          reference,
        });
    }
    if (input.reason === "recreate")
      yield* store.fact({
        id: `recreation:${record.incarnation_id}`,
        incarnation_id: record.incarnation_id,
        thread_id: input.threadId,
        kind: "recreation",
        recorded_at: record.created_at,
        reference: record.predecessor ?? record.incarnation_id,
      });
    // Relocation references share the original admin directory and its immutable birth sidecar.
    if (Option.isSome(sidecar) && !(record.reason === "reference" && record.predecessor !== null)) {
      yield* sources.revalidate(source);
      yield* sidecar.value.publish(record);
      yield* sources.revalidate(source);
    }
    return record;
  }, Effect.mapError(error));
  return Provenance.of({
    capture,
    lookup: store.lookup,
    production: Effect.fn("Provenance.production")(function* (
      threadId: string,
      reference: string,
      worktreePath: string,
    ) {
      const record = yield* capture({
        threadId,
        reason: "reference",
        createdBy: "jones-producer",
        requireExistingPhysical: true,
      });
      if (record.target.cwd !== worktreePath)
        return yield* new LifecycleError({ reason: "identity" });
      yield* store.fact({
        id: `production:${digest({ incarnation: record.incarnation_id, reference })}`,
        incarnation_id: record.incarnation_id,
        thread_id: threadId,
        kind: "production",
        recorded_at: yield* DateTime.now.pipe(Effect.map(DateTime.formatIso)),
        reference,
      });
    }, Effect.mapError(error)),
    record: Effect.fn("Provenance.record")(function* (source, fact) {
      const recreated = source.target.gitDirectory === null
        ? null
        : yield* Effect.tryPromise({
            try: () => readBreadcrumb(source.target.gitDirectory!),
            catch: (cause) => cause,
          });
      if (
        incarnation(environment, source, recreated?.breadcrumb ?? null) !== fact.incarnation_id ||
        source.target.threadId !== fact.thread_id
      )
        return yield* new LifecycleError({ reason: "identity" });
      yield* sources.revalidate(source);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            payload_json: string;
          }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${fact.thread_id}`;
          if (!rows[0]) return yield* new LifecycleError({ reason: "identity" });
          const thread = yield* Schema.decodeEffect(threadSchema)(rows[0].payload_json);
          const target = source.target;
          const projects = yield* sql<{
            workspace_root: string;
            updated_at: string;
          }>`SELECT workspace_root,updated_at FROM projection_projects WHERE project_id=${target.projectId} AND deleted_at IS NULL`;
          if (
            thread.projectId !== target.projectId ||
            thread.updatedAt !== target.threadRevision ||
            thread.worktreePath !== target.worktreePath ||
            thread.branch !== target.threadBranch ||
            projects[0]?.workspace_root !== target.projectRoot ||
            projects[0]?.updated_at !== target.projectRevision
          )
            return yield* new LifecycleError({ reason: "stale" });
          yield* store.fact(fact);
        }),
      );
    }, Effect.mapError(error)),
  });
});
const observations = Sources.layer.pipe(
  Layer.provide(Targets.layer),
  Layer.provideMerge(TargetNode.layer),
);
export const layer = Layer.effect(Provenance, make);
export const layerLive = layer.pipe(
  Layer.provide(observations),
  Layer.provideMerge(Store.provenanceLayer),
  Layer.provide(sidecarLayer),
);

// The current capture adapter does Git I/O, multiple SQL writes and an uninterruptible
// sidecar publication. It has no cancellable 100ms persistence boundary. Do not
// start that work on an ordinary operation's path or leave it running in a race.
// Explicit lifecycle capture remains available to callers that own that work.
export const optionalCapture = Effect.map(
  Effect.serviceOption(Provenance),
  (service) =>
    (_input: Capture): Effect.Effect<void> =>
      Option.isSome(service)
        ? Effect.logWarning("Workspace provenance capture skipped", { reason: "unbounded-sink" })
        : Effect.void,
);
export const optionalLookup = Effect.map(
  Effect.serviceOption(Provenance),
  (service) =>
    (input: {
      readonly path?: string;
      readonly threadId?: string;
    }): Effect.Effect<readonly WorktreeProvenanceEntry[] | undefined> =>
      Option.isSome(service)
        ? service.value.lookup(input).pipe(
            Effect.map((entries) => entries.map(toLookup)),
            Effect.catch(() =>
              Effect.logWarning("Workspace provenance lookup unavailable").pipe(
                Effect.andThen(Effect.undefined),
              ),
            ),
          )
        : Effect.undefined,
);
const toLookup = ({
  provenance: p,
  threads,
  tasks,
  facts,
}: ProvenanceEntry): WorktreeProvenanceEntry => ({
  schema: p.schema,
  incarnationId: p.incarnation_id,
  environmentId: p.environment_id,
  projectId: p.project_id,
  path: p.target.checkoutKey,
  branch: p.target.branch,
  head: p.head,
  reason: p.reason,
  createdAt: p.created_at,
  createdBy: p.created_by,
  baseRef: p.base_ref,
  baseHead: p.base_head,
  birth: p.birth,
  adminId: p.admin_id,
  predecessor: p.predecessor,
  threads,
  tasks,
  facts: facts.map((f) => ({
    id: f.id,
    kind: f.kind,
    recordedAt: f.recorded_at,
    reference: f.reference,
  })),
  authorityEffect: "none",
});

export const optionalCommittedCapture = Effect.gen(function* () {
  const capture = yield* optionalCapture;
  return (events: readonly RecordedEvent[]): Effect.Effect<void> =>
    Effect.forEach(
      events,
      (event) => {
        if (
          event.type === "context-transfer.created" &&
          (event.payload.type === "fork" || event.payload.type === "merge_back")
        )
          return Effect.forEach(
            [event.payload.sourceThreadId, event.payload.targetThreadId],
            (threadId) => capture({ threadId, reason: "reference", createdBy: "system" }),
            { concurrency: 1, discard: true },
          );
        if (event.type === "thread.metadata-updated" || event.type === "thread.unsettled")
          return capture({
            threadId: event.threadId,
            reason: "reference",
            createdBy: "system",
            requireExistingPhysical: true,
          });
        return Effect.void;
      },
      { concurrency: 1, discard: true },
    );
});
