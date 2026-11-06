import { RetirementError } from "../workspaceRetirement/Receipts.ts";
import type { RecordedEvent } from "../../orchestration-v2/RecordedTypes.ts";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical, digest } from "../ordinaryCheckout/CheckoutProtocol.ts";
import {
  Binding,
  LifecycleError,
  Provenance,
  ProvenanceEntry,
  ProvenanceFact,
} from "./Protocol.ts";

const Transition = Schema.Struct({
  id: Schema.NonEmptyString,
  source: Schema.NonEmptyString,
  binding: Binding,
  expectedGeneration: Schema.NullOr(Schema.NonEmptyString),
  generation: Schema.NullOr(Schema.NonEmptyString),
  state: Schema.Literals(["prepared", "invoking", "confirmed"]),
});
export type Transition = typeof Transition.Type;
const isLifecycleError = Schema.is(LifecycleError);
const decodeBinding = Schema.decodeEffect(Schema.fromJsonString(Binding));
const decodeTransition = Schema.decodeUnknownEffect(Transition);
export class TransitionStore extends Context.Service<
  TransitionStore,
  {
    readonly outsideTransaction: Effect.Effect<void, LifecycleError>;
    readonly read: (id: string) => Effect.Effect<Transition | null, LifecycleError>;
    readonly current: (worktree: string) => Effect.Effect<Transition | null, LifecycleError>;
    readonly prior: (worktree: string) => Effect.Effect<Transition | null, LifecycleError>;
    readonly invalidateRecreated: (worktree: string) => Effect.Effect<void, LifecycleError>;
    readonly prepare: (transition: Transition) => Effect.Effect<Transition, LifecycleError>;
    readonly begin: (transition: Transition) => Effect.Effect<void, LifecycleError>;
    readonly confirm: (
      transition: Transition,
      generation: string,
    ) => Effect.Effect<Transition, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/TransitionStore") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const storageError = (cause: unknown) =>
    isLifecycleError(cause) ? cause : new LifecycleError({ reason: "storage", cause });
  const assertThread = Effect.fn("WorkspaceLifecycleTransitionStore.assertThread")(function* (
    transition: Transition,
  ) {
    const retiring =
      yield* sql`SELECT 1 FROM jones_workspace_retirement_state WHERE worktree=${transition.binding.worktree} AND state='retiring'`;
    if (retiring.length)
      return yield* new RetirementError({
        reason: "retiring",
        target: transition.binding.worktree,
      });
    const rows =
      yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id=${transition.binding.thread_id}`;
    if (rows.length !== 1) return yield* new LifecycleError({ reason: "identity" });
  });
  const read = Effect.fn("WorkspaceLifecycleTransitionStore.read")(function* (id: string) {
    const rows = yield* sql<{
      transition_id: string;
      worktree: string;
      source_json: string;
      binding_json: string;
      expected_generation: string | null;
      confirmed_generation: string | null;
      state: "prepared" | "invoking" | "confirmed";
    }>`SELECT * FROM jones_workspace_lifecycle_transitions WHERE transition_id=${id}`;
    const row = rows[0];
    if (!row) return null;
    const binding = yield* decodeBinding(row.binding_json);
    if (binding.worktree !== row.worktree) return yield* new LifecycleError({ reason: "identity" });
    return yield* decodeTransition({
      id: row.transition_id,
      source: row.source_json,
      binding,
      expectedGeneration: row.expected_generation,
      generation: row.confirmed_generation,
      state: row.state,
    });
  }, Effect.mapError(storageError));
  const original = Effect.fn("WorkspaceLifecycleTransitionStore.original")(function* (
    transition: Transition,
  ) {
    const row = yield* read(transition.id);
    if (
      !row ||
      row.source !== transition.source ||
      canonical(row.binding) !== canonical(transition.binding) ||
      row.expectedGeneration !== transition.expectedGeneration
    )
      return yield* new LifecycleError({ reason: "identity" });
    return row;
  });
  const prior = Effect.fn("WorkspaceLifecycleTransitionStore.prior")(function* (worktree: string) {
    const rows = yield* sql<{
      transition_id: string;
    }>`SELECT transition_id FROM jones_workspace_lifecycle_bindings WHERE worktree=${worktree}`;
    return rows[0] ? yield* read(rows[0].transition_id) : null;
  }, Effect.mapError(storageError));
  return TransitionStore.of({
    outsideTransaction: Effect.gen(function* () {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new LifecycleError({ reason: "transaction" });
    }),
    read,
    prior,
    invalidateRecreated: Effect.fn("WorkspaceLifecycleTransitionStore.invalidateRecreated")(
      function* (worktree) {
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const retiring =
                yield* sql`SELECT 1 FROM jones_workspace_retirement_state WHERE worktree=${worktree} AND state='retiring'`;
              if (retiring.length) return yield* new LifecycleError({ reason: "conflict" });
              const pending =
                yield* sql`SELECT 1 FROM jones_workspace_lifecycle_transitions WHERE worktree=${worktree} AND state!='confirmed' LIMIT 1`;
              if (pending.length) return yield* new LifecycleError({ reason: "unknown" });
              // Historical transitions survive; validation can no longer accept their source generation as current.
              yield* sql`DELETE FROM jones_workspace_lifecycle_bindings WHERE worktree=${worktree}`;
            }),
          )
          .pipe(Effect.mapError(storageError));
      },
    ),
    current: Effect.fn("WorkspaceLifecycleTransitionStore.current")(function* (worktree) {
      const pending =
        yield* sql`SELECT transition_id FROM jones_workspace_lifecycle_transitions WHERE worktree=${worktree} AND state!='confirmed' LIMIT 1`;
      if (pending.length > 0) return yield* new LifecycleError({ reason: "unknown" });
      return yield* prior(worktree);
    }, Effect.mapError(storageError)),
    prepare: Effect.fn("WorkspaceLifecycleTransitionStore.prepare")(function* (transition) {
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* assertThread(transition);
            const prior = yield* read(transition.id);
            if (prior !== null) return yield* original(transition);
            if (transition.state !== "prepared" || transition.generation !== null)
              return yield* new LifecycleError({ reason: "identity" });
            const pending =
              yield* sql`SELECT transition_id FROM jones_workspace_lifecycle_transitions WHERE worktree=${transition.binding.worktree} AND state!='confirmed' LIMIT 1`;
            if (pending.length > 0) return yield* new LifecycleError({ reason: "unknown" });
            yield* sql`INSERT INTO jones_workspace_lifecycle_transitions VALUES (
          ${transition.id},${transition.binding.worktree},${transition.source},${canonical(transition.binding)},
          ${transition.expectedGeneration},NULL,'prepared')`;
            return yield* original(transition);
          }),
        )
        .pipe(Effect.mapError(storageError));
    }),
    begin: Effect.fn("WorkspaceLifecycleTransitionStore.begin")(function* (transition) {
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* assertThread(transition);
            if ((yield* original(transition)).state !== "prepared")
              return yield* new LifecycleError({ reason: "unknown" });
            yield* sql`UPDATE jones_workspace_lifecycle_transitions SET state='invoking' WHERE transition_id=${transition.id} AND state='prepared'`;
          }),
        )
        .pipe(Effect.mapError(storageError));
    }),
    confirm: Effect.fn("WorkspaceLifecycleTransitionStore.confirm")(
      function* (transition, generation) {
        return yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* assertThread(transition);
              const prior = yield* original(transition);
              if (
                prior.state === "prepared" ||
                (prior.generation !== null && prior.generation !== generation)
              )
                return yield* new LifecycleError({ reason: "conflict" });
              if (prior.state === "confirmed") return prior;
              yield* sql`UPDATE jones_workspace_lifecycle_transitions SET state='confirmed',confirmed_generation=${generation} WHERE transition_id=${transition.id}`;
              yield* sql`INSERT INTO jones_workspace_lifecycle_bindings (worktree,transition_id,thread_id) VALUES (${transition.binding.worktree},${transition.id},${transition.binding.thread_id})
          ON CONFLICT(worktree) DO UPDATE SET transition_id=excluded.transition_id,thread_id=excluded.thread_id`;
              return { ...prior, state: "confirmed" as const, generation };
            }),
          )
          .pipe(Effect.mapError(storageError));
      },
    ),
  });
});
export const layer = Layer.effect(TransitionStore, make);

// Facts are separate from assessments and transition generations; this store grants no lifecycle authority.
export class ProvenanceStore extends Context.Service<
  ProvenanceStore,
  {
    readonly capture: (
      record: Provenance,
      threadId: string,
      taskId: string | null,
      runId?: string,
    ) => Effect.Effect<Provenance, LifecycleError>;
    readonly events: (events: readonly RecordedEvent[]) => Effect.Effect<void, LifecycleError>;
    readonly fact: (fact: ProvenanceFact) => Effect.Effect<void, LifecycleError>;
    readonly lookup: (input: {
      readonly path?: string;
      readonly threadId?: string;
    }) => Effect.Effect<readonly ProvenanceEntry[], LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/TransitionStore/ProvenanceStore") {}

const makeProvenanceStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const storage = (cause: unknown) =>
    Schema.is(LifecycleError)(cause) ? cause : new LifecycleError({ reason: "storage", cause });
  const decode = Schema.decodeEffect(Schema.fromJsonString(Provenance));
  const lookup = Effect.fn("ProvenanceStore.lookup")(function* (input: {
    readonly path?: string;
    readonly threadId?: string;
  }) {
    if (input.path === undefined && input.threadId === undefined)
      return yield* new LifecycleError({ reason: "identity" });
    const rows = yield* sql<{
      provenance_json: string;
    }>`SELECT DISTINCT CASE WHEN length(CAST(p.provenance_json AS BLOB)) <= 65536
      THEN p.provenance_json ELSE '' END AS provenance_json
      FROM jones_workspace_provenance p
      LEFT JOIN jones_workspace_provenance_threads t ON t.incarnation_id=p.incarnation_id
      WHERE (${input.path ?? null} IS NULL OR p.worktree=${input.path ?? null})
      AND (${input.threadId ?? null} IS NULL OR t.thread_id=${input.threadId ?? null})
      ORDER BY p.rowid LIMIT 17`;
    if (rows.length > 16) return yield* new LifecycleError({ reason: "unavailable" });
    let remainingBytes = 65536;
    return yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        remainingBytes -= new TextEncoder().encode(row.provenance_json).byteLength;
        if (remainingBytes < 0) return yield* new LifecycleError({ reason: "unavailable" });
        const provenance = yield* decode(row.provenance_json);
        const relations = yield* sql<{
          thread_id: string;
          task_id: string;
        }>`SELECT substr(thread_id,1,65537) AS thread_id,substr(task_id,1,65537) AS task_id FROM jones_workspace_provenance_threads WHERE incarnation_id=${provenance.incarnation_id} ORDER BY thread_id,task_id LIMIT 65`;
        const facts = yield* sql<{
          fact_json: string;
        }>`SELECT CASE WHEN length(CAST(fact_json AS BLOB)) <= 65536 THEN fact_json ELSE '' END AS fact_json FROM jones_workspace_provenance_facts WHERE incarnation_id=${provenance.incarnation_id} ORDER BY event_id LIMIT 65`;
        if (relations.length > 64 || facts.length > 64)
          return yield* new LifecycleError({ reason: "unavailable" });
        for (const relation of relations)
          remainingBytes -= new TextEncoder().encode(
            relation.thread_id + relation.task_id,
          ).byteLength;
        for (const fact of facts)
          remainingBytes -= new TextEncoder().encode(fact.fact_json).byteLength;
        if (remainingBytes < 0) return yield* new LifecycleError({ reason: "unavailable" });
        return yield* Schema.decodeEffect(ProvenanceEntry)({
          provenance,
          threads: [...new Set(relations.map((r) => r.thread_id))],
          tasks: [...new Set(relations.map((r) => r.task_id).filter(Boolean))],
          facts: yield* Effect.forEach(facts, (r) =>
            Schema.decodeEffect(Schema.fromJsonString(ProvenanceFact))(r.fact_json),
          ),
        });
      }),
    );
  }, Effect.mapError(storage));
  const fact = Effect.fn("ProvenanceStore.fact")(function* (input: ProvenanceFact) {
    const fact = yield* Schema.decodeEffect(ProvenanceFact)(input);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const owners =
          yield* sql`SELECT 1 FROM jones_workspace_provenance_threads WHERE incarnation_id=${fact.incarnation_id} AND thread_id=${fact.thread_id} LIMIT 1`;
        if (!owners[0]) return yield* new LifecycleError({ reason: "identity" });
        const prior = yield* sql<{
          fact_json: string;
        }>`SELECT fact_json FROM jones_workspace_provenance_facts WHERE event_id=${fact.id}`;
        if (prior[0]) {
          if (prior[0].fact_json !== canonical(fact))
            return yield* new LifecycleError({ reason: "conflict" });
          return;
        }
        if (fact.kind === "pr_link" || fact.kind === "pr_merge") {
          const existing = yield* sql`SELECT 1 FROM jones_workspace_provenance_facts
            WHERE incarnation_id=${fact.incarnation_id} AND json_extract(fact_json,'$.kind')=${fact.kind}
            AND json_extract(fact_json,'$.thread_id')=${fact.thread_id}
            AND json_extract(fact_json,'$.reference')=${fact.reference} LIMIT 1`;
          if (existing[0]) return;
        }
        if (fact.kind === "first_meaningful_write") {
          const first =
            yield* sql`SELECT 1 FROM jones_workspace_provenance_facts WHERE incarnation_id=${fact.incarnation_id} AND json_extract(fact_json,'$.kind')='first_meaningful_write' LIMIT 1`;
          if (first[0]) return;
        }
        yield* sql`INSERT INTO jones_workspace_provenance_facts VALUES (${fact.id},${fact.incarnation_id},${canonical(fact)})`;
      }),
    );
  }, Effect.mapError(storage));
  return ProvenanceStore.of({
    lookup,
    events: Effect.fn("ProvenanceStore.events")(function* (events) {
      for (const event of events) {
        const facts: { kind: ProvenanceFact["kind"]; reference: string }[] = [];
        let runId: string | undefined;
        if (
          event.type === "turn-item.updated" &&
          event.payload.type === "file_change" &&
          event.payload.status === "completed" &&
          event.payload.toolNonExecutionKind === undefined
        ) {
          runId = event.payload.runId ?? undefined;
          if (runId === undefined) continue;
          facts.push({ kind: "first_meaningful_write", reference: event.id });
        }
        if (event.type === "thread.settled" || event.type === "thread.unsettled")
          facts.push({
            kind: "settlement_outcome",
            reference: `${event.type === "thread.settled" ? "settled" : "unsettled"}:${event.id}`,
          });
        const links =
          event.type === "thread.pull-request-link-updated"
            ? [event.payload]
            : event.type === "thread.pull-request-synced"
              ? (event.payload.pullRequests ?? [])
              : [];
        for (const link of links) {
          if (link.source === "stack-dismissed") continue;
          const reference = `${link.host ?? "github.com"}/${link.repository}#${link.number}`;
          facts.push({ kind: "pr_link", reference });
          if (link.snapshot?.state === "merged") facts.push({ kind: "pr_merge", reference });
        }
        if (facts.length === 0) continue;
        const owners =
          runId === undefined
            ? yield* sql<{
                incarnation_id: string;
              }>`SELECT DISTINCT incarnation_id FROM jones_workspace_provenance_threads WHERE thread_id=${event.threadId}`
            : yield* sql<{
                incarnation_id: string;
              }>`SELECT incarnation_id FROM jones_workspace_provenance_runs WHERE run_id=${runId} AND thread_id=${event.threadId}`;
        for (const owner of owners)
          for (const item of facts)
            yield* fact({
              id: `provenance:${digest({ event: event.id, incarnation: owner.incarnation_id, ...item })}`,
              incarnation_id: owner.incarnation_id,
              thread_id: event.threadId,
              ...item,
              recorded_at: DateTime.formatIso(event.occurredAt),
            });
      }
    }, Effect.mapError(storage)),
    capture: Effect.fn("ProvenanceStore.capture")(function* (input, threadId, taskId, runId) {
      const record = yield* Schema.decodeEffect(Provenance)(input);
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* sql<{
            payload_json: string;
          }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${threadId}`;
          if (!current[0]) return yield* new LifecycleError({ reason: "identity" });
          const bound = yield* Schema.decodeEffect(
            Schema.fromJsonString(
              Schema.Struct({
                projectId: Schema.String,
                updatedAt: Schema.String,
                worktreePath: Schema.NullOr(Schema.String),
                branch: Schema.NullOr(Schema.String),
              }),
            ),
          )(current[0].payload_json);
          if (
            bound.projectId !== record.project_id ||
            bound.updatedAt !== record.target.threadRevision ||
            bound.worktreePath !== record.target.worktreePath ||
            bound.branch !== record.target.threadBranch
          )
            return yield* new LifecycleError({ reason: "stale" });
          const rows = yield* sql<{
            provenance_json: string;
          }>`SELECT provenance_json FROM jones_workspace_provenance WHERE incarnation_id=${record.incarnation_id}`;
          const original = rows[0] ? yield* decode(rows[0].provenance_json) : record;
          if (
            original.environment_id !== record.environment_id ||
            original.project_id !== record.project_id ||
            original.target.checkoutKey !== record.target.checkoutKey ||
            canonical(original.birth) !== canonical(record.birth) ||
            canonical(original.admin_id) !== canonical(record.admin_id) ||
            canonical(original.target.gitCommonIdentity) !==
              canonical(record.target.gitCommonIdentity)
          )
            return yield* new LifecycleError({ reason: "identity" });
          if (!rows[0])
            yield* sql`INSERT INTO jones_workspace_provenance VALUES (${record.incarnation_id},${record.target.checkoutKey},${record.project_id},${canonical(record)})`;
          yield* sql`INSERT INTO jones_workspace_provenance_threads VALUES (${record.incarnation_id},${threadId},${taskId ?? ""}) ON CONFLICT DO NOTHING`;
          if (runId !== undefined) {
            const bound = yield* sql<{
              incarnation_id: string;
              thread_id: string;
            }>`SELECT incarnation_id,thread_id FROM jones_workspace_provenance_runs WHERE run_id=${runId}`;
            if (
              bound[0] &&
              (bound[0].incarnation_id !== record.incarnation_id || bound[0].thread_id !== threadId)
            )
              return yield* new LifecycleError({ reason: "stale" });
            yield* sql`INSERT INTO jones_workspace_provenance_runs VALUES (${runId},${record.incarnation_id},${threadId}) ON CONFLICT DO NOTHING`;
          }
          return original;
        }),
      );
    }, Effect.mapError(storage)),
    fact,
  });
});
export const provenanceLayer = Layer.effect(ProvenanceStore, makeProvenanceStore);
