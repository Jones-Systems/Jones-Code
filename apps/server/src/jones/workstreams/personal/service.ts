import { pinOrderKeyBetween } from "@t3tools/shared/jones/pinOrder";
import * as Digest from "effect/crypto/Digest";
import * as Hex from "effect/encoding/Hex";
import * as DateTime from "effect/DateTime";
import type { SqlError } from "effect/sql/SqlError";
import { ThreadId } from "@t3tools/contracts";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../../config.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import * as AutoRouteStore from "../autoRoute/AutoRouteStore.ts";

export class PersonalWorkstreams extends Context.Service<
  PersonalWorkstreams,
  {
    readonly snapshot: () => Effect.Effect<
      P.PersonalWorkstreamsSnapshot,
      P.WorkstreamsPersonalError
    >;
    readonly members: (
      id: string,
    ) => Effect.Effect<P.PersonalWorkstreamMembers, P.WorkstreamsPersonalError>;
    readonly changes: Stream.Stream<P.PersonalWorkstreamsSnapshot, P.WorkstreamsPersonalError>;
    readonly create: (
      input: Omit<P.NewPersonalWorkstreamInput, "orderKey" | "now" | "origin">,
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly put: (
      workstream: P.PersonalWorkstream,
      expected?: P.PersonalWorkstream,
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly assign: (
      input: P.AssignPersonalWorkstreamInput,
      options?: { readonly supersedeInitialAutoAssignment: boolean },
    ) => Effect.Effect<P.AssignResult, P.WorkstreamsPersonalError>;
    readonly assignAutomatically: (
      input: AutoRouteStore.DecisionKey & {
        readonly workstreamId: string;
        readonly workstreamName: string;
        readonly confidence: "high";
        readonly basis: "name" | "similar_threads" | "both";
      },
    ) => Effect.Effect<void, P.WorkstreamsPersonalError>;
    readonly readThread: (id: ThreadId) => Effect.Effect<
      {
        assignment: P.PersonalWorkstreamAssignment;
        workstream: P.PersonalWorkstream | null;
        /** The thread's latest automatic-routing decision; absent without the Jones table. */
        autoRoute?: AutoRouteStore.AutoRouteDecisionView | null;
      },
      P.WorkstreamsPersonalError
    >;
    readonly readDefinitions: (
      ids?: readonly string[],
    ) => Effect.Effect<readonly P.PersonalWorkstream[], P.WorkstreamsPersonalError>;
    readonly counts: () => Effect.Effect<
      { observedAt: string; sequence: number; total: number; active: number },
      P.WorkstreamsPersonalError
    >;
  }
>()("t3/jones/workstreams/personal/service/PersonalWorkstreams") {}

const definitionColumns = `id, name, lifecycle, color, name_icon_project_key AS nameIconProjectKey, name_icon_updated_at AS nameIconUpdatedAt, order_key AS orderKey, created_at AS createdAt, updated_at AS updatedAt, deleted_at AS deletedAt, rev, lifecycle_revision AS lifecycleRevision, origin`;
type DefinitionRow = P.PersonalWorkstream & {
  nameIconProjectKey: string | null;
  nameIconUpdatedAt: string | null;
};
const normalizeDefinition = ({
  lifecycleRevision,
  nameIconProjectKey,
  nameIconUpdatedAt,
  ...definition
}: DefinitionRow): P.PersonalWorkstream => ({
  ...definition,
  ...(lifecycleRevision ? { lifecycleRevision } : {}),
  ...(nameIconUpdatedAt
    ? { nameIcon: { projectKey: nameIconProjectKey, updatedAt: nameIconUpdatedAt } }
    : {}),
});
const assignmentColumns = `a.thread_id AS threadId, a.workstream_id AS workstreamId, a.revision, a.updated_at AS updatedAt`;
const ImportFile = Schema.Struct({
  schema: Schema.Literal("jones.workstreams-personal-import/v1"),
  importId: Schema.String,
  targetEnvironmentId: Schema.String,
  workstreams: Schema.Array(P.PersonalWorkstream),
  assignments: Schema.Array(
    Schema.Struct({ threadId: ThreadId, workstreamId: P.PersonalWorkstreamId }),
  ),
});
const isPersonalError = Schema.is(P.WorkstreamsPersonalError);
const decodeDefinition = Schema.decodeEffect(P.PersonalWorkstream);
const decodeAssignment = Schema.decodeEffect(P.AssignPersonalWorkstreamInput);
const decodeImport = Schema.decodeEffect(Schema.fromJsonString(ImportFile));
const storage = (error: unknown) =>
  isPersonalError(error) ? error : new P.WorkstreamsPersonalError({ code: "storage_unavailable" });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const digester = yield* Digest.Digest;
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const fs = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const environmentId = yield* (yield* ServerEnvironment.ServerEnvironmentIdentity)
    .getEnvironmentId;
  const invalidations = yield* PubSub.sliding<void>(1);
  const autoRoute = yield* AutoRouteStore.make;
  let importIssue: P.PersonalWorkstreamsImportIssue | null = null;
  const sequence = Effect.fn("PersonalWorkstreams.sequence")(function* () {
    const rows = yield* sql<{
      value: string;
    }>`SELECT value FROM jones_personal_workstream_meta WHERE key='sequence'`;
    return Number(rows[0]?.value ?? 0);
  });
  const bump = Effect.fn("PersonalWorkstreams.bump")(function* () {
    yield* sql`INSERT INTO jones_personal_workstream_meta(key,value) VALUES('sequence','1') ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)`;
  });
  const definitions = () =>
    sql
      .unsafe<DefinitionRow>(`SELECT ${definitionColumns} FROM jones_personal_workstreams`)
      .pipe(Effect.map((rows) => rows.map(normalizeDefinition)));
  const definition = Effect.fn("PersonalWorkstreams.definition")(function* (id: string) {
    const rows = yield* sql.unsafe<DefinitionRow>(
      `SELECT ${definitionColumns} FROM jones_personal_workstreams WHERE id=?`,
      [id],
    );
    return rows[0] ? normalizeDefinition(rows[0]) : null;
  });
  const exists = (id: ThreadId) =>
    sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id=${id} AND deleted_at IS NULL`.pipe(
      Effect.map((rows) => rows.length > 0),
    );
  const assignment = Effect.fn("PersonalWorkstreams.assignment")(function* (id: ThreadId) {
    const rows = yield* sql.unsafe<P.PersonalWorkstreamAssignment>(
      `SELECT ${assignmentColumns} FROM jones_personal_workstream_assignments a WHERE a.thread_id=?`,
      [id],
    );
    return rows[0] ?? { threadId: id, workstreamId: null, revision: 0, updatedAt: null };
  });
  const persistAssignment = Effect.fn("PersonalWorkstreams.persistAssignment")(function* (
    threadId: ThreadId,
    workstreamId: string | null,
    revision: number,
  ) {
    const next = { threadId, workstreamId, revision, updatedAt: yield* now };
    yield* sql`INSERT INTO jones_personal_workstream_assignments ${sql.insert({ thread_id: threadId, workstream_id: workstreamId, revision, updated_at: next.updatedAt })} ON CONFLICT(thread_id) DO UPDATE SET workstream_id=excluded.workstream_id,revision=excluded.revision,updated_at=excluded.updated_at`;
    yield* bump();
    return next;
  });
  const store = Effect.fn("PersonalWorkstreams.store")(function* (w: P.PersonalWorkstream) {
    yield* sql`INSERT INTO jones_personal_workstreams ${sql.insert({ id: w.id, name: w.name, lifecycle: w.lifecycle, color: w.color, name_icon_project_key: w.nameIcon?.projectKey ?? null, name_icon_updated_at: w.nameIcon?.updatedAt ?? null, order_key: w.orderKey, created_at: w.createdAt, updated_at: w.updatedAt, deleted_at: w.deletedAt, rev: w.rev, lifecycle_revision: w.lifecycleRevision ?? 0, origin: w.origin })} ON CONFLICT(id) DO UPDATE SET name=excluded.name,lifecycle=excluded.lifecycle,color=excluded.color,name_icon_project_key=excluded.name_icon_project_key,name_icon_updated_at=excluded.name_icon_updated_at,order_key=excluded.order_key,created_at=excluded.created_at,updated_at=excluded.updated_at,deleted_at=excluded.deleted_at,rev=excluded.rev,lifecycle_revision=excluded.lifecycle_revision,origin=excluded.origin`;
  });
  const unsettledMember = (id: string) =>
    sql`SELECT 1
      FROM jones_personal_workstream_assignments a
      JOIN orchestration_v2_projection_threads t ON t.thread_id = a.thread_id
      WHERE a.workstream_id = ${id} AND t.deleted_at IS NULL
      AND json_extract(t.payload_json, '$.settledOverride') IS NOT 'settled'
      LIMIT 1`;
  const members = Effect.fn("PersonalWorkstreams.members")((id: string) =>
    sql<{ threadId: ThreadId; title: string; settled: number }>`SELECT
        a.thread_id AS threadId,
        t.title,
        json_extract(t.payload_json, '$.settledOverride') IS 'settled' AS settled
      FROM jones_personal_workstream_assignments a
      JOIN orchestration_v2_projection_threads t ON t.thread_id = a.thread_id
      WHERE a.workstream_id = ${id} AND t.deleted_at IS NULL
      ORDER BY a.updated_at DESC, a.thread_id`.pipe(
      Effect.map((rows) => rows.map((row) => ({ ...row, settled: row.settled === 1 }))),
      Effect.mapError(storage),
    ),
  );
  const merge = Effect.fn("PersonalWorkstreams.merge")(function* (
    candidate: P.PersonalWorkstream,
    expected?: P.PersonalWorkstream,
  ): Effect.fn.Return<P.PutResult, SqlError | P.WorkstreamsPersonalError> {
    const old = yield* definition(candidate.id);
    const w = old ? P.preserveWorkstreamNameIcon(candidate, old) : candidate;
    const comparison = old === null ? 1 : P.comparePersonalWorkstreams(w, old);
    if (comparison <= 0)
      return {
        outcome: comparison === 0 ? "already_satisfied" : "superseded",
        workstream: old!,
        sequence: yield* sequence(),
      };
    if (
      w.deletedAt === null &&
      P.isClosedPersonalWorkstream(w.lifecycle) &&
      old?.lifecycle !== w.lifecycle &&
      (yield* unsettledMember(w.id)).length > 0
    )
      return yield* new P.WorkstreamsPersonalError({ code: "unsettled_threads" });
    if (
      old?.deletedAt === null &&
      w.deletedAt === null &&
      P.isClosedPersonalWorkstream(old.lifecycle) !== P.isClosedPersonalWorkstream(w.lifecycle) &&
      (w.lifecycleRevision ?? 0) <= (old.lifecycleRevision ?? 0)
    )
      return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
    if (
      expected &&
      (!old ||
        P.comparePersonalWorkstreams(
          old,
          expected.nameIcon === undefined ? P.preserveWorkstreamNameIcon(expected, old) : expected,
        ) !== 0)
    )
      return yield* new P.WorkstreamsPersonalError({ code: "precondition_failed" });
    yield* store(w);
    if (w.deletedAt !== null)
      yield* sql`UPDATE jones_personal_workstream_assignments SET workstream_id=NULL, revision=revision+1, updated_at=${w.updatedAt} WHERE workstream_id=${w.id}`;
    yield* bump();
    return { outcome: "applied", workstream: w, sequence: yield* sequence() };
  });
  // The mask spans commit and publication: request cancellation cannot hide a durable write.
  const write = <A, E>(body: Effect.Effect<A, E>) =>
    Effect.uninterruptible(
      sql
        .withTransaction(
          Effect.gen(function* () {
            const before = yield* sequence();
            const result = yield* body;
            return { result, changed: (yield* sequence()) !== before };
          }),
        )
        .pipe(
          Effect.flatMap(({ result, changed }) =>
            changed
              ? PubSub.publish(invalidations, undefined).pipe(Effect.as(result))
              : Effect.succeed(result),
          ),
          Effect.mapError(storage),
        ),
    );
  const snapshot = Effect.fn("PersonalWorkstreams.snapshot")(() =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const autoAssignments = yield* autoRoute.currentAutoAssignments();
          return {
            environmentId,
            sequence: yield* sequence(),
            workstreams: yield* definitions(),
            assignments: yield* sql.unsafe<P.PersonalWorkstreamAssignment>(
              `SELECT ${assignmentColumns} FROM jones_personal_workstream_assignments a JOIN orchestration_v2_projection_threads t ON t.thread_id=a.thread_id WHERE t.deleted_at IS NULL`,
            ),
            importIssue,
            supportsNameIcons: true as const,
            ...(autoAssignments === undefined ? {} : { autoAssignments }),
          };
        }),
      )
      .pipe(Effect.mapError(storage)),
  );
  const put = Effect.fn("PersonalWorkstreams.put")(
    (w: P.PersonalWorkstream, expected?: P.PersonalWorkstream) =>
      decodeDefinition(w).pipe(
        Effect.mapError(() => new P.WorkstreamsPersonalError({ code: "invalid_request" })),
        Effect.flatMap((valid) => write(merge(valid, expected))),
      ),
  );
  const create = Effect.fn("PersonalWorkstreams.create")(
    (input: Omit<P.NewPersonalWorkstreamInput, "orderKey" | "now" | "origin">) =>
      write(
        Effect.gen(function* (): Effect.fn.Return<
          P.PutResult,
          SqlError | P.WorkstreamsPersonalError
        > {
          const old = yield* definition(input.id);
          if (old) {
            if (old.deletedAt !== null)
              return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
            return {
              outcome: old.name === input.name.trim() ? "already_satisfied" : "superseded",
              workstream: old,
              sequence: yield* sequence(),
            };
          }
          const all = yield* definitions();
          const last =
            all
              .filter((w) => w.deletedAt === null)
              .map((w) => w.orderKey)
              .sort()
              .at(-1) ?? null;
          const key = pinOrderKeyBetween(last, null);
          if (key === null)
            return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
          const timestamp = yield* now;
          const candidate = yield* Effect.try({
            try: () =>
              P.newPersonalWorkstream({
                ...input,
                orderKey: key,
                now: timestamp,
                origin: `env:${environmentId}`,
              }),
            catch: () => new P.WorkstreamsPersonalError({ code: "invalid_request" }),
          });
          return yield* merge(candidate);
        }),
      ),
  );
  const assign = Effect.fn("PersonalWorkstreams.assign")(
    (
      input: P.AssignPersonalWorkstreamInput,
      options?: { readonly supersedeInitialAutoAssignment: boolean },
    ) =>
      decodeAssignment(input).pipe(
        Effect.mapError(() => new P.WorkstreamsPersonalError({ code: "invalid_request" })),
        Effect.flatMap((valid) =>
          write(
            Effect.gen(function* (): Effect.fn.Return<
              P.AssignResult,
              SqlError | P.WorkstreamsPersonalError
            > {
              if (!(yield* exists(valid.threadId)))
                return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
              const carried = valid.workstream;
              const local = carried === null ? null : yield* definition(carried.id);
              const differentLifecycle =
                carried !== null &&
                local !== null &&
                P.isClosedPersonalWorkstream(carried.lifecycle) !==
                  P.isClosedPersonalWorkstream(local.lifecycle);
              const workstream =
                carried === null
                  ? null
                  : differentLifecycle || P.isClosedPersonalWorkstream(carried.lifecycle)
                    ? local
                    : (yield* merge(carried)).workstream;
              if (carried && !workstream)
                return yield* new P.WorkstreamsPersonalError({ code: "workstream_closed" });
              if (workstream?.deletedAt != null)
                return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
              const current = yield* assignment(valid.threadId);
              const target = workstream?.id ?? null;
              const supersedesAutomatic =
                options?.supersedeInitialAutoAssignment === true &&
                valid.expectedRevision === 0 &&
                current.revision === 1 &&
                (yield* autoRoute.hasCommittedAssignment(current));
              if (current.workstreamId === target && !supersedesAutomatic)
                return {
                  outcome: "already_satisfied",
                  assignment: current,
                  workstream,
                  sequence: yield* sequence(),
                };
              if (
                workstream &&
                (P.isClosedPersonalWorkstream(workstream.lifecycle) ||
                  (carried && P.isClosedPersonalWorkstream(carried.lifecycle)))
              )
                return yield* new P.WorkstreamsPersonalError({ code: "workstream_closed" });
              if (valid.expectedRevision !== current.revision && !supersedesAutomatic)
                return yield* new P.WorkstreamsPersonalError({
                  code: "precondition_failed",
                  currentAssignment: current,
                });
              return {
                outcome: "applied",
                assignment: yield* persistAssignment(valid.threadId, target, current.revision + 1),
                workstream,
                sequence: yield* sequence(),
              };
            }),
          ),
        ),
      ),
  );
  const assignAutomatically: PersonalWorkstreams["Service"]["assignAutomatically"] = Effect.fn(
    "PersonalWorkstreams.assignAutomatically",
  )((input) =>
    write(
      Effect.gen(function* () {
        if (!(yield* autoRoute.isAssignableThread(input.threadId)))
          return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
        const current = yield* assignment(input.threadId);
        if (current.revision !== 0 || current.workstreamId !== null)
          return yield* new P.WorkstreamsPersonalError({
            code: "precondition_failed",
            currentAssignment: current,
          });
        const group = yield* definition(input.workstreamId);
        if (group === null || group.lifecycle !== "active" || group.name !== input.workstreamName)
          return yield* new P.WorkstreamsPersonalError({ code: "workstream_not_found" });
        if (group.deletedAt !== null)
          return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });

        // Only our write can create provenance, and both records commit before publication.
        const next = yield* persistAssignment(input.threadId, group.id, 1);
        const recorded = yield* autoRoute.recordAssignment(input, {
          workstreamId: group.id,
          revision: next.revision,
          decidedAt: next.updatedAt,
          confidence: input.confidence,
          basis: input.basis,
        });
        if (!recorded)
          return yield* new P.WorkstreamsPersonalError({ code: "storage_unavailable" });
      }),
    ),
  );
  const readThread = Effect.fn("PersonalWorkstreams.readThread")((id: ThreadId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          if (!(yield* exists(id)))
            return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
          const current = yield* assignment(id);
          const decision = (yield* autoRoute.available) ? yield* autoRoute.latest(id) : undefined;
          return {
            assignment: current,
            workstream:
              current.workstreamId === null ? null : yield* definition(current.workstreamId),
            ...(decision === undefined ? {} : { autoRoute: decision }),
          };
        }),
      )
      .pipe(Effect.mapError(storage)),
  );

  const importStaged = Effect.gen(function* () {
    const path = `${config.baseDir}/workstreams/personal-import.v1.json`;
    if (!(yield* fs.exists(path))) return;
    const bytes = yield* fs.readFile(path);
    const file = yield* decodeImport(new TextDecoder().decode(bytes));
    const digest = Hex.encode(yield* digester.digest(Digest.sha256, bytes));
    const issue = (reason: P.PersonalWorkstreamsImportIssue["reason"]) =>
      Effect.fail({ importId: file.importId, reason });
    yield* write(
      Effect.gen(function* () {
        const markers = yield* sql<{
          value: string;
        }>`SELECT value FROM jones_personal_workstream_meta WHERE key=${`import:${file.importId}`}`;
        if (markers[0]) {
          if (markers[0].value !== digest) return yield* issue("content_mismatch");
          return;
        }
        if (file.targetEnvironmentId !== environmentId) return yield* issue("wrong_environment");
        let changed = false;
        for (const w of file.workstreams) {
          const current = yield* definition(w.id);
          if (
            current &&
            P.canonicalPersonalWorkstream(current) !== P.canonicalPersonalWorkstream(w)
          )
            return yield* issue("conflict");
          if (!current) {
            yield* store(w);
            changed = true;
          }
        }
        for (const a of file.assignments) {
          if (!(yield* exists(a.threadId))) return yield* issue("thread_not_found");
          const w = yield* definition(a.workstreamId);
          if (!w || w.deletedAt !== null) return yield* issue("conflict");
          const current = yield* assignment(a.threadId);
          if (current.revision > 0 && current.workstreamId !== a.workstreamId)
            return yield* issue("conflict");
          if (current.revision === 0) {
            yield* sql`INSERT INTO jones_personal_workstream_assignments(thread_id,workstream_id,revision,updated_at) VALUES(${a.threadId},${a.workstreamId},1,${yield* now})`;
            changed = true;
          }
        }
        yield* sql`INSERT INTO jones_personal_workstream_meta(key,value) VALUES(${`import:${file.importId}`},${digest})`;
        if (changed) yield* bump();
      }).pipe(
        Effect.catch((error) => {
          if (typeof error === "object" && error !== null && "reason" in error)
            importIssue = error as P.PersonalWorkstreamsImportIssue;
          return Effect.fail(error);
        }),
      ),
    );
  });
  yield* importStaged.pipe(
    Effect.catch(() => {
      importIssue ??= { importId: null, reason: "invalid_file" };
      return Effect.void;
    }),
  );
  return PersonalWorkstreams.of({
    snapshot,
    members,
    create,
    put,
    assign,
    assignAutomatically,
    readThread,
    // Subscription acquisition precedes even the first consistent snapshot read.
    changes: Stream.unwrap(
      Effect.gen(function* () {
        const sub = yield* PubSub.subscribe(invalidations);
        return Stream.concat(
          Stream.fromEffect(snapshot()),
          Stream.fromSubscription(sub).pipe(Stream.mapEffect(() => snapshot())),
        );
      }),
    ),
    readDefinitions: Effect.fn("PersonalWorkstreams.readDefinitions")((ids?: readonly string[]) =>
      definitions().pipe(
        Effect.map((rows) => (ids === undefined ? rows : rows.filter((w) => ids.includes(w.id)))),
        Effect.mapError(storage),
      ),
    ),
    counts: Effect.fn("PersonalWorkstreams.counts")(function* () {
      const s = yield* snapshot();
      const live = s.workstreams.filter((w) => w.deletedAt === null);
      return {
        observedAt: yield* now,
        sequence: s.sequence,
        total: live.length,
        active: live.filter((w) => w.lifecycle === "active").length,
      };
    }),
  });
});
export const layer = Layer.effect(PersonalWorkstreams, make);
