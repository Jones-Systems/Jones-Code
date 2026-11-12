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

export class PersonalWorkstreams extends Context.Service<
  PersonalWorkstreams,
  {
    readonly snapshot: () => Effect.Effect<
      P.PersonalWorkstreamsSnapshot,
      P.WorkstreamsPersonalError
    >;
    readonly changes: Stream.Stream<P.PersonalWorkstreamsSnapshot, P.WorkstreamsPersonalError>;
    readonly create: (
      input: Omit<P.NewPersonalWorkstreamInput, "orderKey" | "now" | "origin">,
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly put: (
      workstream: P.PersonalWorkstream,
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly assign: (
      input: P.AssignPersonalWorkstreamInput,
    ) => Effect.Effect<P.AssignResult, P.WorkstreamsPersonalError>;
    readonly readThread: (
      id: ThreadId,
    ) => Effect.Effect<
      { assignment: P.PersonalWorkstreamAssignment; workstream: P.PersonalWorkstream | null },
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

const definitionColumns = `id, name, lifecycle, color, order_key AS orderKey, created_at AS createdAt, updated_at AS updatedAt, deleted_at AS deletedAt, rev, origin`;
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
    sql.unsafe<P.PersonalWorkstream>(`SELECT ${definitionColumns} FROM jones_personal_workstreams`);
  const definition = Effect.fn("PersonalWorkstreams.definition")(function* (id: string) {
    const rows = yield* sql.unsafe<P.PersonalWorkstream>(
      `SELECT ${definitionColumns} FROM jones_personal_workstreams WHERE id=?`,
      [id],
    );
    return rows[0] ?? null;
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
  const store = Effect.fn("PersonalWorkstreams.store")(function* (w: P.PersonalWorkstream) {
    yield* sql`INSERT INTO jones_personal_workstreams ${sql.insert({ id: w.id, name: w.name, lifecycle: w.lifecycle, color: w.color, order_key: w.orderKey, created_at: w.createdAt, updated_at: w.updatedAt, deleted_at: w.deletedAt, rev: w.rev, origin: w.origin })} ON CONFLICT(id) DO UPDATE SET name=excluded.name,lifecycle=excluded.lifecycle,color=excluded.color,order_key=excluded.order_key,created_at=excluded.created_at,updated_at=excluded.updated_at,deleted_at=excluded.deleted_at,rev=excluded.rev,origin=excluded.origin`;
  });
  const merge = Effect.fn("PersonalWorkstreams.merge")(function* (
    w: P.PersonalWorkstream,
  ): Effect.fn.Return<P.PutResult, SqlError> {
    const old = yield* definition(w.id);
    const comparison = old === null ? 1 : P.comparePersonalWorkstreams(w, old);
    if (comparison <= 0)
      return {
        outcome: comparison === 0 ? "already_satisfied" : "superseded",
        workstream: old!,
        sequence: yield* sequence(),
      };
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
          return {
            environmentId,
            sequence: yield* sequence(),
            workstreams: yield* definitions(),
            assignments: yield* sql.unsafe<P.PersonalWorkstreamAssignment>(
              `SELECT ${assignmentColumns} FROM jones_personal_workstream_assignments a JOIN orchestration_v2_projection_threads t ON t.thread_id=a.thread_id WHERE t.deleted_at IS NULL`,
            ),
            importIssue,
          };
        }),
      )
      .pipe(Effect.mapError(storage)),
  );
  const put = Effect.fn("PersonalWorkstreams.put")((w: P.PersonalWorkstream) =>
    decodeDefinition(w).pipe(
      Effect.mapError(() => new P.WorkstreamsPersonalError({ code: "invalid_request" })),
      Effect.flatMap((valid) => write(merge(valid))),
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
  const assign = Effect.fn("PersonalWorkstreams.assign")((input: P.AssignPersonalWorkstreamInput) =>
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
            const workstream =
              valid.workstream === null ? null : (yield* merge(valid.workstream)).workstream;
            if (workstream?.deletedAt != null)
              return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
            const current = yield* assignment(valid.threadId);
            const target = workstream?.id ?? null;
            if (current.workstreamId === target)
              return {
                outcome: "already_satisfied",
                assignment: current,
                workstream,
                sequence: yield* sequence(),
              };
            if (valid.expectedRevision !== current.revision)
              return yield* new P.WorkstreamsPersonalError({
                code: "precondition_failed",
                currentAssignment: current,
              });
            const next = {
              threadId: valid.threadId,
              workstreamId: target,
              revision: current.revision + 1,
              updatedAt: yield* now,
            };
            yield* sql`INSERT INTO jones_personal_workstream_assignments ${sql.insert({ thread_id: next.threadId, workstream_id: target, revision: next.revision, updated_at: next.updatedAt })} ON CONFLICT(thread_id) DO UPDATE SET workstream_id=excluded.workstream_id,revision=excluded.revision,updated_at=excluded.updated_at`;
            yield* bump();
            return {
              outcome: "applied",
              assignment: next,
              workstream,
              sequence: yield* sequence(),
            };
          }),
        ),
      ),
    ),
  );
  const readThread = Effect.fn("PersonalWorkstreams.readThread")((id: ThreadId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          if (!(yield* exists(id)))
            return yield* new P.WorkstreamsPersonalError({ code: "thread_not_found" });
          const current = yield* assignment(id);
          return {
            assignment: current,
            workstream:
              current.workstreamId === null ? null : yield* definition(current.workstreamId),
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
    create,
    put,
    assign,
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
