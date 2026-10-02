import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import {
  ChatAttachment,
  NonNegativeInt,
  OrchestrationMessageContext,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { bench, describe } from "vite-plus/test";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProjectionThreadActivity } from "../../persistence/Services/ProjectionThreadActivities.ts";
import { ProjectionThreadMessage } from "../../persistence/Services/ProjectionThreadMessages.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { projectActivityPayload } from "../ActivityPayloadProjection.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { encodeThreadDetailPageCursor } from "../threadDetailCursor.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";

// Read the unchanged SQL from its owner so an experiment cannot silently weaken
// the baseline. These anonymous databases run the real Node driver and migrations;
// their experimental indexes never enter a migration or a file-backed database.
const source = readFileSync(new URL("./ProjectionSnapshotQuery.ts", import.meta.url), "utf8");
const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");
const sourceSql = (name: string) => {
  const start = source.indexOf(`const ${name} = SqlSchema.findAll({`);
  assert.ok(start >= 0, `Missing production query: ${name}`);
  const opening = source.indexOf("sql`", start);
  const closing = source.indexOf("`", opening + 4);
  assert.ok(opening >= 0 && closing > opening, `Missing SQL template: ${name}`);
  return source.slice(opening + 4, closing).trim();
};
const sourceLimit = (name: string) => {
  const match = source.match(new RegExp(`const ${name} = (\\d+);`));
  assert.ok(match, `Missing production limit: ${name}`);
  return Number(match[1]);
};
const limits = {
  activities: sourceLimit("THREAD_DETAIL_ACTIVITY_LIMIT"),
  payloadBatch: sourceLimit("THREAD_DETAIL_ACTIVITY_PAYLOAD_BATCH_SIZE"),
  rawTurns: sourceLimit("THREAD_DETAIL_MAX_RAW_TURNS_PER_PAGE"),
};
assert.deepEqual(limits, { activities: 500, payloadBatch: 25, rawTurns: 150 });

type Bounds = {
  minAnchorAt: string;
  minTurnKey: string;
  beforeAnchorAt: string;
  beforeTurnKey: string;
};
type Bindings = Record<string, string | number>;
const bind = (template: string, bindings: Bindings) => {
  const values: Array<string | number> = [];
  const text = template.replace(/\$\{([^}]+)\}/g, (_match, key: string) => {
    const value = bindings[key];
    assert.notEqual(value, undefined, `Unexpected SQL interpolation: ${key}`);
    values.push(value!);
    return "?";
  });
  return { text, values };
};
type Statement = ReturnType<typeof bind>;
const execute = (sql: SqlClient.SqlClient, statement: Statement) =>
  sql.unsafe(statement.text, statement.values);
const plainRows = (rows: ReadonlyArray<object>) => rows.map((row) => ({ ...row }));

const baseline = {
  messages: sourceSql("listThreadMessageRowsByThreadWindow"),
  activities: sourceSql("listThreadActivityRowsByThreadWindow"),
  activityIds: sourceSql("listThreadActivityIdsByThreadWindow"),
};
type QueryClass = keyof typeof baseline;
const queryClasses = ["messages", "activities", "activityIds"] as const;
const columns = {
  messages: `message_id AS "messageId", thread_id AS "threadId", turn_id AS "turnId",
    role, text, attachments_json AS "attachments", context_json AS "context",
    is_streaming AS "isStreaming", created_at AS "createdAt", updated_at AS "updatedAt"`,
  activities: `activity_id AS "activityId", thread_id AS "threadId", turn_id AS "turnId",
    tone, kind, summary, payload_json AS "payload", sequence, created_at AS "createdAt"`,
};
const linkedTurnPredicate = `turn_id IN (
  SELECT turn_id FROM projection_turns
  WHERE thread_id = \${threadId} AND turn_id IS NOT NULL
    AND (requested_at > \${minAnchorAt}
      OR (requested_at = \${minAnchorAt} AND turn_id >= \${minTurnKey}))
    AND (requested_at < \${beforeAnchorAt}
      OR (requested_at = \${beforeAnchorAt} AND turn_id < \${beforeTurnKey}))
)`;
const disjointSql = (kind: QueryClass) => {
  const table = kind === "messages" ? "projection_thread_messages" : "projection_thread_activities";
  const union = `SELECT * FROM ${table}
    WHERE thread_id = \${threadId} AND ${linkedTurnPredicate}
    UNION ALL
    SELECT * FROM ${table}
    WHERE thread_id = \${threadId} AND turn_id IS NULL
      AND created_at >= \${minAnchorAt} AND created_at < \${beforeAnchorAt}`;
  if (kind === "messages") {
    return `SELECT ${columns.messages} FROM (${union})
      ORDER BY created_at ASC, message_id ASC`;
  }
  // Membership and NULL-turn branches are disjoint. The newest-500 selection
  // applies once to their combined rows, before restoring the response order.
  const recent = `SELECT * FROM (${union})
    ORDER BY sequence DESC, created_at DESC, activity_id DESC
    LIMIT \${THREAD_DETAIL_ACTIVITY_LIMIT}`;
  return kind === "activityIds"
    ? `SELECT activity_id AS "activityId" FROM (${recent})
        ORDER BY sequence DESC, created_at DESC, activity_id DESC`
    : `SELECT ${columns.activities} FROM (${recent})
        ORDER BY sequence ASC, created_at ASC, activity_id ASC`;
};
const experimentalIndexes = [
  `CREATE INDEX bench_messages_linked
    ON projection_thread_messages(thread_id, turn_id, created_at, message_id)`,
  `CREATE INDEX bench_messages_null
    ON projection_thread_messages(thread_id, created_at, message_id) WHERE turn_id IS NULL`,
  `CREATE INDEX bench_activities_linked
    ON projection_thread_activities(thread_id, turn_id, sequence, created_at, activity_id)`,
  `CREATE INDEX bench_activities_null
    ON projection_thread_activities(thread_id, created_at, sequence, activity_id) WHERE turn_id IS NULL`,
];

const messageSchema = ProjectionThreadMessage.mapFields(
  Struct.assign({
    isStreaming: Schema.Number,
    attachments: Schema.NullOr(Schema.fromJsonString(Schema.Array(ChatAttachment))),
    context: Schema.NullOr(Schema.fromJsonString(OrchestrationMessageContext)),
  }),
);
const activitySchema = ProjectionThreadActivity.mapFields(
  Struct.assign({
    payload: Schema.fromJsonString(Schema.Unknown),
    sequence: Schema.NullOr(NonNegativeInt),
  }),
);
const decodeMessages = Schema.decodeUnknownSync(Schema.Array(messageSchema));
const decodeActivities = Schema.decodeUnknownSync(Schema.Array(activitySchema));
const decodeActivityIds = Schema.decodeUnknownSync(
  Schema.Array(Schema.Struct({ activityId: ProjectionThreadActivity.fields.activityId })),
);
const mapMessages = (rows: unknown) =>
  decodeMessages(rows).map((row) => ({
    id: row.messageId,
    role: row.role,
    text: row.text,
    turnId: row.turnId,
    streaming: row.isStreaming === 1,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    ...(row.attachments !== null ? { attachments: row.attachments } : {}),
    ...(row.context !== null ? { context: row.context } : {}),
  }));
const mapActivities = (rows: unknown) =>
  decodeActivities(rows).map((row) => ({
    id: row.activityId,
    tone: row.tone,
    kind: row.kind,
    summary: row.summary,
    payload: row.payload,
    turnId: row.turnId,
    createdAt: row.createdAt,
    ...(row.sequence !== null ? { sequence: row.sequence } : {}),
  }));
const decodeAndMap = (kind: QueryClass, rows: unknown) =>
  kind === "messages"
    ? mapMessages(rows)
    : kind === "activities"
      ? mapActivities(rows)
      : decodeActivityIds(rows);

const threadId = ThreadId.make("history-bench");
const pinnedIds = ["pinned-approval", "pinned-input"];
const epoch = Date.parse("2026-01-01T00:00:00.000Z");
const at = (milliseconds: number) => new Date(epoch + milliseconds).toISOString();
const key = (index: number) => String(index).padStart(6, "0");
const attachmentJson = JSON.stringify([
  { type: "file", id: "notes", name: "notes.txt", mimeType: "text/plain", sizeBytes: 8 },
]);
const contextJson = JSON.stringify({
  version: 1,
  records: [
    {
      version: 1,
      contextId: "notes-context",
      kind: "file",
      label: "notes.txt",
      attachmentId: "notes",
      name: "notes.txt",
      mimeType: "text/plain",
      sizeBytes: 8,
    },
  ],
});
const message = (id: string, turn: string | null, createdAt: string, role = "assistant") => ({
  message_id: id,
  thread_id: threadId,
  turn_id: turn,
  role,
  text: `${id}: ${"message text. ".repeat(20)}`,
  is_streaming: id.endsWith("000999") ? 1 : 0,
  attachments_json: turn === null ? null : attachmentJson,
  context_json: turn === null ? null : contextJson,
  created_at: createdAt,
  updated_at: createdAt,
});
const activity = (id: string, turn: string | null, createdAt: string, sequence: number | null) => ({
  activity_id: id,
  thread_id: threadId,
  turn_id: turn,
  tone: "tool",
  kind: "tool.completed",
  summary: id,
  payload_json: JSON.stringify({
    itemType: "command_execution",
    status: "completed",
    data: { item: { command: "synthetic", aggregatedOutput: `line one\n${"x".repeat(256)}` } },
  }),
  sequence,
  created_at: createdAt,
});
const makeFixture = (turnCount: number) => {
  const fanoutStart = Math.floor(turnCount / 2) + 64;
  const turns = Array.from({ length: turnCount }, (_, index) => ({
    thread_id: threadId,
    turn_id: `turn-${key(index)}`,
    pending_message_id:
      index % 4 === 0 && !(index >= fanoutStart && index < fanoutStart + 200)
        ? `user-${key(index)}`
        : null,
    state: "completed",
    requested_at: at(Math.floor(index / 2) * 60_000),
    checkpoint_files_json: "[]",
  }));
  const messages = [
    message("import:codex:synthetic:000000", null, at(-120_000), "user"),
    message("import:codex:synthetic:000001", null, at(-60_000)),
  ];
  const activities: Array<ReturnType<typeof activity>> = [];
  let sequence = 0;
  for (const [index, turn] of turns.entries()) {
    if (turn.pending_message_id !== null) {
      messages.push(message(turn.pending_message_id, null, turn.requested_at, "user"));
    }
    // Linked membership must win even when an imported/delayed row's own time
    // is outside its turn's page range. Equal anchors still use the turn key.
    messages.push(
      message(
        `reply-${key(index)}`,
        turn.turn_id,
        index % 17 === 0 ? at(-30_000) : turn.requested_at,
      ),
    );
    if (index % 13 === 0) {
      messages.push(message(`straggler-${key(index)}`, null, turn.requested_at, "user"));
    }
    const dense = index >= turnCount - 150;
    const linkedCount = dense ? 17 : 2;
    const nullCount = dense ? 9 : 1;
    for (let offset = 0; offset < linkedCount + nullCount; offset += 1) {
      sequence += 1;
      activities.push(
        activity(
          `activity-${key(index)}-${key(offset)}`,
          offset < linkedCount ? turn.turn_id : null,
          turn.requested_at,
          sequence % 29 === 0 ? null : Math.floor(sequence / 2),
        ),
      );
    }
  }
  for (const [index, id] of pinnedIds.entries()) {
    activities.push({
      ...activity(id, null, at(-180_000 + index), null),
      tone: "approval",
      kind: index === 0 ? "approval.requested" : "user-input.requested",
      payload_json: JSON.stringify({
        requestId: index === 0 ? "approval-request" : "input-request",
      }),
    });
  }
  messages.push({
    ...message("other-thread-message", "turn-000000", at(0)),
    thread_id: ThreadId.make("other"),
  });
  activities.push({
    ...activity("other-thread-activity", "turn-000000", at(0), 999_999),
    thread_id: ThreadId.make("other"),
  });
  return { turns, messages, activities, fanoutStart };
};
type Fixture = ReturnType<typeof makeFixture>;
const compareText = (left: string, right: string) => (left < right ? -1 : left > right ? 1 : 0);
const activityOrder = (left: Fixture["activities"][number], right: Fixture["activities"][number]) =>
  (left.sequence ?? -1) - (right.sequence ?? -1) ||
  compareText(left.created_at, right.created_at) ||
  compareText(left.activity_id, right.activity_id);
const oracle = (fixture: Fixture, bounds: Bounds) => {
  const turns = new Set(
    fixture.turns
      .filter(
        (turn) =>
          (turn.requested_at > bounds.minAnchorAt ||
            (turn.requested_at === bounds.minAnchorAt && turn.turn_id >= bounds.minTurnKey)) &&
          (turn.requested_at < bounds.beforeAnchorAt ||
            (turn.requested_at === bounds.beforeAnchorAt && turn.turn_id < bounds.beforeTurnKey)),
      )
      .map((turn) => turn.turn_id),
  );
  const includes = (row: { thread_id: string; turn_id: string | null; created_at: string }) =>
    row.thread_id === threadId &&
    (row.turn_id === null
      ? row.created_at >= bounds.minAnchorAt && row.created_at < bounds.beforeAnchorAt
      : turns.has(row.turn_id));
  const messages = fixture.messages
    .filter(includes)
    .toSorted(
      (left, right) =>
        compareText(left.created_at, right.created_at) ||
        compareText(left.message_id, right.message_id),
    )
    .map((row) => ({
      messageId: row.message_id,
      threadId: row.thread_id,
      turnId: row.turn_id,
      role: row.role,
      text: row.text,
      attachments: row.attachments_json,
      context: row.context_json,
      isStreaming: row.is_streaming,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }));
  const recent = fixture.activities
    .filter(includes)
    .toSorted((left, right) => activityOrder(right, left))
    .slice(0, limits.activities);
  const activities = recent.toSorted(activityOrder).map((row) => ({
    activityId: row.activity_id,
    threadId: row.thread_id,
    turnId: row.turn_id,
    tone: row.tone,
    kind: row.kind,
    summary: row.summary,
    payload: row.payload_json,
    sequence: row.sequence,
    createdAt: row.created_at,
  }));
  return {
    messages,
    activities,
    activityIds: recent.map((row) => ({ activityId: row.activity_id })),
  };
};

const makeRuntime = () =>
  ManagedRuntime.make(
    OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(
        Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
          resolve: () => Effect.succeed(null),
        }),
      ),
      Layer.provideMerge(SqlitePersistenceMemory),
    ),
  );
type Runtime = ReturnType<typeof makeRuntime>;
const seed = Effect.fnUntraced(
  function* (fixture: Fixture) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO projection_projects
    (project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES ('history-project', 'Synthetic history', '/synthetic-history', '[]', ${at(0)}, ${at(0)})`;
    yield* sql`INSERT INTO projection_threads
    (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode,
      pending_approval_count, pending_user_input_count, created_at, updated_at)
    VALUES (${threadId}, 'history-project', 'Synthetic history',
      '{"provider":"codex","model":"gpt-5-codex"}', 'full-access', 'default', 1, 1, ${at(0)}, ${at(0)})`;
    for (let offset = 0; offset < fixture.turns.length; offset += 256) {
      yield* sql`INSERT INTO projection_turns ${sql.insert(fixture.turns.slice(offset, offset + 256))}`;
    }
    for (let offset = 0; offset < fixture.messages.length; offset += 256) {
      yield* sql`INSERT INTO projection_thread_messages ${sql.insert(fixture.messages.slice(offset, offset + 256))}`;
    }
    for (let offset = 0; offset < fixture.activities.length; offset += 256) {
      yield* sql`INSERT INTO projection_thread_activities ${sql.insert(fixture.activities.slice(offset, offset + 256))}`;
    }
    yield* sql`INSERT INTO projection_pending_approvals
    (request_id, thread_id, status, created_at)
    VALUES ('approval-request', ${threadId}, 'pending', ${at(-180_000)})`;
  },
  (effect) => Effect.flatMap(SqlClient.SqlClient, (sql) => sql.withTransaction(effect)),
);

const timed = async <T>(operation: () => Promise<T>) => {
  const start = performance.now();
  const value = await operation();
  return { value, ms: performance.now() - start };
};
const statistics = (samples: number[]) => {
  const sorted = samples.toSorted((left, right) => left - right);
  const mean = samples.reduce((sum, value) => sum + value, 0) / samples.length;
  return {
    samplesMs: samples,
    p50Ms: sorted[Math.ceil(sorted.length * 0.5) - 1],
    p95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1],
    minMs: sorted[0],
    maxMs: sorted.at(-1),
    meanMs: mean,
    sdMs: Math.sqrt(samples.reduce((sum, value) => sum + (value - mean) ** 2, 0) / samples.length),
  };
};
const report = (kind: string, values: object) => console.log(JSON.stringify({ kind, ...values }));
const turnWindowTemplate = sourceSql("listTurnWindowRows");
const decodeTurnWindow = Schema.decodeUnknownSync(
  Schema.Array(
    Schema.Struct({
      anchorAt: Schema.String,
      turnKey: Schema.String,
    }),
  ),
);
type PageCase = { name: string; turnLimit: number; cursorIndex?: number; empty?: boolean };
const resolvePage = async (runtime: Runtime, fixture: Fixture, page: PageCase) => {
  const sql = await runtime.runPromise(SqlClient.SqlClient);
  const before = page.empty
    ? { beforeAnchorAt: at(-240_000), beforeTurnKey: "" }
    : page.cursorIndex === undefined
      ? { beforeAnchorAt: "~", beforeTurnKey: "" }
      : {
          beforeAnchorAt: fixture.turns[page.cursorIndex]!.requested_at,
          beforeTurnKey: fixture.turns[page.cursorIndex]!.turn_id,
        };
  const input = {
    threadId,
    ...before,
    userTurnLimit: page.turnLimit,
    maxRawTurns: limits.rawTurns,
  };
  const rows = decodeTurnWindow(
    await runtime.runPromise(execute(sql, bind(turnWindowTemplate, input))),
  );
  const oldest = rows[0];
  const older =
    oldest === undefined
      ? []
      : decodeTurnWindow(
          await runtime.runPromise(
            execute(
              sql,
              bind(turnWindowTemplate, {
                threadId,
                beforeAnchorAt: oldest.anchorAt,
                beforeTurnKey: oldest.turnKey,
                userTurnLimit: 1,
                maxRawTurns: 1,
              }),
            ),
          ),
        );
  const bounds: Bounds =
    oldest === undefined
      ? { minAnchorAt: "", minTurnKey: "", beforeAnchorAt: "", beforeTurnKey: "" }
      : {
          minAnchorAt: older.length > 0 ? oldest.anchorAt : "",
          minTurnKey: older.length > 0 ? oldest.turnKey : "",
          ...before,
        };
  const window = {
    turnLimit: page.turnLimit,
    ...(page.cursorIndex !== undefined || page.empty
      ? {
          beforeCursor: encodeThreadDetailPageCursor({
            threadId,
            beforeAnchorAt: before.beforeAnchorAt,
            beforeTurnId: before.beforeTurnKey,
          }),
        }
      : {}),
  };
  return { bounds, window, rawTurnCount: rows.length };
};
type Variant = { name: string; runtime: Runtime; sql: SqlClient.SqlClient; disjoint: boolean };
const statementFor = (variant: Variant, kind: QueryClass, bounds: Bounds) =>
  bind(variant.disjoint ? disjointSql(kind) : baseline[kind], {
    threadId,
    ...bounds,
    THREAD_DETAIL_ACTIVITY_LIMIT: limits.activities,
  });
const hydratedCollections = async (variant: Variant, bounds: Bounds) => {
  const messages = mapMessages(
    await variant.runtime.runPromise(
      execute(variant.sql, statementFor(variant, "messages", bounds)),
    ),
  );
  const selected = decodeActivityIds(
    await variant.runtime.runPromise(
      execute(variant.sql, statementFor(variant, "activityIds", bounds)),
    ),
  );
  const ids = [...new Set([...selected.map((row) => row.activityId), ...pinnedIds])];
  const activities: Array<ReturnType<typeof projectActivityPayload>> = [];
  let batches = 0;
  for (let offset = 0; offset < ids.length; offset += limits.payloadBatch) {
    const batch = ids.slice(offset, offset + limits.payloadBatch);
    const rows = await variant.runtime.runPromise(
      variant.sql.unsafe(
        `SELECT ${columns.activities} FROM projection_thread_activities
        WHERE activity_id IN (${batch.map(() => "?").join(", ")})`,
        batch,
      ),
    );
    activities.push(...mapActivities(rows).map(projectActivityPayload));
    batches += 1;
  }
  activities.sort(
    (left, right) =>
      (left.sequence ?? -1) - (right.sequence ?? -1) ||
      left.createdAt.localeCompare(right.createdAt) ||
      left.id.localeCompare(right.id),
  );
  return { messages, activities, batches };
};

// Measure only the insert statement inside an open transaction. Rollback is
// outside that interval and keeps every query sample on the identical fixture.
const insertion = Effect.fnUntraced(function* (fixture: Fixture, kind: "messages" | "activities") {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql
    .withTransaction(
      Effect.gen(function* () {
        const start = performance.now();
        if (kind === "messages") {
          yield* sql`INSERT INTO projection_thread_messages ${sql.insert(
            fixture.messages
              .slice(0, 128)
              .map((row) => ({ ...row, message_id: `insert-probe:${row.message_id}` })),
          )}`;
        } else {
          yield* sql`INSERT INTO projection_thread_activities ${sql.insert(
            fixture.activities
              .slice(0, 128)
              .map((row) => ({ ...row, activity_id: `insert-probe:${row.activity_id}` })),
          )}`;
        }
        return yield* Effect.fail({
          _tag: "InsertionRollback" as const,
          ms: performance.now() - start,
        });
      }),
    )
    .pipe(Effect.catchTag("InsertionRollback", (result) => Effect.succeed(result.ms)));
});

for (const turnCount of [1_000, 10_000]) {
  describe(`history SQL / ${turnCount} turns`, () => {
    const fixture = makeFixture(turnCount);
    // The installed benchmark runner skips suite hooks. Keep acquisition and
    // disposal inside its callback, including when setup or a sample rejects.
    const withVariants = async (run: (variants: Variant[]) => Promise<void>) => {
      const runtimes: Runtime[] = [];
      const variants: Variant[] = [];
      let failed = false;
      try {
        const setup: object[] = [];
        for (const indexed of [false, true]) {
          const runtime = makeRuntime();
          runtimes.push(runtime);
          const initialized = await timed(() => runtime.runPromise(SqlClient.SqlClient));
          const sql = initialized.value;
          const seeded = await timed(() => runtime.runPromise(seed(fixture)));
          const indexBuild = await timed(async () => {
            if (indexed) {
              for (const statement of experimentalIndexes)
                await runtime.runPromise(sql.unsafe(statement));
            }
          });
          variants.push({
            name: indexed ? "index-only" : "unchanged",
            runtime,
            sql,
            disjoint: false,
          });
          setup.push({
            variant: indexed ? "index-only" : "unchanged",
            schemaMs: initialized.ms,
            seedMs: seeded.ms,
            indexBuildMs: indexed ? indexBuild.ms : null,
          });
        }
        variants.push({ ...variants[1]!, name: "disjoint-indexed", disjoint: true });
        const base = variants[0]!;
        report("history-fixture", {
          turnCount,
          fixtureSha256: sha256(JSON.stringify(fixture)),
          sourceSha256: sha256(source),
          limits,
          rows: {
            turns: fixture.turns.length,
            messages: fixture.messages.length,
            activities: fixture.activities.length,
          },
          node: process.version,
          platform: process.platform,
          arch: process.arch,
          sqlite: await base.runtime.runPromise(base.sql`SELECT sqlite_version() AS version`),
          journalMode: await base.runtime.runPromise(base.sql`PRAGMA journal_mode`),
          synchronous: await base.runtime.runPromise(base.sql`PRAGMA synchronous`),
          foreignKeys: await base.runtime.runPromise(base.sql`PRAGMA foreign_keys`),
          upstreamMigrations: await base.runtime.runPromise(
            base.sql`SELECT * FROM effect_sql_migrations`,
          ),
          forkMigrations: await base.runtime.runPromise(
            base.sql`SELECT * FROM jones_sql_migrations`,
          ),
          indexes: experimentalIndexes,
          setup,
          durability: "anonymous in-memory database; no file/WAL durability measurement",
        });
        for (const kind of ["messages", "activities"] as const) {
          const samples: number[][] = [[], []];
          for (let round = 0; round < 7; round += 1) {
            for (const index of round % 2 === 0 ? [0, 1] : [1, 0]) {
              samples[index]!.push(
                await variants[index]!.runtime.runPromise(insertion(fixture, kind)),
              );
            }
          }
          for (const variant of variants.slice(0, 2)) {
            const counts = await variant.runtime.runPromise(variant.sql`
              SELECT (SELECT COUNT(*) FROM projection_thread_messages) AS messages,
                (SELECT COUNT(*) FROM projection_thread_activities) AS activities`);
            assert.deepEqual(plainRows(counts), [
              { messages: fixture.messages.length, activities: fixture.activities.length },
            ]);
          }
          report("history-insertion", {
            turnCount,
            queryClass: kind,
            batchRows: 128,
            interval: "insert construction/execution in open transaction; rolled back after timing",
            variants: variants
              .slice(0, 2)
              .map((variant, index) => ({ variant: variant.name, ...statistics(samples[index]!) })),
          });
        }
        await run(variants);
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        const results = await Promise.allSettled(
          runtimes.toReversed().map(async (runtime) => runtime.dispose()),
        );
        const failures = results.flatMap((result) =>
          result.status === "rejected" ? [result.reason] : [],
        );
        report("history-cleanup", {
          turnCount,
          runtimeCount: runtimes.length,
          status: failures.length === 0 ? "disposed" : "failed",
          failureCount: failures.length,
        });
        if (failures.length > 0) {
          const cleanupError = new AggregateError(failures, "History benchmark disposal failed");
          if (failed) console.error(cleanupError);
          else throw cleanupError;
        }
      }
    };

    const pages: PageCase[] = [
      { name: "first-recent-dense", turnLimit: 10 },
      { name: "recent-sparse", turnLimit: 10, cursorIndex: turnCount - 150 },
      { name: "middle-sparse", turnLimit: 10, cursorIndex: turnCount / 2 },
      { name: "oldest-imported", turnLimit: 10, cursorIndex: 32 },
      { name: "empty-before-oldest", turnLimit: 10, empty: true },
      { name: "equal-timestamp-keyset", turnLimit: 1, cursorIndex: turnCount / 2 + 1 },
      { name: "subagent-ceiling", turnLimit: 10, cursorIndex: fixture.fanoutStart + 160 },
    ];
    for (const page of pages) {
      bench(
        page.name,
        async () => {
          await withVariants(async (variants) => {
            const resolved = await resolvePage(variants[0]!.runtime, fixture, page);
            assert.deepEqual(await resolvePage(variants[1]!.runtime, fixture, page), resolved);
            if (page.empty) assert.equal(resolved.rawTurnCount, 0);
            if (page.name === "subagent-ceiling")
              assert.equal(resolved.rawTurnCount, limits.rawTurns);
            if (page.name === "equal-timestamp-keyset") {
              assert.equal(resolved.bounds.minAnchorAt, resolved.bounds.beforeAnchorAt);
              assert.equal(resolved.rawTurnCount, 1);
            }
            const expected = oracle(fixture, resolved.bounds);
            if (page.name === "first-recent-dense")
              assert.equal(expected.activities.length, limits.activities);
            if (page.name === "oldest-imported") {
              assert.equal(
                expected.messages.filter((row) => row.messageId.startsWith("import:")).length,
                2,
              );
            }
            if (page.empty)
              assert.deepEqual(expected, { messages: [], activities: [], activityIds: [] });
            report("history-page", {
              turnCount,
              page: page.name,
              ...resolved,
              memoryBefore: process.memoryUsage(),
            });

            for (const kind of queryClasses) {
              const statements = variants.map((variant) =>
                statementFor(variant, kind, resolved.bounds),
              );
              const samples = variants.map(() => ({
                sql: [] as number[],
                decodeMap: [] as number[],
              }));
              const plans: object[] = [];
              for (const [index, variant] of variants.entries()) {
                const statement = statements[index]!;
                const first = await timed(() =>
                  variant.runtime.runPromise(execute(variant.sql, statement)),
                );
                assert.deepEqual(
                  plainRows(first.value),
                  expected[kind],
                  `${variant.name}/${kind}: complete ordered rows`,
                );
                assert.deepEqual(
                  decodeAndMap(kind, first.value),
                  decodeAndMap(kind, expected[kind]),
                );
                plans.push({
                  variant: variant.name,
                  sql: statement.text,
                  bindings: statement.values,
                  sqlSha256: sha256(statement.text),
                  firstObservedMs: first.ms,
                  plan: await variant.runtime.runPromise(
                    variant.sql.unsafe(`EXPLAIN QUERY PLAN ${statement.text}`, statement.values),
                  ),
                });
              }
              // Warm the exact statements, then rotate and reverse the variant order
              // across fixed repeats. No cache eviction or wall-time assertion.
              for (let round = -3; round < 15; round += 1) {
                const order = round % 2 === 0 ? [0, 1, 2] : [2, 1, 0];
                for (const index of order.map((value) => (value + Math.abs(round)) % 3)) {
                  const variant = variants[index]!;
                  const measured = await timed(() =>
                    variant.runtime.runPromise(execute(variant.sql, statements[index]!)),
                  );
                  const decodeStart = performance.now();
                  decodeAndMap(kind, measured.value);
                  const decodeMs = performance.now() - decodeStart;
                  if (round >= 0) {
                    samples[index]!.sql.push(measured.ms);
                    samples[index]!.decodeMap.push(decodeMs);
                  }
                }
              }
              report("history-query", {
                turnCount,
                page: page.name,
                queryClass: kind,
                returnedRows: expected[kind].length,
                rawOutputBytes: Buffer.byteLength(JSON.stringify(expected[kind])),
                mappedOutputBytes: Buffer.byteLength(
                  JSON.stringify(decodeAndMap(kind, expected[kind])),
                ),
                plans,
                variants: variants.map((variant, index) => ({
                  variant: variant.name,
                  sql: statistics(samples[index]!.sql),
                  decodeAndMap: statistics(samples[index]!.decodeMap),
                })),
              });
            }

            const snapshot = (runtime: Runtime) =>
              runtime.runPromise(
                Effect.flatMap(ProjectionSnapshotQuery, (query) =>
                  query.getThreadDetailSnapshot(threadId, resolved.window),
                ),
              );
            const unchangedSnapshot = Option.getOrThrow(await snapshot(variants[0]!.runtime));
            assert.deepEqual(
              Option.getOrThrow(await snapshot(variants[1]!.runtime)),
              unchangedSnapshot,
            );
            const collections = variants.map(() => [] as number[]);
            const snapshots: number[][] = [[], []];
            for (let round = -1; round < 5; round += 1) {
              for (const index of round % 2 === 0 ? [0, 1, 2] : [2, 1, 0]) {
                const measured = await timed(() =>
                  hydratedCollections(variants[index]!, resolved.bounds),
                );
                assert.deepEqual(measured.value.messages, unchangedSnapshot.thread.messages);
                assert.deepEqual(measured.value.activities, unchangedSnapshot.thread.activities);
                assert.ok(
                  measured.value.batches <=
                    Math.ceil((limits.activities + pinnedIds.length) / limits.payloadBatch),
                );
                if (round >= 0) collections[index]!.push(measured.ms);
              }
              for (const index of round % 2 === 0 ? [0, 1] : [1, 0]) {
                const measured = await timed(() => snapshot(variants[index]!.runtime));
                assert.deepEqual(Option.getOrThrow(measured.value), unchangedSnapshot);
                if (round >= 0) snapshots[index]!.push(measured.ms);
              }
            }
            for (const id of pinnedIds)
              assert.ok(unchangedSnapshot.thread.activities.some((row) => row.id === id));
            report("history-hydration", {
              turnCount,
              page: page.name,
              interval:
                "messages + activity IDs + fixed unresolved IDs + sequential 25-row payload hydration/projection/order",
              pinnedIds,
              activityRows: unchangedSnapshot.thread.activities.length,
              collectionVariants: variants.map((variant, index) => ({
                variant: variant.name,
                ...statistics(collections[index]!),
              })),
              productionSnapshotVariants: variants
                .slice(0, 2)
                .map((variant, index) => ({
                  variant: variant.name,
                  ...statistics(snapshots[index]!),
                })),
              memoryAfter: process.memoryUsage(),
              processMaxRssKiB: process.resourceUsage().maxRSS,
              limits:
                "sampled process memory, not per-query allocation; first observed query is not a cold-cache claim; disjoint SQL is not integrated into the production snapshot",
            });
          });
        },
        { time: 0, iterations: 1, warmupTime: 0, warmupIterations: 0 },
      );
    }
  });
}
