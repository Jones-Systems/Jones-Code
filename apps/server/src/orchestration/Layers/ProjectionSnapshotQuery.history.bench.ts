import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Collection reads SQL before runtimes exist.
import * as NodeFS from "node:fs";
import * as NodePerfHooks from "node:perf_hooks";
import { ChatAttachment, OrchestrationMessageContext, ThreadId } from "@t3tools/contracts";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { bench, describe } from "vite-plus/test";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProjectionThreadMessage } from "../../persistence/Services/ProjectionThreadMessages.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { encodeThreadDetailPageCursor } from "../threadDetailCursor.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";

// Read the unchanged SQL from its owner so an experiment cannot silently weaken
// the baseline. These anonymous databases run the real Node driver and migrations;
// both message variants share one unchanged schema and fixture, with no added indexes.
const source = NodeFS.readFileSync(
  new URL("./ProjectionSnapshotQuery.ts", import.meta.url),
  "utf8",
);
const sha256 = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const sourceSql = (name: string) => {
  const start = source.indexOf(`const ${name} = SqlSchema.findAll({`);
  NodeAssert.ok(start >= 0, `Missing production query: ${name}`);
  const opening = source.indexOf("sql`", start);
  const closing = source.indexOf("`", opening + 4);
  NodeAssert.ok(opening >= 0 && closing > opening, `Missing SQL template: ${name}`);
  return source.slice(opening + 4, closing).trim();
};
const sourceLimit = (name: string) => {
  const match = source.match(new RegExp(`const ${name} = (\\d+);`));
  NodeAssert.ok(match, `Missing production limit: ${name}`);
  return Number(match[1]);
};
const limits = {
  activities: sourceLimit("THREAD_DETAIL_ACTIVITY_LIMIT"),
  payloadBatch: sourceLimit("THREAD_DETAIL_ACTIVITY_PAYLOAD_BATCH_SIZE"),
  rawTurns: sourceLimit("THREAD_DETAIL_MAX_RAW_TURNS_PER_PAGE"),
};
NodeAssert.deepEqual(limits, { activities: 500, payloadBatch: 25, rawTurns: 150 });

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
    NodeAssert.notEqual(value, undefined, `Unexpected SQL interpolation: ${key}`);
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
};
type QueryClass = keyof typeof baseline;
const queryClasses = ["messages"] as const;
const columns = {
  messages: `message_id AS "messageId", thread_id AS "threadId", turn_id AS "turnId",
    role, text, attachments_json AS "attachments", context_json AS "context",
    is_streaming AS "isStreaming", created_at AS "createdAt", updated_at AS "updatedAt"`,
};
const linkedTurnPredicate = `turn_id IN (
  SELECT turn_id FROM projection_turns
  WHERE thread_id = \${threadId} AND turn_id IS NOT NULL
    AND (requested_at > \${minAnchorAt}
      OR (requested_at = \${minAnchorAt} AND turn_id >= \${minTurnKey}))
    AND (requested_at < \${beforeAnchorAt}
      OR (requested_at = \${beforeAnchorAt} AND turn_id < \${beforeTurnKey}))
)`;
const disjointMessageUnion = `SELECT * FROM projection_thread_messages
    WHERE thread_id = \${threadId} AND ${linkedTurnPredicate}
    UNION ALL
    SELECT * FROM projection_thread_messages
    WHERE thread_id = \${threadId} AND turn_id IS NULL
      AND created_at >= \${minAnchorAt} AND created_at < \${beforeAnchorAt}`;
const disjointMessageSql = `SELECT ${columns.messages} FROM (${disjointMessageUnion})
      ORDER BY created_at ASC, message_id ASC`;

const messageSchema = ProjectionThreadMessage.mapFields(
  Struct.assign({
    isStreaming: Schema.Number,
    attachments: Schema.NullOr(Schema.fromJsonString(Schema.Array(ChatAttachment))),
    context: Schema.NullOr(Schema.fromJsonString(OrchestrationMessageContext)),
  }),
);
const decodeMessages = Schema.decodeUnknownSync(Schema.Array(messageSchema));
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

const threadId = ThreadId.make("history-bench");
const pinnedIds = ["pinned-approval", "pinned-input"];
const epoch = DateTime.toEpochMillis(DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"));
const at = (milliseconds: number) => DateTime.formatIso(DateTime.makeUnsafe(epoch + milliseconds));
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
  return { messages };
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
  const start = NodePerfHooks.performance.now();
  const value = await operation();
  return { value, ms: NodePerfHooks.performance.now() - start };
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
const report = (kind: string, values: object) =>
  Effect.runSync(Console.log(JSON.stringify({ kind, ...values })));
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
  bind(variant.disjoint ? disjointMessageSql : baseline[kind], {
    threadId,
    ...bounds,
  });

for (const turnCount of [10_000]) {
  describe(`history SQL / ${turnCount} turns`, () => {
    const fixture = makeFixture(turnCount);
    // The installed benchmark runner skips suite hooks. Keep acquisition and
    // disposal inside its callback, including when setup or a sample rejects.
    const withVariants = async (run: (variants: Variant[]) => Promise<void>) => {
      const runtimes: Runtime[] = [];
      const variants: Variant[] = [];
      let failed = false;
      try {
        const runtime = makeRuntime();
        runtimes.push(runtime);
        const initialized = await timed(() => runtime.runPromise(SqlClient.SqlClient));
        const sql = initialized.value;
        const seeded = await timed(() => runtime.runPromise(seed(fixture)));
        variants.push(
          { name: "unchanged", runtime, sql, disjoint: false },
          { name: "disjoint-unchanged-schema", runtime, sql, disjoint: true },
        );
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
          platform: await base.runtime.runPromise(HostProcessPlatform),
          arch: await base.runtime.runPromise(HostProcessArchitecture),
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
          schema: await runtime.runPromise(sql`
            SELECT name, type, tbl_name, sql FROM sqlite_master
            WHERE type IN ('table', 'index') AND tbl_name IN
              ('projection_turns', 'projection_thread_messages', 'projection_thread_activities')
            ORDER BY type, name`),
          addedIndexes: [],
          setup: [
            {
              database: "shared-unchanged-schema",
              schemaMs: initialized.ms,
              seedMs: seeded.ms,
            },
          ],
          durability: "anonymous in-memory database; no file/WAL durability measurement",
        });
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
          if (failed) Effect.runSync(Console.error(cleanupError));
          else throw cleanupError;
        }
      }
    };

    const pages: PageCase[] = [
      { name: "first-recent-dense", turnLimit: 10 },
      { name: "middle-sparse", turnLimit: 10, cursorIndex: turnCount / 2 },
      { name: "oldest-imported", turnLimit: 10, cursorIndex: 32 },
      { name: "subagent-ceiling", turnLimit: 10, cursorIndex: fixture.fanoutStart + 160 },
    ];
    for (const page of pages) {
      bench(
        page.name,
        async () => {
          await withVariants(async (variants) => {
            const resolved = await resolvePage(variants[0]!.runtime, fixture, page);
            NodeAssert.deepEqual(await resolvePage(variants[1]!.runtime, fixture, page), resolved);
            if (page.name === "subagent-ceiling")
              NodeAssert.equal(resolved.rawTurnCount, limits.rawTurns);
            const expected = oracle(fixture, resolved.bounds);
            if (page.name === "oldest-imported") {
              NodeAssert.equal(
                expected.messages.filter((row) => row.messageId.startsWith("import:")).length,
                2,
              );
            }
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
                NodeAssert.deepEqual(
                  plainRows(first.value),
                  expected[kind],
                  `${variant.name}/${kind}: complete ordered rows`,
                );
                NodeAssert.deepEqual(mapMessages(first.value), mapMessages(expected[kind]));
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
              // Warm the exact statements, then alternate the two variant orders
              // across fixed repeats on their shared database. No wall-time assertion.
              for (let round = -3; round < 15; round += 1) {
                for (const index of round % 2 === 0 ? [0, 1] : [1, 0]) {
                  const variant = variants[index]!;
                  const measured = await timed(() =>
                    variant.runtime.runPromise(execute(variant.sql, statements[index]!)),
                  );
                  const decodeStart = NodePerfHooks.performance.now();
                  mapMessages(measured.value);
                  const decodeMs = NodePerfHooks.performance.now() - decodeStart;
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
                mappedOutputBytes: Buffer.byteLength(JSON.stringify(mapMessages(expected[kind]))),
                plans,
                variants: variants.map((variant, index) => ({
                  variant: variant.name,
                  sql: statistics(samples[index]!.sql),
                  decodeAndMap: statistics(samples[index]!.decodeMap),
                })),
                memoryAfter: process.memoryUsage(),
                processMaxRssKiB: process.resourceUsage().maxRSS,
                limits:
                  "sampled process memory, not per-query allocation; first observed query is not a cold-cache claim; message SQL is not integrated into the production snapshot; reporter callback duration includes setup",
              });
            }
          });
        },
        { time: 0, iterations: 1, warmupTime: 0, warmupIterations: 0 },
      );
    }
  });
}
