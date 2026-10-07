import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { makeSqlitePersistenceLive } from "../../src/persistence/Layers/Sqlite.ts";
import { initializeV2Database } from "../../src/persistence/initializeV2Database.ts";
import * as EventSink from "../../src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../src/orchestration-v2/ProjectionStore.ts";
import {
  assertOwnedDatabase,
  createOwnedRoot,
  observeSyntheticClose,
  sealSyntheticFixture,
} from "../../../../scripts/jones/performance/guard.mjs";
import type {
  OwnedRoot,
  PerformanceBinding,
  StagingPolicy,
  SyntheticFixtureReceipt,
} from "../../../../scripts/jones/performance/guard.mjs";
import { assertCurrentDatabaseSource } from "../../../../scripts/jones/performance/sources.mjs";
import type { CurrentDatabaseSource } from "../../../../scripts/jones/performance/sources.mjs";

export type CurrentFixtureProfile = "health-offline-delete" | "benchmark-wal";
export interface CurrentFixtureRecipe {
  readonly kind: "coherent-v2";
  readonly threads: number;
  readonly historyTurns: number;
  readonly payloadBytes: number;
}
export interface CurrentFixtureOptions {
  readonly producer?: "current-v2";
  readonly parentPath: string;
  readonly childName: string;
  readonly binding: PerformanceBinding;
  readonly policy: StagingPolicy;
  readonly databaseSource: CurrentDatabaseSource;
  readonly profile?: CurrentFixtureProfile;
  readonly recipe?: Partial<CurrentFixtureRecipe>;
  readonly signal?: AbortSignal;
}
type Services =
  | SqlClient.SqlClient
  | EventSink.EventSinkV2
  | EventStore.EventStoreV2
  | ProjectionStore.ProjectionStoreV2;
export interface CurrentFixtureContext {
  readonly owner: OwnedRoot;
  readonly paths: { readonly dbPath: string };
  readonly databaseSource: CurrentDatabaseSource;
  readonly recipe: CurrentFixtureRecipe;
  readonly run: <A, E>(effect: Effect.Effect<A, E, Services>) => Promise<A>;
}
export interface CurrentFixtureReceipt extends Omit<SyntheticFixtureReceipt, "schema"> {
  readonly schema: "jones-performance-fixture/v2";
  readonly producer: "current-v2";
  readonly databaseSource: CurrentDatabaseSource;
  readonly runtime: {
    readonly nodeVersion: string;
    readonly versions: Readonly<Record<string, string | undefined>>;
    readonly sqliteVersion: string;
    readonly profile: CurrentFixtureProfile;
    readonly pragmas: Readonly<Record<string, string | number>>;
  };
}
export interface CurrentFixtureCapture {
  readonly schema: "jones-performance-capture/v1";
  readonly databaseSource: CurrentDatabaseSource;
  readonly recipe: CurrentFixtureRecipe;
  readonly runtime: CurrentFixtureReceipt["runtime"];
  readonly profile: {
    readonly kind: CurrentFixtureProfile;
    readonly stage: "sealed";
    readonly productionClosed: true;
    readonly productionObservations: readonly {
      readonly phase: string;
      readonly pragmas: Readonly<Record<string, string | number>>;
    }[];
    readonly maintenance?: {
      readonly header: {
        readonly bytesRead: number;
        readonly writeVersion: number;
        readonly readVersion: number;
      };
      readonly sidecars: {
        readonly wal: boolean;
        readonly shm: boolean;
        readonly journal: boolean;
      };
    };
  };
  readonly tables: Readonly<Record<string, { readonly status: "present"; readonly count: number }>>;
  readonly ledgers: Readonly<
    Record<string, readonly { readonly id: number; readonly name: string }[]>
  >;
  readonly integrity: { readonly results: readonly string[]; readonly ok: boolean };
  readonly foreignKeys: { readonly violations: number; readonly sha256: string };
}
export interface CurrentProductionResult<A> {
  readonly owner: OwnedRoot;
  closeKnown: boolean;
  value?: A;
  error?: Error;
  retainReason?: string;
  receipt?: CurrentFixtureReceipt;
  receiptSha256?: string;
  capture?: CurrentFixtureCapture;
  captureSha256?: string;
}
const hash = (value: unknown) =>
  Crypto.createHash("sha256")
    .update(`${JSON.stringify(value)}\n`)
    .digest("hex");
function freezeEvidence<A>(value: A): A {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freezeEvidence(child);
    Object.freeze(value);
  }
  return value;
}
const producerPath = FS.realpathSync(Path.resolve(import.meta.dirname, "../../../.."));
const pragmaNames = [
  "journal_mode",
  "synchronous",
  "foreign_keys",
  "busy_timeout",
  "journal_size_limit",
] as const;
const tableNames = [
  "orchestration_events",
  "orchestration_v2_events",
  "orchestration_v2_command_receipts",
  "orchestration_v2_projection_threads",
  "orchestration_v2_projection_runs",
  "orchestration_v2_projection_messages",
] as const;

function recipeOf(input: CurrentFixtureOptions["recipe"]): CurrentFixtureRecipe {
  const recipe = {
    kind: "coherent-v2" as const,
    threads: 2,
    historyTurns: 3,
    payloadBytes: 256,
    ...input,
  };
  if (
    recipe.kind !== "coherent-v2" ||
    !Number.isSafeInteger(recipe.threads) ||
    recipe.threads < 1 ||
    recipe.threads > 16 ||
    !Number.isSafeInteger(recipe.historyTurns) ||
    recipe.historyTurns < 1 ||
    recipe.historyTurns > 256 ||
    !Number.isSafeInteger(recipe.payloadBytes) ||
    recipe.payloadBytes < 1 ||
    recipe.payloadBytes > 65536 ||
    recipe.threads * recipe.historyTurns * recipe.payloadBytes > 4 * 1024 * 1024
  )
    throw Object.assign(new Error("coherent-v2 fixture dimensions exceed the bounded recipe"), {
      code: "invalid_recipe",
    });
  return Object.freeze(recipe);
}

export async function produceCurrentFixture<A>(
  options: CurrentFixtureOptions,
  use: (context: CurrentFixtureContext) => A | Promise<A>,
): Promise<CurrentProductionResult<A>> {
  const source = assertCurrentDatabaseSource(options.databaseSource);
  if (
    source.worktreePath !== producerPath ||
    options.binding.repository !== source.repository ||
    options.binding.sourceRevision !== source.sourceRevision
  )
    throw Object.assign(
      new Error("current producer and binding must name this candidate checkout"),
      { code: "invalid_source" },
    );
  const recipe = recipeOf(options.recipe);
  const profile = options.profile ?? "health-offline-delete";
  if (!["health-offline-delete", "benchmark-wal"].includes(profile) || typeof use !== "function")
    throw Object.assign(new Error("named profile and callback required"), {
      code: "invalid_options",
    });
  if (options.signal !== undefined && !(options.signal instanceof AbortSignal))
    throw new Error("signal must be an AbortSignal");
  options.signal?.throwIfAborted();
  const filesystem = FS.statfsSync(options.parentPath);
  if ([0x01021994, 0x858458f6].includes(filesystem.type))
    throw new Error("current fixture requires disk-backed scratch");
  const owner = createOwnedRoot(options);
  const root = owner.creationReceipt.canonicalRootPath;
  const permit = assertOwnedDatabase(owner, {
    databaseRelativePath: "statev2.sqlite",
    access: "create",
  });
  const paths = { dbPath: permit.canonicalPath };
  // The receiving persistence loader runs both upstream and Jones migrations on the same client.
  const database = makeSqlitePersistenceLive(paths.dbPath).pipe(Layer.provide(NodeServices.layer));
  const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
    Layer.provideMerge(database),
  );
  const runtime = ManagedRuntime.make(
    Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores))).pipe(
      Layer.provide(Logger.layer([])),
    ),
  );
  const result: CurrentProductionResult<A> = { owner, closeKnown: false };
  const pending = new Set<Promise<unknown>>();
  const cancellation = new AbortController();
  const signal = options.signal
    ? AbortSignal.any([options.signal, cancellation.signal])
    : cancellation.signal;
  let open = true;
  const run: CurrentFixtureContext["run"] = (effect) => {
    if (!open) return Promise.reject(new Error("fixture context closed"));
    const promise = runtime.runPromise(effect, { signal });
    pending.add(promise);
    void promise.then(
      () => pending.delete(promise),
      () => pending.delete(promise),
    );
    return promise;
  };
  const query = (text: string) =>
    run(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return yield* sql.unsafe<Record<string, unknown>>(text);
      }),
    );
  const pragmas = async () => {
    const values: Record<string, string | number> = {};
    for (const name of pragmaNames) {
      const value = Object.values((await query(`PRAGMA ${name}`))[0] ?? {})[0];
      if (typeof value !== "string" && typeof value !== "number")
        throw new Error(`missing pragma ${name}`);
      values[name] = value;
    }
    return values;
  };
  const observations: { phase: string; pragmas: Record<string, string | number> }[] = [];
  const observe = async (phase: string) => {
    const observed = await pragmas();
    if (
      observed.journal_mode !== "wal" ||
      (observations[0] && JSON.stringify(observed) !== JSON.stringify(observations[0].pragmas))
    )
      throw new Error("receiving production durability pragmas changed");
    observations.push({ phase, pragmas: observed });
  };
  let capture: Omit<CurrentFixtureCapture, "profile"> | undefined;
  try {
    await Effect.runPromise(
      initializeV2Database(paths.dbPath).pipe(Effect.provide(NodeServices.layer)),
    );
    await observe("before-seed");
    await run(
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const now = DateTime.makeUnsafe("2026-10-02T12:00:00Z");
        const projectId = ProjectId.make("fixture-project");
        const providerInstanceId = ProviderInstanceId.make("codex");
        const modelSelection = { instanceId: providerInstanceId, model: "synthetic-model" };
        yield* sink.commitProjectCommand({
          commandId: CommandId.make("fixture-project-command"),
          projectId,
          commandType: "project.create",
          acceptedAt: now,
          event: {
            eventId: EventId.make("fixture-project-event"),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: "2026-10-02T12:00:00.000Z",
            commandId: CommandId.make("fixture-project-command"),
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.created",
            payload: {
              projectId,
              title: "Synthetic fixture",
              workspaceRoot: root,
              defaultModelSelection: modelSelection,
              scripts: [],
              createdAt: "2026-10-02T12:00:00.000Z",
              updatedAt: "2026-10-02T12:00:00.000Z",
            },
          },
        });
        for (let thread = 0; thread < recipe.threads; thread++) {
          const threadId = ThreadId.make(`fixture-thread-${thread}`);
          yield* sink.commitCommand({
            commandId: CommandId.make(`fixture-create-${thread}`),
            threadId,
            commandType: "thread.create",
            acceptedAt: now,
            effects: [],
            events: [
              {
                id: EventId.make(`fixture-thread-event-${thread}`),
                type: "thread.created",
                threadId,
                occurredAt: now,
                payload: {
                  createdBy: "user",
                  creationSource: "web",
                  id: threadId,
                  projectId,
                  title: `Synthetic ${thread}`,
                  providerInstanceId,
                  modelSelection,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  branch: null,
                  worktreePath: null,
                  activeProviderThreadId: null,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: threadId,
                  },
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                  archivedAt: null,
                  settledOverride: null,
                  settledAt: null,
                  lastVisitedAt: null,
                  deletedAt: null,
                },
              },
            ],
          });
          for (let turn = 0; turn < recipe.historyTurns; turn++) {
            const runId = RunId.make(`fixture-run-${thread}-${turn}`);
            const nodeId = NodeId.make(`fixture-node-${thread}-${turn}`);
            const userMessageId = MessageId.make(`fixture-user-${thread}-${turn}`);
            yield* sink.commitCommand({
              commandId: CommandId.make(`fixture-turn-${thread}-${turn}`),
              threadId,
              commandType: "thread.turn.start",
              acceptedAt: now,
              effects: [],
              events: [
                {
                  id: EventId.make(`fixture-run-event-${thread}-${turn}`),
                  type: "run.created",
                  threadId,
                  runId,
                  occurredAt: now,
                  payload: {
                    id: runId,
                    threadId,
                    ordinal: turn + 1,
                    providerInstanceId,
                    modelSelection,
                    providerThreadId: null,
                    userMessageId,
                    rootNodeId: nodeId,
                    activeAttemptId: null,
                    status: "completed",
                    requestedAt: now,
                    startedAt: now,
                    completedAt: now,
                    checkpointId: null,
                    contextHandoffId: null,
                  },
                },
                ...(["user", "assistant"] as const).map((role) => ({
                  id: EventId.make(`fixture-message-event-${thread}-${turn}-${role}`),
                  type: "message.updated" as const,
                  threadId,
                  runId,
                  nodeId,
                  occurredAt: now,
                  payload: {
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: role === "user" ? ("web" as const) : ("provider" as const),
                    id:
                      role === "user"
                        ? userMessageId
                        : MessageId.make(`fixture-assistant-${thread}-${turn}`),
                    threadId,
                    runId,
                    nodeId,
                    role,
                    text: "x".repeat(recipe.payloadBytes),
                    attachments: [],
                    streaming: false,
                    createdAt: now,
                    updatedAt: now,
                  },
                })),
              ],
            });
          }
          if (
            (yield* projections.getThread(threadId)).id !== threadId ||
            (yield* projections.getMessageCount(threadId)) !== recipe.historyTurns * 2
          )
            throw new Error("V2 fixture projection differs from seeded history");
        }
      }),
    );
    await observe("after-seed");
    await observe("before-callback");
    result.value = await use(Object.freeze({ owner, paths, databaseSource: source, recipe, run }));
    await observe("after-callback");
    const tables: Record<string, { status: "present"; count: number }> = {};
    for (const table of tableNames)
      tables[table] = {
        status: "present",
        count: Number((await query(`SELECT count(*) AS count FROM ${table}`))[0]?.count),
      };
    const ledgers: Record<string, { id: number; name: string }[]> = {};
    for (const table of ["effect_sql_migrations", "jones_sql_migrations"])
      ledgers[table] = (
        await query(`SELECT migration_id AS id, name FROM ${table} ORDER BY migration_id`)
      ).map((row) => ({ id: Number(row.id), name: String(row.name) }));
    const integrity = (await query("PRAGMA integrity_check")).map((row) =>
      String(Object.values(row)[0]),
    );
    const foreignKeys = await query("PRAGMA foreign_key_check");
    if (integrity.length !== 1 || integrity[0] !== "ok" || foreignKeys.length)
      throw new Error("synthetic fixture integrity or foreign keys failed");
    const runtimeObservation = {
      nodeVersion: process.versions.node,
      versions: { ...process.versions },
      sqliteVersion: String((await query("SELECT sqlite_version() AS version"))[0]?.version),
      profile,
      pragmas: observations[0]!.pragmas,
    };
    capture = {
      schema: "jones-performance-capture/v1",
      databaseSource: source,
      recipe,
      runtime: runtimeObservation,
      tables,
      ledgers,
      integrity: { results: integrity, ok: true },
      foreignKeys: { violations: 0, sha256: hash(foreignKeys) },
    };
    await observe("before-production-close");
    if (profile === "health-offline-delete") {
      const checkpoint = (await query("PRAGMA wal_checkpoint(TRUNCATE)"))[0];
      if (
        Number(checkpoint?.busy) !== 0 ||
        Object.values((await query("PRAGMA journal_mode=DELETE"))[0] ?? {})[0] !== "delete"
      )
        throw new Error("DELETE fixture maintenance failed");
    }
  } catch (error) {
    result.error = error instanceof Error ? error : new Error(String(error));
  } finally {
    open = false;
    cancellation.abort();
    await Promise.allSettled([...pending]);
    try {
      const closedProof = await observeSyntheticClose(owner, {
        permit,
        producerStep: options.binding.taskRef,
        resource: runtime,
        close: (resource) => resource.dispose(),
      });
      result.closeKnown = true;
      if (!result.error && capture) {
        let maintenance: CurrentFixtureCapture["profile"]["maintenance"];
        if (profile === "health-offline-delete") {
          const header = Buffer.alloc(100);
          const fd = FS.openSync(paths.dbPath, "r");
          let bytesRead: number;
          try {
            bytesRead = FS.readSync(fd, header, 0, 100, 0);
          } finally {
            FS.closeSync(fd);
          }
          const sidecars = {
            wal: FS.existsSync(`${paths.dbPath}-wal`),
            shm: FS.existsSync(`${paths.dbPath}-shm`),
            journal: FS.existsSync(`${paths.dbPath}-journal`),
          };
          if (
            bytesRead !== 100 ||
            header.toString("ascii", 0, 16) !== "SQLite format 3\0" ||
            header[18] !== 1 ||
            header[19] !== 1 ||
            Object.values(sidecars).some(Boolean)
          )
            throw new Error("closed DELETE header or sidecars differ");
          maintenance = {
            header: { bytesRead, writeVersion: header[18], readVersion: header[19] },
            sidecars,
          };
        }
        assertCurrentDatabaseSource(source);
        const custody = await sealSyntheticFixture(owner, {
          databaseRelativePath: permit.relativePath,
          producerStep: options.binding.taskRef,
          closedProof,
        });
        result.receipt = freezeEvidence({
          ...custody,
          schema: "jones-performance-fixture/v2" as const,
          producer: "current-v2" as const,
          databaseSource: source,
          runtime: capture.runtime,
        });
        result.receiptSha256 = hash(result.receipt);
        result.capture = freezeEvidence({
          ...capture,
          profile: {
            kind: profile,
            stage: "sealed" as const,
            productionClosed: true as const,
            productionObservations: observations,
            ...(maintenance ? { maintenance } : {}),
          },
        });
        result.captureSha256 = hash(result.capture);
      }
    } catch (error) {
      result.error ??= error instanceof Error ? error : new Error(String(error));
      result.retainReason = result.closeKnown ? "fixture_seal_failed" : "unknown_resource_close";
    }
  }
  return result;
}
