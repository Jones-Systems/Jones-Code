import { assert, it } from "@effect/vitest";
import { EventId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as Runtime from "../persistence/ProviderSessionRuntime.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { HistoricalV1 } from "./legacy/LegacyV1ThreadImporter.ts";
import { makeLeaseCleanupLifecycle } from "./ThreadDeletion.ts";
import {
  makeWorktreeOwnershipLeaseStore,
  type WorktreeOwnershipLease,
} from "./WorktreeOwnershipLease.ts";
import * as ProviderSessions from "./ProviderSessionManager.ts";
import * as ProviderRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderIngestor from "./ProviderEventIngestor.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as McpRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import * as Terminals from "../terminal/Manager.ts";
import * as ProcessRunner from "../processRunner.ts";
import { LegacyLeaseInventoryError, type LegacyOwnerAbsencePort } from "./LegacyLeaseCleanup.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provideMerge(database)),
  ProjectionStore.layer.pipe(Layer.provideMerge(database)),
);
const testLayer = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  Runtime.layer.pipe(Layer.provide(database)),
);
const now = "2026-01-01T00:00:00.000Z";
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const absent: LegacyOwnerAbsencePort = (_owner, body) => body(Effect.void);
const seed = Effect.fnUntraced(function* (suffix = "orphan") {
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const leases = yield* makeWorktreeOwnershipLeaseStore();
  const threadId = ThreadId.make(`thread:legacy-lease:${suffix}`);
  const birthId = EventId.make(`event:legacy-lease:${suffix}`);
  const payload = yield* Schema.decodeUnknownEffect(HistoricalV1.threadCreated)({
    threadId,
    projectId: ProjectId.make("project:legacy-lease"),
    title: "Original",
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "original",
    worktreePath: `/workspace/${suffix}`,
    createdAt: now,
    updatedAt: now,
  }).pipe(Effect.orDie);
  const birth = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${birthId}, 'thread', ${threadId}, 1, 'thread.created', ${now}, 'user', ${yield* encodeJson(payload).pipe(Effect.orDie)}, '{}', 1) RETURNING sequence`;
  yield* sql`INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
    VALUES ('projection.threads', ${birth[0]!.sequence}, ${now}) ON CONFLICT(projector) DO UPDATE
    SET last_applied_sequence = excluded.last_applied_sequence`;
  const lease: WorktreeOwnershipLease = {
    resourcePath: payload.worktreePath!,
    leaseId: `lease:${suffix}`,
    ownerThreadId: threadId,
    ownerIncarnation: birthId,
    branch: payload.branch,
    acquiredAtMs: 1,
    renewedAtMs: 2,
    expiresAtMs: 3,
  };
  yield* sql`INSERT INTO worktree_ownership_leases
    (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
    VALUES (${lease.resourcePath}, ${lease.leaseId}, ${threadId}, ${birthId}, ${lease.branch}, 1, 2, 3)`;
  yield* (yield* Runtime.ProviderSessionRuntimeRepository).upsert({
    threadId,
    providerName: "codex",
    providerInstanceId: ProviderInstanceId.make("codex"),
    adapterKey: "codex",
    runtimeMode: "full-access",
    status: "stopped",
    lastSeenAt: now,
    resumeCursor: { threadId: `native:${suffix}` },
    runtimePayload: null,
  });
  const lifecycle = makeLeaseCleanupLifecycle({
    sink,
    leases,
    provider: { withLegacyOwnerAbsent: absent },
    terminal: { withLegacyOwnerAbsent: absent },
  });
  return {
    sql,
    sink,
    leases,
    lifecycle,
    threadId,
    birthId,
    payload,
    lease,
    sequence: birth[0]!.sequence,
  };
});

it.effect(
  "releases an authentic stopped-source startup orphan without fabricated deletion attribution",
  () =>
    Effect.gen(function* () {
      const { lifecycle, leases, lease, sql } = yield* seed();
      assert.equal((yield* lifecycle.cleanupLeaseOwner(lease)).status, "released");
      assert.deepEqual(yield* leases.listAll(), []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const addV2Birth = Effect.fnUntraced(function* (
  fixture: Effect.Success<ReturnType<typeof seed>>,
  imported: boolean,
) {
  const { sink, sql, threadId, payload } = fixture;
  const createdAt = DateTime.makeUnsafe(imported ? now : "2026-02-01T00:00:00.000Z");
  if (imported)
    yield* sql`INSERT INTO orchestration_v2_legacy_imports (thread_id, source_updated_at, shell_imported_at)
    VALUES (${threadId}, ${now}, ${now})`;
  yield* sink.write({
    events: [
      {
        id: EventId.make(
          imported ? `migration:v1:thread:${threadId}:created` : `replacement:${threadId}`,
        ),
        type: "thread.created",
        threadId,
        occurredAt: createdAt,
        payload: {
          id: threadId,
          projectId: payload.projectId,
          title: "Current",
          createdBy: "user",
          creationSource: "web",
          providerInstanceId: ProviderInstanceId.make("codex"),
          modelSelection: payload.modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: imported ? payload.branch : "replacement",
          worktreePath: imported ? payload.worktreePath : "/workspace/replacement",
          activeProviderThreadId: null,
          ...(imported ? { historyOrigin: "v1_import" as const } : {}),
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt,
          updatedAt: createdAt,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      },
    ],
  });
});

it.effect(
  "keeps an authentic attributed V1 deletion and releases only its stopped original lease",
  () =>
    Effect.gen(function* () {
      const { sql, lifecycle, lease, threadId, leases } = yield* seed();
      const rows = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES ('event:legacy:deleted', 'thread', ${threadId}, 2, 'thread.deleted', ${now}, 'command:legacy:delete', 'user',
        ${yield* encodeJson({ threadId, deletedAt: now }).pipe(Effect.orDie)}, '{}', 1) RETURNING sequence`;
      yield* sql`UPDATE projection_state SET last_applied_sequence = ${rows[0]!.sequence} WHERE projector = 'projection.threads'`;
      yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, status, result_sequence, error, accepted_at)
      VALUES ('command:legacy:delete', 'thread', ${threadId}, 'accepted', ${rows[0]!.sequence}, NULL, ${now})`;
      const before = yield* sql`SELECT * FROM orchestration_command_receipts`;
      assert.equal((yield* lifecycle.cleanupLeaseOwner(lease)).status, "released");
      assert.deepEqual(yield* leases.listAll(), []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, before);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("leaves a proved current V2 replacement projection and source unchanged", () =>
  Effect.gen(function* () {
    const fixture = yield* seed();
    yield* addV2Birth(fixture, false);
    const before = yield* fixture.sql`SELECT * FROM orchestration_v2_projection_threads`;
    const source = yield* fixture.sql`SELECT * FROM provider_session_runtime`;
    const basis = yield* fixture.sink.readLegacyLeaseCleanupBasis(fixture.lease);
    assert.equal(basis.status, "ready");
    if (basis.status !== "ready") return;
    assert.equal(basis.basis.owner.replacementBirth?.eventId, `replacement:${fixture.threadId}`);
    assert.equal((yield* fixture.lifecycle.cleanupLeaseOwner(fixture.lease)).status, "released");
    assert.deepEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_projection_threads`, before);
    assert.deepEqual(yield* fixture.sql`SELECT * FROM provider_session_runtime`, source);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

for (const scenario of [
  "stale-cut",
  "ambiguous-birth",
  "running-source",
  "missing-source",
  "synthetic-source",
  "live-import",
  "pending-path",
  "missing-producer",
] as const) {
  it.effect(`retains the original legacy lease for ${scenario}`, () =>
    Effect.gen(function* () {
      const f = yield* seed();
      if (scenario === "stale-cut")
        yield* f.sql`UPDATE projection_state SET last_applied_sequence = 0`;
      if (scenario === "ambiguous-birth")
        yield* f.sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES ('second-v1-birth', 'thread', ${f.threadId}, 2, 'thread.created', ${now}, 'user', ${yield* encodeJson(f.payload).pipe(Effect.orDie)}, '{}', 1)`;
      if (scenario === "running-source")
        yield* f.sql`UPDATE provider_session_runtime SET status = 'running'`;
      if (scenario === "missing-source") yield* f.sql`DELETE FROM provider_session_runtime`;
      if (scenario === "synthetic-source")
        yield* f.sql`UPDATE provider_session_runtime SET runtime_payload_json = '{"importOrigin":"native_import"}'`;
      if (scenario === "live-import") yield* addV2Birth(f, true);
      if (scenario === "pending-path")
        yield* f.sql`INSERT INTO orchestration_v2_worktree_path_admissions
      (operation_id, canonical_path, kind, subject_json, state, started_at, outcome_json, recorded_at, updated_at)
      VALUES ('pending-physical-removal', ${f.lease.resourcePath}, 'worktree_removal', '{}', 'reserved', NULL, NULL, ${now}, ${now})`;
      const lifecycle =
        scenario === "missing-producer"
          ? makeLeaseCleanupLifecycle({ sink: f.sink, leases: f.leases })
          : f.lifecycle;
      assert.equal((yield* lifecycle.reconcileLeaseOwners)[0]?.status, "retained");
      assert.deepEqual(yield* f.leases.listAll(), [f.lease]);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
  );
}

it.effect(
  "retains stale V2 projection state even when the current row looks like a replacement",
  () =>
    Effect.gen(function* () {
      const f = yield* seed();
      yield* addV2Birth(f, false);
      yield* f.sql`UPDATE orchestration_v2_projection_metadata SET last_sequence = 0`;
      assert.equal((yield* f.lifecycle.cleanupLeaseOwner(f.lease)).status, "retained");
      assert.deepEqual(yield* f.leases.listAll(), [f.lease]);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

for (const changed of ["lease", "source"] as const) {
  it.effect(`refuses release after the full ${changed} changes`, () =>
    Effect.gen(function* () {
      const f = yield* seed();
      const basis = yield* f.sink.readLegacyLeaseCleanupBasis(f.lease);
      assert.equal(basis.status, "ready");
      if (basis.status !== "ready") return;
      if (changed === "lease") yield* f.sql`UPDATE worktree_ownership_leases SET renewed_at_ms = 4`;
      else
        yield* f.sql`UPDATE provider_session_runtime SET last_seen_at = '2026-01-02T00:00:00.000Z'`;
      assert.equal(
        (yield* f.sink.releaseLegacyLeaseIfAbsent({
          basis: basis.basis,
          revalidateProvider: Effect.void,
          revalidateTerminals: Effect.void,
        })).status,
        "retained",
      );
      assert.equal((yield* f.leases.listAll()).length, 1);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
  );
}

for (const changed of ["inventory", "source"] as const) {
  it.effect(`rolls back deletion when ${changed} changes during the reserved callback`, () =>
    Effect.gen(function* () {
      const f = yield* seed();
      const basis = yield* f.sink.readLegacyLeaseCleanupBasis(f.lease);
      assert.equal(basis.status, "ready");
      if (basis.status !== "ready") return;
      let validations = 0;
      const revalidate = Effect.gen(function* () {
        if (++validations === 1) return;
        if (changed === "inventory")
          return yield* new LegacyLeaseInventoryError({ threadId: f.threadId, reason: "changed" });
        yield* f.sql`UPDATE provider_session_runtime SET status = 'running'`;
      });
      assert.equal(
        (yield* f.sink
          .releaseLegacyLeaseIfAbsent({
            basis: basis.basis,
            revalidateProvider: revalidate,
            revalidateTerminals: Effect.void,
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(yield* f.leases.listAll(), [f.lease]);
      assert.equal(
        (yield* f.sql<{ readonly status: string }>`SELECT status FROM provider_session_runtime`)[0]!
          .status,
        "stopped",
      );
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
  );
}

it.effect(
  "a malformed owner's source cannot prevent the next independent owner from releasing",
  () =>
    Effect.gen(function* () {
      const bad = yield* seed("a-malformed");
      const good = yield* seed("b-good");
      yield* bad.sql`UPDATE provider_session_runtime SET resume_cursor_json = 'broken-json' WHERE thread_id = ${bad.threadId}`;
      const result = yield* good.lifecycle.reconcileLeaseOwners;
      assert.equal(
        result.find((row) => row.lease.ownerThreadId === bad.threadId)?.status,
        "retained",
      );
      assert.equal(
        result.find((row) => row.lease.ownerThreadId === good.threadId)?.status,
        "released",
      );
      assert.deepEqual(yield* good.leases.listAll(), [bad.lease]);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const managerDependencies = Layer.mergeAll(
  testLayer,
  IdAllocator.layer,
  McpRegistryTestkit.layer,
  FileSystem.layerNoop({ makeDirectory: () => Effect.void }),
  Layer.succeed(ProviderRegistry.ProviderAdapterRegistryV2, {
    get: () => Effect.die("Cleanup must not look up or open a native provider"),
    list: () => Effect.succeed([]),
  }),
);
const composedLayer = Layer.mergeAll(
  ProviderSessions.layerWithOptions({ configureMcp: false }).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        managerDependencies,
        ProviderIngestor.layer.pipe(Layer.provide(managerDependencies)),
      ),
    ),
  ),
  Path.layer,
  Layer.succeed(ProcessRunner.ProcessRunner, {
    run: () => Effect.die("Cleanup must not run a native command"),
  }),
);

it.effect(
  "composes real provider and terminal absence reservations with authentic SQL release",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* seed();
        const provider = yield* ProviderSessions.ProviderSessionManagerV2;
        const terminal = yield* Terminals.makeWithOptions({
          logsDir: "/fixture/no-disk-terminal-logs",
          processTable: Effect.succeed([]),
          ptyAdapter: { spawn: () => Effect.die("Cleanup must not create a terminal") },
          env: {},
        });
        const lifecycle = makeLeaseCleanupLifecycle({
          sink: f.sink,
          leases: f.leases,
          provider,
          terminal,
        });
        assert.equal((yield* lifecycle.cleanupLeaseOwner(f.lease)).status, "released");
        assert.deepEqual(yield* f.leases.listAll(), []);
        assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      }),
    ).pipe(Effect.provide(Layer.fresh(composedLayer))),
);
