import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Ordinary from "../ordinaryCheckout/OrdinaryCheckout.ts";
import * as OrdinarySqlite from "../ordinaryCheckout/OrdinaryCheckoutSqlite.ts";
import { CheckoutTargetObservation } from "../ordinaryCheckout/CheckoutTarget.ts";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { make, ScratchCleanupExclusion } from "./ScratchCleanupAdmission.ts";
import { createRuntimeUseSchema } from "./RuntimeUseSchema.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import { digest, type RuntimeUseBinding, type RuntimeUseProducer } from "./RuntimeUseProtocol.ts";
import deletionMigration from "../persistence/Migrations/104_JonesDeletionAdmission.ts";

const target = {
  path: "/scratch/owned",
  storedPath: "/scratch/owned",
  projectRoot: "/scratch",
  threadId: "thread",
  projectId: "project",
};
const identity = { version: 1 as const, device: "1", inode: "2", birth: "3" };
const testLayer = NodeSqliteClient.layer({ filename: ":memory:" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);

// Isolated SQL fixture for the actual ordinary-owner reader. Filesystem identity
// and exclusion are explicit boundary fakes; these tests are no native race proof.
const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [table, column, extra] of [
    ["native_creation_intents", "worktree_path", ""],
    ["jones_native_workspace_admissions", "worktree_path", ""],
    ["worktree_ownership_leases", "resource_path", ""],
    ["jones_workspace_lifecycle_bindings", "worktree", ""],
    ["jones_workspace_lifecycle_transitions", "worktree", ""],
    ["jones_workspace_retirement_state", "worktree", ""],
  ])
    yield* sql.unsafe(`CREATE TABLE ${table} (${column} TEXT${extra})`);
  yield* deletionMigration;
  yield* sql.withTransaction(createRuntimeUseSchema);
  const runtime = yield* makeRuntimeUseStore;
  yield* sql`CREATE TABLE jones_sql_migrations(migration_id INTEGER)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT,project_id TEXT,payload_json TEXT)`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES('thread','project',${JSON.stringify({ worktreePath: target.path })})`;
  yield* sql`CREATE TABLE jones_ordinary_checkout_owners(checkout_key TEXT,project_root TEXT,directory_identity_json TEXT,git_common_directory TEXT,lease_id TEXT,owner_thread_id TEXT,birth_event_id TEXT,birth_sequence INTEGER,branch TEXT,acquired_at TEXT,renewed_at TEXT,expires_at TEXT,owner_epoch INTEGER,target_revision INTEGER,revision INTEGER,state TEXT)`;
  yield* sql`INSERT INTO jones_ordinary_checkout_owners VALUES(${target.path},'/scratch',${canonical({ version: 1, directory: identity, gitDirectory: null, gitDirectoryIdentity: null, gitCommonIdentity: null })},NULL,'lease','thread','birth',1,NULL,'2026-01-01T00:00:00.000Z','2026-01-01T00:00:00.000Z','2099-01-01T00:00:00.000Z',1,1,1,'owned')`;
  const ordinary = Context.get(yield* Layer.build(OrdinarySqlite.layer), Ordinary.OrdinaryCheckout);
  const writerObservations: boolean[] = [];
  let currentIdentity = identity;
  const admission = yield* make.pipe(
    Effect.provideService(Ordinary.OrdinaryCheckout, {
      ...ordinary,
      readOwner: (key) =>
        ordinary
          .readOwner(key)
          .pipe(
            Effect.tap(() =>
              Effect.serviceOption(sql.transactionService).pipe(
                Effect.tap((tx) => Effect.sync(() => writerObservations.push(Option.isSome(tx)))),
              ),
            ),
          ),
    }),
    Effect.provideService(CheckoutTargetObservation, {
      directory: (path) => Effect.sync(() => ({ realPath: path, identity: currentIdentity })),
      git: () => Effect.succeed(null),
    }),
  );
  return {
    sql,
    admission,
    runtime,
    writerObservations,
    replaceIdentity: () => {
      currentIdentity = { ...identity, inode: "new" };
    },
  };
});

it.effect("requires positive ordinary identity and an owning exclusion capability", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      expect((yield* f.admission.classify(target)).status).toBe("ordinary");
      let removed = 0;
      const reason = yield* f.admission.remove({
        target,
        revalidate: Effect.void,
        remove: Effect.sync(() => {
          removed++;
        }),
      });
      expect(reason).toContain("native-start and lease exclusion unavailable");
      expect(removed).toBe(0);
      expect(f.writerObservations.length).toBeGreaterThan(0);
      expect(f.writerObservations.every(Boolean)).toBe(true);
      yield* f.sql`DELETE FROM jones_ordinary_checkout_owners`;
      expect(yield* f.admission.classify(target)).toMatchObject({
        status: "held",
        reason: "scratch cleanup lacks positive ordinary ownership",
      });
    }),
  ).pipe(Effect.provide(testLayer)),
);

it.effect.each(["native", "lifecycle", "deletion", "identity"] as const)(
  "holds managed, unknown and changed-incarnation targets (%s)",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        if (kind === "native")
          yield* f.sql`INSERT INTO native_creation_intents VALUES(${target.path})`;
        if (kind === "lifecycle")
          yield* f.sql`INSERT INTO jones_workspace_lifecycle_bindings VALUES(${target.path})`;
        if (kind === "deletion")
          yield* f.sql`INSERT INTO jones_deletion_worktree_admissions VALUES('delete',${target.path},'{}','{}','started')`;
        if (kind === "identity") f.replaceIdentity();
        expect((yield* f.admission.classify(target)).status).toBe("held");
      }),
    ).pipe(Effect.provide(testLayer)),
);

it.effect.each([false, true])(
  "runs ordinary removal only after exclusion revalidation, native arrival=%s",
  (arrives) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        let removed = 0;
        const result = yield* f.admission
          .remove({
            target,
            revalidate: Effect.void,
            remove: Effect.sync(() => {
              removed++;
            }),
          })
          .pipe(
            Effect.provideService(ScratchCleanupExclusion, {
              // Explicit external owner fake exercises the adapter's exact callback
              // order. Native start/lease exclusion itself requires its later owner proof.
              removeOrdinary: ({ revalidate, remove }) =>
                Effect.gen(function* () {
                  // A fixture write failure is a test defect, not a port outcome.
                  if (arrives)
                    yield* f.sql`INSERT INTO native_creation_intents VALUES(${target.path})`.pipe(
                      Effect.orDie,
                    );
                  const checked = yield* revalidate.pipe(Effect.result);
                  if (checked._tag === "Failure")
                    return { status: "held" as const, reason: checked.failure.reason };
                  yield* remove;
                  return { status: "removed" as const };
                }),
            }),
          );
        expect(removed).toBe(arrives ? 0 : 1);
        expect(result).toBe(arrives ? "scratch cleanup overlaps native ownership" : null);
      }),
    ).pipe(Effect.provide(testLayer)),
);

// Actual SQLite history and production decoder; the physical target observation
// remains the explicit synthetic boundary documented by this fixture.
it.effect.each([
  "not_invoked",
  "complete",
  "captured_process_only",
  "logical_thread",
  "pending",
  "terminal",
  "native",
  "corrupt",
] as const)("discharges only positively verified ordinary runtime history (%s)", (kind) =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      const binding: RuntimeUseBinding = {
        schema: "jones.workspace-runtime-use/v1",
        useId: "original-use",
        canonicalPath: target.path,
        directoryIdentity: identity,
        threadId: target.threadId,
        actor:
          kind === "terminal"
            ? { kind: "terminal_spawn", terminalSessionId: "terminal" }
            : {
                kind: "provider_open",
                provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
              },
        original: {
          commandId: null,
          runId: null,
          attemptId: null,
          outboxReference: null,
          nativeReference: kind === "native" ? "native-original" : null,
          claim: null,
        },
      };
      yield* f.runtime.reserve(binding);
      if (kind === "not_invoked" || kind === "native" || kind === "corrupt") {
        yield* f.runtime.recordRetirement({
          schema: "jones.workspace-runtime-retirement/v1",
          useId: binding.useId,
          bindingSha256: digest(binding),
          outcome: "not_invoked",
          observationCount: 0,
        });
      } else if (kind !== "pending" && kind !== "terminal") {
        const producer: RuntimeUseProducer = {
          schema: "jones.workspace-runtime-producer/v1",
          useId: binding.useId,
          bindingSha256: digest(binding),
          producerId: "original-producer",
          generation: "generation",
          threadId: target.threadId,
          writerSetSha256: digest(["writer"]),
          coverage: kind,
        };
        yield* f.runtime.observe(producer);
        yield* f.runtime.recordRetirement({
          schema: "jones.workspace-runtime-retirement/v1",
          useId: binding.useId,
          bindingSha256: digest(binding),
          outcome: "producers_exited",
          observationCount: 1,
          nativeBindings: [],
          producers: [
            {
              producerId: producer.producerId,
              generation: producer.generation,
              observationSha256: digest(producer),
              driver: "codex",
              instanceId: "codex",
              providerSessionId: "session",
              threadId: target.threadId,
              pid: 1234,
              scopeUnit: "synthetic-original.scope",
              writerBindingsSha256: digest([]),
            },
          ],
        });
      }
      if (kind === "corrupt") {
        // Model damaged retained bytes after disabling this exact fixture trigger;
        // a retired state string must not hide failed digest verification.
        yield* f.sql`DROP TRIGGER jones_workspace_runtime_events_immutable`;
        yield* f.sql`UPDATE jones_workspace_runtime_use_events SET evidence_sha256=${"a".repeat(64)} WHERE ordinal=1`;
      }
      expect((yield* f.admission.classify(target)).status).toBe(
        kind === "not_invoked" || kind === "complete" ? "ordinary" : "held",
      );
    }),
  ).pipe(Effect.provide(testLayer)),
);
