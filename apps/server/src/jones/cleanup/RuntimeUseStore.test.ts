import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import {
  canonical,
  digest,
  type RuntimeUseBinding,
  type RuntimeUseProducer,
} from "./RuntimeUseProtocol.ts";
import { createRuntimeUseSchema } from "./RuntimeUseSchema.ts";
import { makeRuntimeUseStore } from "./RuntimeUseStore.ts";
import deletionMigration from "../persistence/Migrations/104_JonesDeletionAdmission.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const binding: RuntimeUseBinding = {
  schema: "jones.workspace-runtime-use/v1",
  useId: "original-open",
  canonicalPath: "/synthetic/worktree",
  threadId: "thread",
  directoryIdentity: { version: 1, device: "device", inode: "inode", birth: "birth" },
  actor: {
    kind: "provider_open",
    provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
  },
  original: {
    commandId: null,
    runId: null,
    attemptId: null,
    outboxReference: null,
    nativeReference: null,
    claim: null,
  },
};
const producer = (coverage: RuntimeUseProducer["coverage"] = "complete"): RuntimeUseProducer => ({
  schema: "jones.workspace-runtime-producer/v1",
  useId: binding.useId,
  bindingSha256: digest(binding),
  producerId: "captured-child",
  generation: "generation-1",
  threadId: "thread",
  writerSetSha256: digest(["writer-1"]),
  coverage,
});
const retiredProducer = (
  original: RuntimeUseProducer,
  writerBindings: readonly unknown[] = [],
) => ({
  producerId: original.producerId,
  generation: original.generation,
  observationSha256: digest(original),
  driver: "codex" as const,
  instanceId: "codex",
  providerSessionId: "session",
  threadId: original.threadId,
  pid: 1234,
  scopeUnit: `synthetic-${original.producerId}.scope`,
  writerBindingsSha256: digest(writerBindings),
});
const nativeBinding = (
  generation = "generation-1",
  producerId = "captured-child",
  providerThreadId = "provider-thread-1",
  nativeThreadId = "native-thread-1",
) => ({
  schema: "jones.workspace-runtime-native-binding/v1",
  useId: binding.useId,
  bindingSha256: digest(binding),
  producerId,
  generation,
  binding: {
    threadId: "thread",
    providerThreadId,
    providerSessionId: "session",
    providerInstanceId: "codex",
    driver: "codex",
    nativeThreadId,
    runtimeGeneration: generation,
  },
});
const notInvoked = {
  schema: "jones.workspace-runtime-retirement/v1",
  useId: binding.useId,
  bindingSha256: digest(binding),
  outcome: "not_invoked",
  observationCount: 0,
} as const;
const fixture = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* deletionMigration;
    yield* sql.withTransaction(createRuntimeUseSchema);
    return yield* effect;
  }).pipe(Effect.provide(memory));
const refused = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

describe("workspace runtime use journal persistence", () => {
  it.effect(
    "an original use cannot borrow another app thread's otherwise complete claim in decode or SQL",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          const sql = yield* SqlClient.SqlClient;
          const claimed: RuntimeUseBinding = {
            ...binding,
            useId: "same-thread-claimed-use",
            original: {
              commandId: "synthetic-command",
              runId: null,
              attemptId: null,
              outboxReference: "synthetic-effect",
              nativeReference: null,
              claim: {
                schema: "jones.workspace-runtime-outbox-claim/v1",
                effectId: "synthetic-effect",
                commandId: "synthetic-command",
                threadId: binding.threadId,
                workerId: "synthetic-worker",
                attemptCount: 1,
                claimedAt: "2026-11-04T00:00:00.000Z",
                runId: null,
                attemptId: null,
                requestSha256: "a".repeat(64),
                nativeReference: null,
              },
            },
          };
          const wrong: RuntimeUseBinding = {
            ...claimed,
            useId: "cross-thread-claimed-use",
            threadId: "other-thread",
          };
          yield* refused(store.reserve(wrong));
          yield* refused(sql`INSERT INTO jones_workspace_runtime_uses VALUES
      (${wrong.useId},${wrong.canonicalPath},${wrong.threadId},'provider_open','session',NULL,
        ${canonical(wrong)},${digest(wrong)},'pending',0,NULL)`);
          assert.isNull(yield* store.read(wrong.useId));
          assert.isTrue((yield* store.reserve(claimed)).created);
          assert.deepStrictEqual((yield* store.read(claimed.useId))?.binding, claimed);
        }),
      ),
  );
  it("retains the independently derived canonical original-binding digest", () => {
    assert.strictEqual(
      digest(binding),
      "e00e559c18cf25dde77b6f800a048495f08289e37c7471d2a8a6f57fa0a7492c",
    );
  });
  it.effect(
    "native identity binds only to its original observed producer, once, without rebinding another session",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(binding);
          yield* refused(store.observeNativeBinding(nativeBinding()));
          yield* store.observe(producer("captured_process_only"));
          const first = yield* store.observeNativeBinding(nativeBinding());
          assert.deepStrictEqual(yield* store.observeNativeBinding(nativeBinding()), first);
          for (const changed of [
            { ...nativeBinding(), producerId: "foreign-producer" },
            { ...nativeBinding(), generation: "foreign-generation" },
            {
              ...nativeBinding(),
              binding: { ...nativeBinding().binding, nativeThreadId: "other-native-thread" },
            },
            {
              ...nativeBinding(),
              binding: { ...nativeBinding().binding, providerSessionId: "other-session" },
            },
            {
              ...nativeBinding(),
              binding: { ...nativeBinding().binding, providerInstanceId: "other-instance" },
            },
            {
              ...nativeBinding(),
              binding: { ...nativeBinding().binding, threadId: "other-app-thread" },
            },
          ])
            yield* refused(store.observeNativeBinding(changed));
          assert.deepStrictEqual((yield* store.read(binding.useId))?.nativeBindings, [
            nativeBinding(),
          ]);
        }),
      ),
  );
  it.effect(
    "final writer evidence preserves the full interleaved native-binding and producer history",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          const first = producer("captured_process_only");
          const second = { ...first, producerId: "producer-2", generation: "generation-2" };
          const a = nativeBinding();
          const b = nativeBinding(
            second.generation,
            second.producerId,
            "provider-thread-2",
            "native-thread-2",
          );
          yield* store.reserve(binding);
          yield* store.observe(first);
          yield* store.observeNativeBinding(a);
          yield* store.observe(second);
          yield* store.observeNativeBinding(b);
          const retirement = {
            schema: "jones.workspace-runtime-retirement/v1",
            useId: binding.useId,
            bindingSha256: digest(binding),
            outcome: "producers_exited",
            observationCount: 4,
            producers: [retiredProducer(first, [a]), retiredProducer(second, [b])],
            nativeBindings: [a, b],
          };
          yield* refused(store.recordRetirement({ ...retirement, nativeBindings: [a] }));
          yield* refused(store.recordRetirement({ ...retirement, nativeBindings: [b, a] }));
          yield* refused(
            store.recordRetirement({
              ...retirement,
              producers: [retiredProducer(first), retiredProducer(second, [b])],
            }),
          );
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          assert.strictEqual((yield* store.read(binding.useId))?.lastOrdinal, 4);
          const completed = yield* store.recordRetirement(retirement);
          assert.strictEqual(completed?.state, "retired");
          assert.deepStrictEqual(completed?.nativeBindings, [a, b]);
          assert.deepStrictEqual(yield* store.recordRetirement(retirement), completed);
        }),
      ),
  );
  it.effect(
    "pending use vetoes same, parent and descendant deletion statements while a prefix sibling stays independent",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          const sql = yield* SqlClient.SqlClient;
          yield* store.reserve(binding);
          for (const target of [
            binding.canonicalPath,
            "/synthetic",
            binding.canonicalPath + "/nested",
          ])
            yield* refused(
              sql`INSERT INTO jones_deletion_worktree_admissions VALUES(${target},${target},'{}','{}','started')`,
            );
          yield* sql`INSERT INTO jones_deletion_worktree_admissions VALUES('sibling','/synthetic/worktree-other','{}','{}','unknown')`;
          assert.deepStrictEqual(
            yield* sql`SELECT effect_id FROM jones_deletion_worktree_admissions`,
            [{ effect_id: "sibling" }],
          );
          assert.deepStrictEqual(yield* store.pending(), [binding]);
        }),
      ),
  );
  it.effect(
    "a durable original deletion vetoes overlapping new uses in their actual insert transaction",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          const sql = yield* SqlClient.SqlClient;
          yield* sql`INSERT INTO jones_deletion_worktree_admissions VALUES('original',${binding.canonicalPath},'{}','{}','unknown')`;
          for (const canonicalPath of [
            binding.canonicalPath,
            "/synthetic",
            binding.canonicalPath + "/nested",
          ])
            yield* refused(store.reserve({ ...binding, canonicalPath }));
          assert.deepStrictEqual(yield* store.pending(), []);
          yield* store.reserve({ ...binding, canonicalPath: "/synthetic/worktree-other" });
          assert.strictEqual((yield* store.pending()).length, 1);
        }),
      ),
  );
  it.effect(
    "reserves one original identity and never grants replay from a duplicate pending record",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          const first = yield* store.reserve(binding);
          assert.isTrue(first.created);
          assert.strictEqual(first.record.state, "pending");
          const second = yield* store.reserve(binding);
          assert.isFalse(second.created);
          assert.deepStrictEqual(second.record, first.record);
          assert.deepStrictEqual(yield* store.pending(), [binding]);
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* sql`SELECT use_id,ordinal,action FROM jones_workspace_runtime_use_events`,
            [{ use_id: binding.useId, ordinal: 0, action: "reserved" }],
          );
        }),
      ),
  );
  it.effect(
    "changed original checkout, session, instance, thread or command cannot reuse an ID",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(binding);
          for (const changed of [
            { ...binding, canonicalPath: "/other" },
            { ...binding, threadId: "replacement" },
            {
              ...binding,
              directoryIdentity: { ...binding.directoryIdentity, inode: "replacement" },
            },
            {
              ...binding,
              actor: {
                kind: "provider_open" as const,
                provider: { sessionId: "new", instanceId: "codex", driver: "codex" },
              },
            },
            {
              ...binding,
              actor: {
                kind: "provider_open" as const,
                provider: { sessionId: "session", instanceId: "other", driver: "codex" },
              },
            },
            { ...binding, original: { ...binding.original, commandId: "different" } },
          ])
            yield* refused(store.reserve(changed));
          assert.deepStrictEqual(yield* store.pending(), [binding]);
        }),
      ),
  );
  it.effect(
    "exact decoders reject excess fields, malformed discriminants and aliased path spellings",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          yield* refused(store.reserve({ ...binding, unexpected: true }));
          yield* refused(
            store.reserve({
              ...binding,
              actor: { ...binding.actor, terminalSessionId: "terminal" },
            }),
          );
          for (const canonicalPath of [
            "relative",
            "/synthetic/../worktree",
            "/synthetic/./worktree",
            "/synthetic/worktree/",
          ])
            yield* refused(store.reserve({ ...binding, canonicalPath }));
          assert.deepStrictEqual(yield* store.pending(), []);
        }),
      ),
  );
  it.effect("reservation and its event roll back together on owning transaction failure", () =>
    fixture(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const store = yield* makeRuntimeUseStore;
        yield* refused(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* store.reserve(binding);
              return yield* Effect.fail("synthetic transaction failure");
            }),
          ),
        );
        assert.strictEqual(yield* store.read(binding.useId), null);
        assert.deepStrictEqual(yield* sql`SELECT * FROM jones_workspace_runtime_use_events`, []);
      }),
    ),
  );
  it.effect("retains uncertain use when a fresh service instance reads the same database", () =>
    fixture(
      Effect.gen(function* () {
        yield* (yield* makeRuntimeUseStore).reserve(binding);
        const recovered = yield* makeRuntimeUseStore;
        assert.isFalse((yield* recovered.reserve(binding)).created);
        assert.deepStrictEqual(yield* recovered.pending(), [binding]);
      }),
    ),
  );
  it.effect(
    "observations are append-only, exact duplicates are idempotent and generation aliases fail closed",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(binding);
          const first = yield* store.observe(producer());
          assert.deepStrictEqual(yield* store.observe(producer()), first);
          yield* refused(store.observe({ ...producer(), writerSetSha256: digest(["different"]) }));
          yield* refused(store.observe({ ...producer(), producerId: "replacement" }));
          yield* refused(store.observe({ ...producer(), bindingSha256: digest("other") }));
          assert.strictEqual((yield* store.read(binding.useId))?.producers.length, 1);
        }),
      ),
  );
  it.effect(
    "retires only the exact complete recorded producer set, with identical completion idempotence",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(binding);
          const second = { ...producer(), producerId: "child-2", generation: "generation-2" };
          yield* store.observe(producer());
          yield* store.observe(second);
          const complete = {
            schema: "jones.workspace-runtime-retirement/v1" as const,
            useId: binding.useId,
            bindingSha256: digest(binding),
            outcome: "producers_exited" as const,
            observationCount: 2,
            producers: [retiredProducer(producer()), retiredProducer(second)],
            nativeBindings: [],
          };
          yield* refused(
            store.recordRetirement({
              ...complete,
              observationCount: 1,
              producers: [retiredProducer(producer())],
            }),
          );
          yield* refused(
            store.recordRetirement({
              ...complete,
              producers: [retiredProducer(second), retiredProducer(producer())],
            }),
          );
          const record = yield* store.recordRetirement(complete);
          assert.strictEqual(record?.state, "retired");
          assert.deepStrictEqual(yield* store.recordRetirement(complete), record);
          yield* refused(store.recordRetirement(notInvoked));
          assert.deepStrictEqual(yield* store.pending(), []);
        }),
      ),
  );
  it.effect(
    "logical thread closure or captured-process-only coverage cannot retire a physical open",
    () =>
      fixture(
        Effect.gen(function* () {
          const store = yield* makeRuntimeUseStore;
          for (const coverage of ["logical_thread", "captured_process_only"] as const) {
            const own = { ...binding, useId: coverage };
            const observation = {
              ...producer(coverage),
              useId: own.useId,
              bindingSha256: digest(own),
            };
            yield* store.reserve(own);
            yield* store.observe(observation);
            yield* refused(
              store.recordRetirement({
                schema: "jones.workspace-runtime-retirement/v1",
                useId: own.useId,
                bindingSha256: digest(own),
                outcome: "producers_exited",
                observationCount: 1,
                producers: [observation],
                nativeBindings: [],
              }),
            );
            assert.strictEqual((yield* store.read(own.useId))?.state, "pending");
          }
        }),
      ),
  );
  it.effect("not-invoked persistence is one-way and cannot follow a producer observation", () =>
    fixture(
      Effect.gen(function* () {
        const store = yield* makeRuntimeUseStore;
        yield* store.reserve(binding);
        yield* store.recordRetirement(notInvoked);
        assert.strictEqual((yield* store.read(binding.useId))?.state, "retired");
        const replacement = { ...binding, useId: "replacement" };
        yield* store.reserve(replacement);
        yield* refused(store.observe(producer()));
        yield* store.observe({
          ...producer(),
          useId: replacement.useId,
          bindingSha256: digest(replacement),
        });
        yield* refused(
          store.recordRetirement({
            ...notInvoked,
            useId: replacement.useId,
            bindingSha256: digest(replacement),
          }),
        );
        assert.deepStrictEqual(yield* store.pending(), [replacement]);
        const inventory = yield* store.inventory();
        assert.deepStrictEqual(
          inventory.map((record) => record.binding.useId),
          [binding.useId, replacement.useId],
        );
        assert.deepStrictEqual(
          inventory.map((record) => record.state),
          ["retired", "pending"],
        );
        assert.deepStrictEqual(inventory[0]?.retirement, notInvoked);
        assert.deepStrictEqual(inventory[1]?.producers, [
          {
            ...producer(),
            useId: replacement.useId,
            bindingSha256: digest(replacement),
          },
        ]);
      }),
    ),
  );
  it.effect(
    "direct delete, replace, rebinding, forged state update and event rewrite are rejected",
    () =>
      fixture(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(binding);
          for (const statement of [
            "DELETE FROM jones_workspace_runtime_uses",
            "DELETE FROM jones_workspace_runtime_use_events",
            "UPDATE jones_workspace_runtime_uses SET canonical_path='/other'",
            "UPDATE jones_workspace_runtime_uses SET state='retired',retirement_json='{}',last_ordinal=1",
            "UPDATE jones_workspace_runtime_use_events SET evidence_json='{}'",
            "INSERT OR REPLACE INTO jones_workspace_runtime_uses SELECT * FROM jones_workspace_runtime_uses",
            "INSERT OR REPLACE INTO jones_workspace_runtime_use_events SELECT * FROM jones_workspace_runtime_use_events",
          ])
            yield* refused(sql.unsafe(statement));
          assert.deepStrictEqual(yield* store.pending(), [binding]);
        }),
      ),
  );
  it.effect(
    "the event trigger atomically transitions the matching current row, and rollback preserves pending",
    () =>
      fixture(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(binding);
          yield* refused(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* store.recordRetirement(notInvoked);
                assert.strictEqual((yield* store.read(binding.useId))?.state, "retired");
                return yield* Effect.fail("rollback");
              }),
            ),
          );
          assert.strictEqual((yield* store.read(binding.useId))?.state, "pending");
          assert.deepStrictEqual(
            yield* sql`SELECT ordinal FROM jones_workspace_runtime_use_events`,
            [{ ordinal: 0 }],
          );
        }),
      ),
  );
  it.effect(
    "schema installation rolls back all objects on failure and rejects unexpected existing objects",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* deletionMigration;
        yield* refused(
          sql.withTransaction(createRuntimeUseSchema.pipe(Effect.andThen(Effect.fail("rollback")))),
        );
        assert.deepStrictEqual(
          yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'jones_workspace_runtime_%'`,
          [],
        );
        yield* sql`CREATE TABLE jones_workspace_runtime_use_events (unexpected TEXT)`;
        yield* refused(sql.withTransaction(createRuntimeUseSchema));
        assert.deepStrictEqual(
          yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'jones_workspace_runtime_%'`,
          [{ name: "jones_workspace_runtime_use_events" }],
        );
      }).pipe(Effect.provide(memory)),
  );
  it.effect(
    "storage digest mismatch remains a refusal even when its JSON is syntactically valid",
    () =>
      fixture(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const actor = binding.actor;
          assert.notStrictEqual(actor.kind, "terminal_spawn");
          yield* sql`INSERT INTO jones_workspace_runtime_uses VALUES
        (${binding.useId},${binding.canonicalPath},${binding.threadId},'provider_open','session',NULL,
          ${canonical(binding)},${digest("wrong")},'pending',0,NULL)`;
          const store = yield* makeRuntimeUseStore;
          yield* refused(store.pending());
          yield* refused(store.reserve(binding));
        }),
      ),
  );
});
