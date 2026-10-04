import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type Project,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as IdAllocator from "../orchestration-v2/IdAllocator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";

const projectId = ProjectId.make("agent-session-import-project");
const providerInstanceId = ProviderInstanceId.make("codex");
const providerSessionId = "native-codex-thread";
const threadId = ThreadId.make(`import:${providerInstanceId}:${providerSessionId}`);
const workspaceRoot = "/fixture/project";
const project: Project = {
  id: projectId,
  title: "Fixture project",
  workspaceRoot,
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-09-01T09:00:00.000Z",
  updatedAt: "2026-09-01T09:00:00.000Z",
  deletedAt: null,
};

function importable(
  source: "codex" | "claudeAgent" = "codex",
): Extract<AgentSessionScanner.AgentSessionRecentThread, { readonly _tag: "Importable" }> {
  const sessionId = source === "codex" ? providerSessionId : "94f9c7f3-8cc8-4ef3-9c82-f2d24842bd92";
  const instanceId = ProviderInstanceId.make(source);
  return {
    _tag: "Importable",
    source: {
      provider: source,
      providerInstanceId: instanceId,
      providerSessionId: sessionId,
      filePath: `/fixture/transcripts/${source}/${sessionId}.jsonl`,
      size: 100,
      mtimeMs: 2,
      device: 3,
      inode: 4,
      birthtimeMs: 1,
    },
    thread: {
      source,
      providerInstanceId: instanceId,
      providerSessionId: sessionId,
      title: `Imported ${source} thread`,
      model: source === "codex" ? "gpt-5.4" : "claude-opus-4-6",
      createdAt: "2026-09-01T10:00:00.000Z",
      updatedAt: "2026-09-01T10:01:00.000Z",
      messages: [
        { role: "user", text: "Fix it", createdAt: "2026-09-01T10:00:00.000Z" },
        { role: "assistant", text: "Fixed", createdAt: "2026-09-01T10:01:00.000Z" },
      ],
    },
  };
}

const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  ProviderSessionRuntime.layer,
).pipe(Layer.provideMerge(SqlitePersistenceMemory));
const persistence = EventSink.layer.pipe(Layer.provideMerge(stores));

function baseLayer(input?: {
  readonly outcomes?: ReadonlyArray<AgentSessionScanner.AgentSessionRecentThread>;
  readonly getOutcomes?: () => ReadonlyArray<AgentSessionScanner.AgentSessionRecentThread>;
  readonly getProject?: () => Option.Option<Project>;
  readonly onScan?: (root: string, completed: ReadonlyArray<unknown>) => void;
}) {
  return Layer.mergeAll(
    persistence,
    IdAllocator.layer,
    ThreadCommandExecutor.layer,
    Layer.mock(ProjectService.ProjectService)({
      getById: () => Effect.sync(() => input?.getProject?.() ?? Option.some(project)),
    }),
    Layer.succeed(
      AgentSessionScanner.AgentSessionScanner,
      AgentSessionScanner.AgentSessionScanner.of({
        scan: Effect.die("unused"),
        recentThreads: (root, completed = []) => {
          input?.onScan?.(root, completed);
          return Stream.fromIterable(input?.getOutcomes?.() ?? input?.outcomes ?? [importable()]);
        },
      }),
    ),
  );
}

const runImport = Effect.gen(function* () {
  const importer = yield* AgentSessionImporter.AgentSessionImporter;
  return yield* importer.importRecentAgentThreads({ projectId });
}).pipe(Effect.provide(AgentSessionImporter.layer));

const readDisposition = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly qualification_json: string;
    readonly evidence_json: string | null;
  }>`
    SELECT qualification_json, evidence_json FROM orchestration_v2_legacy_continuation_dispositions
    WHERE thread_id = ${threadId}
  `;
  return rows.map((row) => ({
    qualification: JSON.parse(row.qualification_json),
    evidence: row.evidence_json === null ? null : JSON.parse(row.evidence_json),
  }));
});

it.effect("imports messages once while synthetic stopped status remains unknown", () =>
  Effect.gen(function* () {
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    const first = yield* projections.getThreadProjection(threadId);
    assert.isNull(first.thread.activeProviderThreadId);
    assert.equal(first.thread.historyOrigin, "v1_import");
    assert.deepEqual(
      first.messages.map((message) => message.text),
      ["Fix it", "Fixed"],
    );
    assert.isTrue(
      first.messages.every((message) => message.runId === null && message.nodeId === null),
    );
    assert.deepEqual(first.providerThreads, []);
    assert.deepEqual(first.runs, []);
    assert.deepEqual(first.attempts, []);
    assert.deepEqual(first.plans, []);
    assert.deepEqual(first.runtimeRequests, []);
    assert.deepEqual(first.providerSessions, []);
    const runtime = Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }));
    assert.deepEqual(runtime.resumeCursor, { threadId: providerSessionId });
    assert.equal(runtime.status, "stopped");
    const dispositions = yield* readDisposition;
    assert.equal(dispositions[0]?.qualification.type, "unknown");
    assert.isNull(dispositions[0]?.evidence.stoppedProof);
    const sink = yield* EventSink.EventSinkV2;
    const firstSequence = yield* sink.latestSequence({ threadId });
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    assert.equal(yield* sink.latestSequence({ threadId }), firstSequence);
    assert.deepEqual((yield* projections.getThreadProjection(threadId)).messages, first.messages);
  }).pipe(Effect.provide(baseLayer())),
);

for (const source of ["codex", "claudeAgent"] as const) {
  it.effect(
    `uses the project root and preserves the ${source} historical resume cursor without launching`,
    () => {
      const scanned: string[] = [];
      const outcome = importable(source);
      const importedId = ThreadId.make(
        `import:${outcome.source.providerInstanceId}:${outcome.source.providerSessionId}`,
      );
      return Effect.gen(function* () {
        assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
        const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        const runtime = Option.getOrThrow(yield* runtimes.getByThreadId({ threadId: importedId }));
        assert.deepEqual(
          runtime.resumeCursor,
          source === "codex"
            ? { threadId: outcome.source.providerSessionId }
            : { threadId: importedId, resume: outcome.source.providerSessionId },
        );
        assert.equal(runtime.providerInstanceId, outcome.source.providerInstanceId);
        assert.deepEqual(scanned, [workspaceRoot]);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        assert.deepEqual((yield* projections.getThreadProjection(importedId)).providerSessions, []);
      }).pipe(
        Effect.provide(baseLayer({ outcomes: [outcome], onScan: (root) => scanned.push(root) })),
      );
    },
  );
}

it.effect("rejects a changed project root before scanning or writing", () => {
  let scans = 0;
  return Effect.gen(function* () {
    const error = yield* Effect.gen(function* () {
      const importer = yield* AgentSessionImporter.AgentSessionImporter;
      return yield* importer.importRecentAgentThreads({
        projectId,
        expectedWorkspaceRoot: "/fixture/old-root",
      });
    }).pipe(Effect.provide(AgentSessionImporter.layer), Effect.flip);
    assert.equal(error._tag, "AgentSessionImportProjectChangedError");
    assert.equal(scans, 0);
    assert.deepEqual(yield* readDisposition, []);
  }).pipe(Effect.provide(baseLayer({ onScan: () => scans++ })));
});

it.effect("counts scanner skips without writing a thread or binding", () =>
  Effect.gen(function* () {
    assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
    assert.deepEqual(yield* readDisposition, []);
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    assert.deepEqual(yield* runtimes.list(), []);
  }).pipe(Effect.provide(baseLayer({ outcomes: [{ _tag: "Skipped" }] }))),
);

it.effect(
  "passes committed source identity to a restarted scanner without replacing history",
  () => {
    const completed: Array<ReadonlyArray<unknown>> = [];
    return Effect.gen(function* () {
      yield* runImport;
      yield* runImport;
      assert.lengthOf(completed, 2);
      assert.deepEqual(completed[0], []);
      assert.deepEqual(completed[1], [importable().source]);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.lengthOf((yield* projections.getThreadProjection(threadId)).messages, 2);
    }).pipe(Effect.provide(baseLayer({ onScan: (_root, sources) => completed.push(sources) })));
  },
);

it.effect("records a source marker for completed history that predates transcript metadata", () =>
  Effect.gen(function* () {
    yield* runImport;
    const sql = yield* SqlClient.SqlClient;
    // This completed import predates persisted transcript source metadata.
    yield* sql`UPDATE provider_session_runtime
      SET runtime_payload_json = json_remove(runtime_payload_json, '$.importedTranscripts')
      WHERE thread_id = ${threadId}`;
    const sink = yield* EventSink.EventSinkV2;
    const sequence = yield* sink.latestSequence({ threadId });
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    assert.equal(yield* sink.latestSequence({ threadId }), sequence);
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    const runtime = Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }));
    assert.deepEqual(runtime.resumeCursor, { threadId: providerSessionId });
    assert.deepEqual(runtime.runtimePayload, {
      cwd: workspaceRoot,
      importOrigin: "native_import",
      importedTranscripts: [importable().source],
    });
  }).pipe(Effect.provide(baseLayer())),
);

it.effect("preserves history with an invalid source model and keeps continuation unknown", () => {
  const outcome = importable();
  return Effect.gen(function* () {
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const imported = yield* projections.getThreadProjection(threadId);
    assert.isNotEmpty(imported.thread.modelSelection.model.trim());
    assert.isNull(imported.thread.activeProviderThreadId);
    assert.deepEqual((yield* readDisposition)[0]?.qualification, {
      type: "unknown",
      reason: "historical_model_invalid",
    });
  }).pipe(
    Effect.provide(
      baseLayer({ outcomes: [{ ...outcome, thread: { ...outcome.thread, model: "   " } }] }),
    ),
  );
});

it.effect("rechecks the project root after scanning before committing an import", () => {
  let changed = false;
  return Effect.gen(function* () {
    assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
    assert.deepEqual(yield* readDisposition, []);
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    assert.deepEqual(yield* runtimes.list(), []);
  }).pipe(
    Effect.provide(
      baseLayer({
        getProject: () =>
          Option.some(changed ? { ...project, workspaceRoot: "/fixture/replaced-root" } : project),
        onScan: () => {
          changed = true;
        },
      }),
    ),
  );
});

it.effect("does not replace completed history or an active binding on retry", () =>
  Effect.gen(function* () {
    yield* runImport;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    const previous = Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }));
    yield* runtimes.upsert({
      ...previous,
      status: "running",
      resumeCursor: { threadId: "current-active-native" },
    });
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const before = yield* projections.getThreadProjection(threadId);
    const updatedAt = DateTime.makeUnsafe("2026-09-02T00:00:00.000Z");
    yield* sink.write({
      events: [
        {
          id: EventId.make("import-retry:metadata"),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: updatedAt,
          payload: { ...before.thread, title: "Renamed", updatedAt },
        },
      ],
    });
    const sequence = yield* sink.latestSequence({ threadId });
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    assert.equal(yield* sink.latestSequence({ threadId }), sequence);
    assert.equal((yield* projections.getThreadProjection(threadId)).thread.title, "Renamed");
    assert.deepEqual(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId })).resumeCursor, {
      threadId: "current-active-native",
    });
  }).pipe(Effect.provide(baseLayer())),
);

it.effect("skips malformed Claude ids without creating history or a binding", () => {
  const outcome = importable("claudeAgent");
  const malformed = { ...outcome, thread: { ...outcome.thread, providerSessionId: "malformed" } };
  return Effect.gen(function* () {
    assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    assert.deepEqual(yield* runtimes.list(), []);
  }).pipe(Effect.provide(baseLayer({ outcomes: [malformed] })));
});

for (const change of ["wrong-project", "native-history"] as const) {
  it.effect(`preserves an existing ${change} thread collision`, () =>
    Effect.gen(function* () {
      yield* runImport;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const before = yield* projections.getThreadProjection(threadId);
      yield* sink.write({
        events: [
          {
            id: EventId.make(`import-collision:${change}`),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: before.thread.updatedAt,
            payload: {
              ...before.thread,
              ...(change === "wrong-project"
                ? { projectId: ProjectId.make("another-project") }
                : { historyOrigin: "native" as const }),
            },
          },
        ],
      });
      const sequence = yield* sink.latestSequence({ threadId });
      assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
      assert.equal(yield* sink.latestSequence({ threadId }), sequence);
    }).pipe(Effect.provide(baseLayer())),
  );
}

it.effect(
  "rolls back an interrupted import and retries without a partial thread or source marker",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER fail_native_import_source BEFORE UPDATE ON provider_session_runtime
      WHEN NEW.runtime_payload_json LIKE '%importedTranscripts%'
      BEGIN SELECT RAISE(ABORT, 'injected transcript marker failure'); END`;
      assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
      assert.deepEqual(yield* readDisposition, []);
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      assert.deepEqual(yield* runtimes.list(), []);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.isNull(yield* projections.getThreadShell(threadId));
      const sink = yield* EventSink.EventSinkV2;
      assert.equal(yield* sink.latestSequence({ threadId }), 0);
      yield* sql`DROP TRIGGER fail_native_import_source`;
      assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
      assert.deepEqual(
        (yield* projections.getThreadProjection(threadId)).messages.map((message) => message.text),
        ["Fix it", "Fixed"],
      );
    }).pipe(Effect.provide(baseLayer())),
);

for (const accessibilitySource of ["historical_store", "native_read"] as const) {
  it.effect(
    `qualifies native import only with a genuine stopped source row and ${accessibilitySource} evidence`,
    () => {
      const driver = ProviderDriverKind.make("codex");
      const key = "codex:home:/fixture/historical-home";
      const sourceRow: ProviderSessionRuntime.ProviderSessionRuntime = {
        threadId,
        providerName: driver,
        providerInstanceId: null,
        adapterKey: driver,
        runtimeMode: "full-access",
        status: "stopped",
        lastSeenAt: "2026-09-01T10:01:00.000Z",
        resumeCursor: { threadId: providerSessionId },
        runtimePayload: { cwd: workspaceRoot },
      };
      const evidence = new Map([
        [
          threadId,
          {
            sourceRow,
            driver,
            nativeThreadId: providerSessionId,
            continuationKey: key,
            historicalSourceIdentity: {
              storeIdentity: "fixture-old-store",
              sourceHomeIdentity: "/fixture/historical-home",
            },
            accessibility: {
              providerInstanceId,
              driver,
              nativeThreadId: providerSessionId,
              continuationKey: key,
              source: accessibilitySource,
            },
            target: {
              providerInstanceId,
              driver,
              continuationKey: key,
              supportsNativeResume: true,
            },
          },
        ],
      ]);
      return Effect.gen(function* () {
        assert.deepEqual(
          yield* runImport.pipe(
            Effect.provideService(
              ProviderSessionRuntime.LegacyProviderContinuationInputsV1,
              evidence,
            ),
          ),
          { importedCount: 1, skippedCount: 0 },
        );
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const projection = yield* projections.getThreadProjection(threadId);
        assert.isNotNull(projection.thread.activeProviderThreadId);
        assert.lengthOf(projection.providerThreads, 1);
        assert.equal(projection.providerThreads[0]?.status, "not_loaded");
        assert.isNull(projection.providerThreads[0]?.providerSessionId);
        assert.deepEqual(projection.providerSessions, []);
        assert.deepEqual(projection.runs, []);
        const dispositions = yield* readDisposition;
        assert.equal(dispositions[0]?.qualification.type, "qualified");
        assert.isNull(dispositions[0]?.evidence.providerInstanceId);
        assert.equal(dispositions[0]?.evidence.stoppedProof.source, "persisted_runtime_row");
        assert.equal(dispositions[0]?.evidence.accessibility.source, accessibilitySource);
      }).pipe(Effect.provide(baseLayer()));
    },
  );
}

const readSeal = (id: ThreadId = threadId) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    return yield* sink.readNativeImportTranscriptSeal(id);
  });

it.effect("does not manufacture a snapshot seal from historical retry markers", () => {
  const outcome = importable();
  return Effect.gen(function* () {
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    yield* runtimes.upsert({
      threadId,
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId,
      adapterKey: "codex",
      runtimeMode: "full-access",
      status: "stopped",
      lastSeenAt: outcome.thread.updatedAt,
      resumeCursor: { threadId: providerSessionId },
      runtimePayload: { cwd: workspaceRoot, importOrigin: "native_import" },
    });
    yield* runtimes.recordImportedTranscript({ threadId, source: outcome.source });
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    assert.isNull(yield* readSeal());
    const sink = yield* EventSink.EventSinkV2;
    assert.equal(yield* sink.latestSequence({ threadId }), 0);
  }).pipe(
    Effect.provide(
      baseLayer({
        outcomes: [
          { _tag: "AlreadyImported", source: outcome.source },
          {
            _tag: "Duplicate",
            source: { ...outcome.source, filePath: "/fixture/transcripts/copy.jsonl" },
          },
        ],
      }),
    ),
  );
});

for (const source of ["codex", "claudeAgent"] as const) {
  it.effect(`seals the exact ${source} imported snapshot without qualifying continuation`, () => {
    const outcome = importable(source);
    const id = ThreadId.make(
      `import:${outcome.source.providerInstanceId}:${outcome.source.providerSessionId}`,
    );
    return Effect.gen(function* () {
      yield* runImport;
      const seal = yield* readSeal(id);
      assert.isNotNull(seal);
      assert.equal(seal?.provenance, "native_import");
      assert.equal(seal?.parserPolicy, "agent_session_visible_messages_v1");
      assert.deepEqual(seal?.source, outcome.source);
      assert.equal(seal?.messageCount, 2);
      assert.lengthOf(seal!.eventBasis, 4);
      assert.match(seal!.eventsSha256, /^[0-9a-f]{64}$/);
      assert.equal(seal?.birth.eventId, `agent-session-import:v2:thread:${id}:created`);
      assert.isTrue(
        seal!.eventBasis.every(
          (entry, index, basis) =>
            entry.sequence > (index === 0 ? seal!.birth.sequence : basis[index - 1]!.sequence),
        ),
      );
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.isNull((yield* projections.getThreadProjection(id)).thread.activeProviderThreadId);
      yield* runImport;
      assert.deepEqual(yield* readSeal(id), seal);
    }).pipe(Effect.provide(baseLayer({ outcomes: [outcome] })));
  });
}

it.effect("rolls back a seal insertion failure and seals the same input on retry", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER fail_native_import_seal BEFORE INSERT ON orchestration_v2_native_import_transcript_seals
      BEGIN SELECT RAISE(ABORT, 'injected snapshot seal failure'); END`;
    assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
    assert.isNull(yield* readSeal());
    assert.deepEqual(yield* readDisposition, []);
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    assert.deepEqual(yield* runtimes.list(), []);
    const sink = yield* EventSink.EventSinkV2;
    assert.equal(yield* sink.latestSequence({ threadId }), 0);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    assert.isNull(yield* projections.getThreadShell(threadId));
    yield* sql`DROP TRIGGER fail_native_import_seal`;
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    assert.equal((yield* readSeal())?.messageCount, 2);
  }).pipe(Effect.provide(baseLayer())),
);

it.effect("does not publish a sealed import when its enclosing transaction rolls back", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const published: number[] = [];
    const observed = EventStore.EventStoreV2.of({
      ...eventStore,
      publishCommitted: (events) =>
        Effect.sync(() => void published.push(events.length)).pipe(
          Effect.andThen(eventStore.publishCommitted(events)),
        ),
    });
    const enclosingSink = yield* Effect.gen(function* () {
      return yield* EventSink.EventSinkV2;
    }).pipe(
      Effect.provide(EventSink.layer),
      Effect.provideService(EventStore.EventStoreV2, observed),
    );
    const importer = yield* Effect.gen(function* () {
      return yield* AgentSessionImporter.AgentSessionImporter;
    }).pipe(
      Effect.provide(AgentSessionImporter.layer),
      Effect.provideService(EventSink.EventSinkV2, enclosingSink),
    );
    yield* enclosingSink
      .withTransaction(
        Effect.gen(function* () {
          assert.deepEqual(yield* importer.importRecentAgentThreads({ projectId }), {
            importedCount: 1,
            skippedCount: 0,
          });
          assert.equal(
            (yield* enclosingSink.readNativeImportTranscriptSeal(threadId))?.messageCount,
            2,
          );
          return yield* Effect.fail("injected outer rollback");
        }),
      )
      .pipe(Effect.flip);
    assert.deepEqual(published, []);
    assert.isNull(yield* readSeal());
    assert.equal(yield* sink.latestSequence({ threadId }), 0);
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    assert.deepEqual(yield* runtimes.list(), []);
    assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
    assert.equal((yield* readSeal())?.messageCount, 2);
  }).pipe(Effect.provide(baseLayer())),
);

for (const mismatch of ["provider", "instance", "session"] as const) {
  it.effect(`rolls back a parser source ${mismatch} mismatch instead of sealing it`, () => {
    const outcome = importable();
    const invalid = {
      ...outcome,
      source: {
        ...outcome.source,
        ...(mismatch === "provider"
          ? { provider: "claudeAgent" as const }
          : mismatch === "instance"
            ? { providerInstanceId: ProviderInstanceId.make("other-codex") }
            : { providerSessionId: "other-native-thread" }),
      },
    };
    return Effect.gen(function* () {
      assert.deepEqual(yield* runImport, { importedCount: 0, skippedCount: 1 });
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(
        yield* sql`SELECT thread_id FROM orchestration_v2_native_import_transcript_seals`,
        [],
      );
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      assert.deepEqual(yield* runtimes.list(), []);
    }).pipe(Effect.provide(baseLayer({ outcomes: [invalid] })));
  });
}

for (const mismatch of ["text", "timestamp", "ordinal", "pair", "order"] as const) {
  it.effect(
    `returns unknown when the sealed ${mismatch} no longer matches the ordered event payload`,
    () =>
      Effect.gen(function* () {
        yield* runImport;
        assert.isNotNull(yield* readSeal());
        const sql = yield* SqlClient.SqlClient;
        const messageId = `agent-session-import:v2:message:${threadId}:000000`;
        const itemId = `agent-session-import:v2:turn-item:${threadId}:000000`;
        if (mismatch === "text") {
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.text', 'Changed with the same count')
          WHERE event_id IN (${messageId}, ${itemId})`;
        } else if (mismatch === "timestamp") {
          yield* sql`UPDATE orchestration_events SET occurred_at = '2026-09-03T00:00:00.000Z' WHERE event_id = ${messageId}`;
        } else if (mismatch === "ordinal") {
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.ordinal', 2) WHERE event_id = ${itemId}`;
        } else if (mismatch === "pair") {
          yield* sql`UPDATE orchestration_events SET event_type = 'message.updated' WHERE event_id = ${itemId}`;
        } else {
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.text', 'Fixed')
          WHERE event_id IN (${messageId}, ${itemId})`;
          yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.text', 'Fix it')
          WHERE event_id IN (${`agent-session-import:v2:message:${threadId}:000001`}, ${`agent-session-import:v2:turn-item:${threadId}:000001`})`;
        }
        assert.isNull(yield* readSeal());
      }).pipe(Effect.provide(baseLayer())),
  );
}

it.effect(
  "rejects reordered seal events even when every event id was written in the import transaction",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const reordered = EventSink.EventSinkV2.of({
        ...sink,
        recordNativeImportTranscriptSeal: (input) =>
          sink.recordNativeImportTranscriptSeal({
            ...input,
            messageEvents: [...input.messageEvents].reverse(),
          }),
      });
      assert.deepEqual(
        yield* runImport.pipe(Effect.provideService(EventSink.EventSinkV2, reordered)),
        { importedCount: 0, skippedCount: 1 },
      );
      assert.isNull(yield* readSeal());
      assert.equal(yield* sink.latestSequence({ threadId }), 0);
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      assert.deepEqual(yield* runtimes.list(), []);
    }).pipe(Effect.provide(baseLayer())),
);

it.effect("keeps a changed source retry unknown without replacing the sealed snapshot", () => {
  const outcome = importable();
  let changed = false;
  return Effect.gen(function* () {
    yield* runImport;
    const seal = yield* readSeal();
    assert.isNotNull(seal);
    changed = true;
    yield* runImport;
    assert.isNull(yield* readSeal());
    const sql = yield* SqlClient.SqlClient;
    const stored = yield* sql<{
      readonly events_sha256: string;
    }>`SELECT events_sha256 FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = ${threadId}`;
    assert.deepEqual(stored, [{ events_sha256: seal!.eventsSha256 }]);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    assert.deepEqual(
      (yield* projections.getThreadProjection(threadId)).messages.map((message) => message.text),
      ["Fix it", "Fixed"],
    );
  }).pipe(
    Effect.provide(
      baseLayer({
        getOutcomes: () => [
          changed
            ? {
                ...outcome,
                source: { ...outcome.source, size: 101 },
                thread: {
                  ...outcome.thread,
                  messages: [
                    {
                      role: "user" as const,
                      text: "Changed source",
                      createdAt: outcome.thread.createdAt,
                    },
                  ],
                },
              }
            : outcome,
        ],
      }),
    ),
  );
});

const scanSyntheticTranscript = (
  source: "codex" | "claudeAgent",
  records: (cwd: string) => ReadonlyArray<unknown>,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const base = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3code-import-snapshot-" });
    const cwd = path.join(base, "project");
    const claudeHome = path.join(base, "claude-home");
    const codexHome = path.join(base, "codex-home");
    yield* fileSystem.makeDirectory(cwd, { recursive: true });
    yield* fileSystem.makeDirectory(claudeHome, { recursive: true });
    yield* fileSystem.makeDirectory(codexHome, { recursive: true });
    const session =
      source === "codex" ? providerSessionId : importable("claudeAgent").source.providerSessionId;
    const filePath =
      source === "codex"
        ? path.join(codexHome, "sessions", "2026", "09", "01", "rollout-fixture.jsonl")
        : path.join(claudeHome, "projects", "fixture-project", `${session}.jsonl`);
    yield* fileSystem.makeDirectory(path.dirname(filePath), { recursive: true });
    yield* fileSystem.writeFileString(
      filePath,
      records(cwd)
        .map((record) => JSON.stringify(record))
        .join("\n") + "\n",
    );
    const mtimeMs = Date.parse("2026-09-01T12:00:00.000Z");
    yield* fileSystem.utimes(filePath, mtimeMs / 1000, mtimeMs / 1000);
    yield* TestClock.setTime(mtimeMs + 1000);
    const outcomes = yield* Effect.gen(function* () {
      const scanner = yield* AgentSessionScanner.AgentSessionScanner;
      return Array.from(yield* scanner.recentThreads(cwd).pipe(Stream.runCollect));
    }).pipe(
      Effect.provide(
        AgentSessionScanner.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              ServerSettings.layerTest({
                providers: {
                  codex: { homePath: codexHome },
                  claudeAgent: { homePath: claudeHome },
                },
                providerInstances: {
                  [ProviderInstanceId.make("codex")]: {
                    driver: ProviderDriverKind.make("codex"),
                    config: { homePath: codexHome },
                  },
                  [ProviderInstanceId.make("claudeAgent")]: {
                    driver: ProviderDriverKind.make("claudeAgent"),
                    config: { homePath: claudeHome },
                  },
                },
              }),
              // Project discovery excludes the server's base directory and its descendants.
              ServerConfig.layerTest(base, path.join(base, "server-config")),
              Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
            ),
          ),
        ),
      ),
    );
    const outcome = outcomes.find(
      (
        item,
      ): item is Extract<
        AgentSessionScanner.AgentSessionRecentThread,
        { readonly _tag: "Importable" }
      > => item._tag === "Importable",
    );
    assert.isDefined(outcome);
    return { outcome: outcome!, fixtureProject: { ...project, workspaceRoot: cwd } };
  });

it.effect(
  "seals the parser's first user and latest 199 messages rather than the whole raw transcript",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* scanSyntheticTranscript("codex", (cwd) => [
          { type: "session_meta", payload: { id: providerSessionId, cwd } },
          { type: "event_msg", payload: { type: "user_message", message: "First user" } },
          ...Array.from({ length: 204 }, (_, index) => ({
            type: "response_item",
            payload: {
              type: "message",
              role: "assistant",
              content: [{ type: "output_text", text: `Reply ${index}` }],
            },
          })),
        ]);
        assert.lengthOf(fixture.outcome.thread.messages, 200);
        assert.equal(fixture.outcome.thread.messages[0]?.text, "First user");
        assert.equal(fixture.outcome.thread.messages[1]?.text, "Reply 5");
        assert.equal(fixture.outcome.thread.messages[199]?.text, "Reply 203");
        yield* Effect.gen(function* () {
          assert.deepEqual(yield* runImport, { importedCount: 1, skippedCount: 0 });
          const seal = yield* readSeal();
          assert.equal(seal?.messageCount, 200);
          assert.lengthOf(seal!.eventBasis, 400);
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const messages = (yield* projections.getThreadProjection(threadId)).messages;
          assert.deepEqual(
            messages.map(({ role, text }) => ({ role, text })),
            fixture.outcome.thread.messages.map(({ role, text }) => ({ role, text })),
          );
          assert.isTrue(
            messages.every(
              (message) => DateTime.formatIso(message.createdAt) === "2026-09-01T12:00:00.000Z",
            ),
          );
        }).pipe(
          Effect.provide(
            baseLayer({
              outcomes: [fixture.outcome],
              getProject: () => Option.some(fixture.fixtureProject),
            }),
          ),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

for (const source of ["codex", "claudeAgent"] as const) {
  it.effect(`seals actual ${source} parser filtering and normalized timestamps`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = importable(source).source.providerSessionId;
        const fixture = yield* scanSyntheticTranscript(source, (cwd) =>
          source === "codex"
            ? [
                { type: "session_meta", payload: { id: session, cwd } },
                {
                  type: "response_item",
                  timestamp: "2026-09-01T08:00:00-04:00",
                  payload: {
                    type: "message",
                    role: "user",
                    content: [{ type: "input_text", text: "Keep this prompt" }],
                  },
                },
                {
                  type: "event_msg",
                  timestamp: "2026-09-01T12:00:00.000Z",
                  payload: { type: "user_message", message: "Keep this prompt" },
                },
                {
                  type: "response_item",
                  timestamp: "bad",
                  payload: {
                    type: "message",
                    role: "assistant",
                    content: [{ type: "output_text", text: "Kept reply" }],
                  },
                },
                {
                  type: "response_item",
                  payload: {
                    type: "reasoning",
                    content: [{ type: "text", text: "Discard reasoning" }],
                  },
                },
              ]
            : [
                {
                  type: "user",
                  cwd,
                  sessionId: session,
                  timestamp: "2026-09-01T08:00:00-04:00",
                  message: { content: "Keep this prompt" },
                },
                {
                  type: "assistant",
                  sessionId: session,
                  timestamp: "bad",
                  message: { content: "Kept reply" },
                },
                { type: "user", isSidechain: true, message: { content: "Discard sidechain" } },
                { type: "user", isMeta: true, message: { content: "Discard metadata" } },
                { type: "user", isCompactSummary: true, message: { content: "Discard summary" } },
              ],
        );
        assert.deepEqual(
          fixture.outcome.thread.messages.map((message) => message.text),
          ["Keep this prompt", "Kept reply"],
        );
        assert.isTrue(
          fixture.outcome.thread.messages.every(
            (message) => message.createdAt === "2026-09-01T12:00:00.000Z",
          ),
        );
        yield* Effect.gen(function* () {
          yield* runImport;
          const id = ThreadId.make(
            `import:${fixture.outcome.source.providerInstanceId}:${session}`,
          );
          const seal = yield* readSeal(id);
          assert.equal(seal?.messageCount, 2);
          assert.deepEqual(seal?.source, fixture.outcome.source);
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          assert.deepEqual(
            (yield* projections.getThreadProjection(id)).messages.map((message) => message.text),
            ["Keep this prompt", "Kept reply"],
          );
        }).pipe(
          Effect.provide(
            baseLayer({
              outcomes: [fixture.outcome],
              getProject: () => Option.some(fixture.fixtureProject),
            }),
          ),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
}
