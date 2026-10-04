// @effect-diagnostics nodeBuiltinImport:off - Static architecture test scans source files.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";
import {
  ApplicationProjectCreatedPayload,
  ApplicationProjectMetaUpdatedPayload,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  OrchestrationV2Command,
  OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { HistoricalV1 } from "./legacy/LegacyV1ThreadImporter.ts";

const sourceRoot = NodePath.resolve(import.meta.dirname, "..");
const forbiddenImport =
  /from\s+["'][^"']*(?:ProviderService|ProviderSessionDirectory|ProviderSessionReaper|ProviderCommandReactor|ProviderRuntimeIngestion)[^"']*["']/;
// The V1 read model tables. `projection_projects` is not listed: it is the V2 project store.
const legacyTable =
  /\bprojection_(?:threads|thread_messages|thread_activities|thread_proposed_plans|thread_pull_requests|thread_sessions|turns|pending_approvals|state)\b/;
/** Directories whose files may read the V1 tables: the importer and the schema history. */
const legacyReaders = ["orchestration-v2/legacy/", "persistence/Migrations/"] as const;
/**
 * Individual files allowed to read the V1 tables, each with its reason. Keep this
 * list short; each SQL exception must remain within its named source boundary.
 */
const legacyReaderFiles: Record<string, string> = {
  // Provider history for settings migration reads V1 thread sessions once at load.
  "serverSettings.ts": "one-time provider history for settings migration",
  "orchestration-v2/EventSink.ts":
    "same-snapshot imported-start basis and qualified application attachment source inventory",
};
const retiredPaths = [
  "orchestration",
  "orchestration/Layers/ProviderCommandReactor.ts",
  "orchestration/Layers/ProviderRuntimeIngestion.ts",
  "orchestration/Services/ProviderCommandReactor.ts",
  "orchestration/Services/ProviderRuntimeIngestion.ts",
  "persistence/Services/ProjectionThreads.ts",
  "persistence/Services/ProjectionProjects.ts",
] as const;

function productionTypeScriptFiles(directory: string): ReadonlyArray<string> {
  return NodeFS.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = NodePath.join(directory, entry.name);
    if (entry.isDirectory()) return productionTypeScriptFiles(path);
    return entry.isFile() && entry.name.endsWith(".ts") && !entry.name.includes(".test.")
      ? [path]
      : [];
  });
}

const relativeSources = productionTypeScriptFiles(sourceRoot).map((path) => ({
  path: NodePath.relative(sourceRoot, path).split(NodePath.sep).join("/"),
  source: NodeFS.readFileSync(path, "utf8"),
}));

it("keeps the V1 agent runtime and engine deleted", () => {
  for (const relativePath of retiredPaths) {
    assert.isFalse(NodeFS.existsSync(NodePath.join(sourceRoot, relativePath)), relativePath);
  }
  const violations = relativeSources
    .filter(({ path, source }) => !path.includes("/legacy/") && forbiddenImport.test(source))
    .map(({ path }) => path);
  assert.deepEqual(violations, []);
});

it("reads V1 tables only through declared legacy source boundaries", () => {
  const readers = relativeSources
    .filter(({ source }) => legacyTable.test(source))
    .map(({ path }) => path)
    .filter(
      (path) =>
        !legacyReaders.some((directory) => path.startsWith(directory)) &&
        legacyReaderFiles[path] === undefined,
    );
  assert.deepEqual(readers, []);
  // The allowlist must not outlive the reads it excuses.
  for (const path of Object.keys(legacyReaderFiles)) {
    const file = relativeSources.find((candidate) => candidate.path === path);
    assert.isTrue(file !== undefined && legacyTable.test(file.source), path);
  }
});

it("bounds EventSink V1 reads to imported-start facts and qualified application attachment source collection", () => {
  const sink = relativeSources.find(({ path }) => path === "orchestration-v2/EventSink.ts")!;
  const ranges = [
    ["const readCommitSnapshot =", "const readNativeCommandFactsEffect ="],
    [
      "const collectImportedApplicationAttachmentInventoryEffect =",
      "const prepareImportedApplicationAttachmentInventoryEffect =",
    ],
  ].map(([startName, endName]) => {
    const start = sink.source.indexOf(startName!);
    const end = sink.source.indexOf(endName!, start);
    assert.isAtLeast(start, 0, startName);
    assert.isAbove(end, start, endName);
    return { start, end, source: sink.source.slice(start, end) };
  });
  // This exception belongs to the sole SQL producer's private source boundaries;
  // it grants no raw-table access to projection consumers or dispatch callers.
  for (const match of sink.source.matchAll(new RegExp(legacyTable.source, "g"))) {
    assert.isTrue(
      ranges.some(({ start, end }) => match.index >= start && match.index < end),
      match[0],
    );
  }
  assert.include(ranges[0]!.source, "records.legacy_source_threads");
  assert.include(ranges[0]!.source, "records.legacy_source_messages");
  assert.include(ranges[1]!.source, "SELECT * FROM projection_thread_messages");
  assert.include(ranges[1]!.source, "SELECT * FROM projection_thread_activities");
  assert.include(
    ranges[1]!.source,
    "SELECT projector, last_applied_sequence FROM projection_state",
  );
  assert.include(ranges[1]!.source, "legacy_application_source_cut_incomplete");
  assert.include(ranges[1]!.source, "legacy_application_source_birth_unavailable");
});

it("keeps the legacy importer out of reach of new code", () => {
  const importers = relativeSources
    .filter(
      ({ path, source }) =>
        !path.startsWith("orchestration-v2/legacy/") && /from\s+["'][^"']*\/legacy\//.test(source),
    )
    .map(({ path }) => path)
    .toSorted();
  // Startup imports pending transcripts, the V2 runtime wires the importer, and
  // thread and project services hydrate a V1 transcript before they act on it.
  // Both historical importers use the same pure source-qualification boundary.
  // Orchestrator captures only the read-only transcript parity callback for the
  // imported-start preparation factory; raw V1 tables remain in the importer
  // and the separately bounded EventSink source-qualification functions.
  assert.deepEqual(importers, [
    "orchestration-v2/Orchestrator.ts",
    "orchestration-v2/ThreadManagementService.ts",
    "orchestration-v2/runtimeLayer.ts",
    "project/AgentSessionImporter.ts",
    "project/ProjectService.ts",
    "serverRuntimeStartup.ts",
  ]);
  const orchestrator = relativeSources.find(
    ({ path }) => path === "orchestration-v2/Orchestrator.ts",
  )!;
  assert.isFalse(legacyTable.test(orchestrator.source));
  assert.equal(orchestrator.source.match(/\bLegacyV1ThreadImporter\b/g)?.length, 3);
  const factoryStart = orchestrator.source.indexOf(
    "export const makeImportedHistoryStartExecutionPreparationV2 =",
  );
  const factoryEnd = orchestrator.source.indexOf(
    "export interface OrchestratorV2Shape",
    factoryStart,
  );
  assert.isAtLeast(factoryStart, 0);
  assert.isAbove(factoryEnd, factoryStart);
  const factory = orchestrator.source.slice(factoryStart, factoryEnd);
  assert.include(factory, "const importer = yield* LegacyV1ThreadImporter;");
  assert.include(
    factory,
    "readLegacyTranscript: importer.readTranscriptSnapshotEvidence(choice.threadId)",
  );
  assert.notInclude(factory, "ensureTranscript");
});

const historicalAt = "2026-01-01T00:00:00.000Z";
const historicalThread = {
  id: "thread-1",
  projectId: "project-1",
  title: "Historical thread",
  modelSelection: { provider: "codex", model: "gpt-5.4" },
  branch: null,
  worktreePath: null,
  latestTurn: null,
  createdAt: historicalAt,
  updatedAt: historicalAt,
  messages: [],
};
const isV2Event = Schema.is(OrchestrationV2DomainEvent);

it("decodes historical project.created payloads with a default provider", () => {
  const parsed = Schema.decodeUnknownSync(ApplicationProjectCreatedPayload)({
    projectId: "project-1",
    title: "Project title",
    workspaceRoot: "/workspace/project",
    defaultModelSelection: { provider: "codex", model: "gpt-5.4" },
    scripts: [],
    createdAt: historicalAt,
    updatedAt: historicalAt,
  });
  assert.equal(parsed.defaultModelSelection?.instanceId, "codex");
});

it("decodes project.meta-updated payloads with explicit default provider", () => {
  const parsed = Schema.decodeUnknownSync(ApplicationProjectMetaUpdatedPayload)({
    projectId: "project-1",
    defaultModelSelection: { provider: "claudeAgent", model: "claude-opus-4-6" },
    updatedAt: historicalAt,
  });
  assert.equal(parsed.defaultModelSelection?.instanceId, "claudeAgent");
});

it("decodes thread.created runtime mode for historical events", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.threadCreated)({
    ...historicalThread,
    threadId: "thread-1",
  });
  assert.equal(parsed.runtimeMode, DEFAULT_RUNTIME_MODE);
  assert.equal(parsed.interactionMode, DEFAULT_PROVIDER_INTERACTION_MODE);
  assert.equal(parsed.modelSelection.instanceId, "codex");
});

it("defaults settled fields when decoding historical thread data", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.thread)(historicalThread);
  assert.isNull(parsed.settledOverride);
  assert.isNull(parsed.settledAt);
  assert.deepEqual(parsed.pullRequests, []);
  assert.deepEqual(parsed.proposedPlans, []);
});

it("decodes orchestration session runtime mode defaults", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.session)({
    threadId: "thread-1",
    status: "idle",
    providerName: null,
    activeTurnId: null,
    lastError: null,
    updatedAt: historicalAt,
  });
  assert.equal(parsed.runtimeMode, DEFAULT_RUNTIME_MODE);
  assert.isUndefined(parsed.providerInstanceId);
  assert.isFalse("runtimeIdentity" in parsed);
});

it("decodes stored message events written before turnId existed", () => {
  // A stored event that fails to decode stops the event store read, and with it server startup, so rows written before turnId existed must still load.
  const parsed = Schema.decodeUnknownSync(HistoricalV1.messageSent)({
    threadId: "thread-1",
    messageId: "message-1",
    role: "assistant",
    text: "Stored answer",
    streaming: false,
    createdAt: historicalAt,
    updatedAt: historicalAt,
  });
  assert.isNull(parsed.turnId);
  assert.isFalse(isV2Event({ type: "thread.message-sent", payload: parsed }));
});

it("defaults proposed plan implementation metadata for historical rows", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.proposedPlan)({
    id: "plan-1",
    turnId: "turn-1",
    planMarkdown: "# Plan",
    createdAt: historicalAt,
    updatedAt: historicalAt,
  });
  assert.isNull(parsed.implementedAt);
  assert.isNull(parsed.implementationThreadId);
  assert.isFalse(isV2Event({ type: "thread.proposed-plan-upserted", payload: parsed }));
});

it("preserves proposed plan implementation metadata when present", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.proposedPlan)({
    id: "plan-2",
    turnId: "turn-2",
    planMarkdown: "# Plan",
    implementedAt: "2026-01-02T00:00:00.000Z",
    implementationThreadId: "thread-2",
    createdAt: historicalAt,
    updatedAt: "2026-01-02T00:00:00.000Z",
  });
  assert.equal(parsed.implementedAt, "2026-01-02T00:00:00.000Z");
  assert.equal(parsed.implementationThreadId, "thread-2");
});

it("decodes thread.turn-start-requested defaults for provider, runtime mode, and interaction mode", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.turnStartRequested)({
    threadId: "thread-1",
    messageId: "message-1",
    createdAt: historicalAt,
  });
  assert.isUndefined(parsed.modelSelection);
  assert.equal(parsed.runtimeMode, DEFAULT_RUNTIME_MODE);
  assert.equal(parsed.interactionMode, DEFAULT_PROVIDER_INTERACTION_MODE);
  assert.isUndefined(parsed.sourceProposedPlan);
});

it("decodes thread.turn-start-requested source proposed plan metadata when present", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.turnStartRequested)({
    threadId: "thread-2",
    messageId: "message-2",
    sourceProposedPlan: { threadId: "thread-1", planId: "plan-1" },
    createdAt: historicalAt,
  });
  assert.deepEqual(parsed.sourceProposedPlan, { threadId: "thread-1", planId: "plan-1" });
  assert.isFalse(isV2Event({ type: "thread.turn-start-requested", payload: parsed }));
});

it("decodes thread.turn-start-requested title seed when present", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.turnStartRequested)({
    threadId: "thread-2",
    messageId: "message-2",
    titleSeed: "Investigate reconnect failures",
    createdAt: historicalAt,
  });
  assert.equal(parsed.titleSeed, "Investigate reconnect failures");
});

it("decodes latest turn source proposed plan metadata when present", () => {
  const parsed = Schema.decodeUnknownSync(HistoricalV1.latestTurn)({
    turnId: "turn-2",
    state: "running",
    requestedAt: historicalAt,
    startedAt: "2026-01-01T00:00:01.000Z",
    completedAt: null,
    assistantMessageId: null,
    sourceProposedPlan: { threadId: "thread-1", planId: "plan-1" },
  });
  assert.equal(parsed.turnId, "turn-2");
  assert.deepEqual(parsed.sourceProposedPlan, { threadId: "thread-1", planId: "plan-1" });
  assert.isFalse("runId" in parsed);
});

it("preserves embedded linked pull requests from historical thread data", () => {
  const link = {
    projectId: "project-1",
    repository: "acme/web",
    number: 42,
    url: "https://github.com/acme/web/pull/42",
  };
  const parsed = Schema.decodeUnknownSync(HistoricalV1.thread)({
    ...historicalThread,
    linkedPullRequest: link,
  });
  assert.deepEqual(parsed.linkedPullRequest, link);
  assert.deepEqual(parsed.pullRequests, []);
});

it("rejects thread history imports without messages", () => {
  const input = {
    type: "thread.history.import",
    commandId: "command-empty-history",
    threadId: "thread-1",
  };
  const result = Schema.decodeUnknownOption(HistoricalV1.historyImport)({
    ...input,
    messages: [],
  });
  assert.isTrue(Option.isNone(result));
  const historical = Schema.decodeUnknownSync(HistoricalV1.historyImport)({
    ...input,
    messages: [
      { messageId: "message-1", role: "user", text: "Stored question", createdAt: historicalAt },
    ],
  });
  assert.lengthOf(historical.messages, 1);
  assert.isFalse(Schema.is(OrchestrationV2Command)(historical));
});
