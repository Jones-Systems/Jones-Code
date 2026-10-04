import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import { ThreadId } from "../../packages/contracts/src/index.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ServerConfig from "../../apps/server/src/config.ts";
import * as ServerEnvironment from "../../apps/server/src/environment/ServerEnvironment.ts";
import * as RuntimeRepository from "../../apps/server/src/persistence/ProviderSessionRuntime.ts";
import {
  CodexResumeCursorSchema,
  openCodexThread,
} from "../../apps/server/src/provider/codexThreadOpen.ts";
import { makeClaudeQueryOptions } from "../../apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { qualifyLegacyV1ImportContinuation } from "../../apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import { makePopulatedFixture, readFixture, withDatabaseEffect } from "./fixture.mjs";
import { withRunScratchEffect } from "./support.mjs";

const decodeCodexCursor = Schema.decodeUnknownSync(CodexResumeCursorSchema);

const migratedRuntimes = Effect.fn("qualification.migratedRuntimes")(function* (root) {
  const fixture = yield* Effect.promise(() =>
    makePopulatedFixture({ baseDir: NodePath.join(root, "home") }),
  );
  const before = yield* Effect.promise(() => readFixture(fixture));
  const runtimes = yield* withDatabaseEffect(
    fixture,
    { startup: true },
    Effect.gen(function* () {
      const repository = yield* RuntimeRepository.ProviderSessionRuntimeRepository;
      const listed = yield* repository.list();
      for (const identity of fixture.expected.identities) {
        const runtime = Option.getOrThrow(
          yield* repository.getByThreadId({ threadId: ThreadId.make(identity.threadId) }),
        );
        const persisted = before.tables.provider_session_runtime.find(
          (row) => row.thread_id === identity.threadId,
        );
        NodeAssert.ok(persisted);
        NodeAssert.equal(runtime.providerInstanceId, identity.providerInstanceId);
        NodeAssert.deepEqual(runtime.resumeCursor, JSON.parse(persisted.resume_cursor_json));
        NodeAssert.deepEqual(runtime.runtimePayload, JSON.parse(persisted.runtime_payload_json));
        NodeAssert.deepEqual(
          listed.find((row) => row.threadId === identity.threadId),
          runtime,
        );
      }
      return listed;
    }).pipe(Effect.provide(RuntimeRepository.layer)),
  );
  const reopened = yield* withDatabaseEffect(
    fixture,
    { startup: true },
    Effect.gen(function* () {
      const repository = yield* RuntimeRepository.ProviderSessionRuntimeRepository;
      return yield* repository.list();
    }).pipe(Effect.provide(RuntimeRepository.layer)),
  );
  NodeAssert.deepEqual(reopened, runtimes);
  const after = yield* Effect.promise(() => readFixture(fixture));
  NodeAssert.deepEqual(
    after.tables.provider_session_runtime,
    before.tables.provider_session_runtime,
  );
  return { fixture, runtimes };
});

function codexClient(response, resumeFailure) {
  const calls = [];
  return {
    calls,
    client: {
      raw: {
        request: (method, payload) =>
          Effect.suspend(() => {
            calls.push({ method, payload: JSON.parse(JSON.stringify(payload)) });
            return resumeFailure ? Effect.fail(resumeFailure) : Effect.succeed(response);
          }),
      },
      request: (method, payload) =>
        Effect.sync(() => {
          calls.push({ method, payload: JSON.parse(JSON.stringify(payload)) });
          return response;
        }),
    },
  };
}

const environmentIdentity = Effect.fn("qualification.environmentIdentity")(function* (baseDir) {
  return yield* Effect.gen(function* () {
    const identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const config = yield* ServerConfig.ServerConfig;
    return { id: yield* identity.getEnvironmentId, file: config.environmentIdPath };
  }).pipe(
    Effect.provide(
      ServerEnvironment.identityLayer.pipe(
        Layer.provideMerge(ServerConfig.layerTest(baseDir, baseDir)),
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
    Effect.scoped,
  );
});

function assertDistinctEnvironmentIds(identities) {
  NodeAssert.equal(
    new Set(identities.map(({ id }) => id)).size,
    identities.length,
    "qualification guard: isolated runtime homes reused an environment ID",
  );
}

describe("T4 persisted identity and historical provider request compatibility", () => {
  it.effect(
    "passes the migrated Codex native thread ID to thread/resume without downloading turns",
    () =>
      withRunScratchEffect({ label: "codex-resume" }, ({ root, record }) =>
        Effect.gen(function* () {
          const { fixture, runtimes } = yield* migratedRuntimes(root);
          const runtime = runtimes.find((row) => row.providerName === "codex");
          NodeAssert.ok(runtime);
          const identity = fixture.expected.identities.find(
            (entry) => entry.threadId === runtime.threadId,
          );
          const cursor = decodeCodexCursor(runtime.resumeCursor);
          NodeAssert.equal(cursor.threadId, identity.nativeThreadId);
          NodeAssert.notEqual(cursor.threadId, runtime.threadId);
          const response = {
            cwd: identity.worktree,
            model: "synthetic-model",
            modelProvider: "synthetic",
            serviceTier: "fast",
            thread: { id: cursor.threadId },
            turns: [{ syntheticUnsupportedHistoricalItem: true }],
          };
          const fake = codexClient(response);
          const resumed = yield* openCodexThread({
            client: fake.client,
            threadId: runtime.threadId,
            runtimeMode: runtime.runtimeMode,
            cwd: identity.worktree,
            requestedModel: "synthetic-model",
            serviceTier: "fast",
            resumeThreadId: cursor.threadId,
            requireResume: true,
          });
          NodeAssert.equal(resumed.thread.id, cursor.threadId);
          NodeAssert.deepEqual(fake.calls, [
            {
              method: "thread/resume",
              payload: {
                threadId: cursor.threadId,
                cwd: identity.worktree,
                model: "synthetic-model",
                serviceTier: "fast",
                approvalPolicy: "never",
                sandbox: "danger-full-access",
                approvalsReviewer: "user",
                excludeTurns: true,
              },
            },
          ]);
          record({
            checkId: "codex-resume",
            proofKind: "historical-sqlite-repository-and-codex-request-compatibility",
            result: "passed",
            readback: {
              canonicalThreadId: runtime.threadId,
              nativeThreadId: resumed.thread.id,
              providerInstanceId: runtime.providerInstanceId,
              method: fake.calls[0].method,
              excludeTurns: fake.calls[0].payload.excludeTurns,
            },
            limits: [
              "injected historical request helper; active V2 runtime adoption, native Codex CLI resume and provider-side transcript availability unproved",
            ],
          });
        }),
      ),
  );

  it.effect(
    "fails required Codex resume on invalid metadata or a recoverable error and starts only with no cursor",
    () =>
      withRunScratchEffect({ label: "codex-resume-guards" }, ({ root, record }) =>
        Effect.gen(function* () {
          const response = {
            cwd: root,
            model: "synthetic-model",
            modelProvider: "synthetic",
            thread: { id: "native-id" },
          };
          const input = {
            threadId: ThreadId.make("canonical-id"),
            runtimeMode: "full-access",
            cwd: root,
            requestedModel: "synthetic-model",
            serviceTier: undefined,
            resumeThreadId: "native-id",
            requireResume: true,
          };
          for (const fake of [
            codexClient({ thread: { id: "native-id" } }),
            codexClient(response, new Error("thread not found")),
          ]) {
            const failed = yield* openCodexThread({ ...input, client: fake.client }).pipe(
              Effect.exit,
            );
            NodeAssert.equal(failed._tag, "Failure");
            NodeAssert.deepEqual(
              fake.calls.map(({ method }) => method),
              ["thread/resume"],
            );
          }
          const fresh = codexClient(response);
          const started = yield* openCodexThread({
            ...input,
            resumeThreadId: undefined,
            client: fresh.client,
          });
          NodeAssert.equal(started.thread.id, "native-id");
          NodeAssert.deepEqual(
            fresh.calls.map(({ method }) => method),
            ["thread/start"],
          );
          record({
            checkId: "codex-resume-guards",
            proofKind: "historical-codex-request-compatibility-injected-failures",
            result: "passed",
            readback: {
              invalidMetadataFreshStarts: 0,
              requiredRecoverableFailureFreshStarts: 0,
              absentCursorMethod: "thread/start",
            },
            limits: [
              "synthetic historical request failures; no provider process or active V2 runtime adoption proof",
            ],
          });
        }),
      ),
  );

  it.effect(
    "preserves the historical Claude durable UUID in SDK resume options while qualification stays unknown",
    () =>
      withRunScratchEffect({ label: "claude-resume" }, ({ root, record }) =>
        Effect.gen(function* () {
          const { fixture, runtimes } = yield* migratedRuntimes(root);
          const runtime = runtimes.find((row) => row.providerName === "claudeAgent");
          NodeAssert.ok(runtime);
          NodeAssert.match(runtime.resumeCursor.resume, /^[0-9a-f-]{36}$/);
          const identity = fixture.expected.identities.find(
            (entry) => entry.threadId === runtime.threadId,
          );
          NodeAssert.ok(identity);
          NodeAssert.equal(runtime.threadId, identity.threadId);
          NodeAssert.equal(runtime.providerInstanceId, identity.providerInstanceId);
          NodeAssert.equal(runtime.resumeCursor.resume, identity.durableSessionId);
          NodeAssert.equal(runtime.resumeCursor.resumeSessionAt, "synthetic-stale-assistant");
          const continuation = qualifyLegacyV1ImportContinuation({
            threadId: runtime.threadId,
            provenance: "legacy_row",
            sourceRow: runtime,
            source: "persisted_runtime_row",
          });
          NodeAssert.deepEqual(continuation.qualification, {
            type: "unknown",
            reason: "target_identity_missing",
          });
          const evidence = continuation.evidence;
          NodeAssert.ok(evidence);
          NodeAssert.equal(evidence.threadId, runtime.threadId);
          NodeAssert.equal(evidence.providerInstanceId, runtime.providerInstanceId);
          NodeAssert.equal(evidence.driver, "claudeAgent");
          NodeAssert.deepEqual(evidence.resumeCursor, runtime.resumeCursor);
          NodeAssert.equal(evidence.nativeThreadId, runtime.resumeCursor.resume);
          NodeAssert.notEqual(evidence.nativeThreadId, runtime.threadId);
          NodeAssert.notEqual(evidence.nativeThreadId, runtime.resumeCursor.threadId);
          NodeAssert.equal(evidence.historicalSourceIdentity, null);
          NodeAssert.equal(evidence.accessibility, null);
          NodeAssert.equal(evidence.continuationKey, null);
          // The historical cursor's checkpoint does not establish a V2 resume anchor.
          const options = makeClaudeQueryOptions({
            modelSelection: {
              instanceId: runtime.providerInstanceId,
              model: "claude-sonnet-4-6",
            },
            nativeThreadId: evidence.nativeThreadId,
            resume: true,
            cwd: identity.worktree,
            environment: {},
          });
          NodeAssert.equal(options.resume, runtime.resumeCursor.resume);
          NodeAssert.equal(options.resumeSessionAt, undefined);
          NodeAssert.equal(options.sessionId, undefined);
          NodeAssert.equal(options.cwd, identity.worktree);
          record({
            checkId: "claude-resume",
            proofKind: "historical-persisted-cursor-and-sdk-option-compatibility",
            result: "passed",
            readback: {
              canonicalThreadId: evidence.threadId,
              durableSessionId: options.resume,
              providerInstanceId: evidence.providerInstanceId,
              qualification: continuation.qualification,
              staleCheckpointExcluded: true,
              freshSessionIdExcluded: true,
            },
            limits: [
              "persisted-cursor qualification and SDK options only; no session start, active V2 resume or native adoption proof",
              "historical source identity, accessibility, account state and provider-side transcript availability unproved",
            ],
          });
        }),
      ),
  );

  it.effect(
    "retains identity when reopened and detects copied IDs with a qualification guard",
    () =>
      withRunScratchEffect({ label: "environment-identity" }, ({ root, record }) =>
        Effect.gen(function* () {
          const firstHome = NodePath.join(root, "first-home");
          const secondHome = NodePath.join(root, "second-home");
          const first = yield* environmentIdentity(firstHome);
          NodeAssert.deepEqual(yield* environmentIdentity(firstHome), first);
          const second = yield* environmentIdentity(secondHome);
          assertDistinctEnvironmentIds([first, second]);
          yield* Effect.promise(() => NodeFSP.copyFile(first.file, second.file));
          const copied = yield* environmentIdentity(secondHome);
          NodeAssert.equal(copied.id, first.id);
          NodeAssert.throws(
            () => assertDistinctEnvironmentIds([first, copied]),
            /qualification guard/,
          );
          record({
            checkId: "environment-identity",
            proofKind: "production-identity-layer-and-harness-copy-guard",
            result: "passed",
            readback: {
              stableReopen: true,
              distinctFreshHomes: true,
              copiedIdDetectedByHarness: true,
            },
            limits: [
              "production accepts persisted copied IDs; rejection belongs to the qualification guard, not runtime",
            ],
          });
        }),
      ),
  );
});
