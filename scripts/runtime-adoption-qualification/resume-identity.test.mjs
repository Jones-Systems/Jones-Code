import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it } from "@effect/vitest";
import {
  ClaudeSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "../../packages/contracts/src/index.ts";
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
} from "../../apps/server/src/provider/Layers/CodexSessionRuntime.ts";
import { makeClaudeAdapter } from "../../apps/server/src/provider/Layers/ClaudeAdapter.ts";
import { makePopulatedFixture, readFixture, withDatabaseEffect } from "./fixture.mjs";
import { withRunScratchEffect } from "./support.mjs";

const decodeCodexCursor = Schema.decodeUnknownSync(CodexResumeCursorSchema);
const decodeClaudeSettings = Schema.decodeUnknownSync(ClaudeSettings);

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

function syntheticClaudeQuery() {
  let closed = false;
  const pending = new Set();
  return {
    close: () => {
      closed = true;
      for (const resolve of pending) resolve({ done: true, value: undefined });
      pending.clear();
    },
    setModel: async () => {},
    setPermissionMode: async () => {},
    setMaxThinkingTokens: async () => {},
    [Symbol.asyncIterator]: () => ({
      next: () =>
        closed
          ? Promise.resolve({ done: true, value: undefined })
          : new Promise((resolve) => {
              pending.add(resolve);
            }),
    }),
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

describe("T4 persisted identity and provider resume production seams", () => {
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
            proofKind: "historical-sqlite-production-repository-and-resume-request",
            result: "passed",
            readback: {
              canonicalThreadId: runtime.threadId,
              nativeThreadId: resumed.thread.id,
              providerInstanceId: runtime.providerInstanceId,
              method: fake.calls[0].method,
              excludeTurns: fake.calls[0].payload.excludeTurns,
            },
            limits: [
              "fake app-server client; native Codex CLI resume and provider-side transcript availability unproved",
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
            proofKind: "production-openCodexThread-injected-failures",
            result: "passed",
            readback: {
              invalidMetadataFreshStarts: 0,
              requiredRecoverableFailureFreshStarts: 0,
              absentCursorMethod: "thread/start",
            },
            limits: ["synthetic request failures; no provider process"],
          });
        }),
      ),
  );

  it.effect(
    "passes the migrated Claude durable session UUID to SDK resume without a stale checkpoint",
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
          let captured;
          const query = syntheticClaudeQuery();
          const session = yield* Effect.gen(function* () {
            const adapter = yield* makeClaudeAdapter(
              decodeClaudeSettings({
                binaryPath: NodePath.join(root, "synthetic-never-executed-claude"),
                homePath: NodePath.join(root, "synthetic-claude-home"),
              }),
              {
                environment: {},
                instanceId: ProviderInstanceId.make(runtime.providerInstanceId),
                createQuery: (input) => {
                  captured = input;
                  return query;
                },
              },
            );
            return yield* adapter.startSession({
              threadId: runtime.threadId,
              provider: ProviderDriverKind.make("claudeAgent"),
              resumeCursor: runtime.resumeCursor,
              cwd: identity.worktree,
              runtimeMode: runtime.runtimeMode,
            });
          }).pipe(
            Effect.provide(ServerConfig.layerTest(root, NodePath.join(root, "adapter-home"))),
            Effect.provide(NodeServices.layer),
            Effect.scoped,
          );
          NodeAssert.ok(captured);
          NodeAssert.equal(captured.options.resume, runtime.resumeCursor.resume);
          NodeAssert.equal(captured.options.resumeSessionAt, undefined);
          NodeAssert.equal(captured.options.sessionId, undefined);
          NodeAssert.equal(captured.options.cwd, identity.worktree);
          NodeAssert.equal(session.threadId, runtime.threadId);
          NodeAssert.equal(session.providerInstanceId, runtime.providerInstanceId);
          NodeAssert.equal(session.resumeCursor.resume, runtime.resumeCursor.resume);
          record({
            checkId: "claude-resume",
            proofKind: "historical-sqlite-production-repository-and-adapter-sdk-options",
            result: "passed",
            readback: {
              canonicalThreadId: session.threadId,
              durableSessionId: captured.options.resume,
              providerInstanceId: session.providerInstanceId,
              staleCheckpointExcluded: true,
              freshSessionIdExcluded: true,
            },
            limits: [
              "injected SDK query; native Claude CLI resume, account state and provider-side transcript availability unproved",
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
