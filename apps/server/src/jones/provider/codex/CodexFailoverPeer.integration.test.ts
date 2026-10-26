import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CodexSettings,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2ProviderThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../../config.ts";
import * as CodexAdapterV2 from "../../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { makeReplayServerConfig } from "../../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as ControlAdmission from "../../orchestration/ControlAdmission.ts";
import * as ProcessAttribution from "../../resourceTelemetry/ProcessAttribution.ts";

const peerPath = NodeURL.fileURLToPath(
  new URL("../../../../../../scripts/jones/e2e/codex-failover/peer.mjs", import.meta.url),
);
type Terminal = Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }>;
const Trace = Schema.Struct({
  synthetic: Schema.Literal(true),
  processId: Schema.String,
  instance: Schema.String,
  method: Schema.String,
  nativeThreadId: Schema.NullOr(Schema.String),
  nativeTurnId: Schema.NullOr(Schema.String),
  model: Schema.optional(Schema.NullOr(Schema.String)),
  effort: Schema.optional(Schema.NullOr(Schema.String)),
  serviceTier: Schema.optional(Schema.NullOr(Schema.String)),
  inputMarker: Schema.optional(Schema.String),
});
const decodeTrace = Schema.decodeUnknownEffect(Schema.fromJsonString(Trace));
const decodeHistory = Schema.decodeUnknownEffect(CodexSchema.V2ThreadReadResponse__Thread);
const decodeSettings = Schema.decodeEffect(CodexSettings);

describe("Codex synthetic peer real adapter fixture qualification", () => {
  it.live(
    "normalizes A/B/C terminals and manually resumes the same native history through real subprocess clients",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const config = yield* Effect.acquireRelease(
          makeReplayServerConfig("synthetic-failover-peer"),
          (value) =>
            fs.remove(value.baseDir, { recursive: true }).pipe(
              Effect.andThen(fs.exists(value.baseDir)),
              Effect.tap((exists) => Effect.sync(() => assert.isFalse(exists))),
              Effect.orDie,
            ),
        );
        const stateDir = path.join(config.baseDir, "peer-state");
        const homePath = path.join(config.baseDir, "codex-home");
        const cwd = path.join(config.baseDir, "workspace");
        for (const directory of [stateDir, homePath, cwd]) {
          yield* fs.makeDirectory(directory);
        }
        const realNode = yield* fs.realPath(process.execPath);
        // The executable fixture's shebang must select this runner's Node, and
        // provider setup must not inherit account credentials or launch overrides.
        const pathDelimiter = (yield* HostProcess.Platform) === "win32" ? ";" : ":";
        const environment = {
          HOME: homePath,
          PATH: `${path.dirname(realNode)}${pathDelimiter}/usr/bin${pathDelimiter}/bin`,
        };
        const clientFactory = yield* CodexAdapterV2.CodexAppServerClientFactory.pipe(
          Effect.provide(CodexAdapterV2.layerAppServerClientFactory),
        );
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const attribution = yield* ProcessAttribution.ProcessAttribution;
        const threadId = ThreadId.make("synthetic-adapter-qualification");
        const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd,
        });
        let savedThread: OrchestrationV2ProviderThread | undefined;
        let previousHistory: CodexSchema.V2ThreadReadResponse__Thread | undefined;
        const nativeTurnIds: Array<string> = [];

        for (const [index, account] of ["A", "B", "C"].entries()) {
          const ordinal = index + 1;
          const instanceId = ProviderInstanceId.make(`synthetic-codex-${account}`);
          const providerSessionId = ProviderSessionId.make(`synthetic-session-${account}`);
          const attemptId = RunAttemptId.make(`synthetic-attempt-${account}`);
          const marker = `SYNTHETIC_MANUAL_${account}`;
          const modelSelection: ModelSelection = {
            instanceId,
            model: "synthetic-codex-model",
            options: [
              { id: "reasoningEffort", value: "high" },
              { id: "serviceTier", value: "priority" },
            ],
          };

          // Each scope closes its captured subprocess before the next manual
          // account switch. No failover policy or automatic retry runs here.
          const result = yield* Effect.gen(function* () {
            const settings = yield* decodeSettings({
              binaryPath: peerPath,
              homePath,
              launchArgs: `--state-dir ${JSON.stringify(stateDir)} --account ${account}`,
            });
            const adapter = yield* CodexAdapterV2.createCodexAdapterV2({
              instanceId,
              displayName: undefined,
              enabled: true,
              environment: [],
              config: settings,
            }).pipe(
              Effect.provideService(ServerConfig.ServerConfig, config),
              Effect.provideService(CodexAdapterV2.CodexAppServerClientFactory, clientFactory),
              Effect.provideService(HostProcess.Environment, environment),
              Effect.provideService(HostProcess.HomeDirectory, homePath),
            );
            const runtime = yield* adapter.openSession({
              threadId,
              providerSessionId,
              modelSelection,
              runtimePolicy,
            });
            assert.equal(runtime.instanceId, instanceId);
            const providerThread =
              savedThread === undefined
                ? yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy })
                : yield* runtime.resumeThread({
                    threadId,
                    providerThread: savedThread,
                    modelSelection,
                    runtimePolicy,
                  });
            assert.equal(providerThread.providerInstanceId, instanceId);
            assert.equal(providerThread.providerSessionId, providerSessionId);
            assert.equal(providerThread.appThreadId, threadId);
            if (savedThread !== undefined) {
              assert.equal(providerThread.id, savedThread.id);
              assert.deepEqual(providerThread.nativeThreadRef, savedThread.nativeThreadRef);
            }
            const readHistory = () =>
              runtime
                .readThreadSnapshot({ providerThread })
                .pipe(Effect.flatMap((snapshot) => decodeHistory(snapshot.providerPayload)));
            const before = yield* readHistory();
            assert.equal(before.id, providerThread.nativeThreadRef?.nativeId);
            assert.equal(before.cwd, cwd);
            assert.lengthOf(before.turns, index);
            if (previousHistory !== undefined) assert.deepEqual(before, previousHistory);

            const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
            const terminalReady = yield* Deferred.make<Terminal>();
            yield* runtime.events.pipe(
              Stream.runForEach((event) =>
                Effect.sync(() => events.push(event)).pipe(
                  Effect.andThen(
                    event.type === "turn.terminal"
                      ? Deferred.succeed(terminalReady, event)
                      : Effect.void,
                  ),
                ),
              ),
              Effect.forkScoped,
            );
            const now = yield* DateTime.now;
            yield* runtime.startTurn({
              appThread: {
                createdBy: "user",
                creationSource: "web",
                id: threadId,
                projectId: ProjectId.make("synthetic-project"),
                title: "Synthetic manual continuation",
                providerInstanceId: instanceId,
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: null,
                worktreePath: null,
                activeProviderThreadId: providerThread.id,
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
              threadId,
              runId: RunId.make(`synthetic-run-${account}`),
              runOrdinal: ordinal,
              providerTurnOrdinal: ordinal,
              attemptId,
              rootNodeId: NodeId.make(`synthetic-node-${account}`),
              providerThread,
              message: {
                createdBy: "user",
                creationSource: "web",
                messageId: MessageId.make(`synthetic-message-${account}`),
                text: marker,
                attachments: [],
              },
              modelSelection,
              runtimePolicy,
            });
            const terminal = yield* Deferred.await(terminalReady).pipe(
              Effect.timeout("10 seconds"),
            );
            assert.equal(terminal.evidenceKind, "provider_result");
            assert.equal(terminal.driver, CodexAdapterV2.CODEX_DRIVER_KIND);
            assert.equal(terminal.providerThreadId, providerThread.id);
            assert.equal(terminal.runOrdinal, ordinal);
            assert.equal(terminal.threadDisposition, "reusable");
            assert.equal(terminal.providerTurn?.runAttemptId, attemptId);
            assert.equal(terminal.providerTurn?.ordinal, ordinal);
            const after = yield* readHistory();
            assert.equal(after.id, before.id);
            assert.equal(after.cwd, cwd);
            assert.lengthOf(after.turns, ordinal);
            assert.deepEqual(after.turns.slice(0, index), before.turns);
            const nativeTurn = after.turns[index]!;
            assert.equal(terminal.providerTurn?.nativeTurnRef?.nativeId, nativeTurn.id);
            assert.equal(
              terminal.providerTurnId,
              idAllocator.derive.providerTurn({
                driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                nativeTurnId: nativeTurn.id,
              }),
            );
            assert.deepEqual(
              nativeTurn.items.flatMap((item) =>
                item.type === "userMessage"
                  ? item.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                  : [],
              ),
              [marker],
            );
            if (account === "C") {
              assert.equal(terminal.status, "completed");
              assert.isNull(terminal.failure);
              assert.equal(nativeTurn.status, "completed");
              assert.deepEqual(
                nativeTurn.items.flatMap((item) =>
                  item.type === "agentMessage" ? [item.text] : [],
                ),
                ["SYNTHETIC_C_COMPLETED"],
              );
            } else {
              assert.equal(terminal.status, "failed");
              assert.equal(nativeTurn.status, "failed");
              assert.equal(
                terminal.failure?.class,
                account === "A" ? "usage_limit" : "provider_error",
              );
              assert.equal(
                terminal.failure?.code,
                account === "A" ? "usageLimitExceeded" : "responseStreamDisconnected",
              );
              assert.equal(
                terminal.failure?.message,
                account === "A" ? "SYNTHETIC_A_USAGE_LIMIT" : "SYNTHETIC_B_UNAVAILABLE",
              );
              assert.deepEqual(
                nativeTurn.error?.codexErrorInfo,
                account === "A"
                  ? "usageLimitExceeded"
                  : { responseStreamDisconnected: { httpStatusCode: 503 } },
              );
            }
            assert.lengthOf(
              events.filter((event) => event.type === "turn.terminal"),
              1,
            );
            return { providerThread, history: after, nativeTurnId: nativeTurn.id };
          }).pipe(Effect.scoped);
          savedThread = result.providerThread;
          previousHistory = result.history;
          nativeTurnIds.push(result.nativeTurnId);
          assert.equal((yield* attribution.snapshot).size, 0);
        }

        const traces = yield* Effect.forEach(
          (yield* fs.readDirectory(stateDir)).filter((name) => /^trace-.*\.ndjson$/.test(name)),
          (name) =>
            fs
              .readFileString(path.join(stateDir, name))
              .pipe(
                Effect.flatMap((content) =>
                  Effect.forEach(content.trim().split("\n"), (line) => decodeTrace(line)),
                ),
              ),
        );
        assert.lengthOf(traces, 3);
        assert.equal(new Set(traces.map((trace) => trace[0]!.processId)).size, 3);
        for (const [index, account] of ["A", "B", "C"].entries()) {
          const expectedNativeTurnId = nativeTurnIds[index];
          assert.isDefined(expectedNativeTurnId);
          const trace = traces.find((entries) => entries[0]?.instance === account);
          assert.isDefined(trace);
          assert.lengthOf(
            trace.filter((entry) => entry.method === "thread/start"),
            index === 0 ? 1 : 0,
          );
          assert.lengthOf(
            trace.filter((entry) => entry.method === "thread/resume"),
            index === 0 ? 0 : 1,
          );
          assert.deepEqual(
            trace
              .filter((entry) => entry.method === "turn/start")
              .map((entry) => ({
                nativeThreadId: entry.nativeThreadId,
                nativeTurnId: entry.nativeTurnId,
                model: entry.model,
                effort: entry.effort,
                serviceTier: entry.serviceTier,
                inputMarker: entry.inputMarker,
              })),
            [
              {
                nativeThreadId: savedThread!.nativeThreadRef!.nativeId,
                nativeTurnId: expectedNativeTurnId,
                model: "synthetic-codex-model",
                effort: "high",
                serviceTier: "priority",
                inputMarker: `SYNTHETIC_MANUAL_${account}`,
              },
            ],
          );
          assert.lengthOf(
            trace.filter((entry) => entry.method === "synthetic/terminal"),
            1,
          );
        }
        assert.isFalse(yield* fs.exists(path.join(homePath, "auth.json")));
      }).pipe(
        Effect.scoped,
        Effect.timeout("35 seconds"),
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            IdAllocator.layer,
            McpProviderSessions.layer,
            ControlAdmission.layer,
            ProcessAttribution.layer,
          ),
        ),
        Effect.provideService(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
  );
});
