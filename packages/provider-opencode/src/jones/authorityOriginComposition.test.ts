import type { OpencodeClient } from "@opencode-ai/sdk/v2";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
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
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as Origin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { makeOpenCodeAdapterV2 } from "../server/adapter.ts";
import { OpenCodeDriver } from "../server/driver.ts";
import * as Runtime from "../server/OpenCodeRuntime.ts";
import * as Ledger from "../server/OpenCodeServerLedger.ts";
import * as Policy from "../server/jones/OpenCodeCreationPolicy.ts";
import * as V2 from "../server/v2/adapter.ts";
import * as V2Client from "../server/v2/OpenCode2Client.ts";
import * as V2Server from "../server/v2/OpenCode2Server.ts";

const instanceId = ProviderInstanceId.make("opencode-composition");
const threadId = ThreadId.make("opencode-composition-thread");
const providerSessionId = ProviderSessionId.make("opencode-composition-session");
const cwd = "/synthetic/opencode-composition";
const selection = { instanceId, model: "opencode/synthetic" };
const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd,
});
const identity = {
  productId: "jones-code" as const,
  productName: "Jones Code" as const,
  version: "synthetic-composition-identity",
  source: null,
};
const denied = () =>
  new Runtime.OpenCodeRuntimeError({
    operation: "synthetic-denial",
    detail: "Synthetic authorization refusal.",
  });
const unused = () => Effect.die("Unexpected synthetic operation");
const hooks: Policy.OpenCodeCreationHooks = {
  reserveGeneration: Effect.succeed("physical-generation"),
  authorize: () => Effect.void,
  abandonGeneration: () => Effect.void,
};
const authority = (isCurrent: Effect.Effect<boolean>): Policy.OpenCodeQualifiedAuthority => ({
  creationHooks: hooks,
  authorizeAdoption: () => Effect.succeed(isCurrent),
  authorizeConsumption: () => Effect.void,
});
const services = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  ProviderLatestVersions.layer,
  TestProviderHost.layer({ cwd, runBackgroundWork: false }),
  Layer.succeed(
    ProviderEventLoggers.ProviderEventLoggers,
    ProviderEventLoggers.NoOpProviderEventLoggers,
  ),
).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

describe("OpenCode authority composition", () => {
  it.effect.each(["1.18.32", "2.0.18"])(
    "forwards instance authority through the %s driver to the actual server owner",
    (version) =>
      Effect.gen(function* () {
        const captures: Array<Policy.OpenCodeCreationHooks | undefined> = [];
        const runtime: Runtime.OpenCodeRuntime["Service"] = {
          startOpenCodeServerProcess: (input) =>
            Effect.sync(() => {
              captures.push(input.creationHooks);
            }).pipe(Effect.andThen(Effect.fail(denied()))),
          connectToOpenCodeServer: unused,
          runOpenCodeCommand: () => Effect.succeed({ stdout: version, stderr: "", code: 0 }),
          createOpenCodeSdkClient: () => {
            throw new Error("No SDK before authorization");
          },
          loadOpenCodeInventory: unused,
          loadOpenCodeSkills: unused,
        };
        const instance = yield* OpenCodeDriver.create({
          instanceId,
          displayName: undefined,
          enabled: true,
          environment: [],
          config: OpenCodeDriver.defaultConfig(),
        }).pipe(
          Effect.provideService(Runtime.OpenCodeRuntime, runtime),
          Effect.provideService(Policy.OpenCodeAuthority, {
            forInstance: () => authority(Effect.succeed(true)),
          }),
        );
        const result = version.startsWith("1.")
          ? yield* instance.snapshotForCwd!(cwd).pipe(Effect.exit)
          : yield* instance.orchestrationAdapter
              .openSession({
                threadId,
                providerSessionId,
                modelSelection: selection,
                runtimePolicy,
              })
              .pipe(Effect.exit);
        assert.equal(result._tag, "Failure");
        assert.isAbove(captures.length, 0);
        for (const capture of captures) assert.strictEqual(capture, hooks);
      }).pipe(
        Effect.scoped,
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("Unexpected HTTP before authorization")),
        ),
        Effect.provide(services),
      ),
  );

  it.effect("refuses a present authority service that cannot qualify the instance", () =>
    Effect.gen(function* () {
      const error = yield* OpenCodeDriver.create({
        instanceId,
        displayName: undefined,
        enabled: true,
        environment: [],
        config: OpenCodeDriver.defaultConfig(),
      }).pipe(
        Effect.provideService(Policy.OpenCodeAuthority, { forInstance: () => undefined }),
        Effect.provideService(Runtime.OpenCodeRuntime, {
          startOpenCodeServerProcess: unused,
          connectToOpenCodeServer: unused,
          runOpenCodeCommand: unused,
          createOpenCodeSdkClient: () => {
            throw new Error("Unused SDK");
          },
          loadOpenCodeInventory: unused,
          loadOpenCodeSkills: unused,
        }),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("No request for an unqualified instance")),
        ),
        Effect.flip,
      );
      assert.equal(error._tag, "ProviderDriverError");
      assert.equal(error.detail, "OpenCode instance authority is unavailable.");
    }).pipe(Effect.scoped, Effect.provide(services)),
  );

  it.effect("denies at the actual runtime boundary before a spawn or ledger write", () => {
    const trace: string[] = [];
    const spawner = ChildProcessSpawner.make(() =>
      Effect.sync(() => {
        trace.push("spawn");
      }).pipe(Effect.andThen(Effect.die("Unexpected spawn"))),
    );
    return Effect.gen(function* () {
      const runtime = yield* Runtime.OpenCodeRuntime;
      const error = yield* runtime
        .connectToOpenCodeServer({
          binaryPath: "/synthetic/opencode",
          directory: cwd,
          port: 4200,
          creationHooks: {
            reserveGeneration: Effect.sync(() => {
              trace.push("reserve");
              return "refused-generation";
            }),
            authorize: (capture) =>
              Effect.sync(() => {
                assert.deepEqual(capture, {
                  directory: cwd,
                  runtimeGeneration: "refused-generation",
                });
                assert.isTrue(Object.isFrozen(capture));
                trace.push("authorize");
              }).pipe(Effect.andThen(Effect.fail(denied()))),
            abandonGeneration: (generation) =>
              Effect.sync(() => {
                trace.push(`abandon:${generation}`);
              }),
          },
        })
        .pipe(Effect.flip);
      assert.equal(error.operation, "synthetic-denial");
      assert.deepEqual(trace, ["reserve", "authorize", "abandon:refused-generation"]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Runtime.layer.pipe(
          Layer.provide(Layer.succeed(Ledger.OpenCodeServerLedger, { track: unused })),
          Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
        ),
      ),
      Effect.provideService(HostProcess.Platform, "linux"),
    );
  });

  it.effect("v2 authorizes the physical incarnation and rejects a stale owner before use", () => {
    let current = true;
    const consumed: string[] = [];
    const qualified: Policy.OpenCodeQualifiedAuthority = {
      ...authority(Effect.succeed(true)),
      authorizeConsumption: ({ parent, kind }) =>
        Effect.sync(() => {
          assert.equal(parent.runtimeGeneration, "physical-generation");
          assert.equal(parent.pid, 123);
          consumed.push(kind);
        }),
    };
    const runtime: Runtime.OpenCodeRuntime["Service"] = {
      startOpenCodeServerProcess: (input) =>
        Effect.gen(function* () {
          assert.strictEqual(input.creationHooks, hooks);
          const capture = yield* Policy.prepareGeneration(input.directory, input.creationHooks!);
          const url = "http://synthetic.invalid";
          const version = yield* input.verify!(url);
          return {
            url,
            version,
            runtimeGeneration: capture.runtimeGeneration,
            ownedProcess: {
              incarnation: Object.freeze({ ...capture, pid: 123, url }),
              isCurrent: Effect.sync(() => current),
            },
            isRunning: Effect.succeed(true),
            exitCode: Effect.never,
          };
        }),
      connectToOpenCodeServer: unused,
      runOpenCodeCommand: unused,
      createOpenCodeSdkClient: () => {
        throw new Error("Unused SDK");
      },
      loadOpenCodeInventory: unused,
      loadOpenCodeSkills: unused,
    };
    return Effect.gen(function* () {
      const server = yield* V2Server.make({
        binaryPath: "synthetic",
        directory: cwd,
        serverUrl: "",
        serverPassword: "",
        environment: {},
        authority: qualified,
      });
      const first = yield* server.withConnection(Effect.succeed, undefined, "inventory");
      const second = yield* server.withConnection(Effect.succeed, undefined, "models");
      assert.strictEqual(first, second);
      assert.equal(first.ownedProcess?.incarnation?.runtimeGeneration, "physical-generation");
      assert.isTrue(Object.isFrozen(first.ownedProcess));
      assert.isDefined(first.processExit);
      assert.deepEqual(consumed, ["inventory", "models"]);
      current = false;
      const failure = yield* server
        .withConnection(() => Effect.die("Stale use"), undefined, "session")
        .pipe(Effect.flip);
      assert.equal(failure.operation, "adoptServer");
      assert.deepEqual(consumed, ["inventory", "models"]);
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeCrypto.layer),
      Effect.provideService(Runtime.OpenCodeRuntime, runtime),
      Effect.provideService(V2Client.OpenCode2Client, {
        connect: () =>
          Effect.succeed({
            client: {
              server: { info: () => Effect.succeed({ version: "2.0.18" }) },
            },
          } as unknown as V2Client.OpenCode2Api),
      }),
    );
  });

  it.effect("refuses a qualified external v2 connection before lending it", () =>
    Effect.gen(function* () {
      let used = false;
      const server = yield* V2Server.make({
        binaryPath: "synthetic",
        directory: cwd,
        serverUrl: "http://synthetic.invalid",
        serverPassword: "synthetic",
        environment: {},
        authority: authority(Effect.succeed(true)),
      }).pipe(
        Effect.provideService(V2Client.OpenCode2Client, {
          connect: () =>
            Effect.succeed({
              client: {
                server: { info: () => Effect.succeed({ version: "2.0.18" }) },
              },
            } as unknown as V2Client.OpenCode2Api),
        }),
      );
      const error = yield* server
        .withConnection(() =>
          Effect.sync(() => {
            used = true;
          }),
        )
        .pipe(Effect.flip);
      assert.equal(error.operation, "adoptServer");
      assert.isFalse(used);
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeCrypto.layer),
      Effect.provideService(Runtime.OpenCodeRuntime, {
        startOpenCodeServerProcess: unused,
        connectToOpenCodeServer: unused,
        runOpenCodeCommand: unused,
        createOpenCodeSdkClient: () => {
          throw new Error("Unused SDK");
        },
        loadOpenCodeInventory: unused,
        loadOpenCodeSkills: unused,
      }),
    ),
  );
});

const nativeNever = (signal?: AbortSignal): AsyncIterable<never> => ({
  [Symbol.asyncIterator]: () => ({
    next: () =>
      new Promise<IteratorResult<never>>((resolve) => {
        const done = () => resolve({ done: true, value: undefined });
        if (signal?.aborted) done();
        else signal?.addEventListener("abort", done, { once: true });
      }),
  }),
});

describe("OpenCode session authority and captured origins", () => {
  it.effect("v2 rejects an unqualified physical snapshot before session SDK access", () =>
    Effect.gen(function* () {
      const adapter = yield* V2.make(instanceId).pipe(
        Effect.provideService(Policy.OpenCodeAuthority, {
          forInstance: () => authority(Effect.succeed(true)),
        }),
        Effect.provideService(V2Server.OpenCode2Server, {
          withConnection: (use) =>
            use({
              url: "http://synthetic.invalid",
              version: "2.0.18",
              external: false,
              ownedProcess: { runtimeGeneration: "unknown", isRunning: Effect.succeed(true) },
              client: {},
            } as unknown as V2Server.OpenCode2Connection),
          forget: Effect.void,
          connections: Stream.empty,
        }),
      );
      const error = yield* adapter
        .openSession({
          threadId,
          providerSessionId,
          modelSelection: selection,
          runtimePolicy,
        })
        .pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterOpenSessionError");
      if (error._tag !== "ProviderAdapterOpenSessionError")
        return yield* Effect.die("Expected open-session refusal");
      assert.isTrue(Runtime.isOpenCodeRuntimeError(error.cause));
      if (Runtime.isOpenCodeRuntimeError(error.cause))
        assert.equal(error.cause.operation, "adoptServer");
    }).pipe(Effect.scoped, Effect.provide(services)),
  );

  it.effect.each(["v1", "v2"] as const)(
    "%s binds the captured adoption, forwards identity, and refuses late/stale use",
    (version) =>
      Effect.gen(function* () {
        let current = true;
        const requests: string[] = [];
        const instructions: string[] = [];
        const bindings: Array<Parameters<ProviderAdapter.ProviderRuntimeLifecycle["bind"]>[0]> = [];
        const lifecycle: ProviderAdapter.ProviderRuntimeLifecycle = {
          admit: () => Effect.void,
          reserve: () => Effect.succeed("session-adoption-generation"),
          abandon: unused,
          bind: (input) =>
            Effect.sync(() => {
              bindings.push(input);
              return input.providerThread;
            }),
          invalidate: () => Effect.void,
        };
        const qualified = authority(Effect.sync(() => current));
        const parent: Policy.OpenCodeOwnedProcess = {
          incarnation: Object.freeze({
            directory: cwd,
            runtimeGeneration: "physical-generation",
            pid: 123,
            url: "http://synthetic.invalid",
          }),
          isCurrent: Effect.sync(() => current),
        };
        const info = { id: "ses_synthetic", time: { created: 1, updated: 1 } };
        const v1Client = {
          event: {
            subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => ({
              stream: nativeNever(options.signal),
            }),
          },
          session: {
            create: async () => {
              requests.push("create");
              return { data: info };
            },
            get: async () => {
              requests.push("get");
              return { data: info };
            },
            messages: async () => ({ data: [] }),
            children: async () => ({ data: [] }),
            promptAsync: async (input: { system?: string }) => {
              instructions.push(input.system ?? "");
              return { data: true };
            },
          },
        } as unknown as OpencodeClient;
        const fakeRuntime: Runtime.OpenCodeRuntime["Service"] = {
          startOpenCodeServerProcess: unused,
          connectToOpenCodeServer: (input) =>
            Effect.sync(() => {
              assert.strictEqual(input.creationHooks, hooks);
              return {
                url: parent.incarnation.url,
                ownedProcess: parent,
                version: "1.18.32",
                external: false,
                exitCode: null,
              };
            }),
          runOpenCodeCommand: unused,
          createOpenCodeSdkClient: () => v1Client,
          loadOpenCodeInventory: unused,
          loadOpenCodeSkills: unused,
        };
        const v2Connection = {
          url: parent.incarnation.url,
          version: "2.0.18",
          external: false,
          ownedProcess: {
            ...parent,
            runtimeGeneration: "physical-generation",
            isRunning: parent.isCurrent,
          },
          events: Effect.succeed(Stream.never),
          client: {
            model: { list: () => Effect.succeed({ data: [] }) },
            session: {
              create: () =>
                Effect.sync(() => {
                  requests.push("create");
                  return {
                    id: info.id,
                    model: { id: "synthetic", providerID: "opencode" },
                    agent: "build",
                    location: { directory: cwd },
                  };
                }),
              get: () =>
                Effect.sync(() => {
                  requests.push("get");
                  throw new Error("Unexpected stale get");
                }),
              update: () => Effect.void,
              switchModel: () => Effect.void,
              instructions: {
                entry: {
                  put: (input: { value: string }) =>
                    Effect.sync(() => {
                      instructions.push(input.value);
                    }),
                },
              },
              prompt: () => Effect.void,
            },
          },
        } as unknown as V2Server.OpenCode2Connection;
        const host = yield* ProviderHost.ProviderHost;
        const adapter = yield* (
          version === "v1"
            ? makeOpenCodeAdapterV2({
                instanceId,
                authority: qualified,
                settings: OpenCodeDriver.defaultConfig(),
                environment: {},
              }).pipe(Effect.provideService(Runtime.OpenCodeRuntime, fakeRuntime))
            : V2.make(instanceId).pipe(
                Effect.provideService(Policy.OpenCodeAuthority, { forInstance: () => qualified }),
                Effect.provideService(V2Server.OpenCode2Server, {
                  withConnection: (use, _hooks, kind) => {
                    assert.equal(kind, "session");
                    return use(v2Connection);
                  },
                  forget: Effect.void,
                  connections: Stream.empty,
                }),
              )
        ).pipe(
          Effect.provideService(ProviderHost.ProviderHost, { ...host, appIdentity: identity }),
        );
        const session = yield* adapter.openSession({
          threadId,
          providerSessionId,
          modelSelection: selection,
          runtimePolicy,
          runtimeLifecycle: lifecycle,
        });
        const providerThread = yield* session.ensureThread({
          threadId,
          modelSelection: selection,
          runtimePolicy,
        });
        assert.equal(bindings.length, 1);
        assert.equal(bindings[0]!.runtimeGeneration, "session-adoption-generation");
        assert.equal(bindings[0]!.producerOrigin?.runtimeGeneration, "session-adoption-generation");
        assert.deepEqual(bindings[0]!.observed, ProviderAdapter.unobservedRuntimeIdentity());
        const beforeWrongThread = [...requests];
        const wrongThread = yield* session
          .ensureThread({
            threadId: ThreadId.make("unowned-target"),
            modelSelection: selection,
            runtimePolicy,
          })
          .pipe(Effect.exit);
        assert.equal(wrongThread._tag, "Failure");
        assert.deepEqual(
          requests,
          beforeWrongThread,
          "A different target needs its own adoption before creation",
        );
        const now = yield* DateTime.now;
        yield* session.startTurn({
          threadId,
          runId: RunId.make("composition-run"),
          attemptId: RunAttemptId.make("composition-attempt"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          rootNodeId: NodeId.make("composition-node"),
          providerThread,
          modelSelection: selection,
          runtimePolicy,
          message: {
            messageId: MessageId.make("composition-message"),
            text: "hello",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          appThread: {
            id: threadId,
            projectId: ProjectId.make("composition-project"),
            title: "Synthetic",
            providerInstanceId: instanceId,
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: providerThread.id,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdBy: "user",
            creationSource: "web",
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        });
        assert.isTrue(
          instructions.some(
            (text) => text.includes("App identity: Jones Code") && text.includes(identity.version),
          ),
        );
        const queued = yield* session.events.pipe(Stream.runHead);
        assert.isTrue(Option.isSome(queued));
        if (Option.isNone(queued)) return;
        const captured = Origin.readProviderEventOrigin(queued.value);
        assert.strictEqual(captured?.producer.token, bindings[0]!.producerOrigin?.token);
        assert.equal(captured?.producer.runtimeGeneration, "session-adoption-generation");
        yield* Origin.revalidateProviderEventOrigin(queued.value, session);
        current = false;
        assert.equal(
          (yield* Origin.revalidateProviderEventOrigin(queued.value, session).pipe(Effect.exit))
            ._tag,
          "Failure",
        );
        const before = [...requests];
        assert.equal(
          (yield* session
            .ensureThread({ threadId, modelSelection: selection, runtimePolicy })
            .pipe(Effect.exit))._tag,
          "Failure",
        );
        assert.equal(
          (yield* session.resumeThread({ providerThread }).pipe(Effect.exit))._tag,
          "Failure",
        );
        assert.deepEqual(
          requests,
          before,
          "Stale authority must refuse before a create or get request",
        );
        assert.equal(
          Origin.readProviderEventOrigin(queued.value)?.producer.runtimeGeneration,
          "session-adoption-generation",
        );
      }).pipe(Effect.scoped, Effect.provide(services)),
  );
});
