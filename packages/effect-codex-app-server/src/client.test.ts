import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Queue from "effect/Queue";
import * as CodexError from "./errors.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";

import * as CodexClient from "./client.ts";

const mockPeerPath = Effect.map(Effect.service(Path.Path), (path) =>
  path.join(import.meta.dirname, "../test/fixtures/codex-app-server-mock-peer.ts"),
);
const mockPeerArgs = (path: string) => [path];

it.layer(NodeServices.layer)("effect-codex-app-server client", (it) => {
  const makeHandle = (env?: Record<string, string>) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const peerCwd = path.join(import.meta.dirname, "..");
      const command = ChildProcess.make(process.execPath, mockPeerArgs(yield* mockPeerPath), {
        cwd: peerCwd,
        ...(env ? { env: { ...process.env, ...env } } : {}),
      });
      return yield* spawner.spawn(command);
    });

  it.effect("initializes, handles typed server requests, and reads account and skills data", () =>
    Effect.gen(function* () {
      const userInputRequests = yield* Ref.make<Array<unknown>>([]);
      const messageDeltas = yield* Ref.make<Array<unknown>>([]);
      const handle = yield* makeHandle();
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const result = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;

        yield* client.handleServerRequest("item/tool/requestUserInput", (payload) =>
          Ref.update(userInputRequests, (current) => [...current, payload]).pipe(
            Effect.as({
              answers: {
                approved: {
                  answers: ["yes"],
                },
              },
            }),
          ),
        );

        yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
          Ref.update(messageDeltas, (current) => [...current, payload]),
        );

        const initialized = yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
        assert.equal(initialized.userAgent, "mock-codex-app-server");

        yield* client.notify("initialized", undefined);

        const account = yield* client.request("account/read", {});
        assert.equal(account.requiresOpenaiAuth, false);
        assert.deepEqual(account.account, {
          type: "chatgpt",
          email: "mock@example.com",
          planType: "plus",
        });

        const path = yield* Path.Path;
        const peerCwd = path.join(import.meta.dirname, "..");
        const skills = yield* client.request("skills/list", { cwds: [peerCwd] });
        assert.equal(skills.data.length, 1);
        assert.equal(skills.data[0]?.cwd, peerCwd);

        return {
          account,
          skills,
        };
      }).pipe(Effect.provide(context), Effect.ensuring(Scope.close(scope, Exit.void)));

      assert.equal(result.skills.data[0]?.skills.length, 0);
      assert.deepEqual(yield* Ref.get(userInputRequests), [
        {
          isBlocking: true,
          itemId: "item-approval-1",
          threadId: "thread-1",
          turnId: "turn-1",
          questions: [
            {
              id: "approved",
              header: "Approve",
              question: "Continue with the mock skills request?",
              options: [
                {
                  label: "yes",
                  description: "Approve the request",
                },
              ],
            },
          ],
        },
      ]);
      assert.deepEqual(yield* Ref.get(messageDeltas), [
        {
          delta: "Mock server is ready.",
          itemId: "item-1",
          threadId: "thread-1",
          turnId: "turn-1",
        },
      ]);
    }),
  );
  it.effect("reports the actual child exit status through the client callback", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const handle = yield* spawner.spawn(
        ChildProcess.make(process.execPath, [
          "-e",
          'process.stdin.once("data", () => process.exit(23))',
        ]),
      );
      const terminated = yield* Deferred.make<CodexError.CodexAppServerError>();
      const context = yield* Layer.build(
        CodexClient.layerChildProcess(handle, {
          onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
        }),
      );
      const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
        Effect.provide(context),
      );
      const rejected = yield* client.raw.request("synthetic/exit").pipe(Effect.result);
      const error = yield* Deferred.await(terminated);
      assert.equal(error._tag, "CodexAppServerProcessExitedError");
      if (error._tag === "CodexAppServerProcessExitedError") assert.equal(error.code, 23);
      assert.isTrue(Result.isFailure(rejected));
      if (Result.isFailure(rejected)) assert.equal(rejected.failure, error);
    }),
  );

  it.effect("drains child stderr so large diagnostics cannot block protocol responses", () =>
    Effect.gen(function* () {
      const handle = yield* makeHandle({
        CODEX_APP_SERVER_TEST_STDERR_BYTES: String(512 * 1024),
      });
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const initialized = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;
        return yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
      }).pipe(
        Effect.timeout("5 seconds"),
        Effect.provide(context),
        Effect.ensuring(Scope.close(scope, Exit.void)),
      );

      assert.equal(initialized.userAgent, "mock-codex-app-server");
    }),
  );
});

it.effect("forwards transport termination to the client owner exactly once", () =>
  Effect.gen(function* () {
    const { stdio, input } = yield* makeInMemoryStdio();
    const observed: Array<CodexError.CodexAppServerError> = [];
    const error = new CodexError.CodexAppServerProcessExitedError({ code: 7 });
    const client = yield* CodexClient.make(
      stdio,
      {
        onTermination: (cause) =>
          Effect.sync(() => {
            observed.push(cause);
          }),
      },
      Effect.succeed(error),
    );
    yield* Queue.end(input);
    for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
    assert.deepEqual(observed, [error]);
    const rejected = yield* client.raw.request("turn/start", {}).pipe(Effect.result);
    assert.isTrue(Result.isFailure(rejected));
    if (Result.isFailure(rejected)) assert.equal(rejected.failure, error);
    assert.equal(observed.length, 1);
  }).pipe(Effect.scoped),
);
