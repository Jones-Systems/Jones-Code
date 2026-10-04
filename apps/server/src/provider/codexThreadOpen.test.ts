import { assert, describe, it } from "@effect/vitest";
import { ThreadId, type RuntimeMode } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as CodexErrors from "effect-codex-app-server/errors";
import type * as CodexRpc from "effect-codex-app-server/rpc";

import { CodexResumeCursorSchema, openCodexThread } from "./codexThreadOpen.ts";

const metadata = {
  cwd: "/historical-worktree",
  model: "historical-model",
  modelProvider: "openai",
  serviceTier: "fast",
  thread: { id: "native-history-id" },
};
const requestInput = {
  threadId: ThreadId.make("application-thread-id"),
  runtimeMode: "full-access" as const,
  cwd: metadata.cwd,
  requestedModel: metadata.model,
  serviceTier: "fast" as const,
  resumeThreadId: metadata.thread.id,
  requireResume: true,
};

function makeClient(response: unknown = metadata, failure?: CodexErrors.CodexAppServerError) {
  const calls: Array<{ readonly method: string; readonly payload: unknown }> = [];
  const client: Parameters<typeof openCodexThread>[0]["client"] = {
    raw: {
      request: (method, payload) =>
        Effect.suspend(() => {
          calls.push({ method, payload });
          return failure === undefined ? Effect.succeed(response) : Effect.fail(failure);
        }),
    },
    request: (method, payload) =>
      Effect.sync(() => {
        calls.push({ method, payload });
        return metadata as CodexRpc.ClientRequestResponsesByMethod["thread/start"];
      }),
  };
  return { client, calls };
}

const runtimeModes = [
  ["approval-required", "untrusted", "read-only", "user"],
  ["auto-accept-edits", "on-request", "workspace-write", "user"],
  ["auto", "on-request", "workspace-write", "auto_review"],
  ["full-access", "never", "danger-full-access", "user"],
] as const satisfies ReadonlyArray<readonly [RuntimeMode, string, string, string]>;

describe("historical Codex thread request compatibility", () => {
  it("decodes the persisted native cursor without substituting the application ID", () => {
    assert.deepEqual(
      Schema.decodeUnknownSync(CodexResumeCursorSchema)({ threadId: metadata.thread.id }),
      { threadId: metadata.thread.id },
    );
    assert.isFalse(Schema.is(CodexResumeCursorSchema)({ threadId: 12 }));
    assert.isFalse(Schema.is(CodexResumeCursorSchema)({}));
  });

  for (const [runtimeMode, approvalPolicy, sandbox, approvalsReviewer] of runtimeModes) {
    for (const resume of [true, false]) {
      it.effect(
        `${resume ? "resumes" : "starts without a cursor"} with the ${runtimeMode} request policy`,
        () =>
          Effect.gen(function* () {
            const fake = makeClient();
            const result = yield* openCodexThread({
              ...requestInput,
              client: fake.client,
              runtimeMode,
              resumeThreadId: resume ? metadata.thread.id : undefined,
            });
            assert.equal(result.thread.id, metadata.thread.id);
            assert.deepEqual(fake.calls, [
              {
                method: resume ? "thread/resume" : "thread/start",
                payload: {
                  ...(resume ? { threadId: metadata.thread.id } : {}),
                  cwd: metadata.cwd,
                  approvalPolicy,
                  sandbox,
                  approvalsReviewer,
                  model: metadata.model,
                  serviceTier: "fast",
                  ...(resume ? { excludeTurns: true } : {}),
                },
              },
            ]);
          }),
      );
    }
  }

  it.effect(
    "decodes only resume metadata when the provider returns unsupported historical items",
    () =>
      Effect.gen(function* () {
        const fake = makeClient({ ...metadata, turns: [{ unsupportedHistoricalItem: true }] });
        assert.deepEqual(
          yield* openCodexThread({ ...requestInput, client: fake.client }),
          metadata,
        );
        assert.equal(fake.calls[0]?.method, "thread/resume");
        assert.equal(fake.calls.length, 1);
      }),
  );

  for (const field of ["cwd", "model", "modelProvider", "thread"] as const) {
    it.effect(`rejects malformed ${field} resume metadata without a fresh start`, () =>
      Effect.gen(function* () {
        const fake = makeClient({ ...metadata, [field]: field === "thread" ? { id: 12 } : null });
        const result = yield* openCodexThread({ ...requestInput, client: fake.client }).pipe(
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) {
          const error = Cause.squash(result.cause);
          assert.isTrue(Schema.is(CodexErrors.CodexAppServerRequestError)(error));
        }
        assert.deepEqual(
          fake.calls.map((call) => call.method),
          ["thread/resume"],
        );
      }),
    );
  }

  it.effect(
    "preserves a required resume failure even when its missing-thread error is recoverable",
    () =>
      Effect.gen(function* () {
        const error = new CodexErrors.CodexAppServerRequestError({
          code: -32_000,
          errorMessage: "thread not found",
        });
        const fake = makeClient(metadata, error);
        const result = yield* openCodexThread({ ...requestInput, client: fake.client }).pipe(
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) assert.strictEqual(Cause.squash(result.cause), error);
        assert.deepEqual(
          fake.calls.map((call) => call.method),
          ["thread/resume"],
        );
      }),
  );

  it.effect("retains the original optional missing-thread fallback with the same settings", () =>
    Effect.gen(function* () {
      const fake = makeClient(
        metadata,
        new CodexErrors.CodexAppServerRequestError({
          code: -32_000,
          errorMessage: "thread not found",
        }),
      );
      assert.deepEqual(
        yield* openCodexThread({ ...requestInput, requireResume: false, client: fake.client }),
        metadata,
      );
      assert.deepEqual(
        fake.calls.map((call) => call.method),
        ["thread/resume", "thread/start"],
      );
      assert.deepEqual(fake.calls[1]?.payload, {
        cwd: metadata.cwd,
        model: metadata.model,
        serviceTier: "fast",
        approvalPolicy: "never",
        sandbox: "danger-full-access",
        approvalsReviewer: "user",
      });
    }),
  );

  for (const requireResume of [true, false]) {
    it.effect(`never retries an unrelated RPC failure (required=${requireResume})`, () =>
      Effect.gen(function* () {
        const error = new CodexErrors.CodexAppServerRequestError({
          code: -32_000,
          errorMessage: "provider connection unavailable",
        });
        const fake = makeClient(metadata, error);
        const result = yield* openCodexThread({
          ...requestInput,
          requireResume,
          client: fake.client,
        }).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) assert.strictEqual(Cause.squash(result.cause), error);
        assert.deepEqual(
          fake.calls.map((call) => call.method),
          ["thread/resume"],
        );
      }),
    );
  }

  for (const resume of [true, false]) {
    it.effect(`omits unspecified model and tier for ${resume ? "resume" : "start"}`, () =>
      Effect.gen(function* () {
        const fake = makeClient({ ...metadata, serviceTier: null });
        yield* openCodexThread({
          ...requestInput,
          client: fake.client,
          requestedModel: undefined,
          serviceTier: undefined,
          resumeThreadId: resume ? metadata.thread.id : undefined,
        });
        assert.deepEqual(fake.calls[0]?.payload, {
          ...(resume ? { threadId: metadata.thread.id } : {}),
          cwd: metadata.cwd,
          approvalPolicy: "never",
          sandbox: "danger-full-access",
          approvalsReviewer: "user",
          ...(resume ? { excludeTurns: true } : {}),
        });
      }),
    );
  }
});
