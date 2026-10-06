import { describe, expect, it, vi } from "vite-plus/test";
import { it as effectIt } from "@effect/vitest";
import {
  AuthSessionId,
  VoiceReviewForbiddenError,
  VoiceReviewNotConfiguredError,
  VoiceReviewNotFoundError,
  VoiceReviewConflictError,
  VoiceReviewUnavailableError,
  type EnvironmentSessionPrincipalShape,
  type VoiceReviewDraft,
} from "@t3tools/contracts";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native fixtures prove credential permission and symlink defenses, with exact-root cleanup in finally.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off - native path joins bind permission and symlink fixtures to their exact cleanup-owned root.
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { makeVoiceReviewBridge } from "./bridge.ts";
import * as VoiceReview from "./bridge.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { voiceReviewConfigFromEnv } from "./config.ts";

const principal: EnvironmentSessionPrincipalShape = {
  sessionId: AuthSessionId.make("owner-session"),
  subject: "generic-subject",
  method: "bearer-access-token",
  scopes: new Set(["orchestration:read", "orchestration:operate"]),
};
const draft: VoiceReviewDraft = {
  id: "capture",
  source_id: "microphone",
  state: "held",
  revision: 1,
  text: "literal transcript",
  transcript_provider: "fixture",
  language: null,
  edited: false,
  created_at: "2026-10-02T00:00:00Z",
  updated_at: "2026-10-02T00:00:00Z",
  due_at: "2026-10-02T00:03:00Z",
  remaining_ms: 180_000,
  expires_at: "2026-10-03T00:00:00Z",
  command_id: null,
  command_status: null,
  reason: null,
  server_now: "2026-10-02T00:00:00Z",
};
const withFixture = async (run: (tokenFile: string) => Promise<void>) => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "voice-review-"));
  try {
    const tokenFile = NodePath.join(root, "reviewer-token");
    await NodeFSP.writeFile(tokenFile, "fixture-reviewer-token\n", { mode: 0o600 });
    await run(tokenFile);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
};
const config = (reviewer_token_file: string) => ({
  broker_url: "http://127.0.0.1:7000",
  reviewer_token_file,
  source_id: "microphone",
  allowed_session_ids: new Set(["owner-session"]),
});

describe("voice review server bridge", () => {
  it("defaults deny and validates only a trusted origin and exact session enrollment", () => {
    expect(voiceReviewConfigFromEnv({})).toBeNull();
    const env = {
      T3CODE_VOICE_REVIEW_BROKER_URL: "http://127.0.0.1:7000",
      T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE: "/fixture/token",
      T3CODE_VOICE_REVIEW_SOURCE_ID: "microphone",
      T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: '["owner-session"]',
    };
    expect(voiceReviewConfigFromEnv(env)?.allowed_session_ids.has("owner-session")).toBe(true);
    for (const url of [
      "http://remote.test",
      "http://localhost",
      "https://user:pass@remote.test",
      "https://remote.test/path",
      "https://remote.test/?url=other",
    ]) {
      expect(voiceReviewConfigFromEnv({ ...env, T3CODE_VOICE_REVIEW_BROKER_URL: url })).toBeNull();
    }
    expect(
      voiceReviewConfigFromEnv({ ...env, T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: '["*"]' }),
    ).toBeNull();
    expect(
      voiceReviewConfigFromEnv({ ...env, T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: "[]" }),
    ).toBeNull();
  });
  it("checks session identity and read/operate scopes before credential access or requests", async () => {
    const fetcher = vi.fn<typeof fetch>();
    await expect(
      makeVoiceReviewBridge(null, fetcher).get(principal, "capture"),
    ).rejects.toBeInstanceOf(VoiceReviewNotConfiguredError);
    const bridge = makeVoiceReviewBridge(config("/not-read"), fetcher);
    await expect(
      bridge.get({ ...principal, sessionId: AuthSessionId.make("other") }, "capture"),
    ).rejects.toBeInstanceOf(VoiceReviewForbiddenError);
    await expect(bridge.get({ ...principal, scopes: new Set() }, "capture")).rejects.toBeInstanceOf(
      VoiceReviewForbiddenError,
    );
    await expect(
      bridge.mutate({ ...principal, scopes: new Set(["orchestration:read"]) }, "capture", "pause", {
        expected_revision: 1,
      }),
    ).rejects.toBeInstanceOf(VoiceReviewForbiddenError);
    expect(fetcher).not.toHaveBeenCalled();
  });
  it("keeps credentials server-only, filters fixed source, and blocks a foreign draft before mutation", async () =>
    withFixture(async (tokenFile) => {
      const calls: string[] = [];
      const fetcher = vi.fn<typeof fetch>(async (url, init) => {
        calls.push(String(url));
        expect(init?.redirect).toBe("error");
        expect(init?.cache).toBe("no-store");
        expect(new Headers(init?.headers).get("authorization")).toBe(
          "Bearer fixture-reviewer-token",
        );
        return Response.json(
          String(url).includes("?scope")
            ? {
                server_now: draft.server_now,
                drafts: [draft, { ...draft, id: "foreign", source_id: "other" }],
              }
            : { ...draft, source_id: "other" },
        );
      });
      const bridge = makeVoiceReviewBridge(config(tokenFile), fetcher);
      expect((await bridge.list(principal, "pending", 50)).drafts).toEqual([draft]);
      await expect(
        bridge.mutate(principal, "capture", "send-now", { expected_revision: 1 }),
      ).rejects.toBeInstanceOf(VoiceReviewNotFoundError);
      expect(calls).toHaveLength(2);
      expect(fetcher.mock.calls.every(([, init]) => init?.method === "GET")).toBe(true);
    }));
  it("rejects nonprivate and symlinked credentials without contacting the broker", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>();
      await NodeFSP.chmod(tokenFile, 0o644);
      await expect(
        makeVoiceReviewBridge(config(tokenFile), fetcher).get(principal, "capture"),
      ).rejects.toBeInstanceOf(VoiceReviewUnavailableError);
      await NodeFSP.chmod(tokenFile, 0o600);
      const link = `${tokenFile}-link`;
      await NodeFSP.symlink(tokenFile, link);
      await expect(
        makeVoiceReviewBridge(config(link), fetcher).get(principal, "capture"),
      ).rejects.toBeInstanceOf(VoiceReviewUnavailableError);
      expect(fetcher).not.toHaveBeenCalled();
    }));
  it("forwards one strict revision mutation and sanitizes broker rejection without retry", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>(async (_url, init) =>
        init?.method === "GET"
          ? Response.json(draft)
          : new Response("private exception and transcript", { status: 409 }),
      );
      const bridge = makeVoiceReviewBridge(config(tokenFile), fetcher);
      await expect(
        bridge.mutate(principal, "capture", "pause", { expected_revision: 1 }),
      ).rejects.toBeInstanceOf(VoiceReviewConflictError);
      expect(fetcher).toHaveBeenCalledTimes(2);
      expect(fetcher.mock.calls[1]?.[1]?.body).toBe('{"expected_revision":1}');
      for (const status of [401, 403, 500, 302]) {
        const rejected = makeVoiceReviewBridge(
          config(tokenFile),
          async () => new Response("private detail", { status }),
        );
        await expect(rejected.get(principal, "capture")).rejects.toMatchObject({
          _tag: "VoiceReviewUnavailableError",
        });
      }
    }));
  it("an ambiguous mutation has no automatic retry or replacement submission", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>(async (_url, init) => {
        if (init?.method === "GET") return Response.json(draft);
        throw new Error("connection lost after possible effect: secret transcript");
      });
      await expect(
        makeVoiceReviewBridge(config(tokenFile), fetcher).mutate(principal, "capture", "send-now", {
          expected_revision: 1,
        }),
      ).rejects.toMatchObject({ _tag: "VoiceReviewUnavailableError" });
      expect(fetcher).toHaveBeenCalledTimes(2);
    }));
  it("rejects oversized response before decoding", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>(
        async () => new Response("ignored", { headers: { "content-length": "24000001" } }),
      );
      await expect(
        makeVoiceReviewBridge(config(tokenFile), fetcher).get(principal, "capture"),
      ).rejects.toBeInstanceOf(VoiceReviewUnavailableError);
    }));
});

effectIt.effect("provides broker operations through the injected Effect service", () =>
  Effect.gen(function* () {
    const root = yield* Effect.acquireRelease(
      Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "voice-review-"))),
      (root) => Effect.promise(() => NodeFSP.rm(root, { recursive: true, force: true })),
    );
    const tokenFile = NodePath.join(root, "reviewer-token");
    yield* Effect.promise(() =>
      NodeFSP.writeFile(tokenFile, "fixture-reviewer-token\n", { mode: 0o600 }),
    ).pipe(Effect.uninterruptible);
    const fetcher = vi.fn<typeof fetch>(async () => Response.json(draft));
    const serviceLayer = VoiceReview.layer.pipe(
      Layer.provide(
        Layer.succeed(VoiceReview.VoiceReviewDependencies, {
          config: config(tokenFile),
          fetcher,
        }),
      ),
    );
    const result = yield* Effect.gen(function* () {
      const service = yield* VoiceReview.VoiceReview;
      return yield* service.get(principal, "capture");
    }).pipe(Effect.provide(serviceLayer));
    expect(result).toEqual(draft);
    expect(fetcher).toHaveBeenCalledTimes(1);
  }).pipe(Effect.scoped),
);
