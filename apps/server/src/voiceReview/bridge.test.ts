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
  ThreadRegistryAssociationPayload,
  ThreadRegistryLabelPayload,
  ThreadRegistrySnapshot,
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
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";

vi.mock("node:fs/promises", { spy: true });

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
const config = (reviewer_token_file: string) =>
  voiceReviewConfigFromEnv({
    T3CODE_VOICE_REVIEW_BROKER_URL: "http://127.0.0.1:7000",
    T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE: reviewer_token_file,
    T3CODE_VOICE_REVIEW_SOURCE_ID: "microphone",
    T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: '["owner-session"]',
  })!;

describe("voice review server bridge", () => {
  it("accepts broker configuration without enrollment and ignores stale allowlist variables", () => {
    const env = {
      T3CODE_VOICE_REVIEW_BROKER_URL: "http://127.0.0.1:7000",
      T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE: "/fixture/token",
      T3CODE_VOICE_REVIEW_SOURCE_ID: "microphone",
    };
    const expected = {
      broker_url: "http://127.0.0.1:7000",
      reviewer_token_file: "/fixture/token",
      source_id: "microphone",
    };
    expect(voiceReviewConfigFromEnv(env)).toEqual(expected);
    for (const legacy of [
      "",
      "[]",
      "not-json",
      '["*"]',
      '["old-session"]',
      '[{"subject":"cloud-connect","proof_key_thumbprint":"old-key"}]',
    ]) {
      expect(
        voiceReviewConfigFromEnv({
          ...env,
          T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS: legacy,
          T3CODE_VOICE_REVIEW_ALLOWED_DPOP_IDENTITIES: legacy,
        }),
      ).toEqual(expected);
    }
    expect(voiceReviewConfigFromEnv({})).toBeNull();
    for (const url of [
      "http://remote.test",
      "http://localhost",
      "https://user:pass@remote.test",
      "https://remote.test/path",
      "https://remote.test/?url=other",
    ]) {
      expect(voiceReviewConfigFromEnv({ ...env, T3CODE_VOICE_REVIEW_BROKER_URL: url })).toBeNull();
    }
    for (const file of ["", "relative-token"]) {
      expect(
        voiceReviewConfigFromEnv({ ...env, T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE: file }),
      ).toBeNull();
    }
    for (const source of ["", " source", "*", "x".repeat(257)]) {
      expect(
        voiceReviewConfigFromEnv({ ...env, T3CODE_VOICE_REVIEW_SOURCE_ID: source }),
      ).toBeNull();
    }
  });
  it("accepts scoped cookie bearer and DPoP principals across sessions and proof keys", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>(async (_url, init) =>
        Response.json(init?.method === "POST" ? { draft, edit_handle: null } : draft),
      );
      const bridge = makeVoiceReviewBridge(config(tokenFile), fetcher);
      for (const method of [
        "browser-session-cookie",
        "bearer-access-token",
        "dpop-access-token",
      ] as const) {
        for (const session of ["before-renewal", "after-renewal"]) {
          for (const proofKeyThumbprint of ["first-key", "another-key"]) {
            const device = {
              ...principal,
              method,
              subject: "cloud-connect",
              sessionId: AuthSessionId.make(session),
              proofKeyThumbprint,
            };
            await expect(bridge.get(device, "capture")).resolves.toEqual(draft);
            await expect(
              bridge.mutate(device, "capture", "pause", { expected_revision: 1 }),
            ).resolves.toEqual({ draft, edit_handle: null });
          }
        }
      }
      expect(fetcher).toHaveBeenCalledTimes(36);
    }));
  it("checks read and operate scopes before credential access or requests", async () => {
    const fetcher = vi.fn<typeof fetch>();
    const open = vi.spyOn(NodeFSP, "open");
    try {
      await expect(
        makeVoiceReviewBridge(null, fetcher).get(principal, "capture"),
      ).rejects.toBeInstanceOf(VoiceReviewNotConfiguredError);
      const bridge = makeVoiceReviewBridge(config("/not-read"), fetcher);
      await expect(
        bridge.get({ ...principal, scopes: new Set() }, "capture"),
      ).rejects.toBeInstanceOf(VoiceReviewForbiddenError);
      await expect(
        bridge.get({ ...principal, scopes: new Set(["orchestration:operate"]) }, "capture"),
      ).rejects.toBeInstanceOf(VoiceReviewForbiddenError);
      await expect(
        bridge.mutate(
          { ...principal, scopes: new Set(["orchestration:read"]) },
          "capture",
          "pause",
          {
            expected_revision: 1,
          },
        ),
      ).rejects.toBeInstanceOf(VoiceReviewForbiddenError);
      expect(open).not.toHaveBeenCalled();
      expect(fetcher).not.toHaveBeenCalled();
    } finally {
      open.mockRestore();
    }
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

describe("voice routing and registry bridge", () => {
  it("retains released command text while redacting deleted, expired, unavailable and foreign text", async () =>
    withFixture(async (tokenFile) => {
      const entry = (state: VoiceReviewDraft["state"], source_id = "microphone") => ({
        draft: { ...draft, state, source_id, text: "draft text", command_id: "command" },
        text: "retained literal command",
        original_source_text: "original transcript",
        text_state: "available",
        text_origin: "retained_command",
        command_id: "command",
        workstream_refs: ["inferred:voice"],
      });
      const bridge = makeVoiceReviewBridge(config(tokenFile), async () =>
        Response.json({
          schema: "voice.recent-prompts/v1",
          server_now: draft.server_now,
          partial: false,
          unavailable: [],
          entries: [
            entry("released"),
            entry("deleted"),
            entry("expired"),
            { ...entry("released"), text_state: "unavailable" },
            entry("released", "foreign"),
          ],
        }),
      );
      const result = await bridge.recent(principal);
      expect(result.entries).toHaveLength(4);
      expect(result.entries[0]?.text).toBe("retained literal command");
      expect(result.entries[0]?.original_source_text).toBe("original transcript");
      for (const item of result.entries.slice(1)) {
        expect(item.text).toBeNull();
        expect(item.original_source_text).toBeNull();
        expect(item.draft.text).toBeNull();
      }
    }));
  it("keeps a native read outage explicit and accepts additive response fields", async () =>
    withFixture(async (tokenFile) => {
      const snapshot = {
        schema: "voice.registry-read/v1",
        snapshot_revision: 3,
        next_cursor: null,
        partial: false,
        unavailable: [],
        extension: "future",
        threads: [
          {
            thread_key: '["host","environment","thread"]',
            registration: null,
            summary: null,
            activity: null,
            freshness: {},
            associations: [],
          },
        ],
      };
      expect(Schema.decodeUnknownSync(ThreadRegistrySnapshot)(snapshot).threads).toHaveLength(1);
      const bridge = makeVoiceReviewBridge(config(tokenFile), async () => Response.json(snapshot));
      const result = await bridge.registrySnapshot(principal);
      expect(result.partial).toBe(true);
      expect(result.unavailable).toContain("native_memberships");
      expect(result.threads[0]?.native_memberships).toEqual([]);
    }));
  it("composes only qualified native placements with a separate namespace", async () =>
    withFixture(async (tokenFile) => {
      const key = '["host","environment","thread"]';
      const fixedNow = DateTime.makeUnsafe("2026-10-02T12:00:00Z");
      const placement = {
        membership_id: "membership",
        workstream_id: "voice",
        native_reference_id: "reference",
        kind: "primary" as const,
        source_instance_id: "environment",
        native_thread_id: "thread",
        attestation_version: 1,
        attested_at: DateTime.formatIso(DateTime.add(fixedNow, { minutes: -1 })),
        expires_at: DateTime.formatIso(DateTime.add(fixedNow, { minutes: 1 })),
        evidence_sha256: "a".repeat(64),
        source_binding_version: 1,
        authority_namespace: "authority",
        store_generation: 1,
      };
      const read = vi.fn(async () => ({
        page: {
          inventory_sha256: "b".repeat(64),
          context: {
            owner_id: "owner",
            principal_id: "principal",
            authorization_revision: 1,
            server_generation: 1,
            registry_version: 1,
          },
          items: [placement],
          next_cursor: null,
        },
        trustedEnvironments: [
          { environmentId: "environment", authorityNamespace: "authority", storeGeneration: 1 },
        ],
        readiness: "ready" as const,
      }));
      const bridge = makeVoiceReviewBridge(
        config(tokenFile),
        async () =>
          Response.json({
            schema: "voice.registry-read/v1",
            snapshot_revision: 1,
            next_cursor: null,
            partial: false,
            unavailable: [],
            threads: [
              {
                thread_key: key,
                registration: null,
                summary: null,
                activity: null,
                freshness: {},
                associations: [
                  {
                    subject: `thread:${key}`,
                    workstream_ref: "inferred:voice",
                    state: "active",
                    revision: 1,
                    origin: "owner",
                    job_id: null,
                  },
                ],
              },
            ],
          }),
        {
          identities: () =>
            new Map([[key, { source_instance_id: "environment", native_thread_id: "thread" }]]),
          read,
        },
        () => DateTime.toEpochMillis(fixedNow),
      );
      const result = await bridge.registrySnapshot(principal);
      expect(read).toHaveBeenCalledWith(principal, [
        { source_instance_id: "environment", native_thread_id: "thread" },
      ]);
      expect(result.threads[0]?.native_memberships[0]?.workstream_ref).toBe("native:voice");
      expect(result.threads[0]?.associations[0]?.workstream_ref).toBe("inferred:voice");
      expect(result.unavailable).toEqual([]);
    }));
  it("forwards one CAS/idempotent metadata correction without draft or delivery requests", async () =>
    withFixture(async (tokenFile) => {
      const payload = {
        schema: "voice.association-mutation/v1",
        subject: "prompt:command",
        workstream_ref: "inferred:voice",
        state: "suppressed",
        expected_revision: 2,
        request_id: "correction",
        command_id: "command",
        receipt_id: "receipt",
      } as const;
      const fetcher = vi.fn<typeof fetch>(async (_url, init) => {
        expect(JSON.parse(String(init?.body))).toEqual(payload);
        return Response.json({
          schema: "voice.registry-receipt/v1",
          request_id: "correction",
          revision: 3,
          event_sequence: 4,
          record: { ...payload, revision: 3, origin: "owner", job_id: null },
        });
      });
      await makeVoiceReviewBridge(config(tokenFile), fetcher).correctAssociation(
        principal,
        payload,
      );
      expect(fetcher).toHaveBeenCalledTimes(1);
      expect(String(fetcher.mock.calls[0]?.[0])).toBe(
        "http://127.0.0.1:7000/v1/registry/associations?source_id=microphone",
      );
      expect(fetcher.mock.calls[0]?.[1]?.method).toBe("POST");
    }));
  it("scopes the single label correction POST to the configured reviewer source", async () =>
    withFixture(async (tokenFile) => {
      const payload = {
        schema: "voice.association-mutation/v1",
        label_id: "inferred:voice",
        name: "Voice",
        description: "Routing metadata",
        state: "active",
        expected_revision: 0,
        request_id: "label-correction",
      } as const;
      const fetcher = vi.fn<typeof fetch>(async (_url, init) => {
        expect(JSON.parse(String(init?.body))).toEqual(payload);
        return Response.json({
          schema: "voice.registry-receipt/v1",
          request_id: payload.request_id,
          revision: 1,
          event_sequence: 5,
          record: { ...payload, revision: 1, origin: "owner" },
        });
      });
      await makeVoiceReviewBridge(config(tokenFile), fetcher).correctLabel(principal, payload);
      expect(fetcher).toHaveBeenCalledTimes(1);
      expect(String(fetcher.mock.calls[0]?.[0])).toBe(
        "http://127.0.0.1:7000/v1/registry/labels?source_id=microphone",
      );
      expect(fetcher.mock.calls[0]?.[1]?.method).toBe("POST");
    }));
  it("rejects native inferred corrections and extra fields before transport", async () => {
    const association = {
      schema: "voice.association-mutation/v1",
      subject: "prompt:command",
      workstream_ref: "inferred:voice",
      state: "active",
      expected_revision: 0,
      request_id: "new",
    };
    const decode = Schema.decodeUnknownSync(ThreadRegistryAssociationPayload);
    expect(() => decode({ ...association, workstream_ref: "native:voice" })).toThrow();
    expect(() => decode({ ...association, dispatch: true })).toThrow();
    expect(() => decode({ ...association, expected_revision: "0" })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(ThreadRegistryLabelPayload)({
        schema: "voice.association-mutation/v1",
        label_id: "native:voice",
        name: "name",
        description: "",
        state: "active",
        expected_revision: 0,
        request_id: "new",
      }),
    ).toThrow();
    const fetcher = vi.fn<typeof fetch>();
    await expect(
      makeVoiceReviewBridge(config("/not-read"), fetcher).correctAssociation(principal, {
        ...association,
        workstream_ref: "native:voice",
      } as typeof ThreadRegistryAssociationPayload.Type),
    ).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
  });
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

describe("voice command queue receipt enrichment", () => {
  const recentEntry = (
    command_id: string | null,
    state: VoiceReviewDraft["state"] = "released",
  ) => ({
    draft: { ...draft, state, command_id },
    text: "retained transcript",
    original_source_text: null,
    text_state: "available",
    text_origin: "retained_command",
    command_id,
    workstream_refs: [],
  });
  const recent = (entries: ReturnType<typeof recentEntry>[]) => ({
    schema: "voice.recent-prompts/v1",
    server_now: draft.server_now,
    partial: false,
    unavailable: [],
    entries,
  });
  const delivery = {
    id: "delivery",
    queue_request_id: "queue-request",
    payload_hash: "b".repeat(64),
    queue_payload_sha256: "a".repeat(64),
    destination: { backend: "codex", instance: "worker", thread_id: "thread", project_id: null },
    prompt: "private prompt",
    detail: "private detail",
    result: { private: true },
  };
  const command = (id = "command") => ({
    id,
    source_id: "microphone",
    transcript: "private command transcript",
    deliveries: [delivery],
  });
  it("deduplicates lookups, projects only explicit queue metadata and skips terminal entries", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>(async (url, init) => {
        expect(init?.method).toBe("GET");
        expect(new Headers(init?.headers).get("authorization")).toBe(
          "Bearer fixture-reviewer-token",
        );
        return Response.json(
          String(url).includes("/recent?")
            ? recent([
                recentEntry("command"),
                recentEntry("command"),
                recentEntry(null),
                recentEntry("deleted-command", "deleted"),
                recentEntry("expired-command", "expired"),
              ])
            : {
                ...command(),
                deliveries: [delivery, { ...delivery, id: "nonqueue", queue_request_id: null }],
              },
        );
      });
      const result = await makeVoiceReviewBridge(config(tokenFile), fetcher).recent(principal);
      expect(fetcher).toHaveBeenCalledTimes(2);
      expect(fetcher.mock.calls[1]?.[0]).toBe("http://127.0.0.1:7000/v1/commands/command");
      expect(result.entries[0]?.queue_delivery_links).toEqual({
        status: "ready",
        reason: null,
        unlinked_delivery_count: 1,
        deliveries: [
          {
            delivery_id: "delivery",
            queue_request_id: "queue-request",
            payload_sha256: "a".repeat(64),
            destination: delivery.destination,
          },
        ],
      });
      expect(result.entries[1]?.queue_delivery_links).toEqual(
        result.entries[0]?.queue_delivery_links,
      );
      expect(result.entries[2]?.queue_delivery_links).toBeNull();
      expect(result.entries[3]?.text).toBeNull();
      expect(result.entries[4]?.text).toBeNull();
      expect(JSON.stringify(result)).not.toContain("private");
    }));
  it("uses only the submitted queue digest and counts missing legacy digests as unlinked", async () =>
    withFixture(async (tokenFile) => {
      const { queue_payload_sha256: _digest, ...legacy } = delivery;
      const fetcher = vi.fn<typeof fetch>(async (url) =>
        Response.json(
          String(url).includes("/recent?")
            ? recent([recentEntry("command")])
            : {
                ...command(),
                deliveries: [
                  delivery,
                  { ...legacy, id: "legacy", payload_hash: delivery.queue_payload_sha256 },
                  { ...delivery, id: "missing", queue_payload_sha256: null },
                ],
              },
        ),
      );
      const result = await makeVoiceReviewBridge(config(tokenFile), fetcher).recent(principal);
      expect(result.entries[0]?.text).toBe("retained transcript");
      expect(result.entries[0]?.queue_delivery_links).toEqual({
        status: "ready",
        reason: null,
        unlinked_delivery_count: 2,
        deliveries: [
          {
            delivery_id: delivery.id,
            queue_request_id: delivery.queue_request_id,
            payload_sha256: delivery.queue_payload_sha256,
            destination: delivery.destination,
          },
        ],
      });
      expect(delivery.payload_hash).not.toBe(delivery.queue_payload_sha256);
    }));
  it("counts prepared and failed siblings without queue IDs while preserving explicit links", async () =>
    withFixture(async (tokenFile) => {
      const fetcher = vi.fn<typeof fetch>(async (url) =>
        Response.json(
          String(url).includes("/recent?")
            ? recent([recentEntry("command")])
            : {
                ...command(),
                deliveries: [
                  { ...delivery, status: "accepted" },
                  { ...delivery, id: "prepared", queue_request_id: null, status: "PREPARED" },
                  { ...delivery, id: "failed", queue_request_id: null, status: "FAILED" },
                ],
              },
        ),
      );
      const result = await makeVoiceReviewBridge(config(tokenFile), fetcher).recent(principal);
      expect(result.entries[0]?.queue_delivery_links).toEqual({
        status: "ready",
        reason: null,
        unlinked_delivery_count: 2,
        deliveries: [
          {
            delivery_id: "delivery",
            queue_request_id: "queue-request",
            payload_sha256: "a".repeat(64),
            destination: delivery.destination,
          },
        ],
      });
      expect(result.entries[0]?.text).toBe("retained transcript");
      expect(fetcher).toHaveBeenCalledTimes(2);
      expect(JSON.stringify(result)).not.toMatch(/private|PREPARED|FAILED/);
    }));
  it("preserves recent text and exposes source denial, invalid identity and malformed receipts", async () =>
    withFixture(async (tokenFile) => {
      const variants: readonly [() => Response, string][] = [
        [() => new Response("private", { status: 403 }), "forbidden"],
        [() => new Response("private", { status: 500 }), "source_unavailable"],
        [() => Response.json({ ...command(), source_id: "other" }), "invalid_receipt"],
        [() => Response.json(command("other")), "invalid_receipt"],
        [
          () =>
            Response.json({
              ...command(),
              deliveries: [{ ...delivery, queue_payload_sha256: "wrong" }],
            }),
          "invalid_receipt",
        ],
        [
          () =>
            Response.json({
              ...command(),
              deliveries: [{ ...delivery, queue_request_id: undefined }],
            }),
          "invalid_receipt",
        ],
        [() => new Response("not json"), "invalid_receipt"],
        [() => Response.json({ ...command(), deliveries: Array(201).fill(delivery) }), "limit"],
      ];
      for (const [response, reason] of variants) {
        const fetcher = vi.fn<typeof fetch>(async (url) =>
          String(url).includes("/recent?")
            ? Response.json(recent([recentEntry("command")]))
            : response(),
        );
        const result = await makeVoiceReviewBridge(config(tokenFile), fetcher).recent(principal);
        expect(result.entries[0]?.text).toBe("retained transcript");
        expect(result.entries[0]?.queue_delivery_links).toEqual({
          status: "unavailable",
          reason,
          deliveries: [],
        });
        expect(fetcher).toHaveBeenCalledTimes(2);
      }
    }));
  it("bounds distinct command lookups and concurrency while preserving every recent entry", async () =>
    withFixture(async (tokenFile) => {
      let active = 0;
      let peak = 0;
      let calls = 0;
      let release: () => void = () => undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const fetcher: typeof fetch = async (url) => {
        if (String(url).includes("/recent?"))
          return Response.json(
            recent(Array.from({ length: 55 }, (_, i) => recentEntry(`command-${i}`))),
          );
        active++;
        calls++;
        peak = Math.max(peak, active);
        if (calls === 4) release();
        await gate;
        active--;
        return Response.json(command(String(url).split("/").at(-1)));
      };
      const result = await makeVoiceReviewBridge(config(tokenFile), fetcher).recent(principal, 100);
      expect(calls).toBe(50);
      expect(peak).toBe(4);
      expect(result.entries).toHaveLength(55);
      expect(
        result.entries
          .slice(0, 50)
          .every((entry) => entry.queue_delivery_links?.status === "ready"),
      ).toBe(true);
      expect(
        result.entries.slice(50).every((entry) => entry.queue_delivery_links?.reason === "limit"),
      ).toBe(true);
    }));
  it("uses the overall deadline including the initial recent read", async () =>
    withFixture(async (tokenFile) => {
      let time = 0;
      const fetcher = vi.fn<typeof fetch>(async () => {
        time = 10_000;
        return Response.json(recent([recentEntry("command")]));
      });
      const result = await makeVoiceReviewBridge(
        config(tokenFile),
        fetcher,
        undefined,
        () => time,
      ).recent(principal);
      expect(fetcher).toHaveBeenCalledTimes(1);
      expect(result.entries[0]?.queue_delivery_links?.reason).toBe("limit");
      expect(result.entries[0]?.text).toBe("retained transcript");
    }));
  it("aborts in-flight lookups at the deadline without losing the recent response", async () =>
    withFixture(async (tokenFile) => {
      vi.useFakeTimers();
      let started: () => void = () => undefined;
      const pending = new Promise<void>((resolve) => {
        started = resolve;
      });
      let signal: AbortSignal | null | undefined;
      try {
        const fetcher: typeof fetch = async (url, init) => {
          if (String(url).includes("/recent?"))
            return Response.json(recent([recentEntry("command")]));
          signal = init?.signal;
          started();
          return await new Promise<Response>((_resolve, reject) => {
            signal?.addEventListener("abort", () => reject(new Error("deadline")), { once: true });
          });
        };
        const result = makeVoiceReviewBridge(config(tokenFile), fetcher, undefined, () => 0).recent(
          principal,
        );
        await pending;
        await vi.advanceTimersByTimeAsync(10_000);
        expect((await result).entries[0]?.queue_delivery_links?.reason).toBe("limit");
        expect(signal?.aborted).toBe(true);
      } finally {
        vi.useRealTimers();
      }
    }));
});
