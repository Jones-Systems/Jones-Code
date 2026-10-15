import { expect, it } from "@effect/vitest";
import { EnvironmentId, VoiceReviewNotConfiguredError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { RelayConnectionTarget, type PreparedConnection } from "./connection/model.ts";
import { RemoteEnvironmentAuthorization } from "./authorization/service.ts";
import { ManagedRelayDpopSigner, type ManagedRelayDpopProofInput } from "./relay/managedRelay.ts";
import { layerRemoteHttpClient } from "./rpc/http.ts";
import {
  fetchVoiceReviewDrafts,
  fetchVoiceReviewDraft,
  mutateVoiceReviewDraft,
  fetchVoiceReviewRecent,
  fetchThreadRegistrySnapshot,
  fetchThreadRegistryWorkstreams,
  fetchVoiceReviewDiagnostics,
  correctThreadRegistryAssociation,
} from "./voice-review.ts";

const target = new RelayConnectionTarget({
  environmentId: EnvironmentId.make("fixture-environment"),
  label: "Fixture",
});
const prepared: PreparedConnection = {
  environmentId: target.environmentId,
  label: "Fixture",
  target,
  httpBaseUrl: "https://fixture.test",
  socketUrl: "wss://fixture.test/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture-session-token" },
};
const draft = {
  id: "capture",
  source_id: "microphone",
  state: "paused",
  revision: 2,
  text: "literal text",
  transcript_provider: "fixture",
  language: null,
  edited: false,
  created_at: "2026-10-02T00:00:00Z",
  updated_at: "2026-10-02T00:00:00Z",
  due_at: null,
  remaining_ms: 180000,
  expires_at: "2026-10-03T00:00:00Z",
  command_id: null,
  command_status: null,
  reason: null,
  server_now: "2026-10-02T00:00:00Z",
};
it.effect(
  "uses authenticated environment HTTP for list/detail and mutation with snake_case payloads",
  () =>
    Effect.gen(function* () {
      const calls: Array<{ url: string; init: RequestInit }> = [];
      const layer = layerRemoteHttpClient(async (url, init) => {
        calls.push({ url: String(url), init: init ?? {} });
        if (String(url).includes("?"))
          return Response.json({ server_now: draft.server_now, drafts: [draft] });
        return Response.json(init?.method === "POST" ? { draft, edit_handle: null } : draft);
      });
      const input = { prepared, signer: Option.none<ManagedRelayDpopSigner["Service"]>() };
      const list = yield* fetchVoiceReviewDrafts({ ...input, scope: "recent", limit: 10 }).pipe(
        Effect.provide(layer),
      );
      expect(list.drafts).toEqual([draft]);
      const detail = yield* fetchVoiceReviewDraft({ ...input, id: "capture" }).pipe(
        Effect.provide(layer),
      );
      expect(detail).toEqual(draft);
      yield* mutateVoiceReviewDraft({
        ...input,
        id: "capture",
        action: "pause",
        payload: { expected_revision: 2 },
      }).pipe(Effect.provide(layer));
      expect(calls[0]?.url).toBe(
        "https://fixture.test/api/voice-review/drafts?scope=recent&limit=10",
      );
      expect(calls[2]?.url).toBe("https://fixture.test/api/voice-review/drafts/capture/pause");
      const body = yield* Effect.promise(() => new Response(calls[2]?.init.body).text());
      expect(body).toBe('{"expected_revision":2}');
      expect(
        calls.every(
          ({ init }) =>
            new Headers(init.headers).get("authorization") === "Bearer fixture-session-token",
        ),
      ).toBe(true);
    }),
);
it.effect(
  "exposes a closed not-configured error and leaves old servers as an explicit HTTP failure",
  () =>
    Effect.gen(function* () {
      const input = { prepared, signer: Option.none<ManagedRelayDpopSigner["Service"]>() };
      const closed = layerRemoteHttpClient(async () =>
        Response.json({ _tag: "VoiceReviewNotConfiguredError" }, { status: 503 }),
      );
      const error = yield* fetchVoiceReviewDrafts(input).pipe(Effect.flip, Effect.provide(closed));
      expect(error).toBeInstanceOf(VoiceReviewNotConfiguredError);
      const old = layerRemoteHttpClient(async () => new Response("Missing", { status: 404 }));
      const oldError = yield* fetchVoiceReviewDrafts(input).pipe(Effect.flip, Effect.provide(old));
      expect(oldError).toMatchObject({
        _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
        status: 404,
      });
    }),
);
it.effect(
  "binds relay DPoP to the selected environment request and never retries ambiguous mutations",
  () =>
    Effect.gen(function* () {
      const proofs: ManagedRelayDpopProofInput[] = [];
      let calls = 0;
      const signer = ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed("fixture-thumbprint"),
        createProof: (input) =>
          Effect.sync(() => {
            proofs.push(input);
            return "fixture-proof";
          }),
      });
      const remoteAuthorization = RemoteEnvironmentAuthorization.of({
        authorizeBearer: () => Effect.die("unexpected"),
        authorizeDpop: () => Effect.die("unexpected"),
        authorizeDpopHttp: () =>
          Effect.succeed({
            environmentId: target.environmentId,
            label: "Fixture",
            httpBaseUrl: "https://relay.test",
            httpAuthorization: {
              _tag: "Dpop",
              accessToken: "fresh-token",
              expiresAtEpochMs: 3600000,
            },
          }),
      });
      const layer = layerRemoteHttpClient(async (_url, init) => {
        calls++;
        expect(new Headers(init?.headers).get("authorization")).toBe("DPoP fresh-token");
        expect(new Headers(init?.headers).get("dpop")).toBe("fixture-proof");
        return Response.json({ _tag: "VoiceReviewUnavailableError" }, { status: 502 });
      });
      const error = yield* mutateVoiceReviewDraft({
        prepared: {
          ...prepared,
          httpAuthorization: { _tag: "Dpop", accessToken: "old", expiresAtEpochMs: 0 },
        },
        signer: Option.some(signer),
        remoteAuthorization: Option.some(remoteAuthorization),
        id: "capture",
        action: "send-now",
        payload: { expected_revision: 2 },
      }).pipe(Effect.flip, Effect.provide(layer));
      expect(error).toMatchObject({ _tag: "VoiceReviewUnavailableError" });
      expect(calls).toBe(1);
      expect(proofs).toEqual([
        {
          method: "POST",
          url: "https://relay.test/api/voice-review/drafts/capture/send-now",
          accessToken: "fresh-token",
        },
      ]);
    }),
);

it.effect("signs the exact encoded Voice Review and registry request URLs", () =>
  Effect.gen(function* () {
    const urls: string[] = [];
    const proofs: ManagedRelayDpopProofInput[] = [];
    const signer = ManagedRelayDpopSigner.of({
      thumbprint: Effect.succeed("fixture-thumbprint"),
      createProof: (input) => Effect.sync(() => {
        proofs.push(input);
        return "fixture-proof";
      }),
    });
    const remoteAuthorization = RemoteEnvironmentAuthorization.of({
      authorizeBearer: () => Effect.die("unexpected"),
      authorizeDpop: () => Effect.die("unexpected"),
      authorizeDpopHttp: () => Effect.succeed({
        environmentId: target.environmentId,
        label: "Fixture",
        httpBaseUrl: "https://relay.test",
        httpAuthorization: {
          _tag: "Dpop",
          accessToken: "fresh-token",
          expiresAtEpochMs: 3600000,
        },
      }),
    });
    const layer = layerRemoteHttpClient(async (url, init) => {
      urls.push(String(url));
      expect(new Headers(init?.headers).get("authorization")).toBe("DPoP fresh-token");
      expect(new Headers(init?.headers).get("dpop")).toBe("fixture-proof");
      return Response.json({ _tag: "VoiceReviewNotConfiguredError" }, { status: 503 });
    });
    const input = {
      prepared: {
        ...prepared,
        httpAuthorization: { _tag: "Dpop" as const, accessToken: "old", expiresAtEpochMs: 0 },
      },
      signer: Option.some(signer),
      remoteAuthorization: Option.some(remoteAuthorization),
    };
    const unavailable = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(
      Effect.provide(layer),
      Effect.flip,
      Effect.tap((error) => Effect.sync(() => expect(error).toBeInstanceOf(VoiceReviewNotConfiguredError))),
    );
    const id = "capture:/ α?x#%";
    yield* unavailable(fetchVoiceReviewDrafts({ ...input, scope: "recent", limit: 17 }));
    yield* unavailable(fetchVoiceReviewDraft({ ...input, id }));
    yield* unavailable(mutateVoiceReviewDraft({
      ...input, id, action: "pause", payload: { expected_revision: 2 },
    }));
    yield* unavailable(fetchVoiceReviewRecent({ ...input, limit: 19 }));
    yield* unavailable(fetchThreadRegistrySnapshot({ ...input, limit: 23, cursor: "next:/ α?x#%&=" }));
    yield* unavailable(fetchThreadRegistryWorkstreams(input));
    yield* unavailable(fetchVoiceReviewDiagnostics({ ...input, id }));
    yield* unavailable(correctThreadRegistryAssociation({
      ...input,
      payload: {
        schema: "voice.association-mutation/v1",
        subject: "thread:fixture",
        workstream_ref: "inferred:fixture",
        state: "active",
        expected_revision: 0,
        request_id: "request:fixture",
      },
    }));
    const expected = [
      "https://relay.test/api/voice-review/drafts?scope=recent&limit=17",
      "https://relay.test/api/voice-review/drafts/capture%3A%2F%20%CE%B1%3Fx%23%25",
      "https://relay.test/api/voice-review/drafts/capture%3A%2F%20%CE%B1%3Fx%23%25/pause",
      "https://relay.test/api/voice-review/recent?limit=19",
      "https://relay.test/api/voice-review/registry/snapshot?limit=23&cursor=next%3A%2F+%CE%B1%3Fx%23%25%26%3D",
      "https://relay.test/api/voice-review/registry/workstreams",
      "https://relay.test/api/voice-review/drafts/capture%3A%2F%20%CE%B1%3Fx%23%25/diagnostics",
      "https://relay.test/api/voice-review/registry/associations",
    ];
    expect(urls).toEqual(expected);
    expect(proofs).toEqual(expected.map((url, index) => ({
      method: index === 2 || index === 7 ? "POST" : "GET",
      url,
      accessToken: "fresh-token",
    })));
  }),
);
