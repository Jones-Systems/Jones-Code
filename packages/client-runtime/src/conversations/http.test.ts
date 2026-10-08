import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { RemoteEnvironmentAuthorization } from "../authorization/service.ts";
import { PrimaryConnectionTarget, type PreparedConnection } from "../connection/model.ts";
import { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../rpc/http.ts";
import { fetchEnvironmentConversationLibraryRequest } from "./http.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Primary",
  httpBaseUrl: "https://environment.example.test/base",
  wsBaseUrl: "wss://environment.example.test",
});

const prepared = (
  httpAuthorization: PreparedConnection["httpAuthorization"] = null,
): PreparedConnection => ({
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: "wss://environment.example.test/ws",
  httpAuthorization,
  target: TARGET,
});

const hello = {
  kind: "hello",
  protocol: "t3.conversation-library.v1",
  revision: 1,
  canWrite: true,
  capture: "not-enabled",
};

const fetchWithJson = (
  body: unknown,
  status = 200,
  onRequest?: (request: RequestInfo | URL, init: RequestInit) => void,
): typeof fetch =>
  ((request, init) => {
    onRequest?.(request, init ?? {});
    return Promise.resolve(Response.json(body, { status }));
  }) satisfies typeof fetch;

describe("conversation library HTTP transport", () => {
  it.effect("uses cookie credentials for the primary environment and posts the typed request", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [RequestInfo | URL, RequestInit]> = [];
      const result = yield* fetchEnvironmentConversationLibraryRequest({
        prepared: prepared(),
        request: { kind: "hello" },
        signer: Option.none(),
      }).pipe(
        Effect.provide(
          layerRemoteHttpClient(
            fetchWithJson(hello, 200, (request, init) => calls.push([request, init])),
          ),
        ),
      );

      expect(result).toEqual(hello);
      expect(calls).toHaveLength(1);
      const [request, init] = calls[0]!;
      expect(String(request)).toBe("https://environment.example.test/api/conversation-library");
      expect(init.method).toBe("POST");
      expect(init.credentials).toBe("include");
      const body =
        typeof init.body === "string"
          ? init.body
          : init.body instanceof Uint8Array
            ? new TextDecoder().decode(init.body)
            : "";
      expect(body).toContain('"kind":"hello"');
    }),
  );

  it.effect("sends bearer authorization without cookie credentials", () =>
    Effect.gen(function* () {
      let requestInit: RequestInit | undefined;
      yield* fetchEnvironmentConversationLibraryRequest({
        prepared: prepared({ _tag: "Bearer", token: "environment-token" }),
        request: { kind: "hello" },
        signer: Option.none(),
      }).pipe(
        Effect.provide(
          layerRemoteHttpClient(
            fetchWithJson(hello, 200, (_request, init) => {
              requestInit = init;
            }),
          ),
        ),
      );

      expect(new Headers(requestInit?.headers).get("authorization")).toBe(
        "Bearer environment-token",
      );
      expect(requestInit?.credentials).toBeUndefined();
    }),
  );

  it.effect("binds DPoP proofs to the conversation library POST URL", () =>
    Effect.gen(function* () {
      let proofInput: Parameters<ManagedRelayDpopSigner["Service"]["createProof"]>[0] | undefined;
      let requestInit: RequestInit | undefined;
      const signer: ManagedRelayDpopSigner["Service"] = {
        thumbprint: Effect.succeed("thumbprint"),
        createProof: (input) => {
          proofInput = input;
          return Effect.succeed("proof-value");
        },
      };
      const remoteAuthorization = RemoteEnvironmentAuthorization.of({
        authorizeBearer: () => Effect.die("unused"),
        authorizeDpop: () => Effect.die("unused"),
        authorizeDpopHttp: () =>
          Effect.succeed({
            environmentId: TARGET.environmentId,
            label: TARGET.label,
            httpBaseUrl: TARGET.httpBaseUrl,
            httpAuthorization: {
              _tag: "Dpop" as const,
              accessToken: "relay-token",
              expiresAtEpochMs: 10_000,
            },
          }),
      });

      yield* fetchEnvironmentConversationLibraryRequest({
        prepared: prepared({ _tag: "Dpop", accessToken: "relay-token", expiresAtEpochMs: 10_000 }),
        request: { kind: "hello" },
        signer: Option.some(signer),
      }).pipe(
        Effect.provideService(RemoteEnvironmentAuthorization, remoteAuthorization),
        Effect.provide(
          layerRemoteHttpClient(
            fetchWithJson(hello, 200, (_request, init) => {
              requestInit = init;
            }),
          ),
        ),
      );

      expect(proofInput).toEqual({
        method: "POST",
        url: "https://environment.example.test/api/conversation-library",
        accessToken: "relay-token",
      });
      expect(new Headers(requestInit?.headers).get("authorization")).toBe("DPoP relay-token");
      expect(new Headers(requestInit?.headers).get("dpop")).toBe("proof-value");
      expect(requestInit?.credentials).toBeUndefined();
    }),
  );

  it.effect("preserves the unsupported feature code returned by an older server", () =>
    Effect.gen(function* () {
      const unsupported = {
        kind: "error",
        code: "unsupported",
        message: "Conversation library is not available on this server.",
        traceId: "trace-old-server",
      };
      const error = yield* fetchEnvironmentConversationLibraryRequest({
        prepared: prepared(),
        request: { kind: "hello" },
        signer: Option.none(),
      }).pipe(Effect.provide(layerRemoteHttpClient(fetchWithJson(unsupported, 501))), Effect.flip);

      expect(error).toMatchObject({ code: "unsupported", traceId: "trace-old-server" });
    }),
  );

  it.effect("returns structured library errors without erasing their domain code", () =>
    Effect.gen(function* () {
      const conflict = {
        kind: "error",
        code: "conflict",
        message: "The conversation library changed.",
        traceId: "trace-conflict",
      };
      const error = yield* fetchEnvironmentConversationLibraryRequest({
        prepared: prepared(),
        request: { kind: "import", accountId: "account-1", conversations: [] },
        signer: Option.none(),
      }).pipe(Effect.provide(layerRemoteHttpClient(fetchWithJson(conflict, 409))), Effect.flip);

      expect(error).toMatchObject({ code: "conflict", traceId: "trace-conflict" });
    }),
  );
});
