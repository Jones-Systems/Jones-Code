import { describe, expect, it } from "@effect/vitest";
import * as Fiber from "effect/Fiber";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { RelayConnectionTarget } from "../../connection/model.ts";
import type { PreparedConnection } from "../../connection/model.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import { nativeReadRequest, type NativeReadScope } from "../../state/workstreams/nativeRead.ts";
import {
  fetchEnvironmentNativeWorkstreamRead,
  fetchEnvironmentNativeWorkstreamExport,
} from "./nativeHttp.ts";

const scope: NativeReadScope = {
  capability: {
    contract: "workstreams-native/1.0.0",
    mode: "native-member",
    home_link: "ok",
    reads: ["workstreams-read/1.0.0"],
    placement_proofs: ["workstreams-t3-placement/2.0.0"],
    registry: {
      registry_instance_id: "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
      owner_id: "owner",
      canonical_origin: "https://registry.invalid",
      home_environment_id: "env:home",
      server_generation: 7,
    },
  },
  binding: {
    registryId: "https://registry.invalid",
    ownerId: "owner",
    principalId: "principal",
    authorizationRevision: 1,
    serverGeneration: 7,
    registryVersion: 11,
    permissions: ["workstreams:read"],
    contractVersion: "workstreams/1.0.0",
    contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
  },
};
const at = "2026-11-03T00:00:00Z";
const envelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: scope.capability.registry!.registry_instance_id,
    owner_id: "owner",
    canonical_origin: "https://registry.invalid",
    server_generation: 7,
    registry_version: 11,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "env:home",
    served_by_environment_id: "env:home",
    source: "live",
    observed_at: at,
    stale: false,
  },
  coverage: { registry: "complete", sources: [] },
};
const counts = {
  context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
  principal_id: "principal",
  observed_at: at,
  active: 0,
  total: 0,
  unknown_lifecycle: 0,
  authority_effect: "none",
};
const countResult = () => ({
  contract: "workstreams-native-home-read-result/1.0.0",
  route: "counts",
  value: structuredClone(counts),
  envelope: structuredClone(envelope),
  authorization_revision: 1,
});
const request = nativeReadRequest({ route: "counts", params: {} });
const sha256 = async (bytes: Uint8Array) =>
  Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
const prepared: PreparedConnection = {
  environmentId: EnvironmentId.make("env:member"),
  label: "Selected member",
  httpBaseUrl: "https://old.invalid",
  socketUrl: "wss://old.invalid/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture-token" },
  target: new RelayConnectionTarget({
    environmentId: EnvironmentId.make("env:member"),
    label: "Selected member",
  }),
};
const exportBytes = async () => {
  // Whitespace is intentionally retained and participates in the original UTF-8 digest.
  const page =
    JSON.stringify({ envelope, counts, offset: 0, items: [] }, null, 1).replaceAll("\n", " ") +
    "\n";
  const trailer = JSON.stringify({
    complete: true,
    registry_version: 11,
    sha256: await sha256(new TextEncoder().encode(page)),
  });
  return new TextEncoder().encode(page + trailer + "\n");
};
function harness(
  reply: (url: string, n: number, init: RequestInit) => Response | Promise<Response>,
) {
  const calls: { url: string; init: RequestInit }[] = [];
  const proofs: Parameters<ManagedRelayDpopSigner["Service"]["createProof"]>[0][] = [];
  const authorizations: { expectedEnvironmentId: EnvironmentId; rejectedAccessToken?: string }[] =
    [];
  const input = {
    prepared,
    scope,
    sha256,
    signer: Option.some(
      ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed("fixture-thumbprint"),
        createProof: (p) =>
          Effect.sync(() => {
            proofs.push(p);
            return "fixture-proof";
          }),
      }),
    ),
    remoteAuthorization: Option.some(
      RemoteEnvironmentAuthorization.of({
        authorizeBearer: () => Effect.die("No WebSocket preparation."),
        authorizeDpop: () => Effect.die("No WebSocket preparation."),
        authorizeDpopHttp: (p) =>
          Effect.sync(() => {
            authorizations.push(p);
            return {
              environmentId: prepared.environmentId,
              label: prepared.label,
              httpBaseUrl: "https://current.invalid",
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: p.rejectedAccessToken ? "renewed" : "current",
                expiresAtEpochMs: 9999999999999,
              },
            };
          }),
      }),
    ),
  };
  const layer = layerRemoteHttpClient(async (url, init) => {
    const i = init ?? {};
    calls.push({ url: String(url), init: i });
    return reply(String(url), calls.length, i);
  });
  return { input, layer, calls, proofs, authorizations };
}
const denied = () =>
  Response.json(
    {
      _tag: "EnvironmentAuthInvalidError",
      code: "auth_invalid",
      reason: "invalid_credential",
      traceId: "fixture",
    },
    { status: 401 },
  );
const ndjson = (bytes: Uint8Array) =>
  new Response(new Uint8Array(bytes), {
    headers: { "content-type": "application/x-ndjson; charset=utf-8" },
  });
const mustFail = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    expect((yield* Effect.result(effect))._tag).toBe("Failure");
  });
const dpopPrepared = {
  ...prepared,
  httpAuthorization: { _tag: "Dpop" as const, accessToken: "old", expiresAtEpochMs: 0 },
};
describe("authenticated native Workstreams HTTP", () => {
  it.effect.each(["cookie", "bearer"] as const)(
    "uses current %s authentication on the fixed read endpoint",
    (mode) =>
      Effect.gen(function* () {
        const h = harness(() => Response.json(countResult()));
        const result = yield* fetchEnvironmentNativeWorkstreamRead({
          ...h.input,
          prepared: {
            ...prepared,
            httpAuthorization: mode === "cookie" ? null : prepared.httpAuthorization,
          },
          request,
        }).pipe(Effect.provide(h.layer));
        expect(result.route).toBe("counts");
        expect(h.calls).toHaveLength(1);
        expect(h.calls[0]!.url).toBe("https://old.invalid/api/workstreams/native/v1/read");
        expect(h.calls[0]!.init.method).toBe("POST");
        expect(JSON.parse(String(h.calls[0]!.init.body))).toEqual(request);
        expect(new Headers(h.calls[0]!.init.headers).get("authorization")).toBe(
          mode === "bearer" ? "Bearer fixture-token" : null,
        );
        expect(h.calls[0]!.init.credentials).toBe(mode === "cookie" ? "include" : undefined);
      }),
  );
  it.effect(
    "renews only rejected DPoP credentials and signs the exact endpoint without replacing the socket",
    () =>
      Effect.gen(function* () {
        const h = harness((_url, n) => (n === 1 ? denied() : Response.json(countResult())));
        const result = yield* fetchEnvironmentNativeWorkstreamRead({
          ...h.input,
          prepared: dpopPrepared,
          request,
        }).pipe(Effect.provide(h.layer));
        expect(result.route).toBe("counts");
        expect(h.authorizations).toEqual([
          { expectedEnvironmentId: prepared.environmentId },
          { expectedEnvironmentId: prepared.environmentId, rejectedAccessToken: "current" },
        ]);
        expect(h.proofs.map((p) => [p.method, p.url, p.accessToken])).toEqual([
          ["POST", "https://current.invalid/api/workstreams/native/v1/read", "current"],
          ["POST", "https://current.invalid/api/workstreams/native/v1/read", "renewed"],
        ]);
        expect(h.calls.map((c) => JSON.parse(String(c.init.body)))).toEqual([request, request]);
      }),
  );
  it.effect("refuses a missing DPoP signer without sending an unsigned request", () =>
    Effect.gen(function* () {
      const h = harness(() => Response.json(countResult()));
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamRead({
          ...h.input,
          signer: Option.none(),
          prepared: dpopPrepared,
          request,
        }).pipe(Effect.provide(h.layer)),
      );
      expect(h.calls).toHaveLength(0);
    }),
  );
  it.effect("does not retry a second credential rejection or fall back to a legacy endpoint", () =>
    Effect.gen(function* () {
      const h = harness(denied);
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamRead({ ...h.input, prepared: dpopPrepared, request }).pipe(
          Effect.provide(h.layer),
        ),
      );
      expect(h.calls).toHaveLength(2);
      expect(
        h.calls.every((c) => new URL(c.url).pathname === "/api/workstreams/native/v1/read"),
      ).toBe(true);
    }),
  );
  it.effect.each(["capability", "request"] as const)(
    "refuses invalid %s before network work",
    (bad) =>
      Effect.gen(function* () {
        const h = harness(() => Response.json(countResult()));
        const input = { ...h.input, request };
        if (bad === "capability")
          input.scope = { ...scope, binding: { ...scope.binding, permissions: [] } };
        else
          Object.assign(input, {
            request: { ...request, origin: "https://body-selected.invalid" },
          });
        yield* mustFail(fetchEnvironmentNativeWorkstreamRead(input).pipe(Effect.provide(h.layer)));
        expect(h.calls).toHaveLength(0);
      }),
  );
  it.effect("rejects a decoded native result from the wrong home", () =>
    Effect.gen(function* () {
      const reply = countResult();
      reply.envelope.provenance.home_environment_id = "another";
      const h = harness(() => Response.json(reply));
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamRead({ ...h.input, request }).pipe(Effect.provide(h.layer)),
      );
      expect(h.calls).toHaveLength(1);
    }),
  );
  it.effect(
    "returns exact original export bytes only after complete validation and final current counts",
    () =>
      Effect.gen(function* () {
        const bytes = yield* Effect.promise(exportBytes);
        const h = harness((url) =>
          new URL(url).pathname.endsWith("/export") ? ndjson(bytes) : Response.json(countResult()),
        );
        const result = yield* fetchEnvironmentNativeWorkstreamExport({
          ...h.input,
          pageSize: 1,
        }).pipe(Effect.provide(h.layer));
        expect(result.bytes).toEqual(bytes);
        expect(result.completion.complete).toBe(true);
        expect(result.completion.pages).toHaveLength(1);
        expect(h.calls.map((c) => [c.init.method, c.url])).toEqual([
          ["POST", "https://old.invalid/api/workstreams/native/v1/read"],
          ["GET", "https://old.invalid/api/workstreams/native/v1/export?page_size=1"],
          ["POST", "https://old.invalid/api/workstreams/native/v1/read"],
        ]);
      }),
  );
  it.effect(
    "signs the exact DPoP export query and keeps both count checks on the same environment",
    () =>
      Effect.gen(function* () {
        const bytes = yield* Effect.promise(exportBytes);
        const h = harness((url) =>
          new URL(url).pathname.endsWith("/export") ? ndjson(bytes) : Response.json(countResult()),
        );
        yield* fetchEnvironmentNativeWorkstreamExport({
          ...h.input,
          pageSize: 7,
          prepared: dpopPrepared,
        }).pipe(Effect.provide(h.layer));
        expect(h.proofs.map((p) => [p.method, p.url])).toEqual([
          ["POST", "https://current.invalid/api/workstreams/native/v1/read"],
          ["GET", "https://current.invalid/api/workstreams/native/v1/export?page_size=7"],
          ["POST", "https://current.invalid/api/workstreams/native/v1/read"],
        ]);
        expect(
          h.authorizations.every((p) => p.expectedEnvironmentId === prepared.environmentId),
        ).toBe(true);
      }),
  );
  it.effect.each(["principal", "revision", "generation", "stale"] as const)(
    "withholds bytes after final %s drift",
    (field) =>
      Effect.gen(function* () {
        const bytes = yield* Effect.promise(exportBytes);
        const h = harness((url, n) => {
          if (new URL(url).pathname.endsWith("/export")) return ndjson(bytes);
          const value = countResult();
          if (n === 3) {
            if (field === "principal") value.value.principal_id = "another";
            if (field === "revision") value.authorization_revision++;
            if (field === "generation") value.envelope.registry.server_generation++;
            if (field === "stale") value.envelope.provenance.stale = true;
          }
          return Response.json(value);
        });
        yield* mustFail(
          fetchEnvironmentNativeWorkstreamExport(h.input).pipe(Effect.provide(h.layer)),
        );
        expect(h.calls).toHaveLength(3);
      }),
  );
  it.effect.each(["truncated", "digest", "trailing", "utf8", "content-type"] as const)(
    "rejects %s exports without a completion or final read",
    (field) =>
      Effect.gen(function* () {
        let bytes = yield* Effect.promise(exportBytes);
        if (field === "truncated") bytes = bytes.slice(0, -2);
        if (field === "digest")
          bytes = new TextEncoder().encode(
            new TextDecoder().decode(bytes).replace('"sha256":"', '"sha256":"0'),
          );
        if (field === "trailing")
          bytes = new TextEncoder().encode(new TextDecoder().decode(bytes) + " \n");
        if (field === "utf8") bytes = new Uint8Array([0xff, ...bytes]);
        const h = harness((url) => {
          if (!new URL(url).pathname.endsWith("/export")) return Response.json(countResult());
          return field === "content-type"
            ? new Response(new Uint8Array(bytes), { headers: { "content-type": "text/plain" } })
            : ndjson(bytes);
        });
        yield* mustFail(
          fetchEnvironmentNativeWorkstreamExport(h.input).pipe(Effect.provide(h.layer)),
        );
        expect(h.calls).toHaveLength(2);
      }),
  );
  it.effect("refuses an invalid page size before counts or export", () =>
    Effect.gen(function* () {
      const h = harness(() => Response.json(countResult()));
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamExport({ ...h.input, pageSize: 101 }).pipe(
          Effect.provide(h.layer),
        ),
      );
      expect(h.calls).toHaveLength(0);
    }),
  );
  it.effect("refuses a different original principal before downloading export bytes", () =>
    Effect.gen(function* () {
      const value = countResult();
      value.value.principal_id = "another";
      const h = harness(() => Response.json(value));
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamExport(h.input).pipe(Effect.provide(h.layer)),
      );
      expect(h.calls).toHaveLength(1);
    }),
  );
  it.effect("retains the original decoded request while credential signing yields", () =>
    Effect.gen(function* () {
      const original = nativeReadRequest({
        route: "history",
        params: { workstream_id: "original" },
      });
      const h = harness(() =>
        Response.json({
          ...countResult(),
          route: "history",
          value: { context: counts.context, items: [], next_cursor: null },
        }),
      );
      const signer = ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed("fixture"),
        createProof: () =>
          Effect.sync(() => {
            Object.assign(original.request.params, { workstream_id: "replacement" });
            return "fixture-proof";
          }),
      });
      yield* fetchEnvironmentNativeWorkstreamRead({
        ...h.input,
        signer: Option.some(signer),
        request: original,
        prepared: dpopPrepared,
      }).pipe(Effect.provide(h.layer));
      expect(JSON.parse(String(h.calls[0]!.init.body)).request.params.workstream_id).toBe(
        "original",
      );
    }),
  );
  it.effect("renews a raw-export credential rejection through the existing auth owner", () =>
    Effect.gen(function* () {
      const bytes = yield* Effect.promise(exportBytes);
      const h = harness((url, n) => {
        if (!new URL(url).pathname.endsWith("/export")) return Response.json(countResult());
        return n === 2 ? denied() : ndjson(bytes);
      });
      const result = yield* fetchEnvironmentNativeWorkstreamExport({
        ...h.input,
        pageSize: 1,
        prepared: dpopPrepared,
      }).pipe(Effect.provide(h.layer));
      expect(result.bytes).toEqual(bytes);
      expect(h.calls).toHaveLength(4);
      expect(h.authorizations.filter((a) => a.rejectedAccessToken !== undefined)).toEqual([
        { expectedEnvironmentId: prepared.environmentId, rejectedAccessToken: "current" },
      ]);
      expect(h.calls.map((c) => new URL(c.url).pathname)).toEqual([
        "/api/workstreams/native/v1/read",
        "/api/workstreams/native/v1/export",
        "/api/workstreams/native/v1/export",
        "/api/workstreams/native/v1/read",
      ]);
    }),
  );
  it.effect("does not retry a second raw-export credential rejection", () =>
    Effect.gen(function* () {
      const h = harness((url) =>
        new URL(url).pathname.endsWith("/export") ? denied() : Response.json(countResult()),
      );
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamExport({ ...h.input, prepared: dpopPrepared }).pipe(
          Effect.provide(h.layer),
        ),
      );
      expect(h.calls).toHaveLength(3);
    }),
  );
  it.effect("bounds raw response consumption before validation and releases no result", () =>
    Effect.gen(function* () {
      const h = harness((url) => {
        if (!new URL(url).pathname.endsWith("/export")) return Response.json(countResult());
        let n = 0;
        return new Response(
          new ReadableStream<Uint8Array>({
            pull(controller) {
              if (n++ === 18) controller.close();
              else controller.enqueue(new Uint8Array(1024 * 1024));
            },
          }),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      });
      yield* mustFail(
        fetchEnvironmentNativeWorkstreamExport(h.input).pipe(Effect.provide(h.layer)),
      );
      expect(h.calls).toHaveLength(2);
    }),
  );
  it.effect("cancels a pending response body without a completion, final read or retry", () =>
    Effect.gen(function* () {
      let reached!: () => void;
      const started = new Promise<void>((resolve) => {
        reached = resolve;
      });
      let cancelled = false;
      const h = harness((url) => {
        if (!new URL(url).pathname.endsWith("/export")) return Response.json(countResult());
        return new Response(
          new ReadableStream<Uint8Array>({
            pull() {
              reached();
              return new Promise<void>(() => {});
            },
            cancel() {
              cancelled = true;
            },
          }),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      });
      const running = yield* Effect.forkChild(
        fetchEnvironmentNativeWorkstreamExport(h.input).pipe(Effect.provide(h.layer)),
      );
      yield* Effect.promise(() => started);
      yield* Fiber.interrupt(running);
      expect((yield* Fiber.await(running))._tag).toBe("Failure");
      expect(cancelled).toBe(true);
      expect(h.calls).toHaveLength(2);
    }),
  );
});
