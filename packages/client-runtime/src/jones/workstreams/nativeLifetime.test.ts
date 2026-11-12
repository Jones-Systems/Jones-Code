import { makeNativeWorkstreamsTestRunner } from "./nativeLifetime.testkit.ts";
import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { RelayConnectionTarget, type PreparedConnection } from "../../connection/model.ts";
import { layerRemoteHttpClient } from "../../rpc/http.ts";
import { nativeReadRequest, type NativeReadScope } from "../../state/workstreams/nativeRead.ts";
import {
  EnvironmentNativeWorkstreamsLifetime,
  type NativeWorkstreamsEffectRunner,
} from "./nativeLifetime.ts";

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
const context = { owner_id: "owner", server_generation: 7, registry_version: 11 };
const counts = {
  context,
  principal_id: "principal",
  observed_at: at,
  active: 0,
  total: 0,
  unknown_lifecycle: 0,
  authority_effect: "none",
};
const result = (route = "counts", value: unknown = counts) => ({
  contract: "workstreams-native-home-read-result/1.0.0",
  route,
  value,
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
  label: "Member",
  httpBaseUrl: "https://member.invalid",
  socketUrl: "wss://member.invalid/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture" },
  target: new RelayConnectionTarget({
    environmentId: EnvironmentId.make("env:member"),
    label: "Member",
  }),
};
function harness(
  reply: (url: string, init: RequestInit, count: number) => Response | Promise<Response>,
) {
  const calls: { url: string; init: RequestInit }[] = [];
  const state = { current: true };
  const layer = layerRemoteHttpClient((url, init) => {
    const input = init ?? {};
    calls.push({ url: String(url), init: input });
    return Promise.resolve(reply(String(url), input, calls.length));
  });
  const run: NativeWorkstreamsEffectRunner = (effect, signal) =>
    testRun(effect.pipe(Effect.provide(layer)), signal);
  const make = (signal?: AbortSignal) =>
    new EnvironmentNativeWorkstreamsLifetime({
      prepared,
      scope,
      sha256,
      run,
      isCurrent: () => state.current,
      ...(signal ? { signal } : {}),
    });
  return { make, state, calls };
}
async function exported() {
  const page = JSON.stringify({ envelope, counts, offset: 0, items: [] }) + "\n";
  return new TextEncoder().encode(
    page +
      JSON.stringify({
        complete: true,
        registry_version: 11,
        sha256: await sha256(new TextEncoder().encode(page)),
      }) +
      "\n",
  );
}
describe("native Workstreams application lifetime", () => {
  it.effect("uses the captured authenticated connection and validates the route", () =>
    underTest(async () => {
      const h = harness(() => Response.json(result()));
      const reader = h.make();
      expect((await reader.read(request)).route).toBe("counts");
      expect(h.calls[0]!.url).toBe("https://member.invalid/api/workstreams/native/v1/read");
      expect(new Headers(h.calls[0]!.init.headers).get("authorization")).toBe("Bearer fixture");
      reader.dispose();
    }),
  );
  it.effect("rejects stale application authority before any request", () =>
    underTest(async () => {
      const h = harness(() => Response.json(result()));
      h.state.current = false;
      await expect(h.make().read(request)).rejects.toMatchObject({ code: "scope_changed" });
      expect(h.calls).toHaveLength(0);
    }),
  );
  it.effect("withholds a late result after application authority replacement", () =>
    underTest(async () => {
      const started = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<Response>();
      const h = harness(() => {
        started.resolve();
        return reply.promise;
      });
      const reader = h.make();
      const reading = reader.read(request);
      await started.promise;
      h.state.current = false;
      reply.resolve(Response.json(result()));
      await expect(reading).rejects.toMatchObject({ code: "scope_changed" });
      h.state.current = true;
      await expect(reader.read(request)).rejects.toMatchObject({ code: "scope_changed" });
      expect(h.calls).toHaveLength(1);
    }),
  );
  it.effect("cancels owned HTTP when its parent lifetime closes", () =>
    underTest(async () => {
      const started = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<Response>();
      const h = harness(() => {
        started.resolve();
        return reply.promise;
      });
      const parent = new AbortController();
      const reader = h.make(parent.signal);
      const reading = reader.read(request);
      const settled = reading.catch((error: unknown) => error);
      await started.promise;
      parent.abort();
      expect(h.calls[0]!.init.signal?.aborted).toBe(true);
      reply.resolve(Response.json(result()));
      await settled;
      await expect(reader.read(request)).rejects.toMatchObject({ code: "scope_changed" });
    }),
  );
  it.effect("never lets a disposed reader replace its independent successor", () =>
    underTest(async () => {
      const started = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<Response>();
      const h = harness((_url, _init, count) => {
        if (count === 1) {
          started.resolve();
          return reply.promise;
        }
        return Response.json(result());
      });
      const previous = h.make();
      const reading = previous.read(request).catch((error: unknown) => error);
      await started.promise;
      previous.dispose();
      const successor = h.make();
      expect((await successor.read(request)).route).toBe("counts");
      reply.resolve(Response.json(result()));
      await reading;
      expect((await successor.read(request)).route).toBe("counts");
      successor.dispose();
    }),
  );
  it.effect("keeps one coherent reader across all pages", () =>
    underTest(async () => {
      const h = harness((_url, _init, count) =>
        Response.json(
          result("references", {
            context,
            items: [],
            next_cursor: count === 1 ? "cursor1" : null,
          }),
        ),
      );
      const reader = h.make();
      const read = await reader.complete(nativeReadRequest({ route: "references", params: {} }));
      expect(read.pages).toHaveLength(2);
      expect(read.stale).toBe(false);
      expect(h.calls).toHaveLength(2);
      reader.dispose();
    }),
  );
  it.effect("refuses incoherent pages without a legacy request", () =>
    underTest(async () => {
      const h = harness((_url, _init, count) => {
        const read = result("references", {
          context,
          items: [],
          next_cursor: count === 1 ? "cursor1" : null,
        });
        if (count === 2) read.envelope.provenance.observed_at = "2026-11-03T00:00:01Z";
        return Response.json(read);
      });
      const reader = h.make();
      await expect(
        reader.complete(nativeReadRequest({ route: "references", params: {} })),
      ).rejects.toMatchObject({ code: "invalid_response" });
      expect(h.calls.every((call) => call.url.endsWith("/native/v1/read"))).toBe(true);
      reader.dispose();
    }),
  );
  it.effect("releases only complete exact export bytes under the current scope", () =>
    underTest(async () => {
      const bytes = await exported();
      const h = harness((url) =>
        url.includes("/native/v1/read")
          ? Response.json(result())
          : new Response(new Uint8Array(bytes), {
              headers: { "content-type": "application/x-ndjson" },
            }),
      );
      const reader = h.make();
      const read = await reader.export();
      expect(read.bytes).toEqual(bytes);
      expect(read.completion.trailer.complete).toBe(true);
      expect(h.calls).toHaveLength(3);
      reader.dispose();
    }),
  );
  it.effect(
    "withholds complete export bytes if application authority changes during final counts",
    () =>
      underTest(async () => {
        const bytes = await exported();
        const h = harness((url, _init, count) => {
          if (count === 3) h.state.current = false;
          return url.includes("/native/v1/read")
            ? Response.json(result())
            : new Response(new Uint8Array(bytes), {
                headers: { "content-type": "application/x-ndjson" },
              });
        });
        const reader = h.make();
        await expect(reader.export()).rejects.toMatchObject({ code: "scope_changed" });
        expect(h.calls).toHaveLength(3);
      }),
  );
  it.effect("cancelling one request leaves the lifetime available for a fresh read", () =>
    underTest(async () => {
      const started = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<Response>();
      const h = harness((_url, _init, count) => {
        if (count === 1) {
          started.resolve();
          return reply.promise;
        }
        return Response.json(result());
      });
      const reader = h.make();
      const controller = new AbortController();
      const first = reader.read(request, controller.signal).catch((error: unknown) => error);
      await started.promise;
      controller.abort();
      reply.resolve(Response.json(result()));
      await first;
      expect((await reader.read(request)).route).toBe("counts");
      reader.dispose();
    }),
  );
  it.effect("refuses a malformed or withheld capability without executing transport", () =>
    underTest(async () => {
      const h = harness(() => Response.json(result()));
      const withheld = structuredClone(scope);
      expect(
        () =>
          new EnvironmentNativeWorkstreamsLifetime({
            prepared,
            scope: { ...withheld, capability: { ...withheld.capability, reads: [] } },
            sha256,
            run: () => Promise.reject(new Error("must not run")),
            isCurrent: () => true,
          }),
      ).toThrow();
      expect(h.calls).toHaveLength(0);
    }),
  );
});

let testRun: Effect.Success<typeof makeNativeWorkstreamsTestRunner>;
const underTest = (use: () => Promise<void>) =>
  Effect.gen(function* () {
    testRun = yield* makeNativeWorkstreamsTestRunner;
    yield* Effect.promise(use);
  });
