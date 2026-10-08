import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { BearerConnectionTarget, type PreparedConnection } from "../../connection/model.ts";
import {
  layerRemoteHttpClient,
  RemoteEnvironmentAuthUndeclaredStatusError,
} from "../../rpc/http.ts";
import {
  fetchCompanionHosts,
  fetchCompanionThread,
  setCompanionDefault,
  setCompanionThread,
  isCompanionEndpointUnsupported,
} from "./http.ts";
const environmentId = EnvironmentId.make("env");
const prepared: PreparedConnection = {
  environmentId,
  label: "Fixture",
  httpBaseUrl: "https://fixture.test",
  socketUrl: "wss://fixture.test/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture-only" },
  target: new BearerConnectionTarget({ environmentId, label: "Fixture", connectionId: "fixture" }),
};
describe("companion shared authenticated HTTP owner", () => {
  it.effect("uses existing authentication for all four JSON endpoints without a wsTicket", () =>
    Effect.gen(function* () {
      const seen: Array<{ url: string; method: string; authorization: string | null }> = [];
      const fetchFn = vi.fn<typeof fetch>(async (input, init) => {
        const url = String(input);
        const method = init?.method ?? "GET";
        seen.push({ url, method, authorization: new Headers(init?.headers).get("authorization") });
        if (method === "PUT") return new Response(null, { status: 204 });
        return Response.json(
          url.endsWith("/hosts")
            ? { hosts: [], environmentDefault: { _tag: "server" } }
            : { selection: null, effective: { _tag: "server" }, tabs: [] },
        );
      });
      yield* Effect.gen(function* () {
        yield* fetchCompanionHosts(prepared);
        yield* fetchCompanionThread(prepared, ThreadId.make("thread"));
        yield* setCompanionDefault(prepared, { _tag: "server" });
        yield* setCompanionThread(prepared, ThreadId.make("thread"), null);
      }).pipe(Effect.provide(layerRemoteHttpClient(fetchFn)));
      expect(seen).toEqual([
        {
          url: "https://fixture.test/api/jones/preview-companion/hosts",
          method: "GET",
          authorization: "Bearer fixture-only",
        },
        {
          url: "https://fixture.test/api/jones/preview-companion/threads/thread",
          method: "GET",
          authorization: "Bearer fixture-only",
        },
        {
          url: "https://fixture.test/api/jones/preview-companion/default",
          method: "PUT",
          authorization: "Bearer fixture-only",
        },
        {
          url: "https://fixture.test/api/jones/preview-companion/threads/thread",
          method: "PUT",
          authorization: "Bearer fixture-only",
        },
      ]);
    }),
  );
  it("only treats an undeclared endpoint 404 as older-server unsupported", () => {
    expect(
      isCompanionEndpointUnsupported(
        new RemoteEnvironmentAuthUndeclaredStatusError("https://fixture.test", 404),
      ),
    ).toBe(true);
    expect(
      isCompanionEndpointUnsupported(
        new RemoteEnvironmentAuthUndeclaredStatusError("https://fixture.test", 403),
      ),
    ).toBe(false);
    expect(
      isCompanionEndpointUnsupported({ _tag: "EnvironmentResourceNotFoundError", status: 404 }),
    ).toBe(false);
    expect(isCompanionEndpointUnsupported({ _tag: "EnvironmentAuthInvalidError" })).toBe(false);
  });
});
