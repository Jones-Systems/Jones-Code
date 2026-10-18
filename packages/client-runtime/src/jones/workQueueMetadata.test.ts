import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { RelayConnectionTarget, type PreparedConnection } from "../connection/model.ts";
import type { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../rpc/http.ts";
import { fetchWorkQueueMetadata } from "./workQueueMetadata.ts";

function connection(id: string, cookie = false): PreparedConnection {
  const environmentId = EnvironmentId.make(id);
  return {
    environmentId,
    label: id,
    target: new RelayConnectionTarget({ environmentId, label: id }),
    httpBaseUrl: `https://${id}.test`,
    socketUrl: `wss://${id}.test/ws`,
    httpAuthorization: cookie ? null : { _tag: "Bearer", token: `synthetic-${id}` },
  };
}
const signer = Option.none<ManagedRelayDpopSigner["Service"]>();
it.effect("binds each metadata query to the selected environment and its credentials", () =>
  Effect.gen(function* () {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const layer = layerRemoteHttpClient(async (url, init) => {
      calls.push({ url: String(url), init: init ?? {} });
      return Response.json({ status: "unconfigured", reason: "not_configured" });
    });
    for (const id of ["first", "second"])
      expect(
        yield* fetchWorkQueueMetadata({ prepared: connection(id), signer }).pipe(
          Effect.provide(layer),
        ),
      ).toEqual({ status: "unconfigured", reason: "not_configured" });
    expect(calls.map(({ url }) => url)).toEqual([
      "https://first.test/api/work-queue/metadata",
      "https://second.test/api/work-queue/metadata",
    ]);
    expect(calls.map(({ init }) => new Headers(init.headers).get("authorization"))).toEqual([
      "Bearer synthetic-first",
      "Bearer synthetic-second",
    ]);
  }),
);
it.effect("retains cookie authentication and rejects private or malformed response fields", () =>
  Effect.gen(function* () {
    const layer = layerRemoteHttpClient(async (_url, init) => {
      expect(init?.credentials).toBe("include");
      return Response.json({
        status: "unavailable",
        reason: "source_unavailable",
        path: "/private",
      });
    });
    const error = yield* fetchWorkQueueMetadata({
      prepared: connection("local", true),
      signer,
    }).pipe(Effect.flip, Effect.provide(layer));
    expect(error._tag).toBe("RemoteEnvironmentAuthInvalidJsonError");
  }),
);
it.effect("keeps old or unreachable servers unavailable without returning mock or empty rows", () =>
  Effect.gen(function* () {
    const layer = layerRemoteHttpClient(async () => new Response("Missing", { status: 404 }));
    const error = yield* fetchWorkQueueMetadata({ prepared: connection("old"), signer }).pipe(
      Effect.flip,
      Effect.provide(layer),
    );
    expect(error).toMatchObject({
      _tag: "RemoteEnvironmentAuthUndeclaredStatusError",
      status: 404,
    });
  }),
);
