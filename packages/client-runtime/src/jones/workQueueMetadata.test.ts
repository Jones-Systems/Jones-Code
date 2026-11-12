/// <reference path="../../../contracts/src/workstreams-fixtures/raw-fixtures.d.ts" />
import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { RelayConnectionTarget, type PreparedConnection } from "../connection/model.ts";
import type { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import { layerRemoteHttpClient } from "../rpc/http.ts";
import {
  fetchWorkQueueMetadata,
  publishWorkQueueLocalNames,
  readWorkQueueLocalNames,
  subscribeWorkQueueLocalNames,
  workQueueDestinationLabel,
} from "./workQueueMetadata.ts";
import producerV4Raw from "../../../contracts/src/jones/fixtures/work_queue_metadata_v4.json.fixture?raw";

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

it.effect("fetches v4 unchanged from the selected environment without a name lookup", () =>
  Effect.gen(function* () {
    const snapshot = JSON.parse(producerV4Raw);
    const response = { status: "ready", snapshot, expires_at_ms: 30_100 };
    const calls: string[] = [];
    const layer = layerRemoteHttpClient(async (url) => {
      calls.push(String(url));
      return Response.json(response);
    });
    expect(
      yield* fetchWorkQueueMetadata({ prepared: connection("selected"), signer }).pipe(
        Effect.provide(layer),
      ),
    ).toEqual(response);
    expect(calls).toEqual(["https://selected.test/api/work-queue/metadata"]);
  }),
);
it("uses only same-environment safe local labels and clears them with their owner", () => {
  let notifications = 0;
  const unsubscribe = subscribeWorkQueueLocalNames(() => {
    notifications += 1;
  });
  const release = publishWorkQueueLocalNames("env", [
    { workstreamId: "work", name: "Local project" },
  ]);
  try {
    expect(workQueueDestinationLabel("work", "env", readWorkQueueLocalNames())).toBe(
      "Local project (work)",
    );
    expect(workQueueDestinationLabel("work", "other", readWorkQueueLocalNames())).toBe("work");
    expect(workQueueDestinationLabel("missing", "env", readWorkQueueLocalNames())).toBe("missing");
    for (const name of ["", "   ", "x".repeat(257), "bad\nname", "hidden\u202ename"]) {
      expect(
        workQueueDestinationLabel("work", "env", {
          environmentId: "env",
          names: new Map([["work", name]]),
        }),
      ).toBe("work");
    }
  } finally {
    release();
    unsubscribe();
  }
  expect(readWorkQueueLocalNames()).toBeNull();
  expect(notifications).toBe(2);
});
