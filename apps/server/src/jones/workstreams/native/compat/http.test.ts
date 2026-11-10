import { expect, it } from "@effect/vitest";
import * as Wire from "@t3tools/contracts";
import { workstreamsHttpResponseHeaders } from "@t3tools/contracts/jones/workstreamsHttp";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import { FetchHttpClient, HttpClient, HttpServerRequest } from "effect/http";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { LegacyHttpApi, principal } from "../conformance/testkit.ts";

class CurrentApi extends HttpApi.make("environment").add(
  Wire.EnvironmentHttpApi.groups.workstreams,
) {}

const decodeReceipt = Schema.decodeUnknownEffect(Wire.WorkstreamReceipt);
const isConflict = Schema.is(Wire.EnvironmentHttpConflictError);

// The mounted old-service fixture uses the pinned b9719594 endpoint metadata, ignores the new
// request header and emits the old 200-only success and stale-refusal forms. No listener is opened.
it.effect.each(["pending", "unresolved"] as const)(
  "current opted-in decoder accepts an old server's 200 %s and stale refusal",
  (state) =>
    Effect.gen(function* () {
      const receipt = yield* decodeReceipt({
        command_id: "old-server-command-0001",
        owner_id: "synthetic-owner",
        actor: { principal_id: "synthetic-principal" },
        operation: "create_workstream",
        request_sha256: "1".repeat(64),
        server_generation: 1,
        accepted_at: "2026-10-26T00:00:00Z",
        state,
        retry_after_seconds: 1,
      });
      const seen: Array<string | undefined> = [];
      const observe = Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        seen.push(request.headers["x-t3-workstreams-response-contract"]);
      });
      const routes = HttpApiBuilder.layer(LegacyHttpApi).pipe(
        Layer.provide(
          HttpApiBuilder.group(LegacyHttpApi, "workstreams", (handlers) =>
            handlers
              .handle("command", () => observe.pipe(Effect.as(receipt)))
              .handle("submit", ({ payload }) =>
                observe.pipe(
                  Effect.andThen(
                    payload.command.command_id === receipt.command_id
                      ? Effect.succeed(receipt)
                      : Effect.fail(
                          new Wire.EnvironmentHttpConflictError({
                            message: "workstream_command_not_current",
                          }),
                        ),
                  ),
                ),
              ),
          ),
        ),
        Layer.provide(
          Layer.succeed(Wire.EnvironmentAuthenticatedAuth, (effect) =>
            Effect.provideService(effect, Wire.EnvironmentAuthenticatedPrincipal, principal),
          ),
        ),
        Layer.provideMerge(
          HttpPlatform.layer.pipe(
            Layer.provideMerge(NodeServices.layer),
            Layer.provideMerge(Etag.layerWeak),
          ),
        ),
        Layer.provide(NodeServices.layer),
      );
      const app = yield* Effect.acquireRelease(
        Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
        (value) => Effect.promise(() => value.dispose()),
      );
      const client = yield* Effect.flatMap(HttpClient.HttpClient, (httpClient) =>
        HttpApiClient.group(CurrentApi, {
          group: "workstreams",
          baseUrl: "http://old-service",
          httpClient,
        }),
      ).pipe(
        Effect.provide(
          FetchHttpClient.layer.pipe(
            Layer.provide(
              Layer.succeed(FetchHttpClient.Fetch, (input, init) =>
                app.handler(new Request(input, init)),
              ),
            ),
          ),
        ),
      );
      const command: Wire.WorkstreamCommand = {
        command_id: receipt.command_id,
        expected_server_generation: 1,
        expected_registry_version: 0,
        action: {
          operation: "create_workstream",
          name: "Old server",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        },
      };
      const headers = {
        authorization: "Bearer synthetic-old-service",
        ...workstreamsHttpResponseHeaders,
      };
      expect(yield* client.submit({ headers, payload: { command } })).toEqual(receipt);
      expect(yield* client.command({ headers, params: { commandId: command.command_id } })).toEqual(
        receipt,
      );
      const stale = yield* client
        .submit({
          headers,
          payload: { command: { ...command, command_id: "old-server-stale-00001" } },
        })
        .pipe(Effect.result);
      expect(stale._tag).toBe("Failure");
      if (stale._tag !== "Failure") throw new Error("Old service fixture accepted a stale command");
      expect(isConflict(stale.failure)).toBe(true);
      expect(stale.failure).toMatchObject({ message: "workstream_command_not_current" });
      expect(seen).toEqual(Array(3).fill("workstreams-http-response/1.0.0"));
    }).pipe(Effect.scoped),
);
