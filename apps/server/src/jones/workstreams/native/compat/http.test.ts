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
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The test owns exact-path cleanup after its scoped SQLite services close.
import * as NodeFS from "node:fs";
import * as SqlClient from "effect/sql/SqlClient";
import { vi } from "vite-plus/test";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import * as Receipts from "../store/receiptStore.ts";
import {
  LegacyHttpApi,
  principal,
  fresh,
  seed,
  harness,
  http,
  app,
  json,
  Store,
  scope,
  legacyWorkstreamCommandNoEffect,
} from "../conformance/testkit.ts";

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

it.effect(
  "headerless legacy reads and stale-generation submits stay inside the selected native home",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const cp = vi
        .spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport")
        .mockImplementation(() => {
          throw new Error("Native HOME constructed the legacy Control Plane transport");
        });
      const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
        throw new Error("Native HOME attempted an external request");
      });
      try {
        const prepared = yield* seed(f);
        yield* Effect.gen(function* () {
          const h = yield* harness,
            web = yield* http,
            store = yield* Store.RegistryStore,
            sql = yield* SqlClient.SqlClient;
          const id = yield* h.create("Native HOME visible to an old client");
          const command = {
            ...(yield* h.command({
              operation: "create_workstream",
              name: "Old generation must not write",
              lifecycle: "active",
              progress: { state: "unknown" },
              sort_order: 0,
            })),
            expected_server_generation: 2,
          };
          const before = yield* store.context(scope);
          const changes = yield* sql`SELECT total_changes() AS count`;
          // These are the original read endpoints and DTOs, without either native opt-in header.
          const list = yield* web.request("?limit=50");
          expect(list.status).toBe(200);
          expect(list.headers.get("cache-control")).toBe("private, no-store");
          const page = yield* Effect.flatMap(
            json(list),
            Schema.decodeUnknownEffect(Wire.T3WorkstreamListResult),
          );
          expect(page.items.map((item) => item.workstreamId)).toEqual([id]);
          expect(page).toMatchObject({ source: "live", stale: false, nextCursor: null });
          const detail = yield* web.request("/" + id);
          expect(detail.status).toBe(200);
          expect(
            yield* Effect.flatMap(json(detail), Schema.decodeUnknownEffect(Wire.WorkstreamDetail)),
          ).toMatchObject({
            workstream: { workstream_id: id, name: "Native HOME visible to an old client" },
          });
          for (const [path, schema] of [
            ["/references?limit=50", Wire.WorkstreamReferencePage],
            ["/" + id + "/memberships?limit=50", Wire.WorkstreamMembershipPage],
            ["/" + id + "/declarations?limit=50", Wire.WorkstreamDeclarationPage],
            ["/" + id + "/edges?limit=50", Wire.WorkstreamEdgePage],
            ["/" + id + "/history?limit=50", Wire.WorkstreamHistoryPage],
          ] as const) {
            const response = yield* web.request(path);
            expect(response.status).toBe(200);
            expect(yield* json(response)).toSatisfy(Schema.is(schema));
          }
          const refused = yield* web.oldClient
            .submit({ headers: web.headers, payload: { command } })
            .pipe(Effect.result);
          expect(refused._tag).toBe("Failure");
          if (refused._tag !== "Failure")
            throw new Error("Old-generation command mutated the native registry");
          expect(legacyWorkstreamCommandNoEffect(refused.failure)).toBe("stale");
          const modern = yield* web.modernRequest("/commands", { command });
          expect(modern.status).toBe(409);
          expect(yield* json(modern)).toMatchObject({ message: "version_conflict" });
          const legacy = yield* web.request("/commands", { command });
          expect(legacy.status).toBe(409);
          expect(yield* json(legacy)).toMatchObject({ message: "workstream_command_not_current" });
          expect(yield* (yield* Receipts.ReceiptStore).read(scope, command.command_id)).toBeNull();
          expect(yield* store.context(scope)).toEqual(before);
          expect(yield* sql`SELECT total_changes() AS count`).toEqual(changes);
          expect(prepared.provider.calls).toHaveLength(0);
          expect(cp).not.toHaveBeenCalled();
          expect(fetch).not.toHaveBeenCalled();
        }).pipe(app, Effect.provide(prepared.services));
      } finally {
        cp.mockRestore();
        fetch.mockRestore();
      }
    }).pipe(
      Effect.scoped,
      Effect.provide(f.layer),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(f.home, { recursive: true, force: true }))),
    );
  },
);
