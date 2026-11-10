import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Dashboard from "./dashboard.ts";
import * as Summaries from "./summaryRead.ts";
import * as Routes from "./routes.ts";
import { RollupError } from "./evidence.ts";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
const caller: AdmittedCaller = {
  principal_id: "principal:test",
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const layer = Routes.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      Layer.succeed(Dashboard.Dashboard, {
        list: () => Effect.succeed({ items: [], next_cursor: null }),
        detail: () => Effect.fail(new RollupError({ code: "content_unavailable" })),
      }),
      Layer.succeed(Summaries.SummaryRead, {
        read: () =>
          Effect.succeed({
            contract: "workstreams-rollups/1.0.0",
            state: "provider_unconfigured",
            summary: null,
            freshness: "inaccessible",
          }),
        view: () => Effect.fail(new RollupError({ code: "forbidden" })),
      }),
    ),
  ),
);
it.effect(
  "read-equivalent typed routes keep forbidden404, private no-store and no payload on errors",
  () =>
    Effect.gen(function* () {
      const routes = yield* Routes.RollupRoutes;
      const summary = yield* routes.handle(caller, {
        method: "GET",
        path: "/dashboard/api/workstreams/ws:test/summary",
        query: {},
      });
      expect(summary.status).toBe(404);
      expect(summary.body).toEqual({ code: "not_found" });
      expect(summary.headers["cache-control"]).toBe("private, no-store");
      const missing = yield* routes.handle(caller, {
        method: "GET",
        path: "/dashboard/api/workstreams/ws:test",
        query: {},
      });
      expect(missing.status).toBe(503);
      expect(missing.body).toEqual({ code: "content_unavailable" });
      const native = yield* routes.handle(caller, {
        method: "GET",
        path: "/api/workstreams/native/rollup-summary",
        query: { workstream_id: "ws:test" },
      });
      expect(native.status).toBe(200);
      expect(native.body).toMatchObject({ state: "provider_unconfigured", summary: null });
      const list = yield* routes.handle(caller, {
        method: "GET",
        path: "/dashboard/api/workstreams",
        query: { limit: "50" },
      });
      expect(list.body).toEqual({ items: [], next_cursor: null });
      for (const request of [
        { method: "POST", path: "/dashboard/api/workstreams", query: {} },
        {
          method: "GET",
          path: "/dashboard/api/workstreams/ws:test/summary",
          query: { extra: "1" },
        },
        { method: "GET", path: "/api/workstreams/native/rollup-summary", query: {} },
      ]) {
        const response = yield* routes.handle(caller, request);
        expect(response.status >= 400).toBe(true);
        expect(response.headers["x-content-type-options"]).toBe("nosniff");
      }
    }).pipe(Effect.provide(layer)),
);
