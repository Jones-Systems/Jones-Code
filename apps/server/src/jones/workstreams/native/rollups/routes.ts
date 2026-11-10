import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { AdmittedCaller } from "../mode/authorityMode.ts";
import * as Dashboard from "./dashboard.ts";
import * as SummaryRead from "./summaryRead.ts";
import { RollupError } from "./evidence.ts";
export interface RouteResponse {
  readonly status: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: unknown;
}
export class RollupRoutes extends Context.Service<
  RollupRoutes,
  {
    readonly handle: (
      caller: AdmittedCaller,
      request: {
        readonly method: string;
        readonly path: string;
        readonly query: Readonly<Record<string, string>>;
      },
    ) => Effect.Effect<RouteResponse>;
  }
>()("t3/jones/workstreams/native/rollups/routes/RollupRoutes") {}
const headers = {
  "cache-control": "private, no-store",
  "x-content-type-options": "nosniff",
  "content-type": "application/json",
};
const make = Effect.gen(function* () {
  const dashboard = yield* Dashboard.Dashboard,
    summaries = yield* SummaryRead.SummaryRead;
  return RollupRoutes.of({
    handle: Effect.fn("WorkstreamRollupRoutes.handle")((caller, request) =>
      Effect.gen(function* () {
        if (request.method !== "GET") return yield* new RollupError({ code: "not_found" });
        if (request.path === "/dashboard/api/workstreams") {
          if (
            Object.keys(request.query).some((k) => k !== "limit" && k !== "cursor") ||
            (request.query.limit !== undefined && !/^[1-9][0-9]{0,2}$/.test(request.query.limit))
          )
            return yield* new RollupError({ code: "invalid_request" });
          return yield* dashboard.list(caller, {
            ...(request.query.limit === undefined ? {} : { limit: Number(request.query.limit) }),
            ...(request.query.cursor === undefined ? {} : { cursor: request.query.cursor }),
          });
        }
        const match = /^\/dashboard\/api\/workstreams\/([A-Za-z0-9._:-]{1,512})(\/summary)?$/.exec(
          request.path,
        );
        if (match) {
          if (Object.keys(request.query).length)
            return yield* new RollupError({ code: "invalid_request" });
          return yield* match[2]
            ? summaries.view(caller, match[1]!)
            : dashboard.detail(caller, match[1]!);
        }
        if (request.path === "/api/workstreams/native/rollup-summary") {
          if (Object.keys(request.query).length !== 1 || !request.query.workstream_id)
            return yield* new RollupError({ code: "invalid_request" });
          return yield* summaries.read(caller, request.query.workstream_id);
        }
        return yield* new RollupError({ code: "not_found" });
      }).pipe(
        Effect.map((body) => ({ status: 200, headers, body })),
        Effect.catchTags({
          WorkstreamsRollupError: (e) =>
            Effect.succeed({
              status:
                e.code === "invalid_request"
                  ? 400
                  : e.code === "not_found" || e.code === "forbidden"
                    ? 404
                    : 503,
              headers,
              body: { code: e.code === "forbidden" ? "not_found" : e.code },
            }),
        }),
      ),
    ),
  });
});
/** @public Typed transport adapter; the parent mounts it behind authenticated admission on both listeners. */
export const layer = Layer.effect(RollupRoutes, make);
