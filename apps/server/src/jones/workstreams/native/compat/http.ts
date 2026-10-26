import type { WorkstreamReceipt } from "@t3tools/contracts";
import {
  WORKSTREAMS_HTTP_RESPONSE_CONTRACT_HEADER,
  WorkstreamsHttpResponseContract,
} from "@t3tools/contracts/jones/workstreamsHttp";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

const ModernResponse = Context.Reference<boolean>(
  "jones/workstreams/native/compat/http/ModernResponse",
  { defaultValue: () => false },
);
const supported = Schema.is(WorkstreamsHttpResponseContract);

/** Selection is request-local and applies only to raw command submit and receipt polling. */
export const withResponseContract = <E, R>(
  effect: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
  request: {
    readonly method: string;
    readonly path: string | undefined;
    readonly headers: Readonly<Record<string, string | undefined>>;
  },
): Effect.Effect<HttpServerResponse.HttpServerResponse, E, R> => {
  const selected =
    (request.method === "POST" && request.path === "/api/workstreams/commands") ||
    (request.method === "GET" && /^\/api\/workstreams\/commands\/[^/]+$/.test(request.path ?? ""));
  if (!selected) return effect;
  const contract = request.headers[WORKSTREAMS_HTTP_RESPONSE_CONTRACT_HEADER];
  if (contract !== undefined && !supported(contract))
    return Effect.succeed(HttpServerResponse.empty({ status: 400 }));
  return Effect.provideService(effect, ModernResponse, contract !== undefined);
};

export const receiptStatus = (receipt: WorkstreamReceipt) =>
  Effect.flatMap(ModernResponse, (modern) =>
    modern && (receipt.state === "pending" || receipt.state === "unresolved")
      ? HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(HttpServerResponse.setStatus(response, 202)),
        )
      : Effect.void,
  );

export const conflictMessage = (reason: "version-conflict" | "authorization-not-current") =>
  Effect.map(ModernResponse, (modern) =>
    modern && reason === "version-conflict" ? "version_conflict" : "workstream_command_not_current",
  );
