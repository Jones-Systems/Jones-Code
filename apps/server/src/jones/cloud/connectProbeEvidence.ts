import { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { FetchHttpClient, HttpClient, HttpServer } from "effect/http";

import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import { loopbackUrlHostForAddress } from "../../startupAccess.ts";

export interface ConnectHttpEvidence {
  readonly status: number;
  readonly contentType?: string;
  readonly rayId?: string;
}

export type ConnectLocalEvidence = "matching_environment" | "different_environment" | "unverified";

const CONTENT_TYPES = new Set([
  "application/json",
  "application/problem+json",
  "text/html",
  "text/plain",
]);

/** Never retain bodies, cookies, redirects, or arbitrary header values from the public path. */
export function connectHttpEvidence(
  status: number,
  headers: Readonly<Record<string, string | undefined>>,
): ConnectHttpEvidence {
  const mediaType = headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase();
  const rayId = headers["cf-ray"];
  return {
    status,
    ...(mediaType && CONTENT_TYPES.has(mediaType) ? { contentType: mediaType } : {}),
    ...(rayId && /^[a-f0-9]{16,32}-[A-Z]{3}$/.test(rayId) ? { rayId } : {}),
  };
}

/** Uses this server's bound listener, including ephemeral ports; no runtime file or guessed port. */
export const probeConnectLoopback = Effect.fn("ConnectReachability.loopbackDescriptor")(
  function* () {
    const server = yield* HttpServer.HttpServer;
    const environment = yield* ServerEnvironment.ServerEnvironment;
    const expectedId = yield* environment.getEnvironmentId;
    const address = server.address;
    if (typeof address === "string" || !("port" in address)) return "unverified" as const;
    const client = yield* HttpClient.HttpClient;
    const origin = `http://${loopbackUrlHostForAddress(address)}:${address.port}`;
    return yield* client.get(`${origin}/.well-known/t3/environment`).pipe(
      Effect.flatMap((response) => {
        if (response.status !== 200) return Effect.succeed<ConnectLocalEvidence>("unverified");
        // Bound the public descriptor before decoding it. Even a wrong local listener may answer.
        return response.stream.pipe(
          Stream.runFoldEffect(
            () => new Uint8Array(0),
            (bytes, chunk) => {
              if (bytes.byteLength + chunk.byteLength > 64 * 1024) {
                return Effect.fail("descriptor_too_large");
              }
              const next = new Uint8Array(bytes.byteLength + chunk.byteLength);
              next.set(bytes);
              next.set(chunk, bytes.byteLength);
              return Effect.succeed(next);
            },
          ),
          Effect.flatMap((bytes) =>
            Schema.decodeEffect(Schema.fromJsonString(ExecutionEnvironmentDescriptor))(
              new TextDecoder().decode(bytes),
            ),
          ),
          Effect.map((descriptor): ConnectLocalEvidence =>
            descriptor.environmentId === expectedId
              ? "matching_environment"
              : "different_environment",
          ),
        );
      }),
      Effect.provideService(FetchHttpClient.RequestInit, {
        redirect: "manual",
        credentials: "omit",
      }),
      Effect.scoped,
      Effect.timeoutOption("3 seconds"),
      Effect.map(Option.getOrElse((): ConnectLocalEvidence => "unverified")),
      Effect.orElseSucceed((): ConnectLocalEvidence => "unverified"),
    );
  },
);

export function unexpectedConnectResponse(
  evidence: ConnectHttpEvidence,
  local: ConnectLocalEvidence | null | undefined,
): { readonly reason: string; readonly hint: string; readonly fix: string } {
  const localAnswer =
    local === "matching_environment"
      ? " The server answers locally with the expected environment identity."
      : local === "different_environment"
        ? " The local listener answered for a different environment."
        : " Local server reachability is unverified.";
  const details = [
    evidence.contentType ? `Content type: ${evidence.contentType}.` : null,
    evidence.rayId ? `Response Ray ID: ${evidence.rayId} (does not identify the cause).` : null,
  ].filter(Boolean);
  return {
    reason: `This host's anonymous request to its T3 Connect address returned HTTP ${evidence.status}.${localAnswer} Authenticated reachability from a signed-in device is unverified.`,
    hint:
      details.length > 0
        ? details.join(" ")
        : "The response alone does not identify which service refused the request.",
    fix: "Try the connection from a signed-in device, then use Details > Copy diagnostics on that device. Check access rules, proxies, and firewalls for the external path.",
  };
}
