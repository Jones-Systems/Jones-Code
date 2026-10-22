import type { WireFixture } from "./loopbackControlPlane.fixture.mjs";
// @effect-diagnostics nodeBuiltinImport:off -- Test-only sockets and hashing never read host credentials or state.
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
  WorkstreamCommand,
  type WorkstreamsRegistrationContextT3Source,
} from "@t3tools/contracts";
import { makeControlPlaneWorkstreamTransport } from "../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import { makeSyntheticWorkstreamTransport } from "../../../workstreams/SyntheticWorkstreamTransport.ts";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { make, WorkstreamGateway } from "../../../workstreams/WorkstreamGateway.ts";
import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "../../../workstreams/http.ts";
import {
  makeWorkstreamsRegistrationContext,
  WorkstreamsRegistrationContext,
} from "../registrationContext/service.ts";

const decodeCommand = Schema.decodeUnknownSync(Schema.fromJsonString(WorkstreamCommand));
const signingSecret = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
export const build = {
  repository: "Jones-Systems/Jones-Code" as const,
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
export const sources: readonly WorkstreamsRegistrationContextT3Source[] = [
  "source-a",
  "source-b",
].map((id) => ({
  provider: "t3",
  source_instance_id: id,
  authority_namespace: "synthetic-authority",
  store_generation: 1,
  resource_kind: "thread",
  id_kind: "internal",
  account_provenance: { kind: "not_account_scoped" },
  native_protocol: "workstreams-t3-provider/1.0.0",
  build,
}));

/** Owns only its ephemeral loopback listener; close also releases keep-alive connections. */
async function startLoopbackHandler(handler: (request: Request) => Promise<Response>) {
  if (process.env.NODE_ENV === "production") throw new Error("Test-only loopback fixture");
  const server = NodeHttp.createServer((incoming, outgoing) => {
    void (async () => {
      let body = "";
      for await (const chunk of incoming) {
        body += String(chunk);
        if (Buffer.byteLength(body) > 262_144) throw new Error("Fixture request too large");
      }
      const headers = new Headers();
      for (const [key, value] of Object.entries(incoming.headers)) {
        if (value !== undefined) headers.set(key, Array.isArray(value) ? value.join(", ") : value);
      }
      const response = await handler(
        new Request(`http://127.0.0.1${incoming.url}`, {
          method: incoming.method ?? "GET",
          headers,
          ...(body ? { body } : {}),
        }),
      );
      if (response.type === "error") {
        outgoing.destroy();
        return;
      }
      outgoing.writeHead(response.status, Object.fromEntries(response.headers));
      outgoing.end(Buffer.from(await response.arrayBuffer()));
    })().catch(() => {
      outgoing.writeHead(500);
      outgoing.end("fixture failure");
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing loopback address");
  return {
    origin: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) reject(error);
          else if (server.listening || server.address() !== null)
            reject(new Error("Fixture listener still open"));
          else resolve();
        });
        server.closeAllConnections();
      }),
  };
}

/** HTTP adaptation only: all registry mutations and receipts belong to the existing synthetic transport. */
export async function startLoopbackControlPlane() {
  const registry = makeSyntheticWorkstreamTransport({
    principalId: "22222222-2222-4222-8222-222222222222",
    assignments: [
      { sourceInstanceId: "source-a", nativeThreadId: "shared-thread" },
      { sourceInstanceId: "source-b", nativeThreadId: "shared-thread" },
      { sourceInstanceId: "source-b", nativeThreadId: "other-thread" },
    ],
  });
  const observations = {
    accepted: 0,
    rejected: 0,
    submissions: 0,
    capabilities: 0,
    receiptReads: 0,
    refusals: 0,
    commandIds: [] as string[],
  } satisfies WireFixture["control"]["observations"];
  const principalId = "22222222-2222-4222-8222-222222222222";
  const pin = {
    protocol: "workstreams-registry-binding/1.0.0",
    manifest_sha256: "15ce45061966ce9c247192d6ef43724b6638bb0e7ccf7bbe9eae4afdd576f30c",
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    canonical_origin: "https://workstreams.test",
    owner_id: "owner-fixture",
    principal_id: principalId,
    authorization_revision: 1,
  };
  const token = Buffer.from(JSON.stringify(pin)).toString("base64url");
  const authority = { ...pin };
  const faults = {
    changeGrantBeforeSubmit: false,
    commandResponse: "valid" as "valid" | "missing" | "invalid" | "replayed" | "lost",
    listResponse: "valid" as "valid" | "missing",
    receiptResponse: "valid" as "valid" | "missing",
    readResponse: "valid" as "valid" | "missing" | "invalid",
  };
  let previousMac = "";
  const contract = {
    contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
    contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  };
  const listener = await startLoopbackHandler(async (request) => {
    const url = new URL(request.url);
    const body = await request.text();
    const registration = url.pathname === "/workstreams/v1/t3/registration-context";
    const version = registration
      ? WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL
      : WORKSTREAM_CONTRACT_HEADER_VERSION;
    const manifest = registration
      ? WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256
      : WORKSTREAM_CONTRACT_MANIFEST_SHA256;
    const header = (name: string) => request.headers.get(name) ?? "";
    const digest = NodeCrypto.createHash("sha256").update(body).digest("hex");
    // Independent producer-envelope implementation, qualified against the pinned producer vectors.
    // Business authority remains the existing synthetic transport, not a second registry model.
    const canonical = [
      "hmac-sha256-workstreams-registry-v1",
      version,
      header("x-control-request-id"),
      header("idempotency-key"),
      header("x-control-timestamp"),
      header("x-control-nonce"),
      principalId,
      "key-fixture",
      request.method,
      url.pathname + url.search,
      digest,
      manifest,
      header("x-control-registry-binding"),
    ].join("\n");
    const expected = NodeCrypto.createHmac("sha256", Buffer.from(signingSecret, "base64"))
      .update(canonical)
      .digest("base64url");
    if (
      header("x-control-signature") !== expected ||
      header("x-control-content-sha256") !== digest ||
      header("x-control-principal-id") !== principalId ||
      header("x-control-key-id") !== "key-fixture" ||
      header("x-control-contract-version") !== version ||
      header("x-control-contract-manifest") !== manifest ||
      header("x-control-algorithm") !== "hmac-sha256-workstreams-registry-v1"
    ) {
      observations.rejected += 1;
      return new Response("Invalid synthetic signature or contract", { status: 401 });
    }
    observations.accepted += 1;
    const commandPost = url.pathname === "/workstreams/v1/commands" && request.method === "POST";
    if (commandPost) {
      observations.commandIds.push(decodeCommand(body).command_id);
      if (faults.changeGrantBeforeSubmit) {
        authority.authorization_revision += 1;
        faults.changeGrantBeforeSubmit = false;
      }
    }
    const respond = (payload: string, status: number, dispatched: boolean): Response => {
      const responseHeaders = new Headers(
        dispatched
          ? {
              "x-control-contract-version": version,
              "x-control-contract-manifest": manifest,
            }
          : {},
      );
      const dispatch = dispatched ? "dispatched" : "not-dispatched";
      const responseCanonical = [
        "workstreams-registry-response/1.0.0",
        header("x-control-signature"),
        String(status),
        responseHeaders.get("x-control-contract-version") ?? "",
        responseHeaders.get("x-control-contract-manifest") ?? "",
        header("x-control-registry-binding"),
        dispatch,
        NodeCrypto.createHash("sha256").update(payload).digest("hex"),
      ].join("\n");
      const mac = NodeCrypto.createHmac("sha256", Buffer.from(signingSecret, "base64"))
        .update(responseCanonical)
        .digest("base64url");
      const mode = commandPost
        ? faults.commandResponse
        : url.pathname.startsWith("/workstreams/v1/commands/")
          ? faults.receiptResponse
          : url.pathname === "/workstreams/v1/workstreams"
            ? faults.listResponse
            : faults.readResponse;
      if (mode === "lost") return Response.error();
      if (mode !== "missing") {
        responseHeaders.set("x-control-registry-binding", header("x-control-registry-binding"));
        responseHeaders.set("x-control-registry-dispatch", dispatch);
        responseHeaders.set(
          "x-control-registry-response-mac",
          mode === "replayed" ? previousMac : mode === "invalid" ? "A".repeat(43) : mac,
        );
      }
      previousMac = mac;
      responseHeaders.set("content-type", "application/json");
      return new Response(payload, { status, headers: responseHeaders });
    };
    if (
      header("x-control-registry-binding") !==
      Buffer.from(JSON.stringify(authority)).toString("base64url")
    ) {
      observations.refusals += 1;
      return respond('{"code":"registry_binding_not_current"}', 409, false);
    }
    const page = {
      ...contract,
      limit: Number(url.searchParams.get("limit") ?? 100),
      ...(url.searchParams.has("cursor") ? { cursor: url.searchParams.get("cursor")! } : {}),
    };
    let result: unknown;
    const segments = url.pathname.split("/").filter(Boolean);
    if (registration) {
      const capabilities = await Effect.runPromise(registry.getCapabilities(contract));
      result = {
        protocol: WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
        state: "ready",
        ...capabilities.context,
        principal_id: principalId,
        authorization_revision: 1,
        grant_id: "grant-fixture",
        sources,
      };
    } else if (url.pathname === "/workstreams/v1/capabilities") {
      observations.capabilities += 1;
      result = await Effect.runPromise(registry.getCapabilities(contract));
    } else if (segments[2] === "commands" && request.method === "POST") {
      const command = decodeCommand(body);
      observations.submissions += 1;
      result = await Effect.runPromise(
        registry.submitCommand({
          ...contract,
          body,
          command,
          idempotencyKey: header("idempotency-key"),
        }),
      );
    } else if (segments[2] === "commands" && segments[3]) {
      observations.receiptReads += 1;
      result = await Effect.runPromise(
        registry.getCommand({ ...contract, commandId: segments[3] }),
      );
    } else if (segments[2] === "references") {
      result = segments[3]
        ? await Effect.runPromise(
            registry.getReference({ ...contract, nativeReferenceId: segments[3] }),
          )
        : await Effect.runPromise(registry.listReferences(page));
    } else if (segments[2] === "workstreams") {
      const workstreamId = segments[3];
      result = !workstreamId
        ? await Effect.runPromise(registry.listWorkstreams(page))
        : segments[4] === "memberships"
          ? await Effect.runPromise(registry.listMemberships({ ...page, workstreamId }))
          : await Effect.runPromise(registry.getWorkstream({ ...contract, workstreamId }));
    } else return new Response("Fixture route not implemented", { status: 404 });
    return respond(JSON.stringify(result), 200, true);
  });
  return {
    ...listener,
    observations,
    faults,
    authority,
    activation: {
      state: "enabled" as const,
      value: {
        baseUrl: new URL(listener.origin),
        ownerId: "owner-fixture",
        principalId,
        registryBinding: token,
        authorizationRevision: 1,
        keyId: "key-fixture",
        signingSecret,
        registrationContextVersion: "2.0.0" as const,
      },
    },
  };
}

class WorkstreamTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workstreams,
) {}

// Authentication identity is injected; production HTTP scopes and all Workstream handlers still run.
const authLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  handler.pipe(
    Effect.provideService(EnvironmentAuthenticatedPrincipal, {
      sessionId: AuthSessionId.make("multi-environment-fixture"),
      subject: "synthetic-owner",
      method: "bearer-access-token",
      scopes: new Set(["orchestration:read", "orchestration:operate"] as const),
    }),
  ),
);

type ControlPlane = Awaited<ReturnType<typeof startLoopbackControlPlane>>;
export async function startEnvironment(
  control: ControlPlane,
  environmentId: string,
  stamp: typeof build | null = build,
) {
  const configured = makeControlPlaneWorkstreamTransport(control.activation);
  const gateway = await Effect.runPromise(
    make(configured.transport, { binding: configured.binding }),
  );
  const registration = makeWorkstreamsRegistrationContext({
    ...configured.registrationContext,
    authority: {
      readCurrent: Effect.succeed({
        environmentId,
        authorityNamespace: "synthetic-authority",
        storeGeneration: 1,
      }),
    },
    build: Effect.succeed(Option.fromNullishOr(stamp)),
  });
  const app = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(WorkstreamTestApi).pipe(
      Layer.provide(workstreamHttpApiLayer),
      Layer.provide(Layer.succeed(WorkstreamGateway, gateway)),
      Layer.provide(Layer.succeed(WorkstreamsRegistrationContext, registration)),
      Layer.provide(authLayer),
      Layer.provide(workstreamResponseHeadersLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );
  try {
    const listener = await startLoopbackHandler((request) => app.handler(request));
    return {
      ...listener,
      close: async () => {
        try {
          await listener.close();
        } finally {
          await app.dispose();
        }
      },
    };
  } catch (error) {
    await app.dispose();
    throw error;
  }
}
