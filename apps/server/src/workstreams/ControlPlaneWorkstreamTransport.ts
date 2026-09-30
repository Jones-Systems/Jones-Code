import * as NodeCrypto from "node:crypto";

import {
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_VERSION,
  WORKSTREAM_MAX_RESPONSE_BYTES,
  WorkstreamCapabilities,
  WorkstreamDeclarationPage,
  WorkstreamDetail,
  WorkstreamEdgePage,
  WorkstreamHistoryPage,
  WorkstreamMembershipPage,
  WorkstreamPage,
  WorkstreamReceipt,
  WorkstreamReferenceDetail,
  WorkstreamReferencePage,
  WorkstreamError,
  type T3WorkstreamBinding,
  T3_PLACEMENT_CONTRACT,
  T3_PLACEMENT_MANIFEST_SHA256,
  T3_PLACEMENT_ROUTE,
  T3PlacementPage,
  T3PlacementRequest,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import { WorkstreamTransportError, type WorkstreamTransport } from "./WorkstreamGateway.ts";

const EMPTY_SHA256 = NodeCrypto.createHash("sha256").update("").digest("hex");
const TIMEOUT_MS = 15_000;

class BoundedTransportFailure extends Error {
  readonly reason: "invalid_target" | "response_too_large" | "http_error" | "cursor_stale";

  constructor(reason: "invalid_target" | "response_too_large" | "http_error" | "cursor_stale") {
    super(reason);
    this.reason = reason;
  }
}

export interface ControlPlaneWorkstreamActivation {
  readonly baseUrl: URL;
  readonly ownerId: string;
  readonly principalId: string;
  readonly authorizationRevision: number;
  readonly keyId: string;
  readonly signingSecret: string;
}

export type WorkstreamActivation =
  | { readonly state: "disabled" }
  | { readonly state: "enabled"; readonly value: ControlPlaneWorkstreamActivation };

export type WorkstreamFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

function hasCanonicalActivation(config: ControlPlaneWorkstreamActivation): boolean {
  const headerValues = [config.ownerId, config.principalId, config.keyId, config.signingSecret];
  return (
    config.baseUrl.protocol === "https:" &&
    !config.baseUrl.username &&
    !config.baseUrl.password &&
    !config.baseUrl.hash &&
    !config.baseUrl.search &&
    (config.baseUrl.pathname === "/" || config.baseUrl.pathname === "") &&
    Number.isSafeInteger(config.authorizationRevision) &&
    config.authorizationRevision > 0 &&
    headerValues.every((value) => value.length > 0 && !/[\r\n]/.test(value)) &&
    decodeSigningSecret(config.signingSecret) !== null
  );
}

function decodeSigningSecret(value: string): Buffer | null {
  if (!/^[A-Za-z0-9+/]{43}=$/.test(value)) return null;
  const key = Buffer.from(value, "base64");
  return key.length === 32 && key.toString("base64") === value ? key : null;
}

function activationFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): WorkstreamActivation {
  const raw = environment.T3_WORKSTREAM_CONTROL_PLANE_URL;
  const ownerId = environment.T3_WORKSTREAM_OWNER_ID;
  const principalId = environment.T3_WORKSTREAM_PRINCIPAL_ID;
  const keyId = environment.T3_WORKSTREAM_KEY_ID;
  const signingSecret = environment.T3_WORKSTREAM_SIGNING_SECRET;
  const revision = Number(environment.T3_WORKSTREAM_AUTHORIZATION_REVISION);
  if (!raw && !ownerId && !principalId && !keyId && !signingSecret) return { state: "disabled" };
  if (
    !raw ||
    !ownerId ||
    !principalId ||
    !keyId ||
    !signingSecret ||
    !Number.isSafeInteger(revision) ||
    revision < 1
  ) {
    return { state: "disabled" };
  }
  try {
    const baseUrl = new URL(raw);
    const activation = {
      state: "enabled",
      value: {
        baseUrl,
        ownerId,
        principalId,
        authorizationRevision: revision,
        keyId,
        signingSecret,
      },
    } as const;
    return hasCanonicalActivation(activation.value) ? activation : { state: "disabled" };
  } catch {
    return { state: "disabled" };
  }
}

const disabled = (): WorkstreamTransport => {
  const unavailable = (operation: string) =>
    Effect.fail(
      new WorkstreamTransportError({
        operation,
        effect: "no-effect",
        detail: "Workstream control-plane activation is disabled.",
      }),
    );
  return {
    getCapabilities: () => unavailable("capabilities"),
    listWorkstreams: () => unavailable("list_workstreams"),
    getWorkstream: () => unavailable("get_workstream"),
    listReferences: () => unavailable("list_references"),
    getReference: () => unavailable("get_reference"),
    listMemberships: () => unavailable("list_memberships"),
    listDeclarations: () => unavailable("list_declarations"),
    listEdges: () => unavailable("list_edges"),
    listHistory: () => unavailable("list_history"),
    getCommand: () => unavailable("get_command"),
    submitCommand: () => unavailable("submit_command"),
  };
};

function query(input: { readonly limit: number; readonly cursor?: string }): string {
  const value = new URLSearchParams({ limit: String(input.limit) });
  if (input.cursor !== undefined) value.set("cursor", input.cursor);
  return value.toString();
}

export function signWorkstreamRequest(input: {
  readonly contractVersion?:
    | typeof WORKSTREAM_CONTRACT_HEADER_VERSION
    | typeof T3_PLACEMENT_CONTRACT;
  readonly requestId: string;
  readonly idempotencyKey?: string;
  readonly sentAt: string;
  readonly nonce: string;
  readonly principalId: string;
  readonly keyId: string;
  readonly method: "GET" | "POST";
  readonly target: string;
  readonly contentSha256: string;
  readonly signingSecret: string;
}): string {
  const key = decodeSigningSecret(input.signingSecret);
  if (key === null) throw new Error("invalid_workstream_signing_secret");
  const canonical = [
    "hmac-sha256-v1",
    input.contractVersion ?? WORKSTREAM_CONTRACT_HEADER_VERSION,
    input.requestId,
    input.idempotencyKey ?? "",
    input.sentAt,
    input.nonce,
    input.principalId,
    input.keyId,
    input.method,
    input.target,
    input.contentSha256,
  ].join("\n");
  return NodeCrypto.createHmac("sha256", key).update(canonical).digest("base64url");
}

async function readBoundedResponse(response: Response): Promise<string> {
  const declared = response.headers.get("content-length");
  if (declared !== null && Number(declared) > WORKSTREAM_MAX_RESPONSE_BYTES) {
    await response.body?.cancel();
    throw new BoundedTransportFailure("response_too_large");
  }
  if (response.body === null) {
    if (!response.ok) throw new BoundedTransportFailure("http_error");
    return "";
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let total = 0;
  let body = "";
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      total += chunk.value.byteLength;
      if (total > WORKSTREAM_MAX_RESPONSE_BYTES) {
        throw new BoundedTransportFailure("response_too_large");
      }
      body += decoder.decode(chunk.value, { stream: true });
    }
    body += decoder.decode();
  } catch (cause) {
    await reader.cancel().catch(() => undefined);
    throw cause;
  }
  if (!response.ok) {
    try {
      const error = Schema.decodeUnknownSync(Schema.fromJsonString(WorkstreamError))(body, {
        onExcessProperty: "error",
      });
      if (error.code === "cursor_stale") throw new BoundedTransportFailure("cursor_stale");
    } catch (cause) {
      if (cause instanceof BoundedTransportFailure) throw cause;
    }
    throw new BoundedTransportFailure("http_error");
  }
  return body;
}

async function performRequest(input: {
  readonly fetch: WorkstreamFetch;
  readonly config: ControlPlaneWorkstreamActivation;
  readonly method: "GET" | "POST";
  readonly target: string;
  readonly body: string;
  readonly sentAt: string;
  readonly idempotencyKey?: string;
  readonly signal: AbortSignal;
}): Promise<string> {
  const placement =
    input.target === T3_PLACEMENT_ROUTE || input.target.startsWith(`${T3_PLACEMENT_ROUTE}?`);
  if (
    (!input.target.startsWith("/workstreams/v1/") && !placement) ||
    (placement &&
      (input.method !== "POST" ||
        input.target !== T3_PLACEMENT_ROUTE ||
        Buffer.byteLength(input.body) > T3_PLACEMENT_MAX_REQUEST_BYTES)) ||
    input.target.includes("#") ||
    /[\r\n]/.test(input.target)
  )
    throw new BoundedTransportFailure("invalid_target");
  const requestId = NodeCrypto.randomUUID();
  const nonce = NodeCrypto.randomBytes(24).toString("base64url");
  const contentSha256 =
    input.body === ""
      ? EMPTY_SHA256
      : NodeCrypto.createHash("sha256").update(input.body).digest("hex");
  const signature = signWorkstreamRequest({
    contractVersion: placement ? T3_PLACEMENT_CONTRACT : WORKSTREAM_CONTRACT_HEADER_VERSION,
    requestId,
    ...(input.idempotencyKey === undefined ? {} : { idempotencyKey: input.idempotencyKey }),
    sentAt: input.sentAt,
    nonce,
    principalId: input.config.principalId,
    keyId: input.config.keyId,
    method: input.method,
    target: input.target,
    contentSha256,
    signingSecret: input.config.signingSecret,
  });
  const response = await input.fetch(new URL(input.target, input.config.baseUrl), {
    method: input.method,
    redirect: "error",
    signal: input.signal,
    headers: {
      accept: "application/json",
      "content-type": "application/json; charset=utf-8",
      "x-control-algorithm": "hmac-sha256-v1",
      "x-control-contract-version": placement
        ? T3_PLACEMENT_CONTRACT
        : `workstreams/${WORKSTREAM_CONTRACT_VERSION}`,
      "x-control-contract-manifest": placement
        ? T3_PLACEMENT_MANIFEST_SHA256
        : WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      "x-control-request-id": requestId,
      "x-control-timestamp": input.sentAt,
      "x-control-nonce": nonce,
      "x-control-principal-id": input.config.principalId,
      "x-control-key-id": input.config.keyId,
      "x-control-content-sha256": contentSha256,
      "x-control-signature": signature,
      ...(input.idempotencyKey ? { "idempotency-key": input.idempotencyKey } : {}),
    },
    ...(input.body === "" ? {} : { body: input.body }),
  });
  if (
    placement &&
    (response.headers.get("x-control-contract-version") !== T3_PLACEMENT_CONTRACT ||
      response.headers.get("x-control-contract-manifest") !== T3_PLACEMENT_MANIFEST_SHA256)
  ) {
    await response.body?.cancel();
    throw new BoundedTransportFailure("http_error");
  }
  return readBoundedResponse(response);
}

export function makeControlPlaneWorkstreamTransport(
  activation: WorkstreamActivation = activationFromEnvironment(),
  fetchPort: WorkstreamFetch = globalThis.fetch.bind(globalThis),
): {
  readonly transport: WorkstreamTransport;
  readonly binding: Pick<
    T3WorkstreamBinding,
    "registryId" | "ownerId" | "principalId" | "authorizationRevision"
  >;
} {
  if (activation.state === "disabled" || !hasCanonicalActivation(activation.value)) {
    return {
      transport: disabled(),
      binding: {
        registryId: "disabled",
        ownerId: "disabled",
        principalId: "disabled",
        authorizationRevision: 0,
      },
    };
  }
  const config = activation.value;
  const request = <S extends Schema.Top>(
    operation: string,
    method: "GET" | "POST",
    target: string,
    schema: S,
    body = "",
    idempotencyKey?: string,
  ): Effect.Effect<S["Type"], WorkstreamTransportError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const sentAt = DateTime.formatIso(yield* DateTime.now);
      const responseBody = yield* Effect.tryPromise({
        try: (signal) =>
          performRequest({
            fetch: fetchPort,
            config,
            method,
            target,
            body,
            sentAt,
            ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
            signal,
          }),
        catch: (cause) =>
          new WorkstreamTransportError({
            operation,
            effect:
              method === "POST" && operation !== "thread_placements"
                ? "unknown-effect"
                : "no-effect",
            detail:
              cause instanceof BoundedTransportFailure ? cause.reason : "transport_unavailable",
          }),
      }).pipe(
        Effect.timeoutOrElse({
          duration: TIMEOUT_MS,
          orElse: () =>
            Effect.fail(
              new WorkstreamTransportError({
                operation,
                effect:
                  method === "POST" && operation !== "thread_placements"
                    ? "unknown-effect"
                    : "no-effect",
                detail: "Control-plane request timed out.",
              }),
            ),
        }),
      );
      return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(responseBody, {
        onExcessProperty: "error",
      }).pipe(
        Effect.mapError(
          (cause) =>
            new WorkstreamTransportError({
              operation,
              effect:
                method === "POST" && operation !== "thread_placements"
                  ? "unknown-effect"
                  : "no-effect",
              detail: "Control-plane returned invalid JSON for the accepted contract.",
            }),
        ),
      );
    });
  return {
    binding: {
      registryId: config.baseUrl.origin,
      ownerId: config.ownerId,
      principalId: config.principalId,
      authorizationRevision: config.authorizationRevision,
    },
    transport: {
      listThreadPlacements: (input) =>
        Schema.encodeEffect(Schema.fromJsonString(T3PlacementRequest))(input, {
          onExcessProperty: "error",
        }).pipe(
          Effect.mapError(
            () =>
              new WorkstreamTransportError({
                operation: "thread_placements",
                effect: "no-effect",
                detail: "Invalid placement inventory.",
              }),
          ),
          Effect.flatMap((body) =>
            request(
              "thread_placements",
              "POST",
              T3_PLACEMENT_ROUTE,
              T3PlacementPage,
              body,
              NodeCrypto.randomUUID(),
            ),
          ),
        ),
      getCapabilities: () =>
        request("capabilities", "GET", "/workstreams/v1/capabilities", WorkstreamCapabilities),
      listWorkstreams: (input) =>
        request(
          "list_workstreams",
          "GET",
          `/workstreams/v1/workstreams?${query(input)}`,
          WorkstreamPage,
        ),
      getWorkstream: ({ workstreamId }) =>
        request(
          "get_workstream",
          "GET",
          `/workstreams/v1/workstreams/${encodeURIComponent(workstreamId)}`,
          WorkstreamDetail,
        ),
      listReferences: (input) =>
        request(
          "list_references",
          "GET",
          `/workstreams/v1/references?${query(input)}`,
          WorkstreamReferencePage,
        ),
      getReference: ({ nativeReferenceId }) =>
        request(
          "get_reference",
          "GET",
          `/workstreams/v1/references/${encodeURIComponent(nativeReferenceId)}`,
          WorkstreamReferenceDetail,
        ),
      listMemberships: ({ workstreamId, ...input }) =>
        request(
          "list_memberships",
          "GET",
          `/workstreams/v1/workstreams/${encodeURIComponent(workstreamId)}/memberships?${query(input)}`,
          WorkstreamMembershipPage,
        ),
      listDeclarations: ({ workstreamId, ...input }) =>
        request(
          "list_declarations",
          "GET",
          `/workstreams/v1/workstreams/${encodeURIComponent(workstreamId)}/declarations?${query(input)}`,
          WorkstreamDeclarationPage,
        ),
      listEdges: ({ workstreamId, ...input }) =>
        request(
          "list_edges",
          "GET",
          `/workstreams/v1/workstreams/${encodeURIComponent(workstreamId)}/edges?${query(input)}`,
          WorkstreamEdgePage,
        ),
      listHistory: ({ workstreamId, ...input }) =>
        request(
          "list_history",
          "GET",
          `/workstreams/v1/${workstreamId === undefined ? "history" : `workstreams/${encodeURIComponent(workstreamId)}/history`}?${query(input)}`,
          WorkstreamHistoryPage,
        ),
      getCommand: ({ commandId }) =>
        request(
          "get_command",
          "GET",
          `/workstreams/v1/commands/${encodeURIComponent(commandId)}`,
          WorkstreamReceipt,
        ),
      submitCommand: ({ body, idempotencyKey }) =>
        request(
          "submit_command",
          "POST",
          "/workstreams/v1/commands",
          WorkstreamReceipt,
          body,
          idempotencyKey,
        ),
    },
  };
}
