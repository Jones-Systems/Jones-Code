import {
  activationFromEnvironment,
  decodeSigningSecret,
  hasCanonicalActivation,
  type ControlPlaneWorkstreamActivation,
  type WorkstreamActivation,
} from "../jones/workstreams/activation.ts";
export type { WorkstreamActivation } from "../jones/workstreams/activation.ts";

// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous contract hashing and opaque request identities retain their existing wire encoding.
import * as NodeCrypto from "node:crypto";

import {
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_COUNTS_MANIFEST_SHA256,
  WorkstreamCountsCapabilities,
  WorkstreamRegistryCounts,
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
  WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
  type WorkstreamsRegistrationContextVersion,
  WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256,
  WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES,
  WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import {
  WORKSTREAM_ACTIVATION_DISABLED_DETAIL,
  WorkstreamTransportError,
  type WorkstreamTransport,
} from "./WorkstreamGateway.ts";

import {
  WORKSTREAM_APPEARANCE_CONTRACT,
  WORKSTREAM_APPEARANCE_MANIFEST,
  WORKSTREAM_APPEARANCE_ROUTE,
  WorkstreamAppearancePage,
  WorkstreamAppearance,
} from "@t3tools/contracts";

const EMPTY_SHA256 = NodeCrypto.createHash("sha256").update("").digest("hex");
const TIMEOUT_MS = 15_000;

const REGISTRATION_CONTEXT_ROUTE = "/workstreams/v1/t3/registration-context";

export interface WorkstreamRegistrationTransport {
  readonly registrationContextVersion: WorkstreamsRegistrationContextVersion | null;
  readonly activationState?: WorkstreamActivation["state"];
  readonly configuredBinding: Pick<
    T3WorkstreamBinding,
    "ownerId" | "principalId" | "authorizationRevision"
  > | null;
  readonly readRegistrationContext: Effect.Effect<string, WorkstreamTransportError>;
}

class BoundedTransportFailure extends Error {
  readonly reason:
    | "invalid_target"
    | "response_too_large"
    | "http_error"
    | "cursor_stale"
    | "unsupported_appearance";

  constructor(
    reason:
      | "invalid_target"
      | "response_too_large"
      | "http_error"
      | "cursor_stale"
      | "unsupported_appearance",
  ) {
    super(reason);
    this.reason = reason;
  }
}

export type WorkstreamFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

const disabled = (detail: string): WorkstreamTransport => {
  const unavailable = (operation: string) =>
    Effect.fail(
      new WorkstreamTransportError({
        operation,
        effect: "no-effect",
        detail,
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
    | typeof T3_PLACEMENT_CONTRACT
    | typeof WORKSTREAM_APPEARANCE_CONTRACT
    | typeof WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL
    | typeof WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL;
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

async function readBoundedResponse(
  response: Response,
  maxBytes = WORKSTREAM_MAX_RESPONSE_BYTES,
): Promise<string> {
  const declared = response.headers.get("content-length");
  if (declared !== null && Number(declared) > maxBytes) {
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
      if (total > maxBytes) {
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
  readonly registrationContextVersion?: WorkstreamsRegistrationContextVersion | null;
  readonly countsContract?: boolean;
  readonly method: "GET" | "POST";
  readonly target: string;
  readonly body: string;
  readonly sentAt: string;
  readonly idempotencyKey?: string;
  readonly signal: AbortSignal;
}): Promise<string> {
  const counts = input.countsContract === true;
  if (
    counts &&
    (input.method !== "GET" ||
      input.body !== "" ||
      input.idempotencyKey !== undefined ||
      !["/workstreams/v1/capabilities", "/workstreams/v1/counts"].includes(input.target))
  )
    throw new BoundedTransportFailure("invalid_target");
  const registration = input.target.split(/[?#]/, 1)[0] === REGISTRATION_CONTEXT_ROUTE;
  const registrationVersion =
    input.registrationContextVersion === undefined ? "1.0.0" : input.registrationContextVersion;
  if (registration && registrationVersion !== "1.0.0" && registrationVersion !== "2.0.0")
    throw new BoundedTransportFailure("invalid_target");
  const registrationProtocol =
    registrationVersion === "2.0.0"
      ? WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL
      : WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL;
  const registrationManifest =
    registrationVersion === "2.0.0"
      ? WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256
      : WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256;
  const appearance =
    input.target === `${WORKSTREAM_APPEARANCE_ROUTE}/read` ||
    input.target === `${WORKSTREAM_APPEARANCE_ROUTE}/write`;
  const placement =
    input.target === T3_PLACEMENT_ROUTE || input.target.startsWith(`${T3_PLACEMENT_ROUTE}?`);
  if (
    (!input.target.startsWith("/workstreams/v1/") && !placement && !appearance) ||
    (placement &&
      (input.method !== "POST" ||
        input.target !== T3_PLACEMENT_ROUTE ||
        Buffer.byteLength(input.body) > T3_PLACEMENT_MAX_REQUEST_BYTES)) ||
    (registration &&
      (input.method !== "GET" ||
        input.target !== REGISTRATION_CONTEXT_ROUTE ||
        input.body !== "" ||
        input.idempotencyKey !== undefined)) ||
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
    contractVersion: registration
      ? registrationProtocol
      : appearance
        ? WORKSTREAM_APPEARANCE_CONTRACT
        : placement
          ? T3_PLACEMENT_CONTRACT
          : WORKSTREAM_CONTRACT_HEADER_VERSION,
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
      "x-control-contract-version": registration
        ? registrationProtocol
        : appearance
          ? WORKSTREAM_APPEARANCE_CONTRACT
          : placement
            ? T3_PLACEMENT_CONTRACT
            : `workstreams/${WORKSTREAM_CONTRACT_VERSION}`,
      "x-control-contract-manifest": registration
        ? registrationManifest
        : appearance
          ? WORKSTREAM_APPEARANCE_MANIFEST
          : placement
            ? T3_PLACEMENT_MANIFEST_SHA256
            : counts
              ? WORKSTREAM_COUNTS_MANIFEST_SHA256
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
    counts &&
    (response.headers.get("x-control-contract-version") !== WORKSTREAM_CONTRACT_HEADER_VERSION ||
      response.headers.get("x-control-contract-manifest") !== WORKSTREAM_COUNTS_MANIFEST_SHA256)
  ) {
    await response.body?.cancel();
    throw new BoundedTransportFailure("http_error");
  }
  if (appearance) {
    if (response.status === 404 && response.headers.get("x-control-contract-version") === null) {
      await response.body?.cancel();
      throw new BoundedTransportFailure("unsupported_appearance");
    }
    if (
      response.headers.get("x-control-contract-version") !== WORKSTREAM_APPEARANCE_CONTRACT ||
      response.headers.get("x-control-contract-manifest") !== WORKSTREAM_APPEARANCE_MANIFEST
    ) {
      await response.body?.cancel();
      throw new BoundedTransportFailure("http_error");
    }
  }
  if (
    placement &&
    (response.headers.get("x-control-contract-version") !== T3_PLACEMENT_CONTRACT ||
      response.headers.get("x-control-contract-manifest") !== T3_PLACEMENT_MANIFEST_SHA256)
  ) {
    await response.body?.cancel();
    throw new BoundedTransportFailure("http_error");
  }
  if (
    registration &&
    (response.headers.get("x-control-contract-version") !== registrationProtocol ||
      response.headers.get("x-control-contract-manifest") !== registrationManifest)
  ) {
    await response.body?.cancel();
    throw new BoundedTransportFailure("http_error");
  }
  return readBoundedResponse(
    response,
    registration
      ? WORKSTREAMS_REGISTRATION_CONTEXT_MAX_RESPONSE_BYTES
      : WORKSTREAM_MAX_RESPONSE_BYTES,
  );
}

export function makeControlPlaneWorkstreamTransport(
  activation: WorkstreamActivation = activationFromEnvironment(),
  fetchPort: WorkstreamFetch = globalThis.fetch.bind(globalThis),
): {
  readonly activationState: WorkstreamActivation["state"];
  readonly transport: WorkstreamTransport;
  readonly registrationContext: WorkstreamRegistrationTransport;
  readonly binding: Pick<
    T3WorkstreamBinding,
    "registryId" | "ownerId" | "principalId" | "authorizationRevision"
  >;
} {
  if (activation.state !== "enabled" || !hasCanonicalActivation(activation.value)) {
    const activationState = activation.state === "disabled" ? "disabled" : "invalid";
    const detail =
      activationState === "disabled"
        ? WORKSTREAM_ACTIVATION_DISABLED_DETAIL
        : "Workstream control-plane activation is invalid.";
    return {
      activationState,
      transport: disabled(detail),
      registrationContext: {
        registrationContextVersion: null,
        activationState,
        configuredBinding: null,
        readRegistrationContext: Effect.fail(
          new WorkstreamTransportError({
            operation: "registration_context",
            effect: "no-effect",
            detail,
          }),
        ),
      },
      binding: {
        registryId: "disabled",
        ownerId: "disabled",
        principalId: "disabled",
        authorizationRevision: 0,
      },
    };
  }
  const config = activation.value;
  const registrationContextVersion = config.registrationContextVersion ?? "1.0.0";
  const request = <S extends Schema.Top>(
    operation: string,
    method: "GET" | "POST",
    target: string,
    schema: S,
    body = "",
    idempotencyKey?: string,
    countsContract = false,
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
            countsContract,
            ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
            signal,
          }),
        catch: (cause) =>
          new WorkstreamTransportError({
            operation,
            effect:
              method === "POST" &&
              operation !== "thread_placements" &&
              operation !== "appearance_read"
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
                  method === "POST" &&
                  operation !== "thread_placements" &&
                  operation !== "appearance_read"
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
                method === "POST" &&
                operation !== "thread_placements" &&
                operation !== "appearance_read"
                  ? "unknown-effect"
                  : "no-effect",
              detail: "Control-plane returned invalid JSON for the accepted contract.",
            }),
        ),
      );
    });
  return {
    activationState: "enabled",
    registrationContext: {
      registrationContextVersion,
      activationState: "enabled",
      configuredBinding: {
        ownerId: config.ownerId,
        principalId: config.principalId,
        authorizationRevision: config.authorizationRevision,
      },
      readRegistrationContext: Effect.gen(function* () {
        const sentAt = DateTime.formatIso(yield* DateTime.now);
        return yield* Effect.tryPromise({
          try: (signal) =>
            performRequest({
              fetch: fetchPort,
              config,
              method: "GET",
              target: REGISTRATION_CONTEXT_ROUTE,
              registrationContextVersion,
              body: "",
              sentAt,
              signal,
            }),
          catch: (cause) =>
            new WorkstreamTransportError({
              operation: "registration_context",
              effect: "no-effect",
              detail:
                cause instanceof BoundedTransportFailure ? cause.reason : "transport_unavailable",
            }),
        }).pipe(
          Effect.timeoutOrElse({
            duration: WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS,
            orElse: () =>
              Effect.fail(
                new WorkstreamTransportError({
                  operation: "registration_context",
                  effect: "no-effect",
                  detail: "Control-plane request timed out.",
                }),
              ),
          }),
        );
      }),
    },
    binding: {
      registryId: config.baseUrl.origin,
      ownerId: config.ownerId,
      principalId: config.principalId,
      authorizationRevision: config.authorizationRevision,
    },
    transport: {
      readAppearance: (input) =>
        request(
          "appearance_read",
          "POST",
          `${WORKSTREAM_APPEARANCE_ROUTE}/read`,
          WorkstreamAppearancePage,
          JSON.stringify(input),
          NodeCrypto.randomUUID(),
        ).pipe(
          Effect.map((page) => ({ supported: true as const, page })),
          Effect.catch((error) =>
            error.detail === "unsupported_appearance"
              ? Effect.succeed({ supported: false as const })
              : Effect.fail(error),
          ),
        ),
      saveAppearance: (input) =>
        request(
          "appearance_write",
          "POST",
          `${WORKSTREAM_APPEARANCE_ROUTE}/write`,
          WorkstreamAppearance,
          JSON.stringify(input),
          input.command_id,
        ),
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
      getCountsCapabilities: () =>
        request(
          "counts_capabilities",
          "GET",
          "/workstreams/v1/capabilities",
          WorkstreamCountsCapabilities,
          "",
          undefined,
          true,
        ),
      getRegistryCounts: () =>
        request(
          "registry_counts",
          "GET",
          "/workstreams/v1/counts",
          WorkstreamRegistryCounts,
          "",
          undefined,
          true,
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
