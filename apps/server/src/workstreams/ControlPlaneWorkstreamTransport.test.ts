// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synthetic response proofs exercise exact wire bytes without host credentials.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  WORKSTREAMS_REGISTRATION_CONTEXT_CONTRACTS,
  WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
  WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
  WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256,
  T3_PLACEMENT_ROUTE,
  T3_PLACEMENT_CONTRACT,
  T3_PLACEMENT_MANIFEST_SHA256,
  WORKSTREAM_APPEARANCE_CONTRACT,
  WORKSTREAM_APPEARANCE_MANIFEST,
  WORKSTREAM_CONTRACT_FAMILY,
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_COUNTS_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  WORKSTREAM_CONTRACT_VERSION,
  WorkstreamCapabilities,
  type WorkstreamCommand,
} from "@t3tools/contracts";
import {
  makeControlPlaneWorkstreamTransport,
  signWorkstreamRequest,
  type WorkstreamFetch,
} from "./ControlPlaneWorkstreamTransport.ts";

import {
  response as registrationResponse,
  v2Response,
  encodeFixtureJson,
} from "../jones/workstreams/registrationContext/testFixtures.ts";

import {
  makeV3RegistrationFixture,
  v3Response,
} from "../jones/workstreams/testing/registrationV3.ts";

const decodeCapabilities = Schema.decodeUnknownSync(WorkstreamCapabilities);

const registrationHeaders = {
  "x-control-contract-version": WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
  "x-control-contract-manifest": WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256,
};

const activation = {
  state: "enabled",
  value: {
    baseUrl: new URL("https://control-plane.example/"),
    ownerId: "owner-fixture",
    principalId: "principal-fixture",
    authorizationRevision: 2,
    keyId: "key-fixture",
    signingSecret: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
  },
} as const;

const recoveryContract = {
  contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} as const;
const recoveryHeaders = {
  "x-control-contract-version": WORKSTREAM_CONTRACT_HEADER_VERSION,
  "x-control-contract-manifest": WORKSTREAM_CONTRACT_MANIFEST_SHA256,
};
const recoveryCommand: WorkstreamCommand = {
  command_id: "recovery-command-0001",
  expected_server_generation: 7,
  expected_registry_version: 11,
  action: {
    operation: "create_workstream",
    name: "Recovery fixture",
    lifecycle: "active",
    progress: { state: "progressing" },
    sort_order: 10,
  },
};
const recoveryInput = {
  ...recoveryContract,
  command: recoveryCommand,
  body: `${JSON.stringify(recoveryCommand, null, 2)}\n`,
  idempotencyKey: recoveryCommand.command_id,
};
const recoveryError = (code: "not_found" | "idempotency_conflict") => ({
  code,
  status: code === "not_found" ? 404 : 409,
  request_id: "recovery-request-0001",
  recovery: code === "not_found" ? "stop" : "reconcile",
});

it.effect("recognizes verified receipt absence only on the recovery read path", () =>
  Effect.gen(function* () {
    const configured = makeControlPlaneWorkstreamTransport(activation, async (url, init) => {
      expect(String(url)).toBe(
        `https://control-plane.example/workstreams/v1/commands/${recoveryCommand.command_id}`,
      );
      expect(init?.method).toBe("GET");
      return new Response(JSON.stringify(recoveryError("not_found")), {
        status: 404,
        headers: recoveryHeaders,
      });
    });
    const input = { ...recoveryContract, commandId: recoveryCommand.command_id };
    expect(yield* configured.transport.readCommandReceipt!(input)).toEqual({ kind: "absent" });
    expect(yield* configured.transport.getCommand(input).pipe(Effect.flip)).toMatchObject({
      detail: "http_error",
    });
  }),
);

it.effect.each([
  "no-headers",
  "wrong-manifest",
  "wrong-version",
  "bad-json",
  "extra-field",
  "wrong-recovery",
  "wrong-status",
  "server-error",
  "network",
  "invalid-success",
] as const)("keeps recovery receipt %s unknown", (scenario) =>
  Effect.gen(function* () {
    const configured = makeControlPlaneWorkstreamTransport(activation, async () => {
      if (scenario === "network") throw new Error("Synthetic connection loss");
      const error = recoveryError("not_found");
      return new Response(
        scenario === "bad-json"
          ? "not JSON"
          : JSON.stringify({
              ...error,
              ...(scenario === "extra-field" ? { unexpected: true } : {}),
              ...(scenario === "wrong-recovery" ? { recovery: "reconcile" } : {}),
            }),
        {
          status:
            scenario === "server-error"
              ? 503
              : scenario === "wrong-status"
                ? 409
                : scenario === "invalid-success"
                  ? 200
                  : 404,
          headers:
            scenario === "no-headers"
              ? {}
              : {
                  ...recoveryHeaders,
                  ...(scenario === "wrong-manifest"
                    ? { "x-control-contract-manifest": "0".repeat(64) }
                    : {}),
                  ...(scenario === "wrong-version"
                    ? { "x-control-contract-version": "workstreams/0.0.0" }
                    : {}),
                },
        },
      );
    });
    expect(
      yield* configured.transport.readCommandReceipt!({
        ...recoveryContract,
        commandId: recoveryCommand.command_id,
      }).pipe(Effect.flip),
    ).toMatchObject({ effect: "unknown-effect" });
  }),
);

it.effect("requires the configured response proof before accepting recovery evidence", () =>
  Effect.gen(function* () {
    const principalId = "11111111-1111-4111-8111-111111111111";
    const registryBinding = Buffer.from(
      JSON.stringify({
        protocol: "workstreams-registry-binding/1.0.0",
        manifest_sha256: "15ce45061966ce9c247192d6ef43724b6638bb0e7ccf7bbe9eae4afdd576f30c",
        registry_instance_id: "22222222-2222-4222-8222-222222222222",
        canonical_origin: "https://control-plane.example",
        owner_id: activation.value.ownerId,
        principal_id: principalId,
        authorization_revision: activation.value.authorizationRevision,
      }),
    ).toString("base64url");
    for (const code of ["not_found", "idempotency_conflict"] as const) {
      for (const proof of ["valid", "missing", "tampered", "refused"] as const) {
        const configured = makeControlPlaneWorkstreamTransport(
          {
            ...activation,
            value: { ...activation.value, principalId, registryBinding },
          },
          async (_url, init) => {
            const error = recoveryError(code);
            const body =
              proof === "refused"
                ? '{"code":"registry_binding_not_current"}'
                : JSON.stringify(error);
            const status = proof === "refused" ? 409 : error.status;
            const dispatch = proof === "refused" ? "not-dispatched" : "dispatched";
            const canonical = [
              "workstreams-registry-response/1.0.0",
              new Headers(init?.headers).get("x-control-signature"),
              String(status),
              WORKSTREAM_CONTRACT_HEADER_VERSION,
              WORKSTREAM_CONTRACT_MANIFEST_SHA256,
              registryBinding,
              dispatch,
              NodeCrypto.createHash("sha256").update(body).digest("hex"),
            ].join("\n");
            return new Response(body + (proof === "tampered" ? " " : ""), {
              status,
              headers: {
                ...recoveryHeaders,
                ...(proof === "missing"
                  ? {}
                  : {
                      "x-control-registry-binding": registryBinding,
                      "x-control-registry-dispatch": dispatch,
                      "x-control-registry-response-mac": NodeCrypto.createHmac(
                        "sha256",
                        Buffer.from(activation.value.signingSecret, "base64"),
                      )
                        .update(canonical)
                        .digest("base64url"),
                    }),
              },
            });
          },
        );
        const recovery =
          code === "not_found"
            ? configured.transport.readCommandReceipt!({
                ...recoveryContract,
                commandId: recoveryCommand.command_id,
              }).pipe(Effect.map((result): string => result.kind))
            : configured.transport.resubmitCommand!(recoveryInput).pipe(
                Effect.map((result): string => result.kind),
              );
        if (proof === "valid") {
          expect(yield* recovery).toBe(code === "not_found" ? "absent" : "idempotency_conflict");
        } else {
          expect(yield* recovery.pipe(Effect.flip)).toMatchObject({ effect: "unknown-effect" });
        }
      }
    }
  }),
);

it.effect(
  "recognizes recovery conflicts while signing and sending exactly the retained bytes",
  () =>
    Effect.gen(function* () {
      const bodies: NonNullable<RequestInit["body"]>[] = [];
      const configured = makeControlPlaneWorkstreamTransport(activation, async (url, init) => {
        expect(String(url)).toBe("https://control-plane.example/workstreams/v1/commands");
        expect(init?.method).toBe("POST");
        const headers = new Headers(init?.headers);
        expect(init?.body).toBe(recoveryInput.body);
        bodies.push(init!.body!);
        expect(headers.get("idempotency-key")).toBe(recoveryCommand.command_id);
        expect(headers.get("x-control-content-sha256")).toBe(
          NodeCrypto.createHash("sha256").update(recoveryInput.body).digest("hex"),
        );
        expect(headers.get("x-control-signature")).toBe(
          signWorkstreamRequest({
            requestId: headers.get("x-control-request-id")!,
            idempotencyKey: recoveryCommand.command_id,
            sentAt: headers.get("x-control-timestamp")!,
            nonce: headers.get("x-control-nonce")!,
            principalId: activation.value.principalId,
            keyId: activation.value.keyId,
            method: "POST",
            target: "/workstreams/v1/commands",
            contentSha256: headers.get("x-control-content-sha256")!,
            signingSecret: activation.value.signingSecret,
          }),
        );
        return new Response(JSON.stringify(recoveryError("idempotency_conflict")), {
          status: 409,
          headers: recoveryHeaders,
        });
      });
      expect(yield* configured.transport.resubmitCommand!(recoveryInput)).toEqual({
        kind: "idempotency_conflict",
      });
      expect(yield* configured.transport.resubmitCommand!(recoveryInput)).toEqual({
        kind: "idempotency_conflict",
      });
      expect(
        yield* configured.transport.submitCommand(recoveryInput).pipe(Effect.flip),
      ).toMatchObject({
        detail: "http_error",
        effect: "unknown-effect",
      });
      expect(bodies).toEqual([recoveryInput.body, recoveryInput.body, recoveryInput.body]);
    }),
);

it.effect.each([
  ["idempotency_conflict", 409, "reconcile", false],
  ["idempotency_conflict", 503, "reconcile", true],
  ["generation_conflict", 409, "reconcile", true],
  ["version_conflict", 409, "reload", true],
  ["forbidden", 403, "stop", true],
  ["not_found", 404, "stop", true],
  ["unavailable", 503, "reconcile", true],
] as const)(
  "keeps recovery submit %s/%s unknown without qualifying conflict evidence",
  ([code, status, recovery, headers]) =>
    Effect.gen(function* () {
      const configured = makeControlPlaneWorkstreamTransport(
        activation,
        async () =>
          new Response(
            JSON.stringify({ code, status, recovery, request_id: "recovery-request-0001" }),
            { status, headers: headers ? recoveryHeaders : {} },
          ),
      );
      expect(
        yield* configured.transport.resubmitCommand!(recoveryInput).pipe(Effect.flip),
      ).toMatchObject({ effect: "unknown-effect" });
    }),
);

it.effect("negotiates independent appearance without interpreting errors as automatic colors", () =>
  Effect.gen(function* () {
    const old = makeControlPlaneWorkstreamTransport(
      activation,
      async () => new Response("Not found", { status: 404 }),
    );
    expect(
      yield* old.transport.readAppearance!({ workstream_ids: ["workstream-fixture"] }),
    ).toEqual({ supported: false });
    const unavailable = makeControlPlaneWorkstreamTransport(
      activation,
      async () => new Response("Unavailable", { status: 503 }),
    );
    const failure = yield* unavailable.transport.readAppearance!({
      workstream_ids: ["workstream-fixture"],
    }).pipe(Effect.flip);
    expect(failure.effect).toBe("no-effect");
    const configured = makeControlPlaneWorkstreamTransport(activation, async (input, init) => {
      expect(String(input)).toBe("https://control-plane.example/workstream-appearance/v1/read");
      expect(new Headers(init?.headers).get("x-control-contract-version")).toBe(
        WORKSTREAM_APPEARANCE_CONTRACT,
      );
      return new Response(
        JSON.stringify({
          owner_id: "owner-fixture",
          server_generation: 7,
          permissions: ["workstreams:read"],
          items: [{ workstream_id: "workstream-fixture", border_color: "#123ABC", version: 1 }],
        }),
        {
          headers: {
            "x-control-contract-version": WORKSTREAM_APPEARANCE_CONTRACT,
            "x-control-contract-manifest": WORKSTREAM_APPEARANCE_MANIFEST,
          },
        },
      );
    });
    const result = yield* configured.transport.readAppearance!({
      workstream_ids: ["workstream-fixture"],
    });
    expect(result.supported && result.page.items[0]?.border_color).toBe("#123ABC");
  }),
);

it("matches the control-plane canonicalSignedRequest fixed 32-byte key vector", () => {
  expect(
    signWorkstreamRequest({
      requestId: "request-0001",
      idempotencyKey: "command-0001",
      sentAt: "2026-09-12T12:34:56.000Z",
      nonce: "nonce-0001",
      principalId: "principal-0001",
      keyId: "key-0001",
      method: "POST",
      target: "/workstreams/v1/commands",
      contentSha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      signingSecret: activation.value.signingSecret,
    }),
  ).toBe("qToaB5Zn6FJjf3DME-ZsRseYIw_rW-nF-2tiHjCc9V8");
});

it.effect("disables noncanonical or non-32-byte signing secrets without a request", () =>
  Effect.gen(function* () {
    let requests = 0;
    const secret = activation.value.signingSecret;
    for (const signingSecret of [
      "",
      "arbitrary-utf8-secret",
      secret.slice(0, -1),
      `${secret}\n`,
      Buffer.alloc(31).toString("base64"),
      Buffer.alloc(33).toString("base64"),
      `${secret.slice(0, -2)}9=`,
      "_".repeat(43) + "=",
    ]) {
      const configured = makeControlPlaneWorkstreamTransport(
        { ...activation, value: { ...activation.value, signingSecret } },
        async () => {
          requests += 1;
          return new Response("{}");
        },
      );
      expect(configured.binding.authorizationRevision).toBe(0);
      expect(
        (yield* Effect.result(
          configured.transport.getCapabilities({
            contractVersion: "workstreams/1.0.0",
            contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
          }),
        ))._tag,
      ).toBe("Failure");
    }
    expect(requests).toBe(0);
  }),
);

it.effect("sends bounded signed HTTPS requests and runtime-decodes responses", () =>
  Effect.gen(function* () {
    const observed: Array<{ readonly url: string; readonly headers: Headers }> = [];
    const fetchPort: WorkstreamFetch = async (input, init) => {
      observed.push({ url: String(input), headers: new Headers(init?.headers) });
      return new Response(
        JSON.stringify({
          contract_family: WORKSTREAM_CONTRACT_FAMILY,
          contract_version: WORKSTREAM_CONTRACT_VERSION,
          manifest_sha256: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
          context: { owner_id: "owner-fixture", server_generation: 7, registry_version: 11 },
          permissions: ["workstreams:read"],
          max_request_bytes: 32_768,
          max_response_bytes: 1_048_576,
          max_json_depth: 10,
          max_page_items: 100,
          max_pr_response_bytes: 262_144,
          max_pr_request_seconds: 15,
          cursor_ttl_seconds: 900,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    };
    const configured = makeControlPlaneWorkstreamTransport(activation, fetchPort);
    const capabilities = yield* configured.transport.getCapabilities({
      contractVersion: "workstreams/1.0.0",
      contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
    });
    expect(capabilities.context.owner_id).toBe("owner-fixture");
    const captured = observed[0];
    expect(captured).toBeDefined();
    expect(captured?.url).toBe("https://control-plane.example/workstreams/v1/capabilities");
    expect(captured?.headers.get("x-control-signature")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(captured?.headers.get("x-control-content-sha256")).toMatch(/^[0-9a-f]{64}$/);
  }),
);

it.effect("rejects malformed and over-bound control-plane responses", () =>
  Effect.gen(function* () {
    const malformedFetch: WorkstreamFetch = async () =>
      new Response(JSON.stringify({ owner_id: "wrong-shape" }));
    const malformedConfigured = makeControlPlaneWorkstreamTransport(activation, malformedFetch);
    const malformed = yield* malformedConfigured.transport
      .getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);
    expect(malformed.effect).toBe("no-effect");

    const overBoundFetch: WorkstreamFetch = async () =>
      new Response("x".repeat(1_048_577), {
        headers: { "content-length": "1048577" },
      });
    const overBoundConfigured = makeControlPlaneWorkstreamTransport(activation, overBoundFetch);
    const overBound = yield* overBoundConfigured.transport
      .getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);
    expect(overBound.detail).toBe("response_too_large");
  }),
);

it.effect("preserves a validated cursor-stale response for bounded page restart", () =>
  Effect.gen(function* () {
    const configured = makeControlPlaneWorkstreamTransport(
      activation,
      async () =>
        new Response(
          JSON.stringify({
            code: "cursor_stale",
            status: 409,
            request_id: "request-fixture",
            recovery: "restart_page",
          }),
          { status: 409, headers: { "content-type": "application/json" } },
        ),
    );
    const failure = yield* configured.transport
      .listWorkstreams({
        limit: 50,
        cursor: "stale-cursor",
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);
    expect(failure.detail).toBe("cursor_stale");
  }),
);

it.effect("disables non-loopback HTTP and header-unsafe activation before transport", () =>
  Effect.gen(function* () {
    let requests = 0;
    const fetchPort: WorkstreamFetch = async () => {
      requests += 1;
      return new Response("{}");
    };
    const unsafeActivation = {
      ...activation,
      value: {
        ...activation.value,
        baseUrl: new URL("http://control-plane.example/"),
        principalId: "principal-fixture\r\ninjected: value",
      },
    } as const;
    const configured = makeControlPlaneWorkstreamTransport(unsafeActivation, fetchPort);
    const unavailable = yield* configured.transport
      .getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);

    expect(unavailable.effect).toBe("no-effect");
    expect(configured.binding.authorizationRevision).toBe(0);
    expect(requests).toBe(0);
  }),
);

const capabilitiesResponse = () =>
  new Response(
    JSON.stringify({
      contract_family: WORKSTREAM_CONTRACT_FAMILY,
      contract_version: WORKSTREAM_CONTRACT_VERSION,
      manifest_sha256: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      context: { owner_id: "owner-fixture", server_generation: 7, registry_version: 11 },
      permissions: ["workstreams:read"],
      max_request_bytes: 32_768,
      max_response_bytes: 1_048_576,
      max_json_depth: 10,
      max_page_items: 100,
      max_pr_response_bytes: 262_144,
      max_pr_request_seconds: 15,
      cursor_ttl_seconds: 900,
    }),
    { status: 200, headers: { "content-type": "application/json" } },
  );

it.effect("refuses unbound loopback and non-loopback HTTP before any wire request", () =>
  Effect.gen(function* () {
    let requests = 0;
    for (const raw of [
      "http://127.0.0.1:8787/",
      "http://[::1]:8787/",
      "http://localhost:8787/",
      "http://127.0.0.2:8787/",
      "http://127.0.0.1:8787/workstreams/",
      "http://127.0.0.1:8787/?route=workstreams",
      "http://[::1]:8787/#fragment",
      "http://user:pass@127.0.0.1:8787/",
    ]) {
      const configured = makeControlPlaneWorkstreamTransport(
        { ...activation, value: { ...activation.value, baseUrl: new URL(raw) } },
        async () => {
          requests += 1;
          return capabilitiesResponse();
        },
      );
      expect(configured.activationState).toBe("invalid");
      const unavailable = yield* configured.transport
        .getCapabilities({
          contractVersion: "workstreams/1.0.0",
          contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        })
        .pipe(Effect.flip);
      expect(unavailable.effect).toBe("no-effect");
    }
    expect(requests).toBe(0);
  }),
);

it("refuses unbound loopback environment activation before transport", () => {
  const environment = {
    T3_WORKSTREAM_CONTROL_PLANE_URL: "http://127.0.0.1:8787/",
    T3_WORKSTREAM_OWNER_ID: activation.value.ownerId,
    T3_WORKSTREAM_PRINCIPAL_ID: activation.value.principalId,
    T3_WORKSTREAM_KEY_ID: activation.value.keyId,
    T3_WORKSTREAM_SIGNING_SECRET: activation.value.signingSecret,
    T3_WORKSTREAM_AUTHORIZATION_REVISION: String(activation.value.authorizationRevision),
  };
  const noFetch: WorkstreamFetch = async () => {
    throw new TypeError("activation must not fetch");
  };
  const previous = Object.fromEntries(
    Object.keys(environment).map((name) => [name, process.env[name]] as const),
  );
  const configure = (url: string) => {
    Object.assign(process.env, environment, { T3_WORKSTREAM_CONTROL_PLANE_URL: url });
    return makeControlPlaneWorkstreamTransport(undefined, noFetch).binding;
  };
  try {
    expect(configure("http://127.0.0.1:8787/").authorizationRevision).toBe(0);
    expect(configure("http://[::1]:8787/").authorizationRevision).toBe(0);
    expect(configure("http://localhost:8787/").authorizationRevision).toBe(0);
  } finally {
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
});

it.effect("cancels a chunked response as soon as it crosses the byte bound", () =>
  Effect.gen(function* () {
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(700_000));
        controller.enqueue(new Uint8Array(400_000));
      },
      cancel() {
        cancelled = true;
      },
    });
    const configured = makeControlPlaneWorkstreamTransport(
      activation,
      async () => new Response(stream, { headers: { "content-type": "application/json" } }),
    );
    const failure = yield* configured.transport
      .getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);

    expect(failure.detail).toBe("response_too_large");
    expect(cancelled).toBe(true);
  }),
);

it.effect("does not expose transport or decoder causes", () =>
  Effect.gen(function* () {
    const privateMarker = "PRIVATE-MARKER-MUST-NOT-ESCAPE";
    const network = makeControlPlaneWorkstreamTransport(activation, async () => {
      throw new Error(privateMarker);
    });
    const networkFailure = yield* network.transport
      .getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);
    expect(networkFailure.detail).toBe("transport_unavailable");
    expect(networkFailure.cause).toBeUndefined();

    const decoder = makeControlPlaneWorkstreamTransport(
      activation,
      async () => new Response(`{"private":"${privateMarker}"}`),
    );
    const decoderFailure = yield* decoder.transport
      .getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      })
      .pipe(Effect.flip);
    expect(decoderFailure.detail).toBe(
      "Control-plane returned invalid JSON for the accepted contract.",
    );
    expect(decoderFailure.cause).toBeUndefined();
  }),
);

it.effect(
  "signs a fixed registration GET with its separate family and keeps Workstreams and placement families",
  () =>
    Effect.gen(function* () {
      const requests: Array<{ url: string; headers: Headers; init: RequestInit | undefined }> = [];
      const configured = makeControlPlaneWorkstreamTransport(activation, async (input, init) => {
        const url = String(input);
        requests.push({ url, headers: new Headers(init?.headers), init });
        if (url.endsWith("/t3/registration-context")) {
          return new Response(encodeFixtureJson(registrationResponse), {
            headers: registrationHeaders,
          });
        }
        if (url.endsWith(T3_PLACEMENT_ROUTE)) {
          return new Response(
            JSON.stringify({
              inventory_sha256: "a".repeat(64),
              context: {
                owner_id: activation.value.ownerId,
                principal_id: activation.value.principalId,
                authorization_revision: activation.value.authorizationRevision,
                server_generation: 1,
                registry_version: 1,
              },
              items: [],
              next_cursor: null,
            }),
            {
              headers: {
                "x-control-contract-version": T3_PLACEMENT_CONTRACT,
                "x-control-contract-manifest": T3_PLACEMENT_MANIFEST_SHA256,
              },
            },
          );
        }
        return new Response(
          JSON.stringify({
            contract_family: WORKSTREAM_CONTRACT_FAMILY,
            contract_version: WORKSTREAM_CONTRACT_VERSION,
            manifest_sha256: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
            context: {
              owner_id: activation.value.ownerId,
              server_generation: 1,
              registry_version: 1,
            },
            permissions: ["workstreams:read"],
            max_request_bytes: 32_768,
            max_response_bytes: 1_048_576,
            max_json_depth: 10,
            max_page_items: 100,
            max_pr_response_bytes: 262_144,
            max_pr_request_seconds: 15,
            cursor_ttl_seconds: 900,
          }),
        );
      });
      expect(yield* configured.registrationContext.readRegistrationContext).toBe(
        encodeFixtureJson(registrationResponse),
      );
      expect(configured.registrationContext.configuredBinding).toEqual({
        ownerId: activation.value.ownerId,
        principalId: activation.value.principalId,
        authorizationRevision: activation.value.authorizationRevision,
      });
      const registered = requests[0]!;
      expect(registered.url).toBe(
        "https://control-plane.example/workstreams/v1/t3/registration-context",
      );
      expect(registered.init?.method).toBe("GET");
      expect(registered.init?.body).toBeUndefined();
      expect(registered.init?.redirect).toBe("error");
      expect(registered.headers.get("idempotency-key")).toBeNull();
      expect(registered.headers.get("x-control-contract-version")).toBe(
        WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
      );
      expect(registered.headers.get("x-control-contract-manifest")).toBe(
        WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256,
      );
      expect(registered.headers.get("x-control-signature")).toBe(
        signWorkstreamRequest({
          contractVersion: WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL,
          requestId: registered.headers.get("x-control-request-id")!,
          sentAt: registered.headers.get("x-control-timestamp")!,
          nonce: registered.headers.get("x-control-nonce")!,
          principalId: activation.value.principalId,
          keyId: activation.value.keyId,
          method: "GET",
          target: "/workstreams/v1/t3/registration-context",
          contentSha256: registered.headers.get("x-control-content-sha256")!,
          signingSecret: activation.value.signingSecret,
        }),
      );
      yield* configured.transport.getCapabilities({
        contractVersion: "workstreams/1.0.0",
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      });
      yield* configured.transport.listThreadPlacements!({ identities: [], limit: 50 });
      expect(requests[1]?.headers.get("x-control-contract-version")).toBe("workstreams/1.0.0");
      expect(requests[1]?.headers.get("x-control-contract-manifest")).toBe(
        WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      );
      expect(requests[2]?.headers.get("x-control-contract-version")).toBe(T3_PLACEMENT_CONTRACT);
      expect(requests[2]?.headers.get("x-control-contract-manifest")).toBe(
        T3_PLACEMENT_MANIFEST_SHA256,
      );
    }),
);

it.effect("bounds registration response bytes before parsing and rejects another family", () =>
  Effect.gen(function* () {
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(8_192));
        controller.enqueue(new Uint8Array(1));
      },
      cancel() {
        cancelled = true;
      },
    });
    const bounded = makeControlPlaneWorkstreamTransport(
      activation,
      async () => new Response(stream, { headers: registrationHeaders }),
    );
    const tooLarge = yield* bounded.registrationContext.readRegistrationContext.pipe(Effect.flip);
    expect(tooLarge.detail).toBe("response_too_large");
    expect(tooLarge.effect).toBe("no-effect");
    expect(cancelled).toBe(true);
    const wrongFamily = makeControlPlaneWorkstreamTransport(
      activation,
      async () =>
        new Response(encodeFixtureJson(registrationResponse), {
          headers: {
            "x-control-contract-version": "workstreams/1.0.0",
            "x-control-contract-manifest": WORKSTREAM_CONTRACT_MANIFEST_SHA256,
          },
        }),
    );
    expect(
      (yield* wrongFamily.registrationContext.readRegistrationContext.pipe(Effect.flip)).detail,
    ).toBe("http_error");
    const disabled = makeControlPlaneWorkstreamTransport({ state: "disabled" }, async () => {
      throw new TypeError("disabled transport must not fetch");
    });
    expect(disabled.registrationContext.configuredBinding).toBeNull();
    expect(
      (yield* disabled.registrationContext.readRegistrationContext.pipe(Effect.flip)).effect,
    ).toBe("no-effect");
  }),
);

it.effect(
  "distinguishes invalid and absent startup activation without outbound requests or private diagnostics",
  () =>
    Effect.gen(function* () {
      let requests = 0;
      for (const state of ["invalid", "disabled"] as const) {
        const configured = makeControlPlaneWorkstreamTransport({ state }, async () => {
          requests += 1;
          return new Response("{}");
        });
        expect(configured.activationState).toBe(state);
        expect(configured.registrationContext.activationState).toBe(state);
        const failure = yield* configured.transport
          .getCapabilities({
            contractVersion: "workstreams/1.0.0",
            contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
          })
          .pipe(Effect.flip);
        expect(failure.detail).toBe(`Workstream control-plane activation is ${state}.`);
        expect(failure.cause).toBeUndefined();
        expect(
          (yield* configured.registrationContext.readRegistrationContext.pipe(Effect.flip)).detail,
        ).toBe(failure.detail);
      }
      expect(requests).toBe(0);
    }),
);

it.effect(
  "explicit v2 signs its exact empty GET and rejects another version or manifest without fallback",
  () =>
    Effect.gen(function* () {
      const headers = {
        "x-control-contract-version": WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
        "x-control-contract-manifest": WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
      };
      let calls = 0;
      const configured = makeControlPlaneWorkstreamTransport(
        { ...activation, value: { ...activation.value, registrationContextVersion: "2.0.0" } },
        async (url, init) => {
          calls += 1;
          const sent = new Headers(init?.headers);
          expect(String(url)).toBe(
            "https://control-plane.example/workstreams/v1/t3/registration-context",
          );
          expect(init?.method).toBe("GET");
          expect(init?.body).toBeUndefined();
          expect(sent.get("idempotency-key")).toBeNull();
          expect(sent.get("x-control-contract-version")).toBe(
            headers["x-control-contract-version"],
          );
          expect(sent.get("x-control-contract-manifest")).toBe(
            headers["x-control-contract-manifest"],
          );
          expect(sent.get("x-control-signature")).toBe(
            signWorkstreamRequest({
              contractVersion: WORKSTREAMS_REGISTRATION_CONTEXT_V2_PROTOCOL,
              requestId: sent.get("x-control-request-id")!,
              sentAt: sent.get("x-control-timestamp")!,
              nonce: sent.get("x-control-nonce")!,
              principalId: activation.value.principalId,
              keyId: activation.value.keyId,
              method: "GET",
              target: "/workstreams/v1/t3/registration-context",
              contentSha256: sent.get("x-control-content-sha256")!,
              signingSecret: activation.value.signingSecret,
            }),
          );
          return new Response(encodeFixtureJson(v2Response), { headers });
        },
      );
      expect(configured.registrationContext.registrationContextVersion).toBe("2.0.0");
      expect(yield* configured.registrationContext.readRegistrationContext).toBe(
        encodeFixtureJson(v2Response),
      );
      expect(calls).toBe(1);
      for (const wrongHeaders of [
        registrationHeaders,
        {
          ...headers,
          "x-control-contract-manifest": WORKSTREAMS_REGISTRATION_CONTEXT_MANIFEST_SHA256,
        },
        { ...headers, "x-control-contract-version": WORKSTREAMS_REGISTRATION_CONTEXT_PROTOCOL },
      ]) {
        let attempts = 0;
        const wrong = makeControlPlaneWorkstreamTransport(
          { ...activation, value: { ...activation.value, registrationContextVersion: "2.0.0" } },
          async () => {
            attempts += 1;
            return new Response(encodeFixtureJson(v2Response), { headers: wrongHeaders });
          },
        );
        expect(
          (yield* wrong.registrationContext.readRegistrationContext.pipe(Effect.flip)).detail,
        ).toBe("http_error");
        expect(attempts).toBe(1);
      }
      const legacy = makeControlPlaneWorkstreamTransport(
        activation,
        async () => new Response(encodeFixtureJson(v2Response), { headers }),
      );
      expect(
        (yield* legacy.registrationContext.readRegistrationContext.pipe(Effect.flip)).detail,
      ).toBe("http_error");
    }),
);

it.effect("invalid activation performs no registration or ordinary registry fetch", () =>
  Effect.gen(function* () {
    let calls = 0;
    const configured = makeControlPlaneWorkstreamTransport({ state: "invalid" }, async () => {
      calls += 1;
      throw new Error("Invalid activation must not fetch");
    });
    expect(
      (yield* configured.registrationContext.readRegistrationContext.pipe(Effect.flip)).effect,
    ).toBe("no-effect");
    expect(
      (yield* configured.transport
        .getCapabilities({
          contractVersion: "workstreams/1.0.0",
          contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
        })
        .pipe(Effect.flip)).effect,
    ).toBe("no-effect");
    expect(calls).toBe(0);
  }),
);

it.effect("isolates the counts manifest from legacy capabilities, metadata and commands", () =>
  Effect.gen(function* () {
    const requests: Array<{ path: string; manifest: string | null }> = [];
    const configured = makeControlPlaneWorkstreamTransport(activation, async (url, init) => {
      const path = new URL(String(url)).pathname;
      const manifest = new Headers(init?.headers).get("x-control-contract-manifest");
      requests.push({ path, manifest });
      const caps = decodeCapabilities(await capabilitiesResponse().json());
      const payload =
        path === "/workstreams/v1/counts"
          ? {
              context: caps.context,
              principal_id: activation.value.principalId,
              observed_at: "2026-10-18T00:00:00Z",
              authority_effect: "none",
              total: 9,
              active: 3,
              unknown_lifecycle: 1,
            }
          : path === "/workstreams/v1/capabilities"
            ? {
                ...caps,
                manifest_sha256: manifest,
                ...(manifest === WORKSTREAM_COUNTS_MANIFEST_SHA256
                  ? {
                      registry_counts: {
                        version: 1,
                        path: "/workstreams/v1/counts",
                        active_definition: "latest_owner_lifecycle_declaration_active",
                      },
                    }
                  : {}),
              }
            : { context: caps.context, items: [], next_cursor: null };
      return new Response(JSON.stringify(payload), {
        headers: {
          "x-control-contract-version": "workstreams/1.0.0",
          "x-control-contract-manifest": manifest!,
        },
      });
    });
    const legacy = {
      contractVersion: "workstreams/1.0.0",
      contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
    } as const;
    yield* configured.transport.getCountsCapabilities!();
    expect((yield* configured.transport.getRegistryCounts!()).total).toBe(9);
    yield* configured.transport.getCapabilities(legacy);
    yield* configured.transport.listWorkstreams({ ...legacy, limit: 1 });
    // The synthetic command response is intentionally invalid; its request must still use legacy headers.
    yield* configured.transport
      .submitCommand({
        ...legacy,
        body: "{}",
        command: {
          command_id: "synthetic-command-0001",
          expected_server_generation: 7,
          expected_registry_version: 11,
          action: {
            operation: "update_workstream",
            workstream_id: "ws-core-v1",
            expected_version: 3,
            name: "Synthetic",
            lifecycle: "active",
            progress: { state: "progressing" },
            sort_order: 10,
          },
        },
        idempotencyKey: "synthetic-command-0001",
      })
      .pipe(Effect.flip);
    expect(requests).toEqual([
      { path: "/workstreams/v1/capabilities", manifest: WORKSTREAM_COUNTS_MANIFEST_SHA256 },
      { path: "/workstreams/v1/counts", manifest: WORKSTREAM_COUNTS_MANIFEST_SHA256 },
      { path: "/workstreams/v1/capabilities", manifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256 },
      { path: "/workstreams/v1/workstreams", manifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256 },
      { path: "/workstreams/v1/commands", manifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256 },
    ]);
  }),
);

it.effect.each([
  "absent-headers",
  "legacy-manifest",
  "wrong-version",
  "invalid-counts",
  "unavailable",
] as const)("rejects counts response %s", (response) =>
  Effect.gen(function* () {
    const configured = makeControlPlaneWorkstreamTransport(
      activation,
      async () =>
        new Response(
          JSON.stringify({
            context: { owner_id: "owner-fixture", server_generation: 7, registry_version: 11 },
            principal_id: "principal-fixture",
            observed_at: "2026-10-18T00:00:00Z",
            authority_effect: "none",
            total: response === "invalid-counts" ? 3 : 9,
            active: 3,
            unknown_lifecycle: 1,
          }),
          {
            status: response === "unavailable" ? 404 : 200,
            headers:
              response === "absent-headers"
                ? {}
                : {
                    "x-control-contract-version":
                      response === "wrong-version" ? "workstreams/2.0.0" : "workstreams/1.0.0",
                    "x-control-contract-manifest":
                      response === "legacy-manifest"
                        ? WORKSTREAM_CONTRACT_MANIFEST_SHA256
                        : WORKSTREAM_COUNTS_MANIFEST_SHA256,
                  },
          },
        ),
    );
    expect(yield* configured.transport.getRegistryCounts!().pipe(Effect.flip)).toMatchObject({
      _tag: "WorkstreamTransportError",
      operation: "registry_counts",
      effect: "no-effect",
    });
  }),
);

it.effect(
  "v3 signs the selected version and manifest and never falls back after registry rejection",
  () =>
    Effect.gen(function* () {
      const contract = WORKSTREAMS_REGISTRATION_CONTEXT_CONTRACTS["3.0.0"];
      const headers = {
        "x-control-contract-version": contract.protocol,
        "x-control-contract-manifest": contract.manifest,
      };
      let calls = 0;
      const configured = makeControlPlaneWorkstreamTransport(
        { ...activation, value: { ...activation.value, registrationContextVersion: "3.0.0" } },
        async (url, init) => {
          calls += 1;
          const sent = new Headers(init?.headers);
          expect(String(url)).toBe(
            "https://control-plane.example/workstreams/v1/t3/registration-context",
          );
          expect(init?.method).toBe("GET");
          expect(init?.body).toBeUndefined();
          expect(sent.get("idempotency-key")).toBeNull();
          expect(sent.get("x-control-contract-version")).toBe(contract.protocol);
          expect(sent.get("x-control-contract-manifest")).toBe(contract.manifest);
          expect(sent.get("x-control-signature")).toBe(
            signWorkstreamRequest({
              contractVersion: contract.protocol,
              contractManifest: contract.manifest,
              requestId: sent.get("x-control-request-id")!,
              sentAt: sent.get("x-control-timestamp")!,
              nonce: sent.get("x-control-nonce")!,
              principalId: activation.value.principalId,
              keyId: activation.value.keyId,
              method: "GET",
              target: "/workstreams/v1/t3/registration-context",
              contentSha256: sent.get("x-control-content-sha256")!,
              signingSecret: activation.value.signingSecret,
            }),
          );
          return new Response(encodeFixtureJson(v3Response), { headers });
        },
      );
      expect(yield* configured.registrationContext.readRegistrationContext).toBe(
        encodeFixtureJson(v3Response),
      );
      expect(calls).toBe(1);
      for (const rejection of [
        { status: 400, headers },
        { status: 404, headers: {} },
        { status: 200, headers: registrationHeaders },
        {
          status: 200,
          headers: {
            ...headers,
            "x-control-contract-manifest": WORKSTREAMS_REGISTRATION_CONTEXT_V2_MANIFEST_SHA256,
          },
        },
      ]) {
        let attempts = 0;
        const rejected = makeControlPlaneWorkstreamTransport(
          { ...activation, value: { ...activation.value, registrationContextVersion: "3.0.0" } },
          async (_url, init) => {
            attempts += 1;
            expect(new Headers(init?.headers).get("x-control-contract-version")).toBe(
              contract.protocol,
            );
            return new Response("{}", rejection);
          },
        );
        const fixture = makeV3RegistrationFixture({
          readRegistrationContext: rejected.registrationContext.readRegistrationContext,
        });
        expect((yield* fixture.service.read().pipe(Effect.flip)).reason).toBe(
          "registry_unavailable",
        );
        expect(fixture.reads.authority).toBe(0);
        expect(attempts).toBe(1);
      }
    }),
);

it.effect(
  "v3 preserves canonical identity, bound signatures and authenticated bounded response proofs",
  () =>
    Effect.gen(function* () {
      const principalId = "11111111-1111-4111-8111-111111111111";
      const contract = WORKSTREAMS_REGISTRATION_CONTEXT_CONTRACTS["3.0.0"];
      const registryBinding = Buffer.from(
        JSON.stringify({
          protocol: "workstreams-registry-binding/1.0.0",
          manifest_sha256: "15ce45061966ce9c247192d6ef43724b6638bb0e7ccf7bbe9eae4afdd576f30c",
          registry_instance_id: "22222222-2222-4222-8222-222222222222",
          canonical_origin: "https://control-plane.example",
          owner_id: activation.value.ownerId,
          principal_id: principalId,
          authorization_revision: activation.value.authorizationRevision,
        }),
      ).toString("base64url");
      const secret = Buffer.from(activation.value.signingSecret, "base64");
      for (const scenario of ["valid", "missing-proof", "wrong-body", "oversize"] as const) {
        let calls = 0;
        const configured = makeControlPlaneWorkstreamTransport(
          {
            ...activation,
            value: {
              ...activation.value,
              principalId,
              baseUrl: new URL("http://127.0.0.1:8787/"),
              registrationContextVersion: "3.0.0",
              registryBinding,
            },
          },
          async (url, init) => {
            calls += 1;
            expect(String(url)).toBe(
              "http://127.0.0.1:8787/workstreams/v1/t3/registration-context",
            );
            const sent = new Headers(init?.headers);
            expect(sent.get("x-control-registry-binding")).toBe(registryBinding);
            const signature = signWorkstreamRequest({
              contractVersion: contract.protocol,
              contractManifest: contract.manifest,
              registryBinding,
              requestId: sent.get("x-control-request-id")!,
              sentAt: sent.get("x-control-timestamp")!,
              nonce: sent.get("x-control-nonce")!,
              principalId,
              keyId: activation.value.keyId,
              method: "GET",
              target: "/workstreams/v1/t3/registration-context",
              contentSha256: sent.get("x-control-content-sha256")!,
              signingSecret: activation.value.signingSecret,
            });
            expect(sent.get("x-control-signature")).toBe(signature);
            const body = encodeFixtureJson(v3Response);
            const canonical = [
              "workstreams-registry-response/1.0.0",
              signature,
              "200",
              contract.protocol,
              contract.manifest,
              registryBinding,
              "dispatched",
              NodeCrypto.createHash("sha256").update(body).digest("hex"),
            ].join("\n");
            const headers = {
              "x-control-contract-version": contract.protocol,
              "x-control-contract-manifest": contract.manifest,
              ...(scenario === "missing-proof"
                ? {}
                : {
                    "x-control-registry-binding": registryBinding,
                    "x-control-registry-dispatch": "dispatched",
                    "x-control-registry-response-mac": NodeCrypto.createHmac("sha256", secret)
                      .update(canonical)
                      .digest("base64url"),
                  }),
            };
            return new Response(
              scenario === "oversize"
                ? "x".repeat(8_193)
                : body + (scenario === "wrong-body" ? " " : ""),
              { headers },
            );
          },
        );
        expect(configured.binding.registryId).toBe("https://control-plane.example");
        if (scenario === "valid") {
          expect(yield* configured.registrationContext.readRegistrationContext).toBe(
            encodeFixtureJson(v3Response),
          );
        } else {
          const fixture = makeV3RegistrationFixture({
            readRegistrationContext: configured.registrationContext.readRegistrationContext,
          });
          expect((yield* fixture.service.read().pipe(Effect.flip)).reason).toBe(
            "registry_unavailable",
          );
        }
        expect(calls).toBe(1);
      }
    }),
);
