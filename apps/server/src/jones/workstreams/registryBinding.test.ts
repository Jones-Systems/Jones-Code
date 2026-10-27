import * as Effect from "effect/Effect";
import { WORKSTREAM_CONTRACT_MANIFEST_SHA256 } from "@t3tools/contracts";
// @effect-diagnostics nodeBuiltinImport:off -- Pinned synthetic wire vectors use no host credentials.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import { it as effectIt } from "@effect/vitest";
import type { RegistryBinding } from "@t3tools/shared/jones/workstreamsRegistryBinding";
import { expect, it } from "vite-plus/test";
import {
  signWorkstreamRequest,
  makeControlPlaneWorkstreamTransport,
  type WorkstreamFetch,
} from "../../workstreams/ControlPlaneWorkstreamTransport.ts";
import { RegistryBindingRefusal, verifyRegistryResponse } from "./registryBinding.ts";

const vectors = JSON.parse(
  NodeFS.readFileSync(
    new URL(
      "../../../../../packages/shared/src/jones/workstreamsRegistryBinding/contract/vectors.json.raw",
      import.meta.url,
    ),
    "utf8",
  ),
) as {
  synthetic_secret_hex: string;
  binding: RegistryBinding;
  token: string;
  requests: {
    name: string;
    input: Omit<Parameters<typeof signWorkstreamRequest>[0], "signingSecret">;
    signature: string;
    canonical_utf8: string;
    legacy_signature: string;
  }[];
  responses: {
    name: string;
    body: string;
    mac: string;
    canonical_utf8: string;
    input: {
      status: number;
      token: string;
      dispatch: string;
      requestSignature: string;
      contractVersion: string;
      contractManifest: string;
    };
  }[];
};
const secret = Buffer.from(vectors.synthetic_secret_hex, "hex");
it("matches every producer request vector and preserves every legacy signature", () => {
  for (const vector of vectors.requests) {
    const input = { ...vector.input, signingSecret: secret.toString("base64") } as Parameters<
      typeof signWorkstreamRequest
    >[0];
    expect(signWorkstreamRequest(input), vector.name).toBe(vector.signature);
    expect(
      NodeCrypto.createHmac("sha256", secret).update(vector.canonical_utf8).digest("base64url"),
    ).toBe(vector.signature);
    const { registryBinding: _binding, ...legacy } = input;
    expect(signWorkstreamRequest(legacy), vector.name).toBe(vector.legacy_signature);
  }
});
const refusal = vectors.responses[0]!;
const proofInput = () => ({
  response: new Response(refusal.body, {
    status: refusal.input.status,
    headers: {
      "x-control-registry-binding": refusal.input.token,
      "x-control-registry-dispatch": refusal.input.dispatch,
      "x-control-registry-response-mac": refusal.mac,
    },
  }),
  body: Buffer.from(refusal.body),
  token: refusal.input.token,
  requestSignature: refusal.input.requestSignature,
  secret,
  contractVersion: "workstreams/1.0.0",
  contractManifest: vectors.requests[0]!.input.contractManifest!,
});
it("accepts only the exact authenticated refusal envelope", () => {
  expect(() => verifyRegistryResponse(proofInput())).toThrow(RegistryBindingRefusal);
  for (const vector of vectors.responses) {
    expect(
      NodeCrypto.createHmac("sha256", secret).update(vector.canonical_utf8).digest("base64url"),
      vector.name,
    ).toBe(vector.mac);
  }
});
it.each(vectors.responses.filter((vector) => vector.input.dispatch === "dispatched"))(
  "verifies frozen $name with optional command/receipt headers and rejects it for required families",
  (vector) => {
    const input = {
      ...proofInput(),
      body: Buffer.from(vector.body),
      token: vector.input.token,
      requestSignature: vector.input.requestSignature,
      responseContractHeaders: "optional" as const,
      response: new Response(vector.body, {
        status: vector.input.status,
        headers: {
          "x-control-registry-binding": vector.input.token,
          "x-control-registry-dispatch": vector.input.dispatch,
          "x-control-registry-response-mac": vector.mac,
          ...(vector.input.contractVersion
            ? { "x-control-contract-version": vector.input.contractVersion }
            : {}),
          ...(vector.input.contractManifest
            ? { "x-control-contract-manifest": vector.input.contractManifest }
            : {}),
        },
      }),
    };
    expect(() => verifyRegistryResponse(input)).not.toThrow();
    expect(() => verifyRegistryResponse({ ...input, responseContractHeaders: "required" })).toThrow(
      "registry_binding_invalid",
    );
    expect(() => verifyRegistryResponse({ ...input, requestSignature: "another-request" })).toThrow(
      "registry_binding_invalid",
    );
    expect(() =>
      verifyRegistryResponse({ ...input, body: Buffer.from(vector.body + " ") }),
    ).toThrow("registry_binding_invalid");
  },
);

it.each([
  [null, null, true, false],
  ["matching", null, true, false],
  [null, "matching", true, false],
  ["matching", "matching", true, true],
  ["wrong", "matching", false, false],
  ["matching", "wrong", false, false],
  ["", "", false, false],
  ["", null, false, false],
  [null, "", false, false],
  ["matching, matching", "matching", false, false],
] as const)(
  "enforces authenticated response header semantics (%s, %s)",
  (versionValue, manifestValue, optionalAccepted, requiredAccepted) => {
    const input = proofInput();
    input.body = Buffer.from('{"synthetic":"committed"}');
    const version = versionValue === "matching" ? input.contractVersion : versionValue;
    const manifest = manifestValue === "matching" ? input.contractManifest : manifestValue;
    const canonical = [
      "workstreams-registry-response/1.0.0",
      input.requestSignature,
      "200",
      version ?? "",
      manifest ?? "",
      input.token,
      "dispatched",
      NodeCrypto.createHash("sha256").update(input.body).digest("hex"),
    ].join("\n");
    input.response = new Response(input.body, {
      status: 200,
      headers: {
        "x-control-registry-binding": input.token,
        "x-control-registry-dispatch": "dispatched",
        "x-control-registry-response-mac": NodeCrypto.createHmac("sha256", secret)
          .update(canonical)
          .digest("base64url"),
        ...(version === null ? {} : { "x-control-contract-version": version }),
        ...(manifest === null ? {} : { "x-control-contract-manifest": manifest }),
      },
    });
    for (const [policy, accepted] of [
      ["optional", optionalAccepted],
      ["required", requiredAccepted],
    ] as const) {
      const verify = () => verifyRegistryResponse({ ...input, responseContractHeaders: policy });
      if (accepted) expect(verify).not.toThrow();
      else expect(verify).toThrow("registry_binding_invalid");
    }
    // Optional family semantics cannot turn missing proof into accepted data.
    input.response.headers.delete("x-control-registry-response-mac");
    expect(() => verifyRegistryResponse({ ...input, responseContractHeaders: "optional" })).toThrow(
      "registry_binding_invalid",
    );
  },
);

it.each([
  "body",
  "signature",
  "token",
  "status",
  "dispatch",
  "version",
  "manifest",
  "missing",
  "invalid",
  "padding",
])("refuses altered or replayed %s proof", (field) => {
  const input = proofInput();
  if (field === "body") input.body = Buffer.from(refusal.body + " ");
  else if (field === "signature") input.requestSignature = "new-request";
  else if (field === "token") input.token += "a";
  else if (field === "status")
    input.response = new Response(refusal.body, { status: 200, headers: input.response.headers });
  else if (field === "missing") input.response.headers.delete("x-control-registry-response-mac");
  else
    input.response.headers.set(
      field === "dispatch"
        ? "x-control-registry-dispatch"
        : field === "version"
          ? "x-control-contract-version"
          : field === "manifest"
            ? "x-control-contract-manifest"
            : "x-control-registry-response-mac",
      field === "padding" ? refusal.mac + "=" : field === "dispatch" ? "dispatched" : "invalid",
    );
  expect(() => verifyRegistryResponse(input)).toThrow("registry_binding_invalid");
});

it.each([
  [409, "not-dispatched", '{"code":"registry_binding_not_current","extra":true}', "", ""],
  [409, "not-dispatched", '{"code":"registry_binding_not_current"} ', "", ""],
  [409, "dispatched", refusal.body, "", ""],
  [400, "not-dispatched", refusal.body, "", ""],
  [409, "not-dispatched", refusal.body, "workstreams/2.0.0", "a".repeat(64)],
] as const)(
  "does not upgrade an authenticated nonqualifying envelope (%s %s) to a refusal",
  (status, dispatch, body, version, manifest) => {
    const input = proofInput();
    input.body = Buffer.from(body);
    const canonical = [
      "workstreams-registry-response/1.0.0",
      input.requestSignature,
      String(status),
      version,
      manifest,
      input.token,
      dispatch,
      NodeCrypto.createHash("sha256").update(input.body).digest("hex"),
    ].join("\n");
    input.response = new Response(body, {
      status,
      headers: {
        "x-control-registry-binding": input.token,
        "x-control-registry-dispatch": dispatch,
        "x-control-contract-version": version,
        "x-control-contract-manifest": manifest,
        "x-control-registry-response-mac": NodeCrypto.createHmac("sha256", secret)
          .update(canonical)
          .digest("base64url"),
      },
    });
    expect(() => verifyRegistryResponse(input)).toThrow("registry_binding_invalid");
  },
);

effectIt.effect.each([
  { baseUrl: "http://127.0.0.1:8787/", proof: "unsigned" },
  { baseUrl: "http://[::1]:8787/", proof: "unsigned" },
  { baseUrl: "https://transport.synthetic.test/", proof: "unsigned" },
  { baseUrl: "http://127.0.0.1:8787/", proof: "authenticated-missing-contract" },
] as const)(
  "signs all transport families at $baseUrl and fails closed for $proof responses without fallback",
  ({ baseUrl, proof }) =>
    Effect.gen(function* () {
      const observed: Headers[] = [];
      const activation = {
        state: "enabled" as const,
        value: {
          baseUrl: new URL(baseUrl),
          ownerId: vectors.binding.owner_id,
          principalId: vectors.binding.principal_id,
          authorizationRevision: vectors.binding.authorization_revision,
          keyId: "synthetic.key",
          signingSecret: secret.toString("base64"),
          registryBinding: vectors.token,
        },
      };
      const fetchPort: WorkstreamFetch = async (url, init) => {
        expect(new URL(String(url)).origin).toBe(new URL(baseUrl).origin);
        expect(init?.redirect).toBe("error");
        const headers = new Headers(init?.headers);
        observed.push(headers);
        expect(headers.get("x-control-algorithm")).toBe("hmac-sha256-workstreams-registry-v1");
        expect(headers.get("x-control-registry-binding")).toBe(vectors.token);
        const h = (name: string) => headers.get(name) ?? "";
        const target = new URL(String(url));
        const canonical = [
          h("x-control-algorithm"),
          h("x-control-contract-version"),
          h("x-control-request-id"),
          h("idempotency-key"),
          h("x-control-timestamp"),
          h("x-control-nonce"),
          h("x-control-principal-id"),
          h("x-control-key-id"),
          init!.method!,
          target.pathname + target.search,
          h("x-control-content-sha256"),
          h("x-control-contract-manifest"),
          vectors.token,
        ].join("\n");
        expect(h("x-control-signature")).toBe(
          NodeCrypto.createHmac("sha256", secret).update(canonical).digest("base64url"),
        );
        if (proof === "authenticated-missing-contract") {
          const body = "{}";
          const responseCanonical = [
            "workstreams-registry-response/1.0.0",
            h("x-control-signature"),
            "200",
            "",
            "",
            vectors.token,
            "dispatched",
            NodeCrypto.createHash("sha256").update(body).digest("hex"),
          ].join("\n");
          return new Response(body, {
            status: 200,
            headers: {
              "x-control-registry-binding": vectors.token,
              "x-control-registry-dispatch": "dispatched",
              "x-control-registry-response-mac": NodeCrypto.createHmac("sha256", secret)
                .update(responseCanonical)
                .digest("base64url"),
            },
          });
        }
        // An old producer's unsigned error must not trigger a retry with the legacy algorithm.
        return new Response('{"code":"unauthorized"}', { status: 401 });
      };
      const configured = makeControlPlaneWorkstreamTransport(activation, fetchPort);
      expect(configured.binding.registryId).toBe(vectors.binding.canonical_origin);
      const t = configured.transport;
      const contract = {
        contractVersion: "workstreams/1.0.0" as const,
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      };
      const requests: Effect.Effect<
        unknown,
        import("../../workstreams/WorkstreamGateway.ts").WorkstreamTransportError
      >[] = [
        t.getCapabilities(contract),
        t.getCountsCapabilities!(),
        t.getRegistryCounts!(),
        t.listThreadPlacements!({ identities: [], limit: 50 }),
        t.readAppearance!({ workstream_ids: [] }),
        t.saveAppearance!({
          command_id: "appearance-command-001",
          workstream_id: "ws-core-v1",
          expected_server_generation: 1,
          expected_version: 0,
          border_color: null,
        }),
        configured.registrationContext.readRegistrationContext,
        makeControlPlaneWorkstreamTransport(
          { ...activation, value: { ...activation.value, registrationContextVersion: "2.0.0" } },
          fetchPort,
        ).registrationContext.readRegistrationContext,
        t.listWorkstreams({ ...contract, limit: 50 }),
        t.listReferences({ ...contract, limit: 50 }),
        ...(proof === "unsigned"
          ? [t.getCommand({ ...contract, commandId: "receipt-command-001" })]
          : []),
      ];
      for (const request of requests) {
        const result = yield* Effect.result(request);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({
            effect: "unknown-effect",
            detail: "registry_binding_invalid",
          });
      }
      expect(observed).toHaveLength(requests.length);
    }),
);
