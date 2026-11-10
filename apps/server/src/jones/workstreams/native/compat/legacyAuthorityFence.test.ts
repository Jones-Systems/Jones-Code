import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  type WorkstreamCommand,
} from "@t3tools/contracts";
import { makeControlPlaneWorkstreamTransport } from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import { make } from "../../../../workstreams/WorkstreamGateway.ts";
import { makeSyntheticWorkstreamTransport } from "../../../../workstreams/SyntheticWorkstreamTransport.ts";

const contract = {
  contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
  contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} as const;
const activation = {
  state: "enabled",
  value: {
    baseUrl: new URL("https://control-plane.example/"),
    ownerId: "owner-fixture",
    principalId: "11111111-1111-4111-8111-111111111111",
    authorizationRevision: 2,
    keyId: "key-fixture",
    signingSecret: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
  },
} as const;
const registryBinding = Buffer.from(
  JSON.stringify({
    protocol: "workstreams-registry-binding/1.0.0",
    manifest_sha256: "15ce45061966ce9c247192d6ef43724b6638bb0e7ccf7bbe9eae4afdd576f30c",
    registry_instance_id: "22222222-2222-4222-8222-222222222222",
    canonical_origin: activation.value.baseUrl.origin,
    owner_id: activation.value.ownerId,
    principal_id: activation.value.principalId,
    authorization_revision: activation.value.authorizationRevision,
  }),
).toString("base64url");
// Exact CP fence status/header; unsigned bytes intentionally cannot prove dispatch or absence.
const fenced = () =>
  new Response("not an authenticated receipt", {
    status: 503,
    headers: { "x-workstreams-authority": "fenced", "cache-control": "private, no-store" },
  });
const command: WorkstreamCommand = {
  command_id: "command-legacy-fence",
  expected_server_generation: 7,
  expected_registry_version: 11,
  action: {
    operation: "create_workstream",
    name: "Must remain unknown",
    lifecycle: "active",
    progress: { state: "progressing" },
    sort_order: 1,
  },
};
const submission = {
  ...contract,
  command,
  body: JSON.stringify(command),
  idempotencyKey: command.command_id,
};

it.effect.each([
  "capabilities",
  "metadata",
  "counts",
  "appearance",
  "placements",
  "registration",
] as const)("legacy %s reads recognize the CP fence before contract/body decoding", (route) =>
  Effect.gen(function* () {
    const { transport, registrationContext } = makeControlPlaneWorkstreamTransport(
      activation,
      async () => fenced(),
    );
    const read =
      route === "capabilities"
        ? transport.getCapabilities(contract).pipe(Effect.asVoid)
        : route === "metadata"
          ? transport.listWorkstreams({ ...contract, limit: 1 }).pipe(Effect.asVoid)
          : route === "counts"
            ? transport.getRegistryCounts!().pipe(Effect.asVoid)
            : route === "appearance"
              ? transport.readAppearance!({ workstream_ids: [] }).pipe(Effect.asVoid)
              : route === "placements"
                ? transport.listThreadPlacements!({ identities: [], limit: 1 }).pipe(Effect.asVoid)
                : registrationContext.readRegistrationContext.pipe(Effect.asVoid);
    expect(yield* read.pipe(Effect.flip)).toMatchObject({
      detail: "authority_fenced",
      effect: "no-effect",
    });
  }),
);

it.effect.each([false, true])(
  "a bound=%s fence never supplies mutation or recovery authority",
  (bound) =>
    Effect.gen(function* () {
      const { transport } = makeControlPlaneWorkstreamTransport(
        { ...activation, value: { ...activation.value, ...(bound ? { registryBinding } : {}) } },
        async () => fenced(),
      );
      for (const effect of [
        transport.submitCommand(submission).pipe(Effect.asVoid),
        transport.resubmitCommand!(submission).pipe(Effect.asVoid),
        transport.readCommandReceipt!({ ...contract, commandId: command.command_id }).pipe(
          Effect.asVoid,
        ),
      ]) {
        const error = yield* effect.pipe(Effect.flip);
        expect(error).toMatchObject({
          _tag: "WorkstreamTransportError",
          detail: "authority_fenced",
          effect: "unknown-effect",
        });
        expect("cause" in error ? error.cause : undefined).toBeUndefined();
      }
      if (bound)
        expect(yield* transport.getCapabilities(contract).pipe(Effect.flip)).toMatchObject({
          detail: "authority_fenced",
          effect: "unknown-effect",
        });
    }),
);

it.effect.each([
  { status: 503, header: null },
  { status: 503, header: "legacy" },
  { status: 502, header: "fenced" },
])("does not infer a fence from an ordinary response %j", (scenario) =>
  Effect.gen(function* () {
    const { transport } = makeControlPlaneWorkstreamTransport(
      activation,
      async () =>
        new Response("unavailable", {
          status: scenario.status,
          headers: scenario.header === null ? {} : { "x-workstreams-authority": scenario.header },
        }),
    );
    expect(yield* transport.getCapabilities(contract).pipe(Effect.flip)).toMatchObject({
      detail: "http_error",
      effect: "no-effect",
    });
  }),
);

it.effect(
  "a fence between capability admission and metadata response cannot release or retain cached success",
  () =>
    Effect.gen(function* () {
      const fixture = makeSyntheticWorkstreamTransport({
        principalId: activation.value.principalId,
        authorizationRevision: 2,
      });
      const capabilities = yield* fixture.getCapabilities(contract);
      const page = yield* fixture.listWorkstreams({ ...contract, limit: 1 });
      let mode: "live" | "fenced" | "offline" = "live";
      const calls: string[] = [];
      const configured = makeControlPlaneWorkstreamTransport(activation, async (url) => {
        const pathname = new URL(String(url)).pathname;
        calls.push(pathname);
        if (pathname.endsWith("/capabilities")) return new Response(JSON.stringify(capabilities));
        if (mode === "fenced") return fenced();
        if (mode === "offline") throw new Error("Synthetic network loss after observed fence");
        return new Response(JSON.stringify(page));
      });
      const gateway = yield* make(configured.transport, { binding: configured.binding });
      const live = yield* gateway.readMetadata({ limit: 1 });
      expect(live.source).toBe("live");
      expect(live.items).toHaveLength(1);
      mode = "fenced";
      expect(yield* gateway.readMetadata({ limit: 1 }).pipe(Effect.flip)).toMatchObject({
        reason: "authorization-not-current",
        detail: "authority_fenced",
      });
      mode = "offline";
      expect(yield* gateway.readMetadata({ limit: 1 }).pipe(Effect.flip)).toMatchObject({
        reason: "offline",
      });
      expect(calls).toHaveLength(6);
    }),
);

it.effect(
  "an unsigned fence after write admission stays unknown and lookup never reports absence",
  () =>
    Effect.gen(function* () {
      const fixture = makeSyntheticWorkstreamTransport({
        principalId: activation.value.principalId,
        authorizationRevision: 2,
      });
      const capabilities = yield* fixture.getCapabilities(contract);
      const calls: string[] = [];
      const configured = makeControlPlaneWorkstreamTransport(activation, async (url) => {
        const pathname = new URL(String(url)).pathname;
        calls.push(pathname);
        return pathname.endsWith("/capabilities")
          ? new Response(JSON.stringify(capabilities))
          : fenced();
      });
      const gateway = yield* make(configured.transport, { binding: configured.binding });
      expect(yield* gateway.submit(command).pipe(Effect.flip)).toMatchObject({
        reason: "unknown-effect",
        detail: "authority_fenced",
      });
      expect(yield* gateway.readReceipt(command.command_id).pipe(Effect.flip)).toMatchObject({
        reason: "unknown-effect",
      });
      expect(calls.filter((x) => x.endsWith("/commands"))).toHaveLength(1);
      expect(calls.filter((x) => x.endsWith("/commands/" + command.command_id))).toHaveLength(1);
    }),
);
