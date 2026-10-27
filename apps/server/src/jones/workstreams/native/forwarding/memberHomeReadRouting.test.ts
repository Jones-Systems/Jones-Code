import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Routing from "./memberHomeReadRouting.ts";
import * as Client from "./peerHomeReadClient.ts";
import * as Protocol from "./homeReadProtocol.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { RegistryError } from "../registry/session.ts";

// Synthetic admitted local boundary; real routing and fixed-tool adapter are under test.
const home = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
  home_environment_id: "home:test",
  server_generation: 1,
};
const record: Admission.MemberRecordedCallerV1 = {
  schema: "jones-workstreams-member-recorded-caller/1",
  local: {
    schema: "jones-workstreams-member-local-caller/1",
    auth: { kind: "bearer", session_id: "local", subject: "owner", method: "bearer-access-token" },
    origin: { kind: "app" },
    ceiling: {
      permissions: ["workstreams:read"],
      scopes: ["orchestration:read"],
      capabilities: [],
      limits: { runtimeMode: "approval-required", interactionMode: "default" },
    },
    session: { issued_at: 0, expires_at: 10000 },
    environment_id: "member:test",
    request_namespace: null,
    invocation_issued_at: null,
    link_origin: null,
    link_session: null,
    captured_at: 0,
  },
  association: {
    enrollment_id: "enrollment",
    enrollment_session_id: "session",
    intent_sha256: "a".repeat(64),
    home,
    canonical_origin: "https://home.invalid",
    principal_id: "principal",
    singleton_binding_version: 1,
    pin_bound_revision: 2,
    source_binding_version: 1,
    source: {
      environment_id: "member:test",
      authority_namespace: "namespace",
      store_generation: 1,
    },
    link_generation: "opaque-original",
  },
};
it.effect.each(["fixed", "revoked", "reduced"] as const)(
  "fixes every home field and outbound generation; %s evidence",
  (change) => {
    let check = 0;
    const calls: Array<Parameters<Peer.PeerMcpClient["Service"]["call"]>[0]> = [];
    const limits = {
      runtimeMode: "approval-required" as const,
      interactionMode: "default" as const,
    };
    const ports = Layer.mergeAll(
      Layer.mock(Admission.MemberAdmission)({
        current: () =>
          Effect.succeed({
            record,
            limits,
            checkCurrent: () =>
              Effect.suspend(() => {
                check++;
                if (check >= 2 && change === "revoked")
                  return Effect.fail(new RegistryError({ code: "forbidden" }));
                return Effect.succeed(
                  check >= 2 && change === "reduced"
                    ? { ...limits, interactionMode: "plan" as const }
                    : limits,
                );
              }),
          }),
      }),
      Layer.succeed(Peer.PeerMcpClient, {
        call: (input) => {
          calls.push(input);
          // oxlint-disable-next-line t3code/no-inline-schema-compile -- The peer supplies its result decoder per invocation.
          return Schema.decodeUnknownEffect(input.success)({ sentinel: "synthetic" }).pipe(
            Effect.orDie,
          );
        },
      }),
    );
    const mounted = Client.layer.pipe(
      Layer.provide(Routing.layer.pipe(Layer.provide(ports))),
      Layer.provide(ports),
    );
    return Effect.gen(function* () {
      const client = yield* Protocol.HomeReadResultClient;
      const output = yield* client
        .read(home, {
          contract: "workstreams-native-home-read-result/1.0.0",
          request: { route: "counts", params: {} },
        })
        .pipe(Effect.result);
      if (change !== "fixed") {
        expect(output._tag).toBe("Failure");
        expect(calls).toHaveLength(0);
      } else {
        expect(output._tag).toBe("Success");
        expect(calls).toHaveLength(1);
        expect(calls[0]).toMatchObject({
          environmentId: "home:test",
          expectedGeneration: "opaque-original",
          tool: "jones_workstreams_home_read_result",
          limits,
        });
        expect(calls[0]).not.toHaveProperty("origin");
      }
    }).pipe(Effect.provide(mounted));
  },
);
it.effect("wrong expected destination never dispatches", () => {
  let calls = 0;
  const ports = Layer.mergeAll(
    Layer.mock(Admission.MemberAdmission)({
      current: () =>
        Effect.succeed({
          record,
          limits: record.local.ceiling.limits!,
          checkCurrent: () => Effect.succeed(record.local.ceiling.limits!),
        }),
    }),
    Layer.succeed(Peer.PeerMcpClient, {
      call: () => {
        calls++;
        return Effect.die("unexpected transport");
      },
    }),
  );
  return Effect.gen(function* () {
    const client = yield* Protocol.HomeReadResultClient;
    for (const bad of [
      { ...home, owner_id: "other" },
      { ...home, home_environment_id: "other" },
      { ...home, registry_instance_id: "22222222-2222-4222-8222-222222222222" },
      { ...home, server_generation: 2 },
    ]) {
      expect(
        yield* client
          .read(bad, {
            contract: "workstreams-native-home-read-result/1.0.0",
            request: { route: "counts", params: {} },
          })
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
    }
    expect(calls).toBe(0);
  }).pipe(
    Effect.provide(
      Client.layer.pipe(
        Layer.provide(Routing.layer.pipe(Layer.provide(ports))),
        Layer.provide(ports),
      ),
    ),
  );
});
