import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
// @effect-diagnostics nodeBuiltinImport:off -- Hash bounded synthetic bytes in memory; fixtures create no files.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Admission from "../runtime/memberAdmission.ts";
import * as ReadProtocol from "../forwarding/homeReadProtocol.ts";
import * as ExportProtocol from "../forwarding/homeExportProtocol.ts";
import { WORKSTREAMS_HOME_READ_EXPORT_CONTRACT } from "@t3tools/contracts/jones/workstreamsNativeExport";
import { RegistryError } from "../registry/session.ts";
import * as MemberExport from "./memberExport.ts";
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
const envelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: home.registry_instance_id,
    owner_id: home.owner_id,
    server_generation: home.server_generation,
    canonical_origin: "https://home.invalid",
    registry_version: 10,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "home:test",
    served_by_environment_id: "home:test",
    source: "live",
    observed_at: "2026-01-01T00:00:00Z",
    stale: false,
  },
  coverage: { registry: "complete", sources: [] },
};
const result = () => ({
  contract: "workstreams-native-home-read-result/1.0.0",
  route: "counts",
  envelope: structuredClone(envelope),
  authorization_revision: 3,
  value: {
    context: { owner_id: "owner:test", server_generation: 1, registry_version: 10 },
    principal_id: "principal",
    observed_at: envelope.provenance.observed_at,
    authority_effect: "none",
    total: 0,
    active: 0,
    unknown_lifecycle: 0,
  },
});

const page =
  "  " + JSON.stringify({ envelope, counts: result().value, offset: 0, items: [] }) + " \r\n";
const original =
  page +
  JSON.stringify({
    complete: true,
    registry_version: 10,
    sha256: NodeCrypto.createHash("sha256").update(page).digest("hex"),
  }) +
  "\n";
const reply = () => ({
  contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
  home: { ...home },
  principal_id: "principal",
  authorization_revision: 3,
  jsonl: original,
});
const fixture = (
  options: {
    alter?: (result: ReturnType<typeof reply>) => void;
    observe?: (value: ReturnType<typeof result>, ordinal: number) => void;
    check?: (ordinal: number) => Effect.Effect<void, RegistryError>;
    failure?: RegistryError;
    pending?: { ready: Deferred.Deferred<void>; closed: () => void };
  } = {},
) => {
  const calls: string[] = [];
  let checks = 0,
    observations = 0;
  const check = Effect.gen(function* () {
    yield* options.check?.(++checks) ?? Effect.void;
    return record.local.ceiling.limits!;
  });
  const ports = Layer.mergeAll(
    Layer.mock(Admission.MemberAdmission)({
      current: () =>
        Effect.succeed({ record, limits: record.local.ceiling.limits!, checkCurrent: () => check }),
    }),
    Layer.succeed(ReadProtocol.HomeReadResultClient, {
      read: (_home, request) =>
        Effect.gen(function* () {
          const retained = yield* Effect.serviceOption(Admission.RetainedMemberCaller);
          expect(Option.isSome(retained) ? retained.value : null).toBe(record);
          calls.push(request.request.route);
          const value = result();
          options.observe?.(value, ++observations);
          return value;
        }),
    }),
    Layer.succeed(ExportProtocol.HomeExportClient, {
      read: (target, request) =>
        Effect.gen(function* () {
          const retained = yield* Effect.serviceOption(Admission.RetainedMemberCaller);
          expect(Option.isSome(retained) ? retained.value : null).toBe(record);
          expect(target).toEqual(home);
          expect(request).toEqual({
            contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
            expected_home: home,
            page_size: 1,
          });
          calls.push("export");
          if (options.pending)
            return yield* Deferred.succeed(options.pending.ready, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Effect.sync(options.pending.closed)),
            );
          if (options.failure) return yield* options.failure;
          const r = reply();
          options.alter?.(r);
          return r;
        }),
    }),
  );
  return {
    calls,
    layer: MemberExport.layer.pipe(
      Layer.provide(ReadProtocol.homeReadPortLayer.pipe(Layer.provide(ports))),
      Layer.provide(ports),
    ),
  };
};
it.effect(
  "releases only complete original bytes after both current home observations and original-caller checks",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      expect(yield* (yield* MemberExport.MemberExport).read(1)).toBe(original);
      expect(f.calls).toEqual(["counts", "export", "counts"]);
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect.each([
  "principal",
  "revision",
  "home",
  "digest",
  "truncated",
  "trailing",
  "unsupported",
  "field",
] as const)("refuses %s export without releasing a completed value", (change) => {
  const f = fixture({
    alter: (r) => {
      if (change === "principal") r.principal_id = "other";
      if (change === "revision") r.authorization_revision++;
      if (change === "home") r.home.server_generation++;
      if (change === "digest")
        r.jsonl = r.jsonl.replace(/"sha256":"[a-f0-9]{64}"/, '"sha256":"' + "0".repeat(64) + '"');
      if (change === "truncated") r.jsonl = page;
      if (change === "trailing") r.jsonl += "\n";
      if (change === "unsupported") Object.defineProperty(r, "contract", { value: "unsupported" });
      if (change === "field") Object.assign(r, { caller: "forged" });
    },
  });
  return Effect.gen(function* () {
    expect((yield* (yield* MemberExport.MemberExport).read(1).pipe(Effect.result))._tag).toBe(
      "Failure",
    );
  }).pipe(Effect.provide(f.layer));
});
it.effect.each(["grant", "principal", "generation", "version", "stale", "coverage"] as const)(
  "refuses final %s drift after the remote download",
  (change) => {
    const f = fixture({
      observe: (r, n) => {
        if (n !== 2) return;
        if (change === "grant") r.authorization_revision++;
        if (change === "principal") r.value.principal_id = "other";
        if (change === "generation") {
          r.envelope.registry.server_generation++;
          r.value.context.server_generation++;
        }
        if (change === "version") {
          r.envelope.registry.registry_version++;
          r.value.context.registry_version++;
        }
        if (change === "stale") r.envelope.provenance.stale = true;
        if (change === "coverage") r.envelope.coverage.registry = "partial";
      },
    });
    return Effect.gen(function* () {
      expect((yield* (yield* MemberExport.MemberExport).read(1).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect(
  "keeps unknown/unsupported peer failure and local revocation fenced without fallback",
  () => {
    const f = fixture({ failure: new RegistryError({ code: "unsupported" }) });
    const revoked = fixture({
      check: (n) => (n >= 4 ? Effect.fail(new RegistryError({ code: "forbidden" })) : Effect.void),
    });
    return Effect.gen(function* () {
      expect(yield* (yield* MemberExport.MemberExport).read(1).pipe(Effect.flip)).toMatchObject({
        code: "unsupported",
      });
      expect(f.calls).toEqual(["counts", "export"]);
    }).pipe(
      Effect.provide(f.layer),
      Effect.andThen(
        Effect.gen(function* () {
          expect((yield* (yield* MemberExport.MemberExport).read(1).pipe(Effect.result))._tag).toBe(
            "Failure",
          );
          expect(revoked.calls).toEqual(["counts", "export"]);
        }).pipe(Effect.provide(revoked.layer)),
      ),
    );
  },
);
it.effect("rejects unbounded input before admission or transport", () => {
  const f = fixture();
  return Effect.gen(function* () {
    expect(yield* (yield* MemberExport.MemberExport).read(101).pipe(Effect.flip)).toMatchObject({
      code: "invalid_request",
    });
    expect(f.calls).toHaveLength(0);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "cancels a pending single-hop export without releasing a value, final read or retry",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ready = yield* Deferred.make<void>();
        let closed = false,
          completed = false;
        const f = fixture({
          pending: {
            ready,
            closed: () => {
              closed = true;
            },
          },
        });
        const fiber = yield* Effect.flatMap(MemberExport.MemberExport, (exporter) =>
          exporter.read(1),
        ).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              completed = true;
            }),
          ),
          Effect.provide(f.layer),
          Effect.forkChild,
        );
        yield* Deferred.await(ready);
        yield* Fiber.interrupt(fiber);
        expect(closed).toBe(true);
        expect(completed).toBe(false);
        expect(f.calls).toEqual(["counts", "export"]);
      }),
    ),
);
