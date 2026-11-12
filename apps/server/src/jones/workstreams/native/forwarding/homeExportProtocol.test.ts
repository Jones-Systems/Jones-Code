// @effect-diagnostics nodeBuiltinImport:off -- Exact synthetic JSONL digest; real encrypted SQLite/projection/admission stores.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Fixture from "../store/sql.testkit.ts";
import migration from "../../../../jones/persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admin from "../cli.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";
import * as Projection from "../read/projection.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import { WORKSTREAMS_HOME_READ_EXPORT_CONTRACT } from "@t3tools/contracts/jones/workstreamsNativeExport";
import * as ReadProtocol from "./homeReadProtocol.ts";
import * as Protocol from "./homeExportProtocol.ts";

const home = {
  ...Fixture.scope,
  canonical_origin: "https://11111111-1111-4111-8111-111111111111.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const caller: AdmittedCaller = {
  principal_id: "principal:test",
  source_environment_id: "home:test",
  binding_version: 1,
  origin: { kind: "app" },
};
const target = {
  registry_instance_id: home.registry_instance_id,
  owner_id: home.owner_id,
  home_environment_id: home.home_environment_id,
  server_generation: home.server_generation,
};
const request = {
  contract: WORKSTREAMS_HOME_READ_EXPORT_CONTRACT,
  expected_home: target,
  page_size: 1,
};
const fixture = (drift = false) => {
  let admissions = 0;
  const registry = Registry.layer.pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(Fixture.testLayer()),
  );
  const base = Projection.layer.pipe(
    Layer.provideMerge(registry),
    Layer.provideMerge(Layer.succeed(NativeHome, home)),
  );
  const peer = Layer.succeed(ReadProtocol.HomePeerReadAdmission, {
    current: () =>
      Effect.sync(() => ({
        caller,
        authorization_revision: drift && ++admissions >= 3 ? 2 : 1,
        checkCurrent: () => Effect.void,
      })),
  });
  return {
    layer: Protocol.layer.pipe(Layer.provideMerge(base), Layer.provide(peer)),
    admissions: () => admissions,
  };
};
const initialize = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Fixture.now));
  yield* migration;
  const admin = yield* Admin.NativeAdmin.pipe(Effect.provide(Admin.layer));
  const { served_by_environment_id: _served, ...identity } = home;
  yield* admin.initialize({
    registry: identity,
    principal_id: caller.principal_id,
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
});
it.effect(
  "uses the actual encrypted projection and emits only the original complete home snapshot",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      yield* initialize;
      const protocol = yield* Protocol.HomeExportProtocol;
      const result = yield* protocol.read(request);
      expect(result.home).toEqual(target);
      expect(result.principal_id).toBe(caller.principal_id);
      expect(result.authorization_revision).toBe(1);
      const lines = result.jsonl.split("\n");
      expect(lines.at(-1)).toBe("");
      const trailer = JSON.parse(lines.at(-2)!);
      expect(trailer).toEqual({
        complete: true,
        registry_version: 1,
        sha256: NodeCrypto.createHash("sha256")
          .update(lines.slice(0, -2).join("\n") + "\n")
          .digest("hex"),
      });
      expect(JSON.parse(lines[0]!)).toMatchObject({
        offset: 0,
        items: [],
        counts: { total: 0, principal_id: caller.principal_id },
      });
    }).pipe(Effect.provide(f.layer));
  },
);
it.effect("refuses a changed grant before releasing any completed export", () => {
  const f = fixture(true);
  return Effect.gen(function* () {
    yield* initialize;
    expect(
      yield* (yield* Protocol.HomeExportProtocol).read(request).pipe(Effect.flip),
    ).toMatchObject({ code: "forbidden" });
    expect(f.admissions()).toBe(3);
  }).pipe(Effect.provide(f.layer));
});
it.effect("rejects an expected-home substitution before peer admission", () => {
  const f = fixture(true);
  return Effect.gen(function* () {
    expect(
      yield* (yield* Protocol.HomeExportProtocol)
        .read({ ...request, expected_home: { ...target, server_generation: 2 } })
        .pipe(Effect.flip),
    ).toMatchObject({ code: "registry_mismatch" });
    expect(f.admissions()).toBe(0);
  }).pipe(Effect.provide(f.layer));
});
