import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts";
import { WorkstreamsNativePlacementPage } from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as Fixture from "../store/sql.testkit.ts";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admin from "../cli.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Local from "./localTransport.ts";
import * as Registry from "./service.ts";
import * as Session from "./session.ts";
import * as Appearance from "./appearance.ts";
import * as Counts from "./counts.ts";
import * as Placements from "./placements.ts";
import * as Runner from "./effectRunner.ts";
const decodeNativePlacementPage = Schema.decodeUnknownEffect(WorkstreamsNativePlacementPage);
const decodePlacementPage = Schema.decodeUnknownEffect(Wire.T3PlacementPage);
const home = {
  ...Fixture.scope,
  canonical_origin: "https://native.workstreams.invalid",
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
const base = Registry.layer.pipe(
  Layer.provideMerge(Session.layer),
  Layer.provideMerge(Fixture.testLayer()),
);
const services = Local.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(Appearance.layer, Counts.layer, Placements.layer)),
  Layer.provideMerge(base),
  Layer.provide(Layer.succeed(NativeHome, home)),
  Layer.provide(
    Layer.succeed(Placements.PlacementTrust, {
      readTrustSnapshot: () => ({ readiness: "ready", trustedEnvironments: [] }),
    }),
  ),
  Layer.provide(
    Layer.succeed(Runner.EffectRunner, {
      run: () => Effect.die("Unexpected provider"),
      recover: () => Effect.die("Unexpected recovery"),
      recoverOne: () => Effect.die("Unexpected lookup"),
    }),
  ),
);
it.effect(
  "negotiates an honest empty v1 page or explicit native page while preserving the bare legacy adapter",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(Fixture.now));
      yield* migration;
      const admin = yield* Admin.NativeAdmin.pipe(Effect.provide(Admin.layer));
      const { served_by_environment_id: _served, ...registry } = home;
      yield* admin.initialize({
        registry,
        principal_id: caller.principal_id,
        content_key_id: "test-home",
        local_key_id: "test-local",
      });
      const adapter = yield* (yield* Local.LocalTransport).forCaller(caller);
      const request = { identities: [], limit: 50 };
      const defaultPage = yield* adapter.placementPage(request);
      const v1 = yield* adapter.placementPage(request, "workstreams-t3-placement/1.0.0");
      const v2 = yield* adapter.placementPage(request, "workstreams-t3-placement/2.0.0");
      expect(defaultPage).toEqual(v1);
      expect(defaultPage).not.toHaveProperty("provenance");
      expect(yield* decodePlacementPage(v1, { onExcessProperty: "error" })).toEqual(v1);
      expect(
        yield* decodeNativePlacementPage(v2, {
          onExcessProperty: "error",
        }),
      ).toEqual(v2);
      expect(v2).toMatchObject({
        items: [],
        provenance: { authority: "native-home", registry_instance_id: home.registry_instance_id },
      });
      expect(yield* adapter.transport.listThreadPlacements!(request)).toEqual(v1);
    }).pipe(Effect.provide(services)),
);
