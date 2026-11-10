// @effect-diagnostics nodeBuiltinImport:off -- Every file is synthetic task-owned SQLite evidence, retained for the root cleanup owner.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Context from "effect/Context";
import * as SqlClient from "effect/sql/SqlClient";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import * as Fixture from "../store/sql.testkit.ts";
import * as Admin from "../cli.ts";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Store from "../store/registryStore.ts";
import * as Startup from "./startup.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { EnvironmentId } from "@t3tools/contracts";
import * as ServerConfig from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import { decodeNativeConfig } from "../mode/config.ts";
import { WorkstreamsNativeCapability } from "@t3tools/contracts/jones/workstreamsNative";
import * as Schema from "effect/Schema";

const decodeCapability = Schema.decodeUnknownSync(WorkstreamsNativeCapability);

const registry = {
  ...Fixture.scope,
  canonical_origin: "https://native.workstreams.invalid",
  home_environment_id: "home:test",
  server_generation: 1,
};
const fresh = () => {
  const parent = NodePath.resolve(
    process.env.NW_SELECTED_MEMBER_EVIDENCE_DIR ?? "node_modules/.cache/nw-startup-fixtures",
  );
  NodeFS.mkdirSync(parent, { recursive: true });
  const home = NodeFS.mkdtempSync(NodePath.join(parent, "startup-")),
    database = NodePath.join(home, "statev2.sqlite"),
    keys = Fixture.secrets();
  const bootstrap = { home, database, key_provider: "server-secret-store" };
  const config = { mode: "native-home", registry, bootstrap };
  const input = {
    text: JSON.stringify(config),
    legacyActivationPresent: false,
    home,
    database,
    environmentId: registry.home_environment_id,
  };
  const layer = Admin.layer.pipe(
    Layer.provideMerge(Fixture.testLayer({ filename: database, keys })),
    Layer.provideMerge(NodeServices.layer),
  );
  return { home, database, keys, config, input, layer };
};
const seed = Effect.gen(function* () {
  yield* migration;
  yield* (yield* Admin.NativeAdmin).initialize({
    registry,
    principal_id: "principal:test",
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
});

it.effect(
  "absent config preserves legacy or disabled without reading or initializing a native home",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      for (const [legacyActivationPresent, mode] of [
        [false, "disabled"],
        [true, "legacy"],
      ] as const) {
        const result = yield* Startup.select({ ...f.input, text: null, legacyActivationPresent });
        expect(result.mode).toBe(mode);
        expect(result.home).toBeNull();
      }
      const sql = yield* SqlClient.SqlClient;
      expect(
        yield* sql`SELECT name FROM sqlite_schema WHERE name = 'jones_workstreams_registries'`,
      ).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("explicit config selects the existing exact database, keys and singleton home", () => {
  const f = fresh();
  return Effect.gen(function* () {
    yield* seed;
    const before = yield* (yield* Store.RegistryStore).context(registry);
    const result = yield* Startup.select(f.input);
    expect(result).toMatchObject({
      mode: "native-home",
      home: { ...registry, served_by_environment_id: registry.home_environment_id },
      sourceSessionId: null,
      problem: null,
    });
    expect(yield* (yield* Store.RegistryStore).context(registry)).toEqual(before);
    expect(
      Startup.capability(result, { readProjection: false, placementsV2: true }),
    ).toBeUndefined();
    expect(
      Startup.capability(result, { readProjection: true, placementsV2: true }),
    ).not.toHaveProperty("rollups");
  }).pipe(Effect.provide(f.layer));
});

it.effect("native config failures stay fenced and never construct the legacy transport", () => {
  const f = fresh();
  return Effect.gen(function* () {
    yield* seed;
    const sql = yield* SqlClient.SqlClient;
    const inputs = [
      { ...f.input, text: "{" },
      { ...f.input, legacyActivationPresent: true },
      { ...f.input, text: JSON.stringify({ ...f.config, mode: "native-member" }) },
      { ...f.input, text: JSON.stringify({ mode: "native-home", registry }) },
      {
        ...f.input,
        text: JSON.stringify({
          ...f.config,
          bootstrap: { ...f.config.bootstrap, key_provider: "unknown" },
        }),
      },
      {
        ...f.input,
        text: JSON.stringify({
          ...f.config,
          bootstrap: { ...f.config.bootstrap, database: f.database + ".other" },
        }),
      },
      { ...f.input, environmentId: "other-environment" },
      {
        ...f.input,
        text: JSON.stringify({ ...f.config, registry: { ...registry, server_generation: 2 } }),
      },
    ];
    const cp = vi
      .spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport")
      .mockImplementation(() => {
        throw new Error("CP construction forbidden");
      });
    class Selected extends Context.Service<Selected, string>()(
      "t3/jones/workstreams/native/runtime/startup.test/Selected",
    ) {}
    try {
      for (const input of inputs) {
        const selection = yield* Startup.select(input);
        expect(selection.mode).toBe("transition-fenced");
        const selected = Startup.chooseLayer({
          legacy: () => {
            ControlPlane.makeControlPlaneWorkstreamTransport({ state: "disabled" });
            return Layer.succeed(Selected, "legacy");
          },
          home: () => Layer.succeed(Selected, "home"),
          unavailable: () => Layer.succeed(Selected, "unavailable"),
        }).pipe(Layer.provide(Layer.succeed(Startup.Startup, selection)));
        expect(yield* Selected.pipe(Effect.provide(selected))).toBe("unavailable");
      }
      expect(cp).not.toHaveBeenCalled();
    } finally {
      cp.mockRestore();
    }
    yield* sql`UPDATE jones_workstreams_binding_state SET state = 'fenced', binding_version = binding_version + 1 WHERE singleton = 1`;
    expect((yield* Startup.select(f.input)).problem?.code).toBe("binding_mismatch");
  }).pipe(Effect.provide(f.layer));
});

it.effect("missing home, local or index keys fail closed without automatic key creation", () => {
  const f = fresh();
  return Effect.gen(function* () {
    yield* seed;
    // Snapshot because each missing-key probe deletes and then reinserts its Map entry.
    const keyNames = Array.from(f.keys.keys());
    for (const name of keyNames) {
      const value = f.keys.get(name)!;
      f.keys.delete(name);
      const result = yield* Startup.select(f.input);
      expect(result.mode).toBe("transition-fenced");
      expect(result.problem?.code).toBe("key_unavailable");
      f.keys.set(name, value);
    }
  }).pipe(Effect.provide(f.layer));
});

it("keeps the bootstrap schema closed and never accepts configured actor or arbitrary providers", () => {
  const f = fresh();
  expect(() =>
    decodeNativeConfig(JSON.stringify({ ...f.config, principal_id: "chosen-global-actor" })),
  ).toThrow();
  expect(() =>
    decodeNativeConfig(
      JSON.stringify({ ...f.config, bootstrap: { ...f.config.bootstrap, actor: "forged" } }),
    ),
  ).toThrow();
  expect(() =>
    decodeNativeConfig(
      JSON.stringify({
        ...f.config,
        bootstrap: { ...f.config.bootstrap, key_provider: "unregistered" },
      }),
    ),
  ).toThrow();
});

it.effect(
  "the startup layer reads and decodes the explicit config file against application resources",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      yield* seed;
      NodeFS.writeFileSync(
        NodePath.join(f.home, "workstreams-native.json"),
        JSON.stringify(f.config),
      );
      const base = yield* ServerConfig.ServerConfig.pipe(
        Effect.provide(ServerConfig.layerTest(f.home, f.home)),
      );
      const read = (environment: NodeJS.ProcessEnv = {}) =>
        Startup.Startup.pipe(
          Effect.provide(Startup.layer),
          Effect.provideService(ServerConfig.ServerConfig, {
            ...base,
            baseDir: f.home,
            dbPath: f.database,
          }),
          Effect.provideService(Environment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(EnvironmentId.make(registry.home_environment_id)),
          }),
          Effect.provideService(HostProcess.Environment, environment),
        );
      expect((yield* read()).mode).toBe("native-home");
      NodeFS.writeFileSync(
        NodePath.join(f.home, "workstreams-native.json"),
        '{"mode":"native-home","principal_id":"forged"}',
      );
      expect((yield* read()).problem?.code).toBe("invalid_config");
      NodeFS.writeFileSync(
        NodePath.join(f.home, "workstreams-native.json"),
        JSON.stringify(f.config),
      );
      expect((yield* read({ T3_WORKSTREAM_OWNER_ID: "synthetic-legacy-presence" })).mode).toBe(
        "transition-fenced",
      );
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "selects only an explicitly supplied member factory for a qualified member selection",
  () => {
    const f = fresh();
    class Branch extends Context.Service<Branch, string>()(
      "t3/jones/workstreams/native/runtime/startup.test/Branch",
    ) {}
    const selection: Startup.Selection = {
      mode: "native-member",
      config: decodeNativeConfig(JSON.stringify({ ...f.config, mode: "native-member" })),
      home: null,
      sourceSessionId: "session:member",
      problem: null,
    };
    const constructed: string[] = [];
    const factories = {
      legacy: () => {
        constructed.push("legacy");
        return Layer.succeed(Branch, "legacy");
      },
      home: () => {
        constructed.push("home");
        return Layer.succeed(Branch, "home");
      },
      member: () => {
        constructed.push("member");
        return Layer.succeed(Branch, "member");
      },
      unavailable: () => {
        constructed.push("unavailable");
        return Layer.succeed(Branch, "unavailable");
      },
    };
    const read = (value: Startup.Selection, member = true) =>
      Branch.pipe(
        Effect.provide(
          Startup.chooseLayer(
            member
              ? factories
              : {
                  legacy: factories.legacy,
                  home: factories.home,
                  unavailable: factories.unavailable,
                },
          ).pipe(Layer.provide(Layer.succeed(Startup.Startup, value))),
        ),
      );
    return Effect.gen(function* () {
      expect(yield* read(selection)).toBe("member");
      for (const invalid of [
        { ...selection, sourceSessionId: null },
        { ...selection, problem: new Startup.StartupError({ code: "authority_fenced" }) },
        { ...selection, config: null },
      ])
        expect(yield* read(invalid)).toBe("unavailable");
      expect(yield* read(selection, false)).toBe("unavailable");
      expect(constructed).toEqual([
        "member",
        "unavailable",
        "unavailable",
        "unavailable",
        "unavailable",
      ]);
    });
  },
);

it.effect("advertises only fully composed qualified home or member selections", () =>
  Effect.sync(() => {
    const mounted = { readProjection: true, placementsV2: true };
    const member: Startup.Selection = {
      mode: "native-member",
      config: decodeNativeConfig(JSON.stringify({ mode: "native-member", registry })),
      home: null,
      sourceSessionId: "session:member-descriptor",
      problem: null,
    };
    for (const homeLink of ["ok", "missing", "expired", "unknown"] as const) {
      expect(Startup.capability(member, mounted, homeLink)).toMatchObject({
        mode: "native-member",
        registry,
        home_link: homeLink,
        reads: ["workstreams-read/1.0.0"],
        placement_proofs: ["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"],
      });
    }
    const incomplete: readonly Startup.Selection[] = [
      { ...member, sourceSessionId: null },
      { ...member, config: null },
      {
        ...member,
        config: decodeNativeConfig(JSON.stringify({ mode: "disabled", registry: null })),
      },
      { ...member, problem: new Startup.StartupError({ code: "authority_fenced" }) },
      { ...member, mode: "transition-fenced" },
      { ...member, mode: "disabled" },
      { ...member, mode: "legacy" },
    ];
    for (const selection of incomplete)
      expect(Startup.capability(selection, mounted)).toBeUndefined();
    expect(
      Startup.capability(member, { readProjection: false, placementsV2: true }),
    ).toBeUndefined();
    expect(
      Startup.capability(member, { readProjection: true, placementsV2: false }),
    ).toBeUndefined();
    const home: Startup.Selection = {
      ...member,
      mode: "native-home",
      home: { ...registry, served_by_environment_id: registry.home_environment_id },
    };
    const homeCapability = Startup.capability(home, mounted);
    expect(homeCapability).toEqual({
      contract: "workstreams-native/1.0.0",
      mode: "native-home",
      registry,
      reads: ["workstreams-read/1.0.0"],
      placement_proofs: ["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"],
    });
    expect(decodeCapability(homeCapability)).toEqual(homeCapability);
    expect(
      Startup.capability(
        { ...home, problem: new Startup.StartupError({ code: "authority_fenced" }) },
        mounted,
      ),
    ).toBeUndefined();
  }),
);
