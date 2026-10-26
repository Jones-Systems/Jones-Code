import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Pins from "./peerSessionPins.ts";
import * as Authentication from "./peerMcpAuthentication.ts";
const signing = Layer.succeed(
  Secrets.ServerSecretStore,
  Secrets.ServerSecretStore.of({
    get: () => Effect.succeed(Option.some(Test.keyBytes)),
    getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
    set: () => Effect.die("unexpected set"),
    create: () => Effect.die("unexpected create"),
    remove: () => Effect.die("unexpected remove"),
  }),
);
const services = Authentication.layer.pipe(
  Layer.provideMerge(Pins.nativeLayer),
  Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
  Layer.provideMerge(Test.testLayer()),
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "nw-j3a-synthetic-" })),
  Layer.provideMerge(
    Layer.succeed(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  const auth = yield* Auth.EnvironmentAuth;
  const native = yield* Pins.NativePeerSessions;
  const adapter = yield* Authentication.PeerMcpAuthentication;
  const issued = yield* auth.issueMcpClientSession({
    label: "synthetic peer",
    access: "full-access",
    peerLink: true,
    client: { deviceType: "bot" },
  });
  const scope: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("home:test"),
    issuedAt: Date.parse(Test.now),
    requestNamespace: "final-request",
    capabilities: new Set(["orchestration"]),
    thread: undefined,
    client: {
      sessionId: issued.sessionId,
      label: "peer",
      access: "full-access",
      linked: true,
      narrowedTo: { runtimeMode: "approval-required", interactionMode: "plan" },
    },
  };
  const request = {
    cookies: {},
    headers: { authorization: "Bearer " + issued.token },
  } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
  return { auth, native, adapter, scope, request };
});

it.effect(
  "registers the exact final limited/attributed scope through genuine native authentication",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.adapter.register(f.request, f.scope);
      const facts = yield* f.native.current(f.scope);
      expect(facts.session.ceiling.permissions).toEqual(["workstreams:read"]);
      expect(yield* Effect.exit(f.native.current({ ...f.scope }))).toMatchObject({
        _tag: "Failure",
      });
      Object.assign(f.scope, { requestNamespace: "different-run" });
      expect(yield* Effect.exit(f.native.current(f.scope))).toMatchObject({ _tag: "Failure" });
    }).pipe(Effect.provide(services)),
);

it.effect(
  "known Workstreams refusal preserves general linked handling but clears qualified evidence",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.adapter.register(f.request, f.scope);
      yield* f.native.current(f.scope);
      // Preview-only is a genuine generally authenticated linked request, outside Workstreams scope.
      (f.scope.capabilities as Set<Mcp.McpCapability>).delete("orchestration");
      (f.scope.capabilities as Set<Mcp.McpCapability>).add("preview");
      const general = yield* f.auth.authenticateMcpClient(f.request);
      expect(general.peerLink).toBe(true);
      yield* f.adapter.register(f.request, f.scope);
      let handled = false;
      yield* Mcp.requireMcpCapability("preview").pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            handled = true;
          }),
        ),
        Effect.provideService(Mcp.McpInvocationContext, f.scope),
      );
      expect(handled).toBe(true);
      expect(yield* Effect.exit(f.native.current(f.scope))).toMatchObject({ _tag: "Failure" });
      // Restoring the same scope bytes cannot revive the deleted registration.
      (f.scope.capabilities as Set<Mcp.McpCapability>).clear();
      (f.scope.capabilities as Set<Mcp.McpCapability>).add("orchestration");
      expect(yield* Effect.exit(f.native.current(f.scope))).toMatchObject({ _tag: "Failure" });
    }).pipe(Effect.provide(services)),
);

it.effect("nonlinked requests skip registration even when a native qualifier would defect", () => {
  const native = Layer.succeed(Pins.NativePeerSessions, {
    authenticate: () => Effect.die("unexpected registration"),
    current: () => Effect.die("unused"),
  });
  return Effect.gen(function* () {
    const adapter = yield* Authentication.PeerMcpAuthentication;
    yield* adapter.register({} as Parameters<typeof adapter.register>[0], {
      environmentId: EnvironmentId.make("home:test"),
      issuedAt: 0,
      requestNamespace: "ordinary",
      capabilities: new Set(),
      thread: undefined,
      client: undefined,
    });
  }).pipe(Effect.provide(Authentication.layer.pipe(Layer.provide(native))));
});

it.effect("unexpected native defects remain failures", () => {
  const native = Layer.succeed(Pins.NativePeerSessions, {
    authenticate: () => Effect.die("native defect"),
    current: () => Effect.die("unused"),
  });
  return Effect.gen(function* () {
    const adapter = yield* Authentication.PeerMcpAuthentication;
    const result = yield* Effect.exit(
      adapter.register({} as Parameters<typeof adapter.register>[0], {
        environmentId: EnvironmentId.make("home:test"),
        issuedAt: 0,
        requestNamespace: "linked",
        capabilities: new Set(["orchestration"]),
        thread: undefined,
        client: { sessionId: "synthetic", label: "peer", access: "read-only", linked: true },
      }),
    );
    expect(result._tag).toBe("Failure");
  }).pipe(Effect.provide(Authentication.layer.pipe(Layer.provide(native))));
});
