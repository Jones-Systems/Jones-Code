import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  AuthAccessWriteScope,
  AuthOrchestrationOperateScope,
  EnvironmentId,
  WS_METHODS,
  type AuthSessionState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { EnvironmentRegistry } from "../connection/registry.ts";
import { createCommandPermissions } from "../state/commandPermissions.ts";

vi.mock("../state/session.ts", () => ({
  createEnvironmentSessionAtoms: () => ({ sessionStateAtom: sessions }),
}));
const sessions = Atom.family((_id: EnvironmentId) =>
  Atom.make<AsyncResult.AsyncResult<AuthSessionState, string>>(AsyncResult.initial()),
);
const env = EnvironmentId.make("target");
const grant = (allowed: boolean): AuthSessionState => ({
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "test",
  },
  scopes: allowed ? [AuthOrchestrationOperateScope] : [],
  permissions: allowed ? [AuthOrchestrationOperateScope] : [],
});
const runtime = Atom.runtime(
  Layer.succeed(EnvironmentRegistry, {
    run: (_id: EnvironmentId, effect: Effect.Effect<unknown>) => effect,
  } as unknown as EnvironmentRegistry["Service"]),
);
const setup = Effect.gen(function* () {
  const registry = AtomRegistry.make();
  registry.mount(sessions(env));
  yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
  return registry;
});

it.effect("requires access management to link or forget environments", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const registry = yield* setup;
      registry.set(sessions(env), AsyncResult.success(grant(true)));
      for (const method of [WS_METHODS.peerLinksLink, WS_METHODS.peerLinksUnlink]) {
        const peerLinks = createCommandPermissions(runtime, method);
        expect(registry.get(peerLinks.permissionAtom(env))).toBe(false);
        expect(
          (yield* peerLinks.authorize(registry, env).pipe(Effect.flip)).requiredPermission,
        ).toBe(AuthAccessWriteScope);
      }
      registry.set(
        sessions(env),
        AsyncResult.success({
          ...grant(false),
          scopes: [AuthAccessWriteScope],
          permissions: [AuthAccessWriteScope],
        }),
      );
      const link = createCommandPermissions(runtime, WS_METHODS.peerLinksLink);
      expect(registry.get(link.permissionAtom(env))).toBe(true);
      yield* link.authorize(registry, env);
    }),
  ),
);
