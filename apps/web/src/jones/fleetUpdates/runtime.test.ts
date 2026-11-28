import { EnvironmentId } from "@t3tools/contracts";
import type { ConnectionCatalogEntry, NetworkStatus } from "@t3tools/client-runtime/connection";
import type { FleetHostRequest } from "@t3tools/client-runtime/jones/fleet-updates";
import { expect, it, vi } from "vite-plus/test";
import { fleetHost } from "./runtime";

const harness = vi.hoisted(() => ({ calls: [] as string[] }));
const host = EnvironmentId.make("fleet-before-send-test");
vi.mock("../../connection/runtime", async () => {
  const { EnvironmentRegistry, PrimaryConnectionTarget } =
    await import("@t3tools/client-runtime/connection");
  const Effect = await import("effect/Effect");
  const Layer = await import("effect/Layer");
  const Option = await import("effect/Option");
  const SubscriptionRef = await import("effect/SubscriptionRef");
  const { Atom } = await import("effect/reactivity");
  const entry = {
    target: new PrimaryConnectionTarget({
      environmentId: EnvironmentId.make("fleet-before-send-test"),
      label: "Synthetic host",
      httpBaseUrl: "https://fleet.example.test",
      wsBaseUrl: "wss://fleet.example.test",
    }),
    profile: Option.none(),
    enabled: true,
  };
  const registry = Layer.unwrap(
    Effect.gen(function* () {
      const entries = yield* SubscriptionRef.make<
        ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>
      >(new Map([[entry.target.environmentId, entry]]));
      const networkStatus = yield* SubscriptionRef.make<NetworkStatus>("online");
      return Layer.mock(EnvironmentRegistry.EnvironmentRegistry)({ entries, networkStatus });
    }),
  );
  return { connectionAtomRuntime: Atom.runtime(registry) };
});
vi.mock("@t3tools/client-runtime/jones/fleet-updates", async () => {
  const Effect = await import("effect/Effect");
  return {
    requestFleetHost: (_entry: unknown, command: FleetHostRequest) =>
      Effect.promise(async () => {
        harness.calls.push(command.action);
        if ("beforeSend" in command && command.beforeSend !== undefined)
          expect(await command.beforeSend()).toBe(true);
        return {};
      }),
  };
});

it("reads fresh authority from a queued write while keeping host mutations serial", async () => {
  const input: Extract<FleetHostRequest, { action: "fenceDirective" }>["input"] = {
    directiveId: "22222222-2222-4222-8222-222222222222",
    enrollmentId: "11111111-1111-4111-8111-111111111111",
    expectedDirectiveId: null,
    environmentId: host,
    repository: "Jones-Systems/Jones-Code",
    targetSource: "a".repeat(40),
    targetTree: "b".repeat(40),
  };
  const fresh = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const first = fleetHost(host, {
    action: "fenceDirective",
    input,
    beforeSend: async () => {
      await fleetHost(host, { action: "status" });
      fresh.resolve();
      await finish.promise;
      return true;
    },
  });
  const second = fleetHost(host, { action: "planDirective", input });
  const writes = Promise.all([first, second]);
  await Promise.race([fresh.promise, writes]);
  try {
    expect(harness.calls).toEqual(["fenceDirective", "status"]);
  } finally {
    finish.resolve();
    await writes;
  }
  expect(harness.calls).toEqual(["fenceDirective", "status", "planDirective"]);
});
