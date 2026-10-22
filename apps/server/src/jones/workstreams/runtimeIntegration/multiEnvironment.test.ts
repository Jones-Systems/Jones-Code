// @effect-diagnostics globalFetch:off -- Integration tests exercise actual loopback HTTP rather than an injected client.
import {
  T3WorkstreamListResult,
  WorkstreamReceipt,
  WorkstreamReferencePage,
  WorkstreamMembershipPage,
  WorkstreamsRegistrationContextV2Response,
  type WorkstreamCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { it as effectIt } from "@effect/vitest";
import { expect, it } from "vite-plus/test";
import { makeControlPlaneWorkstreamTransport } from "../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import {
  build,
  sources,
  startLoopbackControlPlane,
  startEnvironment,
} from "../testing/loopbackControlPlane.ts";
const decodeReceipt = Schema.decodeUnknownSync(WorkstreamReceipt);
const decodeRegistration = Schema.decodeUnknownSync(WorkstreamsRegistrationContextV2Response);
type ControlPlane = Awaited<ReturnType<typeof startLoopbackControlPlane>>;
async function withEnvironments(
  run: (fixture: { control: ControlPlane; a: string; b: string }) => Promise<void>,
) {
  const control = await startLoopbackControlPlane();
  const environments: Awaited<ReturnType<typeof startEnvironment>>[] = [];
  try {
    environments.push(await startEnvironment(control, "source-a"));
    environments.push(await startEnvironment(control, "source-b"));
    await run({ control, a: environments[0]!.origin, b: environments[1]!.origin });
  } finally {
    try {
      await Promise.all(environments.map((environment) => environment.close()));
    } finally {
      await control.close();
    }
  }
}

const get = (origin: string, path: string) =>
  fetch(`${origin}/api/workstreams${path}`, { signal: AbortSignal.timeout(5_000) });
async function read<A>(origin: string, path: string, schema: Schema.ConstraintDecoder<A, never>) {
  const response = await get(origin, path);
  expect(response.status).toBe(200);
  return Schema.decodeUnknownSync(schema)(await response.json());
}
async function submit(origin: string, command: WorkstreamCommand) {
  const response = await fetch(`${origin}/api/workstreams/commands`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ command }),
    signal: AbortSignal.timeout(5_000),
  });
  expect(response.status).toBe(200);
  return decodeReceipt(await response.json());
}

it("qualifies each environment's enrolled build over signed loopback HTTP", async () => {
  await withEnvironments(async ({ a, b, control }) => {
    for (const [origin, source] of [
      [a, sources[0]!],
      [b, sources[1]!],
    ] as const) {
      const response = await get(origin, "/registration-context");
      expect(response.status).toBe(200);
      expect(response.headers.get("x-jones-workstreams-t3-availability")).toBe("ready");
      const context = decodeRegistration(await response.json());
      expect(context.state).toBe("ready");
      if (context.state === "ready") expect(context.sources).toEqual([source]);
    }
    expect(control.observations.accepted).toBe(2);
    expect(control.observations.rejected).toBe(0);
  });
});

it("assigns colliding native IDs independently, moves and removes memberships across environments", async () => {
  await withEnvironments(async ({ a, b, control }) => {
    const references = await read(a, "/references", WorkstreamReferencePage);
    const native = references.items.filter((reference) => reference.identity.provider === "t3");
    expect(native).toHaveLength(3);
    const referenceA = native.find(
      (reference) => reference.identity.source_instance_id === "source-a",
    )!;
    const referenceB = native.find(
      (reference) =>
        reference.identity.source_instance_id === "source-b" &&
        reference.identity.native_id === "shared-thread",
    )!;
    expect(referenceA.identity.native_id).toBe(referenceB.identity.native_id);
    expect(referenceA.native_reference_id).not.toBe(referenceB.native_reference_id);
    let sequence = 0;
    const commandFor = async (
      origin: string,
      action: WorkstreamCommand["action"],
    ): Promise<WorkstreamCommand> => {
      const list = await read(origin, "", T3WorkstreamListResult);
      return {
        command_id: `multi-env-command-${++sequence}`,
        expected_server_generation: list.binding.serverGeneration,
        expected_registry_version: list.binding.registryVersion,
        action,
      };
    };
    const attachA = await commandFor(a, {
      operation: "attach_primary",
      workstream_id: "ws-core-v1",
      expected_version: 3,
      native_reference_id: referenceA.native_reference_id,
    });
    const receiptA = await submit(a, attachA);
    expect(receiptA.state).toBe("committed");
    expect(await read(b, `/commands/${attachA.command_id}`, WorkstreamReceipt)).toEqual(receiptA);
    const staleReplay = await fetch(`${b}/api/workstreams/commands`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ command: attachA }),
      signal: AbortSignal.timeout(5_000),
    });
    expect(staleReplay.status).toBe(409);
    let memberships = await read(b, "/ws-core-v1/memberships", WorkstreamMembershipPage);
    expect(memberships.items.filter((item) => item.closed === null)).toHaveLength(1);
    const attachB = await commandFor(b, {
      operation: "attach_primary",
      workstream_id: "ws-core-v1",
      expected_version: 4,
      native_reference_id: referenceB.native_reference_id,
    });
    expect((await submit(b, attachB)).state).toBe("committed");
    memberships = await read(a, "/ws-core-v1/memberships", WorkstreamMembershipPage);
    expect(
      new Set(
        memberships.items
          .filter((item) => item.closed === null)
          .map((item) => item.native_reference_id),
      ),
    ).toEqual(new Set([referenceA.native_reference_id, referenceB.native_reference_id]));
    const membershipA = memberships.items.find(
      (item) => item.native_reference_id === referenceA.native_reference_id,
    )!;
    const move = await commandFor(b, {
      operation: "move_primary",
      source_workstream_id: "ws-core-v1",
      expected_source_version: 5,
      source_membership_id: membershipA.membership_id,
      destination_workstream_id: "ws-follow-on",
      expected_destination_version: 1,
    });
    expect((await submit(b, move)).state).toBe("committed");
    const destination = await read(a, "/ws-follow-on/memberships", WorkstreamMembershipPage);
    expect(destination.items).toHaveLength(1);
    expect(destination.items[0]!.native_reference_id).toBe(referenceA.native_reference_id);
    const remove = await commandFor(a, {
      operation: "remove_membership",
      workstream_id: "ws-follow-on",
      expected_version: 2,
      membership_id: destination.items[0]!.membership_id,
    });
    expect((await submit(a, remove)).state).toBe("committed");
    expect(
      (await read(b, "/ws-follow-on/memberships", WorkstreamMembershipPage)).items.every(
        (item) => item.closed !== null,
      ),
    ).toBe(true);
    expect(
      (await read(b, "/ws-core-v1/memberships", WorkstreamMembershipPage)).items
        .filter((item) => item.closed === null)
        .map((item) => item.native_reference_id),
    ).toEqual([referenceB.native_reference_id]);
    expect(control.observations.submissions).toBe(4);
  });
});

it("reports mismatched, unstamped and unenrolled environments without borrowing another source", async () => {
  const control = await startLoopbackControlPlane();
  try {
    for (const [source, stamp, reason] of [
      ["source-a", { ...build, sha: "c".repeat(40) }, "contract_mismatch"],
      ["source-a", null, "build_unavailable"],
      ["missing-source", build, "source_mismatch"],
    ] as const) {
      const environment = await startEnvironment(control, source, stamp);
      try {
        const response = await get(environment.origin, "/registration-context");
        expect(response.status).toBe(200);
        expect(response.headers.get("x-jones-workstreams-t3-availability")).toBe(
          `unavailable;reason=${reason}`,
        );
        const context = decodeRegistration(await response.json());
        expect(context.state).toBe("ready");
        if (context.state === "ready") expect(context.sources).toEqual([]);
      } finally {
        await environment.close();
      }
    }
    expect(control.observations.submissions).toBe(0);
  } finally {
    await control.close();
  }
});

effectIt.effect("rejects a wrong signing key and closes its listener", () =>
  Effect.gen(function* () {
    const control = yield* Effect.promise(startLoopbackControlPlane);
    try {
      const invalid = makeControlPlaneWorkstreamTransport({
        ...control.activation,
        value: {
          ...control.activation.value,
          signingSecret: "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
        },
      });
      const result = yield* invalid.registrationContext.readRegistrationContext.pipe(Effect.flip);
      expect(result.effect).toBe("no-effect");
      expect(control.observations.rejected).toBe(1);
      expect(control.observations.accepted).toBe(0);
    } finally {
      yield* Effect.promise(() => control.close());
    }
    yield* Effect.promise(() =>
      expect(
        // @effect-diagnostics-next-line globalFetchInEffect:off -- Verify the closed socket with the same native HTTP client used by the integration fixture.
        fetch(`${control.origin}/workstreams/v1/capabilities`, {
          signal: AbortSignal.timeout(1_000),
        }),
      ).rejects.toThrow(),
    );
  }),
);

it("closes both environments and the control plane after a failing test body", async () => {
  const failure = new Error("deliberate fixture failure");
  const origins: string[] = [];
  await expect(
    withEnvironments(async ({ a, b, control }) => {
      origins.push(a, b, control.origin);
      throw failure;
    }),
  ).rejects.toBe(failure);
  expect(origins).toHaveLength(3);
  for (const origin of origins) {
    await expect(fetch(origin, { signal: AbortSignal.timeout(1_000) })).rejects.toThrow();
  }
});
