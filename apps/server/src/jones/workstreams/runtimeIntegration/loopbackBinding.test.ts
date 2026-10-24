import { it as effectIt } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeControlPlaneWorkstreamTransport } from "../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import { make } from "../../../workstreams/WorkstreamGateway.ts";
// @effect-diagnostics globalFetch:off -- This fixture exercises the actual isolated HTTP boundary.
import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { T3WorkstreamListResult } from "@t3tools/contracts";
import { startLoopbackControlPlane, startEnvironment } from "../testing/loopbackControlPlane.ts";

const decodeList = Schema.decodeUnknownSync(T3WorkstreamListResult);

it("returns the pinned HTTPS registry identity through real Jones HTTP and the unchanged client decoder", async () => {
  const control = await startLoopbackControlPlane();
  try {
    const environment = await startEnvironment(control, "source-a");
    try {
      const response = await fetch(`${environment.origin}/api/workstreams`, {
        signal: AbortSignal.timeout(5_000),
      });
      expect(response.status).toBe(200);
      const list = decodeList(await response.json());
      expect(list.binding.registryId).toBe("https://workstreams.test");
      expect(list.source).toBe("live");
    } finally {
      await environment.close();
    }
  } finally {
    await control.close();
  }
});

effectIt.effect(
  "never substitutes gateway cached success after list proof loss or identity change",
  () =>
    Effect.gen(function* () {
      const control = yield* Effect.promise(startLoopbackControlPlane);
      try {
        const configured = makeControlPlaneWorkstreamTransport(control.activation);
        const gateway = yield* make(configured.transport, { binding: configured.binding });
        expect((yield* gateway.readMetadata()).source).toBe("live");
        control.faults.listResponse = "missing";
        expect(yield* gateway.readMetadata().pipe(Effect.flip)).toMatchObject({
          reason: "unknown-effect",
        });
        control.faults.listResponse = "valid";
        control.authority.registry_instance_id = "33333333-3333-4333-8333-333333333333";
        expect(yield* gateway.readMetadata().pipe(Effect.flip)).toMatchObject({
          reason: "unknown-effect",
        });
        expect(yield* gateway.readSession().pipe(Effect.flip)).toMatchObject({
          reason: "unknown-effect",
        });
        expect(control.observations.submissions).toBe(0);
      } finally {
        yield* Effect.promise(() => control.close());
      }
    }),
);
