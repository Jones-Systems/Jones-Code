import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Boundary from "./peerWriteAdmission.ts";
import * as Supplied from "./suppliedEvidence.ts";
// Boundary never admits a caller here. This test proves copied JSON cannot mint the
// private supplied-evidence capability, even when it looks like a valid proof.
it.effect("cloned supplied evidence cannot install an atomic verification scope", () =>
  Effect.gen(function* () {
    const supplied = yield* Supplied.SuppliedEvidence;
    const forged = {} as Supplied.QualifiedSuppliedEvidence;
    let ran = false;
    expect(
      (yield* Effect.result(
        supplied.withVerification(
          forged,
          "{}",
          Effect.sync(() => {
            ran = true;
          }),
        ),
      ))._tag,
    ).toBe("Failure");
    expect(ran).toBe(false);
  }).pipe(
    Effect.provide(
      Supplied.layer.pipe(
        Layer.provide(
          Layer.succeed(Boundary.PeerWriteBoundary, {
            current: () => Effect.die("No admission permitted"),
            authorize: () => Effect.die("No admission permitted"),
            checkCurrent: () => Effect.die("No admission permitted"),
            observe: () => Effect.die("No admission permitted"),
            provenance: () => Effect.die("No admission permitted"),
          }),
        ),
      ),
    ),
  ),
);
