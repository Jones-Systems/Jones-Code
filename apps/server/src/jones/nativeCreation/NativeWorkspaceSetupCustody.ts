import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import type { NativeSetupControl } from "../../terminal/NativeSetupControl.ts";
import type * as Repository from "./NativeCreationRepository.ts";
import type { NativeWorkspaceBasis } from "./NativeCreationWorkspaceTypes.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import { nativeCreationCanonicalJson as canonical } from "./NativeCreationPreparation.ts";

declare const freshSetupCustody: unique symbol;
export interface NativeWorkspaceSetupCustody {
  readonly [freshSetupCustody]: true;
}
interface IssuedSetup {
  readonly claim: string;
  readonly basis: string;
  readonly start: Extract<Repository.NativeCreationStartedFact, { kind: "setup" }>;
  consumed: boolean;
}
// This ephemeral capability accompanies only the original successful start transaction.
// Durable recovery remains with migration101; a process restart cannot reconstruct this handle.
const issued = new WeakMap<NativeWorkspaceSetupCustody, IssuedSetup>();
const consumed = new WeakMap<IssuedSetup["start"], IssuedSetup>();
export const issue = (
  claim: Repository.NativeCreationStoredIntent,
  basis: NativeWorkspaceBasis,
  start: Extract<Repository.NativeCreationStartedFact, { kind: "setup" }>,
): NativeWorkspaceSetupCustody => {
  const handle = Object.freeze({}) as NativeWorkspaceSetupCustody;
  issued.set(handle, {
    claim: canonical(claim),
    basis: canonical(basis),
    start: { ...start },
    consumed: false,
  });
  return handle;
};
export const consume = Effect.fn("NativeWorkspaceSetupCustody.consume")(function* (
  handle: NativeWorkspaceSetupCustody | undefined,
  claim: Repository.NativeCreationStoredIntent,
  basis: NativeWorkspaceBasis,
) {
  const value = handle === undefined ? undefined : issued.get(handle);
  if (
    value === undefined ||
    value.consumed ||
    value.claim !== canonical(claim) ||
    value.basis !== canonical(basis)
  )
    return yield* new Workspace.NativeWorkspaceError({
      code: "unknown",
      message: "Native setup has no fresh original-start custody",
    });
  value.consumed = true;
  consumed.set(value.start, value);
  return value.start;
});

interface ActiveSetupRuntimeUse {
  readonly claim: Repository.NativeCreationStoredIntent;
  readonly basis: NativeWorkspaceBasis;
  readonly start: IssuedSetup["start"];
  readonly control: NativeSetupControl;
  readonly fingerprint: string;
  readonly active: () => boolean;
}
class SetupRuntimeUse extends Context.Service<SetupRuntimeUse, ActiveSetupRuntimeUse>()(
  "t3/jones/nativeCreation/NativeWorkspaceSetupCustody/SetupRuntimeUse",
) {}

// This context is issued only from the actual one-shot consume result, while
// the existing executor revalidates its durable start/current grant/physical
// custody. It permits the pre-birth terminal journal path, never retirement.
export const withConsumedSetupRuntimeUse = Effect.fn("NativeWorkspaceSetupCustody.withRuntimeUse")(
  function* <A, E, R>(
    start: IssuedSetup["start"],
    claim: Repository.NativeCreationStoredIntent,
    basis: NativeWorkspaceBasis,
    control: NativeSetupControl,
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | Workspace.NativeWorkspaceError, R> {
    const original = consumed.get(start);
    if (
      original === undefined ||
      !original.consumed ||
      original.claim !== canonical(claim) ||
      original.basis !== canonical(basis) ||
      canonical(original.start) !== canonical(start)
    )
      return yield* new Workspace.NativeWorkspaceError({
        code: "unknown",
        message: "Native setup runtime use lacks its actual consumed original custody",
      });
    // A consumed start grants this continuation once, never a reconstructed or
    // later replay. Child writer fibers may inherit it during runner admission.
    consumed.delete(start);
    let active = true;
    const value = { claim, basis, start, control };
    return yield* effect.pipe(
      Effect.provideService(SetupRuntimeUse, {
        ...value,
        fingerprint: canonical(value),
        active: () => active,
      }),
      Effect.ensuring(
        Effect.sync(() => {
          active = false;
        }),
      ),
    );
  },
);
export const readConsumedSetupRuntimeUse = Effect.fn("NativeWorkspaceSetupCustody.readRuntimeUse")(
  function* (control: NativeSetupControl | undefined) {
    const value = yield* Effect.serviceOption(SetupRuntimeUse);
    if (Option.isNone(value) || !value.value.active() || control === undefined) return null;
    const current = value.value;
    if (
      canonical({
        claim: current.claim,
        basis: current.basis,
        start: current.start,
        control: current.control,
      }) !== current.fingerprint ||
      canonical(control) !== canonical(current.control)
    )
      return yield* new Workspace.NativeWorkspaceError({
        code: "conflict",
        message: "Native setup runtime use original continuation changed",
      });
    return {
      claim: current.claim,
      basis: current.basis,
      start: current.start,
      control: current.control,
    };
  },
);
