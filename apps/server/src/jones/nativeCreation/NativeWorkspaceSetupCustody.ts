import * as Effect from "effect/Effect";
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
export const consume = (
  handle: NativeWorkspaceSetupCustody | undefined,
  claim: Repository.NativeCreationStoredIntent,
  basis: NativeWorkspaceBasis,
) =>
  Effect.gen(function* () {
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
    return value.start;
  });
