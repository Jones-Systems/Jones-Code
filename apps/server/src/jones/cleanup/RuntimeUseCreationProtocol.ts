import * as Schema from "effect/Schema";
import { DirectoryIdentity } from "../ordinaryCheckout/CheckoutProtocol.ts";

const id = Schema.NonEmptyString;
const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const path = Schema.NonEmptyString.check(
  Schema.makeFilter(
    (value) =>
      value.startsWith("/") &&
      !value.includes("//") &&
      (value === "/" || !value.endsWith("/")) &&
      !value.split("/").some((part) => part === "." || part === ".."),
  ),
);
const directory = Schema.Struct({ canonicalPath: path, identity: DirectoryIdentity });

// Sparse references and physical observations only. The original native intent,
// admission, effect facts and verified receipt remain in their owning stores.
export const RuntimeUseCreationAdmission = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-creation-admission/v1"),
  claimId: id,
  commandId: id,
  rootThreadId: id,
  canonicalPath: path,
  installationSha256: hash,
  nativeIntentSha256: hash,
  workspaceBasisSha256: hash,
  worktreeEffectId: hash,
  project: directory,
  parent: directory,
  gitCommon: directory,
});
export type RuntimeUseCreationAdmission = typeof RuntimeUseCreationAdmission.Type;

export const RuntimeUseCreationMaterial = Schema.Struct({
  schema: Schema.Literal("jones.workspace-runtime-creation-material/v1"),
  claimId: id,
  admissionSha256: hash,
  workspaceVerifiedSha256: hash,
  worktreeStartSha256: hash,
  worktreeCompletionSha256: hash,
  target: directory,
  dotGit: Schema.Struct({ canonicalPath: path, identity: DirectoryIdentity }),
  gitDirectory: directory,
  gitCommon: directory,
});
export type RuntimeUseCreationMaterial = typeof RuntimeUseCreationMaterial.Type;
export const RuntimeUseCreationFact = Schema.Union([
  RuntimeUseCreationAdmission,
  RuntimeUseCreationMaterial,
]);
export type RuntimeUseCreationFact = typeof RuntimeUseCreationFact.Type;
export const decodeRuntimeUseCreationFact = Schema.decodeUnknownEffect(RuntimeUseCreationFact, {
  onExcessProperty: "error",
});
