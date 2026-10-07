import * as Schema from "effect/Schema";

export const NativeWorkspaceBasis = Schema.Struct({
  bootId: Schema.NonEmptyString, projectId: Schema.NonEmptyString,
  projectCwd: Schema.NonEmptyString, projectBirth: Schema.NonEmptyString,
  gitCommonDirectory: Schema.NonEmptyString, physicalGitIdentity: Schema.NonEmptyString,
  worktreePath: Schema.NonEmptyString, parentBirth: Schema.NonEmptyString,
  producerId: Schema.NonEmptyString, baseRef: Schema.NonEmptyString,
  setupDefinition: Schema.NullOr(Schema.NonEmptyString),
  configuredSubmodulesDefinition: Schema.String, baseConfigurationDefinition: Schema.String,
});
export type NativeWorkspaceBasis = typeof NativeWorkspaceBasis.Type;
export const NativeWorkspaceProof = Schema.Struct({
  worktreePath: Schema.NonEmptyString, pathBirth: Schema.NonEmptyString,
  gitCommonDirectory: Schema.NonEmptyString, physicalGitIdentity: Schema.NonEmptyString,
  branch: Schema.NonEmptyString, baseRef: Schema.NonEmptyString,
  configuredSubmodulesDigest: Schema.NonEmptyString, baseConfigurationDigest: Schema.NonEmptyString,
});
export type NativeWorkspaceProof = typeof NativeWorkspaceProof.Type;
export const NativeWorkspaceVerified = Schema.Struct({
  claimId: Schema.NonEmptyString, basis: NativeWorkspaceBasis, proof: NativeWorkspaceProof,
  setupTerminalId: Schema.NullOr(Schema.NonEmptyString),
});
export type NativeWorkspaceVerified = typeof NativeWorkspaceVerified.Type;
