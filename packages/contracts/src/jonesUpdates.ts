import * as Schema from "effect/Schema";
import { EnvironmentId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const JonesUpdatePhase = Schema.Literals([
  "checking",
  "building",
  "no-new",
  "available",
  "downloading",
  "verifying",
  "staged",
  "preparing",
  "installing",
  "committed",
  "rolled-back",
  "blocked",
  "error",
]);
export type JonesUpdatePhase = typeof JonesUpdatePhase.Type;

export const JonesUpdateProvenance = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sourceSha: TrimmedNonEmptyString,
  sourceTree: TrimmedNonEmptyString,
  workflow: TrimmedNonEmptyString,
  runId: Schema.Number,
  runAttempt: Schema.Number,
  artifactId: Schema.Number,
  artifactDigest: TrimmedNonEmptyString,
  platform: Schema.Literals(["linux", "darwin"]),
  architecture: Schema.Literals(["x64", "arm64"]),
  version: Schema.optionalKey(TrimmedNonEmptyString),
  payloadSha256: Schema.optionalKey(TrimmedNonEmptyString),
});
export type JonesUpdateProvenance = typeof JonesUpdateProvenance.Type;

export const JonesUpdateCapability = Schema.Struct({
  check: Schema.Boolean,
  download: Schema.Boolean,
  install: Schema.Boolean,
  reason: Schema.optionalKey(
    Schema.Literals([
      "bootstrap-required",
      "source-unqualified",
      "unsupported-platform",
      "native-consent-required",
      "blocked",
    ]),
  ),
});

/** The staged handle is immutable even when a later check discovers another build. */
export const JonesUpdateState = Schema.Struct({
  source: Schema.Literal("jones-actions"),
  channel: Schema.Literal("jones-main"),
  phase: JonesUpdatePhase,
  revision: Schema.optionalKey(Schema.Number),
  capability: JonesUpdateCapability,
  provenance: Schema.optionalKey(JonesUpdateProvenance),
  stagedHandle: Schema.optionalKey(TrimmedNonEmptyString),
  message: Schema.optionalKey(Schema.String),
  checkedAt: Schema.optionalKey(Schema.String),
  environmentId: Schema.optionalKey(EnvironmentId),
  currentVersion: Schema.optionalKey(Schema.String),
});
export type JonesUpdateState = typeof JonesUpdateState.Type;

export const JonesUpdateDownloadInput = Schema.Struct({
  artifactId: Schema.Number,
  sourceSha: TrimmedNonEmptyString,
});
export type JonesUpdateDownloadInput = typeof JonesUpdateDownloadInput.Type;

export const JonesUpdateInstallInput = Schema.Struct({
  stagedHandle: TrimmedNonEmptyString,
  environmentId: EnvironmentId,
  currentVersion: TrimmedNonEmptyString,
  continueRunningThreads: Schema.optionalKey(Schema.Boolean),
});
export type JonesUpdateInstallInput = typeof JonesUpdateInstallInput.Type;
