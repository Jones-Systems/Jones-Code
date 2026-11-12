import * as Schema from "effect/Schema";

/** The detached helper and both native launchers share this protocol. */
const JONES_ACTIVATION_PROTOCOL = 1;

export const JonesActiveInstall = Schema.Struct({
  protocol: Schema.Literal(JONES_ACTIVATION_PROTOCOL),
  owner: Schema.Literal("desktop"),
  generation: Schema.String,
  transactionId: Schema.String,
  home: Schema.String,
  databasePath: Schema.String,
  profile: Schema.String,
  environmentId: Schema.String,
  appPath: Schema.String,
  executablePath: Schema.String,
  version: Schema.String,
  sourceSha: Schema.String,
  sourceTree: Schema.String,
  appDigest: Schema.optionalKey(Schema.String),
  bundleIdentifier: Schema.optionalKey(Schema.String),
});
export type JonesActiveInstall = typeof JonesActiveInstall.Type;

export const JonesStagedMacApp = Schema.Struct({
  handle: Schema.String,
  receiptPath: Schema.String,
  appPath: Schema.String,
  executablePath: Schema.String,
  version: Schema.String,
  sourceSha: Schema.String,
  sourceTree: Schema.String,
  appDigest: Schema.String,
  bundleIdentifier: Schema.optionalKey(Schema.String),
  asarDigest: Schema.String,
  executableDigest: Schema.String,
  startupGateProtocol: Schema.optionalKey(Schema.Literal(1)),
});
export type JonesStagedMacApp = typeof JonesStagedMacApp.Type;

export const JonesInstallIntent = Schema.Struct({
  protocol: Schema.Literal(JONES_ACTIVATION_PROTOCOL),
  transactionId: Schema.String,
  staged: JonesStagedMacApp,
  expected: JonesActiveInstall,
  continuationReceipt: Schema.String,
});
export type JonesInstallIntent = typeof JonesInstallIntent.Type;

/** Inspect discriminators before the warm schema can discard unknown fields. */
export function requireWarmJonesInstallIntent(value: unknown): void {
  if (typeof value !== "object" || value === null)
    throw new Error("Unknown activation intent; startup held.");
  if ("kind" in value && value.kind === "cold-first-transition")
    throw new Error("Cold receiver requires reconciliation; startup held.");
  if (
    ["kind", "maintenanceGeneration", "handoffSha256", "stateTransaction"].some(
      (key) => key in value,
    )
  )
    throw new Error("Unknown or mixed activation intent; startup held.");
}
