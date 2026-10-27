import * as Schema from "effect/Schema";

/**
 * What the desktop's own background preparation is doing. Install stays a manual action in
 * every phase; `pinned` means an accepted install holds the staged build until it settles.
 */
export const JonesDesktopPreparationPhase = Schema.Literals([
  "off",
  "unavailable",
  "blocked",
  "pinned",
  "preparing",
  "watching",
  "settling",
  "backoff",
  "ready",
]);
export type JonesDesktopPreparationPhase = typeof JonesDesktopPreparationPhase.Type;

export const JonesDesktopPreparationState = Schema.Struct({
  enabled: Schema.Boolean,
  phase: JonesDesktopPreparationPhase,
  /** Version of the build being settled, prepared or waited on. */
  targetVersion: Schema.optionalKey(Schema.String),
  /** Unused download that preparation is replacing. */
  replacingVersion: Schema.optionalKey(Schema.String),
  /** ISO time when settling ends or a failed preparation retries. */
  nextAt: Schema.optionalKey(Schema.String),
  reason: Schema.optionalKey(Schema.String),
});
export type JonesDesktopPreparationState = typeof JonesDesktopPreparationState.Type;
