import * as Schema from "effect/Schema";

/**
 * A step key the server declares. Clients render the server's steps and counts; they never
 * hardcode a catalog. An incompatible change uses a new state key instead of `schema: 2`,
 * because current clients decode this record strictly.
 */
const JonesUpdateProgressKey = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,47}$/));
const JonesUpdateProgressLabel = Schema.String.check(Schema.isMaxLength(80));
const JonesUpdateProgressCode = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,63}$/));

export const JonesUpdateProgressStep = Schema.Struct({
  key: JonesUpdateProgressKey,
  label: JonesUpdateProgressLabel,
  startedAt: Schema.optionalKey(Schema.String),
  endedAt: Schema.optionalKey(Schema.String),
});
export type JonesUpdateProgressStep = typeof JonesUpdateProgressStep.Type;

/** The structured record of one download or install attempt on the host. */
export const JonesUpdateProgress = Schema.Struct({
  schema: Schema.Literal(1),
  kind: Schema.Literals(["download", "install"]),
  operationId: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(128))),
  phase: Schema.Literals(["prep", "download", "install", "verify", "done", "failed"]),
  /** One-based position of `stepKey` in the server's step list. */
  stepIndex: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 12 })),
  stepCount: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 12 })),
  stepKey: JonesUpdateProgressKey,
  stepLabel: JonesUpdateProgressLabel,
  stepStartedAt: Schema.String,
  startedAt: Schema.String,
  /** When the record last changed. */
  updatedAt: Schema.String,
  /** The host clock when this state was served; compare with `updatedAt` to cancel clock skew. */
  reportedAt: Schema.String,
  steps: Schema.optionalKey(Schema.Array(JonesUpdateProgressStep).check(Schema.isMaxLength(12))),
  error: Schema.optionalKey(
    Schema.Struct({
      code: JonesUpdateProgressCode,
      message: Schema.String.check(Schema.isMaxLength(240)),
    }),
  ),
});
export type JonesUpdateProgress = typeof JonesUpdateProgress.Type;

/** When the host last ran a check, and how it ended. The reason is sanitized and bounded. */
export const JonesUpdateLastCheck = Schema.Struct({
  at: Schema.String,
  outcome: Schema.Literals(["succeeded", "failed"]),
  reason: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(240))),
});
export type JonesUpdateLastCheck = typeof JonesUpdateLastCheck.Type;
