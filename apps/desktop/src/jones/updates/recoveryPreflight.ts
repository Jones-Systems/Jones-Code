// @effect-diagnostics globalDate:off - Native caller supplies the timestamp used for opaque proof expiry.
import * as Schema from "effect/Schema";
import {
  JonesRecoveryPreflight,
  type JonesRecoveryPreflightInput,
  type JonesRecoveryPolicy,
} from "@t3tools/contracts/jones/jonesOperations";

export const NativeRecoveryMeasurement = Schema.Struct({
  protocol: Schema.Literal(1),
  firstInstall: Schema.Boolean,
  stateBytes: Schema.Number,
  candidateBytes: Schema.Number,
  volumes: JonesRecoveryPreflight.fields.volumes,
  clone: JonesRecoveryPreflight.fields.clone,
});
export type NativeRecoveryMeasurement = typeof NativeRecoveryMeasurement.Type;

export class RecoveryPreflightRefused extends Error {
  readonly code: "recovery-preflight-stale" | "recovery-unsupported" | "recovery-capacity";

  constructor(
    code: "recovery-preflight-stale" | "recovery-unsupported" | "recovery-capacity",
    message: string,
  ) {
    super(message);
    this.code = code;
  }
}

export type RecoveryReview = { readonly policy: JonesRecoveryPolicy; readonly preflightId: string };
export type NativeRecoveryBinding = {
  readonly input: JonesRecoveryPreflightInput;
  readonly nativeIdentity: string;
};
const same = (a: NativeRecoveryBinding, b: NativeRecoveryBinding) =>
  a.nativeIdentity === b.nativeIdentity &&
  a.input.environmentId === b.input.environmentId &&
  a.input.stagedHandle === b.input.stagedHandle &&
  a.input.currentVersion === b.input.currentVersion &&
  a.input.expectedInstalledSource === b.input.expectedInstalledSource;

/** Native memory only: a renderer cannot supply measurements or mint policy authority. */
export class NativeRecoveryPreflights {
  #proof: { binding: NativeRecoveryBinding; value: JonesRecoveryPreflight } | undefined;

  record(
    binding: NativeRecoveryBinding,
    raw: unknown,
    now: number,
    id: string,
  ): JonesRecoveryPreflight {
    const measurement = Schema.decodeUnknownSync(NativeRecoveryMeasurement)(raw);
    if (
      !Number.isFinite(now) ||
      !Number.isSafeInteger(measurement.stateBytes) ||
      measurement.stateBytes < 0 ||
      !Number.isSafeInteger(measurement.candidateBytes) ||
      measurement.candidateBytes < 0 ||
      measurement.volumes.length < 1 ||
      measurement.volumes.some((v) =>
        [v.availableBytes, v.copyRequiredBytes, v.cloneRequiredBytes].some(
          (n) => !Number.isSafeInteger(n) || n < 0,
        ),
      ) ||
      (measurement.firstInstall &&
        (measurement.stateBytes !== 0 || measurement.clone.status === "proved"))
    )
      throw new RecoveryPreflightRefused(
        "recovery-unsupported",
        "Native recovery measurement was not proved.",
      );
    const copy = measurement.volumes.every((v) => v.availableBytes >= v.copyRequiredBytes);
    const clone =
      !measurement.firstInstall &&
      measurement.clone.status === "proved" &&
      measurement.volumes.every((v) => v.availableBytes >= v.cloneRequiredBytes);
    const value = Schema.decodeSync(JonesRecoveryPreflight)({
      ...measurement,
      preflightId: id,
      measuredAt: new Date(now).toISOString(),
      expiresAt: new Date(now + 300_000).toISOString(),
      binding: binding.input,
      policies: {
        cloneOrCopy: {
          admissible: copy,
          ...(copy
            ? {}
            : { reason: "Not enough space for a full backup copy and the candidate app." }),
        },
        cloneRequired: {
          admissible: clone,
          ...(clone
            ? {}
            : {
                reason: measurement.firstInstall
                  ? "No existing state to back up; no clone choice is available."
                  : measurement.clone.status !== "proved"
                    ? "Strict recovery cloning was not proved. Stop; free space before retrying."
                    : "Not enough recovery headroom and candidate app capacity. Stop; free space before retrying.",
              }),
        },
      },
    });
    this.#proof = { binding: structuredClone(binding), value: structuredClone(value) };
    return value;
  }

  require(
    review: RecoveryReview,
    binding: NativeRecoveryBinding,
    now: number,
  ): JonesRecoveryPolicy {
    const proof = this.#proof;
    if (
      proof === undefined ||
      proof.value.preflightId !== review.preflightId ||
      !same(proof.binding, binding) ||
      !Number.isFinite(now) ||
      now < Date.parse(proof.value.measuredAt) ||
      now >= Date.parse(proof.value.expiresAt)
    )
      throw new RecoveryPreflightRefused(
        "recovery-preflight-stale",
        "Recovery preflight expired or the native environment, version, source, generation or selection changed. Review again.",
      );
    if (review.policy !== "clone-required" && review.policy !== "clone-or-copy")
      throw new RecoveryPreflightRefused(
        "recovery-unsupported",
        "Unknown native recovery policy. Stop; review again.",
      );
    const policy =
      review.policy === "clone-required"
        ? proof.value.policies.cloneRequired
        : proof.value.policies.cloneOrCopy;
    if (!policy.admissible)
      throw new RecoveryPreflightRefused(
        review.policy === "clone-required" && proof.value.clone.status !== "proved"
          ? "recovery-unsupported"
          : "recovery-capacity",
        policy.reason ?? "Recovery policy is not admissible. Stop.",
      );
    return review.policy;
  }
}
