import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const NativeSetupControl = Schema.Struct({
  claimId: Schema.NonEmptyString,
  effectId: Schema.NonEmptyString,
  bootId: Schema.NonEmptyString,
  producerId: Schema.NonEmptyString,
  threadId: Schema.NonEmptyString,
  terminalId: Schema.NonEmptyString,
  generation: Schema.NonEmptyString,
  projectCwd: Schema.NonEmptyString,
  worktreePath: Schema.NonEmptyString,
  definitionDigest: Schema.NonEmptyString,
});
export type NativeSetupControl = typeof NativeSetupControl.Type;

export class NativeSetupControlError extends Schema.TaggedError<NativeSetupControlError>()(
  "NativeSetupControlError",
  { operation: Schema.Literals(["open", "write", "observe"]), message: Schema.String },
) {}
export interface NativeSetupSpawnPlan {
  readonly control: NativeSetupControl;
  readonly shell: string;
  readonly shellArgs: readonly string[];
  readonly cwd: string;
}
export interface NativeSetupSpawnProof extends NativeSetupSpawnPlan {
  readonly pid: number;
}
export interface NativeSetupTerminalHooks {
  readonly control: NativeSetupControl;
  readonly beforeSpawn: (plan: NativeSetupSpawnPlan) => Effect.Effect<void, Error>;
  readonly afterSpawn: (proof: NativeSetupSpawnProof) => Effect.Effect<void, Error>;
}
export interface NativeSetupObservation {
  readonly control: NativeSetupControl;
  readonly status: "running" | "exited" | "unknown";
  readonly pid: number | null;
  readonly writeEntered: boolean;
  readonly exitCode: number | null;
}
