import type { AgentScopeShape } from "@t3tools/shared/AgentScope";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { ChildProcess, ChildProcessSpawner } from "effect/process";
import {
  awaitCapturedScopedRetirement,
  captureScopedCommandRetirement,
  type CapturedScopedRetirement,
} from "../../processPlacement/scopedCommand.ts";
import { verifyCodexProcessExit } from "../../orchestration/LateSteer.ts";

export const CodexCapturedProducerIdentity = Schema.Struct({
  driver: Schema.Literal("codex"),
  instanceId: Schema.NonEmptyString,
  providerSessionId: Schema.NonEmptyString,
  threadId: Schema.NonEmptyString,
  generation: Schema.NonEmptyString,
});
export type CodexCapturedProducerIdentity = typeof CodexCapturedProducerIdentity.Type;
const decodeIdentity = Schema.decodeUnknownEffect(CodexCapturedProducerIdentity, {
  onExcessProperty: "error",
});
const sameIdentity = Schema.toEquivalence(CodexCapturedProducerIdentity);
const lifetimeBrand: unique symbol = Symbol("original-codex-producer-lifetime");
export interface CapturedCodexProducerLifetime {
  readonly [lifetimeBrand]: true;
}
export class CodexCapturedLifetimeError extends Schema.TaggedError<CodexCapturedLifetimeError>()(
  "CodexCapturedLifetimeError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
const isLifetimeError = Schema.is(CodexCapturedLifetimeError);
const error = (cause: unknown) =>
  isLifetimeError(cause)
    ? cause
    : new CodexCapturedLifetimeError({ reason: "captured_codex_lifetime_unknown", cause });
const lifetimes = new WeakMap<
  object,
  {
    readonly identity: CodexCapturedProducerIdentity;
    readonly pid: number;
    readonly process: ChildProcessSpawner.ChildProcessHandle;
    readonly scope: CapturedScopedRetirement;
  }
>();

/** The actual factory calls this immediately after spawn, with the same command,
 * original AgentScope and native child handle. Callers cannot pair a serialized
 * scope name with a process exit from another producer. No stop is dispatched. */
export const captureCodexProducerLifetime = Effect.fn("CodexCapturedLifetime.capture")(
  function* (input: {
    readonly identity: CodexCapturedProducerIdentity;
    readonly command: ChildProcess.Command;
    readonly owner: AgentScopeShape;
    readonly handle: ChildProcessSpawner.ChildProcessHandle;
  }) {
    const identity = yield* decodeIdentity(input.identity);
    const scope = captureScopedCommandRetirement(input.command, input.owner);
    // Missing complete-writer custody is unavailable, including unscoped launches.
    if (scope === undefined) return undefined;
    const pid = Number(input.handle.pid);
    if (!Number.isSafeInteger(pid) || pid <= 0)
      return yield* new CodexCapturedLifetimeError({ reason: "invalid_original_process_identity" });
    const receipt: CapturedCodexProducerLifetime = Object.freeze<CapturedCodexProducerLifetime>({
      [lifetimeBrand]: true,
    });
    lifetimes.set(receipt, {
      identity: Object.freeze({ ...identity }),
      pid,
      process: input.handle,
      scope,
    });
    return receipt;
  },
  Effect.mapError(error),
);

/** Requires both original observations. Scope.close, PID absence, an ACK or a
 * current replacement runtime cannot satisfy either of these captured effects. */
export const awaitCapturedCodexProducerLifetime = Effect.fn(
  "CodexCapturedLifetime.awaitRetirement",
)(function* (receipt: unknown, expected: CodexCapturedProducerIdentity) {
  const identity = yield* decodeIdentity(expected);
  const original =
    typeof receipt === "object" && receipt !== null ? lifetimes.get(receipt) : undefined;
  if (original === undefined || !sameIdentity(original.identity, identity))
    return yield* new CodexCapturedLifetimeError({ reason: "original_producer_identity_mismatch" });
  const [, scope] = yield* Effect.all(
    [verifyCodexProcessExit(original.process), awaitCapturedScopedRetirement(original.scope)],
    { concurrency: 2 },
  );
  return { ...original.identity, pid: original.pid, scopeUnit: scope.unit };
}, Effect.mapError(error));
