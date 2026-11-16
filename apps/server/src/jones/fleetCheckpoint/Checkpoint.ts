import {
  FleetCheckpointInput,
  type FleetCheckpointResult,
} from "@t3tools/contracts/jones/fleetCheckpoint";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../../processRunner.ts";
import { Configuration } from "./Configuration.ts";
import { observe, receiptBinding, validateRequest } from "./Evidence.ts";
import { canonical, decode, digest, document } from "./Json.ts";

export class CheckpointError extends Schema.TaggedError<
  CheckpointError,
  { readonly brand: unique symbol }
>()("CheckpointError", {
  reason: Schema.Literals([
    "unavailable",
    "invalid-request",
    "binding-mismatch",
    "cancellation-held",
  ]),
}) {}
export interface Caller {
  readonly environmentId: string;
  readonly threadId: string;
}
export type Action = "place" | "get" | "cancel-pending";
export class Checkpoint extends Context.Service<
  Checkpoint,
  {
    readonly execute: (
      action: Action,
      caller: Caller,
      input: unknown,
    ) => Effect.Effect<FleetCheckpointResult, CheckpointError>;
  }
>()("t3/jones/fleetCheckpoint/Checkpoint") {}

const isConfiguration = Schema.is(Configuration);
const decodeInput = Schema.decodeUnknownEffect(FleetCheckpointInput, { onExcessProperty: "error" });

export const layer = (configuration: Configuration) =>
  Layer.effect(
    Checkpoint,
    Effect.gen(function* () {
      const runner = yield* ProcessRunner.ProcessRunner;
      const path = yield* Path.Path;
      const configured =
        isConfiguration(configuration) &&
        [configuration.executable, configuration.binding, configuration.cwd].every(
          (p) => path.isAbsolute(p) && path.normalize(p) === p,
        ) &&
        configuration.arguments.every((a) => !a.includes("\0"));
      return Checkpoint.of({
        execute: Effect.fn("FleetCheckpoint.execute")(function* (action, caller, raw) {
          if (!configured) return yield* new CheckpointError({ reason: "unavailable" });
          const input: FleetCheckpointInput = yield* decodeInput(raw).pipe(
            Effect.mapError(() => new CheckpointError({ reason: "invalid-request" })),
          );
          yield* Effect.try({
            try: () => validateRequest(input),
            catch: () => new CheckpointError({ reason: "invalid-request" }),
          });
          if (
            input.request.work_key.controller_id !== configuration.controllerId ||
            !caller.environmentId ||
            !caller.threadId
          )
            return yield* new CheckpointError({ reason: "invalid-request" });
          const workKey = {
            ...input.request.work_key,
            work_id:
              "jones-" + digest([caller.environmentId, caller.threadId, input.originalRequestId]),
          };
          const request = { ...input.request, work_key: workKey };
          const expected = { input, workKey, requestDigest: digest(request) };
          const base: FleetCheckpointResult = {
            schema: "jones.fleet-checkpoint-result/v1",
            ...caller,
            originalRequestId: input.originalRequestId,
            workKey,
            expectedRequestDigest: expected.requestDigest,
            transport: "unknown",
            bindingValidation: "unavailable",
            verificationOutcome: "unknown",
            resourceClosure: "unknown",
            fleet: null,
            cancellation: null,
            cost: null,
            artifacts: [],
          };
          const deadline = (yield* Clock.currentTimeMillis) + (input.timeoutMs ?? 10000);
          const run = Effect.fn("FleetCheckpoint.command")(function* (verb: string, body: unknown) {
            const remaining = deadline - (yield* Clock.currentTimeMillis);
            if (remaining <= 0) return null;
            const output = yield* runner
              .run({
                command: configuration.executable,
                args: [...configuration.arguments, verb, "--binding", configuration.binding],
                cwd: configuration.cwd,
                stdin: canonical(body) + "\n",
                timeout: remaining,
                maxOutputBytes: 256 * 1024,
                outputMode: "error",
                timeoutBehavior: "timedOutResult",
              })
              .pipe(Effect.option);
            if (output._tag === "None") return null;
            const o = output.value;
            if (
              o.code !== 0 ||
              o.timedOut ||
              o.stdoutTruncated ||
              o.stderrTruncated ||
              o.stdoutInvalidUtf8 ||
              o.stderrInvalidUtf8
            )
              return null;
            return yield* Effect.try({
              try: () => decode(o.stdout),
              catch: () => new CheckpointError({ reason: "binding-mismatch" }),
            });
          });
          const fleet = yield* run(
            action === "place" ? "place-checkpoint" : "get-checkpoint",
            action === "place"
              ? request
              : { schema: "fleet-ops.checkpoint-lookup/v1", work_key: workKey },
          );
          if (fleet === null) return base;
          const observed = yield* Effect.try({
            try: () => observe(fleet, expected),
            catch: () => new CheckpointError({ reason: "binding-mismatch" }),
          });
          const result: FleetCheckpointResult = {
            ...base,
            transport: "observed",
            bindingValidation: observed.outcome === "unknown" ? "unavailable" : "bound",
            fleet,
            verificationOutcome: observed.outcome,
            cost: observed.cost,
            artifacts: observed.artifacts,
          };
          if (action !== "cancel-pending") return result;
          const selection = fleet.selection === null ? null : document(fleet.selection);
          const receipt = fleet.original_result === null ? null : document(fleet.original_result);
          if (
            selection === null ||
            receipt?.schema !== "fleet-ops.receipt/v1" ||
            receipt.dispatch_knowledge !== "proven-not-dispatched" ||
            !["queued", "held", "cancelled-before-dispatch"].includes(String(receipt.state))
          )
            return yield* new CheckpointError({ reason: "cancellation-held" });
          const cancellation = yield* run("cancel-pending", selection.submission);
          if (cancellation === null) return { ...result, transport: "unknown" };
          yield* Effect.try({
            try: () => {
              const cancelled = receiptBinding(cancellation, selection.submission);
              if (
                cancelled.state !== "cancelled-before-dispatch" ||
                cancelled.dispatch_knowledge !== "proven-not-dispatched"
              )
                throw new TypeError("Cancellation not proven.");
            },
            catch: () => new CheckpointError({ reason: "binding-mismatch" }),
          });
          return { ...result, cancellation };
        }),
      });
    }),
  );
