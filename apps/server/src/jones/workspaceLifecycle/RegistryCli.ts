import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../../processRunner.ts";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { Binding, InventoryEntry, LifecycleError } from "./Protocol.ts";

export class LifecycleRegistry extends Context.Service<
  LifecycleRegistry,
  {
    readonly show: (worktree: string) => Effect.Effect<InventoryEntry, LifecycleError>;
    readonly resume: (
      binding: Binding,
      expectedGeneration: string | null,
    ) => Effect.Effect<string, LifecycleError>;
  }
>()("t3/jones/workspaceLifecycle/RegistryCli/LifecycleRegistry") {}

export interface RegistryCommand {
  readonly executable: string;
  readonly arguments: readonly string[];
  readonly database: string;
  readonly cwd: string;
}
const inventory = Schema.Struct({
  schema: Schema.Literal("workspace-lifecycle-cli/v1"),
  command: Schema.Literal("show"),
  result: Schema.Struct({
    schema: Schema.Literal("workspace-inventory/v1"),
    outcome: Schema.Literals(["ok", "unknown"]),
    results: Schema.Array(InventoryEntry),
    next_after: Schema.Null,
    evidence: Schema.Literal("stored_only"),
    authority_effect: Schema.Literal("none"),
  }),
});
const resumed = Schema.Struct({
  schema: Schema.Literal("workspace-lifecycle-cli/v1"),
  command: Schema.Literal("resume"),
  result: Schema.Struct({
    schema: Schema.Literal("workspace-write/v1"),
    outcome: Schema.Literal("resumed"),
    generation: Schema.NonEmptyString,
    authority_effect: Schema.Literal("none"),
  }),
});
const decodeInventory = Schema.decodeEffect(Schema.fromJsonString(inventory));
const decodeResumed = Schema.decodeEffect(Schema.fromJsonString(resumed));
const decodeBinding = Schema.decodeEffect(Binding, { onExcessProperty: "error" });

// Construction requires an explicitly supplied executable and existing store. There is no discovery, init or migration path.
export const layer = (configuration: RegistryCommand) =>
  Layer.effect(
    LifecycleRegistry,
    Effect.gen(function* () {
      const runner = yield* ProcessRunner.ProcessRunner;
      const path = yield* Path.Path;
      const configured =
        [configuration.executable, configuration.database, configuration.cwd].every(
          (p) =>
            path.isAbsolute(p) &&
            path.normalize(p) === p &&
            [...p].every((c) => c.charCodeAt(0) >= 32),
        ) && configuration.arguments.every((a) => a.length <= 4096 && !a.includes("\0"));
      const run = Effect.fn("LifecycleRegistry.run")(function* (
        args: readonly string[],
        stdin?: string,
      ) {
        if (!configured) return yield* new LifecycleError({ reason: "unavailable" });
        const result = yield* runner
          .run({
            command: configuration.executable,
            args: [...configuration.arguments, "--database", configuration.database, ...args],
            cwd: configuration.cwd,
            stdin,
            timeout: "10 seconds",
            maxOutputBytes: 256 * 1024,
            outputMode: "error",
          })
          .pipe(Effect.mapError((cause) => new LifecycleError({ reason: "unknown", cause })));
        if (
          result.timedOut ||
          result.stdoutTruncated ||
          result.stderrTruncated ||
          result.stdoutInvalidUtf8 ||
          result.stderrInvalidUtf8
        )
          return yield* new LifecycleError({ reason: "unknown" });
        return result;
      });
      return LifecycleRegistry.of({
        show: Effect.fn("LifecycleRegistry.show")(function* (worktree) {
          const result = yield* run(["show", "--worktree", worktree]);
          if (result.code !== 0 && result.code !== 1)
            return yield* new LifecycleError({ reason: "unavailable" });
          const decoded = yield* decodeInventory(result.stdout).pipe(
            Effect.mapError((cause) => new LifecycleError({ reason: "unavailable", cause })),
          );
          const entry = decoded.result.results[0];
          if (
            decoded.result.results.length !== 1 ||
            !entry ||
            entry.worktree !== worktree ||
            (entry.binding !== null && entry.binding.worktree !== worktree)
          )
            return yield* new LifecycleError({ reason: "unavailable" });
          return entry;
        }),
        resume: Effect.fn("LifecycleRegistry.resume")(function* (binding, expectedGeneration) {
          const valid = yield* decodeBinding(binding).pipe(
            Effect.mapError((cause) => new LifecycleError({ reason: "identity", cause })),
          );
          const result = yield* run(
            [
              "resume",
              "--input",
              "-",
              ...(expectedGeneration === null
                ? ["--expect-absent"]
                : ["--expected-generation", expectedGeneration]),
            ],
            canonical(valid),
          );
          if (result.code !== 0)
            return yield* new LifecycleError({
              reason: result.code === 4 ? "conflict" : "unknown",
            });
          const decoded = yield* decodeResumed(result.stdout).pipe(
            Effect.mapError((cause) => new LifecycleError({ reason: "unknown", cause })),
          );
          return decoded.result.generation;
        }),
      });
    }),
  );
