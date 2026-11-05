import { resolveCommandPath, resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";

import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";

export interface ProbedOpenCode {
  readonly generation: "v1" | "v2";
  readonly version: string;
}

// Admission policy, not a background polling interval. Indirect wrappers need the age bound too.
const MAX_PROTOCOL_AGE_NANOS = 60_000_000_000n;

export const readOpenCodeProtocolSource = Effect.fn("OpenCodeProtocolFreshness.readSource")(
  function* (
    settings: { readonly binaryPath: string; readonly serverUrl: string },
    environment?: NodeJS.ProcessEnv,
  ) {
    // External ownership never consults the configured local executable, even for a fingerprint.
    if (settings.serverUrl.trim().length > 0) return "external";
    const fileSystem = yield* FileSystem.FileSystem;
    const command = yield* resolveSpawnCommand(settings.binaryPath, ["--version"], {
      ...(environment === undefined ? {} : { env: environment }),
      bypassNpmShim: true,
    });
    if (command.shell) return JSON.stringify(["shell", command.command, command.args]);
    // Reuse command lookup rather than walking PATH trees or reading executable contents.
    const executable = yield* resolveCommandPath(command.command, {
      ...(environment === undefined ? {} : { env: environment }),
    });
    const target =
      executable === null
        ? undefined
        : yield* fileSystem.realPath(executable).pipe(Effect.orElseSucceed(() => undefined));
    const info =
      target === undefined
        ? undefined
        : yield* fileSystem.stat(target).pipe(Effect.orElseSucceed(() => undefined));
    return JSON.stringify([
      command.command,
      command.args,
      executable,
      target,
      info === undefined
        ? null
        : [
            info.type,
            info.dev,
            Option.getOrNull(info.ino),
            info.mode,
            String(info.size),
            Option.getOrNull(Option.map(info.mtime, (time) => time.getTime())),
            Option.getOrNull(Option.map(info.birthtime, (time) => time.getTime())),
          ],
    ]);
  },
);

const changedDuringProbe = () =>
  new OpenCodeRuntime.OpenCodeRuntimeError({
    operation: "probeOpenCodeRuntime",
    detail: "The OpenCode protocol source changed while checking its version. Try the admission again.",
  });

export const makeOpenCodeProtocolFreshness = <E>(
  probe: Effect.Effect<ProbedOpenCode, E>,
  readSource: Effect.Effect<string>,
) =>
  Effect.sync(() => {
    type Failure = E | OpenCodeRuntime.OpenCodeRuntimeError;
    interface Epoch {
      readonly source: string | undefined;
      success?: { readonly result: ProbedOpenCode; readonly startedAt: bigint };
      flight?: {
        readonly refresh: boolean;
        readonly done: Deferred.Deferred<ProbedOpenCode, Failure>;
      };
    }
    let current: Epoch = { source: undefined };
    let lastSuccess = Option.none<ProbedOpenCode>();

    const read = (refresh: boolean): Effect.Effect<ProbedOpenCode, Failure> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const beforeRead = current;
          const source = yield* restore(readSource);
          // A slow identity read cannot reinstate a source an intervening admission replaced.
          if (current !== beforeRead && current.source !== source) {
            return yield* changedDuringProbe();
          }
          if (current.source !== source || (refresh && !current.flight?.refresh)) {
            current = { source };
          }
          const epoch = current;
          const now = yield* Clock.monotonicTimeNanos;
          if (current !== epoch) return yield* changedDuringProbe();
          if (!refresh && epoch.success !== undefined) {
            const age = now - epoch.success.startedAt;
            if (age >= 0n && age < MAX_PROTOCOL_AGE_NANOS) return epoch.success.result;
          }
          if (epoch.flight !== undefined) return yield* restore(Deferred.await(epoch.flight.done));

          const flight = {
            refresh,
            done: Deferred.makeUnsafe<ProbedOpenCode, Failure>(),
          };
          epoch.flight = flight;
          let exit = yield* Effect.exit(
            restore(
              Effect.gen(function* () {
                const result = yield* probe;
                const afterSource = yield* readSource;
                if (current !== epoch || afterSource !== source) {
                  if (current === epoch) current = { source: afterSource };
                  return yield* changedDuringProbe();
                }
                return result;
              }),
            ),
          );
          if (current !== epoch && Exit.isSuccess(exit)) exit = Exit.fail(changedDuringProbe());
          if (current === epoch && Exit.isSuccess(exit)) {
            epoch.success = { result: exit.value, startedAt: now };
            lastSuccess = Option.some(exit.value);
          }
          // Completion is masked even when the caller was interrupted; waiters cannot be stranded.
          delete epoch.flight;
          yield* Deferred.done(flight.done, exit);
          return yield* exit;
        }),
      );

    return {
      get: read(false),
      refresh: read(true),
      lastSuccess: Effect.sync(() => lastSuccess),
    };
  });
