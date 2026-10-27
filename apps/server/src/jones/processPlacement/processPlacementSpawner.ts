import { claimScopedCommand } from "./scopedCommand.ts";
import { AgentScope, type AgentScopeShape } from "@t3tools/shared/AgentScope";
import type * as Scope from "effect/Scope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import {
  placementCommand,
  ProcessPlacementError,
  type ProcessPlacementBinding,
  type ProcessRole,
} from "./processPlacement.ts";

const roles = new WeakMap<ChildProcess.Command, ProcessRole>();
export function controlCommand<T extends ChildProcess.Command>(command: T): T {
  roles.set(command, "control");
  return command;
}

export function placeCommand(
  command: ChildProcess.Command,
  binding: ProcessPlacementBinding | undefined,
  platform = HostProcess.Platform.defaultValue(),
): ChildProcess.Command {
  if (binding === undefined) return command;
  if (command._tag === "PipedCommand") {
    return ChildProcess.pipeTo(
      placeCommand(command.left, binding, platform),
      placeCommand(command.right, binding, platform),
      command.options,
    );
  }
  const shell = command.options.shell;
  const payload = shell
    ? {
        command: typeof shell === "string" ? shell : "/bin/sh",
        args: ["-c", [command.command, ...command.args].join(" ")],
      }
    : { command: command.command, args: command.args };
  const placed = placementCommand(
    payload.command,
    payload.args,
    roles.get(command) ?? "workload",
    binding,
    false,
    platform,
  );
  return ChildProcess.make(placed.command, placed.args, { ...command.options, shell: false });
}

const placeScopedCommand: (
  command: ChildProcess.Command,
  binding: ProcessPlacementBinding,
  scope: AgentScopeShape,
  platform: NodeJS.Platform,
) => Effect.Effect<ChildProcess.Command, never, Scope.Scope> = Effect.fn(
  "processPlacementSpawner.placeScopedCommand",
)(function* (
  command: ChildProcess.Command,
  binding: ProcessPlacementBinding,
  scope: AgentScopeShape,
  platform: NodeJS.Platform,
): Effect.fn.Return<ChildProcess.Command, never, Scope.Scope> {
  if (command._tag === "PipedCommand") {
    return ChildProcess.pipeTo(
      yield* placeScopedCommand(command.left, binding, scope, platform),
      yield* placeScopedCommand(command.right, binding, scope, platform),
      command.options,
    );
  }
  if (roles.get(command) === "control")
    return yield* Effect.sync(() => placeCommand(command, binding, platform));
  const retained = yield* Effect.sync(() => claimScopedCommand(command, scope));
  if (retained !== undefined) {
    if (retained.stop === undefined)
      return yield* Effect.die(new ProcessPlacementError("scoped workload lifetime is required"));
    yield* Effect.addFinalizer(() => retained.stop!.pipe(Effect.ignoreCause({ log: true })));
    return command;
  }
  const shell = command.options.shell;
  const payload = shell
    ? {
        command: typeof shell === "string" ? shell : "/bin/sh",
        args: ["-c", [command.command, ...command.args].join(" ")],
      }
    : { command: command.command, args: command.args };
  const placed = yield* scope.wrap({ ...payload, name: "workload", env: command.options.env });
  if (placed.stop === undefined)
    return yield* Effect.die(new ProcessPlacementError("scoped workload lifetime is required"));
  yield* Effect.addFinalizer(() => placed.stop!.pipe(Effect.ignoreCause({ log: true })));
  return ChildProcess.make(placed.command, placed.args, { ...command.options, shell: false });
});

export const layer = (binding: ProcessPlacementBinding | undefined) =>
  Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.gen(function* () {
      const platform = yield* HostProcess.Platform;
      const underlying = yield* ChildProcessSpawner.ChildProcessSpawner;
      const scope = yield* AgentScope;
      return ChildProcessSpawner.make((command) =>
        (binding?.version === 2
          ? placeScopedCommand(command, binding, scope, platform)
          : Effect.try({
              try: () => placeCommand(command, binding, platform),
              catch: (cause) =>
                PlatformError.systemError({
                  _tag: "Unknown",
                  module: "ProcessPlacement",
                  method: "spawn",
                  description: "Required process placement unavailable",
                  cause,
                }),
            })
        ).pipe(Effect.flatMap((placed) => underlying.spawn(placed))),
      );
    }),
  );
