import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import {
  placementCommand,
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
  platform = HostProcessPlatform.defaultValue(),
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

export const layer = (binding: ProcessPlacementBinding | undefined) =>
  Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.gen(function* () {
      const platform = yield* HostProcessPlatform;
      const underlying = yield* ChildProcessSpawner.ChildProcessSpawner;
      return ChildProcessSpawner.make((command) =>
        Effect.try({
          try: () => placeCommand(command, binding, platform),
          catch: (cause) =>
            PlatformError.systemError({
              _tag: "Unknown",
              module: "ProcessPlacement",
              method: "spawn",
              description: "Required process placement unavailable",
              cause,
            }),
        }).pipe(Effect.flatMap((placed) => underlying.spawn(placed))),
      );
    }),
  );
