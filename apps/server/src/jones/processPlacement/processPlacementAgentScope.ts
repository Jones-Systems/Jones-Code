import { AgentScope, agentScopeCommand, type AgentScopeShape } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveCommandPath } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import {
  placementArguments,
  ProcessPlacementError,
  type ProcessPlacementBinding,
  validateProcessPlacementBinding,
} from "./processPlacement.ts";

export const layer = (binding: ProcessPlacementBinding | undefined) =>
  Layer.effect(
    AgentScope,
    Effect.gen(function* () {
      const underlying = yield* AgentScope;
      if (binding === undefined) return underlying;
      const platform = yield* HostProcess.Platform;
      const environment = yield* HostProcess.Environment;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      return {
        ...underlying,
        wrap: Effect.fn("ProcessPlacementAgentScope.wrap")(function* (
          input: Parameters<AgentScopeShape["wrap"]>[0],
        ) {
          yield* Effect.sync(() => {
            if (binding.version !== 2)
              throw new ProcessPlacementError("AgentScope integration requires binding v2");
            validateProcessPlacementBinding(binding, platform);
          });
          const resolved = yield* resolveCommandPath(input.command, {
            env: input.env ?? environment,
          }).pipe(
            Effect.provideService(FileSystem.FileSystem, fileSystem),
            Effect.provideService(Path.Path, path),
            Effect.provideService(HostProcess.Platform, platform),
          );
          // Preserve the terminal manager's next-shell fallback without starting a payload.
          if (resolved === null) return { command: input.command, args: input.args };
          const args = [...placementArguments(binding, "workload"), resolved, ...input.args];
          const scoped = yield* underlying.wrap({ ...input, command: binding.helperPath, args });
          const unscoped = agentScopeCommand({ command: binding.helperPath, args });
          const same = (command: string, expectedArgs: ReadonlyArray<string>) =>
            scoped.command === command &&
            scoped.args.length === expectedArgs.length &&
            scoped.args.every((value, index) => value === expectedArgs[index]);
          if (same(binding.helperPath, args) || same(unscoped.command, unscoped.args))
            return yield* Effect.die(
              new ProcessPlacementError("systemd agent scopes are required"),
            );
          return scoped;
        }),
      } satisfies typeof underlying;
    }),
  );
