import { retainScopedCommand, claimScopedCommand } from "./scopedCommand.ts";
import { it, expect } from "@effect/vitest";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { makeCodexAppServerSpawnCommand } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { layer } from "./processPlacementSpawner.ts";

it.effect("retains the actual Codex scope cleanup without adding an outer scope", () => {
  const issued: string[] = [];
  const stopped: string[] = [];
  const scope = {
    ...AgentScope.defaultValue(),
    wrap: ({ command, args }: { command: string; args: ReadonlyArray<string> }) => {
      const unit = `synthetic-${issued.length}`;
      issued.push(unit);
      return Effect.succeed({
        command: "/synthetic/systemd-run",
        args: [unit, "--", command, ...args],
        stop: Effect.sync(() => {
          stopped.push(unit);
        }),
      });
    },
  };
  let argv: ReadonlyArray<string> = [];
  const raw = ChildProcessSpawner.make((command) => {
    if (command._tag === "StandardCommand") argv = command.args;
    return Effect.die("synthetic boundary: no subprocess");
  });
  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const command = yield* makeCodexAppServerSpawnCommand({
        command: "/synthetic/codex",
        args: ["app-server"],
      });
      yield* (yield* ChildProcessSpawner.ChildProcessSpawner).spawn(command);
    }).pipe(Effect.scoped, Effect.exit);
    expect(issued).toEqual(["synthetic-0"]);
    expect(argv).toContain("synthetic-0");
    expect(argv).not.toContain("synthetic-1");
    expect(stopped).toEqual(["synthetic-0"]);
  }).pipe(
    Effect.provide(
      layer({
        version: 2,
        workloadMode: "systemd-descendant",
        helperPath: "/synthetic/helper",
        helperSha256: "a".repeat(64),
        helperDevice: "1",
        helperInode: "2",
        control: { path: "/sys/fs/cgroup/fake/control", device: "1", inode: "3" },
        workload: { path: "/sys/fs/cgroup/fake/workload", device: "1", inode: "4" },
      }).pipe(
        Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, raw)),
        Layer.provide(Layer.succeed(AgentScope, scope)),
      ),
    ),
    Effect.provideService(AgentScope, scope),
    Effect.provideService(HostProcess.Platform, "linux"),
  );
});

it("rejects owner substitution and duplicate launch claims", () => {
  const owner = AgentScope.defaultValue();
  const other = AgentScope.defaultValue();
  const launch = { command: "/synthetic/scope", args: [], stop: Effect.void };
  const command = retainScopedCommand(ChildProcess.make(launch.command), owner, launch);
  expect(() => claimScopedCommand(command, other)).toThrow("owner changed");
  expect(claimScopedCommand(command, owner)).toBe(launch);
  expect(() => claimScopedCommand(command, owner)).toThrow("already claimed");
});

it("does not reconstruct provenance from copied command bytes or JSON", () => {
  const owner = AgentScope.defaultValue();
  const launch = { command: "/synthetic/scope", args: ["--unit=known"], stop: Effect.void };
  retainScopedCommand(ChildProcess.make(launch.command, launch.args), owner, launch);
  expect(claimScopedCommand(ChildProcess.make(launch.command, launch.args), owner)).toBeUndefined();
  expect(
    claimScopedCommand(JSON.parse(JSON.stringify(ChildProcess.make(launch.command))), owner),
  ).toBeUndefined();
});
