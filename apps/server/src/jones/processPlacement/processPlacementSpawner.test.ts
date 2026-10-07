import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { controlCommand, layer, placeCommand } from "./processPlacementSpawner.ts";
import { placementCommand, type ProcessPlacementBinding } from "./processPlacement.ts";

vi.mock("./processPlacement.ts", async (importOriginal) => {
  const original = await importOriginal<typeof import("./processPlacement.ts")>();
  return { ...original, placementCommand: vi.fn(original.placementCommand) };
});

it("preserves disabled command identity, pipeline options and streams", () => {
  const command = ChildProcess.pipeTo(
    ChildProcess.make("one", ["argument"], {
      cwd: "/workspace",
      env: { KEY: "value" },
      shell: "/bin/sh",
    }),
    ChildProcess.make("two"),
    { from: "stderr" },
  );
  expect(placeCommand(command, undefined)).toBe(command);
});

it.effect("fails required placement before the platform spawner executes a provider payload", () =>
  Effect.gen(function* () {
    let spawned = false;
    const underlying = Layer.succeed(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() =>
        Effect.sync(() => {
          spawned = true;
          throw new Error("payload must not execute");
        }),
      ),
    );
    const binding: ProcessPlacementBinding = {
      version: 1,
      helperPath: "/missing/t3-placement-helper",
      helperSha256: "a".repeat(64),
      helperDevice: "1",
      helperInode: "10",
      control: { path: "/sys/fs/cgroup/missing/control", device: "0", inode: "1" },
      workload: { path: "/sys/fs/cgroup/missing/workload", device: "0", inode: "2" },
    };
    const exit = yield* Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      return yield* spawner.spawn(ChildProcess.make("provider-sentinel"));
    }).pipe(
      Effect.scoped,
      Effect.provide(layer(binding).pipe(Layer.provide(underlying))),
      Effect.provideService(HostProcessPlatform, "linux"),
      Effect.exit,
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(spawned).toBe(false);
  }),
);

it("places both pipeline payloads and preserves options with an explicit telemetry control role", () => {
  const binding: ProcessPlacementBinding = {
    version: 1,
    helperPath: "/bound/helper",
    helperSha256: "a".repeat(64),
    helperDevice: "1",
    helperInode: "10",
    control: { path: "/sys/fs/cgroup/bound/control", device: "1", inode: "2" },
    workload: { path: "/sys/fs/cgroup/bound/workload", device: "1", inode: "3" },
  };
  const fakePlacement = vi.mocked(placementCommand).mockImplementation((command, args, role) => ({
    command: binding.helperPath,
    args: [role!, command, ...args],
  }));
  try {
    const options = { cwd: "/workspace", env: { KEY: "value" }, shell: "/bin/sh" } as const;
    const pipeline = ChildProcess.pipeTo(
      ChildProcess.make("git", ["status"], options),
      controlCommand(ChildProcess.make("telemetry", ["--bounded"])),
      { from: "stderr" },
    );
    const placed = placeCommand(pipeline, binding);
    expect(placed._tag).toBe("PipedCommand");
    if (placed._tag !== "PipedCommand") throw new Error("expected pipeline");
    expect(placed.options).toEqual(pipeline.options);
    expect(placed.left._tag).toBe("StandardCommand");
    if (placed.left._tag !== "StandardCommand" || placed.right._tag !== "StandardCommand")
      throw new Error("expected payload commands");
    expect(placed.left.command).toBe(binding.helperPath);
    expect(placed.left.args).toEqual(["workload", "/bin/sh", "-c", "git status"]);
    expect(placed.left.options).toEqual({ ...options, shell: false });
    expect(placed.right.args).toEqual(["control", "telemetry", "--bounded"]);
  } finally {
    fakePlacement.mockRestore();
  }
});
