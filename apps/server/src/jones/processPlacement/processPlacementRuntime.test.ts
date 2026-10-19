import { makeCodexAppServerSpawnCommand } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Shell from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";
import { afterEach, expect, vi } from "vite-plus/test";
import * as ServerConfig from "../../config.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as Placement from "./processPlacement.ts";
import * as PlacementRuntime from "./processPlacementRuntime.ts";

vi.mock("./processPlacement.ts", async (original) => ({
  ...(await original<typeof import("./processPlacement.ts")>()),
  readProcessPlacementBinding: vi.fn(),
  validateProcessPlacementBinding: vi.fn(),
}));
vi.mock("@t3tools/shared/shell", async (original) => ({
  ...(await original<typeof import("@t3tools/shared/shell")>()),
  resolveCommandPath: vi.fn(),
}));
vi.mock("../../process/agentScope.ts", async (original) => ({
  ...(await original<typeof import("../../process/agentScope.ts")>()),
  make: Effect.gen(function* () {
    const management = yield* ProcessRunner.ProcessRunner;
    yield* management
      .run({ command: "/synthetic/control-management", args: [] })
      .pipe(Effect.ignore);
    return {
      ...AgentScope.defaultValue(),
      wrap: ({ command, args }: { command: string; args: ReadonlyArray<string> }) =>
        Effect.succeed({
          command: "/synthetic/systemd-run",
          args: ["--", command, ...args],
          stop: Effect.void,
        }),
    };
  }),
}));
afterEach(() => vi.resetAllMocks());
it.effect(
  "constructs AgentScope with raw management and supplies a placing application runner",
  () => {
    vi.mocked(Placement.readProcessPlacementBinding).mockReturnValue({
      version: 2,
      workloadMode: "systemd-descendant",
      helperPath: "/bound/helper",
      helperSha256: "a".repeat(64),
      helperDevice: "1",
      helperInode: "2",
      control: { path: "/sys/fs/cgroup/app/control", device: "1", inode: "3" },
      workload: { path: "/sys/fs/cgroup/app/workload", device: "1", inode: "4" },
    });
    vi.mocked(Shell.resolveCommandPath).mockImplementation((command) => Effect.succeed(command));
    const calls: Array<{ command: string; args: ReadonlyArray<string> }> = [];
    const raw = ChildProcessSpawner.make((command) => {
      if (command._tag === "StandardCommand") calls.push(command);
      return Effect.fail(
        PlatformError.systemError({
          _tag: "Unknown",
          module: "fixture",
          method: "spawn",
          description: "synthetic spawn boundary",
        }),
      );
    });
    const runtime = PlacementRuntime.layer.pipe(
      Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, raw)),
      Layer.provide(NodeServices.layer),
      Layer.provide(
        ServerConfig.layerTest("/synthetic", { prefix: "placement-composition-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
      ),
      Layer.provide(Layer.succeed(HostProcess.Platform, "linux")),
      Layer.provide(Layer.succeed(HostProcess.Environment, {})),
    );
    return Effect.gen(function* () {
      const runner = yield* ProcessRunner.make();
      yield* runner.run({ command: "/synthetic/git", args: ["status"] }).pipe(Effect.ignore);
      expect(calls).toHaveLength(2);
      expect(calls[0]?.command).toBe("/synthetic/control-management");
      expect(calls[1]?.command).toBe("/synthetic/systemd-run");
      expect(calls[1]?.args).toContain("--verify-descendant");
      expect(calls[1]?.args.slice(-2)).toEqual(["/synthetic/git", "status"]);
      const command = yield* makeCodexAppServerSpawnCommand({
        command: "/synthetic/codex",
        args: ["app-server"],
      });
      yield* (yield* ChildProcessSpawner.ChildProcessSpawner).spawn(command).pipe(Effect.ignore);
      expect(calls).toHaveLength(3);
      expect(calls[2]?.command).toBe("/synthetic/systemd-run");
      expect(calls[2]?.args.filter((arg) => arg === "--verify-descendant")).toHaveLength(1);
      expect(calls[2]?.args.slice(-2)).toEqual(["/synthetic/codex", "app-server"]);
    }).pipe(Effect.provide(Layer.merge(NodeServices.layer, runtime)), Effect.scoped);
  },
);
