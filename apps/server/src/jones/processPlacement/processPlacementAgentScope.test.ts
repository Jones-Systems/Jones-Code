import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { AgentScope, agentScopeCommand, type AgentScopeShape } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Shell from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import { afterEach, expect, vi } from "vite-plus/test";
import * as Placement from "./processPlacement.ts";
import * as PlacementAgentScope from "./processPlacementAgentScope.ts";

vi.mock("./processPlacement.ts", async (original) => ({
  ...(await original<typeof import("./processPlacement.ts")>()),
  validateProcessPlacementBinding: vi.fn(),
}));
vi.mock("@t3tools/shared/shell", async (original) => ({
  ...(await original<typeof import("@t3tools/shared/shell")>()),
  resolveCommandPath: vi.fn(),
}));
afterEach(() => vi.resetAllMocks());
const binding: Placement.ProcessPlacementBinding = {
  version: 2,
  workloadMode: "systemd-descendant",
  helperPath: "/bound/helper",
  helperSha256: "a".repeat(64),
  helperDevice: "1",
  helperInode: "2",
  control: { path: "/sys/fs/cgroup/app/control", device: "1", inode: "3" },
  workload: { path: "/sys/fs/cgroup/app/workload", device: "1", inode: "4" },
};
const input = {
  command: "shell",
  args: ["-l"],
  name: "terminal",
  threadId: "t1",
  survivesRestart: true,
};
function provide(
  underlying: AgentScopeShape,
  selected = binding as Placement.ProcessPlacementBinding | undefined,
) {
  return PlacementAgentScope.layer(selected).pipe(
    Layer.provide(Layer.succeed(AgentScope, underlying)),
    Layer.provide(NodeServices.layer),
    Layer.provide(Layer.succeed(HostProcess.Platform, "linux")),
    Layer.provide(Layer.succeed(HostProcess.Environment, { PATH: "/bound" })),
  );
}
it.effect("passes the entire unconfigured service through unchanged", () => {
  const underlying = AgentScope.defaultValue();
  return Effect.gen(function* () {
    expect(yield* AgentScope).toBe(underlying);
  }).pipe(
    Effect.provide(
      PlacementAgentScope.layer(undefined).pipe(
        Layer.provide(Layer.succeed(AgentScope, underlying)),
        Layer.provide(NodeServices.layer),
      ),
    ),
  );
});
it.effect("puts verification inside the final scope and preserves lifecycle delegation", () => {
  vi.mocked(Shell.resolveCommandPath).mockReturnValue(Effect.succeed("/resolved/shell"));
  const wrap = vi.fn((command: Parameters<AgentScopeShape["wrap"]>[0]) =>
    Effect.succeed(
      agentScopeCommand({
        ...command,
        scope: {
          systemdRun: "/bin/systemd-run",
          slice: "app-t3code-agents.slice",
          unit: "test.scope",
          runtimeDir: "/run/user/1",
          options: [],
        },
      }),
    ),
  );
  const underlying = { ...AgentScope.defaultValue(), wrap };
  return Effect.gen(function* () {
    const scope = yield* AgentScope;
    const result = yield* scope.wrap(input);
    expect(result.args.indexOf("/bin/systemd-run")).toBeLessThan(
      result.args.indexOf(binding.helperPath),
    );
    expect(result.args.slice(-4)).toEqual(["--verify-descendant", "--", "/resolved/shell", "-l"]);
    expect(wrap.mock.calls[0]?.[0]).toMatchObject({ threadId: "t1", survivesRestart: true });
    expect(scope.oomKilled).toBe(underlying.oomKilled);
    expect(scope.clear).toBe(underlying.clear);
  }).pipe(Effect.provide(provide(underlying)));
});
it.effect("preserves unresolved-shell fallback without wrapping a helper", () => {
  vi.mocked(Shell.resolveCommandPath).mockReturnValue(Effect.succeed(null));
  const wrap = vi.fn(AgentScope.defaultValue().wrap);
  return Effect.gen(function* () {
    expect(yield* (yield* AgentScope).wrap(input)).toEqual({
      command: input.command,
      args: input.args,
    });
    expect(wrap).not.toHaveBeenCalled();
  }).pipe(Effect.provide(provide({ ...AgentScope.defaultValue(), wrap })));
});
it.effect.each([false, true])("fails closed for unscoped service (OOM wrapper=%s)", (unscoped) => {
  vi.mocked(Shell.resolveCommandPath).mockReturnValue(Effect.succeed("/resolved/shell"));
  const underlying = {
    ...AgentScope.defaultValue(),
    wrap: (command: Parameters<AgentScopeShape["wrap"]>[0]) =>
      Effect.succeed(
        unscoped ? agentScopeCommand(command) : { command: command.command, args: command.args },
      ),
  };
  return Effect.gen(function* () {
    expect(Exit.isFailure(yield* (yield* AgentScope).wrap(input).pipe(Effect.exit))).toBe(true);
  }).pipe(Effect.provide(provide(underlying)));
});
