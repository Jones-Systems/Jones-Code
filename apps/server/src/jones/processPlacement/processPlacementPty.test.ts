import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { afterEach, expect, vi } from "vite-plus/test";
import * as NodePtyAdapter from "../../terminal/NodePtyAdapter.ts";
import * as Placement from "./processPlacement.ts";
import * as ProcessPlacementPty from "./processPlacementPty.ts";

vi.mock("./processPlacement.ts", async (importOriginal) => {
  const original = await importOriginal<typeof import("./processPlacement.ts")>();
  return { ...original, placementCommand: vi.fn(original.placementCommand) };
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.mocked(Placement.placementCommand).mockReset();
});

const input = {
  shell: "/bin/sh",
  args: ["-l"],
  cwd: "/synthetic/workspace",
  cols: 91,
  rows: 37,
  env: { TERM: "xterm", SYNTHETIC: "value" },
};

function fixture(platform: NodeJS.Platform = "linux") {
  vi.stubEnv("T3_PROCESS_PLACEMENT", undefined);
  vi.stubEnv("T3_PROCESS_PLACEMENT_BOOTSTRAP", undefined);
  const native = {
    pid: 42,
    write: vi.fn(),
    resize: vi.fn(),
    kill: vi.fn(),
    onData: vi.fn(() => ({ dispose: vi.fn() })),
    onExit: vi.fn(() => ({ dispose: vi.fn() })),
  };
  const spawn = vi.fn(() => native);
  const layer = ProcessPlacementPty.layer.pipe(
    Layer.provide(NodePtyAdapter.layer),
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(HostProcess.Platform, platform),
        Layer.succeed(HostProcess.Architecture, "x64"),
        Layer.succeed(NodePtyAdapter.NodePtyModuleLoaderRef, () =>
          Promise.resolve({ spawn } as unknown as typeof import("node-pty")),
        ),
      ),
    ),
  );
  return { native, spawn, layer };
}

it.effect("preserves the unconfigured terminal command and native process controls", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const adapter = yield* PtyAdapter.PtyAdapter;
    const child = yield* adapter.spawn(input);
    expect(f.spawn).toHaveBeenCalledWith(input.shell, input.args, {
      cwd: input.cwd,
      cols: input.cols,
      rows: input.rows,
      env: input.env,
      name: "xterm-256color",
    });
    expect(child.pid).toBe(42);
    child.write("synthetic input");
    child.resize(100, 40);
    expect(f.native.write).toHaveBeenCalledWith("synthetic input");
    expect(f.native.resize).toHaveBeenCalledWith(100, 40);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["invalid-json", "missing-helper", "bootstrap-only"] as const)(
  "refuses %s before spawning a terminal, without falling back",
  (failure) => {
    const f = fixture();
    if (failure === "bootstrap-only") vi.stubEnv("T3_PROCESS_PLACEMENT_BOOTSTRAP", "{}");
    else
      vi.stubEnv(
        "T3_PROCESS_PLACEMENT",
        failure === "invalid-json"
          ? "{"
          : JSON.stringify({
              version: 1,
              helperPath: "/nonexistent-g09/helper",
              helperSha256: "a".repeat(64),
              helperDevice: "1",
              helperInode: "2",
              control: { path: "/sys/fs/cgroup/g09/control", device: "1", inode: "3" },
              workload: { path: "/sys/fs/cgroup/g09/workload", device: "1", inode: "4" },
            }),
      );
    return Effect.gen(function* () {
      const adapter = yield* PtyAdapter.PtyAdapter;
      const result = yield* adapter.spawn(input).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toBeInstanceOf(PtyAdapter.PtySpawnError);
        expect(result.failure.cause).toBeInstanceOf(Placement.ProcessPlacementError);
      }
      expect(f.spawn).not.toHaveBeenCalled();
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("passes the workload helper to node-pty while retaining terminal options", () => {
  const f = fixture();
  // The native boundary is synthetic; helper identity and membership have separate qualification tests.
  vi.mocked(Placement.placementCommand).mockReturnValueOnce({
    command: "/synthetic/helper",
    args: ["--role", "workload", "--", input.shell, ...input.args],
  });
  return Effect.gen(function* () {
    const adapter = yield* PtyAdapter.PtyAdapter;
    yield* adapter.spawn(input);
    expect(Placement.placementCommand).toHaveBeenCalledWith(
      input.shell,
      input.args,
      "workload",
      undefined,
      false,
      "linux",
    );
    expect(f.spawn).toHaveBeenCalledWith(
      "/synthetic/helper",
      ["--role", "workload", "--", input.shell, ...input.args],
      {
        cwd: input.cwd,
        cols: input.cols,
        rows: input.rows,
        env: input.env,
        name: "xterm-256color",
      },
    );
  }).pipe(Effect.provide(f.layer));
});

it.effect("does not treat terminal payload variables as server placement configuration", () => {
  const f = fixture();
  const terminalInput = { ...input, env: { ...input.env, T3_PROCESS_PLACEMENT: "{invalid}" } };
  return Effect.gen(function* () {
    const adapter = yield* PtyAdapter.PtyAdapter;
    yield* adapter.spawn(terminalInput);
    expect(f.spawn).toHaveBeenCalledWith(
      input.shell,
      input.args,
      expect.objectContaining({ env: terminalInput.env }),
    );
  }).pipe(Effect.provide(f.layer));
});
