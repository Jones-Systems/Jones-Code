// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - Exercises generated host JavaScript with deterministic ps observations and an isolated in-memory filesystem.
import { describe, expect, it } from "vite-plus/test";
import * as NodePath from "node:path";
import * as NodeVM from "node:vm";
import { remoteDeviceScript } from "../../device/sshDeviceScript.ts";

const home = "/synthetic/device-host";
const owner = "one";
const generation = "generation-one";
const pid = 71234;
const state = NodePath.join(home, ".t3/device/hosts", owner);
const entryPath = NodePath.join(state, `direct-gateway-${generation}.cjs`);
const recordPath = NodePath.join(state, "direct-gateway.json");
const record = { owner, generation, pid, entryPath };
const source = `const config = ${JSON.stringify({ owner, generation })};\n`;
const ownedCommand = `Sl node ${entryPath}\n`;
interface Observation {
  readonly status: number;
  readonly stdout: string;
  readonly error?: Error;
}
const observed = (stdout: string, status = 0): Observation => ({ status, stdout });
const gone = observed("", 1);
type Inspector = (args: readonly string[], index: number) => Observation;

const retire = async (
  observations: readonly Observation[] | Inspector,
  options: {
    readonly script?: string;
    readonly missingScript?: boolean;
    readonly afterSignal?: (files: Map<string, string>) => void;
  } = {},
) => {
  const files = new Map([[recordPath, JSON.stringify(record)]]);
  if (!options.missingScript) files.set(entryPath, options.script ?? source);
  const signals: Array<{ pid: number; signal: string }> = [];
  const queries: Array<readonly string[]> = [];
  const errors: string[] = [];
  const missing = (path: string): never => {
    throw Object.assign(new Error(`No fixture file ${path}`), { code: "ENOENT" });
  };
  const read = (path: string) => files.get(path) ?? missing(path);
  const hostProcess = {
    pid: 12345,
    platform: "linux",
    exitCode: 0,
    kill: (capturedPid: number, signal: string) => {
      signals.push({ pid: capturedPid, signal });
      options.afterSignal?.(files);
      return true;
    },
  };
  const modules: Record<string, unknown> = {
    "node:fs": {
      mkdirSync: () => undefined,
      readFileSync: read,
      existsSync: (path: string) => files.has(path),
      symlinkSync: (value: string, path: string) => files.set(path, value),
      readlinkSync: read,
      unlinkSync: (path: string) => {
        if (!files.delete(path)) missing(path);
      },
    },
    "node:path": NodePath,
    "node:os": { homedir: () => home },
    "node:net": {},
    "node:crypto": { randomUUID: () => "fixture-lock-token" },
    "node:child_process": {
      spawn: () => {
        throw new Error("Retirement must not spawn a helper");
      },
      spawnSync: (command: string, args: readonly string[]) => {
        if (command === "adb") return observed("", 1);
        if (command !== "ps" || !args.includes(String(pid))) {
          throw new Error("Unexpected external command");
        }
        const index = queries.length;
        queries.push(args);
        const result =
          typeof observations === "function" ? observations(args, index) : observations[index];
        if (!result) throw new Error("Unexpected additional PID inspection");
        return result;
      },
    },
  };
  await NodeVM.runInNewContext(
    remoteDeviceScript(owner, "stop-direct", { hostId: "mini", generation }),
    {
      require: (name: string) => {
        if (!(name in modules)) throw new Error(`Unexpected module ${name}`);
        return modules[name];
      },
      process: hostProcess,
      console: { error: (message: string) => errors.push(message) },
      setTimeout,
      clearTimeout,
    },
  );
  return { files, signals, errors, exitCode: hostProcess.exitCode, queries };
};

const expectRetired = (result: Awaited<ReturnType<typeof retire>>) => {
  expect(result.errors).toEqual([]);
  expect(result.exitCode).toBe(0);
  expect(result.files.has(recordPath)).toBe(false);
  expect(result.files.has(entryPath)).toBe(false);
};
const expectRefused = (result: Awaited<ReturnType<typeof retire>>, message: string) => {
  expect(result.errors).toEqual([message]);
  expect(result.exitCode).toBe(1);
  expect(result.files.get(recordPath)).toBe(JSON.stringify(record));
};

describe("captured direct gateway retirement", () => {
  it("stops only the exactly matched live gateway, then confirms absence", async () => {
    const result = await retire([observed(ownedCommand), gone]);
    expectRetired(result);
    expect(result.signals).toEqual([{ pid, signal: "SIGTERM" }]);
  });

  it("does not truncate a live gateway identity to the terminal width", async () => {
    const result = await retire((args, index) =>
      index > 0 ? gone : observed(args.includes("-ww") ? ownedCommand : ownedCommand.slice(0, 80)),
    );
    expectRetired(result);
    expect(result.signals).toEqual([{ pid, signal: "SIGTERM" }]);
  });

  it.each([gone, observed("Z\n")])(
    "confirms exit after a torn stat/command observation",
    async (exited) => {
      const result = await retire([
        observed(ownedCommand),
        observed("Sl [node] <defunct>\n"),
        exited,
      ]);
      expectRetired(result);
      expect(result.signals).toEqual([{ pid, signal: "SIGTERM" }]);
    },
  );

  it("retires a previously exited PID without sending a signal", async () => {
    const result = await retire([observed("R [node] <defunct>\n"), gone]);
    expectRetired(result);
    expect(result.signals).toEqual([]);
  });

  it("refuses a live foreign PID without sending a signal", async () => {
    const result = await retire([observed("Sl node /other-owner/gateway.cjs\n"), observed("Sl\n")]);
    expectRefused(result, "Direct gateway PID identity changed.");
    expect(result.files.get(entryPath)).toBe(source);
    expect(result.signals).toEqual([]);
  });

  it("does not signal again or remove records when identity changes after SIGTERM", async () => {
    const result = await retire([
      observed(ownedCommand),
      observed("R node /other-owner/gateway.cjs\n"),
      observed("R\n"),
    ]);
    expectRefused(result, "Direct gateway PID identity changed.");
    expect(result.files.get(entryPath)).toBe(source);
    expect(result.signals).toEqual([{ pid, signal: "SIGTERM" }]);
  });

  it.each([observed("", 2), { ...gone, error: new Error("ps unavailable") }])(
    "refuses an unavailable initial PID inspection",
    async (unavailable) => {
      const result = await retire([unavailable]);
      expectRefused(result, "Cannot inspect direct gateway PID.");
      expect(result.signals).toEqual([]);
    },
  );

  it.each([observed("", 2), { ...gone, error: new Error("ps unavailable") }])(
    "does not turn an unavailable exit recheck into absence",
    async (unavailable) => {
      const result = await retire([
        observed(ownedCommand),
        observed("R [node] <defunct>\n"),
        unavailable,
      ]);
      expect(result.exitCode).toBe(1);
      expect(result.errors).toHaveLength(1);
      expect(result.files.get(recordPath)).toBe(JSON.stringify(record));
      expect(result.files.get(entryPath)).toBe(source);
      expect(result.signals).toEqual([{ pid, signal: "SIGTERM" }]);
    },
  );

  it("does not accept a multi-row state as confirmation of exit", async () => {
    const result = await retire([observed("R [node] <defunct>\n"), observed("Z\nR\n")]);
    expectRefused(result, "Direct gateway PID identity changed.");
    expect(result.signals).toEqual([]);
  });

  it("requires the original script to authorize a live PID", async () => {
    const result = await retire([observed(ownedCommand), observed("Sl\n")], {
      missingScript: true,
    });
    expectRefused(result, "Direct gateway PID identity changed.");
    expect(result.signals).toEqual([]);
  });

  it("refuses a substituted owner script before any process inspection", async () => {
    const result = await retire([], {
      script: 'const config = {"owner":"other","generation":"generation-one"};\n',
    });
    expectRefused(result, "Unconfirmed direct gateway script.");
    expect(result.queries).toEqual([]);
    expect(result.signals).toEqual([]);
  });

  it("preserves a successor generation published during termination", async () => {
    const replacementEntry = NodePath.join(state, "direct-gateway-generation-next.cjs");
    const replacement = {
      ...record,
      generation: "generation-next",
      entryPath: replacementEntry,
      pid: pid + 1,
    };
    const result = await retire([observed(ownedCommand), gone], {
      afterSignal: (files) => {
        files.set(recordPath, JSON.stringify(replacement));
        files.set(
          replacementEntry,
          'const config = {"owner":"one","generation":"generation-next"};\n',
        );
      },
    });
    expect(result.errors).toEqual([]);
    expect(result.files.get(recordPath)).toBe(JSON.stringify(replacement));
    expect(result.files.get(entryPath)).toBe(source);
    expect(result.files.has(replacementEntry)).toBe(true);
    expect(result.signals).toEqual([{ pid, signal: "SIGTERM" }]);
  });
});
