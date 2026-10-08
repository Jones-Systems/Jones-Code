// @effect-diagnostics nodeBuiltinImport:off - Fixtures inspect exact process cleanup and filesystem change receipts.
import * as NodeURL from "node:url";
import * as NodeTimersPromises from "node:timers/promises";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { it as effectIt } from "@effect/vitest";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { DecisionSnapshotNativeCounts, DecisionSnapshotToolkitHandlersLive } from "./handlers.ts";
import { DecisionSnapshotToolkit } from "./tools.ts";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  type CollectorBinding,
  monotonicSeconds,
  runBoundCollector,
  makeCollector,
  DecisionSnapshotCollector,
} from "./collector.ts";

const filesystemDelay = vi.hoisted(() => ({ path: null as string | null }));
const decodeUsageFixtureState = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      operation: Schema.optional(Schema.Unknown),
      retry_not_before: Schema.String,
    }),
  ),
);
vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeFSP>();
  const timers = await import("node:timers/promises");
  return {
    ...actual,
    realpath: async (...args: Parameters<typeof NodeFSP.realpath>) => {
      if (args[0] === filesystemDelay.path) await timers.setTimeout(200);
      return actual.realpath(...args);
    },
  };
});

async function closeOwnedFixtureGroups(owner: string, directory: string) {
  const trace = await NodeFSP.readFile(NodePath.join(owner, "trace.fixture"), "utf8").catch(
    (cause: unknown) => {
      if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return "";
      throw cause;
    },
  );
  const groups = new Map<number, string>();
  for (const line of trace.trim().split("\n").filter(Boolean)) {
    const entry = JSON.parse(line);
    if (entry.event === "probe" || entry.event === "usage_start")
      groups.set(entry.pgid, entry.start_ticks);
  }
  const exists = (pgid: number) => {
    try {
      process.kill(-pgid, 0);
      return true;
    } catch (cause) {
      if (cause instanceof Error && "code" in cause && cause.code === "ESRCH") return false;
      throw cause;
    }
  };
  for (const [pgid, startTicks] of groups) {
    if (!exists(pgid)) continue;
    const verify = async () => {
      const processDirectory = `/proc/${pgid}`;
      const evidence = await Promise.all([
        NodeFSP.readFile(`${processDirectory}/stat`, "utf8"),
        NodeFSP.readFile(`${processDirectory}/cmdline`, "utf8"),
        NodeFSP.stat(processDirectory),
      ]).catch((cause: unknown) => {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT" && !exists(pgid))
          return null;
        throw cause;
      });
      if (evidence === null) return false;
      const [stat, command, metadata] = evidence;
      const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
      expect(Number(fields[2])).toBe(pgid);
      expect(fields[19]).toBe(startTicks);
      expect(metadata.uid).toBe(process.getuid!());
      expect(command.split("\0").some((argument) => argument.startsWith(`${directory}/`))).toBe(
        true,
      );
      return true;
    };
    if (!(await verify())) continue;
    process.kill(-pgid, "SIGTERM");
    const grace = monotonicSeconds() + 1.5;
    while (exists(pgid) && monotonicSeconds() < grace) await NodeTimersPromises.setTimeout(25);
    if (exists(pgid) && (await verify())) process.kill(-pgid, "SIGKILL");
    const reap = monotonicSeconds() + 1;
    while (exists(pgid) && monotonicSeconds() < reap) await NodeTimersPromises.setTimeout(25);
    expect(exists(pgid), "owned fixture group must close before scratch removal").toBe(false);
  }
}

async function release(
  script: string,
  check: (binding: CollectorBinding) => Promise<void>,
  modules: Record<string, string> | ((directory: string) => Record<string, string>) = {},
  sourceCommit: string | null = null,
  scratchParent = NodePath.join(
    NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
    "fixtures",
  ),
) {
  const owner = await NodeFSP.mkdtemp(NodePath.join(scratchParent, "decision-snapshot-fixture-"));
  const releaseId = "1".repeat(64);
  const directory = NodePath.join(owner, releaseId);
  try {
    await NodeFSP.mkdir(directory);
    const source = {
      "tools/decision-snapshot.py": script,
      ...(typeof modules === "function" ? modules(directory) : modules),
    };
    const files = [];
    for (const [path, content] of Object.entries(source)) {
      await NodeFSP.mkdir(NodePath.dirname(NodePath.join(directory, path)), { recursive: true });
      await NodeFSP.writeFile(NodePath.join(directory, path), content);
      files.push({
        path,
        size_bytes: Buffer.byteLength(content),
        sha256: NodeCrypto.createHash("sha256").update(content).digest("hex"),
        provenance:
          sourceCommit !== null &&
          [
            "tools/decision-snapshot.py",
            "src/codex_v3/decision_snapshot.py",
            "fixture-source/claude-account-usage.py",
          ].includes(path)
            ? {
                kind: "source",
                repository: "Jones-Systems/Codex-V3",
                commit: sourceCommit,
                path:
                  path === "fixture-source/claude-account-usage.py"
                    ? "tools/claude-account-usage.py"
                    : path,
              }
            : { kind: "fixture", fixture_id: "native-decision-snapshot", path },
      });
    }
    const descriptor = JSON.stringify({
      schema: "codex.decision-snapshot-release/v1",
      release_id: releaseId,
      custody: "immutable-by-api",
      entrypoint: "tools/decision-snapshot.py",
      external_runtime: {
        python: "/usr/bin/python3",
        stdlib: true,
        account_native_bindings: "tools/decision-snapshot-sources.json",
      },
      files,
    });
    await NodeFSP.writeFile(NodePath.join(directory, "decision-snapshot-release.json"), descriptor);
    await check({
      releaseDirectory: await NodeFSP.realpath(directory),
      releaseId,
      custody: "immutable-by-api",
      manifestSha256: NodeCrypto.createHash("sha256").update(descriptor).digest("hex"),
      allowFixtureProvenance: true,
    });
  } finally {
    await closeOwnedFixtureGroups(owner, directory);
    await NodeFSP.rm(owner, { recursive: true, force: true });
    await expect(NodeFSP.stat(owner)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

async function lifecycleRelease(
  check: (binding: CollectorBinding) => Promise<void>,
  autoStop = false,
) {
  const wrapper = await NodeFSP.readFile(
    new URL("./fixtures/decision-snapshot.py.fixture", import.meta.url),
    "utf8",
  );
  const composer = await NodeFSP.readFile(
    new URL("./fixtures/decision_snapshot.py.fixture", import.meta.url),
    "utf8",
  );
  const usage = await NodeFSP.readFile(
    new URL("./fixtures/claude-account-usage.py.fixture", import.meta.url),
    "utf8",
  );
  expect(NodeCrypto.createHash("sha256").update(usage).digest("hex")).toBe(
    "b1cfd3ce3bf6fdf70fd74a381b368d0cb7151e8c89e0719c1525e9e75e139d01",
  );
  const shim = `import importlib.util, json, os, pathlib, subprocess, sys, time
root = pathlib.Path(__file__).resolve().parents[1]
scratch = root.parent
spec = importlib.util.spec_from_file_location('fixture_usage', root / 'fixture-source/claude-account-usage.py')
usage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(usage)
original_popen = subprocess.Popen
def controlled_popen(command, **options):
    return original_popen(['/usr/bin/python3', '-I', '-S', '-B', str(root / 'fixture-source/native.py'), command[2], str(scratch)], **options)
usage.subprocess.Popen = controlled_popen
time.sleep(.15)
with (scratch / 'trace.fixture').open('a') as stream:
    stream.write(json.dumps({'event': 'usage_start', 'pid': os.getpid(), 'pgid': os.getpgrp(), 'start_ticks': pathlib.Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19], 'monotonic': time.monotonic(), 'budget': float(sys.argv[sys.argv.index('--budget-seconds') + 1])}) + '\\n')
raise SystemExit(usage.main(sys.argv[1:] + ['--state-dir', str(scratch / 'state')]))
`;
  const nativeChild = `import json, os, pathlib, signal, sys, time
scratch = pathlib.Path(sys.argv[2])
def trace(event):
    with (scratch / 'trace.fixture').open('a') as stream:
        stream.write(json.dumps({'event': event, 'pid': os.getpid(), 'parent_pid': os.getppid(), 'pgid': os.getpgrp(), 'start_ticks': pathlib.Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19], 'monotonic': time.monotonic()}) + '\\n')
if sys.argv[1] == 'auth':
    trace('auth')
    print(json.dumps({'status': 'bound', 'account': 'malcolm', 'config_dir': str(pathlib.Path.home() / '.claude-t3/malcolm'), 'fingerprint': 'a' * 64}))
else:
    def cancel(*_):
        trace('cleanup_start')
        time.sleep(.7)
        trace('cleanup_end')
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, cancel)
    trace('probe')
    (scratch / 'probe-ready.tmp').write_text(str(os.getpid()))
    os.rename(scratch / 'probe-ready.tmp', scratch / 'probe-ready.fixture')
    ${autoStop ? "time.sleep(.5); os.kill(os.getpid(), signal.SIGTERM)" : "time.sleep(3600)"}
`;
  const host = `import json, pathlib, time
from datetime import datetime, timezone
scratch = pathlib.Path(__file__).resolve().parents[3].parent
with (scratch / 'trace.fixture').open('a') as stream:
    stream.write(json.dumps({'event': 'host', 'monotonic': time.monotonic()}) + '\\n')
print(json.dumps({'schema': 'work-capacity-admission/v1', 'status': 'ok', 'collected_at_utc': datetime.now(timezone.utc).isoformat(), 'admission': {'agent_batch_ceiling': 0, 'serial_test_process_ceiling': 0, 'measurements': {'ram_available_gib': 0, 'cpu_used_percent': 0, 'one_fully_used_core_percent': 6.25}}}))
`;
  const scratchParent = NodePath.join(
    NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
    "fixtures",
  );
  await release(
    wrapper,
    check,
    (directory) => ({
      "src/codex_v3/decision_snapshot.py": composer,
      "tools/claude-account-usage.py": shim,
      "fixture-source/claude-account-usage.py": usage,
      "fixture-source/native.py": nativeChild,
      "skills/inspect-linux-system-telemetry/scripts/collect_work_capacity.py": host,
      "tools/decision-snapshot-sources.json": JSON.stringify({
        schema: "codex.decision-snapshot-sources/v1",
        release_id: "1".repeat(64),
        queue_store: null,
        accounts: {
          malcolm: {
            sdk: NodePath.join(directory, "fixture-source/native.py"),
            cli: NodePath.join(directory, "fixture-source/native.py"),
            launcher: NodePath.join(directory, "fixture-source/native.py"),
            model: "claude-opus-5-5",
          },
        },
      }),
    }),
    "49a5963936ebfbecd1ba9dcb462dac22a5004d74",
    scratchParent,
  );
}

function lifecycleEffect<A, E>(
  use: (binding: CollectorBinding) => Effect.Effect<A, E>,
  autoStop = false,
): Effect.Effect<A, E> {
  return Effect.suspend(() => {
    let ready!: (binding: CollectorBinding) => void;
    let failed!: (cause: unknown) => void;
    const bindingReady = new Promise<CollectorBinding>((resolve, reject) => {
      ready = resolve;
      failed = reject;
    });
    let finish!: () => void;
    const completed = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const fixture = lifecycleRelease((binding) => {
      ready(binding);
      return completed;
    }, autoStop);
    void fixture.catch(failed);
    return Effect.promise(() => bindingReady).pipe(
      Effect.flatMap(use),
      Effect.ensuring(
        Effect.promise(() => {
          finish();
          return fixture;
        }),
      ),
    );
  });
}

describe("fixed release decision collector", () => {
  it.each(["success", "failure", "cancel"] as const)(
    "removes only its captured scratch root after %s",
    async (mode) => {
      let root: string | undefined;
      const original = new Error("synthetic fixture failure");
      const operation = release("print('fixture')\n", async (binding) => {
        root = NodePath.dirname(binding.releaseDirectory);
        if (mode === "failure") throw original;
        const signal = new AbortController();
        if (mode === "cancel") signal.abort();
        await runBoundCollector(binding, "display", "", monotonicSeconds() + 6, signal.signal);
      });
      if (mode === "success") await operation;
      else if (mode === "failure") await expect(operation).rejects.toBe(original);
      else await expect(operation).rejects.toMatchObject({ reason: "timeout" });
      expect(root).toBeDefined();
      await expect(NodeFSP.stat(root!)).rejects.toMatchObject({ code: "ENOENT" });
    },
  );
  it("passes only bounded purpose, budget and native stdin to the fixed collector", async () => {
    await release(
      "import sys,json\nprint(json.dumps({'arguments':sys.argv[1:],'stdin':sys.stdin.read()}))\n",
      async (binding) => {
        const result = JSON.parse(
          await runBoundCollector(
            binding,
            "display",
            "native-fixture",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        );
        expect(result.arguments.slice(0, 4)).toEqual([
          "--purpose",
          "display",
          "--native-counts-stdin",
          "--deadline-monotonic",
        ]);
        expect(Number(result.arguments[4])).toBeGreaterThan(monotonicSeconds() - 1);
        expect(Number(result.arguments[4])).toBeLessThanOrEqual(monotonicSeconds() + 1);
        expect(result.stdin).toBe("native-fixture");
      },
    );
  });
  it("returns typed missing binding and rejects changed release or symlink escape before spawn", async () => {
    await expect(
      runBoundCollector(null, "display", "", monotonicSeconds() + 6, new AbortController().signal),
    ).rejects.toMatchObject({ reason: "runtime_unavailable" });
    await release("raise Exception('must not run')\n", async (binding) => {
      await expect(
        runBoundCollector(
          { ...binding, manifestSha256: "0".repeat(64) },
          "display",
          "",
          monotonicSeconds() + 6,
          new AbortController().signal,
        ),
      ).rejects.toMatchObject({ reason: "release_mismatch" });
      const collector = NodePath.join(binding.releaseDirectory, "tools", "decision-snapshot.py");
      const original = NodePath.join(binding.releaseDirectory, "tools", "original.py");
      await NodeFSP.rename(collector, original);
      await NodeFSP.symlink(original, collector);
      await expect(
        runBoundCollector(
          binding,
          "display",
          "",
          monotonicSeconds() + 6,
          new AbortController().signal,
        ),
      ).rejects.toMatchObject({ reason: "release_mismatch" });
    });
  });
  it("rejects fixture provenance on production bindings before any subprocess starts", async () => {
    await release(
      "from pathlib import Path\nPath('../must-not-spawn.fixture').write_text('spawned')\n",
      async (binding) => {
        await expect(
          runBoundCollector(
            { ...binding, allowFixtureProvenance: false },
            "display",
            "",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "release_mismatch" });
        await expect(
          NodeFSP.stat(
            NodePath.join(NodePath.dirname(binding.releaseDirectory), "must-not-spawn.fixture"),
          ),
        ).rejects.toMatchObject({ code: "ENOENT" });
      },
    );
  });
  it("bounds stdout and cleans its exact hanging child on timeout", async () => {
    await release("print('x' * 65537)\n", async (binding) => {
      await expect(
        runBoundCollector(
          binding,
          "display",
          "",
          monotonicSeconds() + 6,
          new AbortController().signal,
        ),
      ).rejects.toMatchObject({ reason: "output_too_large" });
    });
    await release(
      "import os,time\nopen('pid.fixture','w').write(str(os.getpid()))\ntime.sleep(3600)\n",
      async (binding) => {
        await expect(
          runBoundCollector(
            binding,
            "display",
            "",
            monotonicSeconds() + 5.1,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "timeout" });
        const pid = Number(
          await NodeFSP.readFile(NodePath.join(binding.releaseDirectory, "pid.fixture"), "utf8"),
        );
        expect(() => process.kill(pid, 0)).toThrow();
      },
    );
  });
  it("cancels after a fixture receipt and waits for exact child cleanup", async () => {
    await release(
      "import os,time\nopen('ready.tmp','w').write(str(os.getpid()))\nos.rename('ready.tmp','ready.fixture')\ntime.sleep(3600)\n",
      async (binding) => {
        const cancellation = new AbortController();
        const receipt = new Promise<void>((resolve) => {
          const watcher = NodeFS.watch(binding.releaseDirectory, (_event, file) => {
            if (file === "ready.fixture") {
              watcher.close();
              cancellation.abort();
              resolve();
            }
          });
          cancellation.signal.addEventListener("abort", () => watcher.close(), { once: true });
        });
        const running = runBoundCollector(
          binding,
          "display",
          "",
          monotonicSeconds() + 6,
          cancellation.signal,
        );
        try {
          await expect(running).rejects.toMatchObject({ reason: "timeout" });
          await receipt;
          const pid = Number(
            await NodeFSP.readFile(
              NodePath.join(binding.releaseDirectory, "ready.fixture"),
              "utf8",
            ),
          );
          expect(() => process.kill(pid, 0)).toThrow();
        } finally {
          cancellation.abort();
        }
      },
    );
  });
  it("verifies the imported closure, explicit custody and foreign release identity", async () => {
    const wrapper = `#!/usr/bin/env python3
"""Run the release-local decision composer without selecting external code."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from codex_v3.decision_snapshot import main

if __name__ == '__main__':
    raise SystemExit(main())
`;
    await release(
      wrapper,
      async (binding) => {
        expect(
          (
            await runBoundCollector(
              binding,
              "display",
              "",
              monotonicSeconds() + 6,
              new AbortController().signal,
            )
          ).trim(),
        ).toBe("verified-module");
        await expect(
          runBoundCollector(
            { ...binding, releaseId: "2".repeat(64) },
            "display",
            "",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "release_mismatch" });
        await NodeFSP.writeFile(
          NodePath.join(binding.releaseDirectory, "src/codex_v3/decision_snapshot.py"),
          "def main():\n    print('changed-module')\n    return 0\n",
        );
        await expect(
          runBoundCollector(
            binding,
            "display",
            "",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "release_mismatch" });
      },
      {
        "src/codex_v3/decision_snapshot.py":
          "def main():\n    print('verified-module')\n    return 0\n",
      },
    );
  });
  it("rejects extra shadow modules and integrity drift during execution", async () => {
    await release("print('verified')\n", async (binding) => {
      await NodeFSP.writeFile(NodePath.join(binding.releaseDirectory, "shadow.py"), "");
      await expect(
        runBoundCollector(
          binding,
          "display",
          "",
          monotonicSeconds() + 6,
          new AbortController().signal,
        ),
      ).rejects.toMatchObject({ reason: "release_mismatch" });
    });
    await release(
      "from pathlib import Path\nPath('tools/decision-snapshot.py').write_text('changed')\nprint('untrusted-result')\n",
      async (binding) => {
        await expect(
          runBoundCollector(
            binding,
            "display",
            "",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "release_mismatch" });
      },
    );
  });
  it("rejects missing closure files and closed-manifest traversal or duplicate declarations", async () => {
    await release(
      "print('must not run')\n",
      async (binding) => {
        await NodeFSP.unlink(NodePath.join(binding.releaseDirectory, "src/helper.py"));
        await expect(
          runBoundCollector(
            binding,
            "display",
            "",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "release_mismatch" });
      },
      { "src/helper.py": "pass\n" },
    );
    for (const invalid of ["duplicate", "traversal", "excess"] as const) {
      await release("print('must not run')\n", async (binding) => {
        const path = NodePath.join(binding.releaseDirectory, "decision-snapshot-release.json");
        const descriptor = JSON.parse(await NodeFSP.readFile(path, "utf8"));
        if (invalid === "duplicate") descriptor.files.push(descriptor.files[0]);
        else if (invalid === "traversal") descriptor.files[0].path = "../unowned.py";
        else descriptor.caller_command = "untrusted";
        const bytes = JSON.stringify(descriptor);
        await NodeFSP.writeFile(path, bytes);
        await expect(
          runBoundCollector(
            {
              ...binding,
              manifestSha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
            },
            "display",
            "",
            monotonicSeconds() + 6,
            new AbortController().signal,
          ),
        ).rejects.toMatchObject({ reason: "release_mismatch" });
      });
    }
  });
  it("runs the exact V3 wrapper and composer closure with unavailable sources and controlled native facts", async () => {
    const wrapper = await NodeFSP.readFile(
      new URL("./fixtures/decision-snapshot.py.fixture", import.meta.url),
      "utf8",
    );
    const composer = await NodeFSP.readFile(
      new URL("./fixtures/decision_snapshot.py.fixture", import.meta.url),
      "utf8",
    );
    expect(NodeCrypto.createHash("sha256").update(wrapper).digest("hex")).toBe(
      "9072ac409957463c08644614df17660755da80394fd2011bebedaca2f449cc53",
    );
    expect(NodeCrypto.createHash("sha256").update(composer).digest("hex")).toBe(
      "089bd5fc2c03ec7b7dd2b3b031a68bc1145d3d38f7ad74a9cf4e0c653555d40a",
    );
    await release(
      wrapper,
      async (binding) => {
        const native = {
          schema: "codex.decision-snapshot-native/v1",
          authority_effect: "none",
          provenance: "fixture",
          sources: {
            threads: {
              status: "observed",
              observed_at: DateTime.formatIso(DateTime.nowUnsafe()),
              timestamp_basis: "native_observation",
              scope: {
                environment_id: "fixture-environment",
                project_id: null,
                include_archived: false,
              },
              values: { total: 4, operating: 2 },
              reason: null,
            },
            workstreams: {
              status: "unavailable",
              observed_at: null,
              timestamp_basis: "unknown",
              scope: {
                registry_id: null,
                owner_id: null,
                principal_id: null,
                server_generation: null,
                registry_version: null,
                authorization_revision: null,
              },
              values: {},
              reason: "fixture_unbound",
            },
          },
        };
        const output = JSON.parse(
          await runBoundCollector(
            binding,
            "display",
            JSON.stringify(native),
            monotonicSeconds() + 12,
            new AbortController().signal,
          ),
        );
        expect(output).toMatchObject({
          schema: "codex.decision-snapshot/v1",
          authority_effect: "none",
          coverage: "partial",
          sources: {
            threads: {
              status: "observed",
              values: { total: 4, operating: 2 },
              provenance: { kind: "fixture" },
            },
            host: {
              values: {
                agent_batch_ceiling: 0,
                serial_test_process_ceiling: 0,
                start_fenced: true,
              },
            },
          },
        });
      },
      { "src/codex_v3/decision_snapshot.py": composer },
      "49a5963936ebfbecd1ba9dcb462dac22a5004d74",
    );
  });
  it("bounds the actual composer and usage lifecycle through delayed startup, nested cleanup and a concurrent loser", async () => {
    await lifecycleRelease(async (binding) => {
      const scratch = NodePath.dirname(binding.releaseDirectory);
      const native = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))({
        schema: "codex.decision-snapshot-native/v1",
        authority_effect: "none",
        provenance: "fixture",
        sources: {
          threads: {
            status: "observed",
            observed_at: DateTime.formatIso(DateTime.nowUnsafe()),
            timestamp_basis: "native_observation",
            scope: {
              environment_id: "fixture-environment",
              project_id: null,
              include_archived: false,
            },
            values: { total: 4, operating: 2 },
            reason: null,
          },
        },
      });
      filesystemDelay.path = binding.releaseDirectory;
      const cancellation = new AbortController();
      let watcher: ReturnType<typeof NodeFS.watch> | undefined;
      const receipt = new Promise<void>((resolve) => {
        watcher = NodeFS.watch(scratch, (_event, file) => {
          if (file === "probe-ready.fixture") {
            watcher!.close();
            resolve();
          }
        });
      });
      let first: Promise<string> | undefined;
      try {
        const started = monotonicSeconds();
        const nativeDeadline = started + 13;
        await NodeTimersPromises.setTimeout(100);
        first = runBoundCollector(
          binding,
          "admission",
          native,
          nativeDeadline,
          cancellation.signal,
        );
        await Promise.race([
          receipt,
          first.then(() => {
            throw new Error("fixture probe did not start");
          }),
        ]);
        const operation = JSON.parse(
          await NodeFSP.readFile(NodePath.join(scratch, "state/malcolm.json"), "utf8"),
        ).operation;
        const childPid = Number(
          await NodeFSP.readFile(NodePath.join(scratch, "probe-ready.fixture"), "utf8"),
        );
        expect(operation).toMatchObject({ pid: childPid, pgid: childPid, account: "malcolm" });
        const concurrent = JSON.parse(
          await runBoundCollector(
            binding,
            "admission",
            native,
            monotonicSeconds() + 13,
            new AbortController().signal,
          ),
        );
        expect(concurrent.sources.account_malcolm.reason).toBe("probe_in_progress");
        const result = JSON.parse(await first);
        expect(monotonicSeconds() - started).toBeLessThan(13);
        expect(result).toMatchObject({
          coverage: "partial",
          sources: {
            threads: { values: { operating: 2, total: 4 } },
            host: { status: "observed" },
          },
        });
        const trace = (await NodeFSP.readFile(NodePath.join(scratch, "trace.fixture"), "utf8"))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        const probe = trace.find((entry) => entry.event === "probe");
        const cleanupStart = trace.find((entry) => entry.event === "cleanup_start");
        const cleanupEnd = trace.find((entry) => entry.event === "cleanup_end");
        expect(cleanupEnd.monotonic - cleanupStart.monotonic).toBeGreaterThan(0.5);
        expect(cleanupEnd.monotonic - cleanupStart.monotonic).toBeLessThan(2);
        expect(probe.pgid).toBe(childPid);
        expect(() => process.kill(childPid, 0)).toThrow();
        expect(() => process.kill(probe.parent_pid, 0)).toThrow();
        expect(() => process.kill(-childPid, 0)).toThrow();
        expect(() => process.kill(-probe.parent_pid, 0)).toThrow();
        const firstUsage = trace.find((entry) => entry.event === "usage_start");
        expect(firstUsage.budget).toBeGreaterThan(0);
        expect(firstUsage.budget).toBeLessThan(2.6);
        expect(trace.at(-1).event).toBe("host");
        expect(trace.at(-1).monotonic).toBeGreaterThan(cleanupEnd.monotonic);
        expect(trace.filter((entry) => entry.event === "auth")).toHaveLength(1);
        expect(trace.filter((entry) => entry.event === "probe")).toHaveLength(1);
        const state = JSON.parse(
          await NodeFSP.readFile(NodePath.join(scratch, "state/malcolm.json"), "utf8"),
        );
        expect(Date.parse(state.retry_not_before)).toBeGreaterThan(
          DateTime.toEpochMillis(DateTime.nowUnsafe()),
        );
        expect(state.operation).toBeUndefined();
        const cooldown = JSON.parse(
          await runBoundCollector(
            binding,
            "admission",
            native,
            monotonicSeconds() + 13,
            new AbortController().signal,
          ),
        );
        expect(cooldown.sources.account_malcolm.reason).toBe("retry_not_before");
        const after = (await NodeFSP.readFile(NodePath.join(scratch, "trace.fixture"), "utf8"))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        expect(after.filter((entry) => entry.event === "auth")).toHaveLength(2);
        expect(after.filter((entry) => entry.event === "probe")).toHaveLength(1);
      } finally {
        watcher?.close();
        cancellation.abort();
        if (first !== undefined) await first.catch(() => {});
        filesystemDelay.path = null;
      }
    });
  }, 30_000);
  effectIt.effect(
    "starts an actual native account child before a held registry count packet is released",
    () =>
      lifecycleEffect(
        (binding) =>
          Effect.gen(function* () {
            const scratch = NodePath.dirname(binding.releaseDirectory);
            let releaseRegistry!: () => void;
            const registry = new Promise<void>((resolve) => {
              releaseRegistry = resolve;
            });
            const counts = {
              context: { owner_id: "fixture-owner", server_generation: 1, registry_version: 7 },
              principal_id: "fixture-principal",
              observed_at: DateTime.formatIso(DateTime.nowUnsafe()),
              authority_effect: "none",
              total: 2,
              active: 1,
              unknown_lifecycle: 0,
            };
            const dependencies = Layer.mergeAll(
              Layer.succeed(DecisionSnapshotNativeCounts, {
                readOperatingCounts: () =>
                  Effect.succeed({
                    total: 4,
                    operating: 2,
                    foregroundWaitingApproval: 0,
                    foregroundWaitingInput: 0,
                    foregroundWaitingPlan: 0,
                    backgroundOperating: 0,
                    backgroundUnknown: 0,
                    snapshotSequence: 1,
                    backgroundSampledAt: counts.observed_at,
                    observedAt: counts.observed_at,
                  }),
                readRegistryCounts: () =>
                  Effect.promise(async () => {
                    await registry;
                    return {
                      counts,
                      complete: true,
                      binding: {
                        registryId: "https://registry.example.invalid",
                        ownerId: "fixture-owner",
                        principalId: "fixture-principal",
                        authorizationRevision: 1,
                        serverGeneration: 1,
                        registryVersion: 7,
                      },
                    };
                  }),
              }),
              Layer.succeed(DecisionSnapshotCollector, makeCollector(binding)),
            );
            const program = Effect.gen(function* () {
              const toolkit = yield* DecisionSnapshotToolkit.pipe(
                Effect.provide(
                  DecisionSnapshotToolkitHandlersLive.pipe(Layer.provide(dependencies)),
                ),
              );
              return yield* toolkit.handle("decision_snapshot", { purpose: "admission" }).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((results) => results.at(-1)!.result),
              );
            }).pipe(
              Effect.provide(dependencies),
              Effect.provideService(McpInvocationContext, {
                environmentId: EnvironmentId.make("fixture-environment"),
                requestNamespace: "fixture-session",
                thread: {
                  threadId: ThreadId.make("fixture-thread"),
                  providerSessionId: "fixture-session",
                  providerInstanceId: ProviderInstanceId.make("codex"),
                },
                client: undefined,
                capabilities: new Set(["decision-snapshot"] as const),
                issuedAt: 1,
              }),
            );
            const receiptWait = new AbortController();
            let watcher: ReturnType<typeof NodeFS.watch> | undefined;
            const receipt = new Promise<boolean>((resolve) => {
              watcher = NodeFS.watch(scratch, (_event, file) => {
                if (file === "probe-ready.fixture") {
                  watcher!.close();
                  resolve(true);
                }
              });
            });
            filesystemDelay.path = binding.releaseDirectory;
            const started = monotonicSeconds();
            const running = yield* Effect.forkChild(program);
            try {
              const beforeRelease = yield* Effect.promise(() =>
                Promise.race([
                  receipt,
                  NodeTimersPromises.setTimeout(2_000, false, { signal: receiptWait.signal }),
                ]),
              );
              releaseRegistry();
              expect(beforeRelease).toBe(true);
              const result = yield* Fiber.join(running);
              expect(result).toMatchObject({
                coverage: "partial",
                sources: {
                  threads: { values: { total: 4, operating: 2 } },
                  workstreams: { values: { total: 2, active: 1 } },
                  host: { status: "observed" },
                },
              });
              expect(monotonicSeconds() - started).toBeLessThan(40);
              const trace = (yield* Effect.promise(() =>
                NodeFSP.readFile(NodePath.join(scratch, "trace.fixture"), "utf8"),
              ))
                .trim()
                .split("\n")
                .map((line) => JSON.parse(line));
              const probe = trace.find((entry) => entry.event === "probe");
              const cleanupEnd = trace.find((entry) => entry.event === "cleanup_end");
              expect(trace.find((entry) => entry.event === "usage_start").budget).toBeGreaterThan(
                28,
              );
              expect(trace.at(-1).event).toBe("host");
              expect(trace.at(-1).monotonic).toBeGreaterThan(cleanupEnd.monotonic);
              expect(() => process.kill(-probe.pid, 0)).toThrow();
              expect(() => process.kill(-probe.parent_pid, 0)).toThrow();
            } finally {
              releaseRegistry();
              receiptWait.abort();
              watcher?.close();
              yield* Fiber.interrupt(running);
              filesystemDelay.path = null;
            }
          }),
        true,
      ),
    { timeout: 15_000 },
  );
  it.each(["eof", "timeout"] as const)(
    "retains local facts when deferred native input ends in %s",
    async (mode) => {
      await lifecycleRelease(async (binding) => {
        const input =
          mode === "eof"
            ? Promise.reject(new Error("synthetic native producer failure"))
            : new Promise<string>(() => {});
        const result = JSON.parse(
          await runBoundCollector(
            binding,
            "display",
            input,
            monotonicSeconds() + 13,
            new AbortController().signal,
          ),
        );
        expect(result.coverage).toBe("partial");
        for (const name of ["threads", "workstreams"])
          expect(result.sources[name]).toMatchObject({
            status: mode === "eof" ? "unavailable" : "timeout",
            reason: `native_packet_${mode}`,
            values: {},
          });
        expect(result.sources.host.status).toBe("observed");
        const trace = (
          await NodeFSP.readFile(
            NodePath.join(NodePath.dirname(binding.releaseDirectory), "trace.fixture"),
            "utf8",
          )
        )
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        const probe = trace.find((entry) => entry.event === "probe");
        expect(trace.at(-1).event).toBe("host");
        expect(trace.at(-1).monotonic).toBeGreaterThan(
          trace.find((entry) => entry.event === "cleanup_end").monotonic,
        );
        expect(() => process.kill(-probe.pid, 0)).toThrow();
        expect(() => process.kill(-probe.parent_pid, 0)).toThrow();
      }, true);
    },
    15_000,
  );
  effectIt.effect(
    "cancels its deferred producer and actual nested account groups before returning",
    () =>
      lifecycleEffect((binding) =>
        Effect.gen(function* () {
          const scratch = NodePath.dirname(binding.releaseDirectory);
          let inputClosed = false;
          const input = Effect.never.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                inputClosed = true;
              }),
            ),
          );
          let watcher: ReturnType<typeof NodeFS.watch> | undefined;
          const receipt = new Promise<void>((resolve) => {
            watcher = NodeFS.watch(scratch, (_event, file) => {
              if (file === "probe-ready.fixture") {
                watcher!.close();
                resolve();
              }
            });
          });
          const running = yield* Effect.forkChild(
            makeCollector(binding).collect("admission", input, monotonicSeconds() + 40),
          );
          try {
            yield* Effect.promise(() => receipt);
            yield* Fiber.interrupt(running);
            expect(inputClosed).toBe(true);
            const trace = (yield* Effect.promise(() =>
              NodeFSP.readFile(NodePath.join(scratch, "trace.fixture"), "utf8"),
            ))
              .trim()
              .split("\n")
              .map((line) => JSON.parse(line));
            const probe = trace.find((entry) => entry.event === "probe");
            expect(trace.filter((entry) => entry.event === "host")).toHaveLength(0);
            const cleanupStart = trace.find((entry) => entry.event === "cleanup_start");
            const cleanupEnd = trace.find((entry) => entry.event === "cleanup_end");
            expect(cleanupEnd.monotonic - cleanupStart.monotonic).toBeGreaterThan(0.5);
            expect(cleanupEnd.monotonic - cleanupStart.monotonic).toBeLessThan(2);
            expect(() => process.kill(-probe.pid, 0)).toThrow();
            expect(() => process.kill(-probe.parent_pid, 0)).toThrow();
            const state = decodeUsageFixtureState(
              yield* Effect.promise(() =>
                NodeFSP.readFile(NodePath.join(scratch, "state/malcolm.json"), "utf8"),
              ),
            );
            expect(state.operation).toBeUndefined();
            expect(Date.parse(state.retry_not_before)).toBeGreaterThan(
              DateTime.toEpochMillis(DateTime.nowUnsafe()),
            );
          } finally {
            watcher?.close();
            yield* Fiber.interrupt(running);
          }
        }),
      ),
    { timeout: 15_000 },
  );
  it("does not spawn a cancelled call", async () => {
    await release("raise Exception('must not run')\n", async (binding) => {
      const cancellation = new AbortController();
      cancellation.abort();
      await expect(
        runBoundCollector(binding, "display", "", monotonicSeconds() + 6, cancellation.signal),
      ).rejects.toMatchObject({ reason: "timeout" });
    });
  });
});
