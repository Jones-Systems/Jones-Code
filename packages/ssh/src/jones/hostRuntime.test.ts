import { assert, describe, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Synchronous synthetic shell fixtures execute outside Effect.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off - try/finally owns these synchronous fixture files and cleanup.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Native paths locate the synchronous synthetic shell fixtures.
import * as NodePath from "node:path";

import {
  buildHostRuntimeAttachScript,
  buildHostRuntimePairingScript,
  buildHostRuntimeRunnerScript,
} from "./hostRuntime.ts";

// oxlint-disable-next-line t3code/no-global-process-runtime -- Synthetic shell executables require a POSIX native fixture host.
describe.skipIf(NodeOS.platform() === "win32")("Jones host runtime attachment", () => {
  const hostVersion = "0.0.45-preview.20261010.701.1";

  function withHost(test: (home: string, base: string) => void, baseName = ".t3") {
    const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-host-attach-"));
    try {
      const base = NodePath.join(home, baseName);
      installRuntime(base, hostVersion);
      NodeFS.writeFileSync(
        NodePath.join(base, "runtime", "service-state.json"),
        JSON.stringify({
          protocol: 4,
          activeVersion: hostVersion,
        }),
      );
      test(home, base);
    } finally {
      NodeFS.rmSync(home, { recursive: true, force: true });
    }
  }

  function installRuntime(base: string, version: string) {
    const runtime = NodePath.join(base, "runtime", "versions", version);
    NodeFS.mkdirSync(runtime, { recursive: true });
    NodeFS.writeFileSync(NodePath.join(runtime, ".install-complete"), version);
    NodeFS.writeFileSync(
      NodePath.join(runtime, "t3"),
      `#!/bin/sh
printf '%s %s\\n' '${version}' "$*" >> "$JONES_TEST_CALLS"
case "$1 $2" in
  '__ssh-helper runtime-port')
    [ -z "\${JONES_TEST_PORT_MISSING:-}" ] || exit 1
    printf '42 3774\\n'
    ;;
  '__ssh-helper wait-ready')
    # Fail the first JONES_TEST_READY_FAILURES probes, then follow JONES_TEST_READY_EXIT.
    seen=$(cat "$JONES_TEST_COUNTER" 2>/dev/null || echo 0)
    if [ "$seen" -lt "\${JONES_TEST_READY_FAILURES:-0}" ]; then
      echo $((seen + 1)) > "$JONES_TEST_COUNTER"
      exit 1
    fi
    exit "\${JONES_TEST_READY_EXIT:-0}"
    ;;
  'auth pairing') printf '{"pairing":"synthetic"}\\n' ;;
  '--version ') printf '${version}\\n' ;;
  *) exit 88 ;;
esac
`,
      { mode: 0o700 },
    );
  }

  function run(
    home: string,
    script: string,
    extra: Record<string, string> = {},
    args: string[] = [],
  ) {
    return NodeChildProcess.spawnSync("sh", ["-c", script, "host-test", ...args], {
      encoding: "utf8",
      timeout: 5000,
      env: {
        PATH: process.env.PATH ?? "",
        HOME: home,
        JONES_TEST_CALLS: NodePath.join(home, "calls"),
        JONES_TEST_COUNTER: NodePath.join(home, "ready-count"),
        ...extra,
      },
    });
  }

  function invokedCommands(home: string) {
    return NodeFS.readFileSync(NodePath.join(home, "calls"), "utf8")
      .trim()
      .split("\n")
      .map((call) => call.split(/\s+/u)[1]);
  }

  it("uses the installed host version and leaves launch state untouched", () => {
    withHost((home, base) => {
      const statePath = NodePath.join(base, "runtime", "service-state.json");
      const state = NodeFS.readFileSync(statePath, "utf8");
      const result = run(home, buildHostRuntimeAttachScript());
      assert.equal(result.status, 0, result.stderr);
      assert.deepEqual(JSON.parse(result.stdout), { remotePort: 3774, serverKind: "external" });
      assert.equal(NodeFS.readFileSync(statePath, "utf8"), state);
      assert.deepEqual(NodeFS.readdirSync(base), ["runtime"]);
      assert.include(
        NodeFS.readFileSync(NodePath.join(home, "calls"), "utf8"),
        `${hostVersion} __ssh-helper runtime-port`,
      );
      assert.notInclude(invokedCommands(home), "serve");
    });
  });

  it("resolves the latest host active version again for pairing", () => {
    withHost((home, base) => {
      const nextVersion = "0.0.45-preview.20261010.800.1";
      installRuntime(base, nextVersion);
      NodeFS.writeFileSync(
        NodePath.join(base, "runtime", "service-state.json"),
        JSON.stringify({ protocol: 4, activeVersion: nextVersion }),
      );
      const result = run(home, buildHostRuntimePairingScript());
      assert.equal(result.status, 0, result.stderr);
      assert.deepEqual(JSON.parse(result.stdout), { pairing: "synthetic" });
      assert.include(
        NodeFS.readFileSync(NodePath.join(home, "calls"), "utf8"),
        `${nextVersion} auth pairing create --base-dir ${base} --json`,
      );
    });
  });

  it("fails clearly without a complete host runtime and does not install one", () => {
    withHost((home, base) => {
      NodeFS.rmSync(NodePath.join(base, "runtime", "versions", hostVersion, ".install-complete"));
      const result = run(home, buildHostRuntimeAttachScript());
      assert.equal(result.status, 1);
      assert.include(result.stderr, "Set up Jones Code on this host");
      assert.notInclude(NodeFS.readdirSync(home), "calls");
    });
  });

  it("does not replace a server that fails readiness", () => {
    withHost((home) => {
      const result = run(
        home,
        buildHostRuntimeAttachScript({ attempts: 3, retryDelaySeconds: 0 }),
        { JONES_TEST_READY_EXIT: "1" },
      );
      assert.equal(result.status, 1);
      assert.include(result.stderr, "managed server is not ready");
      assert.notInclude(invokedCommands(home), "serve");
      // Every attempt probed readiness; none started or replaced anything.
      assert.equal(invokedCommands(home).filter((command) => command === "__ssh-helper").length, 6);
    });
  });

  it("retries readiness and attaches once a restarting server answers", () => {
    withHost((home) => {
      const result = run(
        home,
        buildHostRuntimeAttachScript({ attempts: 4, retryDelaySeconds: 0 }),
        { JONES_TEST_READY_FAILURES: "3" },
      );
      assert.equal(result.status, 0, result.stderr);
      assert.deepEqual(JSON.parse(result.stdout), { remotePort: 3774, serverKind: "external" });
      assert.notInclude(invokedCommands(home), "serve");
    });
  });

  it("says the server is not ready, not that setup is missing, after a transient restart", () => {
    withHost((home) => {
      const absent = run(
        home,
        buildHostRuntimeAttachScript({ attempts: 2, retryDelaySeconds: 0 }),
        { JONES_TEST_PORT_MISSING: "1" },
      );
      assert.equal(absent.status, 1);
      assert.include(absent.stderr, "not ready yet");
      assert.include(absent.stderr, "managed server is not running");
      assert.notInclude(absent.stderr, "Set up Jones Code");
      const unready = run(
        home,
        buildHostRuntimeAttachScript({ attempts: 2, retryDelaySeconds: 0 }),
        { JONES_TEST_READY_EXIT: "1" },
      );
      assert.equal(unready.status, 1);
      assert.include(unready.stderr, "not ready yet");
      assert.notInclude(unready.stderr, "Set up Jones Code");
    });
  });

  it("does not retry missing or invalid service state", () => {
    withHost((home, base) => {
      NodeFS.rmSync(NodePath.join(base, "runtime", "service-state.json"));
      const result = run(home, buildHostRuntimeAttachScript({ retryDelaySeconds: 30 }));
      assert.equal(result.status, 1);
      assert.include(result.stderr, "Set up Jones Code on this host");
      assert.include(result.stderr, "service state is missing");
    });
  });

  it("rejects traversal and duplicate active versions before executing anything", () => {
    withHost((home, base) => {
      for (const state of [
        '{"activeVersion":"../outside"}',
        `{"activeVersion":"${hostVersion}","activeVersion":"${hostVersion}"}`,
      ]) {
        NodeFS.writeFileSync(NodePath.join(base, "runtime", "service-state.json"), state);
        const result = run(home, buildHostRuntimeAttachScript());
        assert.equal(result.status, 1);
        assert.notInclude(NodeFS.readdirSync(home), "calls");
      }
    });
  });

  it("refuses handoff schemas instead of executing a nested legacy active version", () => {
    withHost((home, base) => {
      NodeFS.writeFileSync(
        NodePath.join(base, "runtime", "service-state.json"),
        JSON.stringify({
          schema: "task-owned-handoff/v1",
          nativeProtocol3State: { protocol: 3, activeVersion: hostVersion },
        }),
      );
      for (const script of [
        buildHostRuntimeAttachScript(),
        buildHostRuntimePairingScript(),
        buildHostRuntimeRunnerScript(),
      ]) {
        const result = run(home, script, {}, ["--version"]);
        assert.equal(result.status, 1);
        assert.include(result.stderr, "Set up Jones Code on this host");
        assert.include(result.stderr, "unsupported service state schema");
        assert.notInclude(NodeFS.readdirSync(home), "calls");
        assert.deepEqual(NodeFS.readdirSync(base), ["runtime"]);
      }
    });
  });

  it("preserves an explicitly configured host home", () => {
    withHost((home, base) => {
      const result = run(home, buildHostRuntimeRunnerScript(), { T3CODE_HOME: base }, [
        "--version",
      ]);
      assert.equal(result.status, 0, result.stderr);
      assert.equal(result.stdout.trim(), hostVersion);
    });
  });

  it("discovers the runbook's $HOME/.jones-code base when T3CODE_HOME is unset", () => {
    withHost((home, base) => {
      for (const script of [buildHostRuntimeAttachScript(), buildHostRuntimePairingScript()]) {
        const result = run(home, script);
        assert.equal(result.status, 0, result.stderr);
      }
      assert.include(
        NodeFS.readFileSync(NodePath.join(home, "calls"), "utf8"),
        `${hostVersion} auth pairing create --base-dir ${base} --json`,
      );
    }, ".jones-code");
  });

  it("honors T3CODE_HOME over both default bases for attach and pairing", () => {
    withHost((home, base) => {
      const custom = NodePath.join(home, "custom-base");
      NodeFS.renameSync(base, custom);
      // A competing default base must not be chosen once T3CODE_HOME is explicit.
      installRuntime(NodePath.join(home, ".jones-code"), "0.0.1");
      NodeFS.writeFileSync(
        NodePath.join(home, ".jones-code", "runtime", "service-state.json"),
        JSON.stringify({ protocol: 4, activeVersion: "0.0.1" }),
      );
      for (const script of [buildHostRuntimeAttachScript(), buildHostRuntimePairingScript()]) {
        const result = run(home, script, { T3CODE_HOME: custom });
        assert.equal(result.status, 0, result.stderr);
      }
      const calls = NodeFS.readFileSync(NodePath.join(home, "calls"), "utf8");
      assert.include(calls, `${hostVersion} auth pairing create --base-dir ${custom} --json`);
      assert.notInclude(calls, "0.0.1");
    });
  });

  it("refuses ambiguous service state in both default bases without executing anything", () => {
    withHost((home) => {
      const other = NodePath.join(home, ".jones-code");
      installRuntime(other, hostVersion);
      NodeFS.writeFileSync(
        NodePath.join(other, "runtime", "service-state.json"),
        JSON.stringify({ protocol: 4, activeVersion: hostVersion }),
      );
      for (const script of [
        buildHostRuntimeAttachScript(),
        buildHostRuntimePairingScript(),
        buildHostRuntimeRunnerScript(),
      ]) {
        const result = run(home, script, {}, ["--version"]);
        assert.equal(result.status, 1);
        assert.include(result.stderr, "service state exists in both");
        assert.notInclude(NodeFS.readdirSync(home), "calls");
      }
    });
  });
});
