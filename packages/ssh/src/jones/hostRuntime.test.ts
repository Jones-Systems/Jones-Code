import { assert, describe, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  buildHostRuntimeAttachScript,
  buildHostRuntimePairingScript,
  buildHostRuntimeRunnerScript,
} from "./hostRuntime.ts";

// oxlint-disable-next-line t3code/no-global-process-runtime -- Synthetic shell executables require a POSIX native fixture host.
describe.skipIf(NodeOS.platform() === "win32")("Jones host runtime attachment", () => {
  const hostVersion = "0.0.45-preview.20261010.701.1";

  function withHost(test: (home: string, base: string) => void) {
    const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-host-attach-"));
    try {
      const base = NodePath.join(home, ".t3");
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
  '__ssh-helper runtime-port') printf '42 3774\\n' ;;
  '__ssh-helper wait-ready') exit "\${JONES_TEST_READY_EXIT:-0}" ;;
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
      const result = run(home, buildHostRuntimeAttachScript(), { JONES_TEST_READY_EXIT: "1" });
      assert.equal(result.status, 1);
      assert.include(result.stderr, "managed server is not ready");
      assert.notInclude(invokedCommands(home), "serve");
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
});
