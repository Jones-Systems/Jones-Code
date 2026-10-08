// @effect-diagnostics-next-line nodeBuiltinImport:off - synchronous shell fixtures run outside Effect.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off - try/finally owns these synchronous fixture files.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off - paths for the synchronous shell fixtures.
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

import * as SshTunnel from "../tunnel.ts";

const ARCHIVE = { archiveVersion: "1.2.3-preview.20260911.4" } as const;

describe("persistent remote launch", () => {
  it.skipIf(HostProcessPlatform.defaultValue() === "win32").each([
    { name: "same managed PID in the default runtime", runtime: true, ready: true, changed: false },
    { name: "healthy managed PID after runner change", runtime: false, ready: true, changed: true },
    { name: "unready default runtime", runtime: true, ready: false, changed: false },
    { name: "unready recorded managed PID", runtime: false, ready: false, changed: true },
  ])("preserves $name", ({ runtime, ready, changed }) => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-persistent-launch-"));
    try {
      const state = NodePath.join(root, ".t3/ssh-launch/fixture");
      const binary = NodePath.join(root, `.t3/runtime/versions/${ARCHIVE.archiveVersion}`);
      NodeFS.mkdirSync(state, { recursive: true });
      NodeFS.mkdirSync(binary, { recursive: true });
      NodeFS.writeFileSync(NodePath.join(binary, ".install-complete"), ARCHIVE.archiveVersion);
      NodeFS.writeFileSync(
        NodePath.join(binary, "t3"),
        `#!/bin/sh
case "$1 $2" in
  "--version ") echo fixture ;;
  "__ssh-helper runtime-port") ${runtime ? "echo '4242 3773'" : "exit 1"} ;;
  "__ssh-helper wait-ready") ${ready ? "exit 0" : "exit 1"} ;;
  "__ssh-helper pick-port") echo duplicate >> "$HOME/events"; echo 3774 ;;
  *) echo spawn >> "$HOME/events" ;;
esac
`,
        { mode: 0o700 },
      );
      NodeFS.writeFileSync(NodePath.join(state, "pid"), "4242");
      NodeFS.writeFileSync(NodePath.join(state, "port"), "3773");
      NodeFS.writeFileSync(NodePath.join(state, "managed"), "managed");
      NodeFS.writeFileSync(NodePath.join(root, "events"), "");
      NodeFS.writeFileSync(
        NodePath.join(state, "run-t3.sh"),
        changed ? "old runner" : SshTunnel.buildRemoteT3RunnerScript(ARCHIVE) + "\n",
      );
      // Synthetic PIDs never reach the OS. Any signal or duplicate launch is observable.
      const script =
        `kill() {
  if [ "$1" = "-0" ]; then return 0; fi
  echo "kill $*" >> "$HOME/events"
}
sleep() { :; }
` + SshTunnel.buildRemoteLaunchScript(ARCHIVE);
      const result = NodeChildProcess.spawnSync("sh", ["-c", script, "fixture", "fixture"], {
        env: { PATH: process.env.PATH, HOME: root },
        encoding: "utf8",
        timeout: 5000,
      });
      assert.isUndefined(result.error);
      assert.equal(NodeFS.readFileSync(NodePath.join(root, "events"), "utf8"), "");
      assert.equal(result.status, ready ? 0 : 1, result.stderr);
      if (ready) {
        assert.equal(JSON.parse(result.stdout).remotePort, 3773);
      } else {
        assert.include(result.stderr, "already running");
      }
      assert.equal(NodeFS.readFileSync(NodePath.join(state, "pid"), "utf8"), "4242");
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(HostProcessPlatform.defaultValue() === "win32").each([false, true])(
    "failed new launch preserves ownership when exit is unknown: %s",
    (exitUnknown) => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-failed-launch-"));
      try {
        const state = NodePath.join(root, ".t3/ssh-launch/fixture");
        const binary = NodePath.join(root, `.t3/runtime/versions/${ARCHIVE.archiveVersion}`);
        NodeFS.mkdirSync(binary, { recursive: true });
        NodeFS.writeFileSync(NodePath.join(binary, ".install-complete"), ARCHIVE.archiveVersion);
        NodeFS.writeFileSync(
          NodePath.join(binary, "t3"),
          `#!/bin/sh
case "$1 $2" in
  "--version ") echo fixture ;;
  "__ssh-helper runtime-port") exit 1 ;;
  "__ssh-helper wait-ready") exit 1 ;;
  "__ssh-helper pick-port") echo 3773 ;;
esac
`,
          { mode: 0o700 },
        );
        // Intercept only this shell's process effects; no OS PID is signalled.
        const script =
          `stopped=0
kill() {
  if [ "$1" = "-0" ]; then [ "$stopped" = "0" ]; return; fi
  echo "$1" >> "$HOME/signals"
  ${exitUnknown ? ":" : "stopped=1"}
}
nohup() { :; }
sleep() { :; }
` + SshTunnel.buildRemoteLaunchScript(ARCHIVE);
        const result = NodeChildProcess.spawnSync("sh", ["-c", script, "fixture", "fixture"], {
          env: { PATH: process.env.PATH, HOME: root },
          encoding: "utf8",
          timeout: 5000,
        });
        assert.isUndefined(result.error);
        assert.equal(result.status, 1);
        assert.include(result.stderr, "did not become ready");
        const signalled = NodeFS.readFileSync(NodePath.join(root, "signals"), "utf8")
          .trim()
          .split("\n");
        assert.lengthOf(signalled, 1);
        assert.match(signalled[0]!, /^\d+$/u);
        for (const name of ["pid", "port", "managed"]) {
          assert.equal(NodeFS.existsSync(NodePath.join(state, name)), exitUnknown);
        }
        if (exitUnknown) {
          assert.equal(
            NodeFS.readFileSync(NodePath.join(state, "pid"), "utf8").trim(),
            signalled[0],
          );
          assert.include(result.stderr, "ownership files were kept");
        }
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    },
  );
});
