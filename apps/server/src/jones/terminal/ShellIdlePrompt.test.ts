// @effect-diagnostics-next-line nodeBuiltinImport:off -- The live fixture drives exact stdin/stdout callback boundaries and owns the captured child through its finalizer.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Registration-time executable availability decides the platform gate before an Effect test context exists.
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { describe, expect, it } from "@effect/vitest";
import { makeShellIdlePromptIntegration, ShellIdlePrompt } from "./ShellIdlePrompt.ts";

describe("shell prompt proof", () => {
  it("does not mistake a quiet builtin or input for a returned prompt", () => {
    const tracker = new ShellIdlePrompt("test");
    tracker.output(tracker.ready);
    expect(tracker.atPrompt).toBe(true);
    tracker.input();
    expect(tracker.atPrompt).toBe(false);
    // An old startup prompt can arrive after input; preexec revokes it before execution.
    tracker.output(tracker.ready + tracker.busy);
    expect(tracker.atPrompt).toBe(false);
    tracker.output("a command can produce arbitrary text without finishing\n");
    expect(tracker.atPrompt).toBe(false);
    tracker.output(tracker.ready);
    expect(tracker.atPrompt).toBe(true);
  });

  it("strips complete and split markers while preserving visible output", () => {
    for (const marker of ["ready", "busy"] as const) {
      const probe = new ShellIdlePrompt("test");
      for (let split = 0; split <= probe[marker].length; split++) {
        const tracker = new ShellIdlePrompt("test");
        const output =
          tracker.output("before" + tracker[marker].slice(0, split)) +
          tracker.output(tracker[marker].slice(split) + "after");
        expect(output).toBe("beforeafter");
        expect(tracker.atPrompt).toBe(marker === "ready");
      }
    }
  });

  it("does not accept another terminal's marker and retains ordinary escape output", () => {
    const tracker = new ShellIdlePrompt("one");
    const other = new ShellIdlePrompt("two");
    const text = other.ready + "\x1b[31mvisible\x1b[0m";
    expect(tracker.output(text)).toBe(text);
    expect(tracker.atPrompt).toBe(false);
  });
});

for (const name of ["bash", "zsh"]) {
  const shell = `/bin/${name}`;
  // Executable coverage is available only on hosts where this shell is installed.
  it.live.skipIf(!NodeFS.existsSync(shell))(
    `qualifies ${name} startup, builtin command tracking and original configuration in an isolated home`,
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "jones-idle-prompt-test-" });
        yield* fs.writeFileString(`${home}/included.zsh`, "export JONES_TEST_RC=loaded\n");
        yield* fs.writeFileString(`${home}/.zshrc`, 'source "${ZDOTDIR:-$HOME}/included.zsh"\n');
        const integration = yield* makeShellIdlePromptIntegration();
        const prepared = yield* integration.prepare(name, {
          PATH: "/usr/bin:/bin",
          HOME: home,
          TERM: "xterm-256color",
          LANG: "C.UTF-8",
          PROMPT_COMMAND: "builtin printf EXISTING; # retain a trailing comment",
        });
        const tracker = prepared.tracker!;
        // No real user startup files, environment, credentials or state are copied.
        const child = NodeChildProcess.spawn(
          shell,
          name === "bash" ? ["--noprofile", "--norc", "-i"] : ["-i"],
          { cwd: home, env: prepared.env, stdio: "pipe" },
        );
        let output = "";
        let ended = false;
        const waiting = new Set<() => void>();
        const notify = () => {
          for (const next of waiting) next();
        };
        const onData = (chunk: Buffer) => {
          output += chunk.toString();
          tracker.output(chunk.toString());
          notify();
        };
        child.stdout.on("data", onData);
        child.stderr.on("data", onData);
        const exited = new Promise<void>((resolve, reject) => {
          child.once("error", (error) => {
            ended = true;
            notify();
            reject(error);
          });
          child.once("exit", () => {
            ended = true;
            notify();
            resolve();
          });
        });
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            // Captured child only; interruption also releases it before startup files.
            if (!ended) child.kill("SIGKILL");
            await exited;
          }),
        );
        const waitFor = (predicate: () => boolean) =>
          new Promise<void>((resolve, reject) => {
            const done = () => {
              if (predicate()) {
                waiting.delete(done);
                resolve();
              } else if (ended) {
                waiting.delete(done);
                reject(new Error(`Shell ${name} exited before prompt proof`));
              }
            };
            waiting.add(done);
            done();
          });
        const readyCount = () => output.split(tracker.ready).length - 1;
        yield* Effect.promise(async () => {
          let promptBase = 1;
          if (name === "bash") {
            child.stdin.write(
              'printf \'JONES_BASH_VERSION:%s.%s\\n\' "${BASH_VERSINFO[0]}" "${BASH_VERSINFO[1]}"\n',
            );
            await waitFor(() => /JONES_BASH_VERSION:\d+\.\d+/.test(output));
            const version = output.match(/JONES_BASH_VERSION:(\d+)\.(\d+)/)!;
            if (Number(version[1]) < 4 || (Number(version[1]) === 4 && Number(version[2]) < 4)) {
              // macOS system Bash has no PS0. It remains open without false readiness.
              expect(tracker.atPrompt).toBe(false);
              child.stdin.end("exit\n");
              await exited;
              return;
            }
            promptBase = 2;
          }
          await waitFor(() => readyCount() >= promptBase);
          expect(tracker.atPrompt).toBe(true);
          if (name === "bash") expect(output).toContain("EXISTING");
          child.stdin.write("false\n");
          await waitFor(() => readyCount() >= promptBase + 1);
          child.stdin.write("printf 'STATUS:%s\\n' \"$?\"\n");
          await waitFor(() => readyCount() >= promptBase + 2);
          expect(output).toContain("STATUS:1");
          if (name === "zsh") {
            child.stdin.write('printf \'RC:%s DOT:%s\\n\' "$JONES_TEST_RC" "${ZDOTDIR-unset}"\n');
            await waitFor(() => readyCount() >= promptBase + 3);
            expect(output).toContain("RC:loaded DOT:unset");
          }
          const before = readyCount();
          tracker.input();
          const revision = tracker.revision;
          child.stdin.write("read -r jones_idle_value\n");
          await waitFor(() => tracker.revision > revision && !tracker.atPrompt);
          expect(readyCount()).toBe(before);
          child.stdin.write("value\n");
          await waitFor(() => readyCount() > before);
          expect(tracker.atPrompt).toBe(true);
          child.stdin.end("exit\n");
          await exited;
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
}
