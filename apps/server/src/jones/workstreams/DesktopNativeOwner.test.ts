// @effect-diagnostics nodeBuiltinImport:off -- Each test owns inherited pipes and retires its child before returning.
// @effect-diagnostics globalTimers:off -- These timers are owned and cleared by the native inherited-pipe lifecycle.
import * as NodeTimers from "node:timers";
import * as ChildProcess from "node:child_process";
import type * as Stream from "node:stream";
import { expect, it } from "vite-plus/test";

const moduleUrl = new URL("./DesktopNativeOwner.ts", import.meta.url).href;
const binding = {
  home: "/fixture",
  databasePath: "/fixture/userdata/statev2.sqlite",
  profile: "/fixture/profile",
  environmentId: "fixture",
  version: "fixture-version",
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  manifestSha256: "c".repeat(64),
};

it.each(["current", "parent-eof", "replay", "wrong-path", "wrong-run"] as const)(
  "binds the actual inherited channel and rejects %s loss of custody",
  async (fault) => {
    const script = `
      const { openDesktopNativeOwnerChannel } = await import(${JSON.stringify(moduleUrl)});
      const channel = openDesktopNativeOwnerChannel({ binding: ${JSON.stringify(binding)}, assertCurrent() {} }, 8, 9);
      try {
        await channel.refresh(); channel.assertCurrent(); process.stdout.write('qualified\\n');
        await new Promise(resolve => process.stdin.once('data', resolve));
        if (${JSON.stringify(fault)} === 'parent-eof') await new Promise(resolve => setTimeout(resolve, 50));
        else await channel.refresh();
        channel.assertCurrent(); process.stdout.write('current\\n');
      } catch { process.stdout.write('refused\\n'); }
      finally { channel.close(); process.stdin.destroy(); }
    `;
    const child = ChildProcess.spawn(process.execPath, ["--input-type=module", "-e", script], {
      stdio: [
        "pipe",
        "pipe",
        "pipe",
        "ignore",
        "ignore",
        "ignore",
        "ignore",
        "ignore",
        "pipe",
        "pipe",
      ],
    });
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    const descriptors: readonly (Stream.Readable | Stream.Writable | null | undefined)[] =
      child.stdio;
    const replies = descriptors[8] as Stream.Writable;
    const requests = descriptors[9] as Stream.Readable;
    let stdout = "";
    let stderr = "";
    let requestBuffer = "";
    let first: Record<string, unknown> | undefined;
    const runId = "d".repeat(64);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const finished = new Promise<void>((resolve, reject) => {
        timer = NodeTimers.setTimeout(
          () => reject(new Error(`Owner channel test timed out: ${stdout} ${stderr}`)),
          10000,
        );
        child.once("error", reject);
        child.stderr!.on("data", (data) => {
          stderr += String(data);
        });
        child.stdout!.on("data", (data) => {
          stdout += String(data);
          if (stdout.includes("qualified\n") && !child.stdin!.destroyed) {
            if (fault === "parent-eof") replies.end();
            child.stdin!.end("continue");
          }
        });
        requests.on("data", (data) => {
          requestBuffer += String(data);
          let newline: number;
          while ((newline = requestBuffer.indexOf("\n")) >= 0) {
            const request = JSON.parse(requestBuffer.slice(0, newline));
            requestBuffer = requestBuffer.slice(newline + 1);
            expect(request.childPid).toBe(child.pid);
            const response = { ...request, parentPid: process.pid, runId };
            if (first === undefined) first = response;
            else if (fault === "replay") Object.assign(response, first);
            else if (fault === "wrong-path")
              response.binding = { ...binding, databasePath: "/other/statev2.sqlite" };
            else if (fault === "wrong-run") response.runId = "e".repeat(64);
            replies.write(`${JSON.stringify(response)}\n`);
          }
        });
        child.once("close", (code) =>
          code === 0 ? resolve() : reject(new Error(`Child exited ${code}: ${stderr}`)),
        );
      });
      await finished;
      expect(stdout).toContain("qualified\n");
      expect(stdout).toContain(fault === "current" ? "current\n" : "refused\n");
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await closed;
      for (const stream of child.stdio) stream?.destroy();
    }
  },
);
