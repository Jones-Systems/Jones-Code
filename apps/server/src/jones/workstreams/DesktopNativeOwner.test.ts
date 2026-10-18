// @effect-diagnostics nodeBuiltinImport:off -- Each test owns inherited pipes and retires its child before returning.
// @effect-diagnostics globalTimers:off -- These timers are owned and cleared by the native inherited-pipe lifecycle.
import * as NodeTimers from "node:timers";
import * as NodeChildProcess from "node:child_process";
import type * as NodeStream from "node:stream";
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
    const child = NodeChildProcess.spawn(process.execPath, ["--input-type=module", "-e", script], {
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
    const descriptors: readonly (NodeStream.Readable | NodeStream.Writable | null | undefined)[] =
      child.stdio;
    const replies = descriptors[8] as NodeStream.Writable;
    const requests = descriptors[9] as NodeStream.Readable;
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

it("permanently loses qualification when the actual channel parent exits", async () => {
  const childScript = [
    "const {openDesktopNativeOwnerChannel}=await import(" + JSON.stringify(moduleUrl) + ");",
    "const {writeSync,closeSync}=await import('node:fs');const parentPid=process.ppid;let timedOut=false;",
    "const channel=openDesktopNativeOwnerChannel({binding:" +
      JSON.stringify(binding) +
      ",assertCurrent(){}},8,9);",
    "const deadline=setTimeout(()=>{timedOut=true;channel.close();process.exitCode=2;},2000);",
    "try{await channel.refresh();channel.assertCurrent();process.stdout.write('qualified\\n');writeSync(10,'qualified\\n');closeSync(10);",
    "while(process.ppid===parentPid){if(timedOut)throw new Error('Channel parent did not exit');await new Promise(resolve=>setTimeout(resolve,1));}",
    "let refused=false;try{channel.assertCurrent();}catch{refused=true;}if(!refused)throw new Error('Dead parent remained qualified');",
    "let retryRefused=false;try{await channel.refresh();}catch{retryRefused=true;}if(!retryRefused)throw new Error('Dead parent reconnected');process.stdout.write('parent-loss-refused\\n');}",
    "finally{clearTimeout(deadline);channel.close();}",
  ].join("\n");
  const parentScript = [
    "const {spawn}=await import('node:child_process');",
    "const child=spawn(process.execPath,['--input-type=module','-e'," +
      JSON.stringify(childScript) +
      "],{stdio:['ignore',1,2,'ignore','ignore','ignore','ignore','ignore','pipe','pipe','pipe']});",
    "process.once('SIGTERM',()=>{child.kill('SIGKILL');child.once('close',()=>process.exit(1));});",
    "child.once('close',()=>process.exit(1));",
    "let acknowledgment='';child.stdio[10].on('data',data=>{acknowledgment+=String(data);if(acknowledgment==='qualified\\n')process.exit(0);});",
    "let buffer='';child.stdio[9].on('data',data=>{buffer+=String(data);const at=buffer.indexOf('\\n');if(at<0)return;",
    "const request=JSON.parse(buffer.slice(0,at));child.stdio[8].write(JSON.stringify({...request,parentPid:process.pid,runId:'d'.repeat(64)})+'\\n');});",
  ].join("\n");
  const parent = NodeChildProcess.spawn(
    process.execPath,
    ["--input-type=module", "-e", parentScript],
    {
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const closed = new Promise<void>((resolve) => parent.once("close", () => resolve()));
  let stdout = "";
  let stderr = "";
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      timer = NodeTimers.setTimeout(
        () => reject(new Error("Parent-death fixture timed out: " + stderr)),
        10000,
      );
      parent.stdout!.on("data", (data) => {
        stdout += String(data);
      });
      parent.stderr!.on("data", (data) => {
        stderr += String(data);
      });
      parent.once("error", reject);
      parent.once("close", (code) =>
        code === 0 ? resolve() : reject(new Error("Parent fixture failed: " + stderr)),
      );
    });
    expect(stdout).toContain("qualified\n");
    expect(stdout).toContain("parent-loss-refused\n");
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (parent.exitCode === null && parent.signalCode === null) parent.kill("SIGTERM");
    await closed;
    for (const stream of parent.stdio) stream?.destroy();
  }
});

it.each(["channel", "session"] as const)(
  "recovers the same established %s only after a late reply drains and fresh proof arrives",
  async (surface) => {
    const script = `
    const admission = { binding: ${JSON.stringify(binding)}, assertCurrent() {} };
    if (${JSON.stringify(surface)} === 'session') {
      const { registerHooks } = await import('node:module');
      const fixture = 'data:text/javascript,' + encodeURIComponent('export const holdJonesNativeWriterFence = () => ({ binding: ' + JSON.stringify(admission.binding) + ', assertCurrent() {} });');
      registerHooks({ resolve(specifier, context, next) {
        return specifier === '@t3tools/shared/jones/nativeWriterFence' ? { url: fixture, shortCircuit: true } : next(specifier, context);
      } });
    }
    const { openDesktopNativeOwnerChannel, openDesktopNativeOwnerSession } = await import(${JSON.stringify(moduleUrl)});
    const parentPid = process.ppid;
    const channel = ${JSON.stringify(surface)} === 'channel'
      ? openDesktopNativeOwnerChannel(admission, 8, 9)
      : openDesktopNativeOwnerSession({ home: '/fixture', databasePath: admission.binding.databasePath,
          authorityDir: '/fixture/native-store-authority', version: 'fixture-version', buildMetadata: {},
          descriptorPath: undefined, inputFd: 8, outputFd: 9 });
    const report = value => process.stdout.write(JSON.stringify(value) + '\\n');
    const refused = () => { try { channel.assertCurrent(); return false; } catch { return true; } };
    try {
      await channel.refresh(); channel.assertCurrent(); report({ phase: 'qualified' });
      for (let round = 0; round < 2; round++) {
        let timeout;
        try { await channel.refresh(); } catch (error) { timeout = { code: error.code, message: error.message }; }
        admission.assertCurrent();
        const attempts = await Promise.allSettled(Array.from({ length: 20 }, () => channel.refresh()));
        report({ phase: 'gap', round, refused: refused(), timeout, repeatedRefusals: attempts.every(value => value.status === 'rejected'), available: channel.available });
        await new Promise(resolve => process.stdin.once('data', resolve));
        admission.assertCurrent();
        report({ phase: 'after-late', round, refused: refused(), parentCurrent: process.ppid === parentPid, childPid: process.pid });
        try {
          await channel.refresh(); channel.assertCurrent(); report({ phase: 'recovered', round });
        } catch (error) {
          report({ phase: 'unrecovered', round, code: error.code, message: error.message }); break;
        }
      }
    } finally { channel.close(); process.stdin.destroy(); }
  `;
    const child = NodeChildProcess.spawn(process.execPath, ["--input-type=module", "-e", script], {
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
    const descriptors: readonly (NodeStream.Readable | NodeStream.Writable | null | undefined)[] =
      child.stdio;
    const replies = descriptors[8] as NodeStream.Writable;
    const requests = descriptors[9] as NodeStream.Readable;
    const observations: Record<string, unknown>[] = [];
    const lateWrites: { sequence: number; elapsedMs: number; error: string | null }[] = [];
    const delayed: ReturnType<typeof setTimeout>[] = [];
    let deadline: ReturnType<typeof setTimeout> | undefined;
    let stdout = "";
    let requestBuffer = "";
    let stderr = "";
    let requestCount = 0;
    let freshReply: string | undefined;
    let allowFreshReply = false;
    let childAliveAfterLate = false;
    // A timed-out pre-fix reader destroys its pipe; retain that actual write outcome.
    replies.on("error", () => {});
    try {
      await new Promise<void>((resolve, reject) => {
        deadline = NodeTimers.setTimeout(
          () => reject(new Error(`Delayed owner fixture timed out: ${stderr}`)),
          15000,
        );
        child.once("error", reject);
        child.stderr!.on("data", (chunk) => {
          stderr += String(chunk);
        });
        child.stdout!.on("data", (chunk) => {
          stdout += String(chunk);
          let newline: number;
          while ((newline = stdout.indexOf("\n")) >= 0) {
            const observation = JSON.parse(stdout.slice(0, newline)) as Record<string, unknown>;
            stdout = stdout.slice(newline + 1);
            observations.push(observation);
            if (observation.phase === "after-late") {
              childAliveAfterLate = child.exitCode === null && child.signalCode === null;
              allowFreshReply = true;
              if (freshReply !== undefined) {
                replies.write(freshReply);
                freshReply = undefined;
              }
            }
          }
        });
        requests.on("data", (chunk) => {
          requestBuffer += String(chunk);
          let newline: number;
          while ((newline = requestBuffer.indexOf("\n")) >= 0) {
            const request = JSON.parse(requestBuffer.slice(0, newline));
            requestBuffer = requestBuffer.slice(newline + 1);
            requestCount++;
            const reply =
              JSON.stringify({ ...request, parentPid: process.pid, runId: "d".repeat(64) }) + "\n";
            if (request.sequence === 1) replies.write(reply);
            else if (request.sequence === 2 || request.sequence === 4) {
              allowFreshReply = false;
              const sentAt = performance.now();
              delayed.push(
                NodeTimers.setTimeout(() => {
                  replies.write(reply, (error) => {
                    lateWrites.push({
                      sequence: request.sequence,
                      elapsedMs: performance.now() - sentAt,
                      error: error?.message ?? null,
                    });
                    if (error) child.stdin!.write("prove\n");
                  });
                }, 3500),
              );
            } else {
              // Hold fresh proof until the live child proves that late proof alone is insufficient.
              freshReply = reply;
              child.stdin!.write("prove\n");
              if (allowFreshReply) {
                replies.write(freshReply);
                freshReply = undefined;
              }
            }
          }
        });
        child.once("close", (code) =>
          code === 0 ? resolve() : reject(new Error(`Child exited ${code}: ${stderr}`)),
        );
      });
      const evidence = JSON.stringify({
        observations,
        lateWrites,
        requestCount,
        childAliveAfterLate,
      });
      expect(observations[0], evidence).toEqual({ phase: "qualified" });
      expect(
        observations.find((value) => value.phase === "gap"),
        evidence,
      ).toMatchObject({
        refused: true,
        timeout: { code: "launcher_upgrade_required" },
        repeatedRefusals: true,
      });
      expect(lateWrites[0]!.elapsedMs, evidence).toBeGreaterThanOrEqual(3000);
      expect(
        observations.find((value) => value.phase === "after-late"),
        evidence,
      ).toMatchObject({
        refused: true,
        parentCurrent: true,
        childPid: child.pid,
      });
      expect(childAliveAfterLate, evidence).toBe(true);
      expect(
        observations.filter((value) => value.phase === "recovered"),
        evidence,
      ).toHaveLength(2);
      expect(lateWrites, evidence).toHaveLength(2);
      expect(
        lateWrites.every((value) => value.error === null),
        evidence,
      ).toBe(true);
      expect(requestCount, evidence).toBe(5);
      expect(stderr.match(/\[jones\/desktop-native-owner\] suspended/g)).toHaveLength(2);
      expect(stderr.match(/\[jones\/desktop-native-owner\] resumed/g)).toHaveLength(2);
      expect(stderr.match(/\[jones\/desktop-native-owner\] closed/g)).toHaveLength(1);
      if (surface === "session") {
        expect(
          observations
            .filter((value) => value.phase === "gap")
            .every((value) => value.available === false),
          evidence,
        ).toBe(true);
      }
    } finally {
      if (deadline !== undefined) clearTimeout(deadline);
      for (const timer of delayed) clearTimeout(timer);
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await closed;
      for (const stream of child.stdio) stream?.destroy();
    }
  },
);

it.each([
  "initial-timeout",
  "malformed",
  "replay",
  "wrong-binding",
  "wrong-run",
  "duplicate",
  "admission",
] as const)("keeps %s terminal instead of recovering a suspended channel", async (fault) => {
  const script = `
      const { openDesktopNativeOwnerChannel } = await import(${JSON.stringify(moduleUrl)});
      let admitted = true;
      const channel = openDesktopNativeOwnerChannel({ binding: ${JSON.stringify(binding)}, assertCurrent() {
        if (!admitted) throw new Error('Synthetic admission changed');
      } }, 8, 9);
      try {
        if (${JSON.stringify(fault)} !== 'initial-timeout') await channel.refresh();
        let timedOut = false;
        try { await channel.refresh(); } catch { timedOut = true; }
        if (!timedOut) throw new Error('Expected timeout');
        process.stdout.write('timed-out\\n');
        if (${JSON.stringify(fault)} !== 'initial-timeout') {
          await new Promise(resolve => process.stdin.once('data', resolve));
          if (${JSON.stringify(fault)} === 'admission') admitted = false;
          await new Promise(resolve => setTimeout(resolve, 30));
        }
        let refused = false;
        try { await channel.refresh(); } catch { refused = true; }
        if (!refused || channel.suspended) throw new Error('Terminal fault retained recoverable proof');
        try { channel.assertCurrent(); throw new Error('Fault remained qualified'); }
        catch (error) { if (error.message === 'Fault remained qualified') throw error; }
        process.stdout.write('terminal\\n');
      } finally { channel.close(); process.stdin.destroy(); }
    `;
  const child = NodeChildProcess.spawn(process.execPath, ["--input-type=module", "-e", script], {
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
  const descriptors: readonly (NodeStream.Readable | NodeStream.Writable | null | undefined)[] =
    child.stdio;
  const replies = descriptors[8] as NodeStream.Writable;
  const requests = descriptors[9] as NodeStream.Readable;
  let stdout = "";
  let stderr = "";
  let buffer = "";
  let first: Record<string, unknown> | undefined;
  let requestCount = 0;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  let delayed: ReturnType<typeof setTimeout> | undefined;
  replies.on("error", () => {});
  try {
    await new Promise<void>((resolve, reject) => {
      deadline = NodeTimers.setTimeout(
        () => reject(new Error(`Terminal fixture timed out: ${stderr}`)),
        8000,
      );
      child.once("error", reject);
      child.stdout!.on("data", (chunk) => {
        stdout += String(chunk);
      });
      child.stderr!.on("data", (chunk) => {
        stderr += String(chunk);
      });
      requests.on("data", (chunk) => {
        buffer += String(chunk);
        let newline: number;
        while ((newline = buffer.indexOf("\n")) >= 0) {
          const request = JSON.parse(buffer.slice(0, newline));
          buffer = buffer.slice(newline + 1);
          requestCount++;
          if (fault === "initial-timeout") continue;
          const response = { ...request, parentPid: process.pid, runId: "d".repeat(64) };
          if (first === undefined) {
            first = response;
            replies.write(JSON.stringify(response) + "\n");
            continue;
          }
          delayed = NodeTimers.setTimeout(() => {
            if (fault === "admission") {
              child.stdin!.write("prove\n");
              return;
            }
            if (fault === "replay") Object.assign(response, first);
            if (fault === "wrong-binding")
              response.binding = { ...binding, environmentId: "other" };
            if (fault === "wrong-run") response.runId = "e".repeat(64);
            const reply = fault === "malformed" ? "{invalid}\n" : JSON.stringify(response) + "\n";
            replies.write(fault === "duplicate" ? reply + reply : reply, () =>
              child.stdin!.write("prove\n"),
            );
          }, 3500);
        }
      });
      child.once("close", (code) =>
        code === 0 ? resolve() : reject(new Error(`Child exited ${code}: ${stderr}`)),
      );
    });
    expect(stdout).toContain("timed-out\n");
    expect(stdout).toContain("terminal\n");
    expect(requestCount).toBe(fault === "initial-timeout" ? 1 : 2);
    expect(stderr).not.toContain("resumed");
  } finally {
    if (deadline !== undefined) clearTimeout(deadline);
    if (delayed !== undefined) clearTimeout(delayed);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await closed;
    for (const stream of child.stdio) stream?.destroy();
  }
});
