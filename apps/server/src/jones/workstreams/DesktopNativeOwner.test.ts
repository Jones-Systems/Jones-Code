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
