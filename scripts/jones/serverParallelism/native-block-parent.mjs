// Dedicated synthetic parent. It owns FD3 endpoints; observers inherit none of them.
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { join, isAbsolute } from "node:path";
import { ComputeSupervisor } from "../../../apps/server/src/jones/serverCompute/supervisor.ts";
import { ProcessSlot } from "../../../apps/server/src/jones/serverCompute/processSlot.ts";
import { GenerationJournal } from "../../../apps/server/src/jones/serverCompute/journal.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
import { LIMITS } from "../../../apps/server/src/jones/serverCompute/protocol.ts";
const [manifestPath, directory, profile, mode] = process.argv.slice(2);
if (process.argv.length !== 6 || !isAbsolute(manifestPath ?? "") || !isAbsolute(directory ?? "") ||
    !["laptop", "vps"].includes(profile) || !["kill", "release"].includes(mode) || !process.send)
  throw new Error("native-block-parent requires bound fixture IPC and exact arguments");
const count = profile === "vps" ? 3 : 2;
const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const targetWorkspace = join(directory, "blocked-workspace"), barrier = join(directory, "barrier");
await mkdir(barrier, { mode: 0o700 });
await mkdir(targetWorkspace, { mode: 0o700 });
await writeFile(join(targetWorkspace, "file-native-block.txt"), "useful native block result\n", { flag: "wx", mode: 0o600 });
const qualifier = "--qualification-native-block=" + JSON.stringify({
  targetWorkspace, logFileHint: join(barrier, "native-block.log"),
});
const command = { ...manifest, args: [...manifest.args, qualifier], env: minimalWorkerEnvironment(
  process.env, manifest.runtimeForm === "sea", manifest.runtimeForm === "electron") };
const journal = await GenerationJournal.acquire(join(directory, "journal"), count);
const captured = new Map(), attempts = new Map();
let pending, witnessFinished, admitted, permit, finished = false, guardianSent = false;
const emit = (message) => process.send?.(message);
process.on("message", (message) => {
  if (message?.type === "admit" && pending && !admitted) {
    admitted = true;
    permit?.();
  } else if (message?.type === "witness-finished" && admitted && !witnessFinished &&
      Number.isSafeInteger(message.finished) && message.finished <= Date.now()) {
    witnessFinished = message.finished;
  }
});
const supervisor = new ComputeSupervisor({ count, build: command.build, journal,
  start: async (boot, register, onExit) => {
    const child = await ProcessSlot.start(command, boot, register, onExit, (event) => {
      if (event.phase !== "resources" || !pending || !witnessFinished || guardianSent ||
          event.slotId !== pending.subject.slotId || event.generation !== pending.subject.generation ||
          Date.now() <= witnessFinished) return;
      guardianSent = true;
      emit({ type: "guardian-after-witness", subject: pending.subject, at: Date.now(),
        outstanding: true, rssBytes: event.rssBytes });
    });
    captured.set(child.pid, child);
    return { pid: child.pid, bootstrap: child.bootstrap,
      get runtimeMetadata() { return child.runtimeMetadata; },
      request: async (request, signal, observeTiming) => {
        if (request.operation === "open") attempts.set(child.pid, (attempts.get(child.pid) ?? 0) + 1);
        if (request.operation !== "open" || request.input?.cwd !== targetWorkspace)
          return child.request(request, signal, observeTiming);
        if (pending || captured.size !== count || attempts.get(child.pid) > 4 || boot.slotGeneration !== 1)
          throw new Error("Unqualified generation/allocation");
        const subject = { pid: child.pid, mainTid: String(child.pid), build: boot.build,
          authorityId: boot.authorityId, slotId: boot.slotId, generation: boot.slotGeneration,
          nonce: boot.nonce, requestId: request.requestId };
        pending = { subject };
        const gate = new Promise((resolve) => { permit = resolve; });
        emit({ type: "candidate", subject, barrier, children: [...captured.keys()],
          attempt: attempts.get(child.pid), limit: LIMITS.residents, usefulChildren: count });
        await gate;
        const requestIssued = Date.now();
        emit({ type: "request-issued", subject, requestIssued, armExpires: requestIssued + LIMITS.jobMs });
        try { return await child.request(request, signal, observeTiming); }
        finally { pending = undefined; emit({ type: "request-returned", subject }); }
      },
      retire: async () => { await child.retire(); captured.delete(child.pid); },
    };
  },
});
const timeout = setTimeout(() => { void supervisor.shutdown().catch(() => {}); }, 90_000);
process.once("disconnect", () => { if (!finished) void supervisor.shutdown().catch(() => {}); });
try {
  const handles = [];
  for (let i = 0; i < count; i++) {
    const cwd = join(directory, `workspace-${i}`);
    await mkdir(cwd, { mode: 0o700 });
    await writeFile(join(cwd, `file-${i}.txt`), "productive pinned native search\n", { flag: "wx", mode: 0o600 });
    handles.push(supervisor.register({ cwd, variant: "paths" }));
  }
  await Promise.all(handles.map(async (handle) => {
    await supervisor.ensure(handle);
    const result = await supervisor.run(handle, "search", { query: "file", limit: 8 });
    if (!result.entries.length) throw new Error("No useful native result");
  }));
  if (captured.size !== count) throw new Error("Incomplete productive child profile");
  emit({ type: "productive-profile", children: [...captured.keys()], count });
  const target = supervisor.register({ cwd: targetWorkspace, variant: "paths" });
  await supervisor.ensure(target);
  const result = await supervisor.run(target, "search", { query: "file", limit: 8 });
  if (!result.entries.length) throw new Error("Released native finder was not useful");
  await supervisor.shutdown();
  finished = true;
  emit({ type: "clean-result", normalResult: true, capturedRetirement: captured.size === 0 });
} catch (cause) {
  emit({ type: "fixture-error", reason: cause instanceof Error ? cause.message : "fixture failed" });
  await supervisor.shutdown().catch(() => {});
  process.exitCode = 1;
} finally {
  clearTimeout(timeout);
  process.disconnect?.();
}
