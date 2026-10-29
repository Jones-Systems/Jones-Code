import { spawn } from "node:child_process";
import { readFile, writeFile, mkdir, lstat, rm } from "node:fs/promises";
import { join, isAbsolute } from "node:path";
import { setTimeout as pause } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import {
  GenerationJournal,
  proveAbsent,
} from "../../../apps/server/src/jones/serverCompute/journal.ts";
import { ComputeSupervisor } from "../../../apps/server/src/jones/serverCompute/supervisor.ts";
import { ProcessSlot } from "../../../apps/server/src/jones/serverCompute/processSlot.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
import {
  NATIVE_BLOCK_PHASE,
  NATIVE_BLOCK_SCHEMA,
  nativeBlockReceiptPredicate,
  nativeBlockCohortRetired,
} from "./native-block-proof.mjs";
import {
  prepareNativeBlockFifos,
  nativeBlockFifosUnchanged,
  witnessNativeBlock,
  releaseNativeBlockFifo,
} from "./native-block-observer.mjs";

const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
function mailbox(child) {
  const queue = [],
    pending = [];
  let failure,
    returned = false;
  const reject = (cause) => {
    failure = cause;
    for (const waiter of pending.splice(0)) waiter.reject(cause);
  };
  child.on("error", reject);
  child.on("message", (message) => {
    if (Buffer.byteLength(JSON.stringify(message)) > 16_384 || queue.length >= 32)
      return reject(new Error("Fixture IPC exceeded bound"));
    if (message.type === "request-returned") returned = true;
    if (message.type === "fixture-error") return reject(new Error(message.reason));
    const index = pending.findIndex((waiter) => waiter.type === message.type);
    if (index >= 0) pending.splice(index, 1)[0].resolve(message);
    else queue.push(message);
  });
  return {
    get returned() {
      return returned;
    },
    wait(type, ms = 10_000) {
      if (failure) return Promise.reject(failure);
      const index = queue.findIndex((message) => message.type === type);
      if (index >= 0) return Promise.resolve(queue.splice(index, 1)[0]);
      return new Promise((resolve, reject) => {
        const item = {
          type,
          resolve: (value) => {
            clearTimeout(timer);
            resolve(value);
          },
          reject: (cause) => {
            clearTimeout(timer);
            reject(cause);
          },
        };
        const timer = setTimeout(() => {
          const at = pending.indexOf(item);
          if (at >= 0) pending.splice(at, 1);
          reject(new Error(`Missing bounded fixture message: ${type}`));
        }, ms);
        pending.push(item);
      });
    },
  };
}
function capturedExit(child) {
  return new Promise((resolve) => {
    child.once("exit", (code, signal) => resolve({ code, signal, captured: true }));
    child.once("error", () => resolve({ code: null, signal: null, captured: false }));
  });
}
async function boundedExit(exited) {
  let timer;
  try {
    return await Promise.race([
      exited,
      new Promise((resolve) => {
        timer = setTimeout(() => resolve({ captured: false }), 7000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
async function journalBinding(directory, candidate, count) {
  const claim = JSON.parse(await readFile(join(directory, "journal", "pool.claim"), "utf8"));
  if (claim.authorityId !== candidate.subject.authorityId || claim.count !== count)
    throw new Error("Changed journal authority");
  const pids = [],
    slots = [];
  for (let i = 0; i < count; i++) {
    const record = JSON.parse(await readFile(join(directory, "journal", `slot-${i}.json`), "utf8"));
    if (
      record.state !== "registered" ||
      record.authorityId !== claim.authorityId ||
      record.slotId !== i ||
      !Number.isSafeInteger(record.pid) ||
      record.pid <= 0
    )
      throw new Error("Unregistered native generation");
    pids.push(record.pid);
    slots.push(record);
    if (
      i === candidate.subject.slotId &&
      (record.pid !== candidate.subject.pid ||
        record.generation !== candidate.subject.generation ||
        record.nonce !== candidate.subject.nonce)
    )
      throw new Error("Changed selected generation");
  }
  if (
    !same(
      [...pids].sort((a, b) => a - b),
      [...candidate.children].sort((a, b) => a - b),
    )
  )
    throw new Error("Captured cohort differs from journal");
  return { claim, pids, slots };
}
async function absence(pids) {
  const expires = performance.now() + 5000;
  while (!pids.every(proveAbsent) && performance.now() < expires) await pause(20);
  return pids.length > 0 && pids.every(proveAbsent);
}
async function fixture({ manifestPath, directory, profile, mode, artifacts }) {
  await mkdir(directory, { mode: 0o700 });
  const count = profile === "vps" ? 3 : 2;
  const child = spawn(
    process.execPath,
    [
      fileURLToPath(new URL("./native-block-parent.mjs", import.meta.url)),
      manifestPath,
      directory,
      profile,
      mode,
    ],
    {
      stdio: ["ignore", "ignore", "pipe", "ipc"],
      env: minimalWorkerEnvironment(process.env, false, false),
    },
  );
  const exited = capturedExit(child),
    box = mailbox(child);
  let stderrBytes = 0;
  child.stderr?.on("data", (bytes) => {
    stderrBytes += bytes.length;
  });
  let candidate,
    table,
    preNative,
    witness,
    reader,
    issued,
    guardian,
    lastHeldCheck,
    killAt,
    parentExit,
    held = "unknown",
    pids = [],
    cleanupProved = false,
    normalResult = false,
    sameJournalRecovered = false,
    replacementUseful = false,
    replacementAdmissions = 0,
    journalBeforeKill,
    cleanupRecovery = false,
    reason;
  try {
    await box.wait("productive-profile", 60_000);
    candidate = await box.wait("candidate");
    pids = candidate.children;
    if (
      candidate.usefulChildren !== count ||
      candidate.limit !== 4 ||
      candidate.attempt < 1 ||
      candidate.attempt > 4 ||
      !Number.isSafeInteger(child.pid)
    )
      throw new Error("Unbound fixture");
    const binding = await journalBinding(directory, candidate, count);
    journalBeforeKill = binding;
    if (binding.claim.parentPid !== child.pid)
      throw new Error("Journal parent is not captured parent");
    table = await prepareNativeBlockFifos(candidate.barrier, candidate.subject.pid);
    preNative = await witnessNativeBlock({
      subject: candidate.subject,
      capturedParentPid: child.pid,
      table,
      artifact: join(artifacts, `${mode}-prenative-witness.json`),
    });
    if (preNative.outcome !== "unknown")
      throw new Error("Pre-native allocation marker was accepted as native proof");
    child.send({ type: "admit" });
    issued = await box.wait("request-issued");
    if (!same(issued.subject, candidate.subject)) throw new Error("Changed request identity");
    witness = await witnessNativeBlock({
      subject: candidate.subject,
      capturedParentPid: child.pid,
      table,
      artifact: join(artifacts, `${mode}-witness.json`),
    });
    if (box.returned) throw new Error("Native request already returned");
    child.send({ type: "witness-finished", finished: witness.finished });
    guardian = await box.wait("guardian-after-witness", 3000);
    if (
      !same(guardian.subject, candidate.subject) ||
      guardian.outstanding !== true ||
      guardian.at <= witness.finished ||
      !Number.isSafeInteger(guardian.rssBytes) ||
      guardian.rssBytes <= 0
    )
      throw new Error("No independent post-observer guardian resource receipt");
    if (witness.outcome !== true) throw new Error(witness.reason);
    journalBeforeKill = await journalBinding(directory, candidate, count);
    const unchanged = await nativeBlockFifosUnchanged(table);
    lastHeldCheck = Date.now();
    if (!unchanged || box.returned || lastHeldCheck >= issued.armExpires - 1000)
      throw new Error("Expired or changed native barrier");
    if (mode === "release") {
      reader = await releaseNativeBlockFifo(table, witness.fifoPath);
      const completed = await box.wait("clean-result", 20_000);
      parentExit = await boundedExit(exited);
      normalResult =
        completed.normalResult === true &&
        completed.capturedRetirement === true &&
        parentExit.captured === true &&
        parentExit.code === 0 &&
        parentExit.signal === null;
      cleanupProved = normalResult && (await absence(pids));
    } else {
      killAt = Date.now();
      child.kill("SIGKILL"); // Sole test kill target is this directly captured synthetic parent.
      parentExit = await boundedExit(exited);
      held = unchanged && !box.returned && (await nativeBlockFifosUnchanged(table));
      const childrenAbsent = parentExit.captured === true && (await absence(pids));
      if (!childrenAbsent) {
        // Do not even acquire recovery/admit replacement while any old PID is present or unknown.
        reason = "Old generation absence unknown; replacement admissions remain zero";
      } else {
        const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
        const command = {
          ...manifest,
          env: minimalWorkerEnvironment(
            process.env,
            manifest.runtimeForm === "sea",
            manifest.runtimeForm === "electron",
          ),
        };
        const recovered = await GenerationJournal.acquire(join(directory, "journal"), count);
        sameJournalRecovered = recovered.claim.authorityId !== candidate.subject.authorityId;
        const replacement = new ComputeSupervisor({
          count,
          build: manifest.build,
          journal: recovered,
          start: (boot, register, onExit) => {
            replacementAdmissions++;
            return ProcessSlot.start(command, boot, register, onExit);
          },
        });
        try {
          const handle = replacement.register({
            cwd: join(directory, "workspace-0"),
            variant: "paths",
          });
          await replacement.ensure(handle);
          const result = await replacement.run(handle, "search", { query: "file", limit: 8 });
          replacementUseful = result.entries.length > 0;
        } finally {
          await replacement.shutdown();
        }
        cleanupProved = true;
      }
    }
  } catch (cause) {
    reason = cause instanceof Error ? cause.message : "Unknown native block fixture failure";
  } finally {
    // Error cleanup does not turn missing witness evidence into a passing kill proof.
    if (Number.isSafeInteger(child.pid) && child.exitCode === null && child.signalCode === null)
      child.kill("SIGKILL");
    parentExit ??= await boundedExit(exited);
    await reader?.close();
  }
  const childrenAbsent = pids.length === count && (await absence(pids)) ? true : "unknown";
  if (!cleanupProved && childrenAbsent === true && parentExit.captured === true) {
    try {
      // Cleanup recovery creates no native process and never substitutes for a successful witness/control.
      const recovered = await GenerationJournal.acquire(join(directory, "journal"), count);
      await recovered.release();
      cleanupRecovery = true;
      cleanupProved = true;
    } catch {
      /* Uncertain claims stay retained and charged. */
    }
  }
  const lifecycle = {
    mode,
    directory,
    candidate: candidate ?? null,
    issued: issued ?? null,
    preNative: preNative ?? null,
    witness: witness ?? null,
    guardian: guardian ?? null,
    parentExit,
    childrenAbsent,
    replacementAdmissions,
    sameJournalRecovered,
    replacementUseful,
    cleanupProved,
    cleanupRecovery,
    journalBeforeKill: journalBeforeKill ?? null,
    reason: reason ?? null,
    stderrBytes,
    table: table ?? null,
    held,
    lastHeldCheck: lastHeldCheck ?? null,
    killAt: killAt ?? null,
    normalResult,
  };
  await writeFile(
    join(artifacts, `${mode}-lifecycle.json`),
    JSON.stringify(lifecycle, null, 2) + "\n",
    { flag: "wx", mode: 0o600 },
  );
  return { ...lifecycle, held, lastHeldCheck, killAt, normalResult };
}

export async function runNativeBlockCase({ manifestPath, scratch, output, profile }) {
  if (![manifestPath, scratch, output].every(isAbsolute) || !["laptop", "vps"].includes(profile))
    throw new Error("Unbound native block case");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  const allowed = ["schema", "runtimeForm", "file", "args", "build", "identityResolver"];
  if (
    Object.keys(manifest).some((key) => !allowed.includes(key)) ||
    (manifest.identityResolver !== undefined &&
      (manifest.runtimeForm !== "source" ||
        typeof manifest.identityResolver !== "string" ||
        !isAbsolute(manifest.identityResolver))) ||
    manifest.schema !== "jones.server-parallelism.command/v1" ||
    !["source", "bundle", "electron", "sea"].includes(manifest.runtimeForm) ||
    !/^[a-f0-9]{64}$/.test(manifest.build) ||
    !isAbsolute(manifest.file ?? "") ||
    !Array.isArray(manifest.args) ||
    !manifest.args.every(
      (value) =>
        typeof value === "string" && value.length <= 4096 && !/[\x00-\x1f\x7f]/.test(value),
    ) ||
    (manifest.runtimeForm === "sea"
      ? manifest.args.length !== 1
      : manifest.args.length !== 3 ||
        manifest.args[0] !== "--max-old-space-size=256" ||
        !isAbsolute(manifest.args[1])) ||
    manifest.args.at(-1) !== "__jones-server-compute"
  )
    throw new Error("Invalid candidate command manifest");
  await mkdir(output, { mode: 0o700 });
  const negative = await fixture({
    manifestPath,
    directory: join(scratch, "release"),
    profile,
    mode: "release",
    artifacts: output,
  });
  const killed =
    nativeBlockCohortRetired(negative, profile === "vps" ? 3 : 2)
      ? await fixture({
          manifestPath,
          directory: join(scratch, "kill"),
          profile,
          mode: "kill",
          artifacts: output,
        })
      : {
          held: "unknown",
          parentExit: { captured: false },
          childrenAbsent: "unknown",
          sameJournalRecovered: false,
          replacementUseful: false,
          cleanupProved: false,
          reason: "Negative-control retirement unproved; zero subsequent native admissions",
        };
  const os =
    process.platform === "linux" && process.arch === "x64"
      ? "linux-x64"
      : process.platform === "darwin" && process.arch === "arm64"
        ? "macos-arm64"
        : "unsupported";
  const subject = killed.candidate?.subject;
  const selected = killed.table?.find((item) => item.path === killed.witness?.fifoPath);
  const proof = {
    schema: NATIVE_BLOCK_SCHEMA,
    phase: NATIVE_BLOCK_PHASE,
    os,
    form: manifest.runtimeForm,
    profile,
    subject: subject
      ? { ...subject, mainTid: killed.witness?.mainTid ?? "unknown" }
      : {
          pid: "unknown",
          mainTid: "unknown",
          build: manifest.build,
          authorityId: "unknown",
          slotId: "unknown",
          generation: "unknown",
          nonce: "unknown",
          requestId: "unknown",
        },
    binaryDigests: killed.witness?.binaryDigests ?? { fff: "unknown", ffi: "unknown" },
    binaryPaths: killed.witness?.binaryPaths ?? { fff: "unknown", ffi: "unknown" },
    witness: killed.witness?.witness ?? {
      method: os === "linux-x64" ? "linux-syscall-path" : "macos-native-stack",
      artifact: killed.witness?.artifact ?? "unknown",
      sha256: "unknown",
      parserSha256: "unknown",
      observerSha256: "unknown",
    },
    blockerHeldThroughKill: killed.held,
    parentCapturedSigkill:
      killed.killAt !== undefined &&
      killed.parentExit.captured === true &&
      killed.parentExit.code === null &&
      killed.parentExit.signal === "SIGKILL",
    childrenAbsent: killed.childrenAbsent,
    sameJournalRecovered: killed.sameJournalRecovered === true ? true : "unknown",
    replacementUseful: killed.replacementUseful === true ? true : "unknown",
    blocker: {
      fifoPath: selected?.path ?? "unknown",
      device: selected?.device ?? "unknown",
      inode: selected?.inode ?? "unknown",
      mode: selected?.mode ?? "unknown",
      preparedNames: killed.table?.length ?? 0,
      noReader: killed.held,
      unchanged: killed.held,
    },
    timeline: {
      requestIssued: killed.issued?.requestIssued ?? "unknown",
      witnessFinished: killed.witness?.finished ?? "unknown",
      guardianAfterWitness: killed.guardian?.at ?? "unknown",
      lastHeldCheck: killed.lastHeldCheck ?? "unknown",
      kill: killed.killAt ?? "unknown",
      armExpires: killed.issued?.armExpires ?? "unknown",
    },
    allocation: {
      attempt: killed.candidate?.attempt ?? "unknown",
      limit: 4,
      registeredChildren: killed.candidate?.children ?? [],
      outstandingThroughKill: killed.held,
    },
    guardian: {
      postWitnessResources: killed.guardian ? true : "unknown",
      generationUnchanged: killed.guardian ? true : "unknown",
    },
    negativeControl: {
      witnessed: negative.witness?.outcome === true ? true : "unknown",
      exactFifoReleased: negative.normalResult ? true : "unknown",
      normalResult: negative.normalResult ? true : "unknown",
      cleanRetirement: negative.cleanupProved,
    },
    noWitnessRejected: killed.preNative?.outcome === "unknown" ? true : "unknown",
    cleanupProved: negative.cleanupProved && killed.cleanupProved,
  };
  // A pre-native message and a completed subprocess cannot satisfy the closed receipt gate.
  if (
    nativeBlockReceiptPredicate(
      { ...proof, witness: null },
      { os, form: manifest.runtimeForm, profile, build: manifest.build },
    ) !== "unknown"
  )
    throw new Error("No-witness control accepted");
  const predicate = nativeBlockReceiptPredicate(proof, {
    os,
    form: manifest.runtimeForm,
    profile,
    build: manifest.build,
  });
  const result = {
    schema: "jones.native-block-case/v1",
    outcome: predicate === true ? "passed" : predicate === false ? "failed" : "unknown",
    predicate,
    proof,
    cleanupProved: proof.cleanupProved,
    reasons: [negative.reason, killed.reason].filter(Boolean),
    exactProductiveNativePhaseAtKill: "unknown",
  };
  await writeFile(join(output, "receipt.json"), JSON.stringify(result, null, 2) + "\n", {
    flag: "wx",
    mode: 0o600,
  });
  if (proof.cleanupProved) {
    for (const directory of [join(scratch, "release"), join(scratch, "kill")]) {
      if ((await lstat(directory)).isSymbolicLink()) throw new Error("Changed fixture scratch");
      await rm(directory, { recursive: true });
    }
  }
  return result;
}
