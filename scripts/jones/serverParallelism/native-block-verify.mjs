import { readFile, lstat, realpath } from "node:fs/promises";
import { join, dirname } from "node:path";
import { createHash } from "node:crypto";
import {
  nativeBlockReceiptPredicate,
  nativeBlockCohortRetired,
  parseLinuxWitness,
} from "./native-block-proof.mjs";
import { proveAbsent } from "../../../apps/server/src/jones/serverCompute/journal.ts";
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
async function ownedEvidence(path, directory) {
  if (dirname(path) !== directory || (await realpath(directory)) !== directory)
    throw new Error("Evidence outside case directory");
  const stat = await lstat(path);
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.size > 4 * 1024 * 1024 ||
    stat.uid !== process.getuid?.() ||
    (stat.mode & 0o077) !== 0
  )
    throw new Error("Unbound witness artifact");
  return readFile(path);
}
export async function verifyNativeBlockRetirement(receipt, directory, count) {
  try {
    if (receipt?.schema !== "jones.native-block-case/v1" || receipt.cleanupProved !== true)
      return false;
    for (const mode of ["release", "kill"]) {
      const cohort = JSON.parse(
        await ownedEvidence(join(directory, `${mode}-lifecycle.json`), directory),
      );
      if (!nativeBlockCohortRetired(cohort, count) || !cohort.candidate.children.every(proveAbsent))
        return false;
    }
    return true;
  } catch {
    return false;
  }
}

export async function verifyNativeBlockArtifacts(receipt, expected, directory) {
  try {
    const proof = receipt.proof;
    if (
      nativeBlockReceiptPredicate(proof, expected) !== true ||
      receipt.predicate !== true ||
      receipt.outcome !== "passed" ||
      receipt.cleanupProved !== true
    )
      return false;
    const bytes = await ownedEvidence(proof.witness.artifact, directory);
    if (
      digest(bytes) !== proof.witness.sha256 ||
      digest(await readFile(new URL("./native-block-proof.mjs", import.meta.url))) !==
        proof.witness.parserSha256 ||
      digest(await readFile(new URL("./native-block-observer.mjs", import.meta.url))) !==
        proof.witness.observerSha256
    )
      return false;
    const evidence = JSON.parse(bytes),
      killed = JSON.parse(await ownedEvidence(join(directory, "kill-lifecycle.json"), directory));
    const released = JSON.parse(
      await ownedEvidence(join(directory, "release-lifecycle.json"), directory),
    );
    if (
      evidence.schema !== "jones.native-block-witness/v1" ||
      !same(evidence.subject, killed.candidate?.subject) ||
      evidence.subject.build !== expected.build ||
      !same(evidence.binaryPaths, proof.binaryPaths) ||
      !same(evidence.binaryDigests, proof.binaryDigests) ||
      evidence.finished !== proof.timeline.witnessFinished ||
      killed.held !== true ||
      killed.lastHeldCheck !== proof.timeline.lastHeldCheck ||
      killed.killAt !== proof.timeline.kill ||
      killed.parentExit?.captured !== true ||
      killed.parentExit?.code !== null ||
      killed.parentExit?.signal !== "SIGKILL" ||
      killed.childrenAbsent !== true ||
      killed.sameJournalRecovered !== true ||
      killed.replacementUseful !== true ||
      killed.replacementAdmissions < 1 ||
      killed.cleanupProved !== true ||
      killed.guardian?.at !== proof.timeline.guardianAfterWitness ||
      !same(killed.guardian?.subject, killed.candidate?.subject) ||
      killed.issued?.requestIssued !== proof.timeline.requestIssued ||
      killed.issued?.armExpires !== proof.timeline.armExpires ||
      killed.candidate?.attempt !== proof.allocation.attempt ||
      !same(killed.candidate?.children, proof.allocation.registeredChildren)
    )
      return false;
    if (expected.os !== "linux-x64") return false; // No exact-path macOS evidence format is qualified yet.
    const journal = killed.journalBeforeKill;
    if (
      !journal ||
      journal.claim?.authorityId !== proof.subject.authorityId ||
      !same(
        journal.pids,
        journal.slots?.map((value) => value.pid),
      ) ||
      !same(
        [...journal.pids].sort((a, b) => a - b),
        [...proof.allocation.registeredChildren].sort((a, b) => a - b),
      ) ||
      journal.slots.some(
        (value) => value.state !== "registered" || value.authorityId !== proof.subject.authorityId,
      )
    )
      return false;
    const selectedGeneration = journal.slots.find((value) => value.slotId === proof.subject.slotId);
    if (
      selectedGeneration?.pid !== proof.subject.pid ||
      selectedGeneration.generation !== proof.subject.generation ||
      selectedGeneration.nonce !== proof.subject.nonce
    )
      return false;
    const parsed = parseLinuxWitness(
      evidence.raw,
      evidence.subject,
      killed.table.map((item) => item.path),
    );
    if (
      !parsed ||
      parsed.fifoPath !== proof.blocker.fifoPath ||
      parsed.mainTid !== proof.subject.mainTid ||
      !same({ ...evidence.subject, mainTid: parsed.mainTid }, proof.subject)
    )
      return false;
    const selected = killed.table.find((item) => item.path === parsed.fifoPath);
    if (
      !selected ||
      selected.device !== proof.blocker.device ||
      selected.inode !== proof.blocker.inode ||
      selected.mode !== 0o600 ||
      killed.table.length !== 20
    )
      return false;
    for (const name of ["fff", "ffi"]) {
      const path = proof.binaryPaths[name],
        stat = await lstat(path);
      if (
        !stat.isFile() ||
        stat.isSymbolicLink() ||
        stat.size > 128 * 1024 * 1024 ||
        !evidence.raw.maps.split("\n").some((line) => line.endsWith(" " + path)) ||
        digest(await readFile(path)) !== proof.binaryDigests[name]
      )
        return false;
    }
    const beforeBytes = await ownedEvidence(
      join(directory, "kill-prenative-witness.json"),
      directory,
    );
    const before = JSON.parse(beforeBytes);
    if (
      killed.preNative?.outcome !== "unknown" ||
      before.outcome !== "unknown" ||
      killed.preNative.finished > killed.issued.requestIssued ||
      digest(beforeBytes) !== killed.preNative.witness?.sha256 ||
      !same(before.subject, evidence.subject) ||
      parseLinuxWitness(
        before.raw,
        before.subject,
        killed.table.map((item) => item.path),
      )
    )
      return false;
    const negativeBytes = await ownedEvidence(join(directory, "release-witness.json"), directory);
    const negative = JSON.parse(negativeBytes);
    const negativeParsed = parseLinuxWitness(
      negative.raw,
      negative.subject,
      released.table.map((item) => item.path),
    );
    if (
      released.witness?.outcome !== true ||
      !negativeParsed ||
      released.normalResult !== true ||
      released.cleanupProved !== true ||
      released.childrenAbsent !== true ||
      released.parentExit?.captured !== true ||
      released.parentExit?.code !== 0 ||
      released.parentExit?.signal !== null ||
      negative.subject.build !== expected.build ||
      !same(negative.binaryDigests, proof.binaryDigests) ||
      digest(negativeBytes) !== released.witness.witness.sha256 ||
      negativeParsed.fifoPath !== released.witness.fifoPath ||
      negative.parserSha256 !== proof.witness.parserSha256 ||
      negative.observerSha256 !== proof.witness.observerSha256
    )
      return false;
    return true;
  } catch {
    return false;
  }
}
