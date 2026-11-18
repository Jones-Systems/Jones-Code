import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import {
  nativeBlockReceiptPredicate,
  nativeBlockCohortRetired,
  parseDarwinWitness,
  parseLinuxWitness,
} from "./native-block-proof.mjs";
import { proveAbsent } from "../../../apps/server/src/jones/serverCompute/journal.ts";
const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
async function ownedEvidence(path, directory) {
  if (NodePath.dirname(path) !== directory || (await NodeFSP.realpath(directory)) !== directory)
    throw new Error("Evidence outside case directory");
  const stat = await NodeFSP.lstat(path);
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.size > 4 * 1024 * 1024 ||
    stat.uid !== process.getuid?.() ||
    (stat.mode & 0o077) !== 0
  )
    throw new Error("Unbound witness artifact");
  return NodeFSP.readFile(path);
}
export async function verifyNativeBlockRetirement(receipt, directory, count) {
  try {
    if (receipt?.schema !== "jones.native-block-case/v1" || receipt.cleanupProved !== true)
      return false;
    for (const mode of ["release", "kill"]) {
      const cohort = JSON.parse(
        await ownedEvidence(NodePath.join(directory, `${mode}-lifecycle.json`), directory),
      );
      if (!nativeBlockCohortRetired(cohort, count) || !cohort.candidate.children.every(proveAbsent))
        return false;
    }
    return true;
  } catch {
    return false;
  }
}
async function parseRetainedWitness(os, evidence, cohort, mode, directory) {
  if (os === "linux-x64") {
    return parseLinuxWitness(
      evidence.raw,
      evidence.subject,
      cohort.table.map((item) => item.path),
    );
  }
  if (os !== "macos-arm64") return undefined;
  const samplePath = NodePath.join(directory, `${mode}-witness.json.sample`);
  const sample = await ownedEvidence(samplePath, directory);
  if (
    evidence.raw?.sampleArtifact !== samplePath ||
    evidence.raw.sampleSha256 !== digest(sample) ||
    evidence.raw.sample !== sample.toString("utf8")
  )
    return undefined;
  return parseDarwinWitness(
    evidence.raw,
    evidence.subject,
    cohort.table,
    { issued: cohort.issued?.requestIssued, finished: evidence.finished },
    evidence.binaryDigests,
  );
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
      digest(await NodeFSP.readFile(new URL("./native-block-proof.mjs", import.meta.url))) !==
        proof.witness.parserSha256 ||
      digest(await NodeFSP.readFile(new URL("./native-block-observer.mjs", import.meta.url))) !==
        proof.witness.observerSha256
    )
      return false;
    const evidence = JSON.parse(bytes),
      killed = JSON.parse(
        await ownedEvidence(NodePath.join(directory, "kill-lifecycle.json"), directory),
      );
    const released = JSON.parse(
      await ownedEvidence(NodePath.join(directory, "release-lifecycle.json"), directory),
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
    const parsed = await parseRetainedWitness(expected.os, evidence, killed, "kill", directory);
    if (
      !parsed ||
      parsed.fifoPath !== proof.blocker.fifoPath ||
      parsed.mainTid !== proof.subject.mainTid ||
      (expected.os === "macos-arm64" && !same(parsed.binaryPaths, proof.binaryPaths)) ||
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
        stat = await NodeFSP.lstat(path);
      if (
        !stat.isFile() ||
        stat.isSymbolicLink() ||
        stat.size > 128 * 1024 * 1024 ||
        (expected.os === "linux-x64" &&
          !evidence.raw.maps.split("\n").some((line) => line.endsWith(" " + path))) ||
        digest(await NodeFSP.readFile(path)) !== proof.binaryDigests[name]
      )
        return false;
    }
    const beforeBytes = await ownedEvidence(
      NodePath.join(directory, "kill-prenative-witness.json"),
      directory,
    );
    const before = JSON.parse(beforeBytes);
    if (
      killed.preNative?.outcome !== "unknown" ||
      before.outcome !== "unknown" ||
      killed.preNative.finished > killed.issued.requestIssued ||
      digest(beforeBytes) !== killed.preNative.witness?.sha256 ||
      !same(before.subject, evidence.subject) ||
      (expected.os === "linux-x64"
        ? parseLinuxWitness(
            before.raw,
            before.subject,
            killed.table.map((item) => item.path),
          )
        : before.raw?.mach !== undefined)
    )
      return false;
    const negativeBytes = await ownedEvidence(
      NodePath.join(directory, "release-witness.json"),
      directory,
    );
    const negative = JSON.parse(negativeBytes);
    const negativeParsed = await parseRetainedWitness(
      expected.os,
      negative,
      released,
      "release",
      directory,
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
      (expected.os === "macos-arm64" && !same(negativeParsed.binaryPaths, negative.binaryPaths)) ||
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
