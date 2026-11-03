import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import {
  parseLinuxSyscall,
  parseLinuxWitness,
  parseMacSample,
  nativeBlockReceiptPredicate,
  nativeBlockCohortRetired,
} from "./native-block-proof.mjs";
import { nativeBlockTestProof } from "./native-block-proof-fixture.mjs";
import { readBoundedEvidence } from "./native-block-observer.mjs";
const H = "a".repeat(64);
const expected = { os: "linux-x64", form: "source", profile: "laptop", build: H };
const valid = nativeBlockTestProof;

NodeTest.test(
  "OS evidence reads continue through short pages and retain the aggregate byte bound",
  async () => {
    const reader = (text) => {
      const bytes = Buffer.from(text);
      let position = 0;
      return {
        async read(buffer, offset, length, filePosition) {
          NodeAssert.equal(filePosition, null);
          const count = Math.min(3, length, bytes.length - position);
          bytes.copy(buffer, offset, position, position + count);
          position += count;
          return { bytesRead: count };
        },
      };
    };
    const maps = "node image\nlibfff.so\nffi-rs.node\n";
    NodeAssert.equal(await readBoundedEvidence(reader(maps), Buffer.byteLength(maps)), maps);
    NodeAssert.equal(await readBoundedEvidence(reader(""), 1), "");
    await NodeAssert.rejects(readBoundedEvidence(reader(maps), Buffer.byteLength(maps) - 1));
    await NodeAssert.rejects(readBoundedEvidence(reader("x"), 0));
    await NodeAssert.rejects(readBoundedEvidence({ read: async () => ({ bytesRead: -1 }) }, 8));
  },
);

NodeTest.test(
  "next-cohort admission requires captured exit, absence and journal retirement",
  () => {
    const retired = {
      cleanupProved: true,
      childrenAbsent: true,
      parentExit: { captured: true, code: 0, signal: null },
      candidate: { children: [42, 43] },
      normalResult: true,
      cleanupRecovery: false,
      sameJournalRecovered: false,
      replacementUseful: false,
    };
    NodeAssert.equal(nativeBlockCohortRetired(retired, 2), true);
    for (const value of [
      undefined,
      {},
      { ...retired, cleanupProved: false },
      { ...retired, cleanupProved: "unknown" },
      { ...retired, childrenAbsent: "unknown" },
      { ...retired, parentExit: { captured: false } },
      { ...retired, parentExit: { captured: true, code: 1, signal: null } },
      { ...retired, candidate: { children: [42, 42] } },
      { ...retired, normalResult: false },
    ])
      NodeAssert.equal(nativeBlockCohortRetired(value, 2), false);
    NodeAssert.equal(nativeBlockCohortRetired(retired, 3), false);
    // A nonpassing native proof may continue only after its separate journal cleanup succeeds.
    const unknownProof = { ...retired, normalResult: false, predicate: "unknown" };
    NodeAssert.equal(nativeBlockCohortRetired(unknownProof, 2), false);
    NodeAssert.equal(nativeBlockCohortRetired({ ...unknownProof, cleanupRecovery: true }, 2), true);
    NodeAssert.equal(
      nativeBlockCohortRetired({ ...unknownProof, sameJournalRecovered: true }, 2),
      false,
    );
    NodeAssert.equal(
      nativeBlockCohortRetired(
        { ...unknownProof, sameJournalRecovered: true, replacementUseful: true },
        2,
      ),
      true,
    );
  },
);
// Keep the fixture confined to these tests: it is a schema example, never a runtime receipt.
const syscall = "257 0xffffff9c 0x1000 0x80441 0x1b6 0x0 0x0 0x1234 0x5678\n";
const raw = {
  pid: 42,
  tid: 42,
  syscallBefore: syscall,
  syscallAfter: syscall,
  pathname: "/owned/native-block+100+42.log",
  state: "S ",
  wchan: "wait_for_partner\n",
  startTimeTicks: 17,
  startTimeTicksAfter: 17,
};
NodeTest.test(
  "Linux requires the captured main TID, exact blocking write-open path and stable generation",
  () => {
    NodeAssert.ok(parseLinuxSyscall(syscall));
    NodeAssert.equal(parseLinuxSyscall(syscall.replace("0x80441", "0x80c41")), undefined);
    NodeAssert.equal(parseLinuxSyscall(syscall.replace("0x80441", "0x80442")), undefined);
    NodeAssert.equal(parseLinuxSyscall(syscall.replace("257 ", "0 ")), undefined);
    NodeAssert.equal(parseLinuxSyscall("running"), undefined);
    const paths = [raw.pathname],
      subject = valid().subject;
    NodeAssert.deepEqual(parseLinuxWitness(raw, subject, paths), {
      mainTid: "42",
      fifoPath: raw.pathname,
    });
    for (const change of [
      { tid: 43 },
      { pid: 43 },
      { pathname: "/wrong" },
      { state: "R " },
      { wchan: "futex_wait" },
      { startTimeTicksAfter: 18 },
      { syscallAfter: "running" },
    ])
      NodeAssert.equal(parseLinuxWitness({ ...raw, ...change }, subject, paths), undefined);
    NodeAssert.equal(
      parseLinuxWitness({ pid: 42, cpuDelta: 100, enteringNative: true }, subject, paths),
      undefined,
    );
  },
);
const sample = `Process: node [42]
Call graph:
    100 Thread_0x123 DispatchQueue_1: com.apple.main-thread
    + 100 node_main (in node)
    +   100 fff_create_instance_with (in libfff.dylib)
    +     100 fff::log::init_tracing (in libfff.dylib)
    +       100 fff::log::sigsegv::set_log_fd (in libfff.dylib)
    +         100 __open_nocancel (in libsystem_kernel.dylib)
Total number in stack:
`;
NodeTest.test("macOS requires one nested main-thread chain in the exact fff image", () => {
  NodeAssert.deepEqual(parseMacSample(sample, 42), { mainTid: "0x123", image: "libfff.dylib" });
  for (const invalid of [
    sample.replace("[42]", "[43]"),
    sample.replace("com.apple.main-thread", "worker"),
    sample.replace("fff::log::init_tracing", "???"),
    sample.replace("set_log_fd", "???"),
    sample.replace("__open_nocancel", "read"),
    sample.replace("init_tracing (in libfff.dylib)", "init_tracing (in other.dylib)"),
    sample.replace("    +       100", "    + 100"),
  ])
    NodeAssert.equal(parseMacSample(invalid, 42), undefined);
});
NodeTest.test("strict receipt rejects all before-call/subprocess/CPU-only impostors", () => {
  const proof = valid();
  NodeAssert.equal(nativeBlockReceiptPredicate(proof, expected), true);
  for (const impostor of [
    { code: 0 },
    { enteringNative: true, cpuDelta: 12 },
    { ...proof, witness: null },
    { ...proof, blockerHeldThroughKill: "unknown" },
    { ...proof, allocation: { ...proof.allocation, outstandingThroughKill: "unknown" } },
    { ...proof, guardian: { postWitnessResources: "unknown", generationUnchanged: true } },
    { ...proof, subject: { ...proof.subject, generation: 0 } },
    { ...proof, binaryDigests: { fff: "unknown", ffi: H } },
    { ...proof, timeline: { ...proof.timeline, kill: proof.timeline.armExpires } },
    { ...proof, blocker: { ...proof.blocker, mode: 0o644 } },
    { ...proof, blocker: { ...proof.blocker, preparedNames: 21 } },
    { ...proof, allocation: { ...proof.allocation, attempt: 5 } },
    { ...proof, allocation: { ...proof.allocation, registeredChildren: [42, 42] } },
    { ...proof, negativeControl: { ...proof.negativeControl, witnessed: "unknown" } },
  ])
    NodeAssert.notEqual(nativeBlockReceiptPredicate(impostor, expected), true);
  NodeAssert.equal(
    nativeBlockReceiptPredicate({ ...proof, replacementUseful: false }, expected),
    false,
  );
  NodeAssert.equal(
    nativeBlockReceiptPredicate(proof, { ...expected, build: "b".repeat(64) }),
    "unknown",
  );
  for (const key of Object.keys(proof)) {
    const missing = { ...proof };
    delete missing[key];
    NodeAssert.equal(nativeBlockReceiptPredicate(missing, expected), "unknown", key);
  }
});
