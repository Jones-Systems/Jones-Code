import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import {
  darwinFffCodeDigest,
  parseLinuxSyscall,
  parseLinuxWitness,
  parseMacSample,
  parseDarwinWitness,
  nativeBlockReceiptPredicate,
  nativeBlockCohortRetired,
} from "./native-block-proof.mjs";
import { nativeBlockTestProof } from "./native-block-proof-fixture.mjs";
import { readBoundedEvidence } from "./native-block-observer.mjs";
const H = "a".repeat(64);
const expected = { os: "linux-x64", form: "source", profile: "laptop", build: H };
const valid = nativeBlockTestProof;

NodeTest.test("Mach-O fingerprint preserves all bytes outside signature and signer sizes", () => {
  function image(signatureSize) {
    const signatureStart = 0x180;
    const linkeditStart = 0x100;
    const bytes = Buffer.alloc(signatureStart + signatureSize);
    bytes.writeUInt32LE(0xfeedfacf, 0);
    bytes.writeUInt32LE(0x0100000c, 4);
    bytes.writeUInt32LE(6, 12);
    bytes.writeUInt32LE(2, 16);
    bytes.writeUInt32LE(72 + 16, 20);
    const segment = 32;
    bytes.writeUInt32LE(0x19, segment);
    bytes.writeUInt32LE(72, segment + 4);
    bytes.write("__LINKEDIT", segment + 8);
    bytes.writeBigUInt64LE(BigInt(256 + signatureSize), segment + 32);
    bytes.writeBigUInt64LE(BigInt(linkeditStart), segment + 40);
    bytes.writeBigUInt64LE(BigInt(bytes.length - linkeditStart), segment + 48);
    const signature = segment + 72;
    bytes.writeUInt32LE(0x1d, signature);
    bytes.writeUInt32LE(16, signature + 4);
    bytes.writeUInt32LE(signatureStart, signature + 8);
    bytes.writeUInt32LE(signatureSize, signature + 12);
    bytes.fill(signatureSize, signatureStart);
    return bytes;
  }
  const original = image(32);
  const resigned = image(64);
  const fingerprint = darwinFffCodeDigest(original);
  NodeAssert.match(fingerprint, /^[a-f0-9]{64}$/);
  NodeAssert.equal(darwinFffCodeDigest(resigned), fingerprint);
  resigned[0x140] = 42;
  NodeAssert.notEqual(darwinFffCodeDigest(resigned), fingerprint);
  NodeAssert.equal(darwinFffCodeDigest(original.subarray(0, -1)), undefined);
  const invalidOffset = Buffer.from(original);
  invalidOffset.writeUInt32LE(112, 104 + 8);
  NodeAssert.equal(darwinFffCodeDigest(invalidOffset), undefined);
});

NodeTest.test(
  "macOS requires the same blocked thread, exact FIFO inode and pinned native chain",
  () => {
    const subject = {
      pid: 42,
      build: H,
      authorityId: "a",
      slotId: 0,
      generation: 1,
      nonce: "b",
      requestId: 3,
    };
    const table = Array.from({ length: 20 }, (_, i) => ({
      path: "/owned/native-block+" + (1000 + i) + "+42.log",
      device: 1,
      inode: 100 + i,
      mode: 0o600,
    }));
    const fifo = table[13];
    const observation = {
      path: fifo.path,
      device: fifo.device,
      inode: fifo.inode,
      ordinal: 5,
      flags: "16777737",
      pc: "18123aab8",
      runState: 3,
    };
    const paths = { fff: "/owned/libfff_c.dylib", ffi: "/owned/ffi-rs.darwin-arm64.node" };
    const digests = {
      fff: "d052aae92b8d89f4734399aa53f80a327c4931f1b5dea0a7d96f9c488229ba43",
      ffi: H,
    };
    const sample = `Process: node [42]
Call graph:
    10 Thread_802  DispatchQueue_1: com.apple.main-thread (serial)
    +   10 fff_create_instance_with (in libfff_c.dylib) + 508 [0x100039a30]
    +     10 ??? (in libfff_c.dylib) load address 0x100000000 + 0x1c54a8 [0x1001c54a8]
    +       10 ??? (in libfff_c.dylib) load address 0x100000000 + 0x2cf7b4 [0x1002cf7b4]
    +         10 open (in libsystem_kernel.dylib) + 64 [0x18123bbc0]
    +           10 __open (in libsystem_kernel.dylib) + 8 [0x18123aab8]
Total number in stack
Binary Images:
  0x100000000 - 0x100400000 +libfff_c.dylib (0) <A16BF197-B7BB-31BF-9522-B9F734296D65> /Users/*/libfff_c.dylib
  0x101000000 - 0x101100000 +ffi-rs.darwin-arm64.node (0) <82A0F874-A6D0-3884-B7AE-E1E6B558B5D9> /Users/*/ffi-rs.darwin-arm64.node
`;
    const raw = {
      sample,
      samplerFinished: true,
      fffCodeSha256: "040dff319ec3c6d2c015801dee0bb6f912de2d215749c9638c0dbca10e9331d2",
      mach: {
        subject,
        mainTid: "802",
        before: { at: 103, observation },
        after: { at: 127, observation: { ...observation } },
        images: [
          { path: paths.fff, base: "100000000" },
          { path: paths.ffi, base: "101000000" },
        ],
      },
    };
    const bounds = { issued: 102, finished: 141 };
    NodeAssert.deepEqual(parseDarwinWitness(raw, subject, table, bounds, digests), {
      mainTid: "802",
      fifoPath: fifo.path,
      binaryPaths: paths,
    });
    for (const mutate of [
      (draft) => {
        delete draft.fffCodeSha256;
      },
      (draft) => {
        draft.fffCodeSha256 = H;
      },
      (draft) => {
        delete draft.mach;
      },
      (draft) => {
        draft.mach.subject.requestId++;
      },
      (draft) => {
        draft.mach.mainTid = "803";
      },
      (draft) => {
        draft.mach.after.observation.path = table[14].path;
      },
      (draft) => {
        draft.mach.before.observation.inode++;
        draft.mach.after.observation.inode++;
      },
      (draft) => {
        draft.mach.before.observation.flags = "16777741";
        draft.mach.after.observation.flags = "16777741";
      },
      (draft) => {
        draft.mach.before.at = bounds.issued - 1;
      },
      (draft) => {
        draft.sample = draft.sample.replace("[0x18123aab8]", "[0x18123aabc]");
      },
      (draft) => {
        draft.sample = draft.sample.replace("[0x1002cf7b4]", "[0x1002cf7b8]");
      },
      (draft) => {
        draft.mach.images[0].base = "100000001";
      },
    ]) {
      const draft = structuredClone(raw);
      mutate(draft);
      NodeAssert.equal(parseDarwinWitness(draft, subject, table, bounds, digests), undefined);
    }
    NodeAssert.equal(
      parseDarwinWitness(raw, subject, table, bounds, { ...digests, fff: "invalid" }),
      undefined,
    );
  },
);

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
