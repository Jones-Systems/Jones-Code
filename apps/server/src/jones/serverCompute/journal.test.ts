// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { GenerationJournal, proveAbsent } from "./journal.ts";
// Gate only the external rename, keeping the actual journal and filesystem protocol.
vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return { ...actual, rename: vi.fn(actual.rename) };
});
const homes: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const home of homes.splice(0)) await NodeFSP.rm(home, { recursive: true, force: true });
});
async function home() {
  const directory = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-compute-journal-")),
  );
  homes.push(directory);
  return NodePath.join(directory, "pool");
}
const absent = () => {
  throw Object.assign(new Error("test absence"), { code: "ESRCH" });
};

it("durably reserves before registration and refuses takeover of a live owner", async () => {
  const directory = await home(),
    journal = await GenerationJournal.acquire(directory, 2, "linux");
  await journal.reserve(0, 1, "nonce");
  expect(
    JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "slot-0.json"), "utf8")),
  ).toMatchObject({
    state: "starting",
    generation: 1,
  });
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
  await journal.registered(0, 1, "nonce", 1234);
  await journal.retired(0, 1, "nonce", 1234);
  await journal.release();
  const next = await GenerationJournal.acquire(directory, 2, "linux");
  expect(next.claim.authorityId).not.toBe(journal.claim.authorityId);
  await next.release();
});

it("does not retain a duplicate-start guard during the live owner's pending rename", async () => {
  const directory = await home(),
    owner = await GenerationJournal.acquire(directory, 2, "linux");
  await owner.reserve(0, 1, "nonce");
  const entered = Promise.withResolvers<void>(),
    resume = Promise.withResolvers<void>();
  const actual = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
  vi.mocked(NodeFSP.rename).mockImplementationOnce(async (from, to) => {
    expect(from).toBe(NodePath.join(directory, "slot-0.next"));
    expect(to).toBe(NodePath.join(directory, "slot-0.json"));
    entered.resolve();
    await resume.promise;
    await actual.rename(from, to);
  });
  const registering = owner.registered(0, 1, "nonce", 1234);
  try {
    await entered.promise;
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "slot-0.next"), "utf8")),
    ).toMatchObject({
      state: "registered",
    });
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toMatchObject({
      reason: "retirement-unknown",
    });
    await expect(NodeFSP.stat(NodePath.join(directory, "recovery.claim"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  } finally {
    resume.resolve();
    await registering;
  }
  await owner.retired(0, 1, "nonce", 1234);
  await owner.release();
  const next = await GenerationJournal.acquire(directory, 2, "linux");
  expect(next.claim.authorityId).not.toBe(owner.claim.authorityId);
  await next.release();
});

it.each(["absent", "unknown", "missing"])(
  "keeps an interrupted rename closed when owner is %s",
  async (ownerState) => {
    const directory = await home();
    if (ownerState === "missing") await NodeFSP.mkdir(directory, { mode: 0o700 });
    else await GenerationJournal.acquire(directory, 2, "linux");
    const pending = NodePath.join(directory, "slot-0.next");
    await NodeFSP.writeFile(pending, "interrupted", { mode: 0o600 });
    if (ownerState === "absent") vi.spyOn(process, "kill").mockImplementation(absent);
    if (ownerState === "unknown")
      vi.spyOn(process, "kill").mockImplementation(() => {
        throw Object.assign(new Error("unknown"), { code: "EPERM" });
      });
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toMatchObject({
      reason: "retirement-unknown",
    });
    const guard = await NodeFSP.readFile(NodePath.join(directory, "recovery.claim"), "utf8");
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
    expect(await NodeFSP.readFile(NodePath.join(directory, "recovery.claim"), "utf8")).toBe(guard);
    expect(await NodeFSP.readFile(pending, "utf8")).toBe("interrupted");
  },
);

it.each(["EPERM", "EINVAL"])(
  "retains its recovery guard after an uncertain %s owner probe",
  async (code) => {
    const directory = await home(),
      owner = await GenerationJournal.acquire(directory, 2, "linux");
    vi.spyOn(process, "kill").mockImplementation(() => {
      throw Object.assign(new Error("unknown"), { code });
    });
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toMatchObject({
      reason: "retirement-unknown",
    });
    const guard = await NodeFSP.readFile(NodePath.join(directory, "recovery.claim"), "utf8");
    await owner.release();
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
    expect(await NodeFSP.readFile(NodePath.join(directory, "recovery.claim"), "utf8")).toBe(guard);
  },
);

it.each(["inode", "content"])(
  "preserves a recovery guard whose %s changed during live-owner refusal",
  async (change) => {
    const directory = await home(),
      owner = await GenerationJournal.acquire(directory, 2, "linux");
    const recovery = NodePath.join(directory, "recovery.claim");
    vi.spyOn(process, "kill").mockImplementation(() => {
      const guard = NodeFS.readFileSync(recovery);
      if (change === "inode") {
        NodeFS.renameSync(recovery, NodePath.join(directory, "preserved-recovery.claim"));
        NodeFS.writeFileSync(recovery, guard, { mode: 0o600 });
      } else NodeFS.writeFileSync(recovery, '{"version":1,"owner":"changed"}', { mode: 0o600 });
      return true;
    });
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toMatchObject({
      reason: "retirement-unknown",
    });
    const retained = await NodeFSP.readFile(recovery, "utf8");
    await owner.release();
    await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
    expect(await NodeFSP.readFile(recovery, "utf8")).toBe(retained);
  },
);

it("never removes a preexisting interrupted recovery guard", async () => {
  const directory = await home();
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  const recovery = NodePath.join(directory, "recovery.claim"),
    text = '{"version":1,"owner":"interrupted"}';
  await NodeFSP.writeFile(recovery, text, { mode: 0o600 });
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
  expect(await NodeFSP.readFile(recovery, "utf8")).toBe(text);
  await expect(NodeFSP.stat(NodePath.join(directory, "pool.claim"))).rejects.toMatchObject({
    code: "ENOENT",
  });
});

it("reconciles only unchanged fully registered generations after every PID absence proof", async () => {
  const directory = await home(),
    old = await GenerationJournal.acquire(directory, 3, "linux");
  await old.reserve(0, 1, "nonce");
  await old.registered(0, 1, "nonce", 1234);
  const kill = vi.spyOn(process, "kill").mockImplementation(absent);
  const replacement = await GenerationJournal.acquire(directory, 3, "linux");
  expect(replacement.claim.authorityId).not.toBe(old.claim.authorityId);
  expect(kill.mock.calls.every((call) => call[1] === 0)).toBe(true);
  await replacement.release();
});

it("holds restart admission for an incompletely registered slot, even when parent is absent", async () => {
  const directory = await home(),
    old = await GenerationJournal.acquire(directory, 2, "linux");
  await old.reserve(0, 1, "nonce");
  vi.spyOn(process, "kill").mockImplementation(absent);
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toMatchObject({
    reason: "retirement-unknown",
  });
  expect(
    JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "pool.claim"), "utf8")),
  ).toEqual(old.claim);
  const guard = await NodeFSP.readFile(NodePath.join(directory, "recovery.claim"), "utf8");
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
  expect(await NodeFSP.readFile(NodePath.join(directory, "recovery.claim"), "utf8")).toBe(guard);
});

it.each(["EPERM", "EINVAL"])(
  "treats %s as unknown and never signals a recorded PID",
  async (code) => {
    vi.spyOn(process, "kill").mockImplementation(() => {
      throw Object.assign(new Error("unknown"), { code });
    });
    expect(proveAbsent(1234)).toBe(false);
  },
);

it("refuses symlinked records and interrupted recovery guards", async () => {
  const directory = await home(),
    old = await GenerationJournal.acquire(directory, 2, "linux");
  await NodeFSP.symlink(
    NodePath.join(directory, "pool.claim"),
    NodePath.join(directory, "slot-0.json"),
  );
  vi.spyOn(process, "kill").mockImplementation(absent);
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
  expect(await NodeFSP.readFile(NodePath.join(directory, "pool.claim"), "utf8")).toContain(
    old.claim.authorityId,
  );
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toBeDefined();
});

it("cannot retire a replacement inode or an unproved owned child", async () => {
  const directory = await home(),
    journal = await GenerationJournal.acquire(directory, 2, "linux");
  await journal.reserve(0, 1, "nonce");
  await expect(journal.release()).rejects.toMatchObject({ reason: "retirement-unknown" });
  const claim = await NodeFSP.readFile(NodePath.join(directory, "pool.claim"));
  await NodeFSP.rename(
    NodePath.join(directory, "pool.claim"),
    NodePath.join(directory, "preserved.claim"),
  );
  await NodeFSP.writeFile(NodePath.join(directory, "pool.claim"), claim, { mode: 0o600 });
  await expect(journal.registered(0, 1, "nonce", 1234)).rejects.toMatchObject({
    reason: "retirement-unknown",
  });
});

it("admits only one authority when two fresh starts race", async () => {
  const directory = await home();
  const attempts = await Promise.allSettled([
    GenerationJournal.acquire(directory, 2, "linux"),
    GenerationJournal.acquire(directory, 2, "linux"),
  ]);
  const admitted = attempts.filter((attempt) => attempt.status === "fulfilled");
  expect(admitted).toHaveLength(1);
  if (admitted[0]?.status === "fulfilled") await admitted[0].value.release();
});

it("refuses a new authority when even one registered child PID is still present", async () => {
  const directory = await home(),
    old = await GenerationJournal.acquire(directory, 2, "linux");
  await old.reserve(0, 1, "nonce");
  await old.registered(0, 1, "nonce", process.pid + 1);
  vi.spyOn(process, "kill").mockImplementation((pid) => {
    if (pid === process.pid) return absent();
    return true;
  });
  await expect(GenerationJournal.acquire(directory, 2, "linux")).rejects.toMatchObject({
    reason: "retirement-unknown",
  });
  expect(
    JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "pool.claim"), "utf8")),
  ).toEqual(old.claim);
});
