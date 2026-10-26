import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeTimersPromises from "node:timers/promises";
import * as Identity from "./identity.ts";
import { acquireLock, processIdentity, restorePristine, runChild } from "./patch.ts";
import { admit } from "./typecheck.ts";

const repository = NodePath.resolve(import.meta.dirname, "../../..");
const scratch = NodePath.join(repository, "node_modules/.cache/jones/tsrs-tests");
const roots = new Set();
const jobs = new Set();
const stopping = new AbortController();
async function stop(signal) {
  stopping.abort(signal);
  await Promise.allSettled(jobs);
  await Promise.allSettled(
    [...roots].map((root) => NodeFSP.rm(root, { recursive: true, force: true })),
  );
  process.exitCode = signal === "SIGINT" ? 130 : 143;
}
process.once("SIGINT", () => {
  void stop("SIGINT");
});
process.once("SIGTERM", () => {
  void stop("SIGTERM");
});

async function fixture(t) {
  if (stopping.signal.aborted) throw new Error("Test run cancelled.");
  await NodeFSP.mkdir(scratch, { recursive: true });
  const root = await NodeFSP.mkdtemp(NodePath.join(scratch, "identity-"));
  roots.add(root);
  t.after(async () => {
    await NodeFSP.rm(root, { recursive: true, force: true });
    roots.delete(root);
  });
  const target = Identity.hostTarget();
  NodeAssert.ok(target, "Lifecycle fixture requires an official Effect TSRS supported host");
  const installed = await Identity.resolveInstalled(repository, target);
  const bundled = await Identity.resolveBundled(installed);
  const executable = NodePath.join(root, "node_modules/.pnpm/native/node_modules/native/tsrs");
  await NodeFSP.mkdir(NodePath.dirname(executable), { recursive: true });
  // Resolve the stock bytes whether the repository is pristine or patched.
  const state = await Identity.classify(installed);
  const stock = state.name === "S0" ? installed.executable : `${installed.executable}.original`;
  await NodeFSP.copyFile(stock, executable);
  await NodeFSP.chmod(executable, 0o755);
  return { installed: { ...installed, root, executable }, patched: bundled.artifact };
}

async function patchedFixture(t) {
  const value = await fixture(t);
  await NodeFSP.rename(value.installed.executable, `${value.installed.executable}.original`);
  await NodeFSP.copyFile(value.patched, value.installed.executable);
  await NodeFSP.chmod(value.installed.executable, 0o755);
  return value;
}

NodeTest.test("host matrix and glibc floor", () => {
  for (const [host, expected] of [
    [{ platform: "darwin", architecture: "arm64" }, "darwin-arm64"],
    [{ platform: "linux", architecture: "x64", glibc: "2.35" }, "linux-x64-gnu"],
    [{ platform: "linux", architecture: "arm64", glibc: "2.40" }, "linux-arm64-gnu"],
    [{ platform: "linux", architecture: "x64", glibc: "2.34" }, undefined],
    [{ platform: "linux", architecture: "x64" }, undefined],
    [{ platform: "win32", architecture: "x64" }, undefined],
    [{ platform: "darwin", architecture: "x64" }, undefined],
  ])
    NodeAssert.equal(Identity.hostTarget(host), expected);
});

NodeTest.test("installed chain, official artifact and pristine/patched identities", async (t) => {
  const { installed, patched } = await fixture(t);
  NodeAssert.equal((await Identity.classify(installed)).name, "S0");
  await NodeAssert.rejects(Identity.verified(installed), /not verified/);
  await NodeFSP.rename(installed.executable, `${installed.executable}.original`);
  await NodeFSP.copyFile(patched, installed.executable);
  await NodeFSP.chmod(installed.executable, 0o755);
  await Identity.verified(installed);
  NodeAssert.equal((await Identity.classify(installed)).name, "S1");
  await restorePristine(installed);
  NodeAssert.equal((await Identity.classify(installed)).name, "S0");
});

for (const component of ["node_modules", "node_modules/.pnpm"]) {
  NodeTest.test(`refuses symlinked ${component} before lock creation`, async (t) => {
    const { installed } = await fixture(t);
    const location = NodePath.join(installed.root, component);
    await NodeFSP.rename(location, `${location}-private`);
    await NodeFSP.symlink(`${location}-private`, location, "dir");
    await NodeAssert.rejects(acquireLock(installed), /symlink/);
    await NodeAssert.rejects(
      NodeFSP.stat(NodePath.join(installed.root, "node_modules/.cache/jones/tsrs/patch.lock")),
      { code: "ENOENT" },
    );
  });
}

NodeTest.test("refuses hoisted/outside store and cross-device bindings", async (t) => {
  const { installed } = await fixture(t);
  const outside = NodePath.join(installed.root, "outside");
  await NodeFSP.mkdir(outside);
  await NodeAssert.rejects(
    Identity.provePrivate({ ...installed, executable: NodePath.join(outside, "tsrs") }),
    /private isolated/,
  );
  await NodeAssert.rejects(
    Identity.provePrivate(installed, async (filename) => {
      const info = await Identity.regularInfo(filename);
      return filename.endsWith(".pnpm") && info ? { ...info, dev: info.dev + 1n } : info;
    }),
    /private isolated/,
  );
  await NodeFSP.rename(
    NodePath.join(installed.root, "node_modules/.pnpm"),
    NodePath.join(installed.root, "hoisted"),
  );
  await NodeAssert.rejects(
    Identity.provePrivate({
      ...installed,
      executable: NodePath.join(installed.root, "hoisted/native/node_modules/native/tsrs"),
    }),
    /private isolated/,
  );
});

NodeTest.test("restores renamed, partial, missing-executable and quarantine states", async (t) => {
  const { installed, patched } = await fixture(t);
  const backup = `${installed.executable}.original`;
  for (const state of ["renamed", "partial", "chmod", "quarantine"]) {
    await NodeFSP.rename(installed.executable, backup);
    if (state === "partial") await NodeFSP.writeFile(installed.executable, "incomplete copy");
    if (state === "chmod") {
      await NodeFSP.copyFile(patched, installed.executable);
      await NodeFSP.chmod(installed.executable, 0o600);
    }
    if (state === "quarantine")
      await NodeFSP.writeFile(`${installed.executable}.123-456.patched`, "quarantined copy");
    const actual = await Identity.classify(installed);
    NodeAssert.equal(actual.name, state === "partial" || state === "chmod" ? "S3" : "S2");
    await restorePristine(installed);
    NodeAssert.equal((await Identity.classify(installed)).name, "S0");
  }
});

NodeTest.test("unknown state and unauthenticated backup remain untouched", async (t) => {
  const { installed } = await fixture(t);
  await NodeFSP.writeFile(`${installed.executable}.original`, "untrusted backup");
  const before = await Identity.image(installed.executable);
  NodeAssert.equal((await Identity.classify(installed)).name, "S4");
  await NodeAssert.rejects(restorePristine(installed), /Unknown patch state/);
  NodeAssert.deepEqual(await Identity.image(installed.executable), before);
  NodeAssert.equal(
    await NodeFSP.readFile(`${installed.executable}.original`, "utf8"),
    "untrusted backup",
  );
});

NodeTest.test("rejects hardlinked patched binary and unexpected entries", async (t) => {
  const { installed } = await patchedFixture(t);
  const extra = `${installed.executable}-hardlink`;
  await NodeFSP.link(installed.executable, extra);
  NodeAssert.equal((await Identity.classify(installed)).name, "S4");
  await NodeAssert.rejects(Identity.verified(installed));
  await NodeFSP.unlink(extra);
  await NodeFSP.writeFile(`${installed.executable}.unknown`, "unknown");
  await NodeAssert.rejects(restorePristine(installed), /Unknown patch state/);
});

NodeTest.test("live lock exclusion, stale recovery and unconfirmed spawn refusal", async (t) => {
  const { installed } = await fixture(t);
  const first = await acquireLock(installed);
  await NodeAssert.rejects(acquireLock(installed), /in progress/);
  await first.release();
  const filename = NodePath.join(installed.root, "node_modules/.cache/jones/tsrs/patch.lock");
  let deadIdentity;
  NodeAssert.equal(
    await runChild({
      executable: process.execPath,
      args: ["-e", ""],
      cwd: repository,
      signal: stopping.signal,
      spawned: async (pid) => {
        deadIdentity = await processIdentity(pid);
      },
    }),
    0,
  );
  NodeAssert.ok(deadIdentity);
  await NodeFSP.writeFile(
    filename,
    JSON.stringify({ schema: "jones-tsrs-patch/v1", wrapper: deadIdentity, phase: "allocated" }),
  );
  const recovered = await acquireLock(installed);
  await recovered.release();
  await NodeFSP.writeFile(
    filename,
    JSON.stringify({ schema: "jones-tsrs-patch/v1", wrapper: deadIdentity, phase: "spawning" }),
  );
  const before = await NodeFSP.readFile(filename);
  await NodeAssert.rejects(acquireLock(installed), /unconfirmed/);
  NodeAssert.deepEqual(await NodeFSP.readFile(filename), before);
});

NodeTest.test(
  "cancellation forwards signal, escalates and observes child exit",
  { timeout: 20000 },
  async (t) => {
    const { installed } = await fixture(t);
    const marker = NodePath.join(installed.root, "ready");
    const controller = new AbortController();
    let pid;
    const job = runChild({
      executable: process.execPath,
      args: [
        "-e",
        `process.on('SIGTERM',()=>{}); require('node:fs').writeFileSync(${JSON.stringify(marker)},'ready'); setInterval(()=>{},1000)`,
      ],
      cwd: repository,
      signal: AbortSignal.any([controller.signal, stopping.signal]),
      spawned: async (value) => {
        pid = value;
        for (let attempt = 0; attempt < 200; attempt++) {
          if (
            await NodeFSP.stat(marker).then(
              () => true,
              (error) => {
                if (error.code === "ENOENT") return false;
                throw error;
              },
            )
          ) {
            controller.abort("SIGTERM");
            return;
          }
          await NodeTimersPromises.setTimeout(10, undefined, { signal: stopping.signal });
        }
        throw new Error("Owned child never became ready.");
      },
    });
    jobs.add(job);
    try {
      NodeAssert.equal(await job, 143);
      NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    } finally {
      jobs.delete(job);
    }
  },
);

NodeTest.test("CI refuses an unsupported host before cached compiler work", async () => {
  await NodeAssert.rejects(
    admit(repository, "1", { platform: "win32", architecture: "x64" }),
    /CI requires/,
  );
});

NodeTest.test("identity reads refuse oversized unexpected files before hashing", async (t) => {
  const { installed } = await fixture(t);
  await NodeFSP.truncate(installed.executable, 65 * 1024 * 1024);
  await NodeAssert.rejects(Identity.image(installed.executable), /bounded identity read/);
});
