import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import { runProcess } from "../incremental-typecheck/incrementalTypecheck.ts";
import * as NodeTest from "node:test";
import * as NodeTimersPromises from "node:timers/promises";
import * as Identity from "./identity.ts";
import { acquireLock, processIdentity, restorePristine, runChild, patch } from "./patch.ts";
import { admit, launchVerified } from "./typecheck.ts";

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
    const { installed } = await patchedFixture(t);
    const marker = NodePath.join(installed.root, "ready");
    const controller = new AbortController();
    let pid;
    const job = launchVerified(
      installed,
      AbortSignal.any([controller.signal, stopping.signal]),
      (_compiler, spawned) =>
        runChild({
          executable: process.execPath,
          args: [
            "-e",
            `process.on('SIGTERM',()=>{}); require('node:fs').writeFileSync(${JSON.stringify(marker)},'ready'); setInterval(()=>{},1000)`,
          ],
          cwd: repository,
          signal: AbortSignal.any([controller.signal, stopping.signal]),
          spawned: async (value) => {
            await spawned();
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
        }),
    );
    jobs.add(job);
    try {
      NodeAssert.equal(await job, 143);
      NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
      const mutation = await acquireLock(installed);
      await mutation.release();
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

NodeTest.test("verified launch excludes restore until successful spawn", async (t) => {
  const { installed } = await patchedFixture(t);
  let observedSpawn = false;
  NodeAssert.equal(
    await launchVerified(installed, stopping.signal, async (_compiler, spawned) => {
      await NodeAssert.rejects(acquireLock(installed), /in progress/);
      NodeAssert.equal((await Identity.classify(installed)).name, "S1");
      return runChild({
        executable: process.execPath,
        args: ["-e", "process.exitCode=7"],
        cwd: repository,
        signal: stopping.signal,
        spawned: async () => {
          observedSpawn = true;
          await spawned();
          const mutation = await acquireLock(installed);
          try {
            await restorePristine(installed);
          } finally {
            await mutation.release();
          }
        },
      });
    }),
    7,
  );
  NodeAssert.ok(observedSpawn);
  NodeAssert.equal((await Identity.classify(installed)).name, "S0");
});

NodeTest.test(
  "parallel launchers release the lock before either compiler exits",
  { timeout: 10000 },
  async (t) => {
    const { installed } = await patchedFixture(t);
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, stopping.signal]);
    let firstSpawn;
    const firstReady = new Promise((resolve) => {
      firstSpawn = resolve;
    });
    const marker = NodePath.join(installed.root, "second-spawned");
    const first = launchVerified(installed, signal, (_compiler, spawned) =>
      runChild({
        executable: process.execPath,
        args: [
          "-e",
          `const fs=require('node:fs'); const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(marker)})){clearInterval(timer);process.exitCode=11}},10); setTimeout(()=>{process.exit(99)},5000).unref()`,
        ],
        cwd: repository,
        signal,
        spawned: async () => {
          await spawned();
          firstSpawn();
        },
      }),
    );
    jobs.add(first);
    let second;
    try {
      await firstReady;
      second = launchVerified(installed, signal, (_compiler, spawned) =>
        runChild({
          executable: process.execPath,
          args: ["-e", "process.exitCode=12"],
          cwd: repository,
          signal,
          spawned: async () => {
            await spawned();
            await NodeFSP.writeFile(marker, "ready");
          },
        }),
      );
      jobs.add(second);
      NodeAssert.deepEqual(await Promise.all([first, second]), [11, 12]);
      const mutation = await acquireLock(installed);
      await mutation.release();
    } finally {
      controller.abort("SIGTERM");
      await Promise.allSettled([first, ...(second ? [second] : [])]);
      jobs.delete(first);
      if (second) jobs.delete(second);
    }
  },
);

NodeTest.test("failed spawn and cancelled lock wait release only owned locks", async (t) => {
  const { installed } = await patchedFixture(t);
  await NodeAssert.rejects(
    launchVerified(installed, stopping.signal, (_compiler, spawned) =>
      runChild({
        executable: NodePath.join(installed.root, "missing-executable"),
        args: [],
        cwd: repository,
        signal: stopping.signal,
        spawned,
      }),
    ),
  );
  const mutation = await acquireLock(installed);
  const controller = new AbortController();
  let attempts = 0;
  const waiting = launchVerified(
    installed,
    AbortSignal.any([controller.signal, stopping.signal]),
    async () => {
      attempts++;
      return 0;
    },
  );
  const rejection = NodeAssert.rejects(waiting, /cancelled/);
  try {
    await NodeTimersPromises.setTimeout(75, undefined, { signal: stopping.signal });
    controller.abort("SIGTERM");
    await rejection;
    NodeAssert.equal(attempts, 0);
    await NodeAssert.rejects(acquireLock(installed), /in progress/);
  } finally {
    controller.abort("SIGTERM");
    await Promise.allSettled([waiting]);
    await mutation.release();
  }
});

NodeTest.test("root admission remains uncached and CI uses the root typecheck chain", async () => {
  const manifest = JSON.parse(
    await NodeFSP.readFile(NodePath.join(repository, "package.json"), "utf8"),
  );
  const chain = "vp run tsrs:admit && vp run -r --cache --concurrency-limit 4 typecheck";
  NodeAssert.equal(manifest.scripts.typecheck, chain);
  NodeAssert.equal(manifest.scripts.tc, chain);
  const config = await NodeFSP.readFile(NodePath.join(repository, "vite.config.ts"), "utf8");
  NodeAssert.match(
    config,
    /"tsrs:admit":\s*\{\s*command: "node scripts\/jones\/tsrs\/typecheck\.ts --admit",\s*cache: false,?\s*\}/,
  );
  const workflow = await NodeFSP.readFile(
    NodePath.join(repository, ".github/workflows/ci.yml"),
    "utf8",
  );
  NodeAssert.match(workflow, /run: vpr typecheck/);
  const workspace = await NodeFSP.readFile(
    NodePath.join(repository, "pnpm-workspace.yaml"),
    "utf8",
  );
  NodeAssert.match(
    workspace,
    new RegExp(
      `^  typescript: ${Identity.FALLBACK.catalog.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`,
      "m",
    ),
  );
});

NodeTest.test(
  "pinned fallback preserves TS377067 and discloses its TS377049 limitation",
  async (t) => {
    const parent = NodePath.join(repository, ".t3/test-scratch/tsrs-fallback");
    await NodeFSP.mkdir(parent, { recursive: true });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "control-"));
    roots.add(root);
    t.after(async () => {
      await NodeFSP.rm(root, { recursive: true, force: true });
      roots.delete(root);
    });
    await NodeFSP.writeFile(NodePath.join(root, "package.json"), '{"type":"module"}');
    await NodeFSP.writeFile(
      NodePath.join(root, "tsconfig.json"),
      JSON.stringify({
        extends: NodePath.join(repository, "tsconfig.base.json"),
        files: ["probe.ts"],
      }),
    );
    await NodeFSP.mkdir(NodePath.join(root, "node_modules"));
    const require = NodeModule.createRequire(NodePath.join(repository, "scripts/package.json"));
    const effect = NodePath.dirname(require.resolve("effect/package.json"));
    await NodeFSP.symlink(effect, NodePath.join(root, "node_modules/effect"), "dir");
    const entry = NodePath.join(root, "probe.ts");
    const check = async () => {
      const moduleUrl = new URL("./typecheck.ts", import.meta.url).href;
      const job = runProcess({
        executable: process.execPath,
        args: [
          "--input-type=module",
          "--eval",
          `import {runFallback} from ${JSON.stringify(moduleUrl)};process.stderr.write=(chunk)=>process.stdout.write(chunk);process.exitCode=await runFallback(${JSON.stringify(repository)},${JSON.stringify(root)},["--noEmit","--pretty","false"],new AbortController().signal);`,
        ],
        cwd: root,
        capture: true,
        signal: stopping.signal,
      });
      jobs.add(job);
      try {
        return await job;
      } finally {
        jobs.delete(job);
      }
    };
    await NodeFSP.writeFile(
      entry,
      "import * as Effect from 'effect/Effect'; export const probe = Effect.sync(() => Date.now());\n",
    );
    const negative = await check();
    NodeAssert.equal(negative.code, 2, negative.stdout);
    NodeAssert.match(negative.stdout, /error TS377067/);
    NodeAssert.match(
      negative.stdout,
      /fallback misses configured deterministicKeys ERROR TS377049/,
    );
    await NodeFSP.writeFile(
      entry,
      "import * as Effect from 'effect/Effect'; export const probe = Effect.sync(() => 0);\n",
    );
    const positive = await check();
    NodeAssert.equal(positive.code, 0, positive.stdout);
    NodeAssert.doesNotMatch(positive.stdout, /error TS377067/);
    NodeAssert.match(positive.stdout, /local or cached success requires authoritative/);
  },
);

for (const [signalName, exitCode] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
]) {
  NodeTest.test(`before-spawn cancellation proves no child and ${exitCode}`, async (t) => {
    const { installed } = await patchedFixture(t);
    const controller = new AbortController();
    controller.abort(signalName);
    let spawned = false;
    await NodeAssert.rejects(
      runChild({
        executable: process.execPath,
        args: ["-e", "process.exit(99)"],
        cwd: installed.root,
        signal: controller.signal,
        spawned: async () => {
          spawned = true;
        },
      }),
      (error) => error.exitCode === exitCode && error.childState === "not-spawned",
    );
    NodeAssert.equal(spawned, false);
  });

  for (const stage of ["before", "contention", "acquired"])
    NodeTest.test(
      `mutation-lock cancellation ${signalName} at ${stage} preserves ownership`,
      async (t) => {
        const { installed } = await patchedFixture(t);
        const lockPath = NodePath.join(installed.root, "node_modules/.cache/jones/tsrs/patch.lock");
        const controller = new AbortController();
        const foreign = stage === "contention" ? await acquireLock(installed) : undefined;
        const before = foreign ? await NodeFSP.readFile(lockPath) : undefined;
        let launched = false;
        if (stage === "before") controller.abort(signalName);
        try {
          await NodeAssert.rejects(
            launchVerified(
              installed,
              controller.signal,
              async () => {
                launched = true;
                return 0;
              },
              {
                [stage === "contention" ? "contended" : "acquired"]: () =>
                  controller.abort(signalName),
              },
            ),
            (error) => error.exitCode === exitCode,
          );
          NodeAssert.equal(launched, false);
          if (foreign) NodeAssert.deepEqual(await NodeFSP.readFile(lockPath), before);
          else await NodeAssert.rejects(NodeFSP.stat(lockPath), { code: "ENOENT" });
        } finally {
          await foreign?.release();
        }
      },
    );

  NodeTest.test(
    `patch cancellation ${signalName} after spawning record releases only its known unspawned lock`,
    async (t) => {
      const { installed } = await fixture(t);
      const wrapper = NodePath.join(installed.root, "node_modules/tsrs/package.json");
      const manifest = NodePath.join(NodePath.dirname(installed.executable), "package.json");
      await NodeFSP.mkdir(NodePath.dirname(wrapper), { recursive: true });
      await NodeFSP.writeFile(
        wrapper,
        JSON.stringify({ name: "tsrs", version: Identity.TSRS_VERSION }),
      );
      await NodeFSP.writeFile(
        manifest,
        JSON.stringify({ name: `@tsrs/${installed.target}`, version: Identity.TSRS_VERSION }),
      );
      const binding = NodePath.join(
        NodePath.dirname(wrapper),
        "node_modules/@tsrs",
        installed.target,
      );
      await NodeFSP.mkdir(NodePath.dirname(binding), { recursive: true });
      await NodeFSP.symlink(NodePath.dirname(manifest), binding, "dir");
      const original = await Identity.image(installed.executable);
      const controller = new AbortController();
      let barrier = false;
      const error = await patch(installed.root, "patch", controller.signal, {
        spawningRecorded: async () => {
          const lock = JSON.parse(
            await NodeFSP.readFile(
              NodePath.join(installed.root, "node_modules/.cache/jones/tsrs/patch.lock"),
              "utf8",
            ),
          );
          NodeAssert.equal(lock.phase, "spawning");
          NodeAssert.equal(lock.child, undefined);
          barrier = true;
          controller.abort(signalName);
        },
      }).then(
        () => undefined,
        (error) => error,
      );
      NodeAssert.equal(barrier, true);
      NodeAssert.deepEqual(await Identity.image(installed.executable), original);
      NodeAssert.equal((await Identity.classify(installed)).name, "S0");
      await NodeAssert.rejects(
        NodeFSP.stat(NodePath.join(installed.root, "node_modules/.cache/jones/tsrs/patch.lock")),
        { code: "ENOENT" },
      );
      const next = await acquireLock(installed);
      await next.release();
      NodeAssert.equal(error.exitCode, exitCode);
      NodeAssert.equal(error.childState, "not-spawned");
    },
  );
}
