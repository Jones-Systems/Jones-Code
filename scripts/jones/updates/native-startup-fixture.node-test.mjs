import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import {
  assertFixtureProcessesStopped,
  cleanLaunchEnvironment,
  createNativeState,
  FixtureOwnership,
  packagedStartupNames,
  runOwnedChild,
} from "./native-startup-fixture.mjs";
import {
  ObserverControlError,
  observerControl,
  protectedMutations,
  startObserver,
} from "./native-startup-observer.mjs";

test("qualification accepts the real stage-root package and derives product paths from bundle metadata", () => {
  const metadata = {
    name: "t3code",
    main: "apps/desktop/dist-electron/boot.cjs",
    build: { productName: "Jones Code" },
  };
  const bundle = {
    executableName: "Jones Code",
    bundleName: "Jones Code",
    bundleIdentifier: "com.t3tools.t3code",
  };
  assert.deepEqual(packagedStartupNames(metadata, bundle), [
    "t3code",
    "Jones Code",
    "com.t3tools.t3code",
  ]);
  assert.throws(() =>
    packagedStartupNames({ ...metadata, main: "dist-electron/boot.cjs" }, bundle),
  );
});

test("fixture creation and cleanup preserve a competing home or profile creator", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "jones-fixture-ownership-"));
  const metadata = {
    version: "fixture",
    jonesSource: { sha: "a".repeat(40), tree: "b".repeat(40) },
  };
  try {
    for (const collision of ["home", "profile"]) {
      const directory = path.join(root, collision);
      await fs.mkdir(directory);
      const home = path.join(directory, "home");
      const profile = path.join(directory, "profile");
      const competing = collision === "home" ? home : profile;
      await assert.rejects(fs.lstat(competing), { code: "ENOENT" });
      // A negative observation is deliberately followed by a competing create.
      await fs.mkdir(competing);
      await fs.writeFile(path.join(competing, "foreign"), "preserve");
      const ownership = new FixtureOwnership();
      await assert.rejects(createNativeState(home, profile, metadata, ownership), {
        code: "EEXIST",
      });
      await ownership.cleanup();
      assert.equal(await fs.readFile(path.join(competing, "foreign"), "utf8"), "preserve");
      if (collision === "profile") await assert.rejects(fs.stat(home), { code: "ENOENT" });
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("fixture cleanup preserves replaced roots and does not adopt occupied lease paths", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "jones-fixture-replaced-"));
  try {
    const profile = path.join(root, "profile");
    const ownership = new FixtureOwnership();
    await ownership.mkdir(profile);
    await fs.rename(profile, `${profile}.retained`);
    await fs.mkdir(profile);
    await fs.writeFile(path.join(profile, "foreign"), "preserve");
    await assert.rejects(ownership.cleanup(), /ownership/);
    assert.equal(await fs.readFile(path.join(profile, "foreign"), "utf8"), "preserve");
    const lease = path.join(root, "lease.sqlite");
    await fs.writeFile(lease, "foreign lease");
    const files = new FixtureOwnership();
    await assert.rejects(files.file(lease, "replacement"), { code: "EEXIST" });
    await files.cleanup();
    assert.equal(await fs.readFile(lease, "utf8"), "foreign lease");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("a closed leader cannot leave an unref'd same-group grandchild alive", async () => {
  const result = await runOwnedChild(process.execPath, [
    "-e",
    `
    const child = require('node:child_process').spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], {stdio: 'ignore'});
    child.unref();
    process.stdout.write(JSON.stringify({ group: process.pid, grandchild: child.pid }));
  `,
  ]);
  assert.equal(result.code, 0, result.stderr);
  const ids = JSON.parse(result.stdout);
  assert.throws(() => process.kill(-ids.group, 0), { code: "ESRCH" });
  assert.throws(() => process.kill(ids.grandchild, 0), { code: "ESRCH" });
  assertFixtureProcessesStopped();
});

test("qualification removes pre-JS logging and Node-mode overrides while preserving the native home", () => {
  const env = cleanLaunchEnvironment(
    {
      HOME: "/runner",
      PATH: "/bin",
      ELECTRON_RUN_AS_NODE: "1",
      ELECTRON_ENABLE_LOGGING: "file",
      ELECTRON_LOG_FILE: "/unsafe",
      CHROME_LOG_FILE: "/unsafe",
      NODE_OPTIONS: "--require=unsafe",
      CFFIXED_USER_HOME: "/other",
      T3CODE_HOME: "/other",
      T3CODE_DESKTOP_USER_DATA_DIR: "/other",
      T3CODE_JONES_TRIAL_DESCRIPTOR: "/other",
      VITE_DEV_SERVER_URL: "http://other",
    },
    "/fixture",
  );
  assert.deepEqual(env, {
    HOME: "/runner",
    PATH: "/bin",
    TMPDIR: "/fixture",
    TMP: "/fixture",
    TEMP: "/fixture",
    NODE_DISABLE_COMPILE_CACHE: "1",
  });
});

test("observer preserves transient mutation evidence and fails closed on dropped or ambiguous history", () => {
  const events = [
    { path: "/profile/deleted-before-snapshot", flags: 0x100 | 0x200 },
    { path: "/profile-other/file", flags: 0x100 },
  ];
  assert.deepEqual(protectedMutations(events, ["/profile"]), [events[0]]);
  for (const flag of [0x01, 0x02, 0x04, 0x08, 0x20, 0x40, 0x80])
    assert.throws(
      () => protectedMutations([{ path: "/unrelated", flags: flag }], ["/profile"]),
      /history/,
    );
});

async function controlFixture(fault, run) {
  const ownership = new FixtureOwnership();
  const root = await ownership.temporary(path.join(os.tmpdir(), "jones-observer-phases-"));
  const events = [];
  const phases = [];
  const attempts = new Map();
  let prior = "";
  const observer = {
    events,
    async flush() {
      const directories = await fs.readdir(root);
      if (directories.length === 0) return events.length;
      assert.equal(directories.length, 1);
      const directory = path.join(root, directories[0]);
      const leaves = await fs.readdir(directory);
      assert.ok(
        !leaves.includes("burst") && !leaves.includes("burst.renamed"),
        "The rapid burst must disappear without an intermediate observer flush.",
      );
      const phase = leaves.includes("sentinel")
        ? "created"
        : leaves.includes("sentinel.renamed")
          ? "renamed"
          : phases.includes("removed")
            ? "rapid-burst"
            : "removed";
      if (phase !== prior) {
        assert.equal(
          phases.at(-1),
          { created: undefined, renamed: "created", removed: "renamed", "rapid-burst": "removed" }[
            phase
          ],
        );
        prior = phase;
      }
      const count = (attempts.get(phase) ?? 0) + 1;
      attempts.set(phase, count);
      // A flush may acknowledge before the kernel delivers the next leaf event.
      if (count !== 2) return events.length;
      if (fault === "present-only" && phase === "rapid-burst") return events.length;
      const leaf =
        phase === "rapid-burst"
          ? "burst.renamed"
          : phase === "created"
            ? "sentinel"
            : "sentinel.renamed";
      events.push({
        path: fault === phase ? directory : path.join(directory, leaf),
        flags:
          (fault === "history" ? 0x02 : 0) |
          { created: 0x100, renamed: 0x800, removed: 0x200, "rapid-burst": 0x100 | 0x800 | 0x200 }[
            phase
          ],
      });
      if (fault !== phase) phases.push(phase);
      return events.length;
    },
  };
  try {
    await run({ root, observer, phases });
    assert.deepEqual(await fs.readdir(root), []);
  } finally {
    await ownership.cleanup();
  }
}

test("observer control awaits paced phases and detects a rapid burst after every leaf disappears", () =>
  controlFixture(undefined, async ({ root, observer, phases }) => {
    await observerControl(observer, [root]);
    assert.deepEqual(phases, ["created", "renamed", "removed", "rapid-burst"]);
    assert.deepEqual(
      observer.events.map((event) => event.flags),
      [0x100, 0x800, 0x200, 0x100 | 0x800 | 0x200],
    );
    for (const event of observer.events)
      await assert.rejects(fs.lstat(event.path), { code: "ENOENT" });
  }));

test("paced detection cannot qualify an observer that misses leaves disappearing between flushes", () =>
  controlFixture("present-only", async ({ root, observer, phases }) => {
    await assert.rejects(observerControl(observer, [root], { timeoutMs: 100 }), (error) => {
      assert.ok(error instanceof ObserverControlError);
      assert.equal(error.control.phase, "rapid-burst");
      assert.deepEqual(phases, ["created", "renamed", "removed"]);
      assert.equal(error.control.observedCount, 0);
      assert.match(error.message, /required leaf mutation/);
      return true;
    });
  }));

for (const missing of ["created", "renamed", "removed", "rapid-burst"])
  test(`observer control rejects missing ${missing} leaf evidence and retains bounded phase details`, () =>
    controlFixture(missing, async ({ root, observer }) => {
      await assert.rejects(observerControl(observer, [root], { timeoutMs: 100 }), (error) => {
        assert.ok(error instanceof ObserverControlError);
        assert.equal(error.control.phase, missing);
        assert.ok(error.control.paths.every((name) => !path.isAbsolute(name)));
        assert.equal(error.control.events.length, 1);
        assert.equal(
          error.control.events[0].flags,
          { created: 0x100, renamed: 0x800, removed: 0x200, "rapid-burst": 0x100 | 0x800 | 0x200 }[
            missing
          ],
        );
        assert.match(error.message, /required leaf mutation/);
        return true;
      });
    }));

test("observer control fails on lost history even when the positive leaf event is present", () =>
  controlFixture("history", async ({ root, observer }) => {
    await assert.rejects(observerControl(observer, [root]), (error) => {
      assert.ok(error instanceof ObserverControlError);
      assert.equal(error.control.phase, "created");
      assert.equal(error.control.events[0].flags, 0x102);
      assert.match(error.message, /lost history/);
      return true;
    });
  }));

test("observer consumes acknowledged flushes without exhausting its unmatched-message bound", async () => {
  const observer = await startObserver(process.execPath, [
    "-e",
    `
    const lines = require('node:readline').createInterface({ input: process.stdin });
    const emit = (message) => process.stdout.write(JSON.stringify(message) + '\\n');
    emit({ type: 'ready' });
    lines.on('line', (line) => {
      if (line === 'quit') { lines.close(); process.stdin.destroy(); }
      else emit({ type: 'flushed', token: line.slice(6) });
    });
    `,
  ]);
  try {
    for (let index = 0; index < 60; index++) assert.equal(await observer.flush(), 0);
  } finally {
    await observer.close();
  }
});

test("cancelled qualification reaps only its captured child before fixture removal", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "jones-qualifier-cancel-"));
  const marker = path.join(root, "pid");
  const controller = new AbortController();
  let completion;
  let timer;
  try {
    completion = runOwnedChild(
      process.execPath,
      [
        "-e",
        `require('node:fs').writeFileSync(${JSON.stringify(marker)}, String(process.pid)); setInterval(() => {}, 1000);`,
      ],
      { signal: controller.signal },
    );
    void completion.catch(() => {});
    // The exact child announces its PID; no process-name or host-wide scan is used.
    await new Promise((resolve, reject) => {
      const started = Date.now();
      timer = setInterval(async () => {
        try {
          await fs.stat(marker);
          clearInterval(timer);
          resolve();
        } catch (error) {
          if (error.code !== "ENOENT" || Date.now() - started > 5000) {
            clearInterval(timer);
            reject(error);
          }
        }
      }, 10);
    });
    const pid = Number(await fs.readFile(marker, "utf8"));
    controller.abort();
    await assert.rejects(completion, /cancelled/);
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  } finally {
    clearInterval(timer);
    controller.abort();
    await completion?.catch(() => {});
    assertFixtureProcessesStopped();
    await fs.rm(root, { recursive: true, force: true });
  }
});
