import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
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

NodeTest.test(
  "qualification accepts the real stage-root package and derives product paths from bundle metadata",
  () => {
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
    NodeAssert.deepEqual(packagedStartupNames(metadata, bundle), [
      "t3code",
      "Jones Code",
      "com.t3tools.t3code",
    ]);
    NodeAssert.throws(() =>
      packagedStartupNames({ ...metadata, main: "dist-electron/boot.cjs" }, bundle),
    );
  },
);

NodeTest.test(
  "fixture creation and cleanup preserve a competing home or profile creator",
  async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-fixture-ownership-"));
    const metadata = {
      version: "fixture",
      jonesSource: { sha: "a".repeat(40), tree: "b".repeat(40) },
    };
    try {
      for (const collision of ["home", "profile"]) {
        const directory = NodePath.join(root, collision);
        await NodeFSP.mkdir(directory);
        const home = NodePath.join(directory, "home");
        const profile = NodePath.join(directory, "profile");
        const competing = collision === "home" ? home : profile;
        await NodeAssert.rejects(NodeFSP.lstat(competing), { code: "ENOENT" });
        // A negative observation is deliberately followed by a competing create.
        await NodeFSP.mkdir(competing);
        await NodeFSP.writeFile(NodePath.join(competing, "foreign"), "preserve");
        const ownership = new FixtureOwnership();
        await NodeAssert.rejects(createNativeState(home, profile, metadata, ownership), {
          code: "EEXIST",
        });
        await ownership.cleanup();
        NodeAssert.equal(
          await NodeFSP.readFile(NodePath.join(competing, "foreign"), "utf8"),
          "preserve",
        );
        if (collision === "profile")
          await NodeAssert.rejects(NodeFSP.stat(home), { code: "ENOENT" });
      }
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "fixture cleanup preserves replaced roots and does not adopt occupied lease paths",
  async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-fixture-replaced-"));
    try {
      const profile = NodePath.join(root, "profile");
      const ownership = new FixtureOwnership();
      await ownership.mkdir(profile);
      await NodeFSP.rename(profile, `${profile}.retained`);
      await NodeFSP.mkdir(profile);
      await NodeFSP.writeFile(NodePath.join(profile, "foreign"), "preserve");
      await NodeAssert.rejects(ownership.cleanup(), /ownership/);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(profile, "foreign"), "utf8"),
        "preserve",
      );
      const lease = NodePath.join(root, "lease.sqlite");
      await NodeFSP.writeFile(lease, "foreign lease");
      const files = new FixtureOwnership();
      await NodeAssert.rejects(files.file(lease, "replacement"), { code: "EEXIST" });
      await files.cleanup();
      NodeAssert.equal(await NodeFSP.readFile(lease, "utf8"), "foreign lease");
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);

NodeTest.test("a closed leader cannot leave an unref'd same-group grandchild alive", async () => {
  const result = await runOwnedChild(process.execPath, [
    "-e",
    `
    const child = require('node:child_process').spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], {stdio: 'ignore'});
    child.unref();
    process.stdout.write(JSON.stringify({ group: process.pid, grandchild: child.pid }));
  `,
  ]);
  NodeAssert.equal(result.code, 0, result.stderr);
  const ids = JSON.parse(result.stdout);
  NodeAssert.throws(() => process.kill(-ids.group, 0), { code: "ESRCH" });
  NodeAssert.throws(() => process.kill(ids.grandchild, 0), { code: "ESRCH" });
  assertFixtureProcessesStopped();
});

NodeTest.test(
  "qualification removes pre-JS logging and Node-mode overrides while preserving the native home",
  () => {
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
    NodeAssert.deepEqual(env, {
      HOME: "/runner",
      PATH: "/bin",
      TMPDIR: "/fixture",
      TMP: "/fixture",
      TEMP: "/fixture",
      NODE_DISABLE_COMPILE_CACHE: "1",
    });
  },
);

NodeTest.test(
  "observer preserves transient mutation evidence and fails closed on dropped or ambiguous history",
  () => {
    const events = [
      { path: "/profile/deleted-before-snapshot", flags: 0x100 | 0x200 },
      { path: "/profile-other/file", flags: 0x100 },
    ];
    NodeAssert.deepEqual(protectedMutations(events, ["/profile"]), [events[0]]);
    for (const flag of [0x01, 0x02, 0x04, 0x08, 0x20, 0x40, 0x80])
      NodeAssert.throws(
        () => protectedMutations([{ path: "/unrelated", flags: flag }], ["/profile"]),
        /history/,
      );
  },
);

async function controlFixture(fault, run) {
  const ownership = new FixtureOwnership();
  const root = await ownership.temporary(NodePath.join(NodeOS.tmpdir(), "jones-observer-phases-"));
  const events = [];
  const phases = [];
  const attempts = new Map();
  let prior = "";
  const observer = {
    events,
    async flush() {
      const directories = await NodeFSP.readdir(root);
      if (directories.length === 0) return events.length;
      NodeAssert.equal(directories.length, 1);
      const directory = NodePath.join(root, directories[0]);
      const leaves = await NodeFSP.readdir(directory);
      NodeAssert.ok(
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
        NodeAssert.equal(
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
        path: fault === phase ? directory : NodePath.join(directory, leaf),
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
    NodeAssert.deepEqual(await NodeFSP.readdir(root), []);
  } finally {
    await ownership.cleanup();
  }
}

NodeTest.test(
  "observer control awaits paced phases and detects a rapid burst after every leaf disappears",
  () =>
    controlFixture(undefined, async ({ root, observer, phases }) => {
      await observerControl(observer, [root]);
      NodeAssert.deepEqual(phases, ["created", "renamed", "removed", "rapid-burst"]);
      NodeAssert.deepEqual(
        observer.events.map((event) => event.flags),
        [0x100, 0x800, 0x200, 0x100 | 0x800 | 0x200],
      );
      for (const event of observer.events)
        await NodeAssert.rejects(NodeFSP.lstat(event.path), { code: "ENOENT" });
    }),
);

NodeTest.test(
  "paced detection cannot qualify an observer that misses leaves disappearing between flushes",
  () =>
    controlFixture("present-only", async ({ root, observer, phases }) => {
      await NodeAssert.rejects(observerControl(observer, [root], { timeoutMs: 100 }), (error) => {
        NodeAssert.ok(error instanceof ObserverControlError);
        NodeAssert.equal(error.control.phase, "rapid-burst");
        NodeAssert.deepEqual(phases, ["created", "renamed", "removed"]);
        NodeAssert.equal(error.control.observedCount, 0);
        NodeAssert.match(error.message, /required leaf mutation/);
        return true;
      });
    }),
);

for (const missing of ["created", "renamed", "removed", "rapid-burst"])
  NodeTest.test(
    `observer control rejects missing ${missing} leaf evidence and retains bounded phase details`,
    () =>
      controlFixture(missing, async ({ root, observer }) => {
        await NodeAssert.rejects(observerControl(observer, [root], { timeoutMs: 100 }), (error) => {
          NodeAssert.ok(error instanceof ObserverControlError);
          NodeAssert.equal(error.control.phase, missing);
          NodeAssert.ok(error.control.paths.every((name) => !NodePath.isAbsolute(name)));
          NodeAssert.equal(error.control.events.length, 1);
          NodeAssert.equal(
            error.control.events[0].flags,
            {
              created: 0x100,
              renamed: 0x800,
              removed: 0x200,
              "rapid-burst": 0x100 | 0x800 | 0x200,
            }[missing],
          );
          NodeAssert.match(error.message, /required leaf mutation/);
          return true;
        });
      }),
  );

NodeTest.test(
  "observer control fails on lost history even when the positive leaf event is present",
  () =>
    controlFixture("history", async ({ root, observer }) => {
      await NodeAssert.rejects(observerControl(observer, [root]), (error) => {
        NodeAssert.ok(error instanceof ObserverControlError);
        NodeAssert.equal(error.control.phase, "created");
        NodeAssert.equal(error.control.events[0].flags, 0x102);
        NodeAssert.match(error.message, /lost history/);
        return true;
      });
    }),
);

NodeTest.test(
  "observer consumes acknowledged flushes without exhausting its unmatched-message bound",
  async () => {
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
      for (let index = 0; index < 60; index++) NodeAssert.equal(await observer.flush(), 0);
    } finally {
      await observer.close();
    }
  },
);

NodeTest.test(
  "cancelled qualification reaps only its captured child before fixture removal",
  async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-qualifier-cancel-"));
    const marker = NodePath.join(root, "pid");
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
            await NodeFSP.stat(marker);
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
      const pid = Number(await NodeFSP.readFile(marker, "utf8"));
      controller.abort();
      await NodeAssert.rejects(completion, /cancelled/);
      NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    } finally {
      clearInterval(timer);
      controller.abort();
      await completion?.catch(() => {});
      assertFixtureProcessesStopped();
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);
