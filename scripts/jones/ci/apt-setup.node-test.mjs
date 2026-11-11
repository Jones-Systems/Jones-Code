import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeStream from "node:stream";
import * as NodeTest from "node:test";
import {
  candidate,
  workspace,
  isolation,
  parseArguments,
  validateSources,
  validateTargets,
  classifyFailure,
  createCommandRunner,
  groupMembers,
  setupApt,
} from "./apt-setup.mjs";

const pockets = ["noble", "noble-updates", "noble-backports", "noble-security"];
const components = ["main", "restricted", "universe", "multiverse"];
const sources = ["noble noble-updates noble-backports", "noble-security"]
  .map(
    (suites) =>
      `Types: deb\nURIs: mirror+file:/etc/apt/apt-mirrors.txt\nSuites: ${suites}\nComponents: main restricted universe multiverse\nSigned-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n`,
  )
  .join("\n");
const mirrors =
  "http://azure.archive.ubuntu.com/ubuntu/\tpriority:1\nhttps://archive.ubuntu.com/ubuntu/\tpriority:2\nhttps://security.ubuntu.com/ubuntu/\tpriority:3\n";
const sourceFixture = {
  sources,
  mirrors,
  architecture: "amd64\n",
  osRelease: 'VERSION_CODENAME="noble"\n',
};
const targetText = pockets
  .flatMap((pocket) =>
    components.map(
      (component) =>
        `URI: ${candidate}dists/${pocket}/${component}/binary-amd64/Packages\nRepo-URI: ${candidate}\nFilename: ${workspace}/lists/${pocket}_${component}_Packages\nFallback-Of:\nIdentifier: Packages\nArchitecture: amd64\nComponent: ${component}\n`,
    ),
  )
  .join("\n");
const dns =
  "Err:2 https://security.ubuntu.com/ubuntu noble InRelease\n  Temporary failure resolving 'azure.archive.ubuntu.com'\nW: Failed to fetch mirror+file:/etc/apt/apt-mirrors.txt/dists/noble/InRelease  Temporary failure resolving 'azure.archive.ubuntu.com'\nW: Some index files failed to download. They have been ignored, or old ones used instead.\n";
const ok = (extra = {}) => ({
  code: 0,
  stdout: "",
  stderr: "",
  quiescent: true,
  timedOut: false,
  cancelled: false,
  ...extra,
});
const network = (extra = {}) => ok({ code: 100, stderr: dns, ...extra });

function scenario({
  intercept = () => {},
  files = {},
  route = "auto",
  args = ["--no-install-recommends", "build-essential", "libsecret-1-dev", "pkg-config"],
  signal,
  runner,
} = {}) {
  const calls = [],
    reads = [],
    logs = [];
  const options = {
    args,
    route,
    signal,
    now: () => Date.parse("2026-11-11T16:00:00Z"),
    report: (message) => logs.push(message),
    async readFile(file) {
      reads.push(file);
      if (Object.hasOwn(files, file)) {
        if (files[file] instanceof Error) throw files[file];
        return files[file];
      }
      if (file === "/etc/os-release") return sourceFixture.osRelease;
      if (file === "/etc/apt/apt-mirrors.txt") return mirrors;
      if (file === "/etc/apt/sources.list.d/ubuntu.sources") return sources;
      if (file.startsWith(`${workspace}/lists/`)) return "Date: Wed, 11 Nov 2026 15:00:00 UTC\n";
      throw new Error(`unexpected read: ${file}`);
    },
    async run(argv, settings) {
      calls.push({ argv, settings });
      const special = await intercept(argv, settings, calls.length);
      if (special) return special;
      if (runner && argv[0] === "/usr/bin/apt-get" && !argv.includes(isolation[1]))
        return runner(argv, settings);
      if (argv.includes("--print-architecture")) return ok({ stdout: "amd64\n" });
      if (argv.includes("indextargets")) return ok({ stdout: targetText });
      return ok();
    },
  };
  return { calls, reads, logs, options };
}
const aptCalls = (s) => s.calls.filter(({ argv }) => argv[0] === "/usr/bin/apt-get");
const removed = (s) => s.calls.some(({ argv }) => argv[0] === "/usr/bin/rm");
const rejectsLabel = (promise, label) =>
  NodeAssert.rejects(promise, (error) => error.label === label);

NodeTest.test(
  "normal route refreshes once, downloads then installs offline with package/recommends parity",
  async () => {
    for (const args of [
      ["libsecret-1-dev", "pkg-config"],
      ["--no-install-recommends", "build-essential", "libsecret-1-dev", "pkg-config"],
    ]) {
      const s = scenario({ args });
      await setupApt(s.options);
      NodeAssert.deepEqual(
        aptCalls(s).map(({ argv }) => argv.slice(1)),
        [
          ["update", "--error-on=any"],
          ["install", "-y", ...args, "--download-only"],
          ["install", "-y", ...args, "--no-download", "-o", "DPkg::Lock::Timeout=30"],
        ],
      );
      NodeAssert.deepEqual(
        s.calls.map(({ settings }) => settings.timeoutMs),
        [60_000, 60_000, 120_000],
      );
      NodeAssert.deepEqual(s.reads, []);
    }
  },
);

NodeTest.test(
  "transport failure at refresh or download enters isolated candidate and never reuses default lists",
  async () => {
    for (const failingCall of [1, 2]) {
      const s = scenario({
        intercept: (_argv, _settings, count) => (count === failingCall ? network() : undefined),
      });
      await setupApt(s.options);
      const later = aptCalls(s).slice(failingCall);
      NodeAssert.equal(later.length, 4);
      for (const { argv } of later)
        NodeAssert.deepEqual(argv.slice(1, 1 + isolation.length), isolation);
      NodeAssert.ok(later[0].argv.includes("--no-release-info"));
      NodeAssert.equal(later.filter(({ argv }) => argv.includes("update")).length, 1);
      NodeAssert.equal(later[1].settings.timeoutMs, 120_000);
      NodeAssert.ok(later[2].argv.includes("--download-only"));
      NodeAssert.ok(later[3].argv.includes("--no-download"));
      NodeAssert.equal(removed(s), true);
      NodeAssert.ok(
        s.reads
          .filter((file) => file.endsWith("InRelease"))
          .every((file) => file.startsWith(`${workspace}/lists/`)),
      );
      const written = s.calls.find(({ argv }) => argv[0] === "/usr/bin/tee");
      NodeAssert.equal(
        written.settings.input,
        sources.replace(/^URIs:.*$/gm, `URIs: ${candidate}`),
      );
    }
  },
);

NodeTest.test(
  "clean deadline may fall back; candidate failure never tries healthy originals",
  async () => {
    const normal = scenario({
      intercept: (_a, _s, n) => (n === 1 ? network({ stderr: "", timedOut: true }) : undefined),
    });
    await setupApt(normal.options);
    NodeAssert.equal(aptCalls(normal).length, 5);
    const forced = scenario({
      route: "candidate",
      intercept: (argv) => (argv.includes("update") ? network() : undefined),
    });
    await rejectsLabel(setupApt(forced.options), "network");
    NodeAssert.ok(aptCalls(forced).every(({ argv }) => argv.includes(isolation[1])));
    NodeAssert.equal(removed(forced), true);
  },
);

NodeTest.test(
  "terminal diagnostics override transport/timeouts; exit 100 and HTTP 404 stay unclassified",
  async () => {
    for (const [diagnostic, label] of [
      ["E: GPG error: NO_PUBKEY bad", "signature"],
      ["E: Hash Sum mismatch", "signature"],
      ["E: Release file for URL is expired (invalid since 2d)", "freshness"],
      ["E: Could not get lock /var/lib/dpkg/lock - open (13: Permission denied)", "config"],
      ["E: Command line option --bad is not understood", "config"],
      ["sudo: a password is required", "config"],
      ["E: unexplained apt failure", "unclassified"],
      ["E: Failed to fetch https://example/file 404 Not Found", "unclassified"],
    ]) {
      for (const timedOut of [false, true]) {
        const result = network({ stderr: `${dns}${diagnostic}\n`, timedOut });
        NodeAssert.equal(classifyFailure(result), label);
        const s = scenario({ intercept: (_a, _s, n) => (n === 1 ? result : undefined) });
        await rejectsLabel(setupApt(s.options), label);
        NodeAssert.equal(s.calls.length, 1);
      }
      if (["config", "signature", "freshness"].includes(label)) {
        const s = scenario({
          intercept: (_a, _s, n) => (n === 1 ? ok({ stderr: diagnostic }) : undefined),
        });
        await rejectsLabel(setupApt(s.options), label);
        NodeAssert.equal(s.calls.length, 1);
      }
    }
    NodeAssert.equal(classifyFailure(network({ stderr: "" })), "unclassified");
    NodeAssert.equal(
      classifyFailure(
        network({
          stderr:
            "E: Unable to fetch some archives, maybe run apt-get update or try with --fix-missing?",
        }),
      ),
      "unclassified",
    );
  },
);

NodeTest.test(
  "preflight rejects altered suite, keyring, mirrors, directives, codename or architecture",
  () => {
    NodeAssert.equal(
      validateSources(sourceFixture),
      sources.replace(/^URIs:.*$/gm, `URIs: ${candidate}`),
    );
    const withHeader = `# Image-owned Ubuntu configuration\n\n${sources}`;
    NodeAssert.equal(
      validateSources({ ...sourceFixture, sources: withHeader }),
      withHeader.replace(/^URIs:.*$/gm, `URIs: ${candidate}`),
    );
    for (const change of [
      { sources: sources.replace("noble-backports", "jammy") },
      { sources: sources.replaceAll("ubuntu-archive-keyring.gpg", "other.gpg") },
      { mirrors: mirrors.replace("priority:3", "priority:4") },
      { sources: `${sources}Trusted: yes\n` },
      { sources: `${sources}Check-Valid-Until: no\n` },
      { sources: sources.replace("Components: main", "Snapshot: test\nComponents: main") },
      { architecture: "arm64" },
      { osRelease: "VERSION_CODENAME=jammy" },
    ])
      NodeAssert.throws(
        () => validateSources({ ...sourceFixture, ...change }),
        (error) => error.label === "config",
      );
  },
);

NodeTest.test(
  "cold isolation proof requires candidate URI, all 4x4 amd64 targets and confined filenames without release fields",
  () => {
    validateTargets(targetText);
    for (const text of [
      targetText.replaceAll("noble-security", "jammy-security"),
      targetText.replace("Repo-URI: " + candidate, "Repo-URI: https://archive.ubuntu.com/ubuntu/"),
      targetText.replace(`Filename: ${workspace}/lists/`, "Filename: /var/lib/apt/lists/"),
      targetText
        .replaceAll("binary-amd64", "binary-arm64")
        .replaceAll("Architecture: amd64", "Architecture: arm64"),
      targetText.split(/\n\n/).slice(1).join("\n\n"),
      "",
    ])
      NodeAssert.throws(
        () => validateTargets(text),
        (error) => error.label === "config",
      );
  },
);

NodeTest.test(
  "candidate preflight, keyring and target-proof failures stop before network",
  async () => {
    for (const kind of ["source", "keyring", "targets"]) {
      const s = scenario({
        route: "candidate",
        files:
          kind === "source"
            ? { "/etc/apt/sources.list.d/ubuntu.sources": sources + "Trusted: yes\n" }
            : {},
        intercept: (argv) =>
          kind === "keyring" && argv.includes("--verify")
            ? ok({ stdout: "changed keyring" })
            : kind === "targets" && argv.includes("indextargets")
              ? ok({ stdout: targetText.replaceAll(candidate, "https://foreign/") })
              : undefined,
      });
      await rejectsLabel(setupApt(s.options), "config");
      NodeAssert.equal(
        aptCalls(s).some(({ argv }) => argv.includes("update")),
        false,
      );
      NodeAssert.equal(removed(s), kind === "targets");
    }
  },
);

NodeTest.test("a preexisting workspace is never removed", async () => {
  const s = scenario({
    route: "candidate",
    intercept: (argv) =>
      argv[0] === "/usr/bin/mkdir" ? ok({ code: 1, stderr: "File exists" }) : undefined,
  });
  await rejectsLabel(setupApt(s.options), "config");
  NodeAssert.equal(removed(s), false);
  NodeAssert.equal(aptCalls(s).length, 0);
});

NodeTest.test(
  "freshness uses only acquired files and rejects missing, malformed, stale or future dates",
  async () => {
    for (const value of [
      new Error("missing"),
      "Date: nonsense\n",
      "Date: Sun, 01 Nov 2026 15:00:00 UTC\n",
      "Date: Thu, 12 Nov 2026 15:00:00 UTC\n",
      "Date: Wed, 11 Nov 2026 15:00:00 UTC\nDate: Wed, 11 Nov 2026 15:00:00 UTC\n",
    ]) {
      const s = scenario({
        route: "candidate",
        files: {
          [`${workspace}/lists/mirrors.edge.kernel.org_ubuntu_dists_noble-security_InRelease`]:
            value,
        },
      });
      await rejectsLabel(setupApt(s.options), "freshness");
      NodeAssert.equal(
        aptCalls(s).some(({ argv }) => argv.includes("install")),
        false,
      );
      NodeAssert.equal(removed(s), true);
    }
  },
);

NodeTest.test(
  "invalid positional packages/options and selectors cannot become privileged path overrides",
  async () => {
    for (const args of [
      [],
      ["-o"],
      ["pkg;true"],
      ["/tmp/pkg"],
      ["pkg", "--no-install-recommends"],
      ["--no-install-recommends"],
    ]) {
      NodeAssert.throws(
        () => parseArguments(args),
        (error) => error.label === "config",
      );
    }
    for (const route of ["other", "AUTO", "/tmp/sources"])
      NodeAssert.throws(
        () => parseArguments(["pkg"], route),
        (error) => error.label === "config",
      );
  },
);

function processFixture({
  survivor = false,
  immortal = false,
  lingering = false,
  missing = false,
  liveMissing = false,
  cancel,
  cancelOnQuiet,
  fastExit,
  lateDiagnostic,
} = {}) {
  const events = [],
    logs = [];
  let alive = false,
    leader,
    tick = 0;
  const launch = (file, argv) => {
    const child = new NodeEvents.EventEmitter();
    child.stdout = new NodeStream.PassThrough();
    child.stderr = new NodeStream.PassThrough();
    child.stdin = new NodeStream.Writable({
      write(_chunk, _encoding, done) {
        done();
      },
    });
    if (file === "/usr/bin/kill") {
      NodeAssert.equal(argv.at(-1), "-712");
      const sig = argv[1];
      events.push(sig);
      queueMicrotask(() => {
        if (sig === "TERM") {
          leader.emit("exit", null, "SIGTERM");
          if (!survivor) alive = false;
        }
        if (sig === "KILL" && !immortal) alive = false;
        child.emit("exit", 0);
        leader.emit("close");
      });
    } else {
      NodeAssert.equal(file, "/usr/bin/setsid");
      NodeAssert.deepEqual(argv.slice(0, 3), ["--wait", "/bin/sh", "-c"]);
      leader = child;
      queueMicrotask(() => {
        alive = true;
        if (!missing) child.stdout.write("jones-apt-group 712\n");
        else if (!liveMissing) {
          alive = false;
          child.emit("exit", 1);
          child.emit("close");
        }
      });
      child.stdin.on("finish", () => {
        events.push("ack");
        if (cancel) cancel();
        if (lingering) queueMicrotask(() => child.emit("exit", 0));
        if (fastExit !== undefined)
          queueMicrotask(() => {
            alive = false;
            child.emit("exit", fastExit);
            setImmediate(() => {
              if (lateDiagnostic) child.stderr.write(lateDiagnostic);
              child.emit("close");
            });
          });
      });
    }
    return child;
  };
  const run = createCommandRunner({
    launch,
    privilege: [],
    report: (line) => logs.push(line),
    readStat: async () => {
      events.push("identity");
      return { group: 712, session: 712 };
    },
    members: async () => {
      if (cancelOnQuiet) cancelOnQuiet();
      return alive ? [713] : [];
    },
    markerMs: 15,
    graceMs: 5,
    pollMs: 1,
    now: () => performance.now() + tick,
    pause: async (ms) => {
      tick += ms;
    },
  });
  return { run, events, logs };
}

NodeTest.test(
  "owned runner confirms live identity before ack; TERM leader exit still escalates survivor to KILL before fallback",
  async () => {
    const fixture = processFixture({ survivor: true });
    const s = scenario({
      runner: (argv, settings) => fixture.run(argv, { ...settings, timeoutMs: 15 }),
    });
    await setupApt(s.options);
    NodeAssert.deepEqual(fixture.events, ["identity", "ack", "TERM", "KILL"]);
    NodeAssert.ok(fixture.logs.some((line) => line.includes("712: deadline hit")));
    NodeAssert.ok(fixture.logs.some((line) => line.includes("712: quiescence proven")));
    NodeAssert.equal(s.calls[1].argv[0], "/usr/bin/dpkg");
  },
);

NodeTest.test(
  "survivor outliving KILL stops candidate and retains an owned workspace",
  async () => {
    const fixture = processFixture({ survivor: true, immortal: true });
    const s = scenario({
      route: "candidate",
      intercept: (argv) =>
        argv.includes("update") ? fixture.run(argv, { timeoutMs: 15 }) : undefined,
    });
    await rejectsLabel(setupApt(s.options), "cleanup-unresolved");
    NodeAssert.deepEqual(fixture.events, ["identity", "ack", "TERM", "KILL"]);
    NodeAssert.equal(removed(s), false);
    NodeAssert.equal(
      aptCalls(s).some(({ argv }) => argv.includes("install")),
      false,
    );
  },
);

NodeTest.test("cancellation terminates the current group and never falls back", async () => {
  const controller = new AbortController();
  const fixture = processFixture({ survivor: true, cancel: () => controller.abort() });
  const s = scenario({ signal: controller.signal, runner: fixture.run });
  await rejectsLabel(setupApt(s.options), "cancelled");
  NodeAssert.equal(s.calls.length, 1);
  NodeAssert.deepEqual(fixture.events, ["identity", "ack", "TERM", "KILL"]);
});

NodeTest.test(
  "successful lingering member continues only the same route, never signals and skips removal",
  async () => {
    const fixture = processFixture({ lingering: true });
    const s = scenario({
      route: "candidate",
      intercept: (argv) => (argv.includes("update") ? fixture.run(argv) : undefined),
    });
    await setupApt(s.options);
    NodeAssert.deepEqual(fixture.events, ["identity", "ack"]);
    NodeAssert.equal(removed(s), false);
    NodeAssert.equal(aptCalls(s).filter(({ argv }) => argv.includes("install")).length, 2);
    const failure = scenario({
      intercept: (_a, _s, n) =>
        n === 1 ? ok({ quiescent: false }) : n === 2 ? network() : undefined,
    });
    await rejectsLabel(setupApt(failure.options), "cleanup-unresolved");
    NodeAssert.equal(failure.calls.length, 2);
  },
);

NodeTest.test(
  "fast gated commands wait for diagnostics after exit before classification",
  async () => {
    const quick = processFixture({ fastExit: 0 });
    const result = await quick.run(["/bin/true"]);
    NodeAssert.equal(result.code, 0);
    NodeAssert.deepEqual(quick.events, ["identity", "ack"]);
    const late = processFixture({
      fastExit: 100,
      lateDiagnostic: `${dns}E: GPG error: BADSIG bad\n`,
    });
    const s = scenario({ runner: late.run });
    await rejectsLabel(setupApt(s.options), "signature");
    NodeAssert.equal(s.calls.length, 1);
  },
);

NodeTest.test("cancellation during the post-exit wait terminates surviving members", async () => {
  const controller = new AbortController();
  const fixture = processFixture({
    lingering: true,
    survivor: true,
    cancelOnQuiet: () => controller.abort(),
  });
  const result = await fixture.run(["/bin/true"], { signal: controller.signal });
  NodeAssert.equal(result.cancelled, true);
  NodeAssert.equal(result.quiescent, true);
  NodeAssert.deepEqual(fixture.events, ["identity", "ack", "TERM", "KILL"]);
});

NodeTest.test(
  "missing marker distinguishes exited child from live unresolved child without signaling an unconfirmed group",
  async () => {
    for (const liveMissing of [false, true]) {
      const fixture = processFixture({ missing: true, liveMissing });
      await rejectsLabel(fixture.run(["/bin/true"]), liveMissing ? "cleanup-unresolved" : "config");
      NodeAssert.deepEqual(fixture.events, []);
    }
  },
);

NodeTest.test(
  "Linux real process: TERM exits leader but KILL reaps its surviving group before returning",
  {
    skip:
      // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone process regression must identify the actual host before using Linux process groups.
      process.platform !== "linux"
        ? "requires Linux setsid, /proc and kill; Mac does not qualify production process machinery"
        : false,
  },
  async (t) => {
    const logs = [];
    // The child ignores TERM before telling its parent it is ready. No files or ports are created.
    const script = `trap 'exit 0' TERM
/bin/sh -c 'trap "" TERM; printf "jones-survivor-ready\\n"; while :; do sleep 60; done' &
wait`;
    let survivorReady = false;
    const run = createCommandRunner({
      privilege: [],
      graceMs: 100,
      pollMs: 10,
      report: (line) => {
        logs.push(line);
        if (line.includes("jones-survivor-ready")) survivorReady = true;
      },
    });
    const result = await run(["/bin/sh", "-c", script], { timeoutMs: 1_000, signal: t.signal });
    NodeAssert.equal(survivorReady, true);
    NodeAssert.equal(result.timedOut, true);
    NodeAssert.equal(result.quiescent, true);
    NodeAssert.ok(logs.some((line) => line.endsWith("send TERM")));
    NodeAssert.ok(logs.some((line) => line.endsWith("send KILL")));
    const group = Number(
      logs.find((line) => line.includes("captured and confirmed")).match(/group (\d+)/)[1],
    );
    NodeAssert.deepEqual(await groupMembers(group), []);
  },
);

NodeTest.test(
  "primary workflow uses one exact helper call per package set, preserves compiler checks and dispatch selection",
  async () => {
    const workflow = await NodeFSP.readFile(
      new URL("../../../.github/workflows/ci.yml", import.meta.url),
      "utf8",
    );
    for (const job of ["build", "test", "test_desktop"]) {
      const block = workflow.split(`\n  ${job}:\n`)[1].split(/\n  [\w-]+:\n/)[0];
      NodeAssert.equal(block.match(/node scripts\/jones\/ci\/apt-setup.mjs/g)?.length, 1);
      NodeAssert.doesNotMatch(block, /apt-get/);
      const expected =
        job === "build"
          ? "libsecret-1-dev pkg-config"
          : "--no-install-recommends build-essential libsecret-1-dev pkg-config";
      NodeAssert.ok(block.includes(`node scripts/jones/ci/apt-setup.mjs ${expected}\n`));
      NodeAssert.match(block, /JONES_CI_APT_ROUTE: \$\{\{ inputs.apt_route \}\}/);
      if (job !== "build") NodeAssert.match(block, /command -v cc/);
    }
    NodeAssert.match(
      workflow,
      /apt_route:\n[\s\S]*?type: choice\n\s+options:\n\s+- auto\n\s+- candidate\n\s+default: auto/,
    );
    NodeAssert.match(
      workflow,
      /node --test --test-concurrency=1 scripts\/jones\/ci\/\*\.node-test\.\*/,
    );
  },
);
