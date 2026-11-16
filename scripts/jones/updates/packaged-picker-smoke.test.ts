// @effect-diagnostics nodeBuiltinImport:off - Fail-closed native consumer preflight and exact producer binding.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { afterEach, beforeAll, expect, it } from "vite-plus/test";
import {
  scanPickerBundle,
  readPickerFuses,
  requirePickerCopyPath,
  evaluatePickerReports,
  expectedPickerChecks,
  verifyPickerManifest,
  locatePickerProducer,
  pickerProducerApplicable,
  runPickerPhase,
  requirePickerPhaseCleanup,
  pickerErrorDetail,
  type PickerPhaseObservation,
} from "./packaged-picker-smoke.ts";
import { processAbsence, withSmokeScratch } from "./desktop-worker-smoke.ts";

it("accepts only the entry's declared literal external modules", () => {
  expect(
    scanPickerBundle('const ipc=require("electron");require("node:path");', "main").dependencies,
  ).toEqual(["electron", "node:path"]);
  expect(scanPickerBundle('require("electron")', "preload").dependencies).toEqual(["electron"]);
  for (const source of [
    "require(id)",
    'const load=require;load("electron")',
    'require("node:fs")',
    'require("native.node")',
    "import(name)",
    'module.require("electron")',
    'require("./main.cjs")',
    'require("@clerk/electron")',
    'require("electron-updater")',
    'Module.createRequire(__filename)("original-fs")',
    "process.dlopen(x)",
  ])
    expect(() => scanPickerBundle(source, "preload")).toThrow();
  for (const source of [
    'const {createRequire: load} = require("node:module"); load(__filename)(name)',
    'import {createRequire as load} from "node:module"; load(import.meta.url)(name)',
    'const Module = require("module"); Module._load(name)',
    'process.getBuiltinModule("module").createRequire(__filename)(name)',
    "process.getBuiltinModule(name)",
    "const load = process.getBuiltinModule; load(name)",
  ])
    expect(() => scanPickerBundle(source, "main")).toThrow();
});
it("rejects unknown and enabled OnlyLoadAppFromAsar fuse layouts", () => {
  const binary = Buffer.concat([
    Buffer.from("dL7pKGdnNz796PbbjQWNKmHXBZaB9tsX"),
    Buffer.from([1, 8, 0x31, 0x30, 0x31, 0x31, 0x30, 0x30, 0x30, 0x31]),
  ]);
  expect(readPickerFuses(binary).onlyLoadAppFromAsar).toBe(0x30);
  binary[binary.length - 3] = 0x31;
  expect(() => readPickerFuses(binary)).toThrow("not off");
  expect(() => readPickerFuses(Buffer.alloc(100))).toThrow("Missing");
  expect(() => readPickerFuses(Buffer.concat([binary, binary]))).toThrow("ambiguous");
});
it("refuses unstable copied bundle paths without native execution", () => {
  requirePickerCopyPath("/private/var/folders/runner/picker/Applications/Jones.app");
  for (const value of [
    "/tmp/Jones.app",
    "/Volumes/Jones.app",
    "/x/AppTranslocation/Jones.app",
    "/x/jones-updates/Jones.app",
    "relative.app",
    "/x/../Jones.app",
  ])
    expect(() => requirePickerCopyPath(value)).toThrow();
});
it("never converts missing, synthetic-only or failed evidence into native success", () => {
  const reports = ["catalog", "persisted", "inflight", "held"].map((phase) => ({
    phase,
    status: "pass",
    nativeHop: true,
    checks: [
      "IPC-A1-capability-members",
      "IPC-A2-renderer-require",
      "IPC-A3-renderer-process",
      "IPC-A4-inert-list",
      "IPC-A5-inert-transport",
      "SAFE-A1-zero-native",
      "SAFE-A2-zero-install",
      "SAFE-A3-zero-transport-download",
      "SAFE-A4-no-install-operation",
      ...expectedPickerChecks[phase as keyof typeof expectedPickerChecks],
    ].map((id) => ({ id, status: "pass" })),
    preferences: { contextIsolation: true, nodeIntegration: false, sandbox: true },
    violations: [],
    nativeCalls: [],
    installCalls: 0,
  }));
  expect(evaluatePickerReports(reports)).toBe("PASS");
  expect(() => evaluatePickerReports([])).toThrow();
  expect(() =>
    evaluatePickerReports(reports.map((value) => ({ ...value, nativeHop: false }))),
  ).toThrow();
  expect(() =>
    evaluatePickerReports(reports.map((value) => ({ ...value, installCalls: 1 }))),
  ).toThrow();
});
it("binds the embedded synthetic merge source independently of the producer API head SHA", () => {
  const binding = {
    source: "a".repeat(40),
    tree: "b".repeat(40),
    runId: "123",
    attempt: "2",
    event: "pull_request",
    ref: "refs/pull/872/merge",
  };
  const value = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    ...binding,
    source: binding.source,
    tree: binding.tree,
    runId: binding.runId,
    runAttempt: binding.attempt,
    event: binding.event,
    ref: binding.ref,
    workflow: ".github/workflows/artifact-desktop-mac.yml",
    platform: "darwin",
    architecture: "arm64",
    signed: false,
    notarized: false,
    version: "0.0.44-preview.20261116.123.2",
    artifact: "T3-Code-0.0.44-preview.20261116.123.2-arm64.dmg",
    sha256: "c".repeat(64),
  };
  expect(verifyPickerManifest(value, binding).source).toBe(binding.source);
  expect(() => verifyPickerManifest({ ...value, source: "d".repeat(40) }, binding)).toThrow(
    "mismatch",
  );
  expect(() => verifyPickerManifest({ ...value, runAttempt: "1" }, binding)).toThrow("mismatch");
});
it("bounds producer discovery and rejects stale API sources", async () => {
  let calls = 0,
    waits = 0;
  await expect(
    locatePickerProducer(
      {
        source: "a".repeat(40),
        tree: "b".repeat(40),
        head: "c".repeat(40),
        event: "pull_request",
        pr: 872,
      },
      async () => {
        calls++;
        return {
          workflow_runs: [
            {
              id: 1,
              path: ".github/workflows/artifact-desktop-mac.yml",
              event: "pull_request",
              head_sha: "d".repeat(40),
              status: "completed",
              conclusion: "success",
              repository: { full_name: "Jones-Systems/Jones-Code" },
              head_repository: { full_name: "Jones-Systems/Jones-Code" },
              pull_requests: [{ number: 872 }],
            },
          ],
        };
      },
      async () => {
        waits++;
      },
      3,
    ),
  ).rejects.toThrow("UNAVAILABLE");
  expect(calls).toBe(3);
  expect(waits).toBe(2);
});
const parents: string[] = [];
beforeAll(async () => {
  await NodeFSP.mkdir(NodePath.resolve("node_modules/.cache"), { recursive: true });
});
afterEach(async () => {
  for (const parent of parents.splice(0)) await NodeFSP.rm(parent, { recursive: true });
});
it("cleans exact concurrent invocation roots on success and failure without losing the original result", async () => {
  const parent = await NodeFSP.mkdtemp(NodePath.resolve("node_modules/.cache/picker-smoke-"));
  parents.push(parent);
  const owned: string[] = [];
  const outcomes = await Promise.allSettled([
    withSmokeScratch(parent, async (root) => {
      owned.push(root);
      await NodeFSP.writeFile(NodePath.join(root, "evidence"), "fixture");
      return "ok";
    }),
    withSmokeScratch(parent, async (root) => {
      owned.push(root);
      throw new Error("original failure");
    }),
  ]);
  expect(outcomes[0]).toEqual({ status: "fulfilled", value: "ok" });
  expect(outcomes[1]).toMatchObject({
    status: "rejected",
    reason: expect.objectContaining({ message: "original failure" }),
  });
  expect(new Set(owned).size).toBe(2);
  expect(await NodeFSP.readdir(parent)).toEqual([]);
  await expect(
    withSmokeScratch(
      parent,
      async () => {
        throw new Error("primary");
      },
      async () => {
        throw new Error("cleanup");
      },
    ),
  ).rejects.toMatchObject({
    errors: [
      expect.objectContaining({ message: "primary" }),
      expect.objectContaining({ message: "cleanup" }),
    ],
  });
});

async function phaseFixture(body: string) {
  const parent = await NodeFSP.mkdtemp(NodePath.resolve("node_modules/.cache/picker-failure-"));
  parents.push(parent);
  const output = NodePath.join(parent, "upload"),
    executable = NodePath.join(parent, "child.cjs");
  await NodeFSP.mkdir(output);
  await NodeFSP.writeFile(
    executable,
    `#!${process.execPath}\nconst fs=require("node:fs"),path=require("node:path");const config=JSON.parse(fs.readFileSync(process.argv[2].slice("--jones-picker-config=".length),"utf8"));${body}`,
    { mode: 0o700 },
  );
  const observation: PickerPhaseObservation = {
    phase: "catalog",
    processAbsence: "unknown",
    groupAbsence: "unknown",
    evidence: [],
  };
  let root = "";
  const run = (ports = {}, signal = new AbortController().signal) =>
    withSmokeScratch(
      parent,
      async (owned) => {
        root = owned;
        return runPickerPhase(
          executable,
          { root, phase: "catalog", report: NodePath.join(root, "phase-catalog.json") },
          {},
          signal,
          output,
          observation,
          ports,
        );
      },
      async (owned) => requirePickerPhaseCleanup(observation, owned),
    );
  return { output, executable, observation, run, root: () => root };
}
const diagnostics = `fs.writeFileSync(config.report,JSON.stringify({status:"fail",error:"known catalog error"}));fs.writeFileSync(path.join(config.root,"loader-catalog.json"),JSON.stringify({loaded:true}));console.error("known Electron error");`;

it.each([0, 1])(
  "preserves available diagnostics before exact scratch cleanup for child exit %s",
  async (exit) => {
    const fixture = await phaseFixture(`${diagnostics}process.exitCode=${exit};`);
    if (exit === 1) await expect(fixture.run()).rejects.toThrow("exited 1");
    else await expect(fixture.run()).resolves.toMatchObject({ status: "fail" });
    expect(fixture.observation).toMatchObject({
      exitCode: exit,
      processAbsence: "pass",
      groupAbsence: "pass",
    });
    await expect(NodeFSP.lstat(fixture.root())).rejects.toMatchObject({ code: "ENOENT" });
    await expect(
      NodeFSP.readFile(NodePath.join(fixture.output, "electron-catalog.log"), "utf8"),
    ).resolves.toContain("known Electron error");
    await expect(
      NodeFSP.readFile(NodePath.join(fixture.output, "phase-catalog.json"), "utf8"),
    ).resolves.toContain("known catalog error");
    await expect(
      NodeFSP.readFile(NodePath.join(fixture.output, "loader-catalog.json"), "utf8"),
    ).resolves.toContain("loaded");
    // A zero exit with a failed scenario report still cannot satisfy native acceptance.
    if (exit === 0)
      expect(() => evaluatePickerReports([{ phase: "catalog", status: "fail" }])).toThrow();
  },
);
it.each(["fail", "unknown"] as const)(
  "retains the owned root when group absence is %s independently of exit 1",
  async (groupAbsence) => {
    const fixture = await phaseFixture(`${diagnostics}process.exitCode=1;`);
    await expect(
      fixture.run({ processAbsence: (pid: number) => (pid < 0 ? groupAbsence : "pass") }),
    ).rejects.toThrow("scratch cleanup failed");
    expect(fixture.observation).toMatchObject({
      exitCode: 1,
      processAbsence: "pass",
      groupAbsence,
    });
    expect(
      await NodeFSP.readFile(NodePath.join(fixture.output, "electron-catalog.log"), "utf8"),
    ).toContain("known Electron error");
    expect((await NodeFSP.lstat(fixture.root())).isDirectory()).toBe(true);
    // The failed probe is synthetic. Confirm actual child/group exit before test-owned teardown.
    expect(processAbsence(fixture.observation.pid!)).toBe("pass");
    expect(processAbsence(-fixture.observation.pid!)).toBe("pass");
  },
);
it.each([0, 1])(
  "records an absent phase report without passing or hiding child exit %s",
  async (exit) => {
    const fixture = await phaseFixture(
      `console.error("startup failure before report");process.exitCode=${exit};`,
    );
    await expect(fixture.run()).rejects.toThrow(exit === 1 ? "exited 1" : "ENOENT");
    expect(fixture.observation.evidence).toEqual([
      { file: "phase-catalog.json", status: "absent" },
      { file: "electron-catalog.log", status: "copied" },
      { file: "loader-catalog.json", status: "absent" },
    ]);
    expect(fixture.observation).toMatchObject({
      exitCode: exit,
      processAbsence: "pass",
      groupAbsence: "pass",
    });
    expect(
      await NodeFSP.readFile(NodePath.join(fixture.output, "electron-catalog.log"), "utf8"),
    ).toContain("startup failure before report");
    await expect(NodeFSP.lstat(fixture.root())).rejects.toMatchObject({ code: "ENOENT" });
  },
);
it.each([
  { exit: 0, code: "ENOENT" },
  { exit: 1, code: "EACCES" },
])(
  "retains uncopied diagnostics and primary failure for copy error after exit $exit ($code)",
  async ({ exit, code }) => {
    const fixture = await phaseFixture(`${diagnostics}process.exitCode=${exit};`);
    const failure = await fixture
      .run({
        copyFile: async (source: string, destination: string) => {
          if (source.endsWith("phase-catalog.json"))
            throw Object.assign(new Error("known copy failure"), { code });
          await NodeFSP.copyFile(source, destination);
        },
      })
      .then(
        () => {
          throw new Error("copy failure passed");
        },
        (error: unknown) => error,
      );
    const detail = pickerErrorDetail(failure);
    expect(JSON.stringify(detail)).toContain("known copy failure");
    if (exit === 1) expect(detail.errors?.[0]?.errors?.[0]?.message).toContain("exited 1");
    expect(fixture.observation).toMatchObject({
      exitCode: exit,
      processAbsence: "pass",
      groupAbsence: "pass",
    });
    expect(fixture.observation.evidence).toMatchObject([
      { file: "phase-catalog.json", status: "error" },
      { status: "copied" },
      { status: "copied" },
    ]);
    expect(
      await NodeFSP.readFile(NodePath.join(fixture.root(), "phase-catalog.json"), "utf8"),
    ).toContain("known catalog error");
    expect(
      await NodeFSP.readFile(NodePath.join(fixture.output, "electron-catalog.log"), "utf8"),
    ).toContain("known Electron error");
  },
);
it("closes a cancelled child before preserving its diagnostics and probing absence", async () => {
  const fixture = await phaseFixture(
    `${diagnostics}process.kill(process.ppid,"SIGUSR2");setInterval(()=>{},1000);`,
  );
  const controller = new AbortController();
  const cancel = () => controller.abort(new Error("known cancellation"));
  process.once("SIGUSR2", cancel);
  try {
    await expect(fixture.run({}, controller.signal)).rejects.toThrow("aborted");
  } finally {
    process.removeListener("SIGUSR2", cancel);
  }
  expect(fixture.observation).toMatchObject({
    exitCode: null,
    exitSignal: "SIGTERM",
    processAbsence: "pass",
    groupAbsence: "pass",
  });
  expect(
    await NodeFSP.readFile(NodePath.join(fixture.output, "electron-catalog.log"), "utf8"),
  ).toContain("known Electron error");
  await expect(NodeFSP.lstat(fixture.root())).rejects.toMatchObject({ code: "ENOENT" });
});
it("preserves the startup log and spawn error when no child PID is available", async () => {
  const fixture = await phaseFixture(diagnostics);
  await NodeFSP.unlink(fixture.executable);
  const error = await fixture.run().catch((error: unknown) => error);
  expect(pickerErrorDetail(error).errors?.[0]?.errors?.[0]?.code).toBe("ENOENT");
  expect(fixture.observation.pid).toBeUndefined();
  expect(fixture.observation).toMatchObject({ processAbsence: "unknown", groupAbsence: "unknown" });
  expect(fixture.observation.evidence).toContainEqual({
    file: "electron-catalog.log",
    status: "copied",
  });
  expect((await NodeFSP.lstat(fixture.root())).isDirectory()).toBe(true);
});
it("keeps a pre-launch cancellation primary and reports unavailable process identity", async () => {
  const fixture = await phaseFixture(diagnostics),
    controller = new AbortController();
  controller.abort(new Error("cancelled before launch"));
  const error = await fixture.run({}, controller.signal).catch((error: unknown) => error);
  expect(pickerErrorDetail(error).errors?.[0]?.errors?.[0]?.message).toBe(
    "cancelled before launch",
  );
  expect(fixture.observation.pid).toBeUndefined();
  expect(fixture.observation.processAbsence).toBe("unknown");
  expect(fixture.observation.groupAbsence).toBe("unknown");
  expect(fixture.observation.evidence.every((file) => file.status === "absent")).toBe(true);
  expect((await NodeFSP.lstat(fixture.root())).isDirectory()).toBe(true);
});
it("retains bounded nested aggregate, cause and errno detail", () => {
  const cause = Object.assign(new Error("underlying loader error"), { code: "EIO" });
  const detail = pickerErrorDetail(
    new AggregateError(
      [
        new Error("phase failure", { cause }),
        new AggregateError([new Error("copy failure")], "evidence failure"),
      ],
      "qualification failure",
    ),
  );
  expect(detail).toMatchObject({
    name: "AggregateError",
    errors: [
      { message: "phase failure", cause: { message: "underlying loader error", code: "EIO" } },
      { message: "evidence failure", errors: [{ message: "copy failure" }] },
    ],
  });
  const cycle = new Error("cycle");
  cycle.cause = cycle;
  expect(pickerErrorDetail(cycle).cause?.truncated).toBe(true);
  expect(pickerErrorDetail(new Error("x".repeat(9000))).truncated).toBe(true);
});

it("skips producer lookup only for a complete, current consumer-only PR", async () => {
  const workflow = await NodeFSP.readFile(".github/workflows/artifact-desktop-mac.yml", "utf8");
  const head = "a".repeat(40),
    calls: string[] = [];
  const files = [
    { filename: ".github/workflows/jones-packaged-picker.yml" },
    { filename: "scripts/jones/updates/packaged-picker-smoke.ts" },
  ];
  expect(
    await pickerProducerApplicable({ pr: 1030, head }, workflow, async (endpoint) => {
      calls.push(endpoint);
      return endpoint.includes("/files?")
        ? files
        : { head: { sha: head }, commits: 1, changed_files: files.length };
    }),
  ).toBe(false);
  expect(calls).toEqual([
    "repos/Jones-Systems/Jones-Code/pulls/1030",
    "repos/Jones-Systems/Jones-Code/pulls/1030/files?per_page=100&page=1",
    "repos/Jones-Systems/Jones-Code/pulls/1030",
  ]);
  for (const filename of [
    "apps/desktop/src/jones/updates/pickerVerificationCore.ts",
    "apps/desktop/.hidden/file.ts",
    "apps/desktop/new\nline/file.ts",
    "scripts/jones/updates/desktop-worker-smoke.ts",
    ".github/workflows/artifact-desktop-mac.yml",
  ]) {
    expect(
      await pickerProducerApplicable({ pr: 1030, head }, workflow, async (endpoint) =>
        endpoint.includes("/files?")
          ? [{ filename }]
          : { head: { sha: head }, commits: 1, changed_files: 1 },
      ),
    ).toBe(true);
  }
  expect(
    await pickerProducerApplicable({ pr: 1030, head }, workflow, async (endpoint) =>
      endpoint.includes("/files?")
        ? [{ filename: "docs/moved.ts", previous_filename: "apps/desktop/old.ts" }]
        : { head: { sha: head }, commits: 1, changed_files: 1 },
    ),
  ).toBe(true);
});

it("does not classify incomplete, changing, large or unsupported producer evidence as a skip", async () => {
  const workflow = await NodeFSP.readFile(".github/workflows/artifact-desktop-mac.yml", "utf8");
  const head = "a".repeat(40);
  await expect(
    pickerProducerApplicable({ pr: 1030, head }, workflow, async (endpoint) =>
      endpoint.includes("/files?") ? [] : { head: { sha: head }, commits: 1, changed_files: 1 },
    ),
  ).rejects.toThrow("Incomplete");
  let reads = 0;
  await expect(
    pickerProducerApplicable({ pr: 1030, head }, workflow, async (endpoint) =>
      endpoint.includes("/files?")
        ? [{ filename: "docs/only.md" }]
        : { head: { sha: reads++ === 0 ? head : "b".repeat(40) }, commits: 1, changed_files: 1 },
    ),
  ).rejects.toThrow("Changed");
  expect(
    await pickerProducerApplicable({ pr: 1030, head }, workflow, async () => ({
      head: { sha: head },
      commits: 1,
      changed_files: 301,
    })),
  ).toBe(true);
  await expect(
    pickerProducerApplicable(
      { pr: 1030, head },
      workflow.replace("      - apps/desktop/**", "      - '!apps/desktop/**'"),
      async () => {
        throw new Error("must validate workflow first");
      },
    ),
  ).rejects.toThrow("Unsupported");
});

it("records non-qualification without any producer request or wait", async () => {
  let calls = 0;
  const receipt = await locatePickerProducer(
    { source: "a".repeat(40), tree: "b".repeat(40), event: "pull_request", applicable: false },
    async () => {
      calls++;
      throw new Error("unexpected producer request");
    },
    async () => {
      throw new Error("unexpected producer wait");
    },
  );
  expect(receipt).toMatchObject({
    verdict: "SKIPPED",
    reason: expect.stringContaining("No applicable"),
  });
  expect(calls).toBe(0);
});

it("uses every page before ruling out a producer", async () => {
  const workflow = await NodeFSP.readFile(".github/workflows/artifact-desktop-mac.yml", "utf8");
  const head = "a".repeat(40);
  expect(
    await pickerProducerApplicable({ pr: 1030, head }, workflow, async (endpoint) => {
      if (!endpoint.includes("/files?"))
        return { head: { sha: head }, commits: 1, changed_files: 101 };
      if (endpoint.endsWith("page=1"))
        return Array.from({ length: 100 }, (_, i) => ({ filename: `docs/${i}.md` }));
      return [{ filename: "apps/desktop/src/main.ts" }];
    }),
  ).toBe(true);
});
