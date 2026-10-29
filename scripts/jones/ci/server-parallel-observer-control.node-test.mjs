import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { validateDiagnosticAdmission } from "../serverParallelism/observer-diagnostic.mjs";

const repository = NodePath.resolve(import.meta.dirname, "../../..");
const helperName = "scripts/jones/ci/server-parallel-observer-control.py";
const workflowName = ".github/workflows/server-parallel-qualification.yml";
const source = "8e20d00f7df1c9f86bc48fcc296f0521a9f6e343";
const tree = "39e18d9e7eba8b51695ae053e35259d7300ca00f";
const build = "8cafed1d21a6fa361000b59ce444ab08ecf9af4bfcbdeb46fac58820f0e8b347";
const workspace = "/home/runner/work/Jones-Code/Jones-Code";
const node = "/home/runner/.vite-plus/js_runtime/node/26.12.1/bin/node";
const reportName = "jones-server-parallel-reports-40828828769-1";
const artifactName = "server-parallel-linux-40828828769-1";
const inputs = [
  "package.json",
  "pnpm-lock.yaml",
  "patches/@ff-labs__fff-node@0.11.0.patch",
  "apps/server/src/jones/serverParallelism/diagnosticObservation.ts",
  "apps/server/src/jones/serverParallelism/resourceSamples.ts",
  "apps/server/src/jones/serverParallelism/nativeQualification.ts",
  "apps/server/src/jones/serverParallelism/combinedQualification.ts",
  "apps/server/src/jones/serverCompute/processSlot.ts",
  "scripts/jones/serverParallelism/observer-diagnostic.mjs",
];
const json = (value) => JSON.stringify(value, null, 2) + "\n";
const hash = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");

// The only subprocess is standard-library Python with bytecode writes disabled.
function python(args, options = {}) {
  return NodeChildProcess.spawnSync("python3", ["-B", ...args], {
    encoding: "utf8",
    maxBuffer: 2_000_000,
    timeout: 10_000,
    ...options,
  });
}
function succeeded(result) {
  NodeAssert.equal(result.error, undefined);
  NodeAssert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

async function owned(t) {
  const allocated = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "observer-control-synthetic-"),
  );
  const root = await NodeFSP.realpath(allocated);
  t.after(async () => {
    await NodeFSP.rm(root, { recursive: true });
    await NodeAssert.rejects(NodeFSP.stat(root), { code: "ENOENT" });
  });
  return root;
}

async function fixture(t, mutate = () => {}, afterArchive = () => {}) {
  const root = await owned(t);
  const evidence = NodePath.join(root, "evidence");
  await NodeFSP.mkdir(evidence);
  for (const name of [helperName, workflowName]) {
    const target = NodePath.join(root, "orchestration", name);
    await NodeFSP.mkdir(NodePath.dirname(target), { recursive: true });
    await NodeFSP.copyFile(NodePath.join(repository, name), target);
  }
  for (const name of ["reports", "scratch"]) {
    await NodeFSP.mkdir(NodePath.join(root, name));
    await NodeFSP.writeFile(NodePath.join(root, name, "OWNER"), "40830000000.1\n");
  }
  await NodeFSP.writeFile(NodePath.join(root, "OWNER"), "40830000000.1\n");
  const inputHashes = Object.fromEntries(
    inputs.map((name) => [name, hash("synthetic-input:" + name)]),
  );
  const manifest = {
    schema: "jones.server-parallelism.command/v1",
    runtimeForm: "source",
    file: node,
    args: [
      "--max-old-space-size=256",
      workspace + "/apps/server/src/bin.ts",
      "__jones-server-compute",
    ],
    build,
    identityResolver: workspace + "/apps/server/src/jones/serverCompute/workerCommand.ts",
  };
  const baseline = {
    schema: "jones.server-parallelism.baseline/v1",
    source,
    profile: "laptop",
    count: 2,
    files: 512,
    acquisitions: 500,
    windows: [{}, {}, {}],
    churnWindows: [9, 16].map((workingSet) => ({ workingSet, durationMs: 1, latency: { p99: 1 } })),
    readerCount: 1,
    computePlacement: "main-thread",
    outcome: "passed",
    nativeScopeClosed: true,
    capturedExit: true,
    cleanupProved: true,
    writer: { p95: 1, p99: 1 },
    loop: { p99: 1 },
  };
  const files = {
    OWNER: "40828828769.1\n",
    SOURCE_COMMIT: source + "\n",
    "source-build.patch": "",
    "source-command.json": json(manifest),
    "baseline-laptop.json": json(baseline),
  };
  const artifacts = {};
  for (const kind of ["raw", "trace", "summary", "error", "cleanup"]) {
    const body = json({ synthetic: kind, build, cleanupProved: true });
    files[`observer-full.json.${kind}.json`] = body;
    artifacts[kind] = {
      path: `/home/runner/work/_temp/${reportName}/observer-full.json.${kind}.json`,
      sha256: hash(body),
    };
  }
  const receipt = {
    schema: "jones.server-parallelism.observer-diagnostic/v1",
    source,
    sourceTree: tree,
    build,
    mode: "full",
    runtimeForm: "source",
    profile: "laptop",
    outcome: "recorded",
    eligible: false,
    passed: false,
    complete: true,
    cleanupProved: true,
    aborted: false,
    priorFullHash: null,
    bounds: {
      count: 2,
      windows: 3,
      acquisitions: 500,
      files: 512,
      fullRuns: 1,
      controlRunsMaximum: 1,
    },
    artifacts,
    inputHashes,
    manifestHash: hash(files["source-command.json"]),
    baselineHash: hash(files["baseline-laptop.json"]),
  };
  const state = {
    root,
    evidence,
    files,
    receipt,
    prefix: reportName + "/",
    extra: [],
    generatedManifest: json(manifest),
    context: {
      repository: "Jones-Systems/Jones-Code",
      event: "workflow_dispatch",
      scope: "observer-control",
      platform: "linux",
      source,
      artifactName,
      orchestrationSha: "a".repeat(40),
      workspace,
      runId: 40830000000,
      runAttempt: 1,
      reports: NodePath.join(root, "reports"),
      scratch: NodePath.join(root, "scratch"),
    },
    run: {
      id: 40828828769,
      run_attempt: 1,
      head_sha: source,
      head_commit: { id: source, tree_id: tree },
      workflow_id: 400014551,
      path: workflowName,
      name: "Server parallel qualification",
      event: "workflow_dispatch",
      status: "completed",
      conclusion: "success",
      repository: { id: 1394424242, full_name: "Jones-Systems/Jones-Code" },
      head_repository: { id: 1394424242, full_name: "Jones-Systems/Jones-Code" },
    },
    jobs: {
      total_count: 1,
      jobs: [
        {
          id: 123298044279,
          run_id: 40828828769,
          run_attempt: 1,
          head_sha: source,
          name: "Server parallel (linux)",
          labels: ["ubuntu-24.04"],
          status: "completed",
          conclusion: "success",
          started_at: "2026-10-29T06:22:42Z",
          completed_at: "2026-10-29T06:24:43Z",
        },
      ],
    },
    artifacts: {
      total_count: 1,
      artifacts: [
        {
          id: 12877931910,
          name: artifactName,
          expired: false,
          created_at: "2026-10-29T06:24:40Z",
          expires_at: "9999-11-05T06:24:38Z",
          workflow_run: {
            id: 40828828769,
            repository_id: 1394424242,
            head_repository_id: 1394424242,
            head_sha: source,
          },
        },
      ],
    },
    workflow: { id: 400014551, path: workflowName },
    orchestration: {
      directory: NodePath.join(root, "orchestration"),
      head: "a".repeat(40),
      tree: "b".repeat(40),
      trackedStatus: "",
    },
    beforeSetup: { directory: workspace, head: source, tree, trackedStatus: "" },
    beforeAdmission: {
      directory: workspace,
      head: source,
      tree,
      trackedStatus: "",
      node,
      nodeVersion: "26.12.1",
      inputHashes: { ...inputHashes },
    },
  };
  mutate(state);
  if (!state.omitReceipt) files["observer-full.json"] = json(receipt);
  state.context.receiptSha256 = hash(json(receipt));
  const entries = Object.entries(files).map(([name, body]) => ({
    name: state.prefix + name,
    body,
  }));
  entries.push(...state.extra);
  await NodeFSP.writeFile(NodePath.join(root, "entries.json"), json(entries));
  const zipped = python([
    "-c",
    `
import json, pathlib, stat, struct, sys, zipfile
root = pathlib.Path(sys.argv[1])
with zipfile.ZipFile(root / 'evidence/full.zip', 'x', compression=zipfile.ZIP_DEFLATED) as archive:
    for entry in json.loads((root / 'entries.json').read_text()):
        info = zipfile.ZipInfo(entry['name'])
        info.create_system = 3
        info.external_attr = entry.get('mode', stat.S_IFREG | 0o600) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, entry.get('body', ''))
if sys.argv[2] == 'expanded':
    data = bytearray((root / 'evidence/full.zip').read_bytes())
    offset = data.index(b'PK\\x01\\x02')
    struct.pack_into('<I', data, offset + 24, 180_000_001)
    (root / 'evidence/full.zip').write_bytes(data)
print('{}')
`,
    root,
    state.oversizedExpansion ? "expanded" : "normal",
  ]);
  succeeded(zipped);
  const archive = await NodeFSP.readFile(NodePath.join(evidence, "full.zip"));
  state.context.archiveSha256 = hash(archive);
  state.artifacts.artifacts[0].digest = "sha256:" + hash(archive);
  state.artifacts.artifacts[0].size_in_bytes = archive.length;
  afterArchive(state);
  for (const [name, value] of Object.entries({
    context: state.context,
    run: state.run,
    jobs: state.jobs,
    artifacts: state.artifacts,
    workflow: state.workflow,
    "orchestration-identity": state.orchestration,
    "before-setup": state.beforeSetup,
    "before-admission": state.beforeAdmission,
  }))
    await NodeFSP.writeFile(NodePath.join(evidence, name + ".json"), json(value));
  await NodeFSP.writeFile(
    NodePath.join(evidence, "regenerated-source-command.json"),
    state.generatedManifest,
  );
  return {
    ...state,
    invoke: (action) => python([NodePath.join(root, "orchestration", helperName), action, root]),
  };
}

// Stop at each production boundary. The final command is captured, never executed.
function dispatch(fixture) {
  const commands = [];
  for (const action of ["metadata", "extract", "preflight", "gate"]) {
    const result = fixture.invoke(action);
    if (result.error || result.status !== 0)
      return { commands, failedAt: action, error: result.stderr };
    const admitted = JSON.parse(result.stdout);
    if (action === "gate") commands.push(admitted.command);
  }
  return { commands };
}

NodeTest.test(
  "routes the real nested layout with old measured identity and separate orchestration",
  async (t) => {
    const f = await fixture(t);
    const result = dispatch(f);
    NodeAssert.equal(result.failedAt, undefined, result.error);
    NodeAssert.equal(result.commands.length, 1);
    const prior = NodePath.join(f.root, "observer-prior", reportName);
    const route = JSON.parse(
      await NodeFSP.readFile(NodePath.join(f.evidence, "control-route.json")),
    );
    NodeAssert.equal(route.priorDirectory, prior);
    NodeAssert.equal(route.measured.source, source);
    NodeAssert.equal(route.orchestration.head, "a".repeat(40));
    NodeAssert.notEqual(route.orchestration.head, route.measured.source);
    NodeAssert.equal(route.receiptSha256, hash(json(f.receipt)));
    NodeAssert.equal(route.artifactId, 12877931910);
    for (const [name, body] of Object.entries(f.files)) {
      NodeAssert.deepEqual(await NodeFSP.readFile(NodePath.join(prior, name)), Buffer.from(body));
    }
    await NodeAssert.rejects(
      NodeFSP.stat(NodePath.join(f.root, "observer-prior/observer-full.json")),
      { code: "ENOENT" },
    );
    NodeAssert.deepEqual(result.commands, [
      {
        cwd: workspace,
        file: node,
        args: [
          workspace + "/scripts/jones/serverParallelism/ci-run.mjs",
          f.context.reports,
          NodePath.join(f.context.scratch, "installed"),
          "observer-control",
          prior,
        ],
      },
    ]);
    const admission = {
      manifest: JSON.parse(f.generatedManifest),
      baseline: JSON.parse(f.files["baseline-laptop.json"]),
      mode: "cpu-stat-only",
      source,
      sourceTree: tree,
      build,
      entry: workspace + "/apps/server/src/bin.ts",
      executable: node,
      platform: "linux",
      parallelism: 4,
      prior: f.receipt,
      baselineHash: hash(f.files["baseline-laptop.json"]),
    };
    NodeAssert.doesNotThrow(() => validateDiagnosticAdmission(admission));
    for (const changed of [
      { source: f.context.orchestrationSha },
      { sourceTree: "0".repeat(40) },
      { build: "0".repeat(64) },
      { baselineHash: "0".repeat(64) },
    ]) {
      NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, ...changed }));
    }
    NodeAssert.equal(
      result.commands.filter(
        (command) =>
          command.args.includes("full") || command.args.some((arg) => arg.endsWith("baseline.mjs")),
      ).length,
      0,
    );
    // Neither an unknown command effect nor a successful command permits a second claim.
    NodeAssert.notEqual(f.invoke("gate").status, 0);
    NodeAssert.equal(result.commands.length, 1);
  },
);

NodeTest.test("archive and receipt failures close admission before any command", async (t) => {
  const cases = {
    "flat-only ZIP": (s) => {
      s.prefix = "";
    },
    "wrong run root": (s) => {
      s.prefix = "jones-server-parallel-reports-40828828770-1/";
    },
    "wrong attempt root": (s) => {
      s.prefix = "jones-server-parallel-reports-40828828769-2/";
    },
    "missing receipt": (s) => {
      s.omitReceipt = true;
    },
    "duplicate receipt": (s) => {
      s.extra.push({ name: reportName + "/observer-full.json", body: "{}" });
    },
    traversal: (s) => {
      s.extra.push({ name: reportName + "/../../escaped", body: "escape" });
    },
    "absolute filename": (s) => {
      s.extra.push({ name: "/escaped", body: "escape" });
    },
    "backslash filename": (s) => {
      s.extra.push({ name: reportName + "/..\\escaped", body: "escape" });
    },
    symlink: (s) => {
      s.extra.push({ name: reportName + "/link", body: "../../escaped", mode: 0o120777 });
    },
    "special file": (s) => {
      s.extra.push({ name: reportName + "/fifo", body: "", mode: 0o010600 });
    },
    "file-directory overlap": (s) => {
      s.extra.push(
        { name: reportName + "/x", body: "file" },
        { name: reportName + "/x/y", body: "child" },
      );
    },
    "entry count bound": (s) => {
      s.extra.push(
        ...Array.from({ length: 256 }, (_, i) => ({ name: reportName + "/extra-" + i })),
      );
    },
    "expansion bound": (s) => {
      s.oversizedExpansion = true;
    },
    "changed OWNER": (s) => {
      s.files.OWNER = "40828828769.2\n";
    },
    "changed source evidence": (s) => {
      s.files.SOURCE_COMMIT = "a".repeat(40) + "\n";
    },
    "nonempty source patch": (s) => {
      s.files["source-build.patch"] = "changed";
    },
    "missing sidecar": (s) => {
      delete s.files["observer-full.json.trace.json"];
    },
    "corrupted sidecar": (s) => {
      s.files["observer-full.json.raw.json"] += "changed";
    },
    "changed baseline": (s) => {
      s.files["baseline-laptop.json"] += "\n";
    },
    "changed original manifest": (s) => {
      s.files["source-command.json"] += "\n";
    },
    "incomplete full": (s) => {
      s.receipt.complete = false;
    },
    "eligible full": (s) => {
      s.receipt.eligible = true;
    },
    "non-full prior": (s) => {
      s.receipt.mode = "cpu-stat-only";
    },
    "unproved cleanup": (s) => {
      s.receipt.cleanupProved = false;
    },
    "receipt source mismatch": (s) => {
      s.receipt.source = "a".repeat(40);
    },
    "receipt build mismatch": (s) => {
      s.receipt.build = "a".repeat(64);
    },
  };
  for (const [name, mutate] of Object.entries(cases))
    await t.test(name, async (t) => {
      const f = await fixture(t, mutate);
      const result = dispatch(f);
      NodeAssert.ok(result.failedAt, name);
      NodeAssert.deepEqual(result.commands, []);
      NodeAssert.notEqual(f.invoke("gate").status, 0);
      await NodeAssert.rejects(NodeFSP.stat(NodePath.join(f.root, "escaped")), { code: "ENOENT" });
    });
  for (const collision of ["directory", "symlink"])
    await t.test("preexisting " + collision, async (t) => {
      const f = await fixture(t);
      const target = NodePath.join(f.root, "observer-prior");
      if (collision === "directory") await NodeFSP.mkdir(target);
      else await NodeFSP.symlink(f.evidence, target);
      NodeAssert.deepEqual(dispatch(f).commands, []);
      NodeAssert.notEqual(f.invoke("extract").status, 0);
    });
});

NodeTest.test("joins exact metadata and refuses malformed explicit pins", async (t) => {
  const cases = {
    "source pin mismatch": (s) => {
      s.context.source = "a".repeat(40);
    },
    "orchestration used as measured source": (s) => {
      s.context.source = s.context.orchestrationSha;
    },
    "wrong repository": (s) => {
      s.run.repository.id++;
    },
    "wrong head repository": (s) => {
      s.run.head_repository.full_name = "other/Jones-Code";
    },
    "wrong workflow": (s) => {
      s.workflow.path = ".github/workflows/ci.yml";
    },
    "wrong run": (s) => {
      s.run.id++;
    },
    "wrong attempt": (s) => {
      s.run.run_attempt++;
    },
    "run source mismatch": (s) => {
      s.run.head_sha = "a".repeat(40);
    },
    "job source mismatch": (s) => {
      s.jobs.jobs[0].head_sha = "a".repeat(40);
    },
    "wrong job": (s) => {
      s.jobs.jobs[0].id++;
    },
    "failed job": (s) => {
      s.jobs.jobs[0].conclusion = "failure";
    },
    "failed workflow": (s) => {
      s.run.conclusion = "failure";
    },
    "artifact source mismatch": (s) => {
      s.artifacts.artifacts[0].workflow_run.head_sha = "a".repeat(40);
    },
    "artifact run mismatch": (s) => {
      s.artifacts.artifacts[0].workflow_run.id++;
    },
    "wrong artifact ID": (s) => {
      s.artifacts.artifacts[0].id++;
    },
    "wrong artifact name": (s) => {
      s.artifacts.artifacts[0].name += "-wrong";
    },
    "duplicate named artifact": (s) => {
      s.artifacts.artifacts.push({ ...s.artifacts.artifacts[0] });
      s.artifacts.total_count++;
    },
    "expired artifact": (s) => {
      s.artifacts.artifacts[0].expired = true;
    },
    "retention deadline passed": (s) => {
      s.artifacts.artifacts[0].expires_at = "2000-01-01T00:00:00Z";
    },
    "artifact outside job attempt": (s) => {
      s.artifacts.artifacts[0].created_at = "2026-10-29T06:24:44Z";
    },
    "partial artifact listing": (s) => {
      s.artifacts.total_count++;
    },
    "changed archive pin": (s) => {
      s.context.archiveSha256 = "0".repeat(64);
    },
    "changed receipt pin": (s) => {
      s.context.receiptSha256 = "0".repeat(64);
    },
    "malformed digest": (s) => {
      s.context.archiveSha256 = "A".repeat(64);
    },
    "metadata digest mismatch": (s) => {
      s.artifacts.artifacts[0].digest = "sha256:" + "0".repeat(64);
    },
    "archive size bound": (s) => {
      s.artifacts.artifacts[0].size_in_bytes = 20_000_001;
    },
    "control/full run collision": (s) => {
      s.context.runId = s.run.id;
    },
    "automatic retry": (s) => {
      s.context.runAttempt = 2;
    },
    "wrong orchestration checkout": (s) => {
      s.orchestration.head = "b".repeat(40);
    },
  };
  for (const [name, after] of Object.entries(cases))
    await t.test(name, async (t) => {
      const f = await fixture(t, undefined, after);
      const result = dispatch(f);
      NodeAssert.ok(result.failedAt, name);
      NodeAssert.deepEqual(result.commands, []);
    });
  await t.test("changed archive bytes", async (t) => {
    const f = await fixture(t);
    await NodeFSP.appendFile(NodePath.join(f.evidence, "full.zip"), "corrupt");
    const result = dispatch(f);
    NodeAssert.equal(result.failedAt, "extract");
    NodeAssert.deepEqual(result.commands, []);
  });
});

NodeTest.test(
  "rechecks measured checkout, inputs and regenerated manifest before the command claim",
  async (t) => {
    const cases = {
      "dirty before setup": (s) => {
        s.beforeSetup.trackedStatus = " M package.json";
      },
      "dirty before admission": (s) => {
        s.beforeAdmission.trackedStatus = " M package.json";
      },
      "wrong tree": (s) => {
        s.beforeAdmission.tree = "a".repeat(40);
      },
      "wrong checkout head": (s) => {
        s.beforeAdmission.head = s.context.orchestrationSha;
      },
      "relocated workload": (s) => {
        s.beforeAdmission.directory += "-other";
      },
      "relocated context": (s) => {
        s.context.workspace += "-other";
      },
      "different Node path": (s) => {
        s.beforeAdmission.node = "/usr/bin/node";
      },
      "different Node version": (s) => {
        s.beforeAdmission.nodeVersion = "26.12.0";
      },
      "changed input bytes": (s) => {
        s.beforeAdmission.inputHashes[inputs[0]] = "0".repeat(64);
      },
      "wrong regenerated bytes": (s) => {
        s.generatedManifest += "\n";
      },
      "wrong generated build": (s) => {
        s.generatedManifest = json({ ...JSON.parse(s.generatedManifest), build: "0".repeat(64) });
      },
    };
    for (const [name, mutate] of Object.entries(cases))
      await t.test(name, async (t) => {
        const f = await fixture(t, mutate);
        const result = dispatch(f);
        NodeAssert.ok(result.failedAt, name);
        NodeAssert.deepEqual(result.commands, []);
      });
    await t.test("evidence changed after successful preflight", async (t) => {
      const f = await fixture(t);
      for (const action of ["metadata", "extract", "preflight"]) succeeded(f.invoke(action));
      await NodeFSP.appendFile(
        NodePath.join(f.root, "observer-prior", reportName, "observer-full.json.cleanup.json"),
        "changed",
      );
      NodeAssert.notEqual(f.invoke("gate").status, 0);
      await NodeAssert.rejects(
        NodeFSP.stat(NodePath.join(f.evidence, "control-command-started.json")),
        { code: "ENOENT" },
      );
    });
  },
);

NodeTest.test(
  "workflow selects control pins only and preserves ordinary PR/dispatch source selection",
  async (t) => {
    const root = await owned(t);
    const workflow = await NodeFSP.readFile(NodePath.join(repository, workflowName), "utf8");
    const match = workflow.match(/<<'PY_SELECT'\n([\s\S]*?)\n          PY_SELECT/);
    NodeAssert.ok(match);
    const code = match[1]
      .split("\n")
      .map((line) => line.slice(10))
      .join("\n");
    const env = {
      ...process.env,
      JONES_SP_SCOPE: "observer-control",
      GITHUB_EVENT_NAME: "workflow_dispatch",
      GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
      GITHUB_RUN_ATTEMPT: "1",
      QUALIFICATION_OS: "linux",
      DEFAULT_SOURCE: "a".repeat(40),
      ORCHESTRATION_SHA: "b".repeat(40),
      CONTROL_SOURCE: source,
      FULL_ARTIFACT: artifactName,
      FULL_ARCHIVE_SHA256: "c".repeat(64),
      FULL_RECEIPT_SHA256: "d".repeat(64),
    };
    let sequence = 0;
    async function select(overrides) {
      const output = NodePath.join(root, "selection-" + sequence++);
      const result = python(["-c", code], { env: { ...env, ...overrides, GITHUB_OUTPUT: output } });
      return {
        result,
        selected: result.status === 0 ? await NodeFSP.readFile(output, "utf8") : undefined,
      };
    }
    NodeAssert.equal((await select({})).selected, `commit=${source}\n`);
    for (const scope of ["source-evidence", "full", "observer-diagnostic"]) {
      for (const event of ["pull_request", "workflow_dispatch"]) {
        const ordinary = {
          JONES_SP_SCOPE: scope,
          GITHUB_EVENT_NAME: event,
          CONTROL_SOURCE: "",
          FULL_ARTIFACT: "",
          FULL_ARCHIVE_SHA256: "",
          FULL_RECEIPT_SHA256: "",
        };
        NodeAssert.equal((await select(ordinary)).selected, `commit=${env.DEFAULT_SOURCE}\n`);
        for (const key of [
          "CONTROL_SOURCE",
          "FULL_ARTIFACT",
          "FULL_ARCHIVE_SHA256",
          "FULL_RECEIPT_SHA256",
        ]) {
          NodeAssert.notEqual((await select({ ...ordinary, [key]: env[key] })).result.status, 0);
        }
      }
    }
    for (const rejected of [
      { CONTROL_SOURCE: "" },
      { GITHUB_EVENT_NAME: "pull_request" },
      { QUALIFICATION_OS: "macos" },
      { FULL_ARCHIVE_SHA256: "ABC" },
      { GITHUB_RUN_ATTEMPT: "2" },
    ]) {
      NodeAssert.notEqual((await select(rejected)).result.status, 0);
    }
    NodeAssert.match(
      workflow,
      /DEFAULT_SOURCE: \$\{\{ github\.event\.pull_request\.head\.sha \|\| github\.sha \}\}/,
    );
    NodeAssert.match(workflow, /SOURCE_COMMIT: \$\{\{ steps\.source\.outputs\.commit \}\}/);
    NodeAssert.match(workflow, /steps\.control_preflight\.outcome == 'success'/);
    NodeAssert.match(
      workflow,
      /control\.py" gate "\$CONTROL_ROOT"\n\s+\/home\/runner\/\.vite-plus\/js_runtime\/node\/26\.12\.1\/bin\/node "\$GITHUB_WORKSPACE\/scripts\/jones\/serverParallelism\/ci-run\.mjs"/,
    );
    NodeAssert.doesNotMatch(workflow, /gh run download|observer-prior\/observer-full\.json/);
    NodeAssert.match(workflow, /if: env\.JONES_SP_SCOPE != 'observer-control'\n\s+run: >-/);
    NodeAssert.match(
      workflow,
      /node scripts\/jones\/serverParallelism\/source-command\.mjs.*regenerated-source-command\.json/,
    );
    NodeAssert.doesNotMatch(workflow, /cp .*source-command\.json/);
    const ci = await NodeFSP.readFile(
      NodePath.join(repository, ".github/workflows/ci.yml"),
      "utf8",
    );
    NodeAssert.match(ci, /node --test --test-concurrency=1 scripts\/jones\/ci\/\*\.node-test\.\*/);
  },
);
