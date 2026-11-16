import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import * as NodeVM from "node:vm";
import * as NodeZlib from "node:zlib";

const workflow = await NodeFSP.readFile(
  new URL("../../../.github/workflows/artifact-desktop-mac.yml", import.meta.url),
  "utf8",
);
const repository = "Jones-Systems/Jones-Code";
const root = `repos/${repository}`;
const source = "b".repeat(40),
  installed = "a".repeat(40),
  tree = "c".repeat(40);
const workflowPath = ".github/workflows/artifact-desktop-mac.yml";
const version = "0.1.2-preview.20261002.101.2";
const payloadName = `T3-Code-${version}-arm64.dmg`;
const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const expression = (text, github) => NodeVM.runInNewContext(text, { github }, { timeout: 1000 });
const context = (event_name, ref = "refs/heads/main", pr) => ({
  repository,
  event_name,
  ref,
  event: { pull_request: pr ?? { head: { repo: {} } } },
});

NodeTest.test("every canonical main push publishes a genuine compatibility artifact", () => {
  const triggers = workflow.split("\non:\n")[1].split("\npermissions:")[0];
  const push = triggers.match(/\n  push:\n([\s\S]*?)(?=\n  \w|$)/)?.[1];
  NodeAssert.ok(push, "push-only installed clients need ongoing main-push publication");
  NodeAssert.match(push, /^    branches: \[main\]\s*$/);
  NodeAssert.doesNotMatch(
    push,
    /paths(?:-ignore)?:/,
    "all qualifying main sources must be discoverable",
  );
  for (const route of ["schedule", "workflow_dispatch", "pull_request"])
    NodeAssert.match(triggers, new RegExp(`\\n  ${route}:`));
  const condition = workflow.match(/\n    if: >-\n([\s\S]*?)\n    runs-on:/)[1].trim();
  NodeAssert.equal(expression(condition, context("push")), true);
  NodeAssert.equal(expression(condition, context("push", "refs/heads/other")), false);
  for (const event of ["schedule", "workflow_dispatch"])
    NodeAssert.equal(expression(condition, context(event)), true);
  NodeAssert.equal(
    expression(
      condition,
      context("pull_request", "refs/pull/12/merge", {
        number: 12,
        head: { repo: { full_name: repository } },
      }),
    ),
    true,
  );
  NodeAssert.equal(
    expression(
      condition,
      context("pull_request", "refs/pull/12/merge", {
        number: 12,
        head: { repo: { full_name: "other/fork" } },
      }),
    ),
    false,
  );
  NodeAssert.equal(expression(condition, { ...context("push"), repository: "other/fork" }), false);
  NodeAssert.match(workflow, /SOURCE_COMMIT: \$\{\{ github.sha \}\}/);
  NodeAssert.match(workflow, /event: process.env.GITHUB_EVENT_NAME, ref: process.env.GITHUB_REF/);
  NodeAssert.match(
    workflow,
    /name: desktop-mac-arm64-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}--\$\{\{ steps.version.outputs.value \}\}/,
  );
  const upload = workflow
    .split("      - name: Upload DMG and checksum\n")[1]
    .split("\n      - name:")[0];
  NodeAssert.match(upload, /retention-days: 7/);
});

NodeTest.test("schedule and manual builds cannot cancel the sole compatibility push", () => {
  const group = workflow.match(/\n  group: (.*)\n/)[1];
  const evaluate = (github) =>
    group.replace(/\$\{\{(.*?)\}\}/g, (_, text) => expression(text, github));
  const push = evaluate(context("push"));
  NodeAssert.notEqual(push, evaluate(context("schedule")));
  NodeAssert.notEqual(push, evaluate(context("workflow_dispatch")));
  NodeAssert.equal(push, evaluate(context("push")), "new main pushes may supersede older pushes");
  NodeAssert.match(workflow, /cancel-in-progress: true/);
});

// Git-object snapshots keep CI independent of shallow history and future consumer changes.
// Only module specifiers change for loading; qualification and staging execute verbatim.
const frozen = JSON.parse(
  await NodeFSP.readFile(new URL("./fixtures/pre590/consumer.json", import.meta.url)),
);
NodeAssert.equal(frozen.revision, "e85e058eb7435cab22ba4486c263067755dd098f");
NodeAssert.equal(frozen.repository, repository);
NodeAssert.equal(
  frozen.files["packages/shared/src/jones/jonesActions.ts"].blob,
  "f380879eaaa50a1bf9939ff546a0ebfbbcb9cc9b",
);
const require = NodeModule.createRequire(new URL("../../package.json", import.meta.url));
const modules = new Map();
function frozenModule(file) {
  if (modules.has(file)) return modules.get(file);
  const entry = frozen.files[file];
  NodeAssert.equal(hash(entry.source), entry.sha256, file);
  NodeAssert.equal(
    NodeCrypto.createHash("sha1")
      .update(`blob ${Buffer.byteLength(entry.source)}\0`)
      .update(entry.source)
      .digest("hex"),
    entry.blob,
    file,
  );
  const js = NodeModule.stripTypeScriptTypes(entry.source).replace(
    /(from\s+)(["'])([^"']+)\2/g,
    (_, prefix, quote, specifier) => {
      const resolved = specifier.startsWith(".")
        ? frozenModule(NodePath.posix.join(NodePath.posix.dirname(file), specifier))
        : specifier === "@t3tools/contracts/jones/fleet-updates"
          ? frozenModule("packages/contracts/src/jones/fleetUpdates/source.ts")
          : specifier.startsWith("node:")
            ? specifier
            : NodeURL.pathToFileURL(require.resolve(specifier)).href;
      return `${prefix}${quote}${resolved}${quote}`;
    },
  );
  const url = `data:text/javascript;base64,${Buffer.from(js).toString("base64")}`;
  modules.set(file, url);
  return url;
}
const { JonesActionsClient } = await import(
  frozenModule("packages/shared/src/jones/jonesActions.ts")
);

// Real stored ZIP records exercise the old authenticated envelope and receipt decoder.
function zip(entries) {
  const locals = [],
    centrals = [];
  let offset = 0;
  for (const [filename, text] of entries) {
    const name = Buffer.from(filename),
      data = Buffer.from(text);
    const local = Buffer.alloc(30),
      central = Buffer.alloc(46);
    local.writeUInt32LE(0x04034b50);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(NodeZlib.crc32(data), 14);
    local.writeUInt32LE(data.length, 18);
    local.writeUInt32LE(data.length, 22);
    local.writeUInt16LE(name.length, 26);
    central.writeUInt32LE(0x02014b50);
    central.writeUInt16LE(0x0314, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(NodeZlib.crc32(data), 16);
    central.writeUInt32LE(data.length, 20);
    central.writeUInt32LE(data.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE((0o100600 << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    locals.push(local, name, data);
    centrals.push(central, name);
    offset += local.length + name.length + data.length;
  }
  const directory = Buffer.concat(centrals),
    end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(directory.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, directory, end]);
}

function fixture(options = {}) {
  const payload = "synthetic DMG bytes; never mounted or executed";
  const receipt = {
    schema: 1,
    repository,
    source,
    tree,
    workflow: workflowPath,
    event: "push",
    ref: "refs/heads/main",
    runId: "101",
    runAttempt: "2",
    version,
    platform: "darwin",
    architecture: "arm64",
    artifact: payloadName,
    sha256: hash(payload),
    ...options.receipt,
  };
  const envelope = zip([
    ["ARTIFACT.json", JSON.stringify(receipt)],
    ["SOURCE_COMMIT", source],
    [`${payloadName}.sha256`, `${hash(payload)}  ${payloadName}\n`],
    [payloadName, payload],
  ]);
  const run = {
    path: workflowPath,
    id: 101,
    workflow_id: 5,
    run_attempt: 2,
    head_sha: source,
    head_branch: "main",
    event: "push",
    status: "completed",
    conclusion: "success",
    repository: { id: 77, full_name: repository },
    head_repository: { id: 77, full_name: repository },
  };
  const ci = { ...run, id: 99, path: ".github/workflows/ci.yml", ...options.ci };
  const artifact = {
    id: 301,
    name: `desktop-mac-arm64-101-2--${version}`,
    size_in_bytes: envelope.length,
    expired: false,
    expires_at: "2026-10-09T12:00:00Z",
    digest: `sha256:${hash(envelope)}`,
    workflow_run: {
      id: 101,
      repository_id: 77,
      head_repository_id: 77,
      head_branch: "main",
      head_sha: source,
    },
    ...options.artifact,
  };
  const calls = [];
  const client = new JonesActionsClient({
    now: () => Date.parse("2026-10-02T12:00:00Z"),
    transport: {
      async api(endpoint) {
        calls.push(endpoint);
        if (endpoint === `${root}/commits/main`) return { sha: source };
        if (endpoint === `${root}/git/commits/${source}`)
          return { sha: source, tree: { sha: tree } };
        if (endpoint === `${root}/compare/${installed}...${source}?per_page=1`)
          return { status: "ahead", merge_base_commit: { sha: installed } };
        if (endpoint.startsWith(`${root}/actions/workflows/artifact-desktop-mac.yml/runs?`)) {
          NodeAssert.equal(new URLSearchParams(endpoint.split("?")[1]).get("event"), "push");
          return { workflow_runs: [{ ...run, ...options.listed }] };
        }
        if (endpoint.startsWith(`${root}/actions/workflows/ci.yml/runs?`))
          return { workflow_runs: [ci] };
        if (endpoint === `${root}/actions/runs/101`) return { ...run, ...options.live };
        if (endpoint === `${root}/actions/runs/99`) return ci;
        if (endpoint === `${root}/actions/runs/101/artifacts?per_page=100&page=1`)
          return { artifacts: [artifact] };
        if (endpoint === `${root}/actions/artifacts/301`) return artifact;
        throw new Error(`Unexpected synthetic endpoint: ${endpoint}`);
      },
      async download(id, destination) {
        NodeAssert.equal(id, 301);
        await NodeFSP.writeFile(destination, options.corrupt ? Buffer.from("corrupt") : envelope, {
          flag: "wx",
        });
      },
    },
  });
  return { client, calls };
}
const check = (client) =>
  client.check({ installedSource: installed, platform: "darwin", architecture: "arm64" });
async function withCache(run) {
  const cache = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "desktop-push-compatibility-"),
  );
  try {
    await run(cache);
  } finally {
    await NodeFSP.rm(cache, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(cache), { code: "ENOENT" });
  }
}

NodeTest.test(
  "the frozen pre-590 consumer discovers and stages a genuine push with exact-source CI",
  async () => {
    const { client, calls } = fixture();
    const result = await check(client);
    NodeAssert.equal(result.state, "available");
    NodeAssert.equal(result.candidate.source, source);
    NodeAssert.equal(result.candidate.ciRunId, 99);
    await withCache(async (cache) => {
      const staged = await client.stage(result.candidate, cache);
      NodeAssert.equal(staged.candidate.source, source);
    });
    NodeAssert.ok(
      calls.some((call) => call.includes(`ci.yml/runs?branch=main&event=push&head_sha=${source}`)),
    );
  },
);

NodeTest.test(
  "the frozen consumer rejects scheduled discovery, changed live events, and mismatched CI/source",
  async () => {
    const blocked = { state: "blocked", reason: "unavailable", retryAfterMs: 240000 };
    const building = { state: "building", retryAfterMs: 240000 };
    for (const [options, expected] of [
      [{ listed: { event: "schedule" } }, blocked],
      [{ live: { event: "schedule" } }, blocked],
      [{ ci: { event: "schedule" } }, building],
      [{ ci: { conclusion: "failure" } }, blocked],
      [{ ci: { head_sha: "d".repeat(40) } }, building],
      [
        {
          artifact: {
            workflow_run: {
              id: 101,
              repository_id: 77,
              head_repository_id: 77,
              head_branch: "main",
              head_sha: installed,
            },
          },
        },
        blocked,
      ],
    ]) {
      const result = await check(fixture(options).client);
      NodeAssert.deepEqual(result, expected, JSON.stringify(options));
    }
  },
);

NodeTest.test(
  "the frozen consumer rejects relabelled receipts, source mismatches, and unauthenticated bytes",
  async () => {
    for (const options of [
      { receipt: { event: "schedule" } },
      { receipt: { event: "workflow_dispatch" } },
      { receipt: { source: installed } },
      { corrupt: true },
    ]) {
      const { client } = fixture(options);
      const result = await check(client);
      NodeAssert.equal(result.state, "available");
      await withCache(async (cache) => {
        await NodeAssert.rejects(client.stage(result.candidate, cache), {
          reason: options.corrupt ? "integrity" : "unqualified",
        });
      });
    }
  },
);
