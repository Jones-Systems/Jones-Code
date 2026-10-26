const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { dirname, join } = require("node:path");
const { test } = require("node:test");
const {
  MANIFEST,
  parseManifest,
  compareManifests,
  qualifyRevisions,
  runForEvent,
} = require("./jones-migration-order.cjs");

const identities = [
  [1, "Original"],
  [112, "BeforeGap"],
  [114, "AfterGap"],
  [124, "Continuations"],
  [129, "Failover"],
  [130, "Titles"],
];
function source(entries = identities) {
  return (
    entries
      .map(([id]) => `import Migration${id} from "./Migrations/${id}_Fixture.ts";`)
      .join("\n") +
    `\nexport const jonesMigrationEntries = [\n${entries.map(([id, name]) => `[${id}, ${JSON.stringify(name)}, Migration${id}],`).join("\n")}\n] as const;\n`
  );
}

function compare(candidate, baseline = source()) {
  return compareManifests(parseManifest(baseline), parseManifest(candidate));
}

function withRepository(run) {
  const cwd = mkdtempSync(join(tmpdir(), "jones-migration-order-"));
  const cleanup = () => rmSync(cwd, { recursive: true, force: true });
  const cancelled = () => {
    cleanup();
    process.exit(1);
  };
  process.once("SIGINT", cancelled);
  process.once("SIGTERM", cancelled);
  const git = (...args) =>
    execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
    }).trim();
  try {
    git("init", "--initial-branch=main");
    git("config", "user.name", "Synthetic migration test");
    git("config", "user.email", "synthetic@example.invalid");
    const commit = (text) => {
      mkdirSync(dirname(join(cwd, MANIFEST)), { recursive: true });
      writeFileSync(join(cwd, MANIFEST), text);
      git("add", "--", MANIFEST);
      git("commit", "--quiet", "--allow-empty", "-m", "Synthetic manifest");
      return git("rev-parse", "HEAD");
    };
    return run({ cwd, git, commit });
  } finally {
    process.removeListener("SIGINT", cancelled);
    process.removeListener("SIGTERM", cancelled);
    cleanup();
  }
}

test("literal manifests preserve gaps and only append above the baseline maximum", () => {
  assert.deepEqual(compare(source()), []);
  assert.deepEqual(compare(source([...identities, [131, "Next"]])), [[131, "Next"]]);
  for (const id of [113, 125, 126, 127, 128]) {
    assert.throws(
      () => compare(source([...identities, [id, "Late"]].sort(([a], [b]) => a - b))),
      /above baseline maximum 130/,
    );
  }
  for (const entries of [
    identities.filter(([id]) => id !== 130),
    identities.map(([id, name]) => [id, id === 130 ? "Renamed" : name]),
    identities.map(([id, name]) => [id === 130 ? 131 : id, name]),
  ])
    assert.throws(() => compare(source(entries)), /deleted, renamed or renumbered/);
});

test("unsupported, dynamic, duplicate and malformed manifests fail closed without executing source", () => {
  const valid = source();
  for (const text of [
    "",
    valid.replace("export const", "export let"),
    valid.replace(" as const", ""),
    valid.replace(" as const", " as unknown"),
    valid.replace('[1, "Original", Migration1]', '[1 + 0, "Original", Migration1]'),
    valid.replace('[1, "Original", Migration1]', "[1, name(), Migration1]"),
    valid.replace("Migration1],", "doNotExecute()],"),
    valid.replace("[1,", "[0,"),
    valid.replace("[1,", "[-1,"),
    valid.replace("[1,", "[7,"),
    valid.replace("[1,", "[9007199254740992,"),
    valid.replace("[1,", "[...other, 1,"),
    valid.replace("[1,", "[,1,"),
    valid.replace("[1,", "[1,,"),
    valid.replace("[1,", "[1;"),
    source([...identities, [130, "DuplicateId"]]),
    source([...identities, [131, "Titles"]]),
    valid + "\njonesMigrationEntries.push([125, 'Late', effect]);",
    valid + "\nexport const jonesMigrationEntries = [] as const;",
    valid.replace("[1,", "[0x1,"),
  ])
    assert.throws(() => parseManifest(text), /Unsupported or malformed/);
  assert.throws(() => compare(source(), "bad baseline"), /Unsupported or malformed/);
});

test("canonical main advances invalidate a formerly valid allocation and cannot hide deletion", () =>
  withRepository(({ cwd, git, commit }) => {
    const base = commit(source());
    // Rewrite this exact canonical URL only inside the private fixture. This
    // exercises the real fetch path without network or a shared tracking ref.
    git("config", `url.${cwd}.insteadOf`, "https://github.com/Jones-Systems/Jones-Code.git");
    git("update-ref", "refs/remotes/origin/main", base);
    git("checkout", "-b", "allocation");
    const candidate = commit(source([...identities, [131, "BranchNext"]]));
    git("checkout", "main");
    const logs = [];
    const env = {
      GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
      GITHUB_SHA: candidate,
      GITHUB_EVENT_NAME: "pull_request",
    };
    const event = {
      pull_request: {
        base: { sha: base, ref: "main", repo: { full_name: "Jones-Systems/Jones-Code" } },
      },
    };
    git("checkout", "allocation");
    const run = () => runForEvent({ cwd, env, event, log: (line) => logs.push(line) });
    assert.deepEqual(run().added, [[131, "BranchNext"]]);
    git("checkout", "main");
    const freshMain = commit(source([...identities, [131, "MainNext"]]));
    git("checkout", "allocation");
    assert.throws(run, /131_MainNext was deleted, renamed or renumbered/);
    assert.ok(logs.at(-1).includes(`baseline=${freshMain} candidate=${candidate}`));

    git("checkout", "-b", "unrelated", base);
    const unrelated = commit(source());
    assert.equal(
      runForEvent({
        cwd,
        env: { ...env, GITHUB_SHA: unrelated },
        event,
        fetchMain: () => freshMain,
        log: () => {},
      }).selection,
      "unchanged",
    );
    const deleted = commit(source(identities.slice(0, -1)));
    assert.throws(
      () =>
        qualifyRevisions({
          cwd,
          baseline: freshMain,
          candidate: deleted,
          allowUnchanged: true,
          log: () => {},
        }),
      /deleted, renamed or renumbered/,
    );
    assert.throws(
      () =>
        runForEvent({
          cwd,
          env: { ...env, GITHUB_SHA: deleted },
          event,
          fetchMain: () => {
            throw new Error("Canonical main unavailable");
          },
          log: () => {},
        }),
      /Canonical main unavailable/,
    );
  }));

test("main pushes compare before to after, including a lower insertion and unavailable evidence", () =>
  withRepository(({ cwd, commit, git }) => {
    const before = commit(source());
    const after = commit(source([...identities, [131, "Next"]]));
    const env = {
      GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
      GITHUB_SHA: after,
      GITHUB_EVENT_NAME: "push",
      GITHUB_REF: "refs/heads/main",
    };
    const event = { before, after, ref: env.GITHUB_REF };
    const run = (overrides = {}) =>
      runForEvent({
        cwd,
        env,
        event: { ...event, ...overrides },
        fetchMain: () => {
          throw new Error("push must not fetch its result as baseline");
        },
        log: () => {},
      });
    assert.deepEqual(run().added, [[131, "Next"]]);
    for (const value of [undefined, "0".repeat(40), "f".repeat(40), after])
      assert.throws(() => run({ before: value }));
    for (const overrides of [
      { GITHUB_REPOSITORY: "foreign/repository" },
      { GITHUB_SHA: before },
      { GITHUB_REF: "refs/heads/feature" },
      { GITHUB_EVENT_NAME: "unsupported" },
    ])
      assert.throws(() =>
        runForEvent({ cwd, env: { ...env, ...overrides }, event, log: () => {} }),
      );
    git("checkout", "-b", "late");
    const late = commit(
      source([...identities, [125, "Late"], [131, "Next"]].sort(([a], [b]) => a - b)),
    );
    assert.throws(
      () =>
        runForEvent({
          cwd,
          env: { ...env, GITHUB_SHA: late },
          event: { ...event, before: after, after: late },
          log: () => {},
        }),
      /above baseline maximum 131/,
    );
    assert.throws(
      () => qualifyRevisions({ cwd, baseline: "f".repeat(40), candidate: late, log: () => {} }),
      /Git evidence unavailable/,
    );
    const malformed = commit("export const jonesMigrationEntries = compute();");
    assert.throws(
      () => qualifyRevisions({ cwd, baseline: malformed, candidate: late, log: () => {} }),
      /Unsupported or malformed/,
    );
    git("rm", MANIFEST);
    git("commit", "--quiet", "-m", "Deleted manifest");
    assert.throws(
      () =>
        qualifyRevisions({
          cwd,
          baseline: after,
          candidate: "HEAD",
          allowUnchanged: true,
          log: () => {},
        }),
      /Git evidence unavailable/,
    );
    assert.throws(
      () => qualifyRevisions({ cwd, baseline: "HEAD", candidate: after, log: () => {} }),
      /Git evidence unavailable/,
    );
  }));

test("unconditional Lint admits Jones migration order after dependencies are ready", () => {
  const workflow = readFileSync(join(__dirname, "../../../.github/workflows/ci.yml"), "utf8");
  const lint = workflow.match(/^  lint:\n([\s\S]*?)(?=^  [\w-]+:)/m)[1];
  assert.doesNotMatch(lint, /needs:|\n    if:/);
  assert.match(lint, /fetch-depth: 0/);
  assert.match(
    lint,
    /run-install: true[\s\S]*?- name: Check Jones migration order\n        run: node scripts\/jones\/ci\/jones-migration-order\.cjs/,
  );
});
