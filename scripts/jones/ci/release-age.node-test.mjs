import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  DEFAULT_MIN_AGE_MINUTES,
  lookupPublishTimes,
  parseExcludeBlock,
  parseLockPackages,
  refreshBoundaryAnchors,
  rewriteExcludeBlock,
  run,
  validateExcludes,
} from "./release-age.mjs";

const script = NodePath.resolve(import.meta.dirname, "release-age.mjs");
const NOW = new Date("2026-11-17T00:00:00Z");
// 21 days before NOW: a version published at or before this instant is old enough.
const CUTOFF = "2026-10-27T00:00:00Z";
const OLD = "2026-10-01T00:00:00Z";
const YOUNG = "2026-11-10T00:00:00Z";

const lockEntry = ([name, version]) =>
  `  ${name.startsWith("@") ? `'${name}@${version}'` : `${name}@${version}`}:\n    resolution: {integrity: sha512-${name.length}${version.length}}\n\n`;
const lockfile = (packages, extra = "") =>
  [
    "---",
    "lockfileVersion: '9.0'",
    "",
    "importers:",
    "",
    "  .:",
    "    packageManagerDependencies:",
    "      pnpm:",
    "        specifier: 12.14.0",
    "        version: 12.14.0",
    "",
    "packages:",
    "",
    "  tool-only@9.9.9:",
    "    resolution: {integrity: sha512-tool}",
    "",
    "snapshots:",
    "",
    "---",
    "lockfileVersion: '9.0'",
    "",
    "importers:",
    "",
    "  .: {}",
    "",
    "packages:",
    "",
    ...packages.map(lockEntry),
    extra,
    "snapshots:",
    "",
    "  left-pad@1.0.0: {}",
    "",
  ].join("\n");
const workspace = (entries, tail = "") =>
  `packages:\n  - apps/*\n\nminimumReleaseAgeExclude:\n${entries.map((entry) => `  - "${entry}"\n`).join("")}${tail}`;

const registry = (documents, { onRequest } = {}) => {
  const urls = [];
  let active = 0;
  const stats = { urls, maxActive: 0 };
  const fetch = async (url) => {
    urls.push(url);
    onRequest?.(url);
    active += 1;
    stats.maxActive = Math.max(stats.maxActive, active);
    await new Promise((resolve) => setImmediate(resolve));
    active -= 1;
    const name = decodeURIComponent(url.slice("https://registry.npmjs.org/".length));
    const time = documents[name];
    return time
      ? new Response(JSON.stringify({ time }), { status: 200 })
      : new Response("{}", { status: 404 });
  };
  return Object.assign(fetch, { stats });
};

const files = ({ lock, workspaceText, resolutions }) => {
  const written = {};
  return {
    written,
    readFile: async (path) => {
      const name = NodePath.basename(path);
      if (name === "pnpm-lock.yaml") return lock;
      if (name === "pnpm-workspace.yaml") return workspaceText;
      if (name === "resolutions.json" && resolutions !== undefined) return resolutions;
      throw Object.assign(new Error(`ENOENT: ${path}`), { code: "ENOENT" });
    },
    writeFile: async (path, text) => {
      written[NodePath.basename(path)] = text;
    },
  };
};

const capture = () => {
  const lines = { out: [], err: [] };
  return {
    lines,
    out: (text) => lines.out.push(text),
    err: (text) => lines.err.push(text),
    text: () => [...lines.out, ...lines.err].join("\n"),
  };
};

const noNetwork = async () => {
  throw new Error("offline mode must not use the network");
};
const instant = async () => {};

NodeTest.describe("lockfile parsing", () => {
  NodeTest.it("reads only registry packages from the final document", () => {
    const text = lockfile(
      [
        ["left-pad", "1.0.0"],
        ["@scope/pkg", "2.1.0-beta.3"],
      ],
      [
        "  'local-mod@file:apps/mobile/modules/local':",
        "    resolution: {directory: apps/mobile/modules/local, type: directory}",
        "",
        "  tarball-pkg@file:apps/mobile/deps/tarball-pkg-1.0.0.tgz:",
        "    resolution: {integrity: sha512-abc, tarball: file:apps/mobile/deps/tarball-pkg-1.0.0.tgz}",
        "",
        "  hosted@1.0.0:",
        "    resolution: {integrity: sha512-abc, tarball: https://example.test/hosted-1.0.0.tgz}",
        "",
        "  from-git@1.0.0:",
        "    resolution: {commit: abc, repo: https://example.test/x.git, type: git}",
        "",
      ].join("\n"),
    );
    NodeAssert.deepEqual(
      [...parseLockPackages(text).keys()],
      ["left-pad@1.0.0", "@scope/pkg@2.1.0-beta.3"],
    );
  });

  NodeTest.it("does not read package-like lines outside the packages section", () => {
    const packages = parseLockPackages(lockfile([["left-pad", "1.0.0"]]));
    NodeAssert.equal(packages.has("tool-only@9.9.9"), false);
    NodeAssert.equal(packages.size, 1);
  });

  NodeTest.it("accepts a lockfile with a single document", () => {
    const single =
      "lockfileVersion: '9.0'\n\npackages:\n\n  a@1.0.0:\n    resolution: {integrity: sha512-a}\n";
    NodeAssert.deepEqual([...parseLockPackages(single).keys()], ["a@1.0.0"]);
  });
});

NodeTest.describe("exclusion block parsing", () => {
  NodeTest.it("reads quoted and bare entries and stops at the next key", () => {
    const text = [
      "a: 1",
      "minimumReleaseAgeExclude:",
      '  - "a@1.0.0"',
      "  - 'b@2.0.0'",
      "  - c@3.0.0 # trailing note",
      "",
      "# belongs to the next key",
      "other: true",
      "",
    ].join("\n");
    const block = parseExcludeBlock(text);
    NodeAssert.deepEqual(
      block.entries.map((entry) => [entry.raw, entry.line]),
      [
        ["a@1.0.0", 3],
        ["b@2.0.0", 4],
        ["c@3.0.0", 5],
      ],
    );
    NodeAssert.equal(block.comments, false);
    NodeAssert.deepEqual([block.start, block.end], [1, 5]);
  });

  NodeTest.it("flags a comment between items and accepts an absent or empty block", () => {
    const withComment = 'minimumReleaseAgeExclude:\n  - "a@1.0.0"\n  # note\n  - "b@1.0.0"\n';
    NodeAssert.equal(parseExcludeBlock(withComment).comments, true);
    NodeAssert.deepEqual(parseExcludeBlock("x: 1\n").entries, []);
    NodeAssert.deepEqual(parseExcludeBlock("minimumReleaseAgeExclude: []\n").entries, []);
  });

  NodeTest.it("rejects a flow list it cannot rewrite", () => {
    NodeAssert.throws(
      () => parseExcludeBlock("minimumReleaseAgeExclude: [a@1.0.0]\n"),
      /must be a block list/,
    );
  });
});

NodeTest.describe("exclusion validation", () => {
  const lock = parseLockPackages(
    lockfile([
      ["left-pad", "1.0.0"],
      ["@scope/pkg", "2.0.0"],
    ]),
  );
  const check = (...raw) =>
    validateExcludes(
      raw.map((value, index) => ({ raw: value, line: index + 1 })),
      lock,
    );

  NodeTest.it("accepts exact locked versions", () => {
    const { problems, valid } = check("left-pad@1.0.0", "@scope/pkg@2.0.0");
    NodeAssert.deepEqual(problems, []);
    NodeAssert.equal(valid.size, 2);
  });

  NodeTest.it("rejects wildcards, missing versions, ranges and absent versions", () => {
    const { problems } = check(
      "@scope/*",
      "left-pad@*",
      "left-pad",
      "@scope/pkg",
      "left-pad@^1.0.0",
      "left-pad@1.0.0 || 1.0.1",
      "left-pad@1.0.1",
      "other@1.0.0",
    );
    NodeAssert.equal(problems.length, 8);
    NodeAssert.match(problems[0], /"@scope\/\*" uses a wildcard/);
    NodeAssert.match(problems[1], /"left-pad@\*" uses a wildcard/);
    NodeAssert.match(problems[2], /"left-pad" has no version/);
    NodeAssert.match(problems[3], /"@scope\/pkg" has no version/);
    NodeAssert.match(problems[4], /"left-pad@\^1\.0\.0" is not an exact version/);
    NodeAssert.match(problems[5], /"left-pad@1\.0\.0 \|\| 1\.0\.1" is not an exact version/);
    NodeAssert.match(problems[6], /not a registry package in pnpm-lock\.yaml/);
    NodeAssert.match(problems[7], /not a registry package in pnpm-lock\.yaml/);
  });

  NodeTest.it("rejects a duplicate entry", () => {
    const { problems } = check("left-pad@1.0.0", "left-pad@1.0.0");
    NodeAssert.deepEqual(problems, ['pnpm-workspace.yaml:2: "left-pad@1.0.0" is listed twice.']);
  });
});

NodeTest.describe("exclusion block rewriting", () => {
  const head = "packages:\n  - apps/*\n\n";
  const tail = "\ncatalog:\n  x: 1\n";

  NodeTest.it("sorts entries and keeps the rest of the file byte for byte", () => {
    const text = `${head}minimumReleaseAgeExclude:\n  - "z@1.0.0"\n  - "b@1.0.0"\n${tail}`;
    const next = rewriteExcludeBlock(text, ["z@1.0.0", "a@2.0.0", "b@1.0.0", "a@2.0.0"]);
    NodeAssert.equal(
      next,
      `${head}minimumReleaseAgeExclude:\n  - "a@2.0.0"\n  - "b@1.0.0"\n  - "z@1.0.0"\n${tail}`,
    );
    NodeAssert.equal(rewriteExcludeBlock(next, ["z@1.0.0", "a@2.0.0", "b@1.0.0"]), next);
  });

  NodeTest.it("writes an empty list when no entry remains", () => {
    const text = `${head}minimumReleaseAgeExclude:\n  - "a@1.0.0"\n${tail}`;
    const next = rewriteExcludeBlock(text, []);
    NodeAssert.equal(next, `${head}minimumReleaseAgeExclude: []\n${tail}`);
    NodeAssert.equal(rewriteExcludeBlock(next, []), next);
  });

  NodeTest.it("appends the block when it is missing", () => {
    NodeAssert.equal(
      rewriteExcludeBlock("packages:\n  - apps/*\n", ["a@1.0.0"]),
      'packages:\n  - apps/*\n\nminimumReleaseAgeExclude:\n  - "a@1.0.0"\n',
    );
  });

  NodeTest.it("rebinds every workspace anchor in the boundary resolutions and nothing else", () => {
    const entry = (path, sha256) =>
      `    {\n      "path": "${path}",\n      "sha256": "${sha256}"\n    }`;
    const other = "a".repeat(64);
    const stale = "b".repeat(64);
    const text = `[\n${entry("apps/web/package.json", other)},\n${entry("pnpm-workspace.yaml", stale)},\n${entry("pnpm-workspace.yaml", stale)}\n]`;
    const hash = NodeCrypto.createHash("sha256").update("workspace text").digest("hex");
    NodeAssert.equal(
      refreshBoundaryAnchors(text, "workspace text"),
      `[\n${entry("apps/web/package.json", other)},\n${entry("pnpm-workspace.yaml", hash)},\n${entry("pnpm-workspace.yaml", hash)}\n]`,
    );
  });

  NodeTest.it("refuses to drop comments inside the block", () => {
    const text = 'minimumReleaseAgeExclude:\n  - "a@1.0.0"\n  # why\n  - "b@1.0.0"\n';
    NodeAssert.throws(() => rewriteExcludeBlock(text, ["a@1.0.0"]), /contains comments/);
  });
});

NodeTest.describe("registry lookup", () => {
  const response = (status, body = {}) => new Response(JSON.stringify(body), { status });

  NodeTest.it("retries transient failures with backoff and then succeeds", async () => {
    const answers = [
      () => {
        throw new TypeError("fetch failed");
      },
      () => response(503),
      () => response(429),
      () => response(200, { time: { "1.0.0": OLD } }),
    ];
    const urls = [];
    const delays = [];
    const results = await lookupPublishTimes([{ name: "@scope/pkg", version: "1.0.0" }], {
      fetch: async (url) => {
        urls.push(url);
        return answers.shift()();
      },
      sleep: async (ms) => delays.push(ms),
    });
    NodeAssert.deepEqual(urls, Array(4).fill("https://registry.npmjs.org/@scope%2Fpkg"));
    NodeAssert.deepEqual(delays, [500, 1000, 2000]);
    NodeAssert.equal(
      results.get("@scope/pkg@1.0.0").published.toISOString(),
      "2026-10-01T00:00:00.000Z",
    );
  });

  NodeTest.it("reports an error after three retries", async () => {
    let calls = 0;
    const results = await lookupPublishTimes([{ name: "a", version: "1.0.0" }], {
      fetch: async () => {
        calls += 1;
        return response(500);
      },
      sleep: instant,
    });
    NodeAssert.equal(calls, 4);
    NodeAssert.match(results.get("a@1.0.0").error, /HTTP 500 \(after 3 retries\)/);
  });

  NodeTest.it("does not retry a missing package or a client error", async () => {
    for (const status of [404, 403]) {
      let calls = 0;
      const results = await lookupPublishTimes([{ name: "a", version: "1.0.0" }], {
        fetch: async () => {
          calls += 1;
          return response(status);
        },
        sleep: instant,
      });
      NodeAssert.equal(calls, 1);
      NodeAssert.ok(results.get("a@1.0.0").error);
    }
  });

  NodeTest.it("reports a version the registry has no publish time for", async () => {
    const results = await lookupPublishTimes(
      [
        { name: "a", version: "1.0.0" },
        { name: "a", version: "1.0.1" },
      ],
      { fetch: async () => response(200, { time: { "1.0.0": OLD } }), sleep: instant },
    );
    NodeAssert.ok(results.get("a@1.0.0").published);
    NodeAssert.match(results.get("a@1.0.1").error, /no publish time/);
  });

  NodeTest.it("makes one request per package name and bounds concurrency", async () => {
    const names = Array.from({ length: 12 }, (_, index) => `pkg${index}`);
    const documents = Object.fromEntries(
      names.map((name) => [name, { "1.0.0": OLD, "1.1.0": OLD }]),
    );
    const fetch = registry(documents);
    const results = await lookupPublishTimes(
      names.flatMap((name) => [
        { name, version: "1.0.0" },
        { name, version: "1.1.0" },
      ]),
      { fetch, sleep: instant, concurrency: 3 },
    );
    NodeAssert.equal(fetch.stats.urls.length, 12);
    NodeAssert.equal(results.size, 24);
    NodeAssert.ok(fetch.stats.maxActive <= 3 && fetch.stats.maxActive > 1);
  });
});

NodeTest.describe("run", () => {
  const baseLock = lockfile([["left-pad", "1.0.0"]]);
  const lock = lockfile([
    ["left-pad", "1.0.0"],
    ["fresh", "2.0.0"],
    ["@scope/old", "3.0.0"],
  ]);
  const documents = {
    "left-pad": { "1.0.0": YOUNG },
    fresh: { "2.0.0": YOUNG },
    "@scope/old": { "3.0.0": OLD },
  };
  const invoke = async (
    argv,
    {
      lockText = lock,
      excludes = [],
      docs = documents,
      resolutions,
      fetch = registry(docs),
      ...extra
    } = {},
  ) => {
    const io = files({ lock: lockText, workspaceText: workspace(excludes), resolutions });
    const output = capture();
    const code = await run(["--root", "/repo", ...argv], {
      fetch,
      now: () => NOW,
      sleep: instant,
      readBaseLock: () => baseLock,
      ...io,
      out: output.out,
      err: output.err,
      ...extra,
    });
    return { code, io, output, fetch };
  };

  NodeTest.it("validates the exclusion list offline by default", async () => {
    const ok = await invoke([], { excludes: ["fresh@2.0.0"], fetch: noNetwork });
    NodeAssert.equal(ok.code, 0);
    const bad = await invoke([], { excludes: ["fresh@2.0.0", "gone@1.0.0"], fetch: noNetwork });
    NodeAssert.equal(bad.code, 1);
    NodeAssert.match(bad.output.text(), /"gone@1\.0\.0" is not a registry package/);
    NodeAssert.match(bad.output.text(), /release-age\.mjs --all --write/);
  });

  NodeTest.it("with --base checks only entries new since the base lock", async () => {
    const result = await invoke(["--base", "abc1234"], { excludes: [] });
    NodeAssert.equal(result.code, 1);
    // left-pad@1.0.0 is young but already in the base lock; only the new entries are looked up.
    NodeAssert.deepEqual(result.fetch.stats.urls.sort(), [
      "https://registry.npmjs.org/@scope%2Fold",
      "https://registry.npmjs.org/fresh",
    ]);
    NodeAssert.match(result.output.text(), /fresh@2\.0\.0 \(published 2026-11-10, 7\.0d old\)/);
    NodeAssert.doesNotMatch(result.output.text(), /left-pad/);
    NodeAssert.doesNotMatch(result.output.text(), /@scope\/old@3\.0\.0 \(/);
  });

  NodeTest.it("with --base passes a new young version that is excluded exactly", async () => {
    const result = await invoke(["--base", "abc1234"], { excludes: ["fresh@2.0.0"] });
    NodeAssert.equal(result.code, 0);
    NodeAssert.match(result.output.text(), /none violate it/);
  });

  NodeTest.it("with --base uses no network when the lock has no new package", async () => {
    const result = await invoke(["--base", "abc1234"], {
      lockText: baseLock,
      fetch: noNetwork,
    });
    NodeAssert.equal(result.code, 0);
    NodeAssert.match(result.output.text(), /No new registry packages since abc1234/);
  });

  NodeTest.it("fails closed when the registry cannot be read", async () => {
    const result = await invoke(["--base", "abc1234"], {
      fetch: async () => new Response("", { status: 503 }),
      retries: 1,
    });
    NodeAssert.equal(result.code, 2);
    NodeAssert.match(result.output.text(), /Could not verify 2 package\(s\)/);
  });

  NodeTest.it("treats a version exactly at the policy age as old enough", async () => {
    const result = await invoke(["--all"], {
      docs: {
        "left-pad": { "1.0.0": CUTOFF },
        fresh: { "2.0.0": CUTOFF },
        "@scope/old": { "3.0.0": CUTOFF },
      },
    });
    NodeAssert.equal(result.code, 0);
  });

  NodeTest.it(
    "--all fails on unexcluded young versions and warns about aged-out exclusions",
    async () => {
      const result = await invoke(["--all"], { excludes: ["@scope/old@3.0.0"] });
      NodeAssert.equal(result.code, 1);
      NodeAssert.match(
        result.output.text(),
        /Aged-out exclusions[^\n]*\n {2}@scope\/old@3\.0\.0 \(/,
      );
      NodeAssert.match(
        result.output.text(),
        /2 locked version\(s\) are younger than the 30240 minutes/,
      );
      NodeAssert.deepEqual(result.io.written, {});

      const clean = await invoke(["--all"], {
        excludes: ["@scope/old@3.0.0", "fresh@2.0.0", "left-pad@1.0.0"],
      });
      NodeAssert.equal(clean.code, 0);
      NodeAssert.match(clean.output.text(), /Aged-out exclusions/);
    },
  );

  NodeTest.it("--all --write adds violators and drops stale and aged-out entries", async () => {
    const result = await invoke(["--all", "--write"], {
      excludes: ["@scope/old@3.0.0", "gone@1.0.0", "left-pad@1.0.0"],
    });
    NodeAssert.equal(result.code, 0);
    NodeAssert.equal(
      result.io.written["pnpm-workspace.yaml"],
      workspace(["fresh@2.0.0", "left-pad@1.0.0"]),
    );
    NodeAssert.match(
      result.output.text(),
      /Rewrote minimumReleaseAgeExclude: 2 entries \(1 added, 2 removed\)/,
    );
    NodeAssert.equal(result.io.written["pnpm-lock.yaml"], undefined);
  });

  NodeTest.it("--all --write refreshes the workspace hashes the boundary check binds", async () => {
    const anchor = (path, sha256) => `{ "path": "${path}", "sha256": "${sha256}" }`;
    const stale = "0".repeat(64);
    const resolutions = `[${anchor("apps/web/package.json", stale)}, ${anchor("pnpm-workspace.yaml", stale)}]`;
    const result = await invoke(["--all", "--write"], { resolutions });
    const next = workspace(["fresh@2.0.0", "left-pad@1.0.0"]);
    const hash = NodeCrypto.createHash("sha256").update(next).digest("hex");
    NodeAssert.equal(
      result.io.written["resolutions.json"],
      `[${anchor("apps/web/package.json", stale)}, ${anchor("pnpm-workspace.yaml", hash)}]`,
    );
    NodeAssert.match(result.output.text(), /Refreshed the pnpm-workspace\.yaml hashes/);

    const current = await invoke(["--all", "--write"], {
      excludes: ["fresh@2.0.0", "left-pad@1.0.0"],
      resolutions: result.io.written["resolutions.json"],
    });
    NodeAssert.deepEqual(current.io.written, {});
  });

  NodeTest.it("--all --write leaves a current block untouched", async () => {
    const result = await invoke(["--all", "--write"], {
      excludes: ["fresh@2.0.0", "left-pad@1.0.0"],
    });
    NodeAssert.equal(result.code, 0);
    NodeAssert.deepEqual(result.io.written, {});
  });

  NodeTest.it("--all --write does not write when a lookup failed", async () => {
    const result = await invoke(["--all", "--write"], {
      fetch: async () => new Response("", { status: 404 }),
    });
    NodeAssert.equal(result.code, 2);
    NodeAssert.deepEqual(result.io.written, {});
  });

  NodeTest.it("--min-age-minutes overrides the policy", async () => {
    const strict = await invoke(["--all", "--min-age-minutes", "100000"]);
    NodeAssert.equal(strict.code, 1);
    NodeAssert.match(strict.output.text(), /100000 minutes/);
    const lenient = await invoke(["--all", "--min-age-minutes", "1"]);
    NodeAssert.equal(lenient.code, 0);
    NodeAssert.equal(DEFAULT_MIN_AGE_MINUTES, 30240);
  });

  NodeTest.it("rejects unsupported options and shows the usage", async () => {
    for (const [argv, message] of [
      [["--write"], /--write requires --all/],
      [["--all", "--base", "abc1234"], /exclusive/],
      [["--base"], /--base needs a value/],
      [["--base", "-x"], /--base needs a commit or ref/],
      [["--min-age-minutes", "soon"], /whole number/],
      [["--nope"], /Unknown option: --nope/],
    ]) {
      const result = await invoke(argv, { fetch: noNetwork, readBaseLock: undefined });
      NodeAssert.equal(result.code, 2, argv.join(" "));
      NodeAssert.match(result.output.text(), message);
      NodeAssert.match(result.output.text(), /usage: release-age\.mjs/);
    }
  });

  NodeTest.it("does not show the usage for a failure that is not about options", async () => {
    const result = await invoke(["--base", "abc1234"], {
      readBaseLock: () => {
        throw new Error("fatal: bad object abc1234");
      },
    });
    NodeAssert.equal(result.code, 2);
    NodeAssert.match(result.output.text(), /bad object abc1234/);
    NodeAssert.doesNotMatch(result.output.text(), /usage:/);
  });
});

NodeTest.describe("command line", () => {
  const sandbox = async (t, { workspaceText, lock }) => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "release-age-"));
    t.after(() => NodeFSP.rm(root, { recursive: true, force: true }));
    await NodeFSP.writeFile(NodePath.join(root, "pnpm-workspace.yaml"), workspaceText);
    await NodeFSP.writeFile(NodePath.join(root, "pnpm-lock.yaml"), lock);
    return root;
  };
  const exec = (args) =>
    NodeChildProcess.spawnSync(process.execPath, [script, ...args], { encoding: "utf8" });

  NodeTest.it("exits non-zero on an invalid exclusion list and zero on a valid one", async (t) => {
    const lock = lockfile([["left-pad", "1.0.0"]]);
    const valid = await sandbox(t, { workspaceText: workspace(["left-pad@1.0.0"]), lock });
    const ok = exec(["--root", valid]);
    NodeAssert.equal(ok.status, 0, ok.stderr);
    const invalid = await sandbox(t, { workspaceText: workspace(["left-pad@*"]), lock });
    const bad = exec(["--root", invalid]);
    NodeAssert.equal(bad.status, 1);
    NodeAssert.match(bad.stderr, /uses a wildcard/);
  });

  NodeTest.it("reads the base lock from git for --base", async (t) => {
    const root = await sandbox(t, {
      workspaceText: workspace([]),
      lock: lockfile([["left-pad", "1.0.0"]]),
    });
    const git = (...args) =>
      NodeChildProcess.execFileSync(
        "git",
        [
          "-c",
          "user.name=t",
          "-c",
          "user.email=t@example.test",
          "-c",
          "commit.gpgsign=false",
          ...args,
        ],
        { cwd: root, encoding: "utf8" },
      ).trim();
    git("init", "--quiet");
    git("add", ".");
    git("commit", "--quiet", "-m", "base");
    const sha = git("rev-parse", "HEAD");
    const unchanged = exec(["--root", root, "--base", sha]);
    NodeAssert.equal(unchanged.status, 0, unchanged.stderr);
    NodeAssert.match(unchanged.stdout, /No new registry packages/);

    const byRef = exec(["--root", root, "--base", "HEAD"]);
    NodeAssert.equal(byRef.status, 0, byRef.stderr);

    const missing = exec(["--root", root, "--base", "0".repeat(40)]);
    NodeAssert.equal(missing.status, 2);
    NodeAssert.doesNotMatch(missing.stderr, /usage:/);
    for (const unsafe of ["main; echo", "--output=x", "-x", "a b"]) {
      const malformed = exec(["--root", root, "--base", unsafe]);
      NodeAssert.equal(malformed.status, 2, unsafe);
      NodeAssert.match(malformed.stderr, /needs a (?:value|commit or ref)/, unsafe);
    }
  });
});
