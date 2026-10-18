// @effect-diagnostics nodeBuiltinImport:off -- Build-job source stamping and its isolated Git fixture use native Node adapters.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

const fixtures: string[] = [];
afterEach(async () => {
  for (const fixture of fixtures.splice(0))
    await NodeFSP.rm(fixture, { recursive: true, force: true });
});

async function fixture() {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-stamp-test-"));
  fixtures.push(root);
  for (const dir of ["scripts", "apps/server", "apps/desktop"])
    await NodeFSP.mkdir(NodePath.join(root, dir), { recursive: true });
  await NodeFSP.copyFile(
    new URL("./stamp-jones-build.ts", import.meta.url),
    NodePath.join(root, "scripts/stamp-jones-build.ts"),
  );
  for (const name of ["server", "desktop"])
    await NodeFSP.writeFile(
      NodePath.join(root, `apps/${name}/package.json`),
      JSON.stringify({ name, version: "1.2.3", retained: { nested: true } }),
    );
  const git = (args: string[]) =>
    NodeChildProcess.execFileSync("git", args, {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  git(["init", "--initial-branch=fixture"]);
  git(["add", "apps"]);
  git([
    "-c",
    "user.name=Fixture",
    "-c",
    "user.email=fixture@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "-m",
    "fixture",
  ]);
  return { root, sha: git(["rev-parse", "HEAD"]), tree: git(["rev-parse", "HEAD^{tree}"]) };
}

describe("Jones build source stamp", () => {
  it.each(["workflow fallback", "explicit head", "outside workflow"] as const)(
    "binds both source packages to the checked-in tree while preserving build versions: %s",
    async (sourceSelection) => {
      const input = await fixture(),
        envFile = NodePath.join(input.root, "build-env");
      const serverFile = NodePath.join(input.root, "apps/server/package.json");
      const server = JSON.parse(await NodeFSP.readFile(serverFile, "utf8")) as Record<
        string,
        unknown
      >;
      server.version = "1.2.3-preview.20261002.4.2";
      await NodeFSP.writeFile(serverFile, JSON.stringify(server));
      NodeChildProcess.execFileSync(
        process.execPath,
        [NodePath.join(input.root, "scripts/stamp-jones-build.ts")],
        {
          cwd: input.root,
          env: {
            ...process.env,
            JONES_EXPECTED_SOURCE_SHA: sourceSelection === "explicit head" ? input.sha : undefined,
            GITHUB_SHA:
              sourceSelection === "outside workflow"
                ? undefined
                : sourceSelection === "explicit head"
                  ? "d".repeat(40)
                  : input.sha,
            GITHUB_ENV: envFile,
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      for (const name of ["server", "desktop"]) {
        expect(
          JSON.parse(
            await NodeFSP.readFile(NodePath.join(input.root, `apps/${name}/package.json`), "utf8"),
          ),
        ).toMatchObject({
          name,
          retained: { nested: true },
          startupGateProtocol: 1,
          jonesSource: { repository: "Jones-Systems/Jones-Code", sha: input.sha, tree: input.tree },
        });
      }
      expect(JSON.parse(await NodeFSP.readFile(serverFile, "utf8"))).toHaveProperty(
        "version",
        "1.2.3-preview.20261002.4.2",
      );
      expect(await NodeFSP.readFile(envFile, "utf8")).toBe(
        `JONES_SOURCE_SHA=${input.sha}\nJONES_SOURCE_TREE=${input.tree}\n`,
      );
    },
  );
  it.each([
    {
      name: "mismatched workflow fallback",
      expected: undefined,
      error: "Checkout does not match workflow source.",
    },
    {
      name: "mismatched explicit head",
      expected: "d".repeat(40),
      error: "Checkout does not match workflow source.",
    },
    { name: "empty explicit head", expected: "", error: "Invalid expected Jones build source." },
    {
      name: "malformed explicit head",
      expected: "not-a-source-sha",
      error: "Invalid expected Jones build source.",
    },
  ])("refuses $name before writing manifests or the receipt", async ({ expected, error }) => {
    const input = await fixture();
    const manifests = ["server", "desktop"].map((name) =>
      NodePath.join(input.root, `apps/${name}/package.json`),
    );
    const before = await Promise.all(manifests.map((file) => NodeFSP.readFile(file, "utf8")));
    const envFile = NodePath.join(input.root, "build-env");
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [NodePath.join(input.root, "scripts/stamp-jones-build.ts")],
      {
        cwd: input.root,
        env: {
          ...process.env,
          JONES_EXPECTED_SOURCE_SHA: expected,
          GITHUB_SHA: expected === undefined ? "d".repeat(40) : input.sha,
          GITHUB_ENV: envFile,
        },
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    expect(result.status).toBe(1);
    expect(result.stderr).toContain(error);
    expect(await Promise.all(manifests.map((file) => NodeFSP.readFile(file, "utf8")))).toEqual(
      before,
    );
    await expect(NodeFSP.lstat(envFile)).rejects.toMatchObject({ code: "ENOENT" });
  });
});
