// @effect-diagnostics nodeBuiltinImport:off - Synthetic owned-checkout fixtures, never a build or release.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { readJonesStartupGateProtocol } from "../../../apps/server/src/jones/cloud/qualifiedStartup.ts";
import {
  bundlesJonesNativeHelper,
  JonesDesktopBuildMetadata,
  stampJonesBuildSource,
  verifyJonesPackagedStartupGate,
} from "./build-provenance.ts";

const fixtures = new Set<string>();
async function cleanupFixture(root: string): Promise<void> {
  await NodeFSP.rm(root, { recursive: true, force: true });
  fixtures.delete(root);
}
afterEach(async () => {
  for (const root of fixtures) await cleanupFixture(root);
});

async function fixture() {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, "provenance-"));
  fixtures.add(root);
  for (const name of ["server", "desktop"]) {
    const directory = NodePath.join(root, "apps", name);
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(directory, "package.json"),
      JSON.stringify({ name, version: "1.2.3", retained: { nested: true } }),
    );
  }
  const gitEnv = {
    ...Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith("GIT_"))),
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: NodeOS.devNull,
  };
  const git = (args: readonly string[]) =>
    NodeChildProcess.execFileSync("git", [...args], {
      cwd: root,
      env: gitEnv,
      encoding: "utf8",
      maxBuffer: 16384,
      timeout: 10000,
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

describe("Jones build provenance", () => {
  const decodeDesktopMetadata = Schema.decodeUnknownSync(JonesDesktopBuildMetadata);
  const source = {
    repository: "Jones-Systems/Jones-Code",
    sha: "b".repeat(40),
    tree: "c".repeat(40),
  };

  it("refuses packaged metadata that lost its implemented gate or source binding", () => {
    const metadata = {
      version: "0.0.45-preview.20261010.38048764253.1",
      jonesSource: source,
      startupGateProtocol: 1,
    };
    const expected = { version: metadata.version, source };
    expect(() => verifyJonesPackagedStartupGate(metadata, expected)).not.toThrow();
    for (const invalid of [
      { ...metadata, startupGateProtocol: undefined },
      { ...metadata, startupGateProtocol: 2 },
      { ...metadata, jonesSource: { ...source, sha: "d".repeat(40) } },
      { ...metadata, jonesSource: { ...source, tree: "d".repeat(40) } },
      { ...metadata, version: "older" },
    ]) {
      expect(() => verifyJonesPackagedStartupGate(invalid, expected)).toThrow();
    }
  });

  it("preserves absent capability and only propagates an explicit source-bound marker", () => {
    expect(decodeDesktopMetadata({})).toEqual({});
    expect(decodeDesktopMetadata({ jonesSource: source })).toEqual({ jonesSource: source });
    expect(decodeDesktopMetadata({ jonesSource: source, startupGateProtocol: 1 })).toEqual({
      jonesSource: source,
      startupGateProtocol: 1,
    });
  });

  it.each([undefined, null, false, true, "1", 0, 2])(
    "rejects a malformed supplied capability %s",
    (startupGateProtocol) => {
      expect(() => decodeDesktopMetadata({ jonesSource: source, startupGateProtocol })).toThrow();
    },
  );

  it("requires validated Jones source metadata for the startup marker", () => {
    expect(() => decodeDesktopMetadata({ startupGateProtocol: 1 })).toThrow();
    for (const jonesSource of [
      null,
      { ...source, repository: "another/repository" },
      { ...source, sha: "not-a-source-sha" },
      { ...source, tree: "not-a-source-tree" },
    ]) {
      expect(() => decodeDesktopMetadata({ jonesSource, startupGateProtocol: 1 })).toThrow();
    }
  });

  it("binds both packages to the committed tree while preserving preview versions", async () => {
    const f = await fixture();
    try {
      const serverFile = NodePath.join(f.root, "apps/server/package.json");
      const server = JSON.parse(await NodeFSP.readFile(serverFile, "utf8")) as Record<
        string,
        unknown
      >;
      server.version = "1.2.3-preview.20261002.4.2";
      await NodeFSP.writeFile(serverFile, JSON.stringify(server));
      const envFile = NodePath.join(f.root, "build-env");
      const identity = stampJonesBuildSource({
        root: f.root,
        workflowSha: f.sha,
        githubEnvFile: envFile,
      });
      expect(identity).toEqual({
        repository: "Jones-Systems/Jones-Code",
        sha: f.sha,
        tree: f.tree,
      });
      for (const name of ["server", "desktop"]) {
        const stamped = JSON.parse(
          await NodeFSP.readFile(NodePath.join(f.root, `apps/${name}/package.json`), "utf8"),
        );
        expect(stamped).toMatchObject({ name, retained: { nested: true }, jonesSource: identity });
        expect(stamped.startupGateProtocol).toBe(readJonesStartupGateProtocol());
        expect(decodeDesktopMetadata(stamped)).toMatchObject({
          startupGateProtocol: 1,
          jonesSource: identity,
        });
      }
      expect(JSON.parse(await NodeFSP.readFile(serverFile, "utf8"))).toHaveProperty(
        "version",
        server.version,
      );
      expect(await NodeFSP.readFile(envFile, "utf8")).toBe(
        `JONES_SOURCE_SHA=${f.sha}\nJONES_SOURCE_TREE=${f.tree}\n`,
      );
    } finally {
      await cleanupFixture(f.root);
    }
  });

  it("refuses a mismatched workflow before either package changes", async () => {
    const f = await fixture();
    try {
      const files = ["server", "desktop"].map((name) =>
        NodePath.join(f.root, `apps/${name}/package.json`),
      );
      const before = await Promise.all(files.map((file) => NodeFSP.readFile(file, "utf8")));
      expect(() => stampJonesBuildSource({ root: f.root, workflowSha: "d".repeat(40) })).toThrow(
        "workflow source",
      );
      expect(await Promise.all(files.map((file) => NodeFSP.readFile(file, "utf8")))).toEqual(
        before,
      );
      expect(() => stampJonesBuildSource({ root: NodePath.join(f.root, "apps") })).toThrow(
        "exact checkout root",
      );
    } finally {
      await cleanupFixture(f.root);
    }
  });

  it.each(["0.0.44-preview.20261002.11", "0.0.44-preview.20261002.11.2"])(
    "includes the native helper only in unsigned Darwin preview %s",
    (version) => {
      expect(bundlesJonesNativeHelper("mac", version, false)).toBe(true);
      expect(bundlesJonesNativeHelper("mac", version, true)).toBe(false);
      expect(bundlesJonesNativeHelper("linux", version, false)).toBe(false);
      expect(bundlesJonesNativeHelper("win", version, false)).toBe(false);
    },
  );
  it.each(["0.0.44", "0.0.44-pr.11.2", "0.0.44-preview.20261002.11.2.extra"])(
    "excludes non-preview version %s",
    (version) => {
      expect(bundlesJonesNativeHelper("mac", version, false)).toBe(false);
    },
  );
});
