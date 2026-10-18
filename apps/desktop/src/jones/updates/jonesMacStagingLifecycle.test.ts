// @effect-diagnostics nodeBuiltinImport:off - Real isolated stage files with injected native mount/copy boundaries; never mounts a DMG or launches an app.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { stageJonesMacApp } from "./jonesMacStaging.ts";

const syncFailure = vi.hoisted(() => ({ enabled: false }));

vi.mock("node:child_process", async (original) => ({
  ...(await original<typeof import("node:child_process")>()),
  execFile: vi.fn(),
}));
vi.mock("@t3tools/shared/jones/jonesActions", async (original) => ({
  ...(await original<typeof import("@t3tools/shared/jones/jonesActions")>()),
  // Archive qualification is covered by jonesActions.test; this fixture starts at native staging.
  validateJonesStagedArtifact: vi.fn(async () => undefined),
}));
vi.mock("node:fs", async (original) => {
  const actual = await original<typeof import("node:fs")>();
  return {
    ...actual,
    promises: {
      ...actual.promises,
      open: async (...args: Parameters<typeof actual.promises.open>) => {
        const handle = await actual.promises.open(...args);
        const sync = handle.sync.bind(handle);
        handle.sync = async () => {
          if (syncFailure.enabled && typeof args[0] === "string" && args[0].includes(".app/")) {
            throw new Error("Injected app payload sync failure");
          }
          await sync();
        };
        return handle;
      },
    },
  };
});
vi.mock("node:fs/promises", async (original) => {
  const actual = await original<typeof import("node:fs/promises")>();
  return {
    ...actual,
    readFile: (
      path: Parameters<typeof actual.readFile>[0],
      options: Parameters<typeof actual.readFile>[1],
    ) =>
      actual.readFile(
        typeof path === "string" && path.endsWith("/app.asar/package.json")
          ? path.replace(/app\.asar\/package\.json$/, "package.fixture.json")
          : path,
        options,
      ),
  };
});

afterEach(() => {
  vi.mocked(NodeChildProcess.execFile).mockReset();
  syncFailure.enabled = false;
});

async function fixture(
  run: (input: {
    root: string;
    artifact: JonesStagedArtifact;
    fail: (phase: "attach" | "copy" | "metadata" | "sync" | "detach" | undefined) => void;
  }) => Promise<void>,
) {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-mac-stage-test-"));
  try {
    const root = await NodeFSP.realpath(allocated);
    const source = "b".repeat(40);
    const tree = "c".repeat(40);
    const version = "1.0.0-preview.20261010.11.1";
    const artifact: JonesStagedArtifact = {
      schema: 1,
      source: "jones-actions",
      channel: "jones-main",
      stagedHandle: "d".repeat(64),
      payloadPath: NodePath.join(root, "qualified.dmg"),
      candidate: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source,
        tree,
        installedSource: "a".repeat(40),
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        workflowId: 1,
        runId: 11,
        runAttempt: 1,
        ciRunId: 12,
        artifactId: 13,
        artifactName: "fixture",
        artifactDigest: "e".repeat(64),
        artifactBytes: 10,
        expiresAt: "2030-01-01T00:00:00Z",
        version,
        platform: "darwin",
        architecture: "arm64",
      },
      receipt: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source,
        tree,
        workflow: ".github/workflows/artifact-desktop-mac.yml",
        event: "push",
        ref: "refs/heads/main",
        runId: "11",
        runAttempt: "1",
        version,
        platform: "darwin",
        architecture: "arm64",
        artifact: "qualified.dmg",
        sha256: "f".repeat(64),
      },
    };
    const stageRoot = NodePath.join(root, "apps");
    await NodeFSP.mkdir(stageRoot);
    let fail: "attach" | "copy" | "metadata" | "sync" | "detach" | undefined;
    vi.mocked(NodeChildProcess.execFile).mockImplementation(
      (command, rawArgs, _options, callback) => {
        if (typeof callback !== "function") throw new Error("Missing native callback.");
        const args = rawArgs as string[];
        void (async () => {
          if (command === "/usr/bin/hdiutil" && args[0] === "attach") {
            if (fail === "attach") throw new Error("Injected attach failure");
            const mount = args[args.indexOf("-mountpoint") + 1]!;
            const app = NodePath.join(mount, "Candidate.app");
            await NodeFSP.mkdir(NodePath.join(app, "Contents", "MacOS"), { recursive: true });
            await NodeFSP.mkdir(NodePath.join(app, "Contents", "Resources"));
            await NodeFSP.writeFile(NodePath.join(app, "Contents", "MacOS", "Jones"), "executable");
            await NodeFSP.writeFile(
              NodePath.join(app, "Contents", "Resources", "app.asar"),
              "archive",
            );
            await NodeFSP.writeFile(
              NodePath.join(app, "Contents", "Resources", "package.fixture.json"),
              JSON.stringify({
                version,
                startupGateProtocol: 1,
                jonesSource: { repository: "Jones-Systems/Jones-Code", sha: source, tree },
              }),
            );
            return "";
          }
          if (command === "/usr/bin/ditto") {
            await NodeFSP.cp(args[0]!, args[1]!, { recursive: true });
            if (fail === "copy") throw new Error("Injected interrupted copy");
            return "";
          }
          if (command === "/usr/bin/plutil") {
            if (fail === "metadata") throw new Error("Injected validation failure");
            return JSON.stringify({
              CFBundleExecutable: "Jones",
              CFBundleIdentifier: "com.jones.code",
              CFBundleShortVersionString: version,
            });
          }
          if (command === "/usr/bin/lipo") return "arm64";
          if (command === "/usr/bin/hdiutil" && args[0] === "detach") {
            if (fail === "detach") throw new Error("Injected detach failure");
            await NodeFSP.rm(args[1]!, { recursive: true });
            return "";
          }
          throw new Error(`Unexpected native command ${command}`);
        })().then(
          (stdout) => callback(null, stdout, ""),
          (cause: Error) => callback(cause, "", ""),
        );
        return {} as ReturnType<typeof NodeChildProcess.execFile>;
      },
    );
    await run({
      root: stageRoot,
      artifact,
      fail: (phase) => {
        fail = phase;
        syncFailure.enabled = phase === "sync";
      },
    });
  } finally {
    await NodeFSP.rm(allocated, { recursive: true, force: true });
    await expect(NodeFSP.lstat(allocated)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

it.each(["attach", "copy", "metadata", "sync", "detach"] as const)(
  "retries after %s failure without selecting or overwriting the incomplete attempt",
  (phase) =>
    fixture(async ({ root, artifact, fail }) => {
      fail(phase);
      await expect(stageJonesMacApp(artifact, root, "darwin")).rejects.toThrow();
      await expect(NodeFSP.lstat(NodePath.join(root, "completed"))).rejects.toMatchObject({
        code: "ENOENT",
      });
      const retained = await NodeFSP.readdir(NodePath.join(root, "attempts"));
      expect(retained).toHaveLength(1);
      fail(undefined);
      const result = await stageJonesMacApp(artifact, root, "darwin");
      expect(result.handle).toBe(artifact.stagedHandle);
      expect(result.startupGateProtocol).toBe(1);
      expect(result.appPath).not.toContain(`${retained[0]}/`);
      expect(await NodeFSP.readdir(NodePath.join(root, "attempts"))).toHaveLength(2);
      const commandCount = vi.mocked(NodeChildProcess.execFile).mock.calls.length;
      expect(await stageJonesMacApp(artifact, root, "darwin")).toEqual(result);
      expect(vi.mocked(NodeChildProcess.execFile).mock.calls).toHaveLength(commandCount);
    }),
);
