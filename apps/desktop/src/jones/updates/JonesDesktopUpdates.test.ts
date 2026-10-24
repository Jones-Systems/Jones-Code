// @effect-diagnostics nodeBuiltinImport:off - Source-qualified synthetic desktop fixture, never launches a native app.
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { DesktopUpdateState } from "@t3tools/contracts";
import type {
  JonesActionsCandidate,
  JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import {
  JonesDesktopUpdateController,
  type JonesDesktopUpdateOptions,
} from "./JonesDesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../../updates/updateMachine.ts";
import { hashMacApp, hashMacFile, preflightJonesCandidateStartupGate } from "./jonesMacStaging.ts";
import { prepareJonesStage } from "./stagingJobs.ts";

vi.mock("node:child_process", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:child_process")>();
  return { ...original, execFile: vi.fn(original.execFile), spawn: vi.fn(original.spawn) };
});

vi.mock("./jonesMacStaging.ts", async (importOriginal) => {
  const original = await importOriginal<typeof import("./jonesMacStaging.ts")>();
  return {
    ...original,
    preflightJonesCandidateStartupGate: vi.fn(original.preflightJonesCandidateStartupGate),
  };
});

const candidate: JonesActionsCandidate = {
  schema: 1,
  repository: "Jones-Systems/Jones-Code",
  source: "b".repeat(40),
  tree: "c".repeat(40),
  installedSource: "a".repeat(40),
  workflow: ".github/workflows/artifact-desktop-mac.yml",
  workflowId: 10,
  runId: 11,
  runAttempt: 1,
  ciRunId: 12,
  artifactId: 13,
  artifactName: "qualified-mac",
  artifactDigest: "d".repeat(64),
  artifactBytes: 100,
  expiresAt: "2030-01-01T00:00:00Z",
  version: "0.0.44-preview.20261002.11.1",
  platform: "darwin",
  architecture: "arm64",
};
const handle = "e".repeat(64);
const candidateBuildStartedAt = "2026-10-20T21:13:43Z";

/** The actual helper durably writes the claim before it reports preparation-claimed. */
function claimedPreparation(home: string, transactionId: unknown): string {
  if (typeof transactionId !== "string") throw new Error("Missing attempt ID.");
  NodeFS.writeFileSync(
    NodePath.join(
      home,
      "runtime",
      "jones-updates",
      "transactions",
      transactionId,
      "prepare-intent.json",
    ),
    JSON.stringify({ transactionId, stagedHandle: handle }),
  );
  return JSON.stringify({
    protocol: 1,
    operation: "claim-preparation",
    handle,
    transactionId,
    status: "preparation-claimed",
  });
}

const fixtures = new Set<string>();
async function cleanupFixture(root: string): Promise<void> {
  await NodeFSP.rm(root, { recursive: true, force: true });
  await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  fixtures.delete(root);
}
afterEach(async () => {
  for (const root of fixtures) await cleanupFixture(root);
});

async function fixture(
  bootstrap = true,
  terminal?: "committed" | "rolled-back",
  attemptLayout = false,
  buildMetadata: Record<string, unknown> = {},
) {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const home = await NodeFSP.mkdtemp(NodePath.join(parent, "desktop-controller-"));
  fixtures.add(home);
  const app = NodePath.join(home, "Previous.app"),
    profile = NodePath.join(home, "profile");
  await NodeFSP.mkdir(app);
  await NodeFSP.mkdir(profile);
  await NodeFSP.mkdir(NodePath.join(home, "userdata"));
  await NodeFSP.mkdir(NodePath.join(home, "runtime"));
  await NodeFSP.writeFile(NodePath.join(profile, "opaque"), "same-host-profile");
  const databasePath = NodePath.join(home, "userdata", "state.sqlite");
  await NodeFSP.writeFile(databasePath, "live-state");
  const executable = NodePath.join(app, "Jones");
  await NodeFSP.writeFile(executable, "native-executable");
  const version = "0.0.44-preview.20261002.10";
  await NodeFSP.writeFile(
    NodePath.join(app, "package.json"),
    JSON.stringify({
      version,
      ...buildMetadata,
      jonesSource: {
        repository: candidate.repository,
        sha: candidate.installedSource,
        tree: "f".repeat(40),
      },
    }),
  );
  const active = {
    protocol: 1,
    owner: "desktop",
    generation: terminal === "committed" ? handle : "previous-generation",
    transactionId: terminal === "committed" ? handle : "bootstrap",
    home,
    databasePath,
    profile,
    environmentId: "native-environment",
    appPath: app,
    executablePath: executable,
    version,
    sourceSha: candidate.installedSource,
    sourceTree: "f".repeat(40),
    appDigest: await hashMacApp(app),
  };
  await NodeFSP.writeFile(
    NodePath.join(home, "runtime", "jones-active-install.json"),
    JSON.stringify(active),
  );
  if (terminal !== undefined) {
    const transaction = NodePath.join(home, "runtime", "jones-updates", "transactions", handle);
    await NodeFSP.mkdir(transaction, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(transaction, "journal.json"),
      JSON.stringify({
        phase: terminal === "committed" ? "resumed" : "rolled-back",
        intent: {
          protocol: 1,
          transactionId: handle,
          expected:
            terminal === "rolled-back"
              ? active
              : {
                  ...active,
                  generation: "prior",
                  transactionId: "prior",
                  sourceSha: "0".repeat(40),
                },
          continuationReceipt: NodePath.join(transaction, "continuation.json"),
          staged: {
            handle,
            receiptPath: NodePath.join(transaction, "app-receipt.json"),
            appPath: active.appPath,
            executablePath: active.executablePath,
            version: active.version,
            sourceSha: active.sourceSha,
            sourceTree: active.sourceTree,
            appDigest: active.appDigest,
            asarDigest: "1".repeat(64),
            executableDigest: "2".repeat(64),
          },
        },
      }),
    );
  }
  const selections = [
    candidate,
    {
      ...candidate,
      source: "f".repeat(40),
      artifactId: 99,
      version: "0.0.44-preview.20261002.99.1",
    },
  ];
  let checks = 0,
    stages = 0;
  let ownedArtifact: JonesStagedArtifact | undefined;
  const options: JonesDesktopUpdateOptions = {
    home,
    appRoot: app,
    appPath: app,
    executablePath: executable,
    profile,
    activeGeneration: bootstrap ? active.generation : undefined,
    architecture: "arm64",
    platform: "darwin",
    disabledByEnv: false,
    initialState: createInitialDesktopUpdateState(
      version,
      { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
      "latest",
    ),
    onState: async () => {},
    processProofs: async () => [],
    listener: async () => "http://127.0.0.1:3777",
    timestamp: async () => "2026-10-02T00:00:00Z",
    readInstalledBundleIdentity: async () => {
      let bundleIdentifier = "com.t3tools.t3code";
      try {
        const xml = await NodeFSP.readFile(NodePath.join(app, "Contents", "Info.plist"), "utf8");
        bundleIdentifier =
          xml.match(/<key>CFBundleIdentifier<\/key>\s*<string>([^<]+)<\/string>/)?.[1] ?? "";
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
      }
      return { bundleIdentifier, version, executablePath: executable };
    },
    client: {
      check: async () => {
        const index = Math.min(checks++, 1);
        return {
          state: "available",
          candidate: selections[index]!,
          ...(index === 0 ? { buildStartedAt: candidateBuildStartedAt } : {}),
        };
      },
      stage: async (selection) => {
        stages++;
        const artifact: JonesStagedArtifact = {
          schema: 1,
          source: "jones-actions",
          channel: "jones-main",
          stagedHandle: handle,
          candidate: selection,
          payloadPath: NodePath.join(
            home,
            "runtime",
            "jones-updates",
            "artifacts",
            ...(attemptLayout ? ["attempts", "stage-artifact"] : []),
            handle,
            "qualified.dmg",
          ),
          receipt: {
            schema: 1,
            repository: selection.repository,
            source: selection.source,
            tree: selection.tree,
            workflow: selection.workflow,
            event: "push",
            ref: "refs/heads/main",
            runId: String(selection.runId),
            runAttempt: String(selection.runAttempt),
            version: selection.version,
            platform: "darwin",
            architecture: "arm64",
            artifact: "qualified.dmg",
            sha256: "d".repeat(64),
          },
        };
        await NodeFSP.mkdir(NodePath.dirname(artifact.payloadPath), { recursive: true });
        ownedArtifact = artifact;
        return artifact;
      },
    },
    stageApp: async (artifact, stageRoot) => {
      const appPath = NodePath.join(
        stageRoot,
        ...(attemptLayout ? ["attempts", "stage-native"] : []),
        handle,
        "Candidate.app",
      );
      const executablePath = NodePath.join(appPath, "Contents", "MacOS", "Jones");
      const asarPath = NodePath.join(appPath, "Contents", "Resources", "app.asar");
      await NodeFSP.mkdir(NodePath.dirname(executablePath), { recursive: true });
      await NodeFSP.mkdir(NodePath.dirname(asarPath));
      await NodeFSP.writeFile(executablePath, "staged-native-executable");
      await NodeFSP.writeFile(asarPath, "staged-asar");
      const staged = {
        handle,
        receiptPath: NodePath.join(NodePath.dirname(appPath), "mac-app-receipt.json"),
        appPath,
        executablePath,
        version: artifact.candidate.version,
        sourceSha: artifact.candidate.source,
        sourceTree: artifact.candidate.tree,
        appDigest: await hashMacApp(appPath),
        asarDigest: await hashMacFile(asarPath),
        executableDigest: await hashMacFile(executablePath),
      };
      await NodeFSP.writeFile(
        staged.receiptPath,
        JSON.stringify({ app: staged, artifact, candidate: artifact.candidate }),
      );
      return staged;
    },
    validateArtifact: async (directory) => {
      if (ownedArtifact === undefined || NodePath.dirname(ownedArtifact.payloadPath) !== directory)
        throw new Error("Unknown artifact fixture.");
      return ownedArtifact;
    },
  };
  const controller = new JonesDesktopUpdateController(options);
  return {
    home,
    app,
    active,
    options,
    profile,
    databasePath,
    controller,
    restart: () => new JonesDesktopUpdateController(options),
    stages: () => stages,
    cleanup: () => cleanupFixture(home),
  };
}

describe("Jones desktop updates", () => {
  it.each([
    { metadata: { jonesBuildStartedAt: "2026-10-20T20:13:43Z" }, expected: "2026-10-20T20:13:43Z" },
    { metadata: {}, expected: undefined },
    { metadata: { jonesBuildStartedAt: "invalid" }, expected: undefined },
    {
      metadata: { version: "another-build", jonesBuildStartedAt: "2026-10-20T20:13:43Z" },
      expected: undefined,
    },
  ])(
    "pairs installed build time with the running version: $metadata",
    async ({ metadata, expected }) => {
      const f = await fixture(true, undefined, false, metadata);
      try {
        await f.controller.configure();
        expect(f.controller.state.jones?.currentVersion).toBe(
          f.options.initialState.currentVersion,
        );
        expect(f.controller.state.jones?.currentBuildStartedAt).toBe(expected);
        expect(f.controller.state.jones?.capability.check).toBe(true);
        const manifest = NodePath.join(f.app, "package.json");
        const content = JSON.parse(await NodeFSP.readFile(manifest, "utf8"));
        delete content.jonesBuildStartedAt;
        await NodeFSP.writeFile(manifest, JSON.stringify(content));
        await f.controller.configure();
        expect(f.controller.state.jones?.currentBuildStartedAt).toBeUndefined();
      } finally {
        await f.cleanup();
      }
    },
  );
  it("clears an offered time when a replacement candidate has no recorded time", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      await f.controller.check();
      expect(f.controller.state.jones?.provenance?.buildStartedAt).toBe(candidateBuildStartedAt);
      await f.controller.check();
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(99);
      expect(f.controller.state.jones?.provenance?.buildStartedAt).toBeUndefined();
      await f.controller.download();
      expect(f.controller.state.jones?.provenance?.buildStartedAt).toBeUndefined();
      expect(candidate).not.toHaveProperty("buildStartedAt");
    } finally {
      await f.cleanup();
    }
  });
  it("re-adopts a mismatched stable bundle and leaves Check, Download and Install usable", async () => {
    const f = await fixture(false);
    const nativeCommand = vi.mocked(NodeChildProcess.execFile);
    try {
      const contents = NodePath.join(f.app, "Contents");
      const executable = NodePath.join(contents, "MacOS", "Jones");
      const infoPlist = NodePath.join(contents, "Info.plist");
      nativeCommand.mockImplementation((command, args, options, callback) => {
        if (args?.[3] === "--reconcile-running") {
          const requestPath = args[4];
          if (typeof requestPath !== "string" || typeof callback !== "function")
            throw new Error("Missing recovery request.");
          void (async () => {
            const request = JSON.parse(await NodeFSP.readFile(requestPath, "utf8"));
            const file = NodePath.join(f.home, "runtime", "jones-active-install.json");
            const old = await NodeFSP.readFile(file, "utf8");
            await NodeFSP.writeFile(file + ".superseded", old);
            await NodeFSP.writeFile(file, JSON.stringify(request.observed));
            callback(
              null,
              JSON.stringify({ protocol: 1, status: "reconciled", active: request.observed }),
              "",
            );
          })().catch((cause) => callback(cause, "", ""));
          return new NodeChildProcess.ChildProcess();
        }
        expect(command).toBe("/usr/bin/plutil");
        expect(args).toEqual(["-convert", "json", "-o", "-", infoPlist]);
        expect(options).toEqual({ encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 120000 });
        if (typeof callback !== "function")
          throw new Error("The plist command requires a callback.");
        void NodeFSP.readFile(infoPlist, "utf8").then(
          (xml) => {
            const info = Object.fromEntries(
              [...xml.matchAll(/<key>([^<]+)<\/key>\s*<string>([^<]+)<\/string>/g)].map((match) => [
                match[1],
                match[2],
              ]),
            );
            callback(null, JSON.stringify(info), "");
          },
          (cause: NodeChildProcess.ExecFileException) => callback(cause, "", ""),
        );
        return new NodeChildProcess.ChildProcess();
      });
      await NodeFSP.mkdir(NodePath.dirname(executable), { recursive: true });
      await NodeFSP.rename(f.active.executablePath, executable);
      await NodeFSP.writeFile(
        infoPlist,
        `<?xml version="1.0"?><plist version="1.0"><dict>
        <key>CFBundleExecutable</key><string>Jones</string>
        <key>CFBundleIdentifier</key><string>com.jones.code</string>
        <key>CFBundleShortVersionString</key><string>${f.active.version}</string>
      </dict></plist>`,
      );
      await NodeFSP.rename(f.databasePath, NodePath.join(f.home, "userdata", "statev2.sqlite"));
      await NodeFSP.writeFile(
        NodePath.join(f.home, "userdata", "environment-id"),
        "native-environment\n",
      );
      const stale = JSON.stringify({ ...f.active, sourceSha: "0".repeat(40) });
      await NodeFSP.writeFile(NodePath.join(f.home, "runtime", "jones-active-install.json"), stale);
      // An escaping payload link would fail a complete app hash. Configure reads only bound metadata.
      await NodeFSP.symlink(f.home, NodePath.join(f.app, "opaque-link"));
      const controller = new JonesDesktopUpdateController({
        ...f.options,
        executablePath: executable,
        stageApp: async (artifact, stageRoot, platform) => {
          const staged = {
            ...(await f.options.stageApp!(artifact, stageRoot, platform)),
            startupGateProtocol: 1 as const,
          };
          await NodeFSP.writeFile(
            staged.receiptPath,
            JSON.stringify({ app: staged, artifact, candidate: artifact.candidate }),
          );
          return staged;
        },
      });
      await controller.configure();
      expect(controller.state.jones?.capability.install).toBe(true);
      const active = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(f.home, "runtime", "jones-active-install.json"),
          "utf8",
        ),
      );
      expect(active.bundleIdentifier).toBe("com.jones.code");
      expect(active.sourceSha).toBe(candidate.installedSource);
      expect(active.databasePath).toBe(NodePath.join(f.home, "userdata", "statev2.sqlite"));
      const restarted = new JonesDesktopUpdateController({
        ...f.options,
        executablePath: executable,
      });
      await restarted.configure();
      expect(restarted.state.jones?.capability.install).toBe(true);
      expect(await NodeFSP.readFile(controller.manifestPath + ".superseded", "utf8")).toBe(stale);
      await controller.check();
      expect(await controller.download()).toEqual({ accepted: true, completed: true });
      expect(controller.state.jones?.capability).toEqual({
        check: true,
        download: true,
        install: true,
      });
      expect(await NodeFSP.readFile(active.databasePath, "utf8")).toBe("live-state");
      const preflight = vi.mocked(preflightJonesCandidateStartupGate);
      const spawn = vi.mocked(NodeChildProcess.spawn);
      let transactionId = "";
      const installing = new JonesDesktopUpdateController({
        ...f.options,
        executablePath: executable,
        prepareNative: async (stagedHandle, id) => {
          transactionId = id;
          await NodeFSP.writeFile(
            NodePath.join(controller.updaterRoot, "transactions", id, "continuation.json"),
            JSON.stringify({
              prepared: true,
              preparationClaimProtocol: 3,
              stagedHandle,
              transactionId: id,
            }),
          );
        },
      });
      await installing.configure();
      preflight.mockResolvedValueOnce(undefined);
      nativeCommand.mockImplementation((_command, args, _options, callback) => {
        if (typeof callback !== "function") throw new Error("Missing callback.");
        if (args?.[0] === "--version") callback(null, "Python3", "");
        else if (args?.[3] === "--claim-preparation")
          callback(null, claimedPreparation(f.home, args[6]), "");
        else
          callback(
            null,
            JSON.stringify({
              protocol: 1,
              operation: "claim-activation",
              handle,
              transactionId,
              status: "claimed",
              intentPath: NodePath.join(
                controller.updaterRoot,
                "transactions",
                transactionId,
                "intent.json",
              ),
            }),
            "",
          );
        return new NodeChildProcess.ChildProcess();
      });
      spawn.mockImplementation(() => {
        const child = new NodeChildProcess.ChildProcess();
        void Promise.resolve().then(() => child.emit("spawn"));
        return child;
      });
      try {
        expect(await installing.install(handle)).toMatchObject({ accepted: true, failed: false });
      } finally {
        preflight.mockReset();
        spawn.mockReset();
      }
    } finally {
      nativeCommand.mockReset();
      await f.cleanup();
    }
  });

  it.each([
    [
      Object.assign(new Error("secret /private/download"), { code: "ENOSPC" }),
      "not enough disk space",
    ],
    [
      new Error("secret https://token@example.test", {
        cause: Object.assign(new Error("private"), { code: "EACCES" }),
      }),
      "file access denied",
    ],
    [new Error("Digest mismatch at /private/token"), "artifact integrity check failed"],
    [new Error("private unclassified credential"), "artifact staging failed"],
  ])("shows a safe download cause for %s", async (cause, category) => {
    const f = await fixture();
    try {
      const controller = new JonesDesktopUpdateController({
        ...f.options,
        client: {
          ...f.options.client!,
          stage: async () => {
            throw cause;
          },
        },
      });
      await controller.configure();
      await controller.check();
      expect(await controller.download()).toEqual({ accepted: true, completed: false });
      expect(controller.state.message).toBe(
        `Download failed (${category}). Check for builds and retry; the active app is still running.`,
      );
    } finally {
      await f.cleanup();
    }
  });

  it("preserves an occupied manifest that does not bind the current source", async () => {
    const f = await fixture(false);
    try {
      const file = NodePath.join(f.home, "runtime", "jones-active-install.json");
      const occupied = JSON.stringify({ ...f.active, sourceSha: "0".repeat(40) });
      await NodeFSP.writeFile(file, occupied);
      await f.controller.configure();
      expect(f.controller.state.jones?.capability.install).toBe(false);
      expect(f.controller.state.jones?.phase).toBe("blocked");
      expect(f.controller.state.jones?.installedSource).toBeUndefined();
      expect(await NodeFSP.readFile(file, "utf8")).toBe(occupied);
    } finally {
      await f.cleanup();
    }
  });

  it("blocks native activation when its explicit fleet binding cannot persist", async () => {
    const f = await fixture();
    const nativeCommand = vi.mocked(NodeChildProcess.execFile);
    const preflight = vi.mocked(preflightJonesCandidateStartupGate);
    const campaignId = "11111111-1111-4111-8111-111111111111";
    const order: string[] = [];
    const fleet = {
      bindInstall: vi.fn(async () => {
        order.push("bind");
        throw new Error("storage unavailable");
      }),
      recordOutcome: vi.fn(),
    };
    try {
      const controller = new JonesDesktopUpdateController({
        ...f.options,
        fleet,
        prepareNative: async (selected, transactionId) => {
          order.push("prepare");
          expect(order).toEqual(["claim", "prepare"]);
          await NodeFSP.writeFile(
            NodePath.join(
              f.home,
              "runtime",
              "jones-updates",
              "transactions",
              transactionId,
              "continuation.json",
            ),
            JSON.stringify({
              prepared: true,
              preparationClaimProtocol: 3,
              stagedHandle: selected,
              transactionId,
            }),
          );
        },
      });
      await controller.configure();
      await controller.check();
      await controller.download();
      preflight.mockResolvedValueOnce(undefined);
      nativeCommand.mockImplementation((command, args, _options, callback) => {
        expect(command).toBe("/usr/bin/python3");
        if (typeof callback !== "function") throw new Error("Missing callback.");
        if (args?.[0] === "--version") callback(null, "Python 3", "");
        else {
          expect(args?.[3]).toBe("--claim-preparation");
          expect(args?.[4]).toBe(handle);
          expect(args).toContain("--withdrawable");
          order.push("claim");
          callback(null, claimedPreparation(f.home, args?.[6]), "");
        }
        return new NodeChildProcess.ChildProcess();
      });
      expect(await controller.install(handle, campaignId)).toMatchObject({
        accepted: false,
        completed: false,
        failed: true,
      });
      expect(fleet.bindInstall).toHaveBeenCalledExactlyOnceWith({
        campaignId,
        stagedHandle: handle,
        targetSource: candidate.source,
        transactionId: expect.stringMatching(/^[a-f0-9]{64}$/),
        fromGeneration: f.active.generation,
      });
      const transactionId = nativeCommand.mock.calls[1]?.[1]?.[6];
      if (typeof transactionId !== "string") throw new Error("Missing attempt ID.");
      expect(transactionId).not.toBe(handle);
      await expect(
        NodeFSP.lstat(
          NodePath.join(
            f.home,
            "runtime",
            "jones-updates",
            "transactions",
            transactionId,
            "intent.json",
          ),
        ),
      ).rejects.toMatchObject({ code: "ENOENT" });
      expect(nativeCommand).toHaveBeenCalledTimes(2);
      expect(order).toEqual(["claim", "prepare", "bind"]);
      expect(controller.state.jones?.capability.reason).toBe("blocked");
      expect(await controller.install(handle, campaignId)).toMatchObject({
        accepted: false,
        failed: false,
      });
      expect(nativeCommand).toHaveBeenCalledTimes(2);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
    } finally {
      preflight.mockReset();
      nativeCommand.mockReset();
      await f.cleanup();
    }
  });

  it.each([
    "selection-mismatch",
    "activation-pending",
    "unknown-state",
    "busy",
    "uncertain",
    "malformed",
    "wrong-handle",
    "wrong-transaction",
    "wrong-operation",
    "transport",
  ] as const)("does not prepare the backend after a %s preparation claim", async (outcome) => {
    const f = await fixture();
    const nativeCommand = vi.mocked(NodeChildProcess.execFile);
    const preflight = vi.mocked(preflightJonesCandidateStartupGate);
    const prepareNative = vi.fn();
    try {
      const controller = new JonesDesktopUpdateController({ ...f.options, prepareNative });
      await controller.configure();
      await controller.check();
      await controller.download();
      preflight.mockResolvedValueOnce(undefined);
      nativeCommand.mockImplementation((command, args, _options, callback) => {
        expect(command).toBe("/usr/bin/python3");
        if (typeof callback !== "function") throw new Error("Missing callback.");
        if (args?.[0] === "--version") callback(null, "Python 3", "");
        else {
          expect(args?.[3]).toBe("--claim-preparation");
          expect(args?.[4]).toBe(handle);
          expect(args?.[5]).toBe("--transaction");
          expect(args?.[7]).toBe("--selection");
          expect(args?.[9]).toBe("--selection-sha256");
          if (outcome === "transport") callback(new Error("helper response lost"), "", "");
          else if (outcome === "malformed") callback(null, "invalid JSON", "");
          else
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation: outcome === "wrong-operation" ? "claim-activation" : "claim-preparation",
                handle: outcome === "wrong-handle" ? "different" : handle,
                transactionId: outcome === "wrong-transaction" ? "0".repeat(64) : args?.[6],
                status:
                  outcome === "uncertain"
                    ? "uncertain"
                    : outcome === "wrong-handle"
                      ? "preparation-claimed"
                      : "refused",
                reason: outcome,
              }),
              "",
            );
        }
        return new NodeChildProcess.ChildProcess();
      });
      expect(await controller.install(handle)).toMatchObject({
        accepted: false,
        completed: false,
        failed: true,
      });
      expect(prepareNative).not.toHaveBeenCalled();
      expect(nativeCommand).toHaveBeenCalledTimes(2);
      const claimArgs = nativeCommand.mock.calls[1]?.[1];
      const selectionPath = claimArgs?.[8];
      if (typeof selectionPath !== "string") throw new Error("Missing fixed selection path.");
      expect(claimArgs?.[10]).toBe(
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(selectionPath, "utf8"))
          .digest("hex"),
      );
      expect(controller.state.jones?.stagedHandle).toBe(handle);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      if (outcome === "selection-mismatch") {
        expect(controller.state.jones?.capability.reason).not.toBe("blocked");
        expect(controller.state.jones?.updateId).toBeUndefined();
      } else {
        expect(controller.state.jones?.capability.reason).toBe("blocked");
        expect(controller.state.jones?.updateId).toBe(claimArgs?.[6]);
        expect(await controller.install(handle)).toMatchObject({ accepted: false, failed: false });
        expect(nativeCommand).toHaveBeenCalledTimes(2);
      }
    } finally {
      preflight.mockReset();
      nativeCommand.mockReset();
      await f.cleanup();
    }
  });

  it.each(["claimed", "refused"] as const)(
    "publishes the prepared attempt ID while preserving its stage when activation is %s",
    async (claimStatus) => {
      const f = await fixture();
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      const nativeSpawn = vi.mocked(NodeChildProcess.spawn);
      const preflight = vi.mocked(preflightJonesCandidateStartupGate);
      const states: DesktopUpdateState[] = [];
      let transactionId = "";
      try {
        const controller = new JonesDesktopUpdateController({
          ...f.options,
          onState: async (state) => {
            states.push(state);
          },
          prepareNative: async (stagedHandle, selectedTransaction, active) => {
            expect(stagedHandle).toBe(handle);
            expect(selectedTransaction).toBe(transactionId);
            expect(active).toEqual(f.active);
            await NodeFSP.writeFile(
              NodePath.join(
                f.home,
                "runtime",
                "jones-updates",
                "transactions",
                transactionId,
                "continuation.json",
              ),
              JSON.stringify({
                prepared: true,
                preparationClaimProtocol: 3,
                stagedHandle,
                transactionId,
              }),
            );
          },
        });
        await controller.configure();
        await controller.check();
        await controller.download();
        preflight.mockResolvedValueOnce(undefined);
        nativeCommand.mockImplementation((_command, args, _options, callback) => {
          if (typeof callback !== "function") throw new Error("Missing callback.");
          if (args?.[0] === "--version") callback(null, "Python 3", "");
          else if (args?.[3] === "--claim-preparation") {
            const selected = args?.[6];
            if (typeof selected !== "string") throw new Error("Missing attempt ID.");
            transactionId = selected;
            callback(null, claimedPreparation(f.home, transactionId), "");
          } else {
            expect(args?.[3]).toBe("--claim-activation");
            expect(args?.[4]).toBe(
              NodePath.join(
                f.home,
                "runtime",
                "jones-updates",
                "transactions",
                transactionId,
                "activation-request.json",
              ),
            );
            expect(args?.[5]).toBe("--staged-handle");
            expect(args?.[6]).toBe(handle);
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation: "claim-activation",
                handle,
                transactionId,
                status: claimStatus,
                ...(claimStatus === "refused"
                  ? { reason: "busy" }
                  : {
                      intentPath: NodePath.join(
                        f.home,
                        "runtime",
                        "jones-updates",
                        "transactions",
                        transactionId,
                        "intent.json",
                      ),
                    }),
              }),
              "",
            );
          }
          return new NodeChildProcess.ChildProcess();
        });
        nativeSpawn.mockImplementation(() => {
          const child = new NodeChildProcess.ChildProcess();
          void Promise.resolve().then(() => child.emit("spawn"));
          return child;
        });
        expect(await controller.install(handle)).toMatchObject({
          failed: claimStatus === "refused",
          accepted: claimStatus === "claimed",
        });
        expect(nativeCommand).toHaveBeenCalledTimes(3);
        expect(transactionId).not.toBe(handle);
        const preparing = states.filter((state) => state.jones?.phase === "preparing");
        expect(preparing).toHaveLength(1);
        expect(preparing[0]?.jones).toMatchObject({
          updateId: transactionId,
          stagedHandle: handle,
        });
        const installing = states.filter((state) => state.jones?.phase === "installing");
        expect(installing).toHaveLength(claimStatus === "claimed" ? 1 : 0);
        if (claimStatus === "claimed")
          expect(installing[0]?.jones).toMatchObject({
            updateId: transactionId,
            stagedHandle: handle,
          });
        expect(controller.state.jones).toMatchObject({
          phase: claimStatus === "claimed" ? "installing" : "blocked",
          updateId: transactionId,
          stagedHandle: handle,
        });
        expect(nativeSpawn).toHaveBeenCalledTimes(claimStatus === "claimed" ? 1 : 0);
        const request = JSON.parse(
          await NodeFSP.readFile(
            NodePath.join(
              f.home,
              "runtime",
              "jones-updates",
              "transactions",
              transactionId,
              "activation-request.json",
            ),
            "utf8",
          ),
        );
        expect(request).toMatchObject({ transactionId, staged: { handle }, expected: f.active });
        expect(request.continuationReceipt).toBe(
          NodePath.join(
            f.home,
            "runtime",
            "jones-updates",
            "transactions",
            transactionId,
            "continuation.json",
          ),
        );
      } finally {
        preflight.mockReset();
        nativeCommand.mockReset();
        nativeSpawn.mockReset();
        await f.cleanup();
      }
    },
  );

  it("reuses the persisted attempt after a lost helper response and controller recreation", async () => {
    const f = await fixture();
    const nativeCommand = vi.mocked(NodeChildProcess.execFile);
    const preflight = vi.mocked(preflightJonesCandidateStartupGate);
    const attempts: string[] = [];
    const prepareNative = vi.fn();
    try {
      await f.controller.configure();
      await f.controller.check();
      await f.controller.download();
      preflight.mockResolvedValue(undefined);
      nativeCommand.mockImplementation((_command, args, _options, callback) => {
        if (typeof callback !== "function") throw new Error("Missing callback.");
        if (args?.[0] === "--version") callback(null, "Python 3", "");
        else {
          expect(args?.[3]).toBe("--claim-preparation");
          const transactionId = args?.[6];
          if (typeof transactionId !== "string") throw new Error("Missing attempt ID.");
          attempts.push(transactionId);
          callback(new Error("response lost"), "", "");
        }
        return new NodeChildProcess.ChildProcess();
      });
      expect(await f.controller.install(handle)).toMatchObject({ failed: true });
      const directory = NodePath.join(f.home, "runtime", "jones-updates", "attempt-selections");
      const files = await NodeFSP.readdir(directory);
      expect(files).toHaveLength(1);
      const recordPath = NodePath.join(directory, files[0]!);
      const before = await NodeFSP.readFile(recordPath, "utf8");
      const restarted = new JonesDesktopUpdateController({ ...f.options, prepareNative });
      await restarted.configure();
      expect(await restarted.install(handle)).toMatchObject({ failed: true });
      expect(attempts).toHaveLength(2);
      expect(attempts[1]).toBe(attempts[0]);
      expect(attempts[0]).not.toBe(handle);
      expect(await NodeFSP.readFile(recordPath, "utf8")).toBe(before);
      expect(await NodeFSP.readdir(directory)).toEqual(files);
      expect(prepareNative).not.toHaveBeenCalled();
      expect(f.stages()).toBe(1);
    } finally {
      preflight.mockReset();
      nativeCommand.mockReset();
      await f.cleanup();
    }
  });

  it.each(["busy", "selection-mismatch"] as const)(
    "uses a fresh attempt after rollback without leaking the old ID on %s",
    async (refusal) => {
      const f = await fixture(true, "rolled-back");
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      const preflight = vi.mocked(preflightJonesCandidateStartupGate);
      const journalPath = NodePath.join(
        f.home,
        "runtime",
        "jones-updates",
        "transactions",
        handle,
        "journal.json",
      );
      try {
        const journal = await NodeFSP.readFile(journalPath, "utf8");
        await f.controller.configure();
        expect(f.controller.state.jones?.updateId).toBe(handle);
        await f.controller.check();
        await f.controller.download();
        expect(f.controller.state.jones?.updateId).toBeUndefined();
        const appPath = NodePath.join(
          f.home,
          "runtime",
          "jones-updates",
          "apps",
          handle,
          "Candidate.app",
        );
        const receiptPath = NodePath.join(NodePath.dirname(appPath), "mac-app-receipt.json");
        const receipt = await NodeFSP.readFile(receiptPath, "utf8");
        const digest = await hashMacApp(appPath);
        preflight.mockResolvedValueOnce(undefined);
        nativeCommand.mockImplementation((_command, args, _options, callback) => {
          if (typeof callback !== "function") throw new Error("Missing callback.");
          if (args?.[0] === "--version") callback(null, "Python 3", "");
          else {
            expect(args?.[3]).toBe("--claim-preparation");
            expect(args?.[4]).toBe(handle);
            expect(args?.[6]).toMatch(/^[a-f0-9]{64}$/);
            expect(args?.[6]).not.toBe(handle);
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation: "claim-preparation",
                handle,
                transactionId: args?.[6],
                status: "refused",
                reason: refusal,
              }),
              "",
            );
          }
          return new NodeChildProcess.ChildProcess();
        });
        expect(await f.controller.install(handle)).toMatchObject({ failed: true });
        expect(nativeCommand).toHaveBeenCalledTimes(2);
        expect(f.controller.state.jones?.updateId).toBe(
          refusal === "selection-mismatch" ? undefined : nativeCommand.mock.calls[1]?.[1]?.[6],
        );
        expect(await NodeFSP.readFile(journalPath, "utf8")).toBe(journal);
        expect(await NodeFSP.readFile(receiptPath, "utf8")).toBe(receipt);
        expect(await hashMacApp(appPath)).toBe(digest);
        expect(f.stages()).toBe(1);
      } finally {
        preflight.mockReset();
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );

  it.each([
    { phase: "cold-receiver-prepared", fields: { kind: "cold-first-transition" } },
    { phase: "resumed", fields: { kind: "cold-first-transition" } },
    { phase: "rolled-back", fields: { kind: "cold-first-transition" } },
    { phase: "resumed", fields: { kind: "warm" } },
    { phase: "rolled-back", fields: { kind: null } },
    { phase: "resumed", fields: { handoffSha256: "a".repeat(64) } },
    { phase: "resumed", fields: { maintenanceGeneration: "a".repeat(64) } },
    { phase: "resumed", fields: { stateTransaction: {} } },
  ])(
    "holds receiver or mixed intent $fields in $phase without reporting success or launching",
    async ({ phase, fields }) => {
      const f = await fixture(true, "committed");
      const fleet = { bindInstall: vi.fn(), recordOutcome: vi.fn().mockResolvedValue(undefined) };
      const journalPath = NodePath.join(
        f.options.home,
        "runtime",
        "jones-updates",
        "transactions",
        handle,
        "journal.json",
      );
      const manifestPath = NodePath.join(f.options.home, "runtime", "jones-active-install.json");
      try {
        const journal = JSON.parse(await NodeFSP.readFile(journalPath, "utf8"));
        journal.phase = phase;
        Object.assign(journal.intent, fields);
        await NodeFSP.writeFile(journalPath, JSON.stringify(journal));
        const before = await NodeFSP.readFile(manifestPath, "utf8");
        const spawnCalls = vi.mocked(NodeChildProcess.spawn).mock.calls.length;
        const commandCalls = vi.mocked(NodeChildProcess.execFile).mock.calls.length;
        const controller = new JonesDesktopUpdateController({ ...f.options, fleet });
        await controller.configure();
        expect(controller.state.jones?.phase).toBe("blocked");
        expect(controller.state.jones?.capability.install).toBe(false);
        expect(fleet.recordOutcome).not.toHaveBeenCalled();
        expect(vi.mocked(NodeChildProcess.spawn).mock.calls).toHaveLength(spawnCalls);
        expect(vi.mocked(NodeChildProcess.execFile).mock.calls).toHaveLength(commandCalls);
        expect(await NodeFSP.readFile(manifestPath, "utf8")).toBe(before);
        expect(await NodeFSP.readFile(journalPath, "utf8")).toBe(JSON.stringify(journal));
      } finally {
        await f.cleanup();
      }
    },
  );

  it.each(["committed", "rolled-back"] as const)(
    "records fleet %s proof only after native journal and active-install reconciliation",
    async (terminal) => {
      const f = await fixture(true, terminal);
      const fleet = { bindInstall: vi.fn(), recordOutcome: vi.fn().mockResolvedValue(undefined) };
      try {
        const controller = new JonesDesktopUpdateController({ ...f.options, fleet });
        await controller.configure();
        expect(fleet.recordOutcome).toHaveBeenCalledExactlyOnceWith({
          transactionId: handle,
          status: terminal,
          activeGeneration: f.active.generation,
          activeSource: f.active.sourceSha,
        });
        fleet.recordOutcome.mockClear();
        await NodeFSP.writeFile(
          controller.manifestPath,
          JSON.stringify({ ...f.active, sourceSha: "0".repeat(40) }),
        );
        const invalid = new JonesDesktopUpdateController({ ...f.options, fleet });
        await invalid.configure();
        expect(fleet.recordOutcome).not.toHaveBeenCalled();
      } finally {
        await f.cleanup();
      }
    },
  );

  it.each(["discarded", "refused", "uncertain"] as const)(
    "handles native discard %s without removing candidate bytes or live state",
    async (status) => {
      const f = await fixture();
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      try {
        await f.controller.configure();
        await f.controller.check();
        await f.controller.download();
        let selectionPath = "";
        nativeCommand.mockImplementation((command, args, _options, callback) => {
          expect(command).toBe("/usr/bin/python3");
          if (typeof callback !== "function" || !Array.isArray(args))
            throw new Error("Invalid command fixture.");
          expect(args[3]).toBe("--discard-staged");
          expect(args[4]).toBe(handle);
          selectionPath = String(args[6]);
          void (async () => {
            const raw = await NodeFSP.readFile(selectionPath, "utf8");
            expect(args[8]).toBe(NodeCrypto.createHash("sha256").update(raw).digest("hex"));
            if (status === "discarded") await NodeFSP.unlink(selectionPath);
            callback(
              null,
              JSON.stringify({ protocol: 1, operation: "discard-staged", handle, status }),
              "",
            );
          })().catch((cause: NodeChildProcess.ExecFileException) => callback(cause, "", ""));
          return new NodeChildProcess.ChildProcess();
        });
        expect(await f.controller.discard("wrong-handle")).toEqual({
          accepted: false,
          completed: false,
        });
        expect(await f.controller.discard(handle)).toEqual({
          accepted: status === "discarded",
          completed: status === "discarded",
        });
        if (status === "discarded") {
          expect(f.controller.state.jones?.stagedHandle).toBeUndefined();
          await expect(NodeFSP.lstat(selectionPath)).rejects.toMatchObject({ code: "ENOENT" });
          const restarted = f.restart();
          await restarted.configure();
          expect(restarted.state.jones?.stagedHandle).toBeUndefined();
          await f.controller.check();
          expect(f.controller.state.jones?.provenance?.artifactId).toBe(99);
        } else {
          expect(f.controller.state.jones?.stagedHandle).toBe(handle);
          expect((await NodeFSP.lstat(selectionPath)).isFile()).toBe(true);
          if (status === "uncertain")
            expect(f.controller.state.jones?.capability.reason).toBe("blocked");
        }
        expect(
          (
            await NodeFSP.lstat(
              NodePath.join(f.home, "runtime", "jones-updates", "apps", handle, "Candidate.app"),
            )
          ).isDirectory(),
        ).toBe(true);
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );

  it.each(["discarded", "refused", "uncertain"] as const)(
    "replaces an unaccepted stage only after a %s native discard proves it",
    async (status) => {
      const f = await fixture();
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      try {
        await f.controller.configure();
        await f.controller.check();
        await f.controller.download();
        await f.controller.check();
        const next = f.controller.latestAvailable!;
        expect(next.artifactId).toBe(99);
        expect(f.controller.state.jones?.stagedHandle).toBe(handle);
        expect(f.controller.state.jones?.provenance?.buildStartedAt).toBe(candidateBuildStartedAt);
        expect(f.controller.preparationHold).toBeUndefined();
        let discards = 0;
        nativeCommand.mockImplementation((_command, args, _options, callback) => {
          if (typeof callback !== "function" || !Array.isArray(args))
            throw new Error("Invalid command fixture.");
          expect(args[3]).toBe("--discard-staged");
          discards++;
          void (async () => {
            if (status === "discarded") await NodeFSP.unlink(String(args[6]));
            callback(
              null,
              JSON.stringify({ protocol: 1, operation: "discard-staged", handle, status }),
              "",
            );
          })().catch((cause: NodeChildProcess.ExecFileException) => callback(cause, "", ""));
          return new NodeChildProcess.ChildProcess();
        });

        // Only the latest checked build may replace the stage; a copy of it is not that build.
        expect(await f.controller.supersede(handle, { ...next })).toBe("refused");
        expect(await f.controller.supersede("wrong-handle", next)).toBe("refused");
        expect(discards).toBe(0);

        const reclaimed: Array<{ handle: string; busy: string | null }> = [];
        const stages = f.stages();
        const outcome = await f.controller.supersede(handle, next, async (replaced) => {
          reclaimed.push({ handle: replaced, busy: f.controller.busy });
          // The fixture reuses one handle, so its next stage needs the replaced app removed.
          await NodeFSP.rm(NodePath.join(f.home, "runtime", "jones-updates", "apps", replaced), {
            recursive: true,
          });
        });
        expect(discards).toBe(1);
        expect(f.controller.busy).toBeNull();
        if (status === "discarded") {
          expect(outcome).toBe("replaced");
          expect(reclaimed).toEqual([{ handle, busy: "reclaim" }]);
          expect(f.stages()).toBe(stages + 1);
          expect(f.controller.state.jones?.provenance?.artifactId).toBe(99);
          // The replacement carries its own build time, which this check did not report.
          expect(f.controller.state.jones?.provenance?.buildStartedAt).toBeUndefined();
          expect(f.controller.stagedBuild?.version).toBe(next.version);
        } else {
          expect(outcome).toBe(status);
          expect(reclaimed).toEqual([]);
          expect(f.stages()).toBe(stages);
          expect(f.controller.state.jones?.provenance?.buildStartedAt).toBe(
            candidateBuildStartedAt,
          );
          expect(f.controller.stagedBuild?.version).toBe(candidate.version);
          expect(f.controller.preparationHold?.kind).toBe(
            status === "uncertain" ? "blocked" : undefined,
          );
        }
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );

  it("binds an out-of-process preparation to its on-disk receipts before selecting it", async () => {
    const f = await fixture();
    try {
      const prepare =
        (forge: boolean): NonNullable<JonesDesktopUpdateOptions["prepareStage"]> =>
        async (selected, roots, onVerifying) => {
          const prepared = await prepareJonesStage(selected, roots, "darwin", {
            actions: f.options.client!,
            stageApp: f.options.stageApp!,
            onVerifying,
          });
          return forge
            ? { ...prepared, app: { ...prepared.app, appDigest: "0".repeat(64) } }
            : prepared;
        };
      const forged = new JonesDesktopUpdateController({
        ...f.options,
        prepareStage: prepare(true),
      });
      await forged.configure();
      await forged.check();
      expect(await forged.download()).toEqual({ accepted: true, completed: false });
      expect(forged.state.jones?.stagedHandle).toBeUndefined();
      await NodeFSP.rm(NodePath.join(f.home, "runtime", "jones-updates", "apps", handle), {
        recursive: true,
      });

      const phases: Array<string | undefined> = [];
      const honest = new JonesDesktopUpdateController({
        ...f.options,
        prepareStage: prepare(false),
        onState: async (state) => {
          phases.push(state.jones?.phase);
        },
      });
      await honest.configure();
      await honest.check();
      expect(await honest.download()).toEqual({ accepted: true, completed: true });
      expect(honest.state.jones?.stagedHandle).toBe(handle);
      expect(phases).toContain("verifying");
    } finally {
      await f.cleanup();
    }
  });

  it("retains a markerless candidate without preparing or launching native activation", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      await f.controller.check();
      await f.controller.download();
      expect(f.controller.state.jones?.capability.install).toBe(false);
      expect(await f.controller.install(handle)).toEqual({
        accepted: false,
        completed: false,
        failed: false,
        refusal: "startup-gate-unavailable",
      });
      expect(f.controller.state.jones?.stagedHandle).toBe(handle);
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(candidate.artifactId);
      expect(f.controller.state.downloadedVersion).toBe(candidate.version);
      const transactions = NodePath.join(f.home, "runtime", "jones-updates", "transactions");
      await expect(NodeFSP.stat(transactions)).rejects.toMatchObject({ code: "ENOENT" });
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      expect(await NodeFSP.readFile(NodePath.join(f.profile, "opaque"), "utf8")).toBe(
        "same-host-profile",
      );
      const restored = f.restart();
      await restored.configure();
      expect(restored.state.jones?.stagedHandle).toBe(handle);
      expect(restored.state.jones?.capability.install).toBe(false);
    } finally {
      await f.cleanup();
    }
  });
  it("refuses a snapshot selection changed by a host check before download reservation", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      await f.controller.check();
      const selection = {
        artifactId: f.controller.state.jones!.provenance!.artifactId,
        sourceSha: f.controller.state.jones!.provenance!.sourceSha,
      };
      const concurrentCheck = f.controller.check();
      await concurrentCheck;
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(99);
      expect(await f.controller.download(selection)).toEqual({
        accepted: false,
        completed: false,
        refusal: "selection-mismatch",
      });
      expect(f.stages()).toBe(0);
      expect(f.controller.state.jones?.stagedHandle).toBeUndefined();
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      expect(
        await f.controller.download({
          artifactId: 99,
          sourceSha: "f".repeat(40),
        }),
      ).toEqual({ accepted: true, completed: true });
      expect(f.stages()).toBe(1);
    } finally {
      await f.cleanup();
    }
  });
  it.each(["intent.json", "prepare-intent.json"])(
    "holds a retained %s without a completed transaction receipt",
    async (marker) => {
      const f = await fixture();
      try {
        const transaction = NodePath.join(
          f.home,
          "runtime",
          "jones-updates",
          "transactions",
          handle,
        );
        await NodeFSP.mkdir(transaction, { recursive: true });
        const file = NodePath.join(transaction, marker);
        await NodeFSP.writeFile(file, "retained-unknown-intent");
        await f.controller.configure();
        expect(f.controller.state.jones?.phase).toBe("blocked");
        expect(f.controller.state.jones?.capability.install).toBe(false);
        expect(f.controller.state.jones?.capability.reason).toBe("blocked");
        expect(await NodeFSP.readFile(file, "utf8")).toBe("retained-unknown-intent");
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        await f.cleanup();
      }
    },
  );
  it.each(["retired", "writer-busy", "uncertain", "wrong-binding"] as const)(
    "retains current and failed generations while guarded old-success retirement reports %s",
    async (outcome) => {
      const f = await fixture(true, "committed");
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      const operations: string[] = [];
      try {
        const transactions = NodePath.join(f.home, "runtime", "jones-updates", "transactions");
        const currentRaw = await NodeFSP.readFile(
          NodePath.join(transactions, handle, "journal.json"),
          "utf8",
        );
        const current = JSON.parse(currentRaw);
        const oldId = "1".repeat(64),
          failedId = "2".repeat(64);
        const old = NodePath.join(transactions, oldId),
          failed = NodePath.join(transactions, failedId);
        await NodeFSP.mkdir(old);
        await NodeFSP.mkdir(failed);
        const oldRaw = JSON.stringify({
          ...current,
          intent: { ...current.intent, transactionId: oldId },
        });
        await NodeFSP.writeFile(NodePath.join(old, "journal.json"), oldRaw);
        await NodeFSP.writeFile(NodePath.join(old, "retained-payload"), "old recovery");
        await NodeFSP.writeFile(
          NodePath.join(failed, "journal.json"),
          JSON.stringify({
            ...current,
            phase: "rolled-back",
            intent: { ...current.intent, transactionId: failedId },
          }),
        );
        nativeCommand.mockImplementation((_command, args, _options, callback) => {
          if (typeof callback !== "function") throw new Error("Missing callback.");
          const operation = String(args?.[3]).slice(2);
          operations.push(operation);
          expect(args?.[4]).toBe(oldId);
          if (operation === "retire-transaction" && outcome === "retired")
            NodeFS.unlinkSync(NodePath.join(old, "retained-payload"));
          callback(
            null,
            JSON.stringify({
              protocol: 1,
              operation,
              transactionId: oldId,
              status:
                operation === "inspect-retirement"
                  ? "ready"
                  : outcome === "retired"
                    ? "retired"
                    : outcome === "uncertain"
                      ? "uncertain"
                      : "refused",
              reason: outcome === "writer-busy" ? outcome : undefined,
              planSha256: "a".repeat(64),
              activeBinding:
                outcome === "wrong-binding" ? { ...f.active, generation: "other" } : f.active,
              journalSha256: NodeCrypto.createHash("sha256").update(oldRaw).digest("hex"),
              targets: [{ path: NodePath.join(old, "retained-payload"), bytes: 12 }],
            }),
            "",
          );
          return new NodeChildProcess.ChildProcess();
        });
        await f.controller.configure();
        expect(operations).toEqual(
          outcome === "wrong-binding"
            ? ["inspect-retirement"]
            : ["inspect-retirement", "retire-transaction"],
        );
        expect(
          await NodeFSP.readFile(NodePath.join(transactions, handle, "journal.json"), "utf8"),
        ).toBe(currentRaw);
        expect(await NodeFSP.readFile(NodePath.join(old, "journal.json"), "utf8")).toBe(oldRaw);
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
        if (outcome === "retired") {
          await expect(NodeFSP.lstat(NodePath.join(old, "retained-payload"))).rejects.toMatchObject(
            { code: "ENOENT" },
          );
        } else {
          expect(await NodeFSP.readFile(NodePath.join(old, "retained-payload"), "utf8")).toBe(
            "old recovery",
          );
          expect(f.controller.state.message).toContain("Older recovery data remains retained");
        }
      } finally {
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );
  it.each(["withdrawn", "uncertain", "wrong-digest", "wrong-binding"] as const)(
    "recovers a dispatched preparation only with exact withdrawal proof: %s",
    async (outcome) => {
      const f = await fixture();
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      const operations: string[] = [];
      try {
        await f.controller.configure();
        await f.controller.check();
        await f.controller.download();
        const updater = NodePath.join(f.home, "runtime", "jones-updates");
        const staging = NodePath.join(updater, "staging");
        const selectionPath = NodePath.join(staging, (await NodeFSP.readdir(staging))[0]!);
        const selection = JSON.parse(await NodeFSP.readFile(selectionPath, "utf8"));
        selection.app.startupGateProtocol = 1;
        await NodeFSP.writeFile(selectionPath, JSON.stringify(selection));
        const stageReceipt = JSON.parse(await NodeFSP.readFile(selection.app.receiptPath, "utf8"));
        stageReceipt.app = selection.app;
        await NodeFSP.writeFile(selection.app.receiptPath, JSON.stringify(stageReceipt));
        const selectionSha256 = NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(selectionPath))
          .digest("hex");
        const directory = NodePath.join(updater, "transactions", handle);
        await NodeFSP.mkdir(directory, { recursive: true });
        const claim = {
          protocol: 1,
          preparationClaimProtocol: 3,
          transactionId: handle,
          stagedHandle: handle,
          selectionPath,
          selectionSha256,
          home: f.home,
          databasePath: f.databasePath,
          profile: f.profile,
          environmentId: f.active.environmentId,
        };
        const claimRaw = JSON.stringify(claim) + "\n";
        const claimSha256 = NodeCrypto.createHash("sha256").update(claimRaw).digest("hex");
        await NodeFSP.writeFile(NodePath.join(directory, "prepare-intent.json"), claimRaw);
        await NodeFSP.writeFile(NodePath.join(directory, "prepare-dispatched.json"), claimRaw);
        nativeCommand.mockImplementation((_command, args, _options, callback) => {
          if (typeof callback !== "function") throw new Error("Missing callback.");
          const operation = String(args?.[3]).slice(2);
          operations.push(operation);
          expect(args?.[4]).toBe(handle);
          if (operation === "withdraw-preparation") {
            expect(args).toContain("--claim-sha256");
            expect(args).toContain(claimSha256);
            const proofPath = NodePath.join(directory, "preparation-withdrawal.json");
            const proofRaw =
              JSON.stringify({
                protocol: 1,
                withdrawalProtocol: 1,
                transactionId: handle,
                claimSha256,
                claim,
                expected:
                  outcome === "wrong-binding" ? { ...f.active, generation: "other" } : f.active,
                evidence: "withdrawal-protocol",
              }) + "\n";
            NodeFS.writeFileSync(proofPath, proofRaw);
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation,
                transactionId: handle,
                status: outcome === "uncertain" ? "uncertain" : "withdrawn",
                claimSha256,
                proofPath,
                proofSha256:
                  outcome === "wrong-digest"
                    ? "0".repeat(64)
                    : NodeCrypto.createHash("sha256").update(proofRaw).digest("hex"),
                reason: outcome === "uncertain" ? "unknown-effect" : undefined,
              }),
              "",
            );
          } else {
            if (operation === "retire-preparation")
              NodeFS.renameSync(directory, NodePath.join(f.home, "retired"));
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation,
                transactionId: handle,
                status: operation === "inspect-preparation" ? "ready" : "retired",
                evidence: "withdrawn",
                planSha256: "a".repeat(64),
              }),
              "",
            );
          }
          return new NodeChildProcess.ChildProcess();
        });
        const restarted = f.restart();
        await restarted.configure();
        expect(operations[0]).toBe("withdraw-preparation");
        if (outcome === "withdrawn") {
          expect(operations).toEqual([
            "withdraw-preparation",
            "inspect-preparation",
            "retire-preparation",
          ]);
          expect(restarted.state.jones?.capability.install).toBe(true);
          await expect(NodeFSP.lstat(directory)).rejects.toMatchObject({ code: "ENOENT" });
        } else {
          expect(operations).toEqual(["withdraw-preparation"]);
          expect(restarted.state.jones?.phase).toBe("blocked");
          expect(restarted.state.jones?.capability.install).toBe(false);
          expect(
            await NodeFSP.readFile(NodePath.join(directory, "prepare-intent.json"), "utf8"),
          ).toBe(claimRaw);
        }
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );
  it.each(["retired", "writer-unknown"] as const)(
    "resolves a retained preparation-only claim at startup when the helper reports %s",
    async (outcome) => {
      const f = await fixture();
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      const transactions = NodePath.join(f.home, "runtime", "jones-updates", "transactions");
      const transaction = NodePath.join(transactions, handle);
      const operations: string[] = [];
      try {
        await NodeFSP.mkdir(transaction, { recursive: true });
        await NodeFSP.writeFile(NodePath.join(transaction, "prepare-intent.json"), "claim");
        await NodeFSP.writeFile(
          NodePath.join(transaction, "prepare-failure.json"),
          JSON.stringify({ protocol: 1, reason: "Native preparation authorization was refused" }),
        );
        nativeCommand.mockImplementation((command, args, _options, callback) => {
          if (typeof callback !== "function") throw new Error("Missing callback.");
          expect(command).toBe("/usr/bin/python3");
          const operation = String(args?.[3]).slice(2);
          operations.push(operation, ...(args?.slice(5) ?? []).map(String));
          expect(args?.[4]).toBe(handle);
          void (async () => {
            if (operation === "retire-preparation") {
              const retired = NodePath.join(f.home, "runtime", "jones-updates", "retired");
              await NodeFSP.mkdir(retired);
              await NodeFSP.rename(transaction, NodePath.join(retired, handle));
            }
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation,
                transactionId: handle,
                ...(outcome === "writer-unknown"
                  ? { status: "refused", reason: outcome, planSha256: "a".repeat(64) }
                  : operation === "inspect-preparation"
                    ? { status: "ready", evidence: "requester-exited", planSha256: "a".repeat(64) }
                    : { status: "retired", evidence: "requester-exited" }),
              }),
              "",
            );
          })().catch((cause) => callback(cause, "", ""));
          return new NodeChildProcess.ChildProcess();
        });
        await f.controller.configure();
        if (outcome === "retired") {
          expect(operations).toEqual([
            "inspect-preparation",
            "retire-preparation",
            "--plan-sha256",
            "a".repeat(64),
          ]);
          expect(f.controller.state.jones?.capability.install).toBe(true);
          expect(await NodeFSP.readdir(transactions)).toEqual([]);
        } else {
          expect(operations).toEqual(["inspect-preparation"]);
          expect(f.controller.state.jones?.phase).toBe("blocked");
          expect(f.controller.state.jones?.capability.reason).toBe("blocked");
          expect(f.controller.state.message).toContain(handle);
          expect(f.controller.state.message).toContain(
            "Native preparation authorization was refused",
          );
          expect(f.controller.state.message).toContain("writer-unknown");
          expect(f.controller.state.message).toContain("--retire-preparation");
          expect(
            await NodeFSP.readFile(NodePath.join(transaction, "prepare-intent.json"), "utf8"),
          ).toBe("claim");
        }
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );
  it.each(["backend-refused", "transport"] as const)(
    "keeps the original cause visible when preparation fails with %s",
    async (failure) => {
      const f = await fixture();
      const nativeCommand = vi.mocked(NodeChildProcess.execFile);
      const preflight = vi.mocked(preflightJonesCandidateStartupGate);
      const proofs = [{ pid: 4242, identity: "Sun Oct 18 10:00:00 2026 Jones Code" }];
      let transactionId = "";
      let request: unknown;
      try {
        const controller = new JonesDesktopUpdateController({
          ...f.options,
          processProofs: async () => proofs,
          prepareNative: async () => {
            if (failure === "transport")
              throw new Error("Native preparation authorization was refused (HTTP 401).");
            return {
              phase: "blocked",
              message:
                "Native preparation was refused: the backend has not observed this staged build.",
            };
          },
        });
        await controller.configure();
        await controller.check();
        await controller.download();
        preflight.mockResolvedValueOnce(undefined);
        nativeCommand.mockImplementation((_command, args, _options, callback) => {
          if (typeof callback !== "function") throw new Error("Missing callback.");
          const tx = NodePath.join(f.home, "runtime", "jones-updates", "transactions");
          if (args?.[0] === "--version") callback(null, "Python 3", "");
          else if (args?.[3] === "--claim-preparation") {
            transactionId = String(args[6]);
            callback(null, claimedPreparation(f.home, transactionId), "");
          } else {
            const operation = String(args?.[3]).slice(2);
            expect(args?.[4]).toBe(transactionId);
            request ??= JSON.parse(
              NodeFS.readFileSync(NodePath.join(tx, transactionId, "prepare-request.json"), "utf8"),
            );
            const refused = failure === "transport";
            if (operation === "retire-preparation")
              NodeFS.renameSync(NodePath.join(tx, transactionId), NodePath.join(f.home, "retired"));
            callback(
              null,
              JSON.stringify({
                protocol: 1,
                operation,
                transactionId,
                ...(refused
                  ? { status: "refused", reason: "writer-unknown" }
                  : operation === "inspect-preparation"
                    ? { status: "ready", evidence: "backend-refused", planSha256: "b".repeat(64) }
                    : { status: "retired", evidence: "backend-refused" }),
              }),
              "",
            );
          }
          return new NodeChildProcess.ChildProcess();
        });
        expect(await controller.install(handle)).toEqual({
          accepted: false,
          completed: false,
          failed: true,
        });
        const claim = NodeFS.readFileSync(
          NodePath.join(
            f.home,
            ...(failure === "transport"
              ? ["runtime", "jones-updates", "transactions", transactionId]
              : ["retired"]),
            "prepare-intent.json",
          ),
        );
        expect(request).toEqual({
          protocol: 1,
          transactionId,
          stagedHandle: handle,
          claimSha256: NodeCrypto.createHash("sha256").update(claim).digest("hex"),
          processes: proofs,
        });
        const cause =
          failure === "transport"
            ? "Native preparation authorization was refused (HTTP 401)"
            : "Native preparation was refused: the backend has not observed this staged build";
        expect(controller.state.message).toContain(cause);
        if (failure === "transport") {
          expect(controller.state.jones?.phase).toBe("blocked");
          expect(controller.state.jones?.capability.reason).toBe("blocked");
          expect(controller.state.message).toContain("Restart Jones Code");
          expect(
            JSON.parse(
              NodeFS.readFileSync(
                NodePath.join(
                  f.home,
                  "runtime",
                  "jones-updates",
                  "transactions",
                  transactionId,
                  "prepare-failure.json",
                ),
                "utf8",
              ),
            ),
          ).toEqual({ protocol: 1, reason: cause });
        } else {
          expect(controller.state.jones?.phase).toBe("staged");
          expect(controller.state.jones?.capability.reason).not.toBe("blocked");
          expect(controller.state.jones?.updateId).toBeUndefined();
          expect(controller.state.message).toContain("Install can be retried");
        }
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        preflight.mockReset();
        nativeCommand.mockReset();
        await f.cleanup();
      }
    },
  );
  it.each([false, true])(
    "restores the fixed stage after restart (attempt layout: %s)",
    async (attemptLayout) => {
      const f = await fixture(true, undefined, attemptLayout);
      try {
        await f.controller.configure();
        await f.controller.check();
        await f.controller.download();
        expect(f.controller.state.jones?.provenance?.buildStartedAt).toBe(candidateBuildStartedAt);
        const restored = f.restart();
        await restored.configure();
        expect(restored.state.jones?.provenance?.buildStartedAt).toBeUndefined();
        expect(restored.state.jones?.phase).toBe("staged");
        expect(restored.state.jones?.stagedHandle).toBe(handle);
        expect(restored.state.jones?.provenance?.artifactId).toBe(candidate.artifactId);
        expect(await restored.download()).toEqual({ accepted: true, completed: true });
        expect(f.stages()).toBe(1);
        expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      } finally {
        await f.cleanup();
      }
    },
  );
  it.each(["committed", "rolled-back"] as const)(
    "reads %s after ordinary stable-path relaunch without a helper generation",
    async (terminal) => {
      const f = await fixture(true, terminal);
      try {
        await f.controller.configure();
        expect(f.controller.state.jones?.phase).toBe(terminal);
        expect(f.controller.state.jones?.capability.install).toBe(true);
      } finally {
        await f.cleanup();
      }
      const unqualified = await fixture(false, terminal);
      try {
        await unqualified.controller.configure();
        expect(unqualified.controller.state.jones?.phase).toBe(terminal);
        expect(unqualified.controller.state.jones?.capability.install).toBe(true);
      } finally {
        await unqualified.cleanup();
      }
    },
  );
  it("keeps Download as staging and fixes Install to the original artifact after a newer check", async () => {
    const f = await fixture();
    try {
      await f.controller.configure();
      expect(f.controller.state.jones?.capability.install).toBe(true);
      await f.controller.check();
      expect(await f.controller.download()).toEqual({ accepted: true, completed: true });
      await f.controller.check();
      expect(f.controller.state.jones?.provenance?.artifactId).toBe(candidate.artifactId);
      expect(f.controller.state.jones?.provenance?.sourceSha).toBe(candidate.source);
      expect(f.controller.state.jones?.provenance?.buildStartedAt).toBe(candidateBuildStartedAt);
      expect(f.controller.state.jones?.installedSource).toEqual({
        repository: candidate.repository,
        sha: f.active.sourceSha,
        tree: f.active.sourceTree,
      });
      expect(f.controller.state.jones?.stagedHandle).toBe(handle);
      expect(f.controller.state.downloadedVersion).toBe(candidate.version);
      expect(f.stages()).toBe(1);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
      expect(await NodeFSP.readFile(NodePath.join(f.profile, "opaque"), "utf8")).toBe(
        "same-host-profile",
      );
      expect((await f.controller.install()).accepted).toBe(false);
      expect((await f.controller.install("different-handle")).accepted).toBe(false);
    } finally {
      await f.cleanup();
    }
  });

  it("allows stable-path checks and staging after a normal launch", async () => {
    const f = await fixture(false);
    try {
      await f.controller.configure();
      expect(f.controller.state.jones?.capability).toEqual({
        check: true,
        download: true,
        install: true,
      });
      await f.controller.check();
      await f.controller.download();
      expect((await f.controller.install(handle)).accepted).toBe(false);
      expect(await NodeFSP.readFile(f.databasePath, "utf8")).toBe("live-state");
    } finally {
      await f.cleanup();
    }
  });
});

it.each(["", "com.jonessystems.jonescode"])(
  "does not discover preview updates for missing or unadmitted production identity %s",
  async (bundleIdentifier) => {
    const f = await fixture();
    try {
      const check = vi.fn(async () => ({ state: "no-new" as const }));
      const controller = new JonesDesktopUpdateController({
        ...f.options,
        client: { check, stage: f.options.client!.stage },
        readInstalledBundleIdentity: async () => ({
          bundleIdentifier,
          version: f.active.version,
          executablePath: f.active.executablePath,
        }),
      });
      await controller.configure();
      expect(await controller.check()).toBe(true);
      expect(controller.state.status).toBe("error");
      expect(check).not.toHaveBeenCalled();
    } finally {
      await f.cleanup();
    }
  },
);
